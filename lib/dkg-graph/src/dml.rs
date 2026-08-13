//! Row encoding and query SQL for ClickHouse.
//!
//! Records are dynamic (`serde_json` objects keyed by column name), so rows
//! are written with generated `INSERT ... VALUES` statements rather than the
//! `clickhouse` crate's static `Row` derive. Literals are encoded per the
//! column's declared [`ColumnType`] with strict escaping, and only columns
//! that exist in the schema are emitted — author data never reaches the SQL
//! unescaped.

use dkg_config::{Column, ColumnType, Table};
use dkg_core::{Error, Result};
use serde_json::Value;

/// A single record: column name → JSON value.
pub type Record = serde_json::Map<String, Value>;

/// A comparison operator for a [`Filter`] clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `IN (...)` — `value` must be a JSON array.
    In,
    /// Substring match, ClickHouse `ILIKE` (case-insensitive). `value` is a
    /// raw pattern the caller supplies already containing any `%`/`_`
    /// wildcards it wants (e.g. `%acme%`).
    Like,
}

/// One query predicate: `column <op> value`. Any number of `Filter`s are
/// ANDed together by [`select_sql`] / [`count_sql`].
#[derive(Debug, Clone)]
pub struct Filter {
    /// The column being filtered.
    pub column: String,
    /// The comparison operator.
    pub op: Op,
    /// The comparison value. For [`Op::In`] this must be a JSON array; for
    /// [`Op::Like`] a string pattern; otherwise a scalar matching the
    /// column's declared type.
    pub value: Value,
}

impl Filter {
    /// An equality filter — the shape every filter used to be.
    pub fn eq(column: impl Into<String>, value: Value) -> Self {
        Filter {
            column: column.into(),
            op: Op::Eq,
            value,
        }
    }
}

impl From<(&str, &Value)> for Filter {
    fn from((column, value): (&str, &Value)) -> Self {
        Filter::eq(column, value.clone())
    }
}

/// Sort order for [`select_sql`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// An `ORDER BY` clause: the column and its direction.
#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: SortDirection,
}

/// Escape and quote a string as a ClickHouse string literal.
fn quote_string(s: &str) -> String {
    // ClickHouse uses backslash escaping inside single-quoted strings.
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

/// Encode a single JSON value as a ClickHouse literal for a scalar column of
/// the given `ty`. Used for non-array columns and as the element encoder for
/// arrays.
fn encode_scalar(ty: ColumnType, value: &Value) -> Result<String> {
    match (ty, value) {
        (_, Value::Null) => Ok("NULL".to_string()),
        (ColumnType::Uuid, Value::String(s)) => Ok(format!("toUUID({})", quote_string(s))),
        (ColumnType::String, Value::String(s)) => Ok(quote_string(s)),
        (ColumnType::Int, Value::Number(n)) if n.is_i64() || n.is_u64() => Ok(n.to_string()),
        (ColumnType::Float, Value::Number(n)) => Ok(n.to_string()),
        (ColumnType::Bool, Value::Bool(b)) => Ok(if *b { "1".into() } else { "0".into() }),
        (ColumnType::Datetime, Value::String(s)) => {
            Ok(format!("parseDateTime64BestEffort({})", quote_string(s)))
        }
        // JSON columns store the serialized document as a string.
        (ColumnType::Json, v) => Ok(quote_string(&v.to_string())),
        (ty, v) => Err(Error::Invalid(format!(
            "value {v} is not valid for a {ty:?} column"
        ))),
    }
}

/// Encode a JSON value as a ClickHouse literal for `column`, honoring its
/// `array` modifier.
fn encode_value(column: &Column, value: &Value) -> Result<String> {
    if column.array {
        match value {
            Value::Null => Ok("[]".to_string()),
            Value::Array(items) => {
                let elems: Result<Vec<String>> = items
                    .iter()
                    .map(|v| encode_scalar(column.column_type, v))
                    .collect();
                Ok(format!("[{}]", elems?.join(", ")))
            }
            other => Err(Error::Invalid(format!(
                "column `{}` is an array but got {other}",
                column.name
            ))),
        }
    } else {
        encode_scalar(column.column_type, value)
    }
}

/// Build an `INSERT INTO ... VALUES` statement for a batch of records into
/// `table`. Only columns declared on the table are written; a record may
/// omit columns (they take ClickHouse defaults) but may not contain unknown
/// ones.
///
/// # Errors
///
/// Returns [`Error::Invalid`] if a record names an unknown column or a value
/// does not match its column type, or if `records` is empty.
pub fn insert_sql(table: &Table, records: &[Record]) -> Result<String> {
    if records.is_empty() {
        return Err(Error::Invalid("no records to insert".into()));
    }

    // The storage column set includes the system `id` (uuid) primary key.
    let effective = table.effective_columns();

    // Use the union of columns present across the batch, in table-declared
    // order, so every row lists the same columns.
    let columns: Vec<&Column> = effective
        .iter()
        .filter(|c| records.iter().any(|r| r.contains_key(&c.name)))
        .collect();

    // Reject unknown columns up front for a clear error. The system version
    // column is allowed even though it is not in the authored schema.
    for record in records {
        for key in record.keys() {
            if key != crate::ddl::VERSION_COLUMN && !effective.iter().any(|c| &c.name == key) {
                return Err(Error::Invalid(format!(
                    "unknown column `{key}` for table `{}`",
                    table.name
                )));
            }
        }
    }

    // Keyed tables carry the system version column. It is stamped here, not
    // by the caller: a record's own `_dkg_version` (a stringified datetime) is
    // honoured when present, otherwise ClickHouse's `now64(3)` is written, so
    // the latest write wins on replace.
    let versioned = crate::ddl::has_replacing_engine(table);

    let mut col_list = columns
        .iter()
        .map(|c| format!("`{}`", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    if versioned {
        if !col_list.is_empty() {
            col_list.push_str(", ");
        }
        col_list.push_str(&format!("`{}`", crate::ddl::VERSION_COLUMN));
    }

    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let mut vals = Vec::with_capacity(columns.len() + 1);
        for column in &columns {
            let v = record.get(&column.name).unwrap_or(&Value::Null);
            vals.push(encode_value(column, v)?);
        }
        if versioned {
            vals.push(version_literal(record.get(crate::ddl::VERSION_COLUMN)));
        }
        rows.push(format!("({})", vals.join(", ")));
    }

    Ok(format!(
        "INSERT INTO `{}` ({}) VALUES {}",
        table.name,
        col_list,
        rows.join(", ")
    ))
}

/// Encode the value for the system version column. A caller-supplied string
/// timestamp is quoted as a `DateTime64` literal; anything else (including a
/// missing value) falls back to ClickHouse's `now64(3)`, so the row is
/// versioned at insert time.
fn version_literal(value: Option<&Value>) -> String {
    match value.and_then(Value::as_str) {
        Some(ts) => quote_string(ts),
        None => "now64(3)".to_string(),
    }
}

/// Render one filter's predicate, e.g. `` `age` >= 21 `` or
/// `` `industry` IN ('roofing', 'plumbing') ``.
///
/// # Errors
///
/// Returns [`Error::Invalid`] if the filter names a column that is not on
/// the table, a value that does not match the column type, or an [`Op::In`]
/// filter whose value is not a JSON array.
fn render_predicate(effective: &[Column], table_name: &str, filter: &Filter) -> Result<String> {
    let column = effective
        .iter()
        .find(|c| c.name == filter.column)
        .ok_or_else(|| {
            Error::Invalid(format!(
                "unknown filter column `{}` for table `{table_name}`",
                filter.column
            ))
        })?;

    match filter.op {
        Op::Eq => Ok(format!(
            "`{}` = {}",
            column.name,
            encode_value(column, &filter.value)?
        )),
        Op::Ne => Ok(format!(
            "`{}` != {}",
            column.name,
            encode_value(column, &filter.value)?
        )),
        Op::Gt => Ok(format!(
            "`{}` > {}",
            column.name,
            encode_value(column, &filter.value)?
        )),
        Op::Gte => Ok(format!(
            "`{}` >= {}",
            column.name,
            encode_value(column, &filter.value)?
        )),
        Op::Lt => Ok(format!(
            "`{}` < {}",
            column.name,
            encode_value(column, &filter.value)?
        )),
        Op::Lte => Ok(format!(
            "`{}` <= {}",
            column.name,
            encode_value(column, &filter.value)?
        )),
        Op::In => {
            let Value::Array(items) = &filter.value else {
                return Err(Error::Invalid(format!(
                    "filter on `{}` uses `in`, which requires an array value",
                    column.name
                )));
            };
            if items.is_empty() {
                // An empty IN-list matches nothing; short-circuit rather than
                // emit invalid SQL (`IN ()`).
                return Ok("1 = 0".to_string());
            }
            let elems: Result<Vec<String>> = items
                .iter()
                .map(|v| encode_scalar(column.column_type, v))
                .collect();
            Ok(format!("`{}` IN ({})", column.name, elems?.join(", ")))
        }
        Op::Like => {
            let Value::String(pattern) = &filter.value else {
                return Err(Error::Invalid(format!(
                    "filter on `{}` uses `like`, which requires a string value",
                    column.name
                )));
            };
            Ok(format!("`{}` ILIKE {}", column.name, quote_string(pattern)))
        }
    }
}

/// Build the `WHERE` clause (including the leading space) for `filters`
/// ANDed together, or the empty string if there are none.
fn where_clause(table: &Table, filters: &[Filter]) -> Result<String> {
    let effective = table.effective_columns();
    if filters.is_empty() {
        return Ok(String::new());
    }
    let predicates: Result<Vec<String>> = filters
        .iter()
        .map(|f| render_predicate(&effective, &table.name, f))
        .collect();
    Ok(format!(" WHERE {}", predicates?.join(" AND ")))
}

/// `FINAL` if `table` is a `ReplacingMergeTree` (every table is, today), else
/// empty. A `ReplacingMergeTree` table may hold not-yet-merged duplicate rows
/// for the same id; `FINAL` collapses them so a read after a re-sync returns
/// the latest overview, not a stale or duplicate copy.
fn final_clause(table: &Table) -> &'static str {
    if crate::ddl::has_replacing_engine(table) {
        " FINAL"
    } else {
        ""
    }
}

/// Build a `SELECT * FROM table` query, optionally filtered by any number of
/// `Filter`s (ANDed together), sorted, offset, and limited. The output
/// format is set separately by the caller (via `fetch_bytes`), so no
/// `FORMAT` clause is included.
///
/// # Errors
///
/// Returns [`Error::Invalid`] if a filter or `order_by` names a column that
/// is not on the table, or a filter value that does not match the column
/// type.
pub fn select_sql(
    table: &Table,
    filters: &[Filter],
    order_by: Option<&OrderBy>,
    offset: usize,
    limit: usize,
) -> Result<String> {
    let where_clause = where_clause(table, filters)?;

    let order_clause = match order_by {
        Some(order) => {
            let effective = table.effective_columns();
            let column = effective
                .iter()
                .find(|c| c.name == order.column)
                .ok_or_else(|| {
                    Error::Invalid(format!(
                        "unknown order-by column `{}` for table `{}`",
                        order.column, table.name
                    ))
                })?;
            let dir = match order.direction {
                SortDirection::Ascending => "ASC",
                SortDirection::Descending => "DESC",
            };
            format!(" ORDER BY `{}` {}", column.name, dir)
        }
        None => String::new(),
    };

    let offset_clause = if offset > 0 {
        format!(" OFFSET {offset}")
    } else {
        String::new()
    };

    Ok(format!(
        "SELECT * FROM `{}`{}{}{} LIMIT {}{}",
        table.name,
        final_clause(table),
        where_clause,
        order_clause,
        limit,
        offset_clause
    ))
}

/// The output column [`count_sql`] aliases its `count()` to. Namespaced
/// (rather than the obvious `count`) so it cannot collide with an
/// author-declared column of that name in the `WHERE` clause — ClickHouse
/// rejects `SELECT count() AS count ... WHERE count >= 5` as an illegal
/// aggregate reference once the alias and a real column share a name.
pub const COUNT_ALIAS: &str = "_dkg_count";

/// Build a `SELECT count() FROM table` query, optionally filtered by any
/// number of `Filter`s (ANDed together). The count is returned under the
/// [`COUNT_ALIAS`] column.
///
/// # Errors
///
/// Returns [`Error::Invalid`] if a filter names a column that is not on the
/// table or a value that does not match the column type.
pub fn count_sql(table: &Table, filters: &[Filter]) -> Result<String> {
    let where_clause = where_clause(table, filters)?;
    Ok(format!(
        "SELECT count() AS `{COUNT_ALIAS}` FROM `{}`{}{}",
        table.name,
        final_clause(table),
        where_clause
    ))
}

/// `SELECT entity_id` from an alias table for one `(alias_type, alias_value)`
/// pair. `FINAL` collapses any superseded mapping so the current `entity_id`
/// is returned. Used to translate a user alias to the system id.
pub fn select_alias_sql(alias_table: &str, alias_type: &str, alias_value: &str) -> String {
    format!(
        "SELECT `entity_id` FROM `{}` FINAL WHERE `alias_type` = {} AND `alias_value` = {} LIMIT 1",
        alias_table,
        quote_string(alias_type),
        quote_string(alias_value)
    )
}

/// `INSERT` one or more `(alias_type, alias_value, entity_id)` mappings into an
/// alias table, stamping the system version so a re-registration replaces the
/// prior mapping. `aliases` is a slice of `(alias_type, alias_value)`; all map
/// to the same `entity_id`.
pub fn insert_aliases_sql(
    alias_table: &str,
    entity_id: &str,
    aliases: &[(String, String)],
) -> String {
    let owned: Vec<(String, String, String)> = aliases
        .iter()
        .map(|(t, v)| (t.clone(), v.clone(), entity_id.to_string()))
        .collect();
    insert_alias_rows_sql(alias_table, &owned)
}

/// `INSERT` alias mappings for *many* entities in one statement.
///
/// Each row is `(alias_type, alias_value, entity_id)`. Batching matters: one
/// statement per entity turns a bulk sync into a round trip per entity, which
/// is the dominant cost once the rows themselves are batched.
///
/// Returns an empty string when there is nothing to write, so callers can skip
/// the round trip entirely.
pub fn insert_alias_rows_sql(alias_table: &str, rows: &[(String, String, String)]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let values: Vec<String> = rows
        .iter()
        .map(|(alias_type, alias_value, entity_id)| {
            format!(
                "({}, {}, toUUID({}), now64(3))",
                quote_string(alias_type),
                quote_string(alias_value),
                quote_string(entity_id)
            )
        })
        .collect();
    format!(
        "INSERT INTO `{}` (`alias_type`, `alias_value`, `entity_id`, `{}`) VALUES {}",
        alias_table,
        crate::ddl::VERSION_COLUMN,
        values.join(", ")
    )
}

/// `SELECT` every `(alias_type, alias_value)` registered for `entity_id` — the
/// reverse of [`select_alias_sql`].
///
/// Facts are indexed under the exact entity URN the caller recorded them with,
/// so an entity written under several aliases has its facts split across
/// several URN keys. Syncing needs all of them, which means asking the alias
/// table what other names this entity answers to.
///
/// `FINAL` collapses superseded mappings so a re-pointed alias is not returned
/// for its former entity.
pub fn select_aliases_for_entity_sql(alias_table: &str, entity_id: &str) -> String {
    format!(
        "SELECT `alias_type`, `alias_value` FROM `{}` FINAL WHERE `entity_id` = toUUID({})",
        alias_table,
        quote_string(entity_id)
    )
}

/// `SELECT` every registered alias for *many* `entity_id`s in one query — the
/// bulk form of [`select_aliases_for_entity_sql`].
///
/// `entity_id` comes back alongside the alias columns so the caller can bucket
/// each row by the entity it belongs to. `FINAL` collapses superseded
/// mappings, matching the single-entity form.
///
/// Returns an empty string for an empty input, so callers can skip the query.
pub fn select_aliases_for_entities_sql(alias_table: &str, entity_ids: &[String]) -> String {
    if entity_ids.is_empty() {
        return String::new();
    }
    let ids: Vec<String> = entity_ids
        .iter()
        .map(|id| format!("toUUID({})", quote_string(id)))
        .collect();
    format!(
        "SELECT `alias_type`, `alias_value`, `entity_id` FROM `{}` FINAL WHERE `entity_id` IN ({})",
        alias_table,
        ids.join(", ")
    )
}

/// `SELECT` the `entity_id` for *many* `(alias_type, alias_value)` pairs in one
/// query, replacing a round trip per alias with a single statement.
///
/// The predicate is a disjunction of exact pairs rather than two independent
/// `IN` lists: `alias_type IN (..) AND alias_value IN (..)` would match the
/// cross product and resolve aliases that were never asked for.
///
/// `FINAL` collapses superseded mappings, matching [`select_alias_sql`]. The
/// alias columns come back alongside `entity_id` so the caller can attribute
/// each row to the pair that produced it.
///
/// Returns an empty string for an empty input, so callers can skip the query.
pub fn select_aliases_sql(alias_table: &str, aliases: &[(String, String)]) -> String {
    if aliases.is_empty() {
        return String::new();
    }
    let predicates: Vec<String> = aliases
        .iter()
        .map(|(alias_type, alias_value)| {
            format!(
                "(`alias_type` = {} AND `alias_value` = {})",
                quote_string(alias_type),
                quote_string(alias_value)
            )
        })
        .collect();
    format!(
        "SELECT `alias_type`, `alias_value`, `entity_id` FROM `{}` FINAL WHERE {}",
        alias_table,
        predicates.join(" OR ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table() -> Table {
        Table {
            name: "people".into(),
            columns: vec![
                Column {
                    name: "name".into(),
                    column_type: ColumnType::String,
                    nullable: false,
                    array: false,
                },
                Column {
                    name: "city".into(),
                    column_type: ColumnType::String,
                    nullable: true,
                    array: false,
                },
                Column {
                    name: "nicknames".into(),
                    column_type: ColumnType::String,
                    nullable: false,
                    array: true,
                },
            ],
            alternate_keys: Vec::new(),
        }
    }

    fn record(v: Value) -> Record {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn insert_encodes_types_and_arrays() {
        let sql = insert_sql(
            &table(),
            &[record(json!({
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "Ada",
                "nicknames": ["Addie", "A"]
            }))],
        )
        .unwrap();
        // A keyed table appends the system version column, stamped at insert.
        assert!(
            sql.contains("INSERT INTO `people` (`id`, `name`, `nicknames`, `_dkg_version`) VALUES")
        );
        assert!(sql.contains("toUUID('00000000-0000-0000-0000-000000000001')"));
        assert!(sql.contains("'Ada'"));
        assert!(sql.contains("['Addie', 'A']"));
        assert!(sql.contains("now64(3)"));
    }

    #[test]
    fn insert_honours_a_supplied_version() {
        let sql = insert_sql(
            &table(),
            &[record(json!({
                "id": "00000000-0000-0000-0000-000000000001",
                "_dkg_version": "2026-06-19 10:00:00.000"
            }))],
        )
        .unwrap();
        // The caller's version is used verbatim instead of now64(3).
        assert!(sql.contains("'2026-06-19 10:00:00.000'"));
        assert!(!sql.contains("now64(3)"));
    }

    #[test]
    fn insert_appends_version_for_every_table() {
        // Every table is keyed (system `id`), so the version column is always
        // appended. Here the record omits `id`, so the column list starts with
        // the authored `msg`, then the system version.
        let logs = Table {
            name: "logs".into(),
            columns: vec![Column {
                name: "msg".into(),
                column_type: ColumnType::String,
                nullable: false,
                array: false,
            }],
            alternate_keys: Vec::new(),
        };
        let sql = insert_sql(&logs, &[record(json!({ "msg": "hi" }))]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `logs` (`msg`, `_dkg_version`) VALUES ('hi', now64(3))"
        );
    }

    #[test]
    fn insert_escapes_string_quotes() {
        let sql = insert_sql(&table(), &[record(json!({ "name": "O'Brien" }))]).unwrap();
        assert!(sql.contains("'O\\'Brien'"), "got {sql}");
    }

    #[test]
    fn insert_rejects_unknown_column() {
        let err = insert_sql(&table(), &[record(json!({ "bogus": 1 }))]).unwrap_err();
        assert!(matches!(err, Error::Invalid(_)));
    }

    #[test]
    fn select_with_filter_builds_where() {
        // `people` has an `id` column → ReplacingMergeTree → reads use FINAL.
        let sql = select_sql(&table(), &[Filter::eq("name", json!("Ada"))], None, 0, 10).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `people` FINAL WHERE `name` = 'Ada' LIMIT 10"
        );
    }

    #[test]
    fn select_with_multiple_filters_ands_them() {
        let sql = select_sql(
            &table(),
            &[
                Filter::eq("name", json!("Ada")),
                Filter::eq("city", json!("Boston")),
            ],
            None,
            0,
            10,
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `people` FINAL WHERE `name` = 'Ada' AND `city` = 'Boston' LIMIT 10"
        );
    }

    #[test]
    fn select_always_uses_final() {
        // Every table is a keyed ReplacingMergeTree now, so reads always use
        // FINAL to collapse not-yet-merged duplicates.
        let table = Table {
            name: "logs".into(),
            columns: vec![Column {
                name: "msg".into(),
                column_type: ColumnType::String,
                nullable: false,
                array: false,
            }],
            alternate_keys: Vec::new(),
        };
        let sql = select_sql(&table, &[], None, 0, 10).unwrap();
        assert_eq!(sql, "SELECT * FROM `logs` FINAL LIMIT 10");
    }

    #[test]
    fn select_rejects_unknown_filter_column() {
        let err = select_sql(&table(), &[Filter::eq("nope", json!(1))], None, 0, 10).unwrap_err();
        assert!(matches!(err, Error::Invalid(_)));
    }

    #[test]
    fn select_with_range_filters() {
        let cols = Table {
            name: "widgets".into(),
            columns: vec![Column {
                name: "count".into(),
                column_type: ColumnType::Int,
                nullable: false,
                array: false,
            }],
            alternate_keys: Vec::new(),
        };
        let sql = select_sql(
            &cols,
            &[
                Filter {
                    column: "count".into(),
                    op: Op::Gte,
                    value: json!(5),
                },
                Filter {
                    column: "count".into(),
                    op: Op::Lt,
                    value: json!(10),
                },
            ],
            None,
            0,
            10,
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `widgets` FINAL WHERE `count` >= 5 AND `count` < 10 LIMIT 10"
        );
    }

    #[test]
    fn select_with_in_filter() {
        let sql = select_sql(
            &table(),
            &[Filter {
                column: "name".into(),
                op: Op::In,
                value: json!(["Ada", "Bob"]),
            }],
            None,
            0,
            10,
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `people` FINAL WHERE `name` IN ('Ada', 'Bob') LIMIT 10"
        );
    }

    #[test]
    fn select_with_empty_in_filter_matches_nothing() {
        let sql = select_sql(
            &table(),
            &[Filter {
                column: "name".into(),
                op: Op::In,
                value: json!([]),
            }],
            None,
            0,
            10,
        )
        .unwrap();
        assert_eq!(sql, "SELECT * FROM `people` FINAL WHERE 1 = 0 LIMIT 10");
    }

    #[test]
    fn select_with_like_filter() {
        let sql = select_sql(
            &table(),
            &[Filter {
                column: "name".into(),
                op: Op::Like,
                value: json!("%ada%"),
            }],
            None,
            0,
            10,
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `people` FINAL WHERE `name` ILIKE '%ada%' LIMIT 10"
        );
    }

    #[test]
    fn select_rejects_in_filter_with_non_array_value() {
        let err = select_sql(
            &table(),
            &[Filter {
                column: "name".into(),
                op: Op::In,
                value: json!("Ada"),
            }],
            None,
            0,
            10,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Invalid(_)));
    }

    #[test]
    fn select_with_order_by_and_offset() {
        let sql = select_sql(
            &table(),
            &[],
            Some(&OrderBy {
                column: "name".into(),
                direction: SortDirection::Descending,
            }),
            20,
            10,
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `people` FINAL ORDER BY `name` DESC LIMIT 10 OFFSET 20"
        );
    }

    #[test]
    fn select_rejects_unknown_order_by_column() {
        let err = select_sql(
            &table(),
            &[],
            Some(&OrderBy {
                column: "nope".into(),
                direction: SortDirection::Ascending,
            }),
            0,
            10,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Invalid(_)));
    }

    #[test]
    fn count_with_filter() {
        let sql = count_sql(&table(), &[Filter::eq("name", json!("Ada"))]).unwrap();
        assert_eq!(
            sql,
            "SELECT count() AS `_dkg_count` FROM `people` FINAL WHERE `name` = 'Ada'"
        );
    }

    #[test]
    fn count_without_filter() {
        let sql = count_sql(&table(), &[]).unwrap();
        assert_eq!(sql, "SELECT count() AS `_dkg_count` FROM `people` FINAL");
    }

    #[test]
    fn select_aliases_matches_exact_pairs_not_the_cross_product() {
        let sql = select_aliases_sql(
            "people_aliases",
            &[
                ("slug".into(), "ada".into()),
                ("email".into(), "ada@example.com".into()),
            ],
        );
        assert_eq!(
            sql,
            "SELECT `alias_type`, `alias_value`, `entity_id` FROM `people_aliases` FINAL \
             WHERE (`alias_type` = 'slug' AND `alias_value` = 'ada') \
             OR (`alias_type` = 'email' AND `alias_value` = 'ada@example.com')"
        );
        // The pairs must stay bound together: a cross-product form would
        // resolve `slug=ada@example.com`, an alias nobody asked about.
        assert!(!sql.contains("IN ("));
    }

    #[test]
    fn select_aliases_of_nothing_is_empty() {
        assert_eq!(select_aliases_sql("people_aliases", &[]), "");
    }

    #[test]
    fn select_aliases_for_entities_reads_many_entities_in_one_query() {
        let sql = select_aliases_for_entities_sql(
            "people_aliases",
            &[
                "0f8fad5b-d9cb-469f-a165-70867728950e".into(),
                "7c9e6679-7425-40de-944b-e07fc1f90ae7".into(),
            ],
        );
        assert_eq!(
            sql,
            "SELECT `alias_type`, `alias_value`, `entity_id` FROM `people_aliases` FINAL \
             WHERE `entity_id` IN (toUUID('0f8fad5b-d9cb-469f-a165-70867728950e'), \
             toUUID('7c9e6679-7425-40de-944b-e07fc1f90ae7'))"
        );
        // `entity_id` comes back so the caller can bucket rows by entity;
        // without it a batch could not tell whose alias is whose.
        assert!(sql.contains("`entity_id`"));
    }

    #[test]
    fn select_aliases_for_entities_of_nothing_is_empty() {
        assert_eq!(select_aliases_for_entities_sql("people_aliases", &[]), "");
    }

    #[test]
    fn select_aliases_for_entities_quotes_its_ids() {
        // Entity ids reach the statement as literals, so a quote in one must
        // be escaped rather than closing the string.
        let sql = select_aliases_for_entities_sql("people_aliases", &["a'b".into()]);
        assert!(!sql.contains("('a'b')"), "unescaped quote: {sql}");
    }

    #[test]
    fn insert_alias_rows_batches_many_entities() {
        let sql = insert_alias_rows_sql(
            "people_aliases",
            &[
                (
                    "slug".into(),
                    "ada".into(),
                    "00000000-0000-0000-0000-000000000001".into(),
                ),
                (
                    "slug".into(),
                    "bob".into(),
                    "00000000-0000-0000-0000-000000000002".into(),
                ),
            ],
        );
        // One statement, two rows, each pointing at its own entity.
        assert_eq!(sql.matches("INSERT INTO").count(), 1);
        assert!(sql.contains("('slug', 'ada', toUUID('00000000-0000-0000-0000-000000000001')"));
        assert!(sql.contains("('slug', 'bob', toUUID('00000000-0000-0000-0000-000000000002')"));
    }

    #[test]
    fn insert_alias_rows_of_nothing_is_empty() {
        assert_eq!(insert_alias_rows_sql("people_aliases", &[]), "");
    }

    #[test]
    fn insert_aliases_still_writes_one_entitys_pairs() {
        // The single-entity helper now delegates to the batch form; its
        // output must not change.
        let sql = insert_aliases_sql(
            "people_aliases",
            "00000000-0000-0000-0000-000000000001",
            &[("slug".into(), "ada".into())],
        );
        assert!(sql.starts_with(
            "INSERT INTO `people_aliases` (`alias_type`, `alias_value`, `entity_id`, `_dkg_version`) VALUES "
        ));
        assert!(sql.contains("('slug', 'ada', toUUID('00000000-0000-0000-0000-000000000001')"));
    }

    #[test]
    fn insert_batches_many_rows_into_one_statement() {
        let sql = insert_sql(
            &table(),
            &[
                record(json!({ "name": "Ada", "nicknames": [] })),
                record(json!({ "name": "Bob", "nicknames": [] })),
            ],
        )
        .unwrap();
        assert_eq!(sql.matches("INSERT INTO").count(), 1);
        assert!(sql.contains("('Ada', []"));
        assert!(sql.contains("('Bob', []"));
    }

    #[test]
    fn count_alias_does_not_collide_with_a_same_named_filter_column() {
        // A table with a `count` column filtered on that column must not
        // render `SELECT count() AS count ... WHERE count >= 5` — ClickHouse
        // rejects that as an illegal aggregate reference once the alias and
        // a real column share a name.
        let widgets = Table {
            name: "widgets".into(),
            columns: vec![Column {
                name: "count".into(),
                column_type: ColumnType::Int,
                nullable: false,
                array: false,
            }],
            alternate_keys: Vec::new(),
        };
        let sql = count_sql(
            &widgets,
            &[Filter {
                column: "count".into(),
                op: Op::Gte,
                value: json!(5),
            }],
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT count() AS `_dkg_count` FROM `widgets` FINAL WHERE `count` >= 5"
        );
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
