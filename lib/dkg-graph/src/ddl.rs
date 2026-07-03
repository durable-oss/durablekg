//! ClickHouse DDL / SQL generation.
//!
//! These are pure functions: given a [`Schema`] (or parts of one) they
//! return the SQL strings the [`crate::clickhouse`] backend executes. Keeping
//! them separate makes the SQL unit-testable without a live ClickHouse, and
//! concentrates identifier handling in one place.
//!
//! All identifiers (table and column names) come from config that was
//! validated with `dkg_config`'s identifier check, so they are safe to
//! interpolate. Generated SQL is additive — `CREATE TABLE IF NOT EXISTS` and
//! `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` — never `DROP`.

use dkg_config::{Column, ColumnType, RelationKind, Relationship, Table};

/// Map a column to its ClickHouse type, applying `array` then `nullable`
/// modifiers. `Nullable(Array(T))` is not valid ClickHouse, so a nullable
/// array becomes `Array(Nullable(T))`.
pub fn clickhouse_type(column: &Column) -> String {
    let base = match column.column_type {
        ColumnType::Uuid => "UUID",
        ColumnType::String => "String",
        ColumnType::Int => "Int64",
        ColumnType::Float => "Float64",
        ColumnType::Bool => "Bool",
        ColumnType::Datetime => "DateTime64(3)",
        // JSON is stored as a String of serialized JSON for portability
        // across ClickHouse versions.
        ColumnType::Json => "String",
    };
    match (column.array, column.nullable) {
        (false, false) => base.to_string(),
        (false, true) => format!("Nullable({base})"),
        (true, false) => format!("Array({base})"),
        (true, true) => format!("Array(Nullable({base}))"),
    }
}

/// The name of the system-managed version column added to every keyed
/// (`ReplacingMergeTree`) table. It is *not* part of the authored schema —
/// the backend adds it during reconcile and stamps it on every insert — but
/// it is a real `DateTime64(3)` column, so it appears in `SELECT *` output.
/// The leading underscore and `dkg` prefix mark it as plumbing.
pub const VERSION_COLUMN: &str = "_dkg_version";

/// The ClickHouse type of [`VERSION_COLUMN`].
pub const VERSION_COLUMN_TYPE: &str = "DateTime64(3)";

/// The `CREATE TABLE IF NOT EXISTS` statement for a table.
///
/// A table with an `id` column is keyed by it and uses the
/// `ReplacingMergeTree([`VERSION_COLUMN`])` engine, so re-inserting a row with
/// an existing `id` *replaces* it, keeping the copy with the greatest version
/// timestamp. That is what lets an entity's overview be re-synced repeatedly:
/// each sync writes the row for that `id` with a fresh version, and the engine
/// keeps the newest. Collapse happens at background merge time, so reads use
/// `FINAL` (see [`has_replacing_engine`] and the query builder) to see the
/// latest row immediately.
///
/// The version column is appended automatically to keyed tables; authors do
/// not declare it. A table with no `id` column has no natural key to replace
/// on, so it stays a plain `MergeTree` ordered by `tuple()` (append-only) and
/// gets no version column.
pub fn create_table(table: &Table) -> String {
    // Every table has the system `id` (uuid) primary key (prepended by
    // `effective_columns`), so every table is keyed and versioned.
    let mut cols: Vec<String> = table
        .effective_columns()
        .iter()
        .map(|c| format!("    `{}` {}", c.name, clickhouse_type(c)))
        .collect();
    cols.push(format!("    `{VERSION_COLUMN}` {VERSION_COLUMN_TYPE}"));

    format!(
        "CREATE TABLE IF NOT EXISTS `{}` (\n{}\n) ENGINE = ReplacingMergeTree(`{VERSION_COLUMN}`) ORDER BY `id`",
        table.name,
        cols.join(",\n"),
    )
}

/// Whether reads of `table` should use `FINAL` to collapse not-yet-merged
/// duplicate rows. Every table is now a keyed `ReplacingMergeTree`, so this is
/// always true; kept as a function for call-site clarity.
pub fn has_replacing_engine(_table: &Table) -> bool {
    true
}

/// The name of the alias-mapping table for `table`: `<table>__aliases`. The
/// double underscore keeps it from colliding with an author table or a
/// many-to-many join table (which use a single underscore).
pub fn alias_table_name(table: &Table) -> String {
    format!("{}__aliases", table.name)
}

/// The `CREATE TABLE IF NOT EXISTS` for a table's alias-mapping table, or
/// `None` when the table declares no alternate keys.
///
/// The mapping is `(alias_type, alias_value) -> entity_id`, keyed on the
/// `(alias_type, alias_value)` pair via `ReplacingMergeTree` so re-registering
/// an alias replaces the prior mapping — that pair stays unique, which is what
/// makes an alias equivalent to the system id.
pub fn create_alias_table(table: &Table) -> Option<String> {
    if table.alternate_keys.is_empty() {
        return None;
    }
    Some(format!(
        "CREATE TABLE IF NOT EXISTS `{}` (\n    `alias_type` String,\n    `alias_value` String,\n    `entity_id` UUID,\n    `{VERSION_COLUMN}` {VERSION_COLUMN_TYPE}\n) ENGINE = ReplacingMergeTree(`{VERSION_COLUMN}`) ORDER BY (`alias_type`, `alias_value`)",
        alias_table_name(table)
    ))
}

/// The `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statement that adds the
/// system version column to an already-existing keyed table. Used during
/// reconcile so tables created before the version column existed gain it
/// additively.
pub fn add_version_column(table_name: &str) -> String {
    format!(
        "ALTER TABLE `{table_name}` ADD COLUMN IF NOT EXISTS `{VERSION_COLUMN}` {VERSION_COLUMN_TYPE}"
    )
}

/// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for a single column. Used to
/// reconcile columns added to an existing table.
pub fn add_column(table_name: &str, column: &Column) -> String {
    format!(
        "ALTER TABLE `{}` ADD COLUMN IF NOT EXISTS `{}` {}",
        table_name,
        column.name,
        clickhouse_type(column)
    )
}

/// The `CREATE TABLE IF NOT EXISTS` statement for a many-to-many join table.
///
/// The join table is named by [`Relationship::join_table`] and has two
/// `UUID` foreign-key columns, `<a>_id` and `<b>_id`, where `a`/`b` are the
/// two table names in the same sorted order the join-table name uses.
pub fn create_join_table(rel: &Relationship) -> String {
    let (a, b) = if rel.from <= rel.to {
        (&rel.from, &rel.to)
    } else {
        (&rel.to, &rel.from)
    };
    format!(
        "CREATE TABLE IF NOT EXISTS `{}` (\n    `{}_id` UUID,\n    `{}_id` UUID\n) ENGINE = MergeTree ORDER BY (`{}_id`, `{}_id`)",
        rel.join_table(),
        a,
        b,
        a,
        b
    )
}

/// Every DDL statement needed to reconcile a freshly-loaded schema into an
/// empty database: a `CREATE TABLE` per table, plus a join table per
/// many-to-many relationship. (Column reconciliation for *existing* tables
/// is handled separately by the backend, which diffs live columns against
/// the schema and emits [`add_column`].)
pub fn create_all(tables: &[Table], relationships: &[Relationship]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for table in tables {
        out.push(create_table(table));
        if let Some(alias_ddl) = create_alias_table(table) {
            out.push(alias_ddl);
        }
    }
    for rel in relationships {
        if matches!(rel.kind, RelationKind::ManyToMany) {
            out.push(create_join_table(rel));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: ColumnType, nullable: bool, array: bool) -> Column {
        Column {
            name: name.into(),
            column_type: ty,
            nullable,
            array,
        }
    }

    #[test]
    fn type_mapping_applies_modifiers() {
        assert_eq!(
            clickhouse_type(&col("a", ColumnType::Uuid, false, false)),
            "UUID"
        );
        assert_eq!(
            clickhouse_type(&col("a", ColumnType::Int, true, false)),
            "Nullable(Int64)"
        );
        assert_eq!(
            clickhouse_type(&col("a", ColumnType::String, false, true)),
            "Array(String)"
        );
        assert_eq!(
            clickhouse_type(&col("a", ColumnType::Float, true, true)),
            "Array(Nullable(Float64))"
        );
    }

    #[test]
    fn create_table_orders_by_id_when_present() {
        let table = Table {
            name: "people".into(),
            columns: vec![col("name", ColumnType::String, false, false)],
            alternate_keys: Vec::new(),
        };
        let sql = create_table(&table);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `people`"));
        assert!(sql.contains("`id` UUID"));
        assert!(sql.contains("ORDER BY `id`"));
        // A keyed table dedups on re-insert via ReplacingMergeTree, versioned
        // by the system column so the newest write wins.
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_dkg_version`)"));
        assert!(sql.contains("`_dkg_version` DateTime64(3)"));
        assert!(has_replacing_engine(&table));
    }

    #[test]
    fn every_table_is_keyed_and_versioned() {
        // The system injects `id`, so even a table that declares only one
        // column is a keyed, versioned ReplacingMergeTree.
        let table = Table {
            name: "logs".into(),
            columns: vec![col("msg", ColumnType::String, false, false)],
            alternate_keys: Vec::new(),
        };
        let sql = create_table(&table);
        assert!(sql.contains("`id` UUID"));
        assert!(sql.contains("`_dkg_version` DateTime64(3)"));
        assert!(sql.contains("ORDER BY `id`"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_dkg_version`)"));
        assert!(has_replacing_engine(&table));
    }

    #[test]
    fn add_version_column_is_idempotent_ddl() {
        assert_eq!(
            add_version_column("businesses"),
            "ALTER TABLE `businesses` ADD COLUMN IF NOT EXISTS `_dkg_version` DateTime64(3)"
        );
    }

    #[test]
    fn create_table_injects_id_even_without_authored_id() {
        // Authors no longer declare `id`; the system prepends it so the table
        // is always keyed by `id` and uses ReplacingMergeTree.
        let table = Table {
            name: "logs".into(),
            columns: vec![col("msg", ColumnType::String, false, false)],
            alternate_keys: Vec::new(),
        };
        let sql = create_table(&table);
        assert!(sql.contains("`id` UUID"));
        assert!(sql.contains("ORDER BY `id`"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`_dkg_version`)"));
        assert!(has_replacing_engine(&table));
    }

    #[test]
    fn join_table_columns_are_sorted_and_keyed() {
        let rel = Relationship {
            kind: RelationKind::ManyToMany,
            from: "people".into(),
            to: "businesses".into(),
            key: None,
        };
        let sql = create_join_table(&rel);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `businesses_people`"));
        assert!(sql.contains("`businesses_id` UUID"));
        assert!(sql.contains("`people_id` UUID"));
    }

    #[test]
    fn add_column_is_idempotent_ddl() {
        let sql = add_column("people", &col("age", ColumnType::Int, true, false));
        assert_eq!(
            sql,
            "ALTER TABLE `people` ADD COLUMN IF NOT EXISTS `age` Nullable(Int64)"
        );
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
