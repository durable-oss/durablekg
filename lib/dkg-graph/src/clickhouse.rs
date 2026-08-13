//! The ClickHouse-backed relational store.
//!
//! Wraps a [`clickhouse::Client`] and runs the SQL built by [`crate::ddl`]
//! and [`crate::dml`]. Construction parses a `clickhouse://` URL. The store
//! reconciles a [`Schema`] into the database additively (create missing
//! tables / columns / join tables, never drop), then serves per-table
//! inserts and queries plus relationship traversal.

use std::collections::HashMap;

use clickhouse::Client;
use dkg_config::RelationKind;
use dkg_core::{Error, Result};
use serde_json::Value;

use crate::dml::{Filter, OrderBy, Record};
use crate::schema::Schema;
use crate::{ddl, dml};

/// One entity to upsert as part of a batch: the row, the aliases identifying
/// it, and optionally the physical uuid to write it under.
///
/// See [`ClickHouseStore::upsert_batch`].
#[derive(Debug, Clone)]
pub struct UpsertItem {
    /// The overview row, without the system `id` column (which is stamped
    /// from the resolved or supplied uuid).
    pub record: Record,
    /// The `(alias_type, alias_value)` pairs identifying this entity.
    pub aliases: Vec<(String, String)>,
    /// A caller-supplied physical uuid. `Some` when the entity was addressed
    /// by `urn:<table>:id:<uuid>`; `None` to resolve from the aliases or mint
    /// a fresh one.
    pub id: Option<String>,
}

/// Escape literal `?` for the `clickhouse` crate's query builder.
///
/// `Client::query` treats `?` as a bound-argument placeholder and rejects a
/// statement whose placeholders outnumber its `bind` calls ("unbound query
/// argument"). Our SQL is fully rendered — every value is already escaped and
/// inlined by [`crate::dml`], with no binds — so any `?` in it is data (a URL
/// query string, a question mark in text). The crate's documented escape for a
/// literal `?` is `??`; double them before handing the statement over.
fn escape_placeholders(sql: &str) -> String {
    sql.replace('?', "??")
}

/// A relational store backed by ClickHouse.
#[derive(Clone)]
pub struct ClickHouseStore {
    client: Client,
}

impl ClickHouseStore {
    /// Connect using a `clickhouse://[user[:password]@]host[:port][/database]`
    /// URL. The scheme is rewritten to `http://` for the ClickHouse HTTP
    /// interface; pass an `http(s)://` URL directly to control the scheme.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if the URL cannot be parsed.
    pub fn connect(url: &str) -> Result<Self> {
        let parsed = ParsedUrl::parse(url)?;
        let mut client = Client::default().with_url(parsed.http_url);
        if let Some(user) = parsed.user {
            client = client.with_user(user);
        }
        if let Some(password) = parsed.password {
            client = client.with_password(password);
        }
        if let Some(database) = parsed.database {
            client = client.with_database(database);
        }
        Ok(ClickHouseStore { client })
    }

    /// Reconcile `schema` into the database: create any missing tables, add
    /// any missing columns to existing tables, and create join tables for
    /// many-to-many relationships. Additive only — nothing is dropped.
    ///
    /// Safe to run repeatedly; a fully-reconciled schema makes no changes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if any DDL statement fails.
    pub async fn reconcile(&self, schema: &Schema) -> Result<()> {
        // Tables and join tables: CREATE ... IF NOT EXISTS is idempotent.
        let tables: Vec<_> = schema.tables().cloned().collect();
        for stmt in ddl::create_all(&tables, schema.relationships()) {
            self.execute(&stmt).await?;
        }

        // Columns added to an already-existing table: diff the live columns
        // against the schema and ADD COLUMN the difference. `effective_columns`
        // includes the system `id`, so a table created before `id` was
        // system-owned gains it here too.
        for table in &tables {
            let existing = self.existing_columns(&table.name).await?;
            for column in &table.effective_columns() {
                if !existing.iter().any(|c| c == &column.name) {
                    self.execute(&ddl::add_column(&table.name, column)).await?;
                }
            }
            // Every table is keyed, so it carries the system version column.
            // ADD COLUMN IF NOT EXISTS makes this a no-op when the
            // freshly-created table already has it.
            if !existing.iter().any(|c| c == ddl::VERSION_COLUMN) {
                self.execute(&ddl::add_version_column(&table.name)).await?;
            }
        }
        Ok(())
    }

    /// Insert records into `table`, validating against the schema.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is undefined,
    /// [`Error::Invalid`] for bad records, or [`Error::Storage`] on backend
    /// failure.
    pub async fn insert(&self, schema: &Schema, table: &str, records: &[Record]) -> Result<()> {
        // `id` is system-owned: a caller may not set it on the public insert
        // path. (The internal sync path stamps it via `insert_with_id`.)
        for record in records {
            if record.contains_key(dkg_config::Table::ID_COLUMN) {
                return Err(Error::Invalid(format!(
                    "column `id` is system-managed and cannot be set on insert into `{table}`"
                )));
            }
        }
        let table = schema.table(table)?;
        let sql = dml::insert_sql(table, records)?;
        self.execute(&sql).await
    }

    /// Insert one record with a system-supplied `id`, used by the alias upsert
    /// path. Unlike the public [`insert`](Self::insert), this *trusts* the
    /// caller to set `id` (it is the minted/resolved uuid).
    async fn insert_with_id(
        &self,
        schema: &Schema,
        table: &str,
        record: &Record,
        id: &str,
    ) -> Result<()> {
        let table = schema.table(table)?;
        let mut row = record.clone();
        row.insert(
            dkg_config::Table::ID_COLUMN.to_string(),
            Value::String(id.to_string()),
        );
        let sql = dml::insert_sql(table, std::slice::from_ref(&row))?;
        self.execute(&sql).await
    }

    /// Query a single table, optionally filtered by any number of
    /// [`Filter`]s (ANDed together), sorted, offset, and limited.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] / [`Error::Invalid`] / [`Error::Storage`].
    pub async fn query(
        &self,
        schema: &Schema,
        table: &str,
        filters: &[Filter],
        order_by: Option<&OrderBy>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Record>> {
        let table = schema.table(table)?;
        let sql = dml::select_sql(table, filters, order_by, offset, limit)?;
        self.fetch_records(&sql).await
    }

    /// Count the rows in a single table, optionally filtered by any number of
    /// [`Filter`]s (ANDed together).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] / [`Error::Invalid`] / [`Error::Storage`].
    pub async fn count(&self, schema: &Schema, table: &str, filters: &[Filter]) -> Result<u64> {
        let table = schema.table(table)?;
        let sql = dml::count_sql(table, filters)?;
        let rows = self.fetch_records(&sql).await?;
        let count = rows
            .into_iter()
            .next()
            .and_then(|r| r.get(dml::COUNT_ALIAS).cloned())
            .ok_or_else(|| Error::storage("count query returned no rows"))?;
        // ClickHouse's JSONEachRow renders UInt64 as a string to avoid
        // precision loss in clients that parse JSON numbers as f64.
        match count {
            Value::Number(n) => n
                .as_u64()
                .ok_or_else(|| Error::storage(format!("count `{n}` is not a valid u64"))),
            Value::String(s) => s
                .parse()
                .map_err(|_| Error::storage(format!("count `{s}` is not a valid u64"))),
            other => Err(Error::storage(format!(
                "expected a numeric count, got {other}"
            ))),
        }
    }

    /// Traverse the relationship from `table` (the row identified by `id`) to
    /// its related rows in `other`. Handles `belongs_to`, `has_many`, and
    /// `many_to_many` using the relationship's foreign key or join table.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if no relationship connects the tables, or
    /// the usual query errors.
    pub async fn related(
        &self,
        schema: &Schema,
        table: &str,
        id: &Value,
        other: &str,
        limit: usize,
    ) -> Result<Vec<Record>> {
        let rel = schema
            .relationship(table, other)
            .ok_or_else(|| Error::NotFound(format!("relationship `{table}` → `{other}`")))?;
        let other_table = schema.table(other)?;

        // For belongs_to / has_many, `relationship(table, other)` only
        // matches a rel declared `from == table, to == other`, so the
        // direction is unambiguous here.
        match rel.kind {
            // `table` belongs_to `other`: read this row's FK, then fetch the
            // single `other` row it points at.
            RelationKind::BelongsTo => {
                let key = rel.key.as_deref().expect("validated key");
                let rows = self
                    .query(schema, table, &[Filter::eq("id", id.clone())], None, 0, 1)
                    .await?;
                let Some(row) = rows.into_iter().next() else {
                    return Ok(vec![]);
                };
                let Some(fk) = row.get(key) else {
                    return Ok(vec![]);
                };
                self.query(
                    schema,
                    other,
                    &[Filter::eq("id", fk.clone())],
                    None,
                    0,
                    limit,
                )
                .await
            }
            // `table` has_many `other`: other.<key> = table.id.
            RelationKind::HasMany => {
                let key = rel.key.as_deref().expect("validated key");
                self.query(
                    schema,
                    other,
                    &[Filter::eq(key, id.clone())],
                    None,
                    0,
                    limit,
                )
                .await
            }
            // many_to_many: ids in the join table, then fetch the rows.
            RelationKind::ManyToMany => {
                let join = rel.join_table();
                let this_col = format!("{table}_id");
                let other_col = format!("{other}_id");
                let id_lit = dml_uuid_literal(id)?;
                // The `other` side may be a ReplacingMergeTree (it has an `id`
                // column it is joined on), so collapse its not-yet-merged
                // duplicates with `FINAL`. The join table has no `id` and stays
                // a plain MergeTree, so it needs none.
                let other_final = if ddl::has_replacing_engine(other_table) {
                    " FINAL"
                } else {
                    ""
                };
                let sql = format!(
                    "SELECT `{other_alias}`.* FROM `{other}` AS `{other_alias}`{other_final} \
                     INNER JOIN `{join}` ON `{join}`.`{other_col}` = `{other_alias}`.`id` \
                     WHERE `{join}`.`{this_col}` = {id_lit} LIMIT {limit}",
                    other = other_table.name,
                    other_alias = other_table.name,
                );
                self.fetch_records(&sql).await
            }
        }
    }

    /// Translate a user alias `(alias_type, alias_value)` on `table` into the
    /// system `id` (uuid), if a mapping exists.
    pub async fn resolve_alias(
        &self,
        schema: &Schema,
        table: &str,
        alias_type: &str,
        alias_value: &str,
    ) -> Result<Option<String>> {
        let table = schema.table(table)?;
        let alias_table = ddl::alias_table_name(table);
        let sql = dml::select_alias_sql(&alias_table, alias_type, alias_value);
        let rows = self.fetch_records(&sql).await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.get("entity_id").and_then(Value::as_str).map(String::from)))
    }

    /// Resolve many `(alias_type, alias_value)` pairs in a single query,
    /// returning a map from pair to the `entity_id` it points at. Pairs with
    /// no mapping are absent from the map.
    ///
    /// This is the bulk form of [`resolve_alias`](Self::resolve_alias): one
    /// round trip for the whole set instead of one per alias.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is undefined, or
    /// [`Error::Storage`] on backend failure.
    pub async fn resolve_aliases(
        &self,
        schema: &Schema,
        table: &str,
        aliases: &[(String, String)],
    ) -> Result<HashMap<(String, String), String>> {
        let mut out = HashMap::new();
        if aliases.is_empty() {
            return Ok(out);
        }
        let table = schema.table(table)?;
        let alias_table = ddl::alias_table_name(table);

        // Deduplicate: the same alias often identifies several entities in a
        // batch, and repeating it just inflates the predicate.
        let unique: Vec<(String, String)> = aliases
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect();

        let sql = dml::select_aliases_sql(&alias_table, &unique);
        for row in self.fetch_records(&sql).await? {
            let (Some(alias_type), Some(alias_value), Some(entity_id)) = (
                row.get("alias_type").and_then(Value::as_str),
                row.get("alias_value").and_then(Value::as_str),
                row.get("entity_id").and_then(Value::as_str),
            ) else {
                continue;
            };
            out.insert(
                (alias_type.to_string(), alias_value.to_string()),
                entity_id.to_string(),
            );
        }
        Ok(out)
    }

    /// Every `(alias_type, alias_value)` registered for `entity_id`.
    ///
    /// The reverse of [`resolve_alias`](Self::resolve_alias), used to find all
    /// the names an entity's facts might have been recorded under.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is undefined, or
    /// [`Error::Storage`] on backend failure.
    pub async fn aliases_for_entity(
        &self,
        schema: &Schema,
        table: &str,
        entity_id: &str,
    ) -> Result<Vec<(String, String)>> {
        let table = schema.table(table)?;
        if table.alternate_keys.is_empty() {
            // No alias table exists for a table without alternate keys.
            return Ok(Vec::new());
        }
        let alias_table = ddl::alias_table_name(table);
        let sql = dml::select_aliases_for_entity_sql(&alias_table, entity_id);
        Ok(self
            .fetch_records(&sql)
            .await?
            .into_iter()
            .filter_map(|row| {
                let alias_type = row.get("alias_type").and_then(Value::as_str)?;
                let alias_value = row.get("alias_value").and_then(Value::as_str)?;
                Some((alias_type.to_string(), alias_value.to_string()))
            })
            .collect())
    }

    /// Every `(alias_type, alias_value)` registered for each of `entity_ids`,
    /// keyed by entity id, in one query.
    ///
    /// The bulk form of [`aliases_for_entity`](Self::aliases_for_entity):
    /// one round trip for a whole batch instead of one per entity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is undefined, or
    /// [`Error::Storage`] on backend failure.
    pub async fn aliases_for_entities(
        &self,
        schema: &Schema,
        table: &str,
        entity_ids: &[String],
    ) -> Result<HashMap<String, Vec<(String, String)>>> {
        let mut out: HashMap<String, Vec<(String, String)>> = HashMap::new();
        if entity_ids.is_empty() {
            return Ok(out);
        }
        let table = schema.table(table)?;
        if table.alternate_keys.is_empty() {
            // No alias table exists for a table without alternate keys.
            return Ok(out);
        }
        let alias_table = ddl::alias_table_name(table);

        // Deduplicate: a batch commonly addresses one entity several times.
        let unique: Vec<String> = entity_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect();

        let sql = dml::select_aliases_for_entities_sql(&alias_table, &unique);
        for row in self.fetch_records(&sql).await? {
            let (Some(alias_type), Some(alias_value), Some(entity_id)) = (
                row.get("alias_type").and_then(Value::as_str),
                row.get("alias_value").and_then(Value::as_str),
                row.get("entity_id").and_then(Value::as_str),
            ) else {
                continue;
            };
            out.entry(entity_id.to_string())
                .or_default()
                .push((alias_type.to_string(), alias_value.to_string()));
        }
        Ok(out)
    }

    /// Upsert `record` into `table` keyed by `aliases`, returning the system
    /// `id` it was written under. Mints a fresh uuid when no alias resolves,
    /// reuses the single resolved uuid, or errors on a merge conflict (aliases
    /// resolving to more than one existing uuid).
    pub async fn upsert_by_aliases(
        &self,
        schema: &Schema,
        table: &str,
        record: &Record,
        aliases: &[(String, String)],
    ) -> Result<String> {
        let table_def = schema.table(table)?;

        // Look each alias up; collect the (alias, uuid) hits so a conflict can
        // name the disagreeing aliases and uuids.
        let mut hits: Vec<(&(String, String), String)> = Vec::new();
        for alias in aliases {
            if let Some(uuid) = self
                .resolve_alias(schema, table, &alias.0, &alias.1)
                .await?
            {
                hits.push((alias, uuid));
            }
        }

        let distinct: std::collections::BTreeSet<&str> =
            hits.iter().map(|(_, u)| u.as_str()).collect();
        let id = match distinct.len() {
            0 => uuid::Uuid::new_v4().to_string(),
            1 => distinct.iter().next().unwrap().to_string(),
            _ => {
                // Merge conflict: name both uuids and the aliases that disagree.
                let detail = hits
                    .iter()
                    .map(|((t, v), u)| format!("{t}={v} -> {u}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::Invalid(format!(
                    "alias conflict on `{table}`: {detail}; refusing to merge distinct records"
                )));
            }
        };

        self.write_row_and_aliases(schema, table_def, record, &id, aliases)
            .await?;
        Ok(id)
    }

    /// Upsert with a caller-supplied physical uuid (the entity was addressed by
    /// `urn:<table>:id:<uuid>`). Writes the row under `id` and registers the
    /// aliases to point at it.
    pub async fn upsert_by_aliases_with_id(
        &self,
        schema: &Schema,
        table: &str,
        record: &Record,
        id: &str,
        aliases: &[(String, String)],
    ) -> Result<String> {
        let table_def = schema.table(table)?;
        // Validate `id` is a uuid so a bad URN fails clearly, not as SQL.
        uuid::Uuid::parse_str(id)
            .map_err(|_| Error::Invalid(format!("entity id `{id}` is not a valid uuid")))?;
        self.write_row_and_aliases(schema, table_def, record, id, aliases)
            .await?;
        Ok(id.to_string())
    }

    /// Upsert many records into one `table` in a single pass.
    ///
    /// Each item is a record plus the aliases identifying it, and optionally a
    /// caller-supplied physical uuid (the entity was addressed by
    /// `urn:<table>:id:<uuid>`). Returns the system `id` written for each
    /// item, in input order.
    ///
    /// Where the per-entity path costs a round trip per alias plus a
    /// single-row `INSERT`, this costs **one** alias `SELECT`, **one** row
    /// `INSERT`, and **one** alias `INSERT` for the whole batch. Single-row
    /// inserts are ClickHouse's documented worst case, so this is the
    /// difference that matters for bulk sync.
    ///
    /// Items are resolved against both the existing alias table *and* each
    /// other: two items in one batch sharing an alias resolve to the same
    /// uuid, exactly as they would if synced one after the other.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the table is undefined,
    /// [`Error::Invalid`] on a bad record, an unparseable supplied id, or an
    /// alias merge conflict, or [`Error::Storage`] on backend failure.
    ///
    /// The write is **not** transactional: a failure of the alias insert
    /// leaves the rows written. Re-running the same batch is safe, since every
    /// table is a `ReplacingMergeTree` keyed by `id`.
    pub async fn upsert_batch(
        &self,
        schema: &Schema,
        table: &str,
        items: &[UpsertItem],
    ) -> Result<Vec<String>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let table_def = schema.table(table)?;

        // One query resolves every alias in the batch.
        let all_aliases: Vec<(String, String)> =
            items.iter().flat_map(|i| i.aliases.clone()).collect();
        let mut resolved = self.resolve_aliases(schema, table, &all_aliases).await?;

        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            let hits: Vec<(&(String, String), &String)> = item
                .aliases
                .iter()
                .filter_map(|a| resolved.get(a).map(|id| (a, id)))
                .collect();

            let distinct: std::collections::BTreeSet<&str> =
                hits.iter().map(|(_, u)| u.as_str()).collect();

            let id = match (&item.id, distinct.len()) {
                // A caller-supplied uuid wins outright; aliases are then
                // (re)pointed at it, matching `upsert_by_aliases_with_id`.
                (Some(supplied), _) => {
                    uuid::Uuid::parse_str(supplied).map_err(|_| {
                        Error::Invalid(format!("entity id `{supplied}` is not a valid uuid"))
                    })?;
                    supplied.clone()
                }
                (None, 0) => uuid::Uuid::new_v4().to_string(),
                (None, 1) => (*distinct.iter().next().unwrap()).to_string(),
                (None, _) => {
                    let detail = hits
                        .iter()
                        .map(|((t, v), u)| format!("{t}={v} -> {u}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(Error::Invalid(format!(
                        "alias conflict on `{table}`: {detail}; refusing to merge distinct records"
                    )));
                }
            };

            // Fold this item's aliases into the resolution map so a later
            // item sharing an alias lands on the same uuid — the behaviour
            // sequential per-entity syncs would produce.
            for alias in &item.aliases {
                resolved.insert(alias.clone(), id.clone());
            }
            ids.push(id);
        }

        // One INSERT for every row in the batch.
        let rows: Vec<Record> = items
            .iter()
            .zip(&ids)
            .map(|(item, id)| {
                let mut row = item.record.clone();
                row.insert(
                    dkg_config::Table::ID_COLUMN.to_string(),
                    Value::String(id.clone()),
                );
                row
            })
            .collect();
        self.execute(&dml::insert_sql(table_def, &rows)?).await?;

        // One INSERT for every alias mapping in the batch.
        let alias_rows: Vec<(String, String, String)> = items
            .iter()
            .zip(&ids)
            .flat_map(|(item, id)| {
                item.aliases
                    .iter()
                    .map(move |(t, v)| (t.clone(), v.clone(), id.clone()))
            })
            .collect();
        if !alias_rows.is_empty() {
            let alias_table = ddl::alias_table_name(table_def);
            self.execute(&dml::insert_alias_rows_sql(&alias_table, &alias_rows))
                .await?;
        }

        Ok(ids)
    }

    /// Write the row under `id`, then (re)register every alias to point at it.
    /// Re-registration replaces a prior mapping (ReplacingMergeTree on the
    /// alias key).
    async fn write_row_and_aliases(
        &self,
        schema: &Schema,
        table_def: &dkg_config::Table,
        record: &Record,
        id: &str,
        aliases: &[(String, String)],
    ) -> Result<()> {
        self.insert_with_id(schema, &table_def.name, record, id)
            .await?;
        if !aliases.is_empty() {
            let alias_table = ddl::alias_table_name(table_def);
            let sql = dml::insert_aliases_sql(&alias_table, id, aliases);
            self.execute(&sql).await?;
        }
        Ok(())
    }

    /// Execute a statement that returns no rows (DDL / INSERT).
    async fn execute(&self, sql: &str) -> Result<()> {
        self.client
            .query(&escape_placeholders(sql))
            .execute()
            .await
            .map_err(Error::storage)
    }

    /// The names of the columns ClickHouse currently has for `table` (empty
    /// if the table does not exist yet).
    async fn existing_columns(&self, table: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '{}'",
            table.replace('\'', "\\'")
        );
        self.client
            .query(&escape_placeholders(&sql))
            .fetch_all::<String>()
            .await
            .map_err(Error::storage)
    }

    /// Run a SELECT and decode its `JSONEachRow` output into records.
    async fn fetch_records(&self, sql: &str) -> Result<Vec<Record>> {
        let bytes = self
            .client
            .query(&escape_placeholders(sql))
            .fetch_bytes("JSONEachRow")
            .map_err(Error::storage)?
            .collect()
            .await
            .map_err(Error::storage)?;

        let mut records = Vec::new();
        for line in bytes.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_slice(line)?;
            match value {
                Value::Object(map) => records.push(map),
                other => {
                    return Err(Error::storage(format!(
                        "expected a JSON object per row, got {other}"
                    )))
                }
            }
        }
        Ok(records)
    }
}

/// Encode a UUID-typed JSON value as a ClickHouse literal for join queries.
fn dml_uuid_literal(id: &Value) -> Result<String> {
    match id {
        Value::String(s) => Ok(format!("toUUID('{}')", s.replace('\'', "\\'"))),
        other => Err(Error::Invalid(format!(
            "expected a UUID string id, got {other}"
        ))),
    }
}

/// A parsed `clickhouse://` connection URL.
struct ParsedUrl {
    http_url: String,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

impl ParsedUrl {
    /// Parse `clickhouse://[user[:password]@]host[:port][/database]`. An
    /// `http://` or `https://` scheme is preserved; `clickhouse://` maps to
    /// `http://`.
    fn parse(url: &str) -> Result<Self> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| Error::config(format!("malformed ClickHouse URL `{url}`")))?;
        let http_scheme = match scheme {
            "clickhouse" | "http" => "http",
            "https" => "https",
            other => return Err(Error::config(format!("unsupported scheme `{other}`"))),
        };

        // Split optional `user:pass@` credentials from `host[:port]/db`.
        let (creds, host_part) = match rest.split_once('@') {
            Some((creds, host)) => (Some(creds), host),
            None => (None, rest),
        };
        let (user, password) = match creds {
            Some(creds) => match creds.split_once(':') {
                Some((u, p)) => (Some(u.to_string()), Some(p.to_string())),
                None => (Some(creds.to_string()), None),
            },
            None => (None, None),
        };

        // Split host[:port] from /database.
        let (host, database) = match host_part.split_once('/') {
            Some((host, db)) if !db.is_empty() => (host, Some(db.to_string())),
            Some((host, _)) => (host, None),
            None => (host_part, None),
        };
        if host.is_empty() {
            return Err(Error::config(format!("ClickHouse URL `{url}` has no host")));
        }

        Ok(ParsedUrl {
            http_url: format!("{http_scheme}://{host}"),
            user,
            password,
            database,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_question_marks_for_the_query_builder() {
        // A URL query string in inlined data must not be read as a bind
        // placeholder: each `?` is doubled, others are left alone.
        assert_eq!(
            escape_placeholders("INSERT INTO t VALUES ('https://x.com/a?s=1&t=2')"),
            "INSERT INTO t VALUES ('https://x.com/a??s=1&t=2')"
        );
        assert_eq!(escape_placeholders("SELECT 1"), "SELECT 1");
        assert_eq!(escape_placeholders("a?b?c"), "a??b??c");
    }

    #[test]
    fn parses_full_url() {
        let p = ParsedUrl::parse("clickhouse://admin:secret@db.example.com:8123/kg").unwrap();
        assert_eq!(p.http_url, "http://db.example.com:8123");
        assert_eq!(p.user.as_deref(), Some("admin"));
        assert_eq!(p.password.as_deref(), Some("secret"));
        assert_eq!(p.database.as_deref(), Some("kg"));
    }

    #[test]
    fn parses_bare_host() {
        let p = ParsedUrl::parse("clickhouse://localhost:8123").unwrap();
        assert_eq!(p.http_url, "http://localhost:8123");
        assert!(p.user.is_none());
        assert!(p.database.is_none());
    }

    #[test]
    fn rejects_schemeless_url() {
        assert!(ParsedUrl::parse("localhost:8123").is_err());
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
