//! Typed schemas for the built-in config kinds.
//!
//! These mirror the data an author writes in Trongoscript. Each registers a
//! [`ConfigKind`] so [`crate::kind::validate`] recognises it, and offers a
//! `from_value` that deserializes an evaluated tree into the typed form.

use dkg_core::{Error, Result};
use serde::{Deserialize, Serialize};
use trongoscript::Value;

use crate::kind::ConfigKind;

/// A scalar column type. Maps onto a ClickHouse column type in the graph
/// backend. The set is deliberately small and portable; modifiers
/// ([`Column::nullable`], [`Column::array`]) extend it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    /// A UUID.
    Uuid,
    /// A UTF-8 string.
    String,
    /// A 64-bit signed integer.
    Int,
    /// A 64-bit float.
    Float,
    /// A boolean.
    Bool,
    /// A timestamp.
    Datetime,
    /// An embedded JSON document.
    Json,
}

/// One column of a [`Table`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    /// Column name. Must be a valid identifier.
    pub name: String,
    /// The column's scalar type.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// Whether the column may hold a null/missing value. Defaults to `false`.
    #[serde(default)]
    pub nullable: bool,
    /// Whether the column holds an array of its type. Defaults to `false`.
    /// May be combined with `nullable` (a nullable array).
    #[serde(default)]
    pub array: bool,
}

/// The name of the system-owned primary-key column injected on every table.
/// Authors never declare it and may never write it: DurableKG creates it as a
/// `uuid`, mints its value, and keys the table on it. See
/// [`Table::ID_COLUMN`] / [`Table::effective_columns`].
pub const SYSTEM_ID_COLUMN: &str = "id";

/// A table definition (DurableKG feature B). Each table is its own config
/// entity (`:kind "table"`) carrying only its own columns — relationships
/// to other tables are declared separately as [`Relationship`] entities.
///
/// Tables hold business records: `people`, `businesses`, and so on.
///
/// Every table has a system-owned `id` (uuid) primary key that authors do
/// **not** declare — it is injected by [`Table::effective_columns`] and minted
/// by DurableKG. Authors may instead declare [`Table::alternate_keys`]: unique
/// alias columns (e.g. `slug`, `domain`) that translate to the system `id` as a
/// user convenience.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Table {
    /// Table name. Unique within a schema; becomes the storage table name.
    pub name: String,
    /// The author-declared columns (the system `id` is *not* among them).
    #[serde(default)]
    pub columns: Vec<Column>,
    /// Names of columns that act as unique alias keys for the row. Each maps
    /// one-to-one to the system `id`, so a row can be addressed by any alias
    /// value or by its UUID. Authored as `:alternate-keys`. Every name must be
    /// a declared column. A convenience lookup layer — not a core identity.
    #[serde(default, alias = "alternate-keys")]
    pub alternate_keys: Vec<String>,
}

impl Table {
    /// The system-owned primary-key column name (`id`).
    pub const ID_COLUMN: &'static str = SYSTEM_ID_COLUMN;

    /// Deserialize a table from an evaluated config tree.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if the tree does not match the schema, a
    /// column name is invalid, the author declares the reserved `id` column,
    /// or an alternate key does not name a declared column.
    pub fn from_value(value: &Value) -> Result<Self> {
        let table: Table = serde_json::from_value(value.clone())
            .map_err(|e| Error::config(format!("invalid table config: {e}")))?;
        if !is_identifier(&table.name) {
            return Err(Error::config(format!(
                "invalid table name `{}` (use letters, digits, underscores)",
                table.name
            )));
        }
        for column in &table.columns {
            if !is_identifier(&column.name) {
                return Err(Error::config(format!(
                    "invalid column name `{}` in table `{}`",
                    column.name, table.name
                )));
            }
            if column.name == SYSTEM_ID_COLUMN {
                return Err(Error::config(format!(
                    "column `id` in table `{}` is system-managed and must not be declared",
                    table.name
                )));
            }
        }
        for key in &table.alternate_keys {
            if !table.columns.iter().any(|c| &c.name == key) {
                return Err(Error::config(format!(
                    "alternate key `{key}` is not a declared column of table `{}`",
                    table.name
                )));
            }
        }
        Ok(table)
    }

    /// The system `id` column.
    fn id_column() -> Column {
        Column {
            name: SYSTEM_ID_COLUMN.to_string(),
            column_type: ColumnType::Uuid,
            nullable: false,
            array: false,
        }
    }

    /// The full column set the storage layer sees: the system `id` (uuid)
    /// prepended to the author-declared columns. DDL and DML use this so the
    /// system key exists on every table without the author declaring it.
    pub fn effective_columns(&self) -> Vec<Column> {
        let mut cols = Vec::with_capacity(self.columns.len() + 1);
        cols.push(Self::id_column());
        cols.extend(self.columns.iter().cloned());
        cols
    }
}

/// The kind of association a [`Relationship`] declares, in Rails-ish terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    /// `from` holds a foreign key referencing one `to` row.
    BelongsTo,
    /// `to` rows reference back to one `from` row (the inverse of a
    /// `belongs_to` on `to`).
    HasMany,
    /// `from` and `to` are linked through an auto-generated join table.
    ManyToMany,
}

/// A relationship between two tables (DurableKG feature B). A standalone
/// config entity (`:kind "relationship"`) — it names two tables but holds
/// no column definitions of its own; those live on the [`Table`] entities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Relationship {
    /// The association kind.
    #[serde(rename = "type")]
    pub kind: RelationKind,
    /// The owning table name.
    pub from: String,
    /// The referenced table name.
    pub to: String,
    /// Foreign-key column on the appropriate side. Required for
    /// `belongs_to` / `has_many`; ignored for `many_to_many` (which uses a
    /// join table). Defaults to `<singular-ish>_id` is *not* inferred — be
    /// explicit.
    #[serde(default)]
    pub key: Option<String>,
}

impl Relationship {
    /// Deserialize a relationship from an evaluated config tree.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if the tree does not match the schema, the
    /// table names are invalid, or a foreign-key relationship is missing its
    /// `key`.
    pub fn from_value(value: &Value) -> Result<Self> {
        let rel: Relationship = serde_json::from_value(value.clone())
            .map_err(|e| Error::config(format!("invalid relationship config: {e}")))?;
        for (which, name) in [("from", &rel.from), ("to", &rel.to)] {
            if !is_identifier(name) {
                return Err(Error::config(format!(
                    "invalid `{which}` table name `{name}` in relationship"
                )));
            }
        }
        if matches!(rel.kind, RelationKind::BelongsTo | RelationKind::HasMany)
            && rel
                .key
                .as_deref()
                .map(|k| !is_identifier(k))
                .unwrap_or(true)
        {
            return Err(Error::config(
                "belongs_to / has_many relationships require a valid `:key` column",
            ));
        }
        Ok(rel)
    }

    /// The name of the auto-generated join table for a `many_to_many`
    /// relationship: the two table names joined by `_`, in sorted order so
    /// `posts↔tags` and `tags↔posts` resolve to the same table.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_config::{Relationship, RelationKind};
    ///
    /// let rel = Relationship {
    ///     kind: RelationKind::ManyToMany,
    ///     from: "tags".into(),
    ///     to: "posts".into(),
    ///     key: None,
    /// };
    /// assert_eq!(rel.join_table(), "posts_tags");
    /// ```
    pub fn join_table(&self) -> String {
        let (a, b) = if self.from <= self.to {
            (&self.from, &self.to)
        } else {
            (&self.to, &self.from)
        };
        format!("{a}_{b}")
    }
}

/// True if `s` is a safe SQL identifier: a non-empty run of ASCII letters,
/// digits, and underscores not starting with a digit. Used to keep
/// author-supplied names from injecting into generated DDL.
pub(crate) fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Which parts of the caller's request to forward to the upstream. Each list
/// is an allowlist of names; anything not named is dropped. All default
/// empty, so by default nothing is forwarded.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ForwardSpec {
    /// Query-parameter names to forward.
    #[serde(default)]
    pub query: Vec<String>,
    /// Request-header names to forward. `Authorization` and `Cookie` are
    /// refused by the endpoints layer even if listed.
    #[serde(default)]
    pub headers: Vec<String>,
    /// Whether to forward the request body.
    #[serde(default)]
    pub body: bool,
}

/// How to compose the upstream request from forwarded request data.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct UpstreamSpec {
    /// A URL template with `{param}` placeholders filled from forwarded
    /// query params, e.g. `https://api/weather/{city}`. When set it replaces
    /// the endpoint `url`; placeholders consume their params and the rest of
    /// the forwarded query is appended. Authored as `:url-template`.
    #[serde(default, alias = "url-template")]
    pub url_template: Option<String>,
    /// Forwarded query-param names to place on the upstream query string
    /// (those not consumed by the template). When empty, every forwarded
    /// param not used by the template is appended.
    #[serde(default)]
    pub query: Vec<String>,
}

/// Which parts of the request vary the cache key. Two calls whose
/// cache-keyed values differ get separate cache entries. All default empty,
/// so by default the cache key is just the endpoint name.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CacheKeySpec {
    /// Query-parameter names that vary the cache.
    #[serde(default)]
    pub query: Vec<String>,
    /// Header names that vary the cache.
    #[serde(default)]
    pub headers: Vec<String>,
}

/// How an endpoint authenticates *itself* to its upstream. The secret value
/// is never in the config — `secret` names a credential resolved at call
/// time (see the `dkg-endpoints` `SecretStore`). This is distinct from
/// forwarding the caller's auth, which is deliberately never done.
///
/// Authored as a tagged map, e.g.
/// `:auth {:type "bearer" :secret "WEATHER_TOKEN"}` or
/// `:auth {:type "header" :name "X-Api-Key" :secret "WEATHER_KEY"}` or
/// `:auth {:type "query" :name "apikey" :secret "WEATHER_KEY"}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthSpec {
    /// `Authorization: Bearer <secret>`.
    Bearer {
        /// Name of the secret holding the token.
        secret: String,
    },
    /// A custom header `<name>: <secret>`.
    Header {
        /// Header name, e.g. `X-Api-Key`.
        name: String,
        /// Name of the secret holding the value.
        secret: String,
    },
    /// A query parameter `<name>=<secret>` on the upstream URL.
    Query {
        /// Query-parameter name, e.g. `apikey`.
        name: String,
        /// Name of the secret holding the value.
        secret: String,
    },
}

impl AuthSpec {
    /// The name of the secret this auth references.
    pub fn secret_name(&self) -> &str {
        match self {
            AuthSpec::Bearer { secret }
            | AuthSpec::Header { secret, .. }
            | AuthSpec::Query { secret, .. } => secret,
        }
    }
}

/// A user-defined API endpoint (DurableKG feature C). When called, the
/// endpoint fetches `url`, stores the result as a document, and serves it
/// from cache for `cache_seconds`.
///
/// By default the endpoint forwards nothing of the caller's request and
/// caches a single body keyed by the endpoint name. The optional `forward`,
/// `upstream`, `cache_key`, and `auth` sections opt into per-request
/// passthrough, per-request cache keys, and upstream authentication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Endpoint {
    /// Route name, used as the public path segment.
    pub name: String,
    /// Upstream URL to fetch.
    pub url: String,
    /// HTTP method for the upstream request. Defaults to `GET`.
    #[serde(default = "default_method")]
    pub method: String,
    /// How long a cached result stays fresh, in seconds. `0` disables
    /// caching. Authored as `:cache-seconds` in Trongoscript.
    #[serde(default, alias = "cache-seconds")]
    pub cache_seconds: u64,
    /// Collection the cached results are stored under. Defaults to the
    /// endpoint name.
    #[serde(default)]
    pub collection: Option<String>,
    /// What of the caller's request to forward upstream. Defaults to nothing.
    #[serde(default)]
    pub forward: ForwardSpec,
    /// How to compose the upstream request. Defaults to fetching `url` as-is.
    #[serde(default)]
    pub upstream: UpstreamSpec,
    /// What varies the cache key. Defaults to the endpoint name only.
    /// Authored as `:cache-key`.
    #[serde(default, alias = "cache-key")]
    pub cache_key: CacheKeySpec,
    /// How the endpoint authenticates to its upstream. `None` (the default)
    /// sends no credential.
    #[serde(default)]
    pub auth: Option<AuthSpec>,
}

fn default_method() -> String {
    "GET".to_string()
}

impl Endpoint {
    /// Deserialize an endpoint from an evaluated config tree.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if the tree does not match the schema.
    pub fn from_value(value: &Value) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|e| Error::config(format!("invalid endpoint config: {e}")))
    }

    /// The collection cached results are stored under (the explicit
    /// `collection`, falling back to the endpoint `name`).
    pub fn target_collection(&self) -> &str {
        self.collection.as_deref().unwrap_or(&self.name)
    }
}

inventory::submit! {
    ConfigKind {
        kind: "table",
        summary: "A relational table definition (feature B)",
        validate: |v| Table::from_value(v).map(|_| ()),
    }
}

inventory::submit! {
    ConfigKind {
        kind: "relationship",
        summary: "An association between two tables (feature B)",
        validate: |v| Relationship::from_value(v).map(|_| ()),
    }
}

inventory::submit! {
    ConfigKind {
        kind: "endpoint",
        summary: "A user-defined caching API endpoint (feature C)",
        validate: |v| Endpoint::from_value(v).map(|_| ()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::evaluate;

    #[test]
    fn parses_a_table() {
        let cfg = evaluate(
            r#"{:kind "table"
                :name "people"
                :columns [{:name "name" :type "string"}
                          {:name "age" :type "int" :nullable true}
                          {:name "tags" :type "string" :array true}]}"#,
        )
        .unwrap();
        let table = Table::from_value(&cfg).unwrap();
        assert_eq!(table.name, "people");
        // The author declares three columns; `id` is system-owned and not
        // among them.
        assert_eq!(table.columns.len(), 3);
        assert_eq!(table.columns[0].column_type, ColumnType::String);
        assert!(table.columns[1].nullable);
        assert!(table.columns[2].array);
        // effective_columns prepends the system uuid `id`.
        let eff = table.effective_columns();
        assert_eq!(eff.len(), 4);
        assert_eq!(eff[0].name, "id");
        assert_eq!(eff[0].column_type, ColumnType::Uuid);
    }

    #[test]
    fn rejects_author_declared_id() {
        let cfg = evaluate(
            r#"{:kind "table" :name "people"
                :columns [{:name "id" :type "uuid"} {:name "name" :type "string"}]}"#,
        )
        .unwrap();
        let err = Table::from_value(&cfg).unwrap_err().to_string();
        assert!(err.contains("system-managed"), "got: {err}");
    }

    #[test]
    fn parses_and_validates_alternate_keys() {
        let cfg = evaluate(
            r#"{:kind "table" :name "businesses"
                :alternate-keys ["slug" "domain"]
                :columns [{:name "slug" :type "string"}
                          {:name "domain" :type "string" :nullable true}]}"#,
        )
        .unwrap();
        let table = Table::from_value(&cfg).unwrap();
        assert_eq!(table.alternate_keys, vec!["slug", "domain"]);
    }

    #[test]
    fn rejects_alternate_key_that_is_not_a_column() {
        let cfg = evaluate(
            r#"{:kind "table" :name "businesses"
                :alternate-keys ["slug" "nope"]
                :columns [{:name "slug" :type "string"}]}"#,
        )
        .unwrap();
        let err = Table::from_value(&cfg).unwrap_err().to_string();
        assert!(err.contains("alternate key `nope`"), "got: {err}");
    }

    #[test]
    fn rejects_unsafe_table_name() {
        let cfg = evaluate(r#"{:kind "table" :name "people; drop" :columns []}"#).unwrap();
        assert!(Table::from_value(&cfg).is_err());
    }

    #[test]
    fn belongs_to_requires_a_key() {
        let ok = evaluate(
            r#"{:kind "relationship" :type "belongs_to" :from "people" :to "businesses" :key "business_id"}"#,
        )
        .unwrap();
        let rel = Relationship::from_value(&ok).unwrap();
        assert_eq!(rel.kind, RelationKind::BelongsTo);
        assert_eq!(rel.key.as_deref(), Some("business_id"));

        let missing = evaluate(
            r#"{:kind "relationship" :type "belongs_to" :from "people" :to "businesses"}"#,
        )
        .unwrap();
        assert!(Relationship::from_value(&missing).is_err());
    }

    #[test]
    fn many_to_many_join_table_is_order_independent() {
        let a = evaluate(
            r#"{:kind "relationship" :type "many_to_many" :from "people" :to "businesses"}"#,
        )
        .unwrap();
        let b = evaluate(
            r#"{:kind "relationship" :type "many_to_many" :from "businesses" :to "people"}"#,
        )
        .unwrap();
        assert_eq!(
            Relationship::from_value(&a).unwrap().join_table(),
            Relationship::from_value(&b).unwrap().join_table()
        );
    }

    #[test]
    fn endpoint_defaults_collection_to_name() {
        let cfg = evaluate(r#"{:kind "endpoint" :name "weather" :url "https://x"}"#).unwrap();
        let ep = Endpoint::from_value(&cfg).unwrap();
        assert_eq!(ep.method, "GET");
        assert_eq!(ep.target_collection(), "weather");
    }

    #[test]
    fn endpoint_passthrough_sections_default_empty() {
        let cfg = evaluate(r#"{:kind "endpoint" :name "weather" :url "https://x"}"#).unwrap();
        let ep = Endpoint::from_value(&cfg).unwrap();
        assert!(ep.forward.query.is_empty());
        assert!(!ep.forward.body);
        assert!(ep.upstream.url_template.is_none());
        assert!(ep.cache_key.query.is_empty());
        assert!(ep.auth.is_none());
    }

    #[test]
    fn endpoint_parses_each_auth_type() {
        let bearer = evaluate(
            r#"{:kind "endpoint" :name "e" :url "https://x"
                :auth {:type "bearer" :secret "TOKEN"}}"#,
        )
        .unwrap();
        assert_eq!(
            Endpoint::from_value(&bearer).unwrap().auth,
            Some(AuthSpec::Bearer {
                secret: "TOKEN".into()
            })
        );

        let header = evaluate(
            r#"{:kind "endpoint" :name "e" :url "https://x"
                :auth {:type "header" :name "X-Api-Key" :secret "KEY"}}"#,
        )
        .unwrap();
        assert_eq!(
            Endpoint::from_value(&header).unwrap().auth,
            Some(AuthSpec::Header {
                name: "X-Api-Key".into(),
                secret: "KEY".into()
            })
        );

        let query = evaluate(
            r#"{:kind "endpoint" :name "e" :url "https://x"
                :auth {:type "query" :name "apikey" :secret "KEY"}}"#,
        )
        .unwrap();
        let ep = Endpoint::from_value(&query).unwrap();
        assert_eq!(ep.auth.as_ref().unwrap().secret_name(), "KEY");
    }

    #[test]
    fn endpoint_auth_header_requires_a_name() {
        // A `header` auth without `:name` fails to deserialize.
        let cfg = evaluate(
            r#"{:kind "endpoint" :name "e" :url "https://x"
                :auth {:type "header" :secret "KEY"}}"#,
        )
        .unwrap();
        assert!(Endpoint::from_value(&cfg).is_err());
    }

    #[test]
    fn endpoint_parses_hyphenated_passthrough_config() {
        let cfg = evaluate(
            r#"{:kind "endpoint"
                :name "weather"
                :url "https://api/weather"
                :forward {:query ["city" "units"] :headers ["accept"] :body false}
                :upstream {:url-template "https://api/weather/{city}" :query ["units"]}
                :cache-key {:query ["city" "units"]}}"#,
        )
        .unwrap();
        let ep = Endpoint::from_value(&cfg).unwrap();
        assert_eq!(ep.forward.query, vec!["city", "units"]);
        assert_eq!(ep.forward.headers, vec!["accept"]);
        assert_eq!(
            ep.upstream.url_template.as_deref(),
            Some("https://api/weather/{city}")
        );
        assert_eq!(ep.upstream.query, vec!["units"]);
        assert_eq!(ep.cache_key.query, vec!["city", "units"]);
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
