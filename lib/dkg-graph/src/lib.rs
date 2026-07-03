//! # dkg-graph — DurableKG feature B
//!
//! A relational record store for business entities — `people`, `businesses`,
//! and whatever else a deployment defines. Each table is authored as its own
//! Trongoscript [`dkg_config::Table`] entity (its columns only), and the
//! associations between tables are separate [`dkg_config::Relationship`]
//! entities (`belongs_to` / `has_many` / `many_to_many`).
//!
//! Those entities are assembled and validated **once on load** into a
//! [`Schema`], then reconciled into ClickHouse additively: missing tables,
//! columns, and many-to-many join tables are created; nothing is ever
//! dropped (cleanup is an out-of-band operation).
//!
//! The default and only backend is ClickHouse ([`ClickHouseStore`]), chosen
//! for fast scans over large record sets. Backends register through the
//! [`GraphBackend`] inventory registry, mirroring the document store, and
//! [`open`] selects one by URL scheme.
//!
//! ```no_run
//! use dkg_graph::{open, Schema};
//! use dkg_config::{Table, Relationship};
//! use serde_json::json;
//!
//! # async fn run(tables: Vec<Table>, rels: Vec<Relationship>) -> dkg_core::Result<()> {
//! let schema = Schema::load(tables, rels)?;
//! let store = open("clickhouse://localhost:8123/kg").await?;
//! store.reconcile(&schema).await?;          // create/alter tables additively
//!
//! store
//!     .insert(&schema, "people", &[json!({ "id": "…", "name": "Ada" }).as_object().unwrap().clone()])
//!     .await?;
//! let people = store.query(&schema, "people", &[], None, 0, 100).await?;
//! # let _ = people;
//! # Ok(())
//! # }
//! ```

pub mod clickhouse;
pub mod ddl;
pub mod dml;
pub mod schema;

use async_trait::async_trait;
use dkg_core::registry::Named;
use dkg_core::{Error, Result};
use serde_json::Value;

pub use clickhouse::ClickHouseStore;
pub use dml::{Filter, Op, OrderBy, Record, SortDirection};
pub use schema::Schema;

/// A relational graph store: reconcile a [`Schema`], then insert, query, and
/// traverse relationships between tables.
///
/// The trait is object-safe so callers can hold an `Arc<dyn GraphStore>` and
/// stay backend-agnostic. The [`Schema`] is passed per call rather than held
/// by the store, so one connection can serve whatever schema the caller has
/// loaded.
#[async_trait]
pub trait GraphStore: Send + Sync {
    /// Reconcile `schema` into the backend additively (create missing
    /// tables, columns, and join tables; never drop).
    ///
    /// # Errors
    ///
    /// Returns an error if any DDL fails.
    async fn reconcile(&self, schema: &Schema) -> Result<()>;

    /// Insert records into `table`.
    ///
    /// # Errors
    ///
    /// Returns an error if the table is unknown, a record is invalid, or the
    /// backend write fails.
    async fn insert(&self, schema: &Schema, table: &str, records: &[Record]) -> Result<()>;

    /// Query a table, optionally filtered by any number of [`Filter`]s
    /// (ANDed together), sorted, offset, and limited.
    ///
    /// # Errors
    ///
    /// Returns an error if the table, a filter column, or the `order_by`
    /// column is unknown, or the backend read fails.
    async fn query(
        &self,
        schema: &Schema,
        table: &str,
        filters: &[Filter],
        order_by: Option<&OrderBy>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Record>>;

    /// Count the rows in a table, optionally filtered by any number of
    /// [`Filter`]s (ANDed together).
    ///
    /// # Errors
    ///
    /// Returns an error if the table or a filter column is unknown, or the
    /// backend read fails.
    async fn count(&self, schema: &Schema, table: &str, filters: &[Filter]) -> Result<u64>;

    /// Fetch the rows of `other` related to the `table` row identified by
    /// `id`, following the declared relationship.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if no relationship connects the tables, or
    /// a query error.
    async fn related(
        &self,
        schema: &Schema,
        table: &str,
        id: &Value,
        other: &str,
        limit: usize,
    ) -> Result<Vec<Record>>;

    /// Translate a user alias `(alias_type, alias_value)` on `table` into the
    /// system `id` (uuid), if a mapping exists. A convenience lookup layer:
    /// the system reasons only about the uuid, but a human may address a row by
    /// any of its declared alternate keys.
    ///
    /// # Errors
    ///
    /// Returns an error if the table is unknown or the backend read fails.
    async fn resolve_alias(
        &self,
        schema: &Schema,
        table: &str,
        alias_type: &str,
        alias_value: &str,
    ) -> Result<Option<String>>;

    /// Upsert a record into `table` keyed by its aliases, returning the system
    /// `id` it was written under.
    ///
    /// `aliases` is the set of `(alias_type, alias_value)` pairs that identify
    /// the entity (blank values must be filtered out by the caller). The store
    /// looks each up: if they resolve to a single existing uuid, that row is
    /// replaced; if to several distinct uuids, it is a merge conflict and an
    /// error is returned; if to none, a fresh v4 uuid is minted. The record is
    /// written under that uuid and all aliases are (re)registered to point at
    /// it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] on an alias merge conflict, or a backend
    /// error.
    async fn upsert_by_aliases(
        &self,
        schema: &Schema,
        table: &str,
        record: &Record,
        aliases: &[(String, String)],
    ) -> Result<String>;

    /// Like [`upsert_by_aliases`](Self::upsert_by_aliases) but the system `id`
    /// is supplied by the caller (the entity was addressed by its physical
    /// uuid). The row is written under `id` and the aliases are registered to
    /// point at it.
    ///
    /// # Errors
    ///
    /// Returns a backend error, or [`Error::Invalid`] if `id` is not a uuid.
    async fn upsert_by_aliases_with_id(
        &self,
        schema: &Schema,
        table: &str,
        record: &Record,
        id: &str,
        aliases: &[(String, String)],
    ) -> Result<String>;
}

#[async_trait]
impl GraphStore for ClickHouseStore {
    async fn reconcile(&self, schema: &Schema) -> Result<()> {
        ClickHouseStore::reconcile(self, schema).await
    }

    async fn insert(&self, schema: &Schema, table: &str, records: &[Record]) -> Result<()> {
        ClickHouseStore::insert(self, schema, table, records).await
    }

    async fn query(
        &self,
        schema: &Schema,
        table: &str,
        filters: &[Filter],
        order_by: Option<&OrderBy>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Record>> {
        ClickHouseStore::query(self, schema, table, filters, order_by, offset, limit).await
    }

    async fn count(&self, schema: &Schema, table: &str, filters: &[Filter]) -> Result<u64> {
        ClickHouseStore::count(self, schema, table, filters).await
    }

    async fn related(
        &self,
        schema: &Schema,
        table: &str,
        id: &Value,
        other: &str,
        limit: usize,
    ) -> Result<Vec<Record>> {
        ClickHouseStore::related(self, schema, table, id, other, limit).await
    }

    async fn resolve_alias(
        &self,
        schema: &Schema,
        table: &str,
        alias_type: &str,
        alias_value: &str,
    ) -> Result<Option<String>> {
        ClickHouseStore::resolve_alias(self, schema, table, alias_type, alias_value).await
    }

    async fn upsert_by_aliases(
        &self,
        schema: &Schema,
        table: &str,
        record: &Record,
        aliases: &[(String, String)],
    ) -> Result<String> {
        ClickHouseStore::upsert_by_aliases(self, schema, table, record, aliases).await
    }

    async fn upsert_by_aliases_with_id(
        &self,
        schema: &Schema,
        table: &str,
        record: &Record,
        id: &str,
        aliases: &[(String, String)],
    ) -> Result<String> {
        ClickHouseStore::upsert_by_aliases_with_id(self, schema, table, record, id, aliases).await
    }
}

/// A registered knowledge-graph backend, dispatched by URL scheme.
pub struct GraphBackend {
    /// URL scheme this backend handles (e.g. `clickhouse`).
    pub scheme: &'static str,
    /// Open a store from a full connection URL.
    pub connect: fn(&str) -> Result<Box<dyn GraphStore>>,
}

impl Named for GraphBackend {
    fn name(&self) -> &str {
        self.scheme
    }
}

inventory::collect!(GraphBackend);

inventory::submit! {
    GraphBackend {
        scheme: "clickhouse",
        connect: |url| Ok(Box::new(ClickHouseStore::connect(url)?)),
    }
}

/// Open a graph store from a connection URL, dispatching on its scheme to a
/// registered [`GraphBackend`].
///
/// # Errors
///
/// Returns [`Error::UnknownBackend`] if no backend handles the URL's scheme,
/// or whatever the backend's `connect` returns.
pub async fn open(url: &str) -> Result<Box<dyn GraphStore>> {
    let scheme = url.split("://").next().unwrap_or("");
    let backend = dkg_core::registry::find::<GraphBackend>(scheme)
        .ok_or_else(|| Error::UnknownBackend(scheme.to_string()))?;
    (backend.connect)(url)
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
