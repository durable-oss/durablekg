//! The composite object-backed document store (DurableKG feature A).
//!
//! [`ObjectDocumentStore`] is the default [`DocumentStore`]. It composes
//! three layers, each good at one job:
//!
//! - [`blobs`] — OpenDAL blob storage holds document **bodies** (filesystem
//!   by default; S3 or another object store by configuration).
//! - [`meta`] — Fjall, an embedded LSM key/value store, holds **metadata**,
//!   the **URN search-id index**, and a per-collection listing index. Fjall
//!   is the **source of truth** for what exists.
//! - [`fulltext`] — Tantivy provides **full-text search** over body and
//!   metadata text. It is a derived index, rebuildable from Fjall.
//!
//! # Write ordering and repair
//!
//! Writes are *Fjall-first*. A create/replace:
//!
//! 1. writes the [`meta::DocRecord`] with status
//!    [`meta::DocStatus::Pending`] (and points the URN / collection
//!    indexes),
//! 2. writes the body blob via OpenDAL,
//! 3. updates the Tantivy index,
//! 4. flips the record to [`meta::DocStatus::Indexed`].
//!
//! If the process dies between steps 1 and 4, the record is left `Pending`.
//! [`ObjectDocumentStore::repair`] finds every `Pending` record and
//! re-drives steps 2–4, so the three layers converge without manual
//! intervention. Because Fjall is the source of truth, a lost or corrupt
//! Tantivy index is simply rebuilt from the records.
//!
//! # Layout
//!
//! A filesystem-backed store rooted at `<root>` uses:
//!
//! ```text
//! <root>/blobs   OpenDAL filesystem root (document bodies)
//! <root>/meta    Fjall database (records + indexes)
//! <root>/index   Tantivy full-text index
//! ```

pub mod blobs;
pub mod fulltext;
pub mod meta;
pub mod worker;

use std::path::PathBuf;

use async_trait::async_trait;
use chrono::Utc;
use dkg_core::{Error, Result};
use uuid::Uuid;

use crate::model::{Document, NewDocument, Query, Urn};
use crate::store::{DocumentStore, StoreBackend, StoreFuture};
use blobs::BlobStore;
use fulltext::FullTextIndex;
use meta::{DocRecord, DocStatus, MetaStore};
use worker::{PoolConfig, WorkerPool};

/// The composite document store. Cloneable; clones share the underlying
/// layers and worker threads.
///
/// The blocking Fjall and Tantivy layers each run behind a [`WorkerPool`]
/// (one serialized writer thread + a pool of reader threads), so async
/// handlers never block a Tokio worker on disk I/O. OpenDAL blob access is
/// already async and stays on the Tokio runtime. Per-subsystem queues apply
/// backpressure: a saturated queue yields [`Error::Overloaded`] rather than
/// unbounded latency.
#[derive(Clone)]
pub struct ObjectDocumentStore {
    blobs: BlobStore,
    meta: WorkerPool<MetaStore>,
    fulltext: WorkerPool<FullTextIndex>,
}

impl ObjectDocumentStore {
    /// Open a filesystem-backed store rooted at `root` with default pool
    /// tuning. See [`open_filesystem_with`](Self::open_filesystem_with) to
    /// configure queue depth, reader count, and the backpressure timeout.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if any layer fails to open.
    pub fn open_filesystem(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_filesystem_with(root, PoolConfig::default())
    }

    /// Open a filesystem-backed store rooted at `root`, creating the
    /// `blobs`, `meta`, and `index` subdirectories as needed, and spawning
    /// the Fjall/Tantivy worker pools with the given `pool` tuning.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if any layer fails to open.
    pub fn open_filesystem_with(root: impl Into<PathBuf>, pool: PoolConfig) -> Result<Self> {
        let root = root.into();
        let blobs_root = root.join("blobs");
        let meta_path = root.join("meta");
        let index_dir = root.join("index");

        let blobs = BlobStore::filesystem(
            blobs_root
                .to_str()
                .ok_or_else(|| Error::storage("non-UTF-8 blob root path"))?,
        )?;
        let meta = MetaStore::open(
            meta_path
                .to_str()
                .ok_or_else(|| Error::storage("non-UTF-8 meta path"))?,
        )?;
        let fulltext = FullTextIndex::open(&index_dir)?;

        Ok(ObjectDocumentStore {
            blobs,
            meta: WorkerPool::spawn("meta", meta, pool),
            fulltext: WorkerPool::spawn("fulltext", fulltext, pool),
        })
    }

    /// Constructor for the [`StoreBackend`] registry. Accepts a `dkg://`
    /// URL whose path is the store root, e.g. `dkg://./data` or
    /// `dkg:///var/lib/durablekg`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on open failure.
    pub async fn connect(url: String) -> Result<Box<dyn DocumentStore>> {
        let root = url.strip_prefix("dkg://").unwrap_or(&url).to_string();
        let store = ObjectDocumentStore::open_filesystem_with(root, PoolConfig::from_env())?;
        Ok(Box::new(store))
    }

    /// Reconstitute a full [`Document`] from a record plus its body blob.
    async fn hydrate(&self, record: DocRecord) -> Result<Document> {
        let body_bytes = self.blobs.get(record.id).await?;
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes).unwrap_or(serde_json::Value::Null);
        Ok(Document {
            id: record.id,
            collection: record.collection,
            search_ids: record.search_ids,
            body,
            metadata: record.metadata,
            created_at: record.created_at,
            updated_at: record.updated_at,
        })
    }

    /// Drive a `Pending` record through body confirmation, indexing, and the
    /// status flip. Used by both the write path and [`Self::repair`].
    async fn finalize(&self, record: &DocRecord, body: &[u8]) -> Result<()> {
        // Ensure the body blob is present (idempotent: a re-run just
        // overwrites identical bytes).
        self.blobs.put(record.id, body).await?;

        let id = record.id;
        let collection = record.collection.clone();
        let body_text = body_to_text(body);
        let metadata_text = value_to_text(&record.metadata);
        self.fulltext
            .write(move |ft| ft.index_document(id, &collection, &body_text, &metadata_text))
            .await?;

        self.meta.write(move |m| m.mark_indexed(id)).await?;
        Ok(())
    }

    /// Shared create/replace core. `previous` is `Some` for a replace.
    async fn write(
        &self,
        id: Uuid,
        doc: NewDocument,
        previous: Option<DocRecord>,
    ) -> Result<Document> {
        {
            let search_ids = doc.search_ids.clone();
            self.meta
                .read(move |m| m.check_urns_free(&search_ids, id))
                .await?;
        }

        let now = Utc::now();
        let created_at = previous.as_ref().map(|p| p.created_at).unwrap_or(now);
        let record = DocRecord {
            id,
            collection: doc.collection,
            search_ids: doc.search_ids,
            metadata: doc.metadata,
            created_at,
            updated_at: now,
            status: DocStatus::Pending,
        };

        // Step 1: record (source of truth) goes down first, Pending.
        {
            let record = record.clone();
            self.meta
                .write(move |m| m.put(&record, previous.as_ref()))
                .await?;
        }

        // Steps 2–4: body, index, mark Indexed.
        let body = serde_json::to_vec(&doc.body)?;
        self.finalize(&record, &body).await?;

        self.hydrate(DocRecord {
            status: DocStatus::Indexed,
            ..record
        })
        .await
    }
}

/// Flatten arbitrary JSON into searchable text. Strings contribute their
/// contents; object keys and values are walked; numbers/bools stringify.
fn value_to_text(value: &serde_json::Value) -> String {
    let mut out = String::new();
    collect_text(value, &mut out);
    out
}

fn collect_text(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::String(s) => {
            out.push_str(s);
            out.push(' ');
        }
        serde_json::Value::Number(n) => {
            out.push_str(&n.to_string());
            out.push(' ');
        }
        serde_json::Value::Bool(b) => {
            out.push_str(if *b { "true " } else { "false " });
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_text(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                out.push_str(k);
                out.push(' ');
                collect_text(v, out);
            }
        }
        serde_json::Value::Null => {}
    }
}

/// Render a body blob (JSON bytes) as searchable text.
fn body_to_text(body: &[u8]) -> String {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(v) => value_to_text(&v),
        // Non-JSON body: index its raw UTF-8, if any.
        Err(_) => String::from_utf8_lossy(body).into_owned(),
    }
}

#[async_trait]
impl DocumentStore for ObjectDocumentStore {
    async fn migrate(&self) -> Result<()> {
        // Opening the store already creates every layer; nothing further is
        // required. Kept so the trait contract holds across backends.
        Ok(())
    }

    async fn create(&self, doc: NewDocument) -> Result<Document> {
        self.write(Uuid::new_v4(), doc, None).await
    }

    async fn get(&self, id: Uuid) -> Result<Document> {
        let record = self.meta.read(move |m| m.get(id)).await?;
        self.hydrate(record).await
    }

    async fn get_by_urn(&self, urn: &Urn) -> Result<Document> {
        let urn = urn.clone();
        let id = self.meta.read(move |m| m.resolve_urn(&urn)).await?;
        self.get(id).await
    }

    async fn replace(&self, id: Uuid, doc: NewDocument) -> Result<Document> {
        let previous = self.meta.read(move |m| m.get(id)).await?;
        self.write(id, doc, Some(previous)).await
    }

    async fn delete(&self, id: Uuid) -> Result<()> {
        let record = self.meta.read(move |m| m.get(id)).await?;
        // Remove derived data first, then the source of truth last so a
        // crash mid-delete leaves a recoverable (still-listed) record.
        self.fulltext
            .write(move |ft| ft.remove_document(id))
            .await?;
        self.blobs.delete(id).await?;
        self.meta.write(move |m| m.delete(&record)).await?;
        Ok(())
    }

    async fn search(&self, query: Query) -> Result<Vec<Document>> {
        let records = match &query.text {
            // Full-text path: rank via Tantivy, then hydrate the winners.
            Some(text) => {
                let text = text.clone();
                let collection = query.collection.clone();
                let limit = query.limit;
                let hits = self
                    .fulltext
                    .read(move |ft| ft.search(&text, collection.as_deref(), limit))
                    .await?;
                let mut records = Vec::with_capacity(hits.len());
                for hit in hits {
                    // The index can momentarily lag a delete; skip misses.
                    if let Ok(record) = self.meta.read(move |m| m.get(hit.id)).await {
                        records.push(record);
                    }
                }
                records
            }
            // Listing path: by collection (or the whole store), then filter
            // by metadata containment.
            None => match query.collection.clone() {
                Some(collection) => {
                    self.meta
                        .read(move |m| m.list_collection(&collection, usize::MAX))
                        .await?
                }
                None => self.meta.read(|m| m.all()).await?,
            },
        };

        let mut out = Vec::new();
        for record in records {
            if out.len() >= query.limit {
                break;
            }
            if metadata_contains(&record.metadata, &query.metadata_contains) {
                out.push(self.hydrate(record).await?);
            }
        }
        Ok(out)
    }

    async fn repair(&self) -> Result<usize> {
        let mut repaired = 0;
        let all = self.meta.read(|m| m.all()).await?;
        for record in all {
            if record.status == DocStatus::Pending {
                // Re-drive from whatever body we can recover. If the body
                // blob never landed, drop the orphaned record.
                match self.blobs.get(record.id).await {
                    Ok(body) => {
                        self.finalize(&record, &body).await?;
                        repaired += 1;
                    }
                    Err(Error::NotFound(_)) => {
                        let record = record.clone();
                        self.meta.write(move |m| m.delete(&record)).await?;
                        repaired += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(repaired)
    }

    async fn shutdown(&self) -> Result<()> {
        // Each pool's `shutdown` blocks until its threads drain and join, so
        // run them off the async runtime. Draining the Tantivy pool before
        // the metadata pool keeps the wind-down ordered with the write path
        // (index, then mark-indexed), though both simply finish queued work.
        let meta = self.meta.clone();
        let fulltext = self.fulltext.clone();
        tokio::task::spawn_blocking(move || {
            fulltext.shutdown();
            meta.shutdown();
        })
        .await
        .map_err(|e| Error::storage(format!("shutdown join failed: {e}")))?;
        Ok(())
    }
}

/// Test whether `metadata` contains every key/value pair in `needle`
/// (recursively for nested objects) — the in-memory analogue of Postgres'
/// `@>` containment.
fn metadata_contains(metadata: &serde_json::Value, needle: &serde_json::Value) -> bool {
    match (metadata, needle) {
        (_, serde_json::Value::Object(n)) if n.is_empty() => true,
        (serde_json::Value::Object(hay), serde_json::Value::Object(n)) => n.iter().all(|(k, v)| {
            hay.get(k)
                .map(|hv| metadata_contains(hv, v))
                .unwrap_or(false)
        }),
        (a, b) => a == b,
    }
}

inventory::submit! {
    StoreBackend {
        scheme: "dkg",
        connect: dkg_connect,
    }
}

fn dkg_connect(url: &str) -> StoreFuture {
    Box::pin(ObjectDocumentStore::connect(url.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_containment_matches_subsets() {
        let hay = serde_json::json!({"lang": "en", "tags": {"x": 1}});
        assert!(metadata_contains(&hay, &serde_json::json!({})));
        assert!(metadata_contains(&hay, &serde_json::json!({"lang": "en"})));
        assert!(metadata_contains(
            &hay,
            &serde_json::json!({"tags": {"x": 1}})
        ));
        assert!(!metadata_contains(&hay, &serde_json::json!({"lang": "fr"})));
        assert!(!metadata_contains(&hay, &serde_json::json!({"missing": 1})));
    }

    #[test]
    fn value_text_flattens_objects_and_arrays() {
        let v = serde_json::json!({"title": "Hello", "tags": ["rust", "db"]});
        let text = value_to_text(&v);
        assert!(text.contains("title"));
        assert!(text.contains("Hello"));
        assert!(text.contains("rust"));
        assert!(text.contains("db"));
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
