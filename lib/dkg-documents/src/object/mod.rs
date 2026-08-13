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
//! 3. stages the document in the Tantivy index,
//! 4. flips the record to [`meta::DocStatus::Indexed`].
//!
//! If the process dies between steps 1 and 4, the record is left `Pending`.
//! [`DocumentStore::repair`] finds every `Pending` record and
//! re-drives steps 2–4, so the three layers converge without manual
//! intervention. Because Fjall is the source of truth, a lost or corrupt
//! Tantivy index is simply rebuilt from the records.
//!
//! [`DocumentStore::repair`]: crate::store::DocumentStore::repair
//!
//! # Group commit
//!
//! Neither blocking layer is flushed per document. An fsync (Fjall) and a
//! segment commit (Tantivy) each cost far more than the write they protect,
//! so paying them once per document caps ingestion at a few dozen writes a
//! second regardless of hardware. Instead each layer's writer thread stages
//! writes and flushes them as a group — on an interval, once enough are
//! staged, or at shutdown. See [`worker::CommitConfig`].
//!
//! Two consequences callers must expect:
//!
//! - **Search lags writes.** A document is not returned by a full-text
//!   [`DocumentStore::search`] until the next Tantivy commit (default: up to
//!   two seconds). Fetching it by id or URN works immediately, since those
//!   read Fjall.
//! - **A hard crash can lose the last unsynced window.** A write is
//!   acknowledged once it is in Fjall's write-ahead log, not once that log is
//!   fsynced, so a power loss or `SIGKILL` may drop writes acknowledged in
//!   the preceding interval. An orderly shutdown flushes and loses nothing.
//!   This trade suits bulk ingestion from a replayable upstream source; a
//!   caller that cannot replay should set `DKG_STORE_COMMIT_INTERVAL_MS=0`
//!   to flush on every write, restoring the previous durability at the
//!   previous throughput.
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
pub mod urn_filter;
pub mod worker;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dkg_core::{Error, Result};
use futures::stream::{StreamExt, TryStreamExt};
use uuid::Uuid;

use crate::model::{Document, NewDocument, Query, Urn};
use crate::store::{DocumentStore, StoreBackend, StoreFuture};
use blobs::BlobStore;
use fulltext::FullTextIndex;
use meta::{DocRecord, DocStatus, MetaStore};
use urn_filter::UrnFilter;
use worker::{PoolConfig, WorkerPool};

/// Blob reads kept in flight while hydrating a grouped search.
///
/// Sized above the reader pool (4 threads by default) so a thread always has
/// another read waiting when it finishes one, but far below the pool's queue
/// depth (1024) so hydration cannot fill the queue and starve concurrent
/// requests — or trip the enqueue timeout into [`Error::Overloaded`].
const HYDRATE_CONCURRENCY: usize = 16;

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
    /// URNs already bound to a document, approximately.
    ///
    /// Consulted before the authoritative `check_urns_free`: a definite
    /// "absent" skips that pool round-trip entirely, which is the common case
    /// during a bulk import. See [`urn_filter`] for why this is safe.
    urns: Arc<UrnFilter>,
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

        // Seed the filter from the URN index before any write can run. An
        // empty filter would report existing URNs absent — a false negative,
        // which would let a write skip the uniqueness check and clobber a
        // live document. The scan is keys-only, so it costs an LSM iteration
        // rather than a decode of every record.
        let existing = meta.all_urns()?;
        // Size for headroom: a store reopened for a bulk import will add far
        // more URNs than it currently holds, and an undersized filter decays
        // into extra lookups (never into wrong answers).
        let urns = Arc::new(UrnFilter::rebuild_from(
            existing.iter().map(String::as_str),
            existing.len().saturating_mul(2).max(10_000),
        ));

        Ok(ObjectDocumentStore {
            blobs,
            meta: WorkerPool::spawn("meta", meta, pool),
            fulltext: WorkerPool::spawn("fulltext", fulltext, pool),
            urns,
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

    /// Drop every entity-index entry for `collection`, reproducing a store
    /// written before the index existed.
    ///
    /// Test-support only: it deliberately degrades the store so the read-path
    /// fallback and [`DocumentStore::migrate`]'s backfill can be exercised
    /// against a realistic pre-index state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on backend failure.
    #[doc(hidden)]
    pub async fn clear_entity_index_for_test(&self, collection: &str) -> Result<usize> {
        let collection = collection.to_string();
        let dropped = self
            .meta
            .write(move |m| m.clear_entity_index(&collection))
            .await?;
        self.meta.write(|m| m.sync()).await?;
        Ok(dropped)
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
        // Consult the Bloom filter first. When it says none of these URNs can
        // be bound, the authoritative check would find nothing — so skip it,
        // and with it a whole round-trip through the worker pool. That is the
        // overwhelmingly common case in a bulk import, where every URN is new.
        //
        // A false positive costs exactly the lookup every write used to pay
        // unconditionally, so the filter is never worse than the old path.
        // A replace (`previous.is_some()`) always checks: its own URNs are in
        // the filter by definition, so the fast path could not fire anyway.
        let maybe_taken = doc
            .search_ids
            .iter()
            .any(|urn| self.urns.might_contain(&urn.to_string()));
        if maybe_taken {
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

        // The URNs are bound now, so the filter must know about them before
        // any later write consults it. Recorded after the put rather than
        // before: adding a URN that then failed to write would cost future
        // writes a needless lookup, and adding one *late* would be a false
        // negative. Only the ordering in this direction is safe.
        //
        // URNs the record dropped (a replace that removed one) are left in
        // the filter — clearing bits could mask another URN and produce the
        // false negative we cannot allow. They cost one wasted lookup each
        // until a rebuild.
        for urn in &record.search_ids {
            self.urns.insert(&urn.to_string());
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
        // Opening the store creates every layer, so the only migration is the
        // entity index: a store written before it existed has records but no
        // index entries. Backfilling here means an upgrade is a restart, and
        // an already-indexed store re-scans nothing.
        //
        // Collections are backfilled one at a time, each as its own job, so a
        // large store does not occupy a reader thread for the whole run.
        //
        // Every step here goes through the unbounded path. Backfilling a
        // multi-million-document collection takes far longer than the
        // execution ceiling that bounds a *request*, and applying that
        // ceiling to a migration would abort the upgrade partway through and
        // fail startup — the store would then run un-backfilled, on the slow
        // fallback, forever.
        let collections = self.meta.write_unbounded(|m| m.collections()).await?;
        for collection in collections {
            let ready = {
                let collection = collection.clone();
                self.meta
                    .write_unbounded(move |m| m.entity_index_ready(&collection))
                    .await?
            };
            if ready {
                continue;
            }
            let indexed = self
                .meta
                .write_unbounded(move |m| m.backfill_entity_index(&collection))
                .await?;
            if indexed > 0 {
                self.meta.write_unbounded(|m| m.sync()).await?;
            }
        }
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
                    // An entity-equality filter is the hot query (every fact
                    // read is one), and it is the query the entity index
                    // exists to serve. Taking it turns a full-collection scan
                    // into a scan of just this entity's documents.
                    match sole_entity_filter(&query.metadata_contains) {
                        Some(entity) => {
                            let entity = entity.to_string();
                            let limit = query.limit;
                            self.meta
                                .read(move |m| {
                                    if m.entity_index_ready(&collection)? {
                                        m.list_entity(&collection, &entity, limit)
                                    } else {
                                        // Un-backfilled store: the index would
                                        // wrongly report no facts, so pay the
                                        // scan rather than lose data.
                                        m.list_collection(&collection, usize::MAX)
                                    }
                                })
                                .await?
                        }
                        None => {
                            self.meta
                                .read(move |m| m.list_collection(&collection, usize::MAX))
                                .await?
                        }
                    }
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

    /// One prefix scan of the collection, bucketing by `field` as it goes.
    ///
    /// The listing path in [`search`](Self::search) decodes every record in
    /// the collection and then discards the ones that do not match — so
    /// calling it once per value costs the whole collection each time. Here
    /// the scan happens once and each record is routed to its bucket by a
    /// hash lookup, which is what turns the resolver's batch sync from
    /// quadratic into linear.
    ///
    /// Bodies are hydrated only for records that land in a wanted bucket, so
    /// the blob reads scale with the results rather than the collection.
    async fn search_grouped(
        &self,
        collection: &str,
        field: &str,
        values: &[String],
        limit_per_value: usize,
    ) -> Result<HashMap<String, Vec<Document>>> {
        if values.is_empty() {
            return Ok(HashMap::new());
        }
        let wanted: std::collections::HashSet<&str> = values.iter().map(String::as_str).collect();

        // Bucket first, hydrate second: hydration is a blob read per record,
        // so it must not happen for records nobody asked for.
        let collection_owned = collection.to_string();
        let indexed = field == meta::ENTITY_FIELD
            && {
                let collection = collection_owned.clone();
                self.meta
                    .read(move |m| m.entity_index_ready(&collection))
                    .await?
            };

        let mut grouped: HashMap<String, Vec<DocRecord>> = HashMap::new();
        if indexed {
            // One indexed prefix scan per requested value. Each touches only
            // that entity's documents, so a chunk of the batch sync costs its
            // own entities rather than a re-scan of the whole collection —
            // which is what made a table-wide sync quadratic in time and
            // linear in resident memory.
            let owned: Vec<String> = values.to_vec();
            let collection = collection_owned.clone();
            let buckets = self
                .meta
                .read(move |m| {
                    let mut buckets = Vec::with_capacity(owned.len());
                    for value in owned {
                        let records = m.list_entity(&collection, &value, limit_per_value)?;
                        if !records.is_empty() {
                            buckets.push((value, records));
                        }
                    }
                    Ok(buckets)
                })
                .await?;
            grouped.extend(buckets);
        } else {
            let records = self
                .meta
                .read(move |m| m.list_collection(&collection_owned, usize::MAX))
                .await?;
            for record in records {
                let Some(key) = record.metadata.get(field).and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some(key) = wanted.get(key) else { continue };
                let bucket = grouped.entry((*key).to_string()).or_default();
                // Cap each bucket independently, mirroring the per-query limit
                // the single-value path applies.
                if bucket.len() < limit_per_value {
                    bucket.push(record);
                }
            }
        }

        // Hydrate concurrently. Each `hydrate` is a blob read that parks on
        // the reader pool, so doing them one at a time leaves every reader
        // thread but one idle and makes a batch cost the *sum* of its blob
        // reads. Flattening to a single stream keeps the pool fed evenly:
        // per-bucket batching would stall on whichever entity happens to have
        // the most fact documents.
        let flat: Vec<(String, DocRecord)> = grouped
            .into_iter()
            .flat_map(|(key, records)| records.into_iter().map(move |r| (key.clone(), r)))
            .collect();

        let hydrated: Vec<(String, Document)> = futures::stream::iter(flat)
            .map(|(key, record)| async move { Ok::<_, Error>((key, self.hydrate(record).await?)) })
            .buffered(HYDRATE_CONCURRENCY)
            .try_collect()
            .await?;

        // `buffered` preserves input order, so each bucket's documents come
        // back in the order the bucketing produced them.
        let mut out: HashMap<String, Vec<Document>> = HashMap::new();
        for (key, doc) in hydrated {
            out.entry(key).or_default().push(doc);
        }
        Ok(out)
    }

    async fn flush(&self) -> Result<()> {
        // Submitted as *write* jobs so they queue behind everything already
        // pending on each writer thread: when these run, every earlier write
        // has been staged, so flushing them covers it. Metadata is synced
        // before the index is committed, matching the write path's ordering.
        self.meta.write(|m| m.sync()).await?;
        self.fulltext.write(|ft| ft.commit()).await?;
        Ok(())
    }

    async fn repair(&self) -> Result<usize> {
        let mut repaired = 0;
        // Unbounded for the same reason as the backfill: this decodes every
        // record in the store, which on a large one takes longer than the
        // ceiling that bounds a request.
        let all = self.meta.write_unbounded(|m| m.all()).await?;
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
        //
        // Both writer threads group-commit a final time as they exit, so an
        // orderly shutdown leaves nothing staged: every acknowledged write is
        // fsynced to Fjall and every staged document is committed to Tantivy.
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

/// The entity value of a containment filter that asks for *exactly* one
/// top-level `entity` string and nothing else.
///
/// Deliberately narrow. The index answers "which documents in this collection
/// carry this entity value" and nothing more, so a filter combining `entity`
/// with another condition is not something it can serve on its own — such a
/// query falls back to the scan path, which still filters correctly.
fn sole_entity_filter(needle: &serde_json::Value) -> Option<&str> {
    let serde_json::Value::Object(map) = needle else {
        return None;
    };
    if map.len() != 1 {
        return None;
    }
    map.get(meta::ENTITY_FIELD)?.as_str()
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
