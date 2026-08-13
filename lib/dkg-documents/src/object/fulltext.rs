//! Full-text search over document bodies and metadata, via [Tantivy].
//!
//! One on-disk Tantivy index covers the whole store. Each document
//! contributes four fields: its `id` and `collection` (stored, for
//! retrieval and filtering) and the tokenized `body` and `metadata` text
//! (the searchable content). A query runs against `body` and `metadata`;
//! results carry the document id and a relevance score.
//!
//! The index is a *derived* structure — Fjall is the source of truth — so a
//! lost or corrupt index can be rebuilt by re-indexing every record. That
//! is what [`DocumentStore::repair`] does.
//!
//! [`DocumentStore::repair`]: crate::store::DocumentStore::repair
//!
//! [Tantivy]: tantivy

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use dkg_core::{Error, Result};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, Value, STORED, STRING, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};
use uuid::Uuid;

/// A full-text search hit: which document, and how well it matched.
#[derive(Debug, Clone)]
pub struct TextHit {
    /// The matched document's id.
    pub id: Uuid,
    /// Tantivy relevance score (higher is better).
    pub score: f32,
}

/// The Tantivy index wrapper.
#[derive(Clone)]
pub struct FullTextIndex {
    index: Index,
    writer: Arc<RwLock<IndexWriter>>,
    reader: IndexReader,
    /// Changes staged since the last commit. Shared across clones so the
    /// writer thread sees work staged through any handle.
    pending: Arc<AtomicUsize>,
    id_field: Field,
    collection_field: Field,
    body_field: Field,
    metadata_field: Field,
}

/// Writer heap budget (50 MB), matching Tantivy's documented minimum-ish
/// working size for a single-thread writer.
const WRITER_HEAP_BYTES: usize = 50_000_000;

impl FullTextIndex {
    /// Open (or create) the index under `dir`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the index cannot be opened or created.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(Error::storage)?;

        let mut builder = Schema::builder();
        // `id` is an exact string (for delete-by-term), stored for retrieval.
        let id_field = builder.add_text_field("id", STRING | STORED);
        let collection_field = builder.add_text_field("collection", TEXT | STORED);
        let body_field = builder.add_text_field("body", TEXT);
        let metadata_field = builder.add_text_field("metadata", TEXT);
        let schema = builder.build();

        let index = if dir.join("meta.json").exists() {
            Index::open_in_dir(dir).map_err(Error::storage)?
        } else {
            Index::create_in_dir(dir, schema).map_err(Error::storage)?
        };

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(Error::storage)?;
        let writer = index.writer(WRITER_HEAP_BYTES).map_err(Error::storage)?;

        Ok(FullTextIndex {
            index,
            writer: Arc::new(RwLock::new(writer)),
            reader,
            pending: Arc::new(AtomicUsize::new(0)),
            id_field,
            collection_field,
            body_field,
            metadata_field,
        })
    }

    /// Index (or re-index) a document. Existing entries for the same id are
    /// replaced.
    ///
    /// The document is staged in the writer's in-memory buffer but **not**
    /// committed: a Tantivy `commit` writes and fsyncs a segment, which is
    /// far too expensive to pay per document. [`Self::commit`] makes staged
    /// work visible to searches, and the pool's writer thread drives it on an
    /// interval. Until then the document is not searchable, though it is
    /// already durable in Fjall and fetchable by id.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on index failure.
    pub fn index_document(
        &self,
        id: Uuid,
        collection: &str,
        body_text: &str,
        metadata_text: &str,
    ) -> Result<()> {
        let writer = self
            .writer
            .write()
            .map_err(|_| Error::storage("index writer lock poisoned"))?;
        let id_str = id.to_string();
        writer.delete_term(Term::from_field_text(self.id_field, &id_str));
        writer
            .add_document(doc!(
                self.id_field => id_str,
                self.collection_field => collection,
                self.body_field => body_text,
                self.metadata_field => metadata_text,
            ))
            .map_err(Error::storage)?;
        drop(writer);
        self.note_pending();
        Ok(())
    }

    /// Remove a document from the index. Removing an absent id is a no-op.
    ///
    /// Staged like [`Self::index_document`]; the removal becomes visible at
    /// the next [`Self::commit`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    pub fn remove_document(&self, id: Uuid) -> Result<()> {
        let writer = self
            .writer
            .write()
            .map_err(|_| Error::storage("index writer lock poisoned"))?;
        writer.delete_term(Term::from_field_text(self.id_field, &id.to_string()));
        drop(writer);
        self.note_pending();
        Ok(())
    }

    /// Record one more staged change, for [`Self::pending`].
    fn note_pending(&self) {
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    /// How many changes are staged but not yet committed.
    ///
    /// The writer thread uses this to commit early when a burst of writes
    /// arrives, rather than letting them sit until the interval elapses.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    /// Commit staged changes and reload the reader, making every write since
    /// the last commit visible to [`Self::search`].
    ///
    /// A no-op when nothing is staged, so an idle interval tick costs
    /// nothing. This is the expensive call the write path deliberately
    /// avoids: amortizing it across a batch is the whole point.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the commit or reader reload fails.
    pub fn commit(&self) -> Result<()> {
        let mut writer = self
            .writer
            .write()
            .map_err(|_| Error::storage("index writer lock poisoned"))?;
        // Re-check under the lock: another thread may have committed the
        // staged work between our read and acquiring the writer.
        if self.pending.load(Ordering::Relaxed) == 0 {
            return Ok(());
        }
        writer.commit().map_err(Error::storage)?;
        // Clear only what this commit covered. Writes staged while we held
        // the lock are impossible (the lock is exclusive), so a swap to zero
        // cannot drop a concurrent increment.
        self.pending.store(0, Ordering::Relaxed);
        drop(writer);
        self.reader.reload().map_err(Error::storage)?;
        Ok(())
    }

    /// Run a full-text query over body and metadata, returning up to `limit`
    /// hits. A `collection` filter, when set, is ANDed into the query.
    ///
    /// Only committed documents are visible; a write staged since the last
    /// commit will not match. See [`Self::commit`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] if the query string does not parse, or
    /// [`Error::Storage`] on search failure.
    pub fn search(
        &self,
        query: &str,
        collection: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TextHit>> {
        let searcher = self.reader.searcher();
        let parser =
            QueryParser::for_index(&self.index, vec![self.body_field, self.metadata_field]);

        // Compose the user's query with an optional collection filter using
        // Tantivy's query syntax, then parse the whole thing once.
        let full = match collection {
            Some(c) => format!("({query}) AND collection:{c}"),
            None => query.to_string(),
        };
        let parsed = parser
            .parse_query(&full)
            .map_err(|e| Error::Invalid(format!("full-text query parse error: {e}")))?;

        let top = searcher
            .search(&parsed, &TopDocs::with_limit(limit))
            .map_err(Error::storage)?;

        let mut hits = Vec::with_capacity(top.len());
        for (score, addr) in top {
            let doc: TantivyDocument = searcher.doc(addr).map_err(Error::storage)?;
            if let Some(id) = doc
                .get_first(self.id_field)
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
            {
                hits.push(TextHit { id, score });
            }
        }
        Ok(hits)
    }
}

impl crate::object::worker::GroupCommit for FullTextIndex {
    fn flush(&self) -> Result<()> {
        self.commit()
    }

    /// Tantivy tracks its own staged count, which is the accurate one: it
    /// counts documents actually added, not jobs the pool happened to run
    /// (a read-only job stages nothing).
    fn should_flush(
        &self,
        _staged: usize,
        elapsed: std::time::Duration,
        config: &crate::object::worker::CommitConfig,
    ) -> bool {
        let pending = self.pending();
        pending > 0 && (pending >= config.max_staged || elapsed >= config.interval)
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
