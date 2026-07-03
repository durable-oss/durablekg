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
//! is what [`crate::object::ObjectDocumentStore::repair`] does.
//!
//! [Tantivy]: tantivy

use std::path::Path;
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
            id_field,
            collection_field,
            body_field,
            metadata_field,
        })
    }

    /// Index (or re-index) a document. Existing entries for the same id are
    /// replaced.
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
        let mut writer = self
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
        writer.commit().map_err(Error::storage)?;
        self.reader.reload().map_err(Error::storage)?;
        Ok(())
    }

    /// Remove a document from the index. Removing an absent id is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    pub fn remove_document(&self, id: Uuid) -> Result<()> {
        let mut writer = self
            .writer
            .write()
            .map_err(|_| Error::storage("index writer lock poisoned"))?;
        writer.delete_term(Term::from_field_text(self.id_field, &id.to_string()));
        writer.commit().map_err(Error::storage)?;
        self.reader.reload().map_err(Error::storage)?;
        Ok(())
    }

    /// Run a full-text query over body and metadata, returning up to `limit`
    /// hits. A `collection` filter, when set, is ANDed into the query.
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

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
