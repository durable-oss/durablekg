//! Metadata and URN-index storage, via [Fjall].
//!
//! Fjall is the *source of truth* for what documents exist. It holds a
//! [`DocRecord`] per document (everything except the body blob), plus two
//! secondary indexes:
//!
//! - **urns**: `urn → id`, so a document is reachable by any caller-assigned
//!   [`Urn`]. Prefix-scanning `urn:<namespace>:` lists a namespace.
//! - **collections**: `<collection>\0<id> → ()`, so documents in a
//!   collection can be listed with a prefix scan.
//!
//! Each record carries a [`DocStatus`]. A freshly-written record is
//! `Pending` until the body and full-text index are confirmed, then
//! `Indexed`. The status lets [`crate::object::ObjectDocumentStore::repair`]
//! detect writes interrupted by a crash.
//!
//! [Fjall]: fjall

use chrono::{DateTime, Utc};
use dkg_core::{Error, Result};
use fjall::{Database, Keyspace, PersistMode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::Urn;

/// Where a document is in its write lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DocStatus {
    /// The record exists but the body and/or full-text index may not be in
    /// sync yet. A repair pass re-drives these to `Indexed`.
    Pending,
    /// Body written and full-text index updated; the document is complete.
    Indexed,
}

/// The persisted form of a document, minus its body blob.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocRecord {
    /// Store-assigned id.
    pub id: Uuid,
    /// Logical grouping.
    pub collection: String,
    /// Caller-assigned search ids.
    pub search_ids: Vec<Urn>,
    /// Queryable annotations.
    pub metadata: serde_json::Value,
    /// First-stored timestamp.
    pub created_at: DateTime<Utc>,
    /// Last-replaced timestamp.
    pub updated_at: DateTime<Utc>,
    /// Write-lifecycle status.
    pub status: DocStatus,
}

/// The Fjall-backed metadata store and its secondary indexes.
#[derive(Clone)]
pub struct MetaStore {
    db: Database,
    docs: Keyspace,
    urns: Keyspace,
    collections: Keyspace,
}

/// Separator between a collection name and an id in the collections index.
/// `0x00` cannot appear in a UTF-8 collection name, so it cleanly bounds the
/// prefix scan.
const SEP: u8 = 0;

impl MetaStore {
    /// Open (or create) the metadata store at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the database or its keyspaces cannot be
    /// opened.
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::builder(path).open().map_err(Error::storage)?;
        let docs = db
            .keyspace("docs", Default::default)
            .map_err(Error::storage)?;
        let urns = db
            .keyspace("urns", Default::default)
            .map_err(Error::storage)?;
        let collections = db
            .keyspace("collections", Default::default)
            .map_err(Error::storage)?;
        Ok(MetaStore {
            db,
            docs,
            urns,
            collections,
        })
    }

    /// The collections-index key for a (collection, id) pair.
    fn coll_key(collection: &str, id: Uuid) -> Vec<u8> {
        let mut k = Vec::with_capacity(collection.len() + 1 + 16);
        k.extend_from_slice(collection.as_bytes());
        k.push(SEP);
        k.extend_from_slice(id.as_bytes());
        k
    }

    /// Look up a record by id.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if absent, or [`Error::Storage`] /
    /// [`Error::Serialization`] on failure.
    pub fn get(&self, id: Uuid) -> Result<DocRecord> {
        let raw = self
            .docs
            .get(id.as_bytes())
            .map_err(Error::storage)?
            .ok_or_else(|| Error::NotFound(format!("document {id}")))?;
        Ok(serde_json::from_slice(&raw)?)
    }

    /// Resolve a URN to a document id.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if no document is registered under the
    /// URN, or [`Error::Storage`] on failure.
    pub fn resolve_urn(&self, urn: &Urn) -> Result<Uuid> {
        let raw = self
            .urns
            .get(urn.to_string().as_bytes())
            .map_err(Error::storage)?
            .ok_or_else(|| Error::NotFound(format!("urn {urn}")))?;
        let bytes: [u8; 16] = raw
            .as_ref()
            .try_into()
            .map_err(|_| Error::storage("corrupt urn index entry"))?;
        Ok(Uuid::from_bytes(bytes))
    }

    /// Reject any URN in `search_ids` already bound to a different document.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] on a conflict, or [`Error::Storage`] on
    /// failure.
    pub fn check_urns_free(&self, search_ids: &[Urn], owner: Uuid) -> Result<()> {
        for urn in search_ids {
            if let Some(raw) = self
                .urns
                .get(urn.to_string().as_bytes())
                .map_err(Error::storage)?
            {
                let existing: [u8; 16] = raw
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::storage("corrupt urn index entry"))?;
                if Uuid::from_bytes(existing) != owner {
                    return Err(Error::Invalid(format!("urn already in use: {urn}")));
                }
            }
        }
        Ok(())
    }

    /// Write a record and (re)point its indexes, then flush to disk.
    ///
    /// `previous` is the prior record being replaced, if any; its indexes
    /// are pruned for entries the new record drops (removed URNs, a changed
    /// collection).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] / [`Error::Serialization`] on failure.
    pub fn put(&self, record: &DocRecord, previous: Option<&DocRecord>) -> Result<()> {
        let id = record.id;

        if let Some(prev) = previous {
            for urn in &prev.search_ids {
                if !record.search_ids.contains(urn) {
                    self.urns
                        .remove(urn.to_string().as_bytes())
                        .map_err(Error::storage)?;
                }
            }
            if prev.collection != record.collection {
                self.collections
                    .remove(Self::coll_key(&prev.collection, id))
                    .map_err(Error::storage)?;
            }
        }

        let value = serde_json::to_vec(record)?;
        self.docs
            .insert(id.as_bytes(), value)
            .map_err(Error::storage)?;
        for urn in &record.search_ids {
            self.urns
                .insert(urn.to_string().as_bytes(), id.as_bytes())
                .map_err(Error::storage)?;
        }
        self.collections
            .insert(Self::coll_key(&record.collection, id), [])
            .map_err(Error::storage)?;

        self.db
            .persist(PersistMode::SyncAll)
            .map_err(Error::storage)?;
        Ok(())
    }

    /// Mark a record `Indexed`. No-op if the record is gone.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] / [`Error::Serialization`] on failure.
    pub fn mark_indexed(&self, id: Uuid) -> Result<()> {
        if let Some(raw) = self.docs.get(id.as_bytes()).map_err(Error::storage)? {
            let mut record: DocRecord = serde_json::from_slice(&raw)?;
            record.status = DocStatus::Indexed;
            self.docs
                .insert(id.as_bytes(), serde_json::to_vec(&record)?)
                .map_err(Error::storage)?;
            self.db
                .persist(PersistMode::SyncAll)
                .map_err(Error::storage)?;
        }
        Ok(())
    }

    /// Delete a record and all its index entries, then flush.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    pub fn delete(&self, record: &DocRecord) -> Result<()> {
        self.docs
            .remove(record.id.as_bytes())
            .map_err(Error::storage)?;
        for urn in &record.search_ids {
            self.urns
                .remove(urn.to_string().as_bytes())
                .map_err(Error::storage)?;
        }
        self.collections
            .remove(Self::coll_key(&record.collection, record.id))
            .map_err(Error::storage)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(Error::storage)?;
        Ok(())
    }

    /// List records in a collection, newest entries first is *not*
    /// guaranteed — order is by id. Caller applies any final ordering.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] / [`Error::Serialization`] on failure.
    pub fn list_collection(&self, collection: &str, limit: usize) -> Result<Vec<DocRecord>> {
        let mut prefix = collection.as_bytes().to_vec();
        prefix.push(SEP);
        let mut out = Vec::new();
        for kv in self.collections.prefix(prefix) {
            if out.len() >= limit {
                break;
            }
            let (key, _) = kv.into_inner().map_err(Error::storage)?;
            // The id is the trailing 16 bytes after the separator.
            let id_bytes: [u8; 16] = key[key.len() - 16..]
                .try_into()
                .map_err(|_| Error::storage("corrupt collection index key"))?;
            out.push(self.get(Uuid::from_bytes(id_bytes))?);
        }
        Ok(out)
    }

    /// Iterate every record in the store (used by repair and full listing).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] / [`Error::Serialization`] on failure.
    pub fn all(&self) -> Result<Vec<DocRecord>> {
        let mut out = Vec::new();
        for kv in self.docs.iter() {
            let (_, value) = kv.into_inner().map_err(Error::storage)?;
            out.push(serde_json::from_slice(&value)?);
        }
        Ok(out)
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
