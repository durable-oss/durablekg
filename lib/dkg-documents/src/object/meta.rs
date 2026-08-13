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
//! - **entities**: `<collection>\0<entity>\0<id> → ()`, so the documents in a
//!   collection carrying a given `metadata.entity` value can be listed
//!   without decoding the ones that do not. See [`ENTITY_FIELD`].
//!
//! Each record carries a [`DocStatus`]. A freshly-written record is
//! `Pending` until the body and full-text index are confirmed, then
//! `Indexed`. The status lets [`DocumentStore::repair`] detect writes
//! interrupted by a crash.
//!
//! [`DocumentStore::repair`]: crate::store::DocumentStore::repair
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
    entities: Keyspace,
}

/// Separator between a collection name and an id in the collections index.
/// `0x00` cannot appear in a UTF-8 collection name, so it cleanly bounds the
/// prefix scan.
const SEP: u8 = 0;

/// The one metadata field that gets a secondary index.
///
/// Indexing arbitrary metadata would mean a keyspace per field and a write
/// amplification nobody asked for. This field earns the exception because it
/// is how facts are addressed: every fact bundle lifts its entity URN into
/// `metadata.entity`, and reading one entity's facts is the store's hottest
/// query. Without the index that read is a full scan of the collection —
/// which, at a few million bundles, saturates the reader pool and takes the
/// document-store endpoints down with it.
///
/// A document whose metadata lacks this field, or whose value is not a
/// string, is simply absent from the index; the unindexed scan path still
/// answers correctly for it.
pub const ENTITY_FIELD: &str = "entity";

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
        let entities = db
            .keyspace("entities", Default::default)
            .map_err(Error::storage)?;
        Ok(MetaStore {
            db,
            docs,
            urns,
            collections,
            entities,
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

    /// The entity-index prefix for a (collection, entity) pair — every key
    /// under it is one document carrying that `metadata.entity` value.
    ///
    /// The trailing separator is what makes the prefix exact: without it,
    /// scanning for entity `urn:people:id:1` would also match
    /// `urn:people:id:10`.
    fn entity_prefix(collection: &str, entity: &str) -> Vec<u8> {
        let mut k = Vec::with_capacity(collection.len() + entity.len() + 2);
        k.extend_from_slice(collection.as_bytes());
        k.push(SEP);
        k.extend_from_slice(entity.as_bytes());
        k.push(SEP);
        k
    }

    /// The entity-index key for a (collection, entity, id) triple.
    fn entity_key(collection: &str, entity: &str, id: Uuid) -> Vec<u8> {
        let mut k = Self::entity_prefix(collection, entity);
        k.extend_from_slice(id.as_bytes());
        k
    }

    /// The indexable `metadata.entity` value of a record, if it has one.
    fn entity_of(record: &DocRecord) -> Option<&str> {
        record.metadata.get(ENTITY_FIELD).and_then(|v| v.as_str())
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
            // Prune the old entity entry whenever the pair it was keyed on
            // changed. Leaving a stale key would make the document answer a
            // lookup for an entity it no longer claims.
            if let Some(prev_entity) = Self::entity_of(prev) {
                let moved = prev.collection != record.collection
                    || Self::entity_of(record) != Some(prev_entity);
                if moved {
                    self.entities
                        .remove(Self::entity_key(&prev.collection, prev_entity, id))
                        .map_err(Error::storage)?;
                }
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
        if let Some(entity) = Self::entity_of(record) {
            self.entities
                .insert(Self::entity_key(&record.collection, entity, id), [])
                .map_err(Error::storage)?;
        }

        // No `persist` here: durability is a *group* barrier owned by the
        // writer thread (see [`Self::sync`]). The write itself is already in
        // Fjall's own write-ahead log; batching the fsync across many writes
        // is what lifts bulk-ingest throughput off the per-document fsync
        // ceiling.
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
            // Group-synced; see [`Self::sync`].
        }
        Ok(())
    }

    /// Delete a record and all its index entries.
    ///
    /// Like [`Self::put`], the durability barrier is deferred to a group
    /// [`Self::sync`]. A crash inside the sync window can resurrect a deleted
    /// record; the delete is simply replayed by the caller.
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
        if let Some(entity) = Self::entity_of(record) {
            self.entities
                .remove(Self::entity_key(&record.collection, entity, record.id))
                .map_err(Error::storage)?;
        }
        Ok(())
    }

    /// Flush every buffered write to disk.
    ///
    /// This is the durability barrier that [`Self::put`], [`Self::delete`],
    /// and [`Self::mark_indexed`] deliberately omit. The writer thread calls
    /// it on an interval and once more at shutdown, so an orderly stop loses
    /// nothing and only a hard crash can drop the last unsynced window.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the flush fails.
    pub fn sync(&self) -> Result<()> {
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

    /// Records in `collection` whose `metadata.entity` equals `entity`, up to
    /// `limit`.
    ///
    /// The indexed counterpart to filtering [`Self::list_collection`] by hand:
    /// this touches only the matching documents, so the cost scales with the
    /// number of facts about one entity rather than with the size of the
    /// collection. That difference is the whole point of the index.
    ///
    /// Returns an empty vector when the entity has no documents — including
    /// the case where the index has not been backfilled yet, which is why
    /// callers use [`Self::entity_index_ready`] to decide whether an empty
    /// result can be trusted.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] / [`Error::Serialization`] on failure.
    pub fn list_entity(&self, collection: &str, entity: &str, limit: usize) -> Result<Vec<DocRecord>> {
        let mut out = Vec::new();
        for kv in self
            .entities
            .prefix(Self::entity_prefix(collection, entity))
        {
            if out.len() >= limit {
                break;
            }
            let (key, _) = kv.into_inner().map_err(Error::storage)?;
            let id_bytes: [u8; 16] = key[key.len() - 16..]
                .try_into()
                .map_err(|_| Error::storage("corrupt entity index key"))?;
            // An index entry whose record is gone is a torn write, not a
            // failure: skip it rather than failing the read. The backfill
            // pass rebuilds the index from the records themselves.
            match self.get(Uuid::from_bytes(id_bytes)) {
                Ok(record) => out.push(record),
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Whether the entity index holds any entry at all for `collection`.
    ///
    /// A store written before the index existed has records but no index
    /// entries, so an indexed lookup would report "no facts" for an entity
    /// that has plenty — and, on the sync path, an empty overview row would
    /// then replace a real one. Callers check this first and fall back to the
    /// unindexed scan when it is `false`, so an un-backfilled store stays
    /// correct (merely slow) instead of silently losing data.
    ///
    /// Scoped per collection because the backfill runs per collection, and
    /// because one collection being indexed says nothing about another.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    pub fn entity_index_ready(&self, collection: &str) -> Result<bool> {
        let mut prefix = collection.as_bytes().to_vec();
        prefix.push(SEP);
        Ok(self.entities.prefix(prefix).next().is_some())
    }

    /// Rebuild the entity index for `collection` from its records, returning
    /// how many entries were written.
    ///
    /// Idempotent: re-inserting an existing key is a no-op, so a partially
    /// completed backfill is resumed simply by running it again. This is the
    /// one place that pays a full scan of the collection deliberately.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] / [`Error::Serialization`] on failure.
    pub fn backfill_entity_index(&self, collection: &str) -> Result<usize> {
        let mut indexed = 0;
        for record in self.list_collection(collection, usize::MAX)? {
            let Some(entity) = Self::entity_of(&record) else {
                continue;
            };
            self.entities
                .insert(Self::entity_key(&record.collection, entity, record.id), [])
                .map_err(Error::storage)?;
            indexed += 1;
        }
        Ok(indexed)
    }

    /// Remove every entity-index entry for `collection`, returning how many
    /// were dropped.
    ///
    /// This exists to reproduce a pre-index store — one written before the
    /// entity keyspace existed — so the read-path fallback and the backfill
    /// can be tested against it. Nothing in the store calls it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    #[doc(hidden)]
    pub fn clear_entity_index(&self, collection: &str) -> Result<usize> {
        let mut prefix = collection.as_bytes().to_vec();
        prefix.push(SEP);
        let mut keys = Vec::new();
        for kv in self.entities.prefix(prefix) {
            let (key, _) = kv.into_inner().map_err(Error::storage)?;
            keys.push(key);
        }
        let dropped = keys.len();
        for key in keys {
            self.entities.remove(key).map_err(Error::storage)?;
        }
        Ok(dropped)
    }

    /// Every collection name that has at least one record, derived from the
    /// collections index.
    ///
    /// Used by the backfill to find what needs indexing without a caller
    /// having to enumerate collections it may not know about.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    pub fn collections(&self) -> Result<Vec<String>> {
        let mut seen = std::collections::BTreeSet::new();
        for kv in self.collections.iter() {
            let (key, _) = kv.into_inner().map_err(Error::storage)?;
            // The key is `<collection>\0<16-byte id>`; everything before the
            // final separator is the name.
            let Some(cut) = key.len().checked_sub(17) else {
                continue;
            };
            if let Ok(name) = std::str::from_utf8(&key[..cut]) {
                seen.insert(name.to_string());
            }
        }
        Ok(seen.into_iter().collect())
    }

    /// Iterate every record in the store (used by repair and full listing).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] / [`Error::Serialization`] on failure.
    /// Every URN currently bound to a document, with a count.
    ///
    /// Reads the `urns` keyspace's keys directly rather than decoding the
    /// document records that reference them: the keyspace *is* the URN index,
    /// so this is a key-only scan with no JSON parsing and no blob reads.
    ///
    /// Used to populate the write path's Bloom filter at open, and to rebuild
    /// it once deleted URNs have accumulated. A filter built from anything
    /// less than the complete set would report a live URN absent, which is
    /// the one error the filter must never make.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on backend failure, or if a key is not
    /// valid UTF-8 — URNs are written as their string form, so that would
    /// mean a corrupt index.
    pub fn all_urns(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for kv in self.urns.iter() {
            let (key, _) = kv.into_inner().map_err(Error::storage)?;
            let urn = std::str::from_utf8(&key)
                .map_err(|_| Error::storage("non-UTF-8 key in the urn index"))?;
            out.push(urn.to_string());
        }
        Ok(out)
    }

    pub fn all(&self) -> Result<Vec<DocRecord>> {
        let mut out = Vec::new();
        for kv in self.docs.iter() {
            let (_, value) = kv.into_inner().map_err(Error::storage)?;
            out.push(serde_json::from_slice(&value)?);
        }
        Ok(out)
    }
}

impl crate::object::worker::GroupCommit for MetaStore {
    fn flush(&self) -> Result<()> {
        self.sync()
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
