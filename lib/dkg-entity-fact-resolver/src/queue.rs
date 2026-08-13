//! Background sync: keeping overview rows current without the client asking.
//!
//! Recording a fact and syncing the entity it is about are two halves of one
//! operation, but making a caller do both means every client re-implements the
//! same bookkeeping — remember which entities were touched, batch them, retry
//! the failures. [`SyncQueue`] moves that into the server.
//!
//! # Shape
//!
//! [`SyncQueue::mark_dirty`] records that an entity's facts changed. A
//! background task ([`SyncQueue::run`]) wakes on an interval, drains the dirty
//! set, and syncs everything in it through
//! [`sync_entities`](crate::sync::sync_entities) — which batches the writes by
//! table.
//!
//! The dirty set is a **set**, not a queue: an entity written fifty times
//! between two ticks syncs once. That is a second layer of batching on top of
//! the per-table `INSERT` batching, and it is what makes a bursty import cheap.
//!
//! # Durability
//!
//! The set is in memory. A crash loses pending syncs: the *facts* are durable
//! (they are documents), but the overview rows for entities that had not yet
//! synced will be stale until something marks them dirty again. This is a
//! deliberate trade — the same one the document store makes on its write path
//! — and it suits an ingestion pipeline whose upstream can replay. A caller
//! that needs an overview row *now* should call
//! [`sync_entity`](crate::sync::sync_entity) directly rather than wait for the
//! background pass.
//!
//! # Consistency
//!
//! Overview rows become eventually consistent with facts, lagging by at most
//! one interval plus the sync itself. A client that records a fact and
//! immediately queries the table may see the previous row.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dkg_facts::{FactStore, Urn};
use dkg_graph::{GraphStore, Schema};

use crate::sync::sync_entities;

/// How often the background task drains the dirty set, when not overridden.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(2);

/// Largest number of entities synced in one pass.
///
/// A drain that found ten thousand dirty entities would build one enormous
/// `INSERT`; chunking keeps any single statement a reasonable size and lets
/// the first rows land while the rest are still going. Leftovers stay dirty
/// and are picked up by the same pass, not the next tick.
pub const MAX_BATCH: usize = 500;

/// A set of entities whose overview rows are out of date.
///
/// Cheap to clone: clones share one dirty set, so a handler can mark entities
/// dirty while the background task drains.
/// Keyed by the URN's string form: [`Urn`] is not `Ord`, and the string is
/// exactly the identity the fact store indexes on anyway.
#[derive(Clone, Default)]
pub struct SyncQueue {
    dirty: Arc<Mutex<BTreeMap<String, Urn>>>,
}

impl SyncQueue {
    /// A new, empty queue.
    pub fn new() -> Self {
        SyncQueue::default()
    }

    /// Note that `entity`'s facts changed and its overview row needs
    /// rebuilding. Marking an already-dirty entity is a no-op, which is what
    /// collapses a burst of writes into a single sync.
    pub fn mark_dirty(&self, entity: Urn) {
        self.lock().insert(entity.to_string(), entity);
    }

    /// Mark several entities at once.
    pub fn mark_all_dirty(&self, entities: impl IntoIterator<Item = Urn>) {
        let mut dirty = self.lock();
        for entity in entities {
            dirty.insert(entity.to_string(), entity);
        }
    }

    /// How many entities are waiting to sync.
    pub fn pending(&self) -> usize {
        self.lock().len()
    }

    /// Take up to `limit` entities out of the dirty set.
    ///
    /// Taken entities are no longer dirty. If a sync then fails, the caller is
    /// responsible for marking them dirty again (see [`Self::sync_once`]).
    fn take(&self, limit: usize) -> Vec<Urn> {
        let mut dirty = self.lock();
        let keys: Vec<String> = dirty.keys().take(limit).cloned().collect();
        keys.into_iter()
            .filter_map(|key| dirty.remove(&key))
            .collect()
    }

    /// A poisoned lock means a panic while holding it. The set is a plain
    /// collection with no invariant to violate, so recovering is safe and
    /// strictly better than taking the whole background task down.
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Urn>> {
        self.dirty.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Drain and sync one batch, returning how many entities synced
    /// successfully and how many failed.
    ///
    /// Entities whose sync failed are marked dirty again so the next pass
    /// retries them. A permanently bad entity (an unknown table, say) will
    /// therefore be retried on every pass; it costs one slot in the batch and
    /// keeps a transient failure from silently dropping an update.
    pub async fn sync_once(
        &self,
        facts: &dyn FactStore,
        graph: &dyn GraphStore,
        schema: &Schema,
    ) -> SyncPass {
        let batch = self.take(MAX_BATCH);
        if batch.is_empty() {
            return SyncPass::default();
        }

        let results = sync_entities(facts, graph, schema, &batch).await;

        let mut pass = SyncPass::default();
        let mut retry = Vec::new();
        for (entity, result) in batch.into_iter().zip(results) {
            match result {
                Ok(_) => pass.synced += 1,
                Err(e) => {
                    pass.failed += 1;
                    pass.last_error = Some(e.to_string());
                    retry.push(entity);
                }
            }
        }
        self.mark_all_dirty(retry);
        pass
    }

    /// Run the background sync loop until `shutdown` resolves.
    ///
    /// Each tick drains up to [`MAX_BATCH`] entities. When a drain fills the
    /// batch there is more waiting, so the next pass runs immediately instead
    /// of sleeping — a backlog clears at full speed rather than
    /// [`MAX_BATCH`] per interval.
    ///
    /// On shutdown it makes one final pass, so an orderly stop syncs what it
    /// can rather than discarding the set.
    pub async fn run(
        self,
        facts: Arc<dyn FactStore>,
        graph: Arc<dyn GraphStore>,
        schema: Arc<Schema>,
        interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            let had_full_batch = self.pending() >= MAX_BATCH;
            if !had_full_batch {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = shutdown.changed() => break,
                }
            }

            let pass = self
                .sync_once(facts.as_ref(), graph.as_ref(), schema.as_ref())
                .await;
            if pass.failed > 0 {
                tracing::warn!(
                    synced = pass.synced,
                    failed = pass.failed,
                    error = pass.last_error.as_deref().unwrap_or(""),
                    "background sync pass had failures; retrying next pass"
                );
            } else if pass.synced > 0 {
                tracing::debug!(synced = pass.synced, "background sync pass");
            }
        }

        // Final pass on the way out.
        let pass = self
            .sync_once(facts.as_ref(), graph.as_ref(), schema.as_ref())
            .await;
        if pass.synced > 0 || pass.failed > 0 {
            tracing::info!(
                synced = pass.synced,
                failed = pass.failed,
                "final background sync pass at shutdown"
            );
        }
    }
}

/// What one drain-and-sync pass did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SyncPass {
    /// Entities whose overview row was written.
    pub synced: usize,
    /// Entities whose sync failed; they were marked dirty again.
    pub failed: usize,
    /// The last error seen this pass, for logging.
    pub last_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urn(slug: &str) -> Urn {
        Urn::new("businesses", format!("slug:{slug}")).unwrap()
    }

    #[test]
    fn marking_the_same_entity_twice_queues_it_once() {
        let q = SyncQueue::new();
        q.mark_dirty(urn("acme"));
        q.mark_dirty(urn("acme"));
        q.mark_dirty(urn("other"));
        assert_eq!(q.pending(), 2, "a set collapses repeat writes");
    }

    #[test]
    fn taking_removes_from_the_dirty_set() {
        let q = SyncQueue::new();
        q.mark_all_dirty((0..5).map(|i| urn(&format!("e{i}"))));
        let taken = q.take(3);
        assert_eq!(taken.len(), 3);
        assert_eq!(q.pending(), 2);
    }

    #[test]
    fn taking_more_than_present_takes_what_there_is() {
        let q = SyncQueue::new();
        q.mark_dirty(urn("only"));
        assert_eq!(q.take(100).len(), 1);
        assert_eq!(q.pending(), 0);
        assert!(q.take(100).is_empty());
    }

    #[test]
    fn clones_share_one_dirty_set() {
        let a = SyncQueue::new();
        let b = a.clone();
        a.mark_dirty(urn("shared"));
        assert_eq!(b.pending(), 1, "a clone must see the same set");
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
