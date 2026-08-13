//! Tests for the *read* cost of the batched sync path.
//!
//! [`batch_sync`](../batch_sync.rs) covers how many write statements a batch
//! issues; this covers how many reads it issues, which is the other half of
//! the same property. The document store is wrapped in a counter so the tests
//! can assert on call counts directly — the fact store is the real
//! [`DocumentFactStore`] over a real object store, since the point is to
//! exercise the genuine bulk read rather than a fake that reimplements it.
//!
//! The property under test: preparing a batch reads each namespace's fact
//! collection a *fixed* number of times, not once per entity. A regression
//! that reinstated per-entity reads would still pass every correctness test
//! and only show up here.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use dkg_core::Result;
use dkg_documents::{Document, DocumentStore, NewDocument, ObjectDocumentStore, Query, Urn};
use dkg_facts::{DocumentFactStore, FactStore, NewFact};
use uuid::Uuid;

/// A [`DocumentStore`] that counts the searches passing through it.
///
/// `search` and `search_grouped` are counted separately: the per-entity path
/// drives the former once per entity, the bulk path drives the latter once per
/// namespace, so the two counters distinguish which path ran.
struct CountingDocs {
    inner: Arc<dyn DocumentStore>,
    searches: AtomicUsize,
    grouped: AtomicUsize,
}

impl CountingDocs {
    fn new(inner: Arc<dyn DocumentStore>) -> Self {
        CountingDocs {
            inner,
            searches: AtomicUsize::new(0),
            grouped: AtomicUsize::new(0),
        }
    }

    fn searches(&self) -> usize {
        self.searches.load(Ordering::Relaxed)
    }

    fn grouped(&self) -> usize {
        self.grouped.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.searches.store(0, Ordering::Relaxed);
        self.grouped.store(0, Ordering::Relaxed);
    }
}

#[async_trait]
impl DocumentStore for CountingDocs {
    async fn migrate(&self) -> Result<()> {
        self.inner.migrate().await
    }
    async fn create(&self, doc: NewDocument) -> Result<Document> {
        self.inner.create(doc).await
    }
    async fn get(&self, id: Uuid) -> Result<Document> {
        self.inner.get(id).await
    }
    async fn get_by_urn(&self, urn: &Urn) -> Result<Document> {
        self.inner.get_by_urn(urn).await
    }
    async fn replace(&self, id: Uuid, doc: NewDocument) -> Result<Document> {
        self.inner.replace(id, doc).await
    }
    async fn delete(&self, id: Uuid) -> Result<()> {
        self.inner.delete(id).await
    }
    async fn search(&self, query: Query) -> Result<Vec<Document>> {
        self.searches.fetch_add(1, Ordering::Relaxed);
        self.inner.search(query).await
    }
    async fn search_grouped(
        &self,
        collection: &str,
        field: &str,
        values: &[String],
        limit_per_value: usize,
    ) -> Result<std::collections::HashMap<String, Vec<Document>>> {
        self.grouped.fetch_add(1, Ordering::Relaxed);
        self.inner
            .search_grouped(collection, field, values, limit_per_value)
            .await
    }
    async fn repair(&self) -> Result<usize> {
        self.inner.repair().await
    }
}

fn entity(namespace: &str, slug: &str) -> Urn {
    Urn::new(namespace, slug).unwrap()
}

/// Record one fact per entity and hand back the counting store plus the fact
/// store over it, with the counters zeroed after ingestion.
async fn seeded(
    dir: &tempfile::TempDir,
    entities: &[Urn],
) -> (Arc<CountingDocs>, DocumentFactStore) {
    let inner: Arc<dyn DocumentStore> =
        Arc::new(ObjectDocumentStore::open_filesystem(dir.path()).expect("open store"));
    let counting = Arc::new(CountingDocs::new(inner));
    let facts = DocumentFactStore::new(counting.clone() as Arc<dyn DocumentStore>);

    for e in entities {
        facts
            .record(NewFact::manual(e.clone(), "phone", "555-0100"))
            .await
            .unwrap();
    }
    // Only reads after this point are under test.
    counting.reset();
    (counting, facts)
}

#[tokio::test]
async fn a_batch_read_costs_one_scan_regardless_of_entity_count() {
    let dir = tempfile::tempdir().unwrap();
    let entities: Vec<Urn> = (0..50)
        .map(|i| entity("businesses", &format!("acme-{i}")))
        .collect();
    let (docs, facts) = seeded(&dir, &entities).await;

    let got = facts.facts_for_many(&entities).await.unwrap();

    assert_eq!(got.len(), 50, "every entity's facts came back");
    // The whole point: one grouped read for fifty entities, and none of the
    // per-entity searches the old path made.
    assert_eq!(docs.grouped(), 1, "one bulk read for the batch");
    assert_eq!(docs.searches(), 0, "no per-entity search");
}

#[tokio::test]
async fn the_scan_count_does_not_grow_with_the_batch() {
    let dir = tempfile::tempdir().unwrap();
    let entities: Vec<Urn> = (0..200)
        .map(|i| entity("businesses", &format!("acme-{i}")))
        .collect();
    let (docs, facts) = seeded(&dir, &entities).await;

    // Ten entities and two hundred cost the same number of reads. This is the
    // difference between linear and quadratic batch preparation.
    facts.facts_for_many(&entities[..10]).await.unwrap();
    let small = docs.grouped();
    docs.reset();

    facts.facts_for_many(&entities).await.unwrap();
    let large = docs.grouped();

    assert_eq!(small, 1);
    assert_eq!(large, 1, "a 20x larger batch still costs one read");
}

#[tokio::test]
async fn a_batch_reads_each_namespace_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut entities: Vec<Urn> = (0..10)
        .map(|i| entity("businesses", &format!("acme-{i}")))
        .collect();
    entities.extend((0..10).map(|i| entity("people", &format!("jane-{i}"))));
    let (docs, facts) = seeded(&dir, &entities).await;

    let got = facts.facts_for_many(&entities).await.unwrap();

    assert_eq!(got.len(), 20);
    // Facts are stored per namespace, so two namespaces cost two reads — and
    // still not one per entity.
    assert_eq!(docs.grouped(), 2, "one read per namespace");
    assert_eq!(docs.searches(), 0);
}

#[tokio::test]
async fn the_bulk_read_agrees_with_reading_one_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let entities: Vec<Urn> = (0..20)
        .map(|i| entity("businesses", &format!("acme-{i}")))
        .collect();
    let (_docs, facts) = seeded(&dir, &entities).await;

    // Record a second fact on one entity so the two paths have something
    // non-uniform to disagree about.
    facts
        .record(NewFact::manual(entities[3].clone(), "industry", "printing"))
        .await
        .unwrap();

    let bulk = facts.facts_for_many(&entities).await.unwrap();
    for e in &entities {
        let single = facts.facts_for(e).await.unwrap();
        let mut from_bulk: Vec<String> = bulk
            .get(&e.to_string())
            .map(|fs| fs.iter().map(|f| f.dimension.clone()).collect())
            .unwrap_or_default();
        let mut from_single: Vec<String> = single.iter().map(|f| f.dimension.clone()).collect();
        from_bulk.sort();
        from_single.sort();
        assert_eq!(from_bulk, from_single, "paths disagree for {e}");
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
