//! End-to-end tests for the document-backed fact store.
//!
//! Each test opens a real object document store in a fresh temp directory and
//! drives facts through it, exercising the metadata-index access patterns the
//! fact store relies on.

use std::sync::Arc;

use dkg_documents::{DocumentStore, ObjectDocumentStore, Query, Urn};
use dkg_facts::{
    facts_collection, Authority, DocumentFactStore, FactStore, NewBundle, NewFact, SourceType,
};

/// Build a fact store and hand back the underlying document store too, so a
/// test can assert which collection a fact landed in.
fn stores(dir: &tempfile::TempDir) -> (DocumentFactStore, Arc<dyn DocumentStore>) {
    let docs: Arc<dyn DocumentStore> =
        Arc::new(ObjectDocumentStore::open_filesystem(dir.path()).expect("open store"));
    (DocumentFactStore::new(docs.clone()), docs)
}

fn fact_store(dir: &tempfile::TempDir) -> DocumentFactStore {
    stores(dir).0
}

fn entity() -> Urn {
    Urn::new("businesses", "acme").unwrap()
}

#[tokio::test]
async fn records_and_reads_back_a_fact() {
    let dir = tempfile::tempdir().unwrap();
    let store = fact_store(&dir);

    let stored = store
        .record(NewFact::manual(entity(), "phone", "603-555-1234"))
        .await
        .unwrap();

    assert_eq!(stored.dimension, "phone");
    assert_eq!(stored.value, serde_json::json!("603-555-1234"));
    assert_eq!(stored.authority.confidence, 1.0);

    // Fetch the same fact straight back by its id.
    let by_id = store.get(stored.id).await.unwrap();
    assert_eq!(by_id, stored);
}

#[tokio::test]
async fn facts_for_returns_every_dimension() {
    let dir = tempfile::tempdir().unwrap();
    let store = fact_store(&dir);

    store
        .record(NewFact::manual(entity(), "phone", "603-555-1234"))
        .await
        .unwrap();
    store
        .record(NewFact::manual(entity(), "industry", "printing"))
        .await
        .unwrap();
    // A second, competing claim on `phone` — both come back; resolution is
    // someone else's job.
    store
        .record(NewFact {
            authority: Authority {
                source_type: SourceType::WebSearch,
                source_url: Some("https://example.com".into()),
                model: Some("grok".into()),
                confidence: 0.6,
            },
            ..NewFact::manual(entity(), "phone", "603-555-9999")
        })
        .await
        .unwrap();

    let all = store.facts_for(&entity()).await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn a_bundle_stores_one_document_but_reads_back_as_many_facts() {
    let dir = tempfile::tempdir().unwrap();
    let (store, docs) = stores(&dir);

    let mut dimensions = serde_json::Map::new();
    dimensions.insert("name".into(), serde_json::json!("Acme Co"));
    dimensions.insert("phone".into(), serde_json::json!("603-555-1234"));
    dimensions.insert("city".into(), serde_json::json!("Manchester"));

    let recorded = store
        .record_bundle(NewBundle {
            entity: entity(),
            aliases: Vec::new(),
            authority: Authority {
                source_type: SourceType::WebsiteScrape,
                source_url: Some("https://acme.example".into()),
                model: None,
                confidence: 0.9,
            },
            observed_at: None,
            dimensions,
        })
        .await
        .unwrap();

    // One document on disk...
    let stored_docs = docs
        .search(Query::in_collection(facts_collection("businesses")))
        .await
        .unwrap();
    assert_eq!(stored_docs.len(), 1, "the bundle is a single document");

    // ...but three facts, one per dimension, all sharing the bundle's
    // authority and id (they are the same physical document).
    assert_eq!(recorded.len(), 3);
    assert!(recorded.iter().all(|f| f.authority.confidence == 0.9));
    assert!(recorded.iter().all(|f| f.id == recorded[0].id));

    // facts_for explodes the stored bundle back into the same three facts.
    let all = store.facts_for(&entity()).await.unwrap();
    assert_eq!(all.len(), 3);
    let phone = store.facts_on(&entity(), "phone").await.unwrap();
    assert_eq!(phone.len(), 1);
    assert_eq!(phone[0].value, serde_json::json!("603-555-1234"));
}

#[tokio::test]
async fn facts_on_filters_to_one_dimension() {
    let dir = tempfile::tempdir().unwrap();
    let store = fact_store(&dir);

    store
        .record(NewFact::manual(entity(), "phone", "603-555-1234"))
        .await
        .unwrap();
    store
        .record(NewFact::manual(entity(), "industry", "printing"))
        .await
        .unwrap();

    let phones = store.facts_on(&entity(), "phone").await.unwrap();
    assert_eq!(phones.len(), 1);
    assert_eq!(phones[0].dimension, "phone");
}

#[tokio::test]
async fn facts_are_scoped_to_their_entity() {
    let dir = tempfile::tempdir().unwrap();
    let store = fact_store(&dir);

    let other = Urn::new("businesses", "globex").unwrap();
    store
        .record(NewFact::manual(entity(), "phone", "603-555-1234"))
        .await
        .unwrap();
    store
        .record(NewFact::manual(other.clone(), "phone", "212-555-0000"))
        .await
        .unwrap();

    assert_eq!(store.facts_for(&entity()).await.unwrap().len(), 1);
    assert_eq!(store.facts_for(&other).await.unwrap().len(), 1);
}

#[tokio::test]
async fn facts_land_in_a_per_table_collection() {
    let dir = tempfile::tempdir().unwrap();
    let (store, docs) = stores(&dir);

    // Two entities in different tables.
    let business = Urn::new("businesses", "acme").unwrap();
    let person = Urn::new("people", "jane").unwrap();
    store
        .record(NewFact::manual(business, "phone", "603-555-1234"))
        .await
        .unwrap();
    store
        .record(NewFact::manual(person, "email", "jane@example.com"))
        .await
        .unwrap();

    // Each fact is stored in its own table's collection, not a shared one.
    let in_businesses = docs
        .search(Query::in_collection(facts_collection("businesses")))
        .await
        .unwrap();
    let in_people = docs
        .search(Query::in_collection(facts_collection("people")))
        .await
        .unwrap();

    assert_eq!(in_businesses.len(), 1);
    assert_eq!(in_people.len(), 1);
    assert_eq!(in_businesses[0].collection, "facts:businesses");
    assert_eq!(in_people[0].collection, "facts:people");
}
