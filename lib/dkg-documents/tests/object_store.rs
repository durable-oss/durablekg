//! End-to-end tests for the composite object document store.
//!
//! Each test opens a real store in a fresh temp directory, exercising the
//! OpenDAL + Fjall + Tantivy layers together.

use dkg_documents::{DocumentStore, NewDocument, ObjectDocumentStore, Query, Urn};
use serde_json::json;

fn store(dir: &tempfile::TempDir) -> ObjectDocumentStore {
    ObjectDocumentStore::open_filesystem(dir.path()).expect("open store")
}

#[tokio::test]
async fn create_get_and_full_text_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    let created = store
        .create(NewDocument {
            collection: "articles".into(),
            search_ids: vec![Urn::new("article", "hello").unwrap()],
            body: json!({ "title": "Embedded search in Rust", "tags": ["tantivy"] }),
            metadata: json!({ "lang": "en" }),
        })
        .await
        .unwrap();

    // Fetch by uuid.
    let by_id = store.get(created.id).await.unwrap();
    assert_eq!(
        by_id.body,
        json!({ "title": "Embedded search in Rust", "tags": ["tantivy"] })
    );

    // Fetch by URN search id.
    let by_urn = store
        .get_by_urn(&Urn::new("article", "hello").unwrap())
        .await
        .unwrap();
    assert_eq!(by_urn.id, created.id);

    // Full-text search hits on body content. The index is group-committed,
    // so flush first to make the write visible to search.
    store.flush().await.unwrap();
    let hits = store.search(Query::full_text("tantivy")).await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, created.id);

    // Metadata-containment listing.
    let listed = store
        .search(Query {
            collection: Some("articles".into()),
            metadata_contains: json!({ "lang": "en" }),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
}

#[tokio::test]
async fn duplicate_urn_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let urn = Urn::new("doc", "x").unwrap();

    store
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![urn.clone()],
            body: json!({}),
            metadata: json!({}),
        })
        .await
        .unwrap();

    let err = store
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![urn],
            body: json!({}),
            metadata: json!({}),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, dkg_core::Error::Invalid(_)), "got {err:?}");
}

/// The write path consults a Bloom filter before the authoritative URN check,
/// and that filter is rebuilt from disk at open. If the rebuild missed
/// anything, the filter would report a live URN absent — a false negative —
/// and the duplicate would be accepted, silently rebinding a URN that already
/// belongs to another document.
///
/// The same-process test above cannot catch that: its filter is warm from the
/// first write. This one reopens the store, so the only thing standing
/// between the duplicate and the store is the rebuilt filter.
#[tokio::test]
async fn duplicate_urn_is_rejected_after_reopening_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let urn = Urn::new("doc", "persisted").unwrap();

    {
        let store = store(&dir);
        store
            .create(NewDocument {
                collection: "c".into(),
                search_ids: vec![urn.clone()],
                body: json!({}),
                metadata: json!({}),
            })
            .await
            .unwrap();
        store.shutdown().await.unwrap();
    }

    // Fresh instance: the filter has just been rebuilt from the urn index.
    let reopened = store(&dir);
    let err = reopened
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![urn.clone()],
            body: json!({}),
            metadata: json!({}),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, dkg_core::Error::Invalid(_)),
        "a reopened store must still reject a duplicate URN; got {err:?}"
    );

    // And the original document is still reachable under that URN — the
    // rejected write must not have rebound or damaged it.
    assert!(reopened.get_by_urn(&urn).await.is_ok());
}

/// A URN freed by a replace must be reusable.
///
/// Deletes are deliberately not applied to the Bloom filter (clearing bits
/// could mask another URN), so a freed URN stays "possibly present" and falls
/// through to the authoritative check. That check must then find it absent
/// and allow the write — the filter must never turn a wasted lookup into a
/// spurious rejection.
#[tokio::test]
async fn a_urn_freed_by_a_replace_can_be_reused() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let freed = Urn::new("doc", "freed").unwrap();
    let kept = Urn::new("doc", "kept").unwrap();

    let created = store
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![freed.clone(), kept.clone()],
            body: json!({}),
            metadata: json!({}),
        })
        .await
        .unwrap();

    // Replace, dropping `freed` from the document's search ids.
    store
        .replace(
            created.id,
            NewDocument {
                collection: "c".into(),
                search_ids: vec![kept],
                body: json!({}),
                metadata: json!({}),
            },
        )
        .await
        .unwrap();

    // `freed` is no longer bound, so a different document may claim it —
    // even though the filter still believes it might be present.
    store
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![freed],
            body: json!({}),
            metadata: json!({}),
        })
        .await
        .expect("a URN released by a replace must be claimable again");
}

#[tokio::test]
async fn replace_updates_body_and_reindexes() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    let created = store
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![],
            body: json!({ "word": "aardvark" }),
            metadata: json!({}),
        })
        .await
        .unwrap();

    store
        .replace(
            created.id,
            NewDocument {
                collection: "c".into(),
                search_ids: vec![],
                body: json!({ "word": "zebra" }),
                metadata: json!({}),
            },
        )
        .await
        .unwrap();

    // Old text no longer matches; new text does. Flush so the re-index is
    // visible to search.
    store.flush().await.unwrap();
    assert!(store
        .search(Query::full_text("aardvark"))
        .await
        .unwrap()
        .is_empty());
    let hits = store.search(Query::full_text("zebra")).await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, created.id);
}

#[tokio::test]
async fn delete_removes_from_all_layers() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let urn = Urn::new("doc", "gone").unwrap();

    let created = store
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![urn.clone()],
            body: json!({ "word": "ephemeral" }),
            metadata: json!({}),
        })
        .await
        .unwrap();

    // Commit the create before deleting, so the search below proves the
    // delete removed an *indexed* document rather than passing because the
    // create was never committed in the first place.
    store.flush().await.unwrap();
    assert_eq!(
        store
            .search(Query::full_text("ephemeral"))
            .await
            .unwrap()
            .len(),
        1,
        "document should be searchable before the delete"
    );

    store.delete(created.id).await.unwrap();

    assert!(matches!(
        store.get(created.id).await.unwrap_err(),
        dkg_core::Error::NotFound(_)
    ));
    assert!(matches!(
        store.get_by_urn(&urn).await.unwrap_err(),
        dkg_core::Error::NotFound(_)
    ));
    store.flush().await.unwrap();
    assert!(store
        .search(Query::full_text("ephemeral"))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn shutdown_then_reopen_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let id = {
        let store = store(&dir);
        let id = store
            .create(NewDocument {
                collection: "c".into(),
                search_ids: vec![Urn::new("doc", "drained").unwrap()],
                body: json!({ "word": "flushed" }),
                metadata: json!({}),
            })
            .await
            .unwrap()
            .id;
        // Graceful drain: workers finish and join before we drop the store.
        store.shutdown().await.unwrap();
        id
    };

    let reopened = store(&dir);
    assert_eq!(
        reopened.get(id).await.unwrap().body,
        json!({ "word": "flushed" })
    );
    let hits = reopened.search(Query::full_text("flushed")).await.unwrap();
    assert_eq!(hits.len(), 1);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn data_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let id = {
        let store = store(&dir);
        let id = store
            .create(NewDocument {
                collection: "c".into(),
                search_ids: vec![],
                body: json!({ "word": "persistent" }),
                metadata: json!({}),
            })
            .await
            .unwrap()
            .id;
        // Shut down rather than just dropping. `Drop` closes the queues but
        // deliberately does not join the worker threads, so the Fjall file
        // lock is released by a thread that is still winding down — reopening
        // straight after a drop races that and intermittently fails with
        // `FjallError: Locked` on a loaded machine.
        store.shutdown().await.unwrap();
        id
    };

    // Reopen the same directory in a fresh store instance.
    let reopened = store(&dir);
    let doc = reopened.get(id).await.unwrap();
    assert_eq!(doc.body, json!({ "word": "persistent" }));
    let hits = reopened
        .search(Query::full_text("persistent"))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);

    // A healthy store repairs nothing.
    assert_eq!(reopened.repair().await.unwrap(), 0);
}

/// The group-commit contract: a fresh write is immediately fetchable by id
/// (Fjall is written synchronously) but is not searchable until the Tantivy
/// index is committed. This is the trade that lifts bulk-ingest throughput
/// off the per-document commit ceiling.
#[tokio::test]
async fn writes_are_fetchable_before_they_are_searchable() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    let created = store
        .create(NewDocument {
            collection: "c".into(),
            search_ids: vec![],
            body: json!({ "word": "deferred" }),
            metadata: json!({}),
        })
        .await
        .unwrap();

    // Readable by id right away — that path reads Fjall, not the index.
    assert_eq!(
        store.get(created.id).await.unwrap().body,
        json!({ "word": "deferred" })
    );

    // Not yet in the full-text index: the commit is still staged. The
    // default interval is seconds, so this is not a race in practice.
    assert!(
        store
            .search(Query::full_text("deferred"))
            .await
            .unwrap()
            .is_empty(),
        "document should not be searchable before the index is committed"
    );

    // An explicit flush makes it visible.
    store.flush().await.unwrap();
    let hits = store.search(Query::full_text("deferred")).await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, created.id);
}

/// A batch of writes costs one group commit, not one per document. This is
/// the throughput property the whole design exists for; assert it directly
/// rather than trusting the write path's comments.
#[tokio::test]
async fn a_batch_of_writes_shares_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    for i in 0..50 {
        store
            .create(NewDocument {
                collection: "bulk".into(),
                search_ids: vec![],
                body: json!({ "n": i, "word": "bulky" }),
                metadata: json!({}),
            })
            .await
            .unwrap();
    }

    // Nothing committed yet: 50 documents staged, well under the
    // commit-early threshold and inside the interval.
    assert!(
        store
            .search(Query::full_text("bulky"))
            .await
            .unwrap()
            .is_empty(),
        "a batch under the staging threshold should not have committed yet"
    );

    store.flush().await.unwrap();
    let hits = store
        .search(Query {
            collection: Some("bulk".into()),
            text: Some("bulky".into()),
            metadata_contains: json!({}),
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 50, "one commit should publish the whole batch");
}

/// Write `bundles` documents spread across `entities` distinct entity values,
/// the shape a fact collection has: many bundles, each tagged with the entity
/// it describes.
async fn seed_entities(store: &ObjectDocumentStore, entities: usize, bundles: usize) {
    for i in 0..bundles {
        let entity = format!("urn:people:id:{}", i % entities);
        store
            .create(NewDocument {
                collection: "facts:people".into(),
                search_ids: vec![],
                body: json!({ "entity": entity, "n": i }),
                metadata: json!({ "entity": entity }),
            })
            .await
            .unwrap();
    }
    store.flush().await.unwrap();
}

fn entity_query(entity: &str) -> Query {
    Query {
        collection: Some("facts:people".into()),
        text: None,
        metadata_contains: json!({ "entity": entity }),
        limit: 1000,
    }
}

#[tokio::test]
async fn entity_lookup_returns_only_that_entitys_documents() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    // 10 entities x 5 bundles each.
    seed_entities(&store, 10, 50).await;

    let hits = store.search(entity_query("urn:people:id:3")).await.unwrap();
    assert_eq!(hits.len(), 5, "five bundles were written for this entity");
    for hit in &hits {
        assert_eq!(hit.metadata.get("entity").unwrap(), "urn:people:id:3");
    }

    // An entity with no documents is empty, not an error.
    let none = store
        .search(entity_query("urn:people:id:nosuch"))
        .await
        .unwrap();
    assert!(none.is_empty(), "unknown entity should return nothing");
}

/// The regression this whole index exists for.
///
/// Reading one entity's facts used to list and decode the *entire* collection
/// and filter in memory, so the cost of one lookup grew with the size of the
/// store. At a few million bundles that pinned every reader thread at 100%
/// CPU for minutes at a time and took the document-store endpoints down.
///
/// Here the same lookup is timed against a small collection and one 16x
/// larger, with the per-entity result size held constant. Indexed, the two
/// cost about the same; unindexed, the second is ~16x the first.
#[tokio::test]
async fn entity_lookup_cost_does_not_grow_with_collection_size() {
    async fn time_lookup(entities: usize, bundles: usize) -> std::time::Duration {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        seed_entities(&store, entities, bundles).await;

        // Warm any lazily-built state so the measurement is of the read.
        let _ = store.search(entity_query("urn:people:id:0")).await.unwrap();

        let start = std::time::Instant::now();
        for i in 0..20 {
            let hits = store
                .search(entity_query(&format!("urn:people:id:{}", i % entities)))
                .await
                .unwrap();
            assert_eq!(hits.len(), 4, "per-entity result size is held constant");
        }
        start.elapsed()
    }

    // Same 4 bundles per entity in both; only the collection size differs.
    let small = time_lookup(50, 200).await;
    let large = time_lookup(800, 3200).await;

    // A generous ceiling: the point is to catch linear-in-collection
    // behaviour (~16x), not to pin down a constant on a noisy machine.
    assert!(
        large < small * 6,
        "lookup cost scaled with collection size: {small:?} over 200 docs vs {large:?} over 3200"
    );
}

#[tokio::test]
async fn replacing_a_document_moves_it_between_entities() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    let created = store
        .create(NewDocument {
            collection: "facts:people".into(),
            search_ids: vec![],
            body: json!({ "n": 1 }),
            metadata: json!({ "entity": "urn:people:id:old" }),
        })
        .await
        .unwrap();
    store.flush().await.unwrap();

    store
        .replace(
            created.id,
            NewDocument {
                collection: "facts:people".into(),
                search_ids: vec![],
                body: json!({ "n": 2 }),
                metadata: json!({ "entity": "urn:people:id:new" }),
            },
        )
        .await
        .unwrap();
    store.flush().await.unwrap();

    // The stale index entry must not survive the move, or the document would
    // answer lookups for an entity it no longer claims.
    let old = store
        .search(entity_query("urn:people:id:old"))
        .await
        .unwrap();
    assert!(old.is_empty(), "document still indexed under its old entity");

    let new = store
        .search(entity_query("urn:people:id:new"))
        .await
        .unwrap();
    assert_eq!(new.len(), 1, "document should be found under its new entity");
    assert_eq!(new[0].id, created.id);
}

#[tokio::test]
async fn deleting_a_document_removes_its_entity_entry() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    seed_entities(&store, 2, 4).await;

    let hits = store.search(entity_query("urn:people:id:0")).await.unwrap();
    assert_eq!(hits.len(), 2);

    store.delete(hits[0].id).await.unwrap();
    store.flush().await.unwrap();

    let after = store.search(entity_query("urn:people:id:0")).await.unwrap();
    assert_eq!(after.len(), 1, "deleted document should leave the index");
    assert_ne!(after[0].id, hits[0].id);
}

#[tokio::test]
async fn grouped_search_matches_per_entity_lookups() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    seed_entities(&store, 6, 30).await;

    let values: Vec<String> = (0..6).map(|i| format!("urn:people:id:{i}")).collect();
    let grouped = store
        .search_grouped("facts:people", "entity", &values, 1000)
        .await
        .unwrap();

    assert_eq!(grouped.len(), 6, "every entity has documents");
    for value in &values {
        let single = store.search(entity_query(value)).await.unwrap();
        let mut from_group: Vec<_> = grouped[value].iter().map(|d| d.id).collect();
        let mut from_single: Vec<_> = single.iter().map(|d| d.id).collect();
        from_group.sort();
        from_single.sort();
        assert_eq!(
            from_group, from_single,
            "grouped and single-entity reads disagree for {value}"
        );
    }

    // An entity nobody wrote is absent from the map, not present-and-empty.
    let missing = store
        .search_grouped(
            "facts:people",
            "entity",
            &["urn:people:id:nosuch".to_string()],
            1000,
        )
        .await
        .unwrap();
    assert!(missing.is_empty());
}

/// Documents with no `metadata.entity` are not indexed, but must still be
/// listable and searchable by other metadata — the index is an addition to
/// the scan path, not a replacement for it.
#[tokio::test]
async fn documents_without_an_entity_field_still_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    store
        .create(NewDocument {
            collection: "notes".into(),
            search_ids: vec![],
            body: json!({ "text": "no entity here" }),
            metadata: json!({ "lang": "en" }),
        })
        .await
        .unwrap();
    store.flush().await.unwrap();

    let by_other = store
        .search(Query {
            collection: Some("notes".into()),
            metadata_contains: json!({ "lang": "en" }),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(by_other.len(), 1);

    // And an entity lookup against that collection finds nothing rather than
    // erroring.
    let by_entity = store
        .search(Query {
            collection: Some("notes".into()),
            metadata_contains: json!({ "entity": "urn:people:id:1" }),
            ..Query::default()
        })
        .await
        .unwrap();
    assert!(by_entity.is_empty());
}

/// A store written before the entity index existed has records but no index
/// entries. Reads against it must stay *correct* — an indexed lookup on an
/// empty index would report "this entity has no facts", and on the sync path
/// that resolves to an empty overview row which then replaces the real one.
///
/// So: correct before `migrate`, correct and indexed after.
#[tokio::test]
async fn a_store_without_the_entity_index_reads_correctly_and_backfills() {
    let dir = tempfile::tempdir().unwrap();

    {
        let store = store(&dir);
        seed_entities(&store, 4, 20).await;
        // Simulate the pre-index on-disk state: drop every entity entry,
        // leaving the records and the collections index untouched. This is
        // exactly what a store written by the previous version looks like.
        store.clear_entity_index_for_test("facts:people").await.unwrap();
        store.shutdown().await.unwrap();
    }

    let store = store(&dir);

    // Before the backfill: the scan fallback must still find the facts.
    let before = store.search(entity_query("urn:people:id:2")).await.unwrap();
    assert_eq!(
        before.len(),
        5,
        "an un-backfilled store must fall back to the scan, not report zero facts"
    );

    // The grouped path (batch sync) has the same obligation.
    let values: Vec<String> = (0..4).map(|i| format!("urn:people:id:{i}")).collect();
    let grouped = store
        .search_grouped("facts:people", "entity", &values, 1000)
        .await
        .unwrap();
    assert_eq!(grouped.len(), 4, "grouped read must not lose entities");

    store.migrate().await.unwrap();

    let after = store.search(entity_query("urn:people:id:2")).await.unwrap();
    let mut before_ids: Vec<_> = before.iter().map(|d| d.id).collect();
    let mut after_ids: Vec<_> = after.iter().map(|d| d.id).collect();
    before_ids.sort();
    after_ids.sort();
    assert_eq!(
        before_ids, after_ids,
        "the backfill must not change what a lookup returns"
    );

    // Running it again is a no-op, so a restart does not re-scan.
    store.migrate().await.unwrap();
    let again = store.search(entity_query("urn:people:id:2")).await.unwrap();
    assert_eq!(again.len(), 5, "migrate must be idempotent");
}

/// A filter that mentions `entity` alongside another condition cannot be
/// served by the index alone; it must fall back to the scan and still apply
/// every condition.
#[tokio::test]
async fn compound_entity_filter_still_applies_every_condition() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    for (entity, lang) in [("urn:people:id:1", "en"), ("urn:people:id:1", "fr")] {
        store
            .create(NewDocument {
                collection: "facts:people".into(),
                search_ids: vec![],
                body: json!({}),
                metadata: json!({ "entity": entity, "lang": lang }),
            })
            .await
            .unwrap();
    }
    store.flush().await.unwrap();

    let hits = store
        .search(Query {
            collection: Some("facts:people".into()),
            metadata_contains: json!({ "entity": "urn:people:id:1", "lang": "fr" }),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "the `lang` condition must still be applied");
    assert_eq!(hits[0].metadata.get("lang").unwrap(), "fr");
}
