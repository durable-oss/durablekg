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

    // Full-text search hits on body content.
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

    // Old text no longer matches; new text does.
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

    store.delete(created.id).await.unwrap();

    assert!(matches!(
        store.get(created.id).await.unwrap_err(),
        dkg_core::Error::NotFound(_)
    ));
    assert!(matches!(
        store.get_by_urn(&urn).await.unwrap_err(),
        dkg_core::Error::NotFound(_)
    ));
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
        store
            .create(NewDocument {
                collection: "c".into(),
                search_ids: vec![],
                body: json!({ "word": "persistent" }),
                metadata: json!({}),
            })
            .await
            .unwrap()
            .id
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
