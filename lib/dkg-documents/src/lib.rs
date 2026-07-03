//! # dkg-documents — DurableKG feature A
//!
//! A searchable store of semistructured documents with rich metadata. A
//! [`Document`] is an opaque JSON `body` plus a queryable JSON `metadata`
//! object, grouped into a named `collection`, and optionally reachable by
//! one or more caller-assigned [`Urn`] search ids.
//!
//! The default store ([`ObjectDocumentStore`]) is a composite of three
//! layers: OpenDAL blob storage for bodies, an embedded Fjall key/value
//! store for metadata and the URN index (the source of truth), and a
//! Tantivy full-text index over body and metadata text. Writes are
//! Fjall-first and a [`DocumentStore::repair`] pass reconciles any write
//! interrupted by a crash. See [`object`] for the design.
//!
//! [`DocumentStore`] abstracts over backends; [`open`] picks one by URL
//! scheme via the [`StoreBackend`] registry (the composite store registers
//! the `dkg` scheme).
//!
//! ```no_run
//! use dkg_documents::{open, NewDocument, Query, Urn};
//! use serde_json::json;
//!
//! # async fn run() -> dkg_core::Result<()> {
//! let store = open("dkg://./data").await?;
//!
//! let doc = store
//!     .create(NewDocument {
//!         collection: "articles".into(),
//!         search_ids: vec![Urn::new("article", "hello")?],
//!         body: json!({ "title": "Hello, embedded search" }),
//!         metadata: json!({ "lang": "en" }),
//!     })
//!     .await?;
//!
//! // Fetch by the caller-assigned URN instead of the uuid.
//! let same = store.get_by_urn(&Urn::new("article", "hello")?).await?;
//! assert_eq!(same.id, doc.id);
//!
//! // Full-text search over body + metadata.
//! let hits = store.search(Query::full_text("embedded search")).await?;
//! assert!(hits.iter().any(|d| d.id == doc.id));
//! # Ok(())
//! # }
//! ```

pub mod model;
pub mod object;
pub mod store;

pub use model::{Document, NewDocument, Query, Urn};
pub use object::worker::PoolConfig;
pub use object::ObjectDocumentStore;
pub use store::{open, DocumentStore, StoreBackend, StoreFuture};

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
