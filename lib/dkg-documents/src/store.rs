//! The storage abstraction and its plugin registry.
//!
//! [`DocumentStore`] is the trait every backend implements. Backends
//! register a constructor with [`inventory`] via [`StoreBackend`] so the
//! server can build a store from a URL scheme without knowing the concrete
//! type. The default backend is the composite object store in
//! [`crate::object`], registered for the `dkg` scheme; additional backends
//! register the same way.

use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use dkg_core::registry::Named;
use dkg_core::Result;
use uuid::Uuid;

use crate::model::{Document, NewDocument, Query, Urn};

/// A searchable store of [`Document`]s.
///
/// The trait is object-safe (via `async_trait`) so callers can hold an
/// `Arc<dyn DocumentStore>` and stay backend-agnostic.
#[async_trait]
pub trait DocumentStore: Send + Sync {
    /// Run any pending schema migrations for this backend.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::Storage`] if the backend rejects the
    /// migration.
    async fn migrate(&self) -> Result<()>;

    /// Store a new document, returning the stored form (with id/timestamps
    /// filled in).
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::Storage`] on backend failure.
    async fn create(&self, doc: NewDocument) -> Result<Document>;

    /// Fetch a document by id.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::NotFound`] if no such document exists, or
    /// [`dkg_core::Error::Storage`] on backend failure.
    async fn get(&self, id: Uuid) -> Result<Document>;

    /// Fetch a document by one of its caller-assigned [`Urn`] search ids.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::NotFound`] if no document is registered
    /// under that URN, or [`dkg_core::Error::Storage`] on backend failure.
    async fn get_by_urn(&self, urn: &Urn) -> Result<Document>;

    /// Replace an existing document's body and metadata, returning the new
    /// stored form.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::NotFound`] if the document is missing, or
    /// [`dkg_core::Error::Storage`] on backend failure.
    async fn replace(&self, id: Uuid, doc: NewDocument) -> Result<Document>;

    /// Delete a document by id.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::NotFound`] if the document is missing, or
    /// [`dkg_core::Error::Storage`] on backend failure.
    async fn delete(&self, id: Uuid) -> Result<()>;

    /// Search for documents matching `query`. When `query.text` is set the
    /// full-text index is consulted; otherwise documents are listed by
    /// collection / metadata containment.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::Storage`] on backend failure.
    async fn search(&self, query: Query) -> Result<Vec<Document>>;

    /// Reconcile the store after a partial write or crash: re-drive any
    /// document left in a non-final state (body present but not yet
    /// indexed) to completion, and report how many records were repaired.
    ///
    /// Safe to call at any time; a healthy store repairs nothing and
    /// returns `0`.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::Storage`] on backend failure.
    async fn repair(&self) -> Result<usize>;

    /// Gracefully drain and stop the store: stop accepting new work, let
    /// in-flight and queued operations finish, and release background
    /// resources (worker threads, connections).
    ///
    /// Call once during process shutdown, after the HTTP server has stopped
    /// accepting requests. After this returns, the store should not be used.
    /// The default implementation does nothing, which suits backends with no
    /// background workers.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::Storage`] if draining fails.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// The boxed future a [`StoreBackend`] constructor returns.
pub type StoreFuture = Pin<Box<dyn Future<Output = Result<Box<dyn DocumentStore>>> + Send>>;

/// A registered document-store backend.
///
/// Backends submit one of these with [`inventory::submit!`]. The `scheme`
/// is matched against the leading component of a connection URL (e.g.
/// `postgres` for `postgres://…`); `connect` opens the store.
///
/// # Examples
///
/// Registering a backend (the composite object store does exactly this):
///
/// ```ignore
/// inventory::submit! {
///     StoreBackend {
///         scheme: "dkg",
///         connect: |url| Box::pin(ObjectDocumentStore::connect(url.to_string())),
///     }
/// }
/// ```
pub struct StoreBackend {
    /// URL scheme this backend handles, without `://`.
    pub scheme: &'static str,
    /// Open a store from a full connection URL.
    pub connect: fn(&str) -> StoreFuture,
}

impl Named for StoreBackend {
    fn name(&self) -> &str {
        self.scheme
    }
}

inventory::collect!(StoreBackend);

/// Open a document store from a connection URL, dispatching on its scheme
/// to a registered [`StoreBackend`].
///
/// # Errors
///
/// Returns [`dkg_core::Error::UnknownBackend`] if no backend is registered
/// for the URL's scheme, or whatever the backend's `connect` returns.
///
/// # Examples
///
/// ```no_run
/// # async fn run() -> dkg_core::Result<()> {
/// let store = dkg_documents::open("dkg://./data").await?;
/// store.migrate().await?;
/// # Ok(())
/// # }
/// ```
pub async fn open(url: &str) -> Result<Box<dyn DocumentStore>> {
    let scheme = url.split("://").next().unwrap_or("");
    let backend = dkg_core::registry::find::<StoreBackend>(scheme)
        .ok_or_else(|| dkg_core::Error::UnknownBackend(scheme.to_string()))?;
    (backend.connect)(url).await
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
