//! Blob storage for document bodies, via [OpenDAL].
//!
//! Bodies are stored as opaque byte blobs keyed by document id. The default
//! service is the local filesystem; OpenDAL's uniform [`Operator`] means an
//! S3 (or other object-store) backend is a configuration change, not a code
//! change.
//!
//! [OpenDAL]: opendal

use dkg_core::{Error, Result};
use opendal::services::Fs;
use opendal::Operator;
use uuid::Uuid;

/// Stores and retrieves document body blobs.
#[derive(Clone)]
pub struct BlobStore {
    op: Operator,
}

impl BlobStore {
    /// Open a filesystem-backed blob store rooted at `root`. The directory
    /// is created by OpenDAL on first write if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] if the operator cannot be built.
    pub fn filesystem(root: &str) -> Result<Self> {
        let op = Operator::new(Fs::default().root(root))
            .map_err(Error::storage)?
            .finish();
        Ok(BlobStore { op })
    }

    /// Build a blob store over an already-constructed OpenDAL [`Operator`],
    /// for callers that wire S3 or another service themselves.
    pub fn from_operator(op: Operator) -> Self {
        BlobStore { op }
    }

    /// The blob key for a document id. Bodies live under `bodies/<id>`.
    fn key(id: Uuid) -> String {
        format!("bodies/{id}")
    }

    /// Write (or overwrite) a document body.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on write failure.
    pub async fn put(&self, id: Uuid, body: &[u8]) -> Result<()> {
        self.op
            .write(&Self::key(id), body.to_vec())
            .await
            .map(|_| ())
            .map_err(Error::storage)
    }

    /// Read a document body.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the blob is absent, or
    /// [`Error::Storage`] on other failures.
    pub async fn get(&self, id: Uuid) -> Result<Vec<u8>> {
        match self.op.read(&Self::key(id)).await {
            Ok(buf) => Ok(buf.to_vec()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                Err(Error::NotFound(format!("body for document {id}")))
            }
            Err(e) => Err(Error::storage(e)),
        }
    }

    /// Delete a document body. Deleting an absent blob is not an error
    /// (OpenDAL deletes are idempotent), which suits repair/cleanup.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    pub async fn delete(&self, id: Uuid) -> Result<()> {
        self.op.delete(&Self::key(id)).await.map_err(Error::storage)
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
