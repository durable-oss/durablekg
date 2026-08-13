//! Blob storage for document bodies, via [OpenDAL].
//!
//! Bodies are stored as opaque byte blobs keyed by document id. The default
//! service is the local filesystem; OpenDAL's uniform [`Operator`] means an
//! S3 (or other object-store) backend is a configuration change, not a code
//! change.
//!
//! # Layout
//!
//! Blobs are fanned out into a two-level tree taken from the first four hex
//! digits of the document id: `bodies/ab/cd/abcdef01-....`. Document ids are
//! v4 UUIDs, so those digits are uniformly random and the tree stays balanced
//! without hashing anything.
//!
//! The flat layout this replaces (`bodies/<id>`) put every blob in one
//! directory. That is workable for point lookups on a filesystem with hashed
//! directory indexes, and unworkable for everything else: `readdir` costs
//! grow with the directory, backup and rsync walk the whole thing, and the
//! dentry cache is dominated by one enormous directory. A production store
//! reached 2.4 million files in `bodies/` before this changed.
//!
//! 256 x 256 leaves put 2.4M blobs at ~37 per directory, and 100M at ~1,500 —
//! both comfortable.
//!
//! # Migration
//!
//! Old stores are read in place. [`get`](BlobStore::get) tries the tree first
//! and falls back to the flat key, so nothing needs converting before the
//! store is usable again.
//!
//! Conversion is **lazy and incremental**: a blob found at the flat location
//! is rewritten into the tree and the flat copy removed, so the store
//! migrates itself as it is read. There is deliberately no batch job — a
//! 2.4-million-file rewrite is a long, failure-prone operation to run against
//! a live store, and spreading it across normal traffic costs each read one
//! extra write, once, and nothing thereafter.
//!
//! Writes always use the tree, so newly written blobs are never in the old
//! layout.
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
    /// Whether a blob found in the old flat layout is migrated into the tree
    /// on read. On by default; see [`without_lazy_migration`].
    ///
    /// [`without_lazy_migration`]: BlobStore::without_lazy_migration
    migrate_on_read: bool,
    /// Whether the backing service can rename.
    ///
    /// Cached at construction because it is consulted on every migrating
    /// read, and `info()` is not free enough to call in a loop. A filesystem
    /// renames as a metadata operation; S3 has no rename and OpenDAL emulates
    /// it as a server-side copy plus delete, which is no better than the
    /// write-and-delete fallback.
    can_rename: bool,
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
        Ok(BlobStore::from_operator(op))
    }

    /// Build a blob store over an already-constructed OpenDAL [`Operator`],
    /// for callers that wire S3 or another service themselves.
    pub fn from_operator(op: Operator) -> Self {
        let can_rename = op.info().full_capability().rename;
        BlobStore {
            op,
            migrate_on_read: true,
            can_rename,
        }
    }

    /// Read old-layout blobs but leave them where they are.
    ///
    /// Lazy migration turns a read into a read-plus-write, which a read-only
    /// replica cannot do and a latency-sensitive path may not want. Reads
    /// still find old blobs either way; only the rewrite is suppressed.
    #[must_use]
    pub fn without_lazy_migration(mut self) -> Self {
        self.migrate_on_read = false;
        self
    }

    /// The blob key for a document id: `bodies/ab/cd/<id>`.
    ///
    /// The two levels come from the first four hex digits of the id. A v4
    /// UUID's first digits are random, so this distributes evenly; ids that
    /// were *not* random would cluster, which is why this is documented as
    /// depending on the id being a v4 UUID.
    fn key(id: Uuid) -> String {
        let hex = id.simple().to_string();
        format!("bodies/{}/{}/{}", &hex[0..2], &hex[2..4], id)
    }

    /// The pre-tree key for a document id, kept for reading old stores.
    fn legacy_key(id: Uuid) -> String {
        format!("bodies/{id}")
    }

    /// Write (or overwrite) a document body.
    ///
    /// Always writes the tree layout. When an old flat copy exists it is
    /// removed, so a rewritten document does not leave a stale body behind
    /// that a later fallback read could return.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on write failure.
    pub async fn put(&self, id: Uuid, body: &[u8]) -> Result<()> {
        self.op
            .write(&Self::key(id), body.to_vec())
            .await
            .map_err(Error::storage)?;

        // Best-effort: a stale flat copy is only reachable if the tree copy
        // vanishes, and failing the write because cleanup failed would be
        // worse than leaving it.
        let _ = self.op.delete(&Self::legacy_key(id)).await;
        Ok(())
    }

    /// Read a document body.
    ///
    /// Tries the tree first, then the old flat location. A blob found in the
    /// old location is migrated into the tree (unless migration is disabled),
    /// so a store converts itself over time as its documents are read.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the blob is in neither location, or
    /// [`Error::Storage`] on other failures.
    pub async fn get(&self, id: Uuid) -> Result<Vec<u8>> {
        match self.op.read(&Self::key(id)).await {
            Ok(buf) => return Ok(buf.to_vec()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::storage(e)),
        }

        // Not in the tree: this is either an old blob or a genuine miss.
        let body = match self.op.read(&Self::legacy_key(id)).await {
            Ok(buf) => buf.to_vec(),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                return Err(Error::NotFound(format!("body for document {id}")))
            }
            Err(e) => return Err(Error::storage(e)),
        };

        if self.migrate_on_read {
            self.migrate(id, &body).await;
        }

        Ok(body)
    }

    /// Move a blob from the old flat location into the tree.
    ///
    /// Best-effort throughout: the caller already has the body, so a failed
    /// migration costs nothing but a retry on the next read. Nothing here
    /// propagates an error, because failing a successful read over a
    /// housekeeping problem would be the wrong trade.
    ///
    /// Prefers `rename`, which on a filesystem is a metadata operation — no
    /// body bytes move, and it is atomic, so there is no window where the
    /// blob exists in both places or neither. That matters at scale: this
    /// runs once per blob across a whole store, 2.4 million times on the
    /// store that motivated the tree.
    ///
    /// Services without rename (S3 and friends, where OpenDAL would emulate
    /// it as a server-side copy plus delete anyway) fall back to writing the
    /// body we just read and deleting the old key. The new copy is written
    /// *before* the old is removed, so a crash between them leaves a
    /// duplicate — harmless, since the tree is read first — rather than a
    /// hole.
    async fn migrate(&self, id: Uuid, body: &[u8]) {
        let from = Self::legacy_key(id);
        let to = Self::key(id);

        // A service can advertise rename and still refuse a particular one (a
        // cross-device link, say), so a failure falls through to
        // copy-and-delete rather than giving up on the blob.
        if self.can_rename && self.op.rename(&from, &to).await.is_ok() {
            return;
        }

        if self.op.write(&to, body.to_vec()).await.is_ok() {
            let _ = self.op.delete(&from).await;
        }
    }

    /// Delete a document body. Deleting an absent blob is not an error
    /// (OpenDAL deletes are idempotent), which suits repair/cleanup.
    ///
    /// Removes both layouts: a document written before the tree existed and
    /// never read since still has only a flat copy, and deleting just the
    /// tree key would silently leave it behind.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on failure.
    pub async fn delete(&self, id: Uuid) -> Result<()> {
        self.op
            .delete(&Self::key(id))
            .await
            .map_err(Error::storage)?;
        self.op
            .delete(&Self::legacy_key(id))
            .await
            .map_err(Error::storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store(dir: &tempfile::TempDir) -> BlobStore {
        BlobStore::filesystem(dir.path().to_str().unwrap()).unwrap()
    }

    #[test]
    fn the_key_fans_out_on_the_first_four_hex_digits() {
        let id = Uuid::parse_str("ab12cd34-0000-4000-8000-000000000000").unwrap();
        assert_eq!(
            BlobStore::key(id),
            "bodies/ab/12/ab12cd34-0000-4000-8000-000000000000"
        );
    }

    #[test]
    fn distinct_prefixes_land_in_distinct_directories() {
        let a = Uuid::parse_str("0011cd34-0000-4000-8000-000000000000").unwrap();
        let b = Uuid::parse_str("ff22cd34-0000-4000-8000-000000000000").unwrap();
        assert!(BlobStore::key(a).starts_with("bodies/00/11/"));
        assert!(BlobStore::key(b).starts_with("bodies/ff/22/"));
    }

    #[tokio::test]
    async fn a_written_blob_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        let id = Uuid::new_v4();

        store.put(id, b"hello").await.unwrap();
        assert_eq!(store.get(id).await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn a_missing_blob_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        let err = store.get(Uuid::new_v4()).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn an_old_flat_blob_is_still_readable() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        let id = Uuid::new_v4();

        // Write directly to the pre-tree location, as an old store would.
        store
            .op
            .write(&BlobStore::legacy_key(id), b"legacy".to_vec())
            .await
            .unwrap();

        assert_eq!(store.get(id).await.unwrap(), b"legacy");
    }

    #[tokio::test]
    async fn reading_an_old_blob_migrates_it_into_the_tree() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        let id = Uuid::new_v4();

        store
            .op
            .write(&BlobStore::legacy_key(id), b"legacy".to_vec())
            .await
            .unwrap();

        assert_eq!(store.get(id).await.unwrap(), b"legacy");

        // The blob is now in the tree, and the flat copy is gone.
        assert!(store.op.read(&BlobStore::key(id)).await.is_ok());
        assert!(store.op.read(&BlobStore::legacy_key(id)).await.is_err());

        // And it still reads correctly from its new home.
        assert_eq!(store.get(id).await.unwrap(), b"legacy");
    }

    #[tokio::test]
    async fn a_filesystem_store_migrates_by_rename() {
        // The fast path: `Fs` advertises rename, so migration should not copy
        // body bytes at all.
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        assert!(store.can_rename, "the Fs service should support rename");

        let id = Uuid::new_v4();
        store
            .op
            .write(&BlobStore::legacy_key(id), b"legacy".to_vec())
            .await
            .unwrap();

        store.migrate(id, b"legacy").await;

        assert!(store.op.read(&BlobStore::key(id)).await.is_ok());
        assert!(store.op.read(&BlobStore::legacy_key(id)).await.is_err());
        assert_eq!(store.get(id).await.unwrap(), b"legacy");
    }

    #[tokio::test]
    async fn migration_falls_back_to_copy_when_rename_is_unavailable() {
        // Services without rename (S3) must still migrate. Simulated by
        // clearing the capability flag rather than standing up a fake
        // service: the branch under test is the one that reads it.
        let dir = tempfile::tempdir().unwrap();
        let mut store = temp_store(&dir);
        store.can_rename = false;

        let id = Uuid::new_v4();
        store
            .op
            .write(&BlobStore::legacy_key(id), b"legacy".to_vec())
            .await
            .unwrap();

        store.migrate(id, b"legacy").await;

        assert!(store.op.read(&BlobStore::key(id)).await.is_ok());
        assert!(store.op.read(&BlobStore::legacy_key(id)).await.is_err());
        assert_eq!(store.get(id).await.unwrap(), b"legacy");
    }

    #[tokio::test]
    async fn migrating_an_absent_blob_is_harmless() {
        // `migrate` is best-effort and must never panic or propagate: a blob
        // deleted between the read and the migration is a real race.
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        store.migrate(Uuid::new_v4(), b"body").await;
    }

    #[tokio::test]
    async fn migration_can_be_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir).without_lazy_migration();
        let id = Uuid::new_v4();

        store
            .op
            .write(&BlobStore::legacy_key(id), b"legacy".to_vec())
            .await
            .unwrap();

        assert_eq!(store.get(id).await.unwrap(), b"legacy");
        // Read-only behaviour: the blob stays where it was.
        assert!(store.op.read(&BlobStore::legacy_key(id)).await.is_ok());
        assert!(store.op.read(&BlobStore::key(id)).await.is_err());
    }

    #[tokio::test]
    async fn writing_over_an_old_blob_removes_the_flat_copy() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        let id = Uuid::new_v4();

        store
            .op
            .write(&BlobStore::legacy_key(id), b"old".to_vec())
            .await
            .unwrap();
        store.put(id, b"new").await.unwrap();

        // Only the tree copy remains, so a fallback read cannot resurrect the
        // stale body.
        assert!(store.op.read(&BlobStore::legacy_key(id)).await.is_err());
        assert_eq!(store.get(id).await.unwrap(), b"new");
    }

    #[tokio::test]
    async fn delete_removes_both_layouts() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        let id = Uuid::new_v4();

        // A store mid-migration can legitimately have both.
        store
            .op
            .write(&BlobStore::legacy_key(id), b"old".to_vec())
            .await
            .unwrap();
        store
            .op
            .write(&BlobStore::key(id), b"new".to_vec())
            .await
            .unwrap();

        store.delete(id).await.unwrap();
        assert!(matches!(
            store.get(id).await.unwrap_err(),
            Error::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn deleting_an_absent_blob_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = temp_store(&dir);
        store.delete(Uuid::new_v4()).await.unwrap();
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
