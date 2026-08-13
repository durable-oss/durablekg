//! # dkg-core
//!
//! Shared foundations for DurableKG: the workspace-wide [`Error`] type, the
//! deployment [`Settings`], and the [`registry`] machinery that every
//! extensible part of the system plugs into.
//!
//! DurableKG offers four capabilities, built up across the workspace's
//! crates:
//!
//! - **A — Documents** (`dkg-documents`): store semistructured data with
//!   rich metadata, searchably.
//! - **B — Knowledge graphs** (`dkg-graph`): ingest, store, and query
//!   user-defined graphs over pluggable backends.
//! - **C — Endpoints** (`dkg-endpoints`): user-defined API endpoints whose
//!   results are cached as documents.
//! - **D — Jobs** (`dkg-jobs`): bulk processing of document data into graph
//!   data.
//!
//! Knowledge-graph shapes and endpoint definitions are authored as
//! Trongoscript and loaded by `dkg-config`.
//!
//! Everything that can vary by deployment or grow over time — storage
//! backends, config kinds, output formats — is registered through
//! [`registry`] rather than hard-coded, so new variants are added by
//! submitting an `inventory` entry instead of editing a central switch.

pub mod config;
pub mod error;
pub mod registry;

pub use config::{BindAddress, Settings};
pub use error::{Error, Result};
pub use registry::{find, names, Named};

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
