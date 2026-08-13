//! # dkg-entity-fact-resolver — facts into an overview row
//!
//! `dkg-facts` stores the many authority-stamped claims about an entity. This
//! crate is the policy layer that collapses them into one
//! best-value-per-dimension overview and projects it onto a graph table — the
//! "overview entry with the highest-authority fact for each dimension".
//!
//! Two steps, kept separate so the policy is pure and testable:
//!
//! 1. [`resolve`] — `&[Fact]` → [`ResolvedEntity`]. Per dimension, highest
//!    confidence wins, ties broken by recency; the winning fact's provenance
//!    is retained. Storage- and schema-free.
//! 2. [`to_record`] — [`ResolvedEntity`] + [`Table`](dkg_config::Table) →
//!    [`Record`](dkg_graph::Record). Keeps only declared columns, stamps the
//!    id; ready for feature D to insert.
//!
//! The [`transform`] module wires both into a feature-D [`Transform`], so a
//! bulk job can turn a collection of fact documents into graph rows.

pub mod queue;
pub mod record;
pub mod resolve;
pub mod sync;
pub mod transform;

pub use queue::{SyncPass, SyncQueue};
pub use record::{is_blank_alias, to_record};
pub use resolve::{resolve, ResolvedDimension, ResolvedEntity};
pub use sync::{
    parse_typed_id, sync_entities, sync_entity, sync_table, SyncOutcome, TableSyncSummary,
};
pub use transform::resolve_entity;

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
