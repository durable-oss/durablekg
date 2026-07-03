//! # dkg-facts — fact storage for DurableKG graph entities
//!
//! The graph schema (feature B) describes the *target shape* of an entity:
//! the `businesses` table and its columns. `dkg-facts` is the layer beneath
//! it — the place observations land before they are resolved into a row.
//!
//! A [`Fact`] is one authority-stamped claim about a single dimension of a
//! single entity: a `(entity, dimension, value)` triple plus an [`Authority`]
//! (source, model, confidence) and timestamps. Facts are append-only. The
//! schema author never authors facts and never edits the schema to enable
//! them; adding a column to a table simply makes that column a dimension
//! facts can target.
//!
//! This crate stores, finds, and indexes facts — nothing more. Turning the
//! many facts about an entity into one best-value-per-dimension overview row
//! is a separate policy concern handled by `dkg-entity-fact-resolver`.
//!
//! Facts are persisted on top of the feature-A document store
//! ([`dkg_documents`]); there is no separate backend. See [`store`] for the
//! mapping and the three access patterns it supports.

pub mod fact;
pub mod store;

pub use fact::{Authority, Fact, NewBundle, NewFact, SourceType};
pub use store::{facts_collection, DocumentFactStore, FactStore, FACTS_COLLECTION_PREFIX};

// Re-export the entity-naming type so callers building facts don't need a
// direct dependency on `dkg-documents` just to name an entity.
pub use dkg_documents::Urn;

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
