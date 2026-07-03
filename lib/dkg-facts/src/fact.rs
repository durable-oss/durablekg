//! The fact data model.
//!
//! A [`Fact`] is one authority-stamped claim about a single *dimension* of a
//! single entity: "the `phone` of `urn:businesses:<id>` is `603-555-1234`,
//! per a web search, confidence 0.6". Facts are append-only — a correction is
//! a newer fact that out-ranks the old one, not a mutation. Resolving the
//! facts for an entity into one best-value-per-dimension record (the
//! "overview") is a separate concern and lives in `dkg-entity-fact-resolver`.
//!
//! An entity is named by a [`dkg_documents::Urn`] whose namespace is the
//! target table and whose id identifies the row: `urn:businesses:<id>`. The
//! same entity may carry several URNs (a primary uuid plus alternate-id
//! mappings such as `urn:businesses:google-place:<place-id>`); every one of
//! them resolves to the same fact set.

use chrono::{DateTime, Utc};
use dkg_documents::Urn;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Where a fact came from, and how much to trust it.
///
/// `confidence` is the knob fact resolution turns: when two facts disagree on
/// a dimension, the higher-confidence one wins (ties broken by recency). The
/// other fields are provenance — kept so a resolved value can point back at
/// the source that produced it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Authority {
    /// The kind of source that produced the fact.
    pub source_type: SourceType,
    /// The specific source URL, when there is one (a scraped page, a
    /// directory listing). `None` for `Manual` or unsourced `Inference`.
    #[serde(default)]
    pub source_url: Option<String>,
    /// The model that produced the fact, when it was machine-generated.
    #[serde(default)]
    pub model: Option<String>,
    /// How much to trust this fact, in `0.0..=1.0`. Higher wins on conflict.
    pub confidence: f32,
}

/// The class of source behind a fact. The set mirrors the prospect-system
/// `source_type` values and is the natural place to hang per-source default
/// trust if resolution ever wants it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    /// A web search result.
    WebSearch,
    /// Content scraped directly from the entity's own site.
    WebsiteScrape,
    /// A third-party directory or registry lookup.
    DirectoryLookup,
    /// Entered by a human.
    Manual,
    /// Derived/guessed rather than directly observed; usually low-confidence.
    Inference,
}

/// A stored fact: one claim about one dimension of one entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fact {
    /// Stable store-assigned identifier.
    pub id: Uuid,
    /// The entity this fact is about: `urn:<table>:<id>`.
    pub entity: Urn,
    /// The dimension claimed — a column name on the entity's target table,
    /// e.g. `"phone"`. Not validated against the schema here; the resolver
    /// projects only the dimensions a real column accepts.
    pub dimension: String,
    /// The claimed value for that dimension. Opaque JSON.
    pub value: serde_json::Value,
    /// Where the fact came from and how much to trust it.
    pub authority: Authority,
    /// When the claim was true / observed at the source. Drives recency
    /// tie-breaks during resolution.
    pub observed_at: DateTime<Utc>,
    /// When the store first recorded the fact.
    pub recorded_at: DateTime<Utc>,
}

/// The fields a caller supplies to record a new fact. The store owns `id`
/// and `recorded_at`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewFact {
    /// The entity the fact is about: `urn:<table>:<id>`.
    pub entity: Urn,
    /// Additional URNs the same entity is reachable by (alternate-id
    /// mappings). The fact is recorded against `entity` and every alias, so
    /// any of them later resolves to it. Defaults to empty.
    #[serde(default)]
    pub aliases: Vec<Urn>,
    /// The dimension claimed.
    pub dimension: String,
    /// The claimed value.
    pub value: serde_json::Value,
    /// Provenance and trust.
    pub authority: Authority,
    /// When the claim was observed. Defaults to the record time when omitted
    /// (filled in by the store).
    #[serde(default)]
    pub observed_at: Option<DateTime<Utc>>,
}

impl NewFact {
    /// Start a manual, fully-trusted fact about `entity`'s `dimension`. A
    /// convenience for the common hand-entered case; adjust `authority` for
    /// anything machine-sourced.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_facts::{NewFact, SourceType};
    /// use dkg_documents::Urn;
    ///
    /// let entity = Urn::new("businesses", "acme").unwrap();
    /// let f = NewFact::manual(entity, "phone", "603-555-1234");
    /// assert_eq!(f.dimension, "phone");
    /// assert_eq!(f.authority.source_type, SourceType::Manual);
    /// assert_eq!(f.authority.confidence, 1.0);
    /// ```
    pub fn manual(
        entity: Urn,
        dimension: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        NewFact {
            entity,
            aliases: Vec::new(),
            dimension: dimension.into(),
            value: value.into(),
            authority: Authority {
                source_type: SourceType::Manual,
                source_url: None,
                model: None,
                confidence: 1.0,
            },
            observed_at: None,
        }
    }
}

/// Several dimensions of one entity, observed together from one source.
///
/// A bundle is the storage unit: all of its dimensions share a single
/// [`Authority`] and a single `observed_at`, so the entity URN, provenance,
/// and timestamps are written *once* per source rather than once per column.
/// Recording an overview (many fields scraped from one page, or imported from
/// one row) is naturally one bundle. On read, a stored bundle explodes back
/// into one [`Fact`] per dimension, so resolution and everything downstream
/// keep seeing individual facts and never learn how they were packed.
///
/// A single-dimension bundle is the degenerate case [`NewFact`] maps onto, so
/// the per-fact path is just a bundle of size one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewBundle {
    /// The entity all the dimensions are about: `urn:<table>:<id>`.
    pub entity: Urn,
    /// Additional URNs the same entity is reachable by (alternate-id
    /// mappings). Every dimension in the bundle is reachable through each.
    #[serde(default)]
    pub aliases: Vec<Urn>,
    /// Provenance and trust shared by every dimension in the bundle.
    pub authority: Authority,
    /// When the source observed these values. Defaults to the record time
    /// when omitted (filled in by the store). One timestamp for the bundle.
    #[serde(default)]
    pub observed_at: Option<DateTime<Utc>>,
    /// The claimed values, keyed by dimension (column) name. Each entry
    /// becomes one [`Fact`] on read.
    pub dimensions: serde_json::Map<String, serde_json::Value>,
}

impl NewBundle {
    /// Promote a single [`NewFact`] into a one-dimension bundle. The per-fact
    /// `record` path is implemented in terms of this, so single facts and
    /// bundles share one storage shape.
    pub fn from_fact(fact: NewFact) -> Self {
        let mut dimensions = serde_json::Map::with_capacity(1);
        dimensions.insert(fact.dimension, fact.value);
        NewBundle {
            entity: fact.entity,
            aliases: fact.aliases,
            authority: fact.authority,
            observed_at: fact.observed_at,
            dimensions,
        }
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
