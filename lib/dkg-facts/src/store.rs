//! The fact store: a typed, append-only facade over the document store.
//!
//! A fact does not need its own backend. Each [`Fact`] is persisted as a
//! [`dkg_documents::Document`] in a dedicated collection, with the entity
//! URN (and any aliases) as the document's search ids and the
//! resolution-relevant fields lifted into queryable metadata. That buys the
//! three access patterns directly off the document store's existing indexes:
//!
//! - **all facts for an entity** — fetch by the entity's URN namespace;
//! - **all facts on one dimension** — a metadata-containment query;
//! - **alternate-id mappings** — the search ids on an entity's facts.
//!
//! The store is append-only: [`record`](FactStore::record) only ever creates
//! documents. A correction is a newer, higher-confidence fact, resolved
//! later — not an in-place edit.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dkg_core::{Error, Result};
use dkg_documents::{Document, DocumentStore, NewDocument, Query, Urn};
use serde_json::json;
use uuid::Uuid;

use crate::fact::{Authority, Fact, NewBundle, NewFact};

/// Prefix for the per-table fact collections. Facts about an entity live in
/// the collection `facts:<table>`, where `<table>` is the entity URN's
/// namespace — so each graph table gets its own fact namespace, listable and
/// clearable on its own, and an entity filter never scans facts about
/// unrelated tables.
pub const FACTS_COLLECTION_PREFIX: &str = "facts";

/// The fact collection for a given target table, e.g. `facts:businesses`.
///
/// # Examples
///
/// ```
/// use dkg_facts::facts_collection;
/// assert_eq!(facts_collection("businesses"), "facts:businesses");
/// ```
pub fn facts_collection(table: &str) -> String {
    format!("{FACTS_COLLECTION_PREFIX}:{table}")
}

/// The on-disk body of a fact document: one entity's dimensions sharing a
/// single authority and observation time. The document store owns the id and
/// the record time, so neither is stored here — they come from the document.
/// A bundle written for a single dimension is the degenerate, per-fact case.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredBundle {
    entity: Urn,
    authority: Authority,
    observed_at: chrono::DateTime<Utc>,
    recorded_at: chrono::DateTime<Utc>,
    dimensions: serde_json::Map<String, serde_json::Value>,
}

impl StoredBundle {
    /// Expand into one [`Fact`] per dimension. Every fact in the bundle is the
    /// same physical document, so they share the document id; the dimension is
    /// what distinguishes them. Facts are addressed as a set through
    /// [`FactStore::facts_for`], so a shared id is sufficient — there is no
    /// per-dimension document to fetch on its own.
    fn explode(self, doc_id: Uuid) -> Vec<Fact> {
        self.dimensions
            .into_iter()
            .map(|(dimension, value)| Fact {
                id: doc_id,
                entity: self.entity.clone(),
                dimension,
                value,
                authority: self.authority.clone(),
                observed_at: self.observed_at,
                recorded_at: self.recorded_at,
            })
            .collect()
    }
}

/// Storage and retrieval of [`Fact`]s about graph entities.
///
/// Append-only by design: there is no update or delete. Object-safe so
/// callers can hold an `Arc<dyn FactStore>`.
#[async_trait]
pub trait FactStore: Send + Sync {
    /// Record a new fact, returning the stored form (with id and record
    /// time filled in). The fact is indexed under its entity URN and every
    /// alias, so any of them later retrieves it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on backend failure, or
    /// [`Error::Serialization`] if the fact cannot be encoded.
    async fn record(&self, fact: NewFact) -> Result<Fact>;

    /// Record many dimensions of one entity as a single document, sharing one
    /// authority and observation time. Returns the stored facts, one per
    /// dimension, with ids filled in. This is the storage-efficient path: the
    /// entity URN, provenance, and timestamps are written once for the whole
    /// bundle instead of once per column.
    ///
    /// [`record`](FactStore::record) is the one-dimension special case of this.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on backend failure, or
    /// [`Error::Serialization`] if the bundle cannot be encoded.
    async fn record_bundle(&self, bundle: NewBundle) -> Result<Vec<Fact>>;

    /// Fetch a single fact by id.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if no such fact exists, or
    /// [`Error::Storage`] on backend failure.
    async fn get(&self, id: Uuid) -> Result<Fact>;

    /// Every fact recorded about `entity`, in unspecified order. Resolving
    /// these into an overview is the resolver's job.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on backend failure.
    async fn facts_for(&self, entity: &Urn) -> Result<Vec<Fact>>;

    /// Every fact recorded about each of `entities`, keyed by the URN string
    /// the facts were recorded under.
    ///
    /// The bulk form of [`facts_for`](Self::facts_for). Reading one entity's
    /// facts costs a scan of its namespace's fact collection, so resolving a
    /// batch one entity at a time costs that scan once per entity; this reads
    /// each namespace once for the whole set. The batch sync path uses it to
    /// gather facts for hundreds of entities (and every alias form each
    /// answers to) in a single pass.
    ///
    /// Entities with no facts are **absent** from the map rather than mapped
    /// to an empty vector. Entities spanning several namespaces are handled;
    /// each namespace is read once.
    ///
    /// The default implementation calls [`facts_for`](Self::facts_for) per
    /// entity, so an implementor with no bulk path stays correct.
    ///
    /// A single entity's read failing does not fail the batch: that entity
    /// is simply absent from the returned map (the same shape as "no facts
    /// recorded"), and every other entity's facts are still returned. A
    /// batch sync caller that cannot distinguish "no facts" from "read
    /// failed" would otherwise treat one bad entity's storage fault as every
    /// entity in the batch having no facts, since callers commonly fall back
    /// to an empty map on `Err` — silently blanking hundreds of unrelated,
    /// perfectly readable entities every pass.
    ///
    /// # Errors
    ///
    /// This default implementation never returns `Err`; a per-entity read
    /// failure is swallowed and that entity is simply omitted. A concrete
    /// backend override may still document its own error conditions if it
    /// cannot offer that same isolation.
    async fn facts_for_many(
        &self,
        entities: &[Urn],
    ) -> Result<std::collections::HashMap<String, Vec<Fact>>> {
        let mut out = std::collections::HashMap::new();
        for entity in entities {
            let Ok(facts) = self.facts_for(entity).await else {
                continue;
            };
            if !facts.is_empty() {
                out.insert(entity.to_string(), facts);
            }
        }
        Ok(out)
    }

    /// Every fact about `entity` claiming the given `dimension`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on backend failure.
    async fn facts_on(&self, entity: &Urn, dimension: &str) -> Result<Vec<Fact>>;

    /// Every distinct entity that has at least one fact recorded about it in
    /// `table`'s fact collection, in unspecified order.
    ///
    /// Entities are returned in the URN form their facts were recorded under.
    /// The same entity can appear more than once when its facts were recorded
    /// through different aliases; the sync path canonicalizes those, so the
    /// duplicates cost a redundant sync rather than a wrong result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Storage`] on backend failure.
    async fn entities_in(&self, table: &str) -> Result<Vec<Urn>>;
}

/// A [`FactStore`] backed by any [`DocumentStore`].
pub struct DocumentFactStore {
    docs: Arc<dyn DocumentStore>,
    limit: usize,
}

/// Default cap on how many fact documents a single query pulls back. Facts
/// for one entity are expected to number in the dozens, not millions.
const DEFAULT_LIMIT: usize = 1000;

impl DocumentFactStore {
    /// Wrap a document store as a fact store.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        DocumentFactStore {
            docs,
            limit: DEFAULT_LIMIT,
        }
    }

    /// Override the per-query result cap (default 1000).
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Build the queryable metadata for a stored bundle. Kept in one place so
    /// recording and the `facts_on` filter agree on key names. `dimensions`
    /// holds every column the bundle claims, so a metadata-containment query
    /// on `{ "dimensions": "phone" }` still selects bundles that mention
    /// `phone` — the same access pattern the per-fact store relied on.
    fn metadata(stored: &StoredBundle) -> serde_json::Value {
        json!({
            "entity": stored.entity.to_string(),
            "dimensions": stored.dimensions.keys().collect::<Vec<_>>(),
            "source_type": stored.authority.source_type,
            "confidence": stored.authority.confidence,
            "observed_at": stored.observed_at,
        })
    }

    /// Explode a stored document back into one [`Fact`] per dimension. The
    /// document store owns the identifier; each fact's `id` is derived from
    /// the document id and its dimension so a bundle's facts stay individually
    /// addressable and stable across reads.
    fn facts_from_doc(doc: Document) -> Result<Vec<Fact>> {
        let bundle: StoredBundle = serde_json::from_value(doc.body).map_err(Error::from)?;
        Ok(bundle.explode(doc.id))
    }
}

#[async_trait]
impl FactStore for DocumentFactStore {
    async fn record(&self, new: NewFact) -> Result<Fact> {
        // A single fact is a one-dimension bundle; share the storage path so
        // there is exactly one on-disk shape. The bundle yields one fact back.
        let mut facts = self.record_bundle(NewBundle::from_fact(new)).await?;
        facts
            .pop()
            .ok_or_else(|| Error::Storage("recorded bundle yielded no fact".into()))
    }

    async fn record_bundle(&self, new: NewBundle) -> Result<Vec<Fact>> {
        let now = Utc::now();
        let stored = StoredBundle {
            entity: new.entity.clone(),
            authority: new.authority,
            observed_at: new.observed_at.unwrap_or(now),
            recorded_at: now,
            dimensions: new.dimensions,
        };

        // The resolution-relevant fields are lifted into metadata so the
        // entity- and dimension-scoped searches hit the store's index. The
        // entity URN is shared across an entity's many bundles, so it cannot go
        // in `search_ids` (the store rejects a reused URN); it is indexed in
        // metadata instead, and `search_ids` is left for genuinely unique ids.
        let metadata = Self::metadata(&stored);
        let body = serde_json::to_value(&stored)?;

        let doc = NewDocument {
            collection: facts_collection(new.entity.namespace()),
            search_ids: Vec::new(),
            body,
            metadata,
        };
        let created = self.docs.create(doc).await?;
        Ok(stored.explode(created.id))
    }

    async fn get(&self, id: Uuid) -> Result<Fact> {
        // `id` is a bundle's document id; its facts are derived from it. With
        // one fact recorded per `record` call (a one-dimension bundle), this
        // returns that fact. For a multi-dimension bundle it returns the first
        // dimension — facts within a bundle share storage and are addressed as
        // a set by `facts_for`, not individually re-fetched by id.
        let doc = self.docs.get(id).await?;
        Self::facts_from_doc(doc)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::NotFound(format!("no facts in bundle {id}")))
    }

    async fn facts_for(&self, entity: &Urn) -> Result<Vec<Fact>> {
        let query = Query {
            collection: Some(facts_collection(entity.namespace())),
            text: None,
            metadata_contains: json!({ "entity": entity.to_string() }),
            limit: self.limit,
        };
        let docs = self.docs.search(query).await?;
        let mut facts = Vec::new();
        for doc in docs {
            facts.extend(Self::facts_from_doc(doc)?);
        }
        Ok(facts)
    }

    async fn facts_for_many(
        &self,
        entities: &[Urn],
    ) -> Result<std::collections::HashMap<String, Vec<Fact>>> {
        use std::collections::{BTreeMap, BTreeSet, HashMap};

        // Facts live in a per-namespace collection, and one `search_grouped`
        // covers a single collection — so bucket the requested URNs by
        // namespace and read each namespace once. Deduplicated: the same URN
        // asked for twice is one lookup.
        let mut by_namespace: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for entity in entities {
            by_namespace
                .entry(entity.namespace())
                .or_default()
                .insert(entity.to_string());
        }

        // Isolated per namespace and per entity: a namespace whose
        // `search_grouped` call fails, or a single entity whose stored
        // document does not parse, is skipped rather than failing the whole
        // batch. A batch sync caller — see `dkg-entity-fact-resolver`'s
        // `prepare_batch` — reads hundreds of entities across many
        // namespaces in one call; without this isolation, one bad namespace
        // or one corrupt document silently blanks facts for every other
        // entity in the batch, which a caller that falls back to an empty
        // map on error cannot tell apart from "these entities have no
        // facts". Those entities are simply absent from the returned map,
        // same as "no facts recorded".
        let mut out = HashMap::new();
        for (namespace, urns) in by_namespace {
            let values: Vec<String> = urns.into_iter().collect();
            let Ok(grouped) = self
                .docs
                .search_grouped(&facts_collection(namespace), "entity", &values, self.limit)
                .await
            else {
                continue;
            };
            for (urn, docs) in grouped {
                let mut facts = Vec::new();
                for doc in docs {
                    let Ok(parsed) = Self::facts_from_doc(doc) else {
                        continue;
                    };
                    facts.extend(parsed);
                }
                if !facts.is_empty() {
                    out.insert(urn, facts);
                }
            }
        }
        Ok(out)
    }

    async fn facts_on(&self, entity: &Urn, dimension: &str) -> Result<Vec<Fact>> {
        // Bundles store their dimensions as an array, which the document
        // store's containment query can't index into; pull the entity's
        // bundles and keep the matching dimension. An entity has dozens of
        // facts at most, so the in-memory filter is cheap.
        Ok(self
            .facts_for(entity)
            .await?
            .into_iter()
            .filter(|f| f.dimension == dimension)
            .collect())
    }

    async fn entities_in(&self, table: &str) -> Result<Vec<Urn>> {
        // Every bundle lifts its entity URN into metadata, so listing the
        // table's fact collection and reading that field enumerates the
        // entities without deserializing the bundle bodies.
        //
        // The per-query cap is deliberately *not* applied here: it exists to
        // bound one entity's fact list, and silently truncating an
        // enumeration would make a "sync everything" caller quietly skip
        // entities it believed it had covered.
        let query = Query {
            collection: Some(facts_collection(table)),
            text: None,
            metadata_contains: json!({}),
            limit: usize::MAX,
        };

        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for doc in self.docs.search(query).await? {
            let Some(entity) = doc.metadata.get("entity").and_then(|v| v.as_str()) else {
                continue;
            };
            if !seen.insert(entity.to_string()) {
                continue;
            }
            match entity.parse::<Urn>() {
                Ok(urn) => out.push(urn),
                // A metadata entry that is not a valid URN cannot be synced;
                // skip it rather than failing the whole enumeration.
                Err(_) => continue,
            }
        }
        Ok(out)
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
