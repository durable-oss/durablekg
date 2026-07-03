//! Wiring fact resolution into a feature-D job.
//!
//! Feature D's [`Transform`] maps **one document at a time** to records. Fact
//! resolution is the opposite shape: the winning value for a dimension can
//! only be known once *all* of an entity's facts are in hand. A single fact
//! document is not enough to resolve.
//!
//! So the registered transform does the document-local half — parse a fact
//! document into the one-cell record it implies, grouped by the entity's
//! target table — and [`resolve_entity`] does the cross-document half for
//! callers (a runner, the CLI) that have already gathered an entity's facts
//! via [`FactStore::facts_for`](dkg_facts::FactStore::facts_for).
//!
//! When the feature-D runner learns to group documents by a key before
//! transforming, the grouped path can move behind the [`Transform`]
//! interface; until then resolution is driven through [`resolve_entity`].

use dkg_config::Table;
use dkg_facts::{Fact, Urn};
use dkg_graph::Record;

use crate::record::to_record;
use crate::resolve::resolve;
use crate::sync::parse_typed_id;

/// Resolve an entity's facts and project them onto `table` — the full,
/// cross-document path. This is what a runner calls after gathering the
/// facts for one entity.
///
/// The returned record does **not** carry the system `id` (that is assigned at
/// the sync boundary). The entity URN's typed alias (e.g. `slug:acme`) is
/// folded in when it names an alternate-key column of `table`.
///
/// # Examples
///
/// ```
/// use dkg_config::{Column, ColumnType, Table};
/// use dkg_entity_fact_resolver::transform::resolve_entity;
/// use dkg_facts::{Authority, Fact, SourceType, Urn};
/// use chrono::{TimeZone, Utc};
/// use uuid::Uuid;
///
/// let table = Table {
///     name: "businesses".into(),
///     columns: vec![
///         Column { name: "phone".into(), column_type: ColumnType::String, nullable: true, array: false },
///     ],
///     alternate_keys: vec![],
/// };
/// let entity = Urn::new("businesses", "slug:acme").unwrap();
/// let mk = |val: &str, conf: f32| Fact {
///     id: Uuid::new_v4(),
///     entity: entity.clone(),
///     dimension: "phone".into(),
///     value: serde_json::json!(val),
///     authority: Authority { source_type: SourceType::WebSearch, source_url: None, model: None, confidence: conf },
///     observed_at: Utc.timestamp_opt(0, 0).unwrap(),
///     recorded_at: Utc.timestamp_opt(0, 0).unwrap(),
/// };
/// let record = resolve_entity(&entity, &[mk("low", 0.2), mk("high", 0.9)], &table);
/// assert_eq!(record.get("phone").unwrap(), &serde_json::json!("high"));
/// ```
pub fn resolve_entity(entity: &Urn, facts: &[Fact], table: &Table) -> Record {
    let resolved = resolve(facts);
    let (keytype, value) = parse_typed_id(entity.id());
    to_record(&resolved, table, Some((keytype, value)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dkg_config::{Column, ColumnType};
    use dkg_facts::{Authority, SourceType};
    use uuid::Uuid;

    fn table() -> Table {
        Table {
            name: "businesses".into(),
            columns: vec![
                Column {
                    name: "slug".into(),
                    column_type: ColumnType::String,
                    nullable: false,
                    array: false,
                },
                Column {
                    name: "phone".into(),
                    column_type: ColumnType::String,
                    nullable: true,
                    array: false,
                },
            ],
            alternate_keys: vec!["slug".into()],
        }
    }

    #[test]
    fn resolves_then_projects() {
        let entity = Urn::new("businesses", "slug:acme").unwrap();
        let facts: Vec<Fact> = [("low", 0.2_f32), ("high", 0.9)]
            .into_iter()
            .map(|(v, c)| Fact {
                id: Uuid::new_v4(),
                entity: entity.clone(),
                dimension: "phone".into(),
                value: serde_json::json!(v),
                authority: Authority {
                    source_type: SourceType::WebSearch,
                    source_url: None,
                    model: None,
                    confidence: c,
                },
                observed_at: chrono::Utc::now(),
                recorded_at: chrono::Utc::now(),
            })
            .collect();

        let record = resolve_entity(&entity, &facts, &table());
        // `id` is not projected; the URN alias `slug:acme` is folded in.
        assert!(!record.contains_key("id"));
        assert_eq!(record.get("slug").unwrap(), &serde_json::json!("acme"));
        assert_eq!(record.get("phone").unwrap(), &serde_json::json!("high"));
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
