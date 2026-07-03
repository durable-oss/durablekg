//! Pure fact resolution.
//!
//! [`resolve`] collapses the many facts about an entity into one best value
//! per dimension. The rule, lifted from the prospect-system `merge_facts`:
//! for each dimension, the **highest-confidence** fact wins; ties are broken
//! by the **latest `observed_at`**. The winning fact's [`Authority`] is kept
//! alongside its value, so a resolved overview can still say where each field
//! came from — something a plain "last write wins" merge throws away.
//!
//! This module is deliberately storage- and schema-free: it takes a slice of
//! facts and returns a [`ResolvedEntity`]. Projecting that onto a concrete
//! [`Table`](dkg_config::Table) — keeping only the dimensions that are real
//! columns — happens in [`crate::record`].

use std::collections::HashMap;

use dkg_facts::{Authority, Fact};

/// One dimension's resolved value plus the provenance of the fact that won.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedDimension {
    /// The winning value for this dimension.
    pub value: serde_json::Value,
    /// The authority of the fact the value came from.
    pub authority: Authority,
}

/// The resolved overview of one entity: its identity and the winning value
/// for every dimension any fact claimed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResolvedEntity {
    /// `dimension name → winning value + provenance`.
    pub dimensions: HashMap<String, ResolvedDimension>,
}

impl ResolvedEntity {
    /// The resolved value for `dimension`, if any fact claimed it.
    pub fn value(&self, dimension: &str) -> Option<&serde_json::Value> {
        self.dimensions.get(dimension).map(|d| &d.value)
    }
}

/// Resolve a set of facts into one best-value-per-dimension overview.
///
/// Facts about different entities are *not* separated here — the caller is
/// expected to pass the facts for a single entity (as
/// [`FactStore::facts_for`](dkg_facts::FactStore::facts_for) returns). Order
/// of `facts` does not matter; the winner is determined by confidence then
/// recency, not position.
///
/// # Examples
///
/// ```
/// use dkg_entity_fact_resolver::resolve;
/// use dkg_facts::{Authority, Fact, SourceType, Urn};
/// use chrono::{TimeZone, Utc};
/// use uuid::Uuid;
///
/// fn fact(dim: &str, val: &str, conf: f32) -> Fact {
///     Fact {
///         id: Uuid::nil(),
///         entity: Urn::new("businesses", "acme").unwrap(),
///         dimension: dim.into(),
///         value: serde_json::json!(val),
///         authority: Authority {
///             source_type: SourceType::WebSearch,
///             source_url: None,
///             model: None,
///             confidence: conf,
///         },
///         observed_at: Utc.timestamp_opt(0, 0).unwrap(),
///         recorded_at: Utc.timestamp_opt(0, 0).unwrap(),
///     }
/// }
///
/// let facts = vec![fact("phone", "low", 0.3), fact("phone", "high", 0.9)];
/// let resolved = resolve(&facts);
/// assert_eq!(resolved.value("phone").unwrap(), &serde_json::json!("high"));
/// ```
pub fn resolve(facts: &[Fact]) -> ResolvedEntity {
    let mut best: HashMap<&str, &Fact> = HashMap::new();

    for fact in facts {
        match best.get(fact.dimension.as_str()) {
            Some(current) if !wins(fact, current) => {}
            _ => {
                best.insert(fact.dimension.as_str(), fact);
            }
        }
    }

    let dimensions = best
        .into_iter()
        .map(|(dim, fact)| {
            (
                dim.to_string(),
                ResolvedDimension {
                    value: fact.value.clone(),
                    authority: fact.authority.clone(),
                },
            )
        })
        .collect();

    ResolvedEntity { dimensions }
}

/// Does `candidate` beat the `current` winner for a dimension? Higher
/// confidence wins; on a confidence tie, the later observation wins. A
/// strict comparison both ways means the first fact seen holds the slot
/// against an exact tie, which keeps resolution deterministic regardless of
/// input order for genuinely-equal facts.
fn wins(candidate: &Fact, current: &Fact) -> bool {
    let cc = candidate.authority.confidence;
    let pc = current.authority.confidence;
    if cc != pc {
        cc > pc
    } else {
        candidate.observed_at > current.observed_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use dkg_facts::{SourceType, Urn};
    use uuid::Uuid;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn fact(dim: &str, val: &str, conf: f32, observed: i64) -> Fact {
        Fact {
            id: Uuid::new_v4(),
            entity: Urn::new("businesses", "acme").unwrap(),
            dimension: dim.into(),
            value: serde_json::json!(val),
            authority: Authority {
                source_type: SourceType::WebSearch,
                source_url: None,
                model: None,
                confidence: conf,
            },
            observed_at: at(observed),
            recorded_at: at(observed),
        }
    }

    #[test]
    fn empty_resolves_to_nothing() {
        assert!(resolve(&[]).dimensions.is_empty());
    }

    #[test]
    fn higher_confidence_wins_regardless_of_order() {
        let lo = fact("phone", "low", 0.3, 100);
        let hi = fact("phone", "high", 0.9, 50);

        let a = resolve(&[lo.clone(), hi.clone()]);
        let b = resolve(&[hi, lo]);

        assert_eq!(a.value("phone").unwrap(), &serde_json::json!("high"));
        assert_eq!(b.value("phone").unwrap(), &serde_json::json!("high"));
    }

    #[test]
    fn confidence_tie_breaks_on_recency() {
        let older = fact("phone", "old", 0.5, 100);
        let newer = fact("phone", "new", 0.5, 200);
        let resolved = resolve(&[older, newer]);
        assert_eq!(resolved.value("phone").unwrap(), &serde_json::json!("new"));
    }

    #[test]
    fn distinct_dimensions_are_independent() {
        // A low-confidence phone and a high-confidence industry both survive:
        // resolution is per-dimension, not per-fact.
        let facts = vec![
            fact("phone", "603-555-1234", 0.4, 100),
            fact("industry", "printing", 0.95, 50),
        ];
        let resolved = resolve(&facts);
        assert_eq!(resolved.dimensions.len(), 2);
        assert_eq!(
            resolved.value("phone").unwrap(),
            &serde_json::json!("603-555-1234")
        );
        assert_eq!(
            resolved.value("industry").unwrap(),
            &serde_json::json!("printing")
        );
    }

    #[test]
    fn winning_provenance_is_retained() {
        let mut manual = fact("email", "info@acme.example", 0.5, 100);
        manual.authority.source_type = SourceType::Manual;
        let web = fact("email", "stale@acme.example", 0.4, 200);

        let resolved = resolve(&[web, manual]);
        let dim = &resolved.dimensions["email"];
        assert_eq!(dim.value, serde_json::json!("info@acme.example"));
        assert_eq!(dim.authority.source_type, SourceType::Manual);
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
