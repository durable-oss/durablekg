//! Tests for the batched sync path ([`sync_entities`]).
//!
//! The graph store is faked in memory so the tests can assert on *how many*
//! statements the batch path issues, which is the property the batching
//! exists for and the one a live-ClickHouse test would obscure.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use dkg_config::{Column, ColumnType, Table};
use dkg_core::Result;
use dkg_entity_fact_resolver::sync_entities;
use dkg_facts::{Authority, Fact, FactStore, NewBundle, NewFact, SourceType, Urn};
use dkg_graph::{Filter, GraphStore, OrderBy, Record, Schema, UpsertItem};
use serde_json::json;
use uuid::Uuid;

/// A fact store holding pre-built facts, keyed by entity URN.
struct FakeFacts {
    facts: Vec<Fact>,
}

#[async_trait]
impl FactStore for FakeFacts {
    async fn record(&self, _: NewFact) -> Result<Fact> {
        unimplemented!("not used by these tests")
    }
    async fn record_bundle(&self, _: NewBundle) -> Result<Vec<Fact>> {
        unimplemented!("not used by these tests")
    }
    async fn get(&self, _: Uuid) -> Result<Fact> {
        unimplemented!("not used by these tests")
    }
    async fn facts_for(&self, entity: &Urn) -> Result<Vec<Fact>> {
        Ok(self
            .facts
            .iter()
            .filter(|f| &f.entity == entity)
            .cloned()
            .collect())
    }
    async fn facts_on(&self, entity: &Urn, dimension: &str) -> Result<Vec<Fact>> {
        Ok(self
            .facts_for(entity)
            .await?
            .into_iter()
            .filter(|f| f.dimension == dimension)
            .collect())
    }
    async fn entities_in(&self, table: &str) -> Result<Vec<Urn>> {
        let mut seen = std::collections::BTreeSet::new();
        Ok(self
            .facts
            .iter()
            .filter(|f| f.entity.namespace() == table)
            .filter(|f| seen.insert(f.entity.to_string()))
            .map(|f| f.entity.clone())
            .collect())
    }
}

/// Records every call so a test can assert on statement counts, and keeps an
/// alias→id map so alias resolution behaves like the real store.
#[derive(Default)]
struct FakeGraphInner {
    /// Rows written, as (table, id, record).
    rows: Vec<(String, String, Record)>,
    /// Registered aliases: (alias_type, alias_value) → entity id.
    aliases: std::collections::HashMap<(String, String), String>,
    /// How many `upsert_batch` calls were made.
    batch_calls: usize,
    /// How many single-entity upserts were made (the fallback path).
    single_calls: usize,
    /// When set, `upsert_batch` fails, forcing the per-entity fallback.
    fail_batch: bool,
}

#[derive(Default, Clone)]
struct FakeGraph(Arc<Mutex<FakeGraphInner>>);

impl FakeGraph {
    fn inner(&self) -> std::sync::MutexGuard<'_, FakeGraphInner> {
        self.0.lock().unwrap()
    }

    fn write(&self, table: &str, record: &Record, id: &str, aliases: &[(String, String)]) {
        let mut g = self.inner();
        g.rows
            .push((table.to_string(), id.to_string(), record.clone()));
        for alias in aliases {
            g.aliases.insert(alias.clone(), id.to_string());
        }
    }
}

#[async_trait]
impl GraphStore for FakeGraph {
    async fn reconcile(&self, _: &Schema) -> Result<()> {
        Ok(())
    }
    async fn insert(&self, _: &Schema, _: &str, _: &[Record]) -> Result<()> {
        Ok(())
    }
    async fn query(
        &self,
        _: &Schema,
        _: &str,
        _: &[Filter],
        _: Option<&OrderBy>,
        _: usize,
        _: usize,
    ) -> Result<Vec<Record>> {
        Ok(Vec::new())
    }
    async fn count(&self, _: &Schema, _: &str, _: &[Filter]) -> Result<u64> {
        Ok(0)
    }
    async fn related(
        &self,
        _: &Schema,
        _: &str,
        _: &serde_json::Value,
        _: &str,
        _: usize,
    ) -> Result<Vec<Record>> {
        Ok(Vec::new())
    }
    async fn resolve_alias(
        &self,
        _: &Schema,
        _: &str,
        alias_type: &str,
        alias_value: &str,
    ) -> Result<Option<String>> {
        Ok(self
            .inner()
            .aliases
            .get(&(alias_type.to_string(), alias_value.to_string()))
            .cloned())
    }
    async fn aliases_for_entity(
        &self,
        _: &Schema,
        _: &str,
        entity_id: &str,
    ) -> Result<Vec<(String, String)>> {
        Ok(self
            .inner()
            .aliases
            .iter()
            .filter(|(_, id)| id.as_str() == entity_id)
            .map(|(pair, _)| pair.clone())
            .collect())
    }
    async fn upsert_by_aliases(
        &self,
        _: &Schema,
        table: &str,
        record: &Record,
        aliases: &[(String, String)],
    ) -> Result<String> {
        self.inner().single_calls += 1;
        let existing = aliases
            .iter()
            .find_map(|a| self.inner().aliases.get(a).cloned());
        let id = existing.unwrap_or_else(|| Uuid::new_v4().to_string());
        self.write(table, record, &id, aliases);
        Ok(id)
    }
    async fn upsert_by_aliases_with_id(
        &self,
        _: &Schema,
        table: &str,
        record: &Record,
        id: &str,
        aliases: &[(String, String)],
    ) -> Result<String> {
        self.inner().single_calls += 1;
        self.write(table, record, id, aliases);
        Ok(id.to_string())
    }
    async fn upsert_batch(
        &self,
        schema: &Schema,
        table: &str,
        items: &[UpsertItem],
    ) -> Result<Vec<String>> {
        self.inner().batch_calls += 1;
        if self.inner().fail_batch {
            return Err(dkg_core::Error::Storage("batch rejected".into()));
        }
        // Mirror the real batch semantics: resolve against existing aliases
        // *and* against earlier items in the same batch.
        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            let id = match &item.id {
                Some(id) => id.clone(),
                None => item
                    .aliases
                    .iter()
                    .find_map(|a| self.inner().aliases.get(a).cloned())
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
            };
            self.write(table, &item.record, &id, &item.aliases);
            ids.push(id);
        }
        let _ = schema;
        Ok(ids)
    }
}

fn schema() -> Schema {
    Schema::load(
        vec![Table {
            name: "businesses".into(),
            columns: vec![
                Column {
                    name: "slug".into(),
                    column_type: ColumnType::String,
                    nullable: true,
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
        }],
        Vec::new(),
    )
    .expect("schema")
}

fn fact(entity: &Urn, dimension: &str, value: serde_json::Value) -> Fact {
    let now = Utc::now();
    Fact {
        id: Uuid::new_v4(),
        entity: entity.clone(),
        dimension: dimension.into(),
        value,
        authority: Authority {
            source_type: SourceType::Manual,
            source_url: None,
            model: None,
            confidence: 0.9,
        },
        observed_at: now,
        recorded_at: now,
    }
}

fn entity(slug: &str) -> Urn {
    Urn::new("businesses", format!("slug:{slug}")).unwrap()
}

#[tokio::test]
async fn a_batch_of_entities_costs_one_write_call() {
    let entities: Vec<Urn> = (0..25).map(|i| entity(&format!("acme-{i}"))).collect();
    let facts = FakeFacts {
        facts: entities
            .iter()
            .map(|e| fact(e, "phone", json!("555-0100")))
            .collect(),
    };
    let graph = FakeGraph::default();
    let schema = schema();

    let results = sync_entities(&facts, &graph, &schema, &entities).await;

    assert_eq!(results.len(), 25);
    assert!(results.iter().all(|r| r.is_ok()), "every entity synced");
    // One batched write for the whole group, not one per entity.
    assert_eq!(graph.inner().batch_calls, 1);
    assert_eq!(graph.inner().single_calls, 0);
    assert_eq!(graph.inner().rows.len(), 25);
}

#[tokio::test]
async fn results_come_back_in_input_order() {
    let entities: Vec<Urn> = (0..5).map(|i| entity(&format!("ord-{i}"))).collect();
    let facts = FakeFacts {
        facts: entities
            .iter()
            .enumerate()
            .map(|(i, e)| fact(e, "phone", json!(format!("555-{i:04}"))))
            .collect(),
    };
    let graph = FakeGraph::default();

    let results = sync_entities(&facts, &graph, &schema(), &entities).await;

    for (i, result) in results.iter().enumerate() {
        let outcome = result.as_ref().expect("synced");
        assert_eq!(
            outcome.record.get("phone").and_then(|v| v.as_str()),
            Some(format!("555-{i:04}").as_str()),
            "result {i} is out of order"
        );
    }
}

#[tokio::test]
async fn entities_sharing_an_alias_resolve_to_one_id_within_a_batch() {
    // Two URNs that resolve to the same slug alias must land on the same
    // system id, exactly as two sequential `sync_entity` calls would.
    let a = entity("shared");
    let b = entity("shared");
    let facts = FakeFacts {
        facts: vec![
            fact(&a, "phone", json!("555-0100")),
            fact(&b, "phone", json!("555-0200")),
        ],
    };
    let graph = FakeGraph::default();

    let results = sync_entities(&facts, &graph, &schema(), &[a, b]).await;

    let first = results[0].as_ref().expect("first synced");
    let second = results[1].as_ref().expect("second synced");
    assert_eq!(
        first.id, second.id,
        "entities sharing an alias must share an id"
    );
}

#[tokio::test]
async fn a_failed_batch_falls_back_to_per_entity_writes() {
    let entities: Vec<Urn> = (0..4).map(|i| entity(&format!("fb-{i}"))).collect();
    let facts = FakeFacts {
        facts: entities
            .iter()
            .map(|e| fact(e, "phone", json!("555-0100")))
            .collect(),
    };
    let graph = FakeGraph::default();
    graph.inner().fail_batch = true;

    let results = sync_entities(&facts, &graph, &schema(), &entities).await;

    // The batch was attempted once, then every entity was retried alone, so
    // one bad record cannot sink its whole group.
    assert_eq!(graph.inner().batch_calls, 1);
    assert_eq!(graph.inner().single_calls, 4);
    assert!(
        results.iter().all(|r| r.is_ok()),
        "fallback should still sync every entity"
    );
}

#[tokio::test]
async fn a_bad_entity_fails_alone() {
    // `nope` is not a table in the schema, so that entity fails to prepare
    // while the rest of the batch syncs.
    let good = entity("fine");
    let bad = Urn::new("nope", "slug:missing").unwrap();
    let facts = FakeFacts {
        facts: vec![fact(&good, "phone", json!("555-0100"))],
    };
    let graph = FakeGraph::default();

    let results = sync_entities(&facts, &graph, &schema(), &[good, bad]).await;

    assert!(results[0].is_ok(), "the valid entity should sync");
    assert!(results[1].is_err(), "the unknown table should fail");
    assert_eq!(graph.inner().rows.len(), 1);
}

/// Facts are indexed under the exact URN they were recorded with. Syncing the
/// *same* entity through a different alias must still find them — otherwise
/// the sync resolves an empty record and, because tables are
/// `ReplacingMergeTree` stamped at insert, that empty row wins and blanks the
/// entity's real overview.
#[tokio::test]
async fn syncing_via_a_second_alias_still_finds_the_facts() {
    let by_slug = entity("acme");
    let facts = FakeFacts {
        facts: vec![fact(&by_slug, "phone", json!("555-0100"))],
    };
    let graph = FakeGraph::default();
    let schema = schema();

    // First sync registers the slug alias and mints an id.
    let first = sync_entities(&facts, &graph, &schema, &[by_slug]).await;
    let id = first[0].as_ref().expect("first sync").id.clone();

    // Now address the same entity by its physical uuid. Its facts live under
    // the slug URN, so a naive lookup finds nothing.
    let by_id = Urn::new("businesses", format!("id:{id}")).unwrap();
    let second = sync_entities(&facts, &graph, &schema, &[by_id]).await;

    let outcome = second[0]
        .as_ref()
        .expect("syncing by uuid should find the facts recorded under the slug");
    assert_eq!(
        outcome.fact_count, 1,
        "facts recorded under one alias must be visible through another"
    );
    assert_eq!(
        outcome.record.get("phone").and_then(|v| v.as_str()),
        Some("555-0100"),
        "the overview must keep its real values, not be blanked"
    );
}

/// An entity with no facts at all must not write a row. Doing so would stamp
/// a newer, empty version over whatever is already there.
#[tokio::test]
async fn an_entity_with_no_facts_is_not_written() {
    let ghost = entity("ghost");
    let facts = FakeFacts { facts: Vec::new() };
    let graph = FakeGraph::default();

    let results = sync_entities(&facts, &graph, &schema(), &[ghost]).await;

    assert!(
        results[0].is_err(),
        "a factless entity must not produce a row"
    );
    assert_eq!(
        graph.inner().rows.len(),
        0,
        "nothing should have been written"
    );
}

/// The queue's whole point: mark entities dirty, and a pass writes their rows
/// without the caller listing them.
#[tokio::test]
async fn a_sync_pass_writes_every_dirty_entity() {
    use dkg_entity_fact_resolver::SyncQueue;

    let entities: Vec<Urn> = (0..3).map(|i| entity(&format!("q{i}"))).collect();
    let facts = FakeFacts {
        facts: entities
            .iter()
            .map(|e| fact(e, "phone", json!("555-0100")))
            .collect(),
    };
    let graph = FakeGraph::default();

    let queue = SyncQueue::new();
    queue.mark_all_dirty(entities.clone());
    // The same entity marked twice must not sync twice.
    queue.mark_dirty(entities[0].clone());
    assert_eq!(queue.pending(), 3);

    let pass = queue.sync_once(&facts, &graph, &schema()).await;

    assert_eq!(pass.synced, 3);
    assert_eq!(pass.failed, 0);
    assert_eq!(queue.pending(), 0, "a clean pass drains the set");
    assert_eq!(graph.inner().rows.len(), 3);
}

/// A failed entity goes back on the dirty set so the next pass retries it,
/// rather than being silently dropped.
#[tokio::test]
async fn a_failed_entity_is_requeued() {
    use dkg_entity_fact_resolver::SyncQueue;

    // No facts recorded, so the sync refuses to write an empty overview.
    let facts = FakeFacts { facts: Vec::new() };
    let graph = FakeGraph::default();

    let queue = SyncQueue::new();
    queue.mark_dirty(entity("doomed"));

    let pass = queue.sync_once(&facts, &graph, &schema()).await;

    assert_eq!(pass.synced, 0);
    assert_eq!(pass.failed, 1);
    assert_eq!(queue.pending(), 1, "a failure must be retried, not dropped");
}

#[tokio::test]
async fn an_empty_batch_writes_nothing() {
    let facts = FakeFacts { facts: Vec::new() };
    let graph = FakeGraph::default();

    let results = sync_entities(&facts, &graph, &schema(), &[]).await;

    assert!(results.is_empty());
    assert_eq!(graph.inner().batch_calls, 0);
    assert_eq!(graph.inner().single_calls, 0);
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
