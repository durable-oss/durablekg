//! Syncing a resolved entity into its graph table.
//!
//! [`sync_entity`] is the write-back path that closes the loop: gather an
//! entity's facts, resolve them to one best-value-per-dimension overview, and
//! upsert that row into the entity's feature-B table. Re-running it for the
//! same entity *replaces* the row rather than appending — every table is a
//! `ReplacingMergeTree` keyed by the system `id`, so the latest sync wins.
//!
//! The target table is the entity URN's namespace: a fact about
//! `urn:businesses:<...>` syncs into the `businesses` table.
//!
//! ## Typed entity URNs and the system id
//!
//! The `id` column is system-owned: it is a uuid that DurableKG mints, never a
//! value taken from the URN. An entity is *addressed* by a typed URN whose id
//! part is `<keytype>:<value>`:
//!
//! - `urn:businesses:id:<uuid>` — the physical uuid directly;
//! - `urn:businesses:slug:acme-roofing-co` — a declared alternate key (alias).
//!
//! On sync, the alias and any alternate-key facts are translated to the uuid
//! (minted on first sight, reused when an alias already maps to a row), and the
//! row is written under it. Aliases are a user-convenience lookup layer, not a
//! core identity — internally everything is the uuid.

use std::collections::BTreeSet;

use dkg_core::{Error, Result};
use dkg_facts::{Fact, FactStore, Urn};
use dkg_graph::{GraphStore, Record, Schema, UpsertItem};

use crate::record::{is_blank_alias, to_record};
use crate::resolve::resolve;

/// Split a typed entity-URN id part into `(keytype, value)` on the first `:`.
///
/// `"slug:acme-roofing-co"` → `("slug", "acme-roofing-co")`;
/// `"id:0f..."` → `("id", "0f...")`. An id with no `:` is treated as a bare
/// alternate-key-less reference and reported as keytype `id` with the whole
/// string as the value, so a caller can still address by raw uuid without the
/// `id:` prefix.
pub fn parse_typed_id(id: &str) -> (&str, &str) {
    match id.split_once(':') {
        Some((keytype, value)) => (keytype, value),
        None => (dkg_config::Table::ID_COLUMN, id),
    }
}

/// The outcome of a sync: the id written, the row, and how many facts produced
/// it.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncOutcome {
    /// The system `id` (uuid) the row was written under.
    pub id: String,
    /// The overview record written into the table (without `id`).
    pub record: Record,
    /// How many facts were resolved into it.
    pub fact_count: usize,
}

/// Resolve `entity`'s facts and upsert the overview row into its graph table.
///
/// The table is looked up in `schema` by the entity URN's namespace. The URN's
/// typed id (`<keytype>:<value>`) selects how the row is addressed:
///
/// - `keytype == "id"`: the value is the physical uuid; the row is written
///   under it directly (and any alternate-key facts are registered as aliases).
/// - otherwise: `keytype` must be a declared alternate key; the alias plus
///   every alternate-key value present in the resolved row are translated to a
///   uuid (minted or reused), and the row is written under it.
///
/// Blank alias values (null / empty / whitespace) are never used or registered.
///
/// # Errors
///
/// - [`Error::NotFound`] if the entity's namespace is not a table in `schema`.
/// - [`Error::Invalid`] if the URN keytype is not `id` or a declared alternate
///   key, or on an alias merge conflict.
/// - Storage errors from the fact store or the graph store.
pub async fn sync_entity(
    facts: &dyn FactStore,
    graph: &dyn GraphStore,
    schema: &Schema,
    entity: &Urn,
) -> Result<SyncOutcome> {
    let prepared = prepare(facts, graph, schema, entity).await?;
    let table_name = schema.table(entity.namespace())?.name.clone();

    let id = match &prepared.item.id {
        // Address by physical uuid: write under it, then register any aliases.
        Some(value) => {
            graph
                .upsert_by_aliases_with_id(
                    schema,
                    &table_name,
                    &prepared.item.record,
                    value,
                    &prepared.item.aliases,
                )
                .await?
        }
        None => {
            graph
                .upsert_by_aliases(schema, &table_name, &prepared.item.record, &prepared.item.aliases)
                .await?
        }
    };

    Ok(SyncOutcome {
        id,
        record: prepared.item.record,
        fact_count: prepared.fact_count,
    })
}

/// How many entities one batched write covers.
///
/// Chunking bounds the size of any single generated `INSERT` and lets the
/// first chunk's rows land while later chunks are still resolving.
pub const SYNC_CHUNK: usize = 500;

/// How many chunks are in flight at once during a table-wide sync.
///
/// Each chunk is an independent read-resolve-write pipeline, so overlapping
/// them keeps ClickHouse busy instead of idle between statements.
///
/// Kept modest for a different reason than it once was: a chunk's fact read is
/// now a single pass over the table's fact collection
/// ([`prepare_batch`]), so concurrent chunks re-scan the same collection
/// rather than dividing it. Raising this trades redundant read work for write
/// overlap; a table-wide sync would be better served by one pass feeding many
/// writes, which is a larger change than this constant.
pub const SYNC_CHUNK_CONCURRENCY: usize = 4;

/// The result of syncing a whole table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableSyncSummary {
    /// The table that was synced.
    pub table: String,
    /// Entities whose overview row was written.
    pub synced: usize,
    /// Entities that failed, with the URN and the error text.
    pub failures: Vec<(String, String)>,
}

/// Sync every entity that has facts in `table`, in parallel chunks.
///
/// Enumerates the table's fact collection, then runs the entities through
/// [`sync_entities`] in chunks of [`SYNC_CHUNK`], with
/// [`SYNC_CHUNK_CONCURRENCY`] chunks in flight. This is the reconciliation
/// path: rebuilding overview rows that drifted, or filling them in after a
/// backlog of facts arrived without a sync.
///
/// Every entity is attempted; failures are collected rather than aborting the
/// run, so one bad entity cannot stop a table-wide rebuild.
///
/// # Errors
///
/// Returns an error only if the table is not in `schema` or the entity
/// enumeration itself fails. Per-entity failures come back in the summary.
pub async fn sync_table(
    facts: &dyn FactStore,
    graph: &dyn GraphStore,
    schema: &Schema,
    table: &str,
) -> Result<TableSyncSummary> {
    use futures::stream::{self, StreamExt};

    // Validate the table up front so a typo fails fast rather than after a
    // full enumeration.
    let table_name = schema.table(table)?.name.clone();
    let entities = facts.entities_in(&table_name).await?;

    let chunks: Vec<Vec<Urn>> = entities.chunks(SYNC_CHUNK).map(<[Urn]>::to_vec).collect();

    let results: Vec<Vec<(String, Result<SyncOutcome>)>> = stream::iter(chunks)
        .map(|chunk| async move {
            let outcomes = sync_entities(facts, graph, schema, &chunk).await;
            chunk
                .into_iter()
                .zip(outcomes)
                .map(|(urn, outcome)| (urn.to_string(), outcome))
                .collect::<Vec<_>>()
        })
        .buffer_unordered(SYNC_CHUNK_CONCURRENCY)
        .collect()
        .await;

    let mut summary = TableSyncSummary {
        table: table_name,
        ..Default::default()
    };
    for (urn, outcome) in results.into_iter().flatten() {
        match outcome {
            Ok(_) => summary.synced += 1,
            Err(e) => summary.failures.push((urn, e.to_string())),
        }
    }
    Ok(summary)
}

/// An entity resolved and ready to write: the upsert item plus how many facts
/// produced it.
struct Prepared {
    item: UpsertItem,
    fact_count: usize,
}

/// An entity's URN split into the parts the sync path needs, validated
/// against its table.
struct Addressed {
    entity: Urn,
    keytype: String,
    value: String,
    is_id: bool,
    table: String,
}

/// Validate an entity URN against its table and split out its typed id.
///
/// Factored out of [`prepare`] so the batch path can validate a whole set up
/// front — the per-entity errors are identical either way.
fn address(schema: &Schema, entity: &Urn) -> Result<Addressed> {
    let table = schema.table(entity.namespace())?;
    let (keytype, value) = parse_typed_id(entity.id());

    let is_id = keytype == dkg_config::Table::ID_COLUMN;
    if !is_id && !table.alternate_keys.iter().any(|k| k == keytype) {
        return Err(Error::Invalid(format!(
            "entity `{entity}`: `{keytype}` is not `id` or a declared alternate key of table `{}`",
            table.name
        )));
    }

    Ok(Addressed {
        entity: entity.clone(),
        keytype: keytype.to_string(),
        value: value.to_string(),
        is_id,
        table: table.name.clone(),
    })
}

/// Build the row and alias set from an entity's already-gathered facts.
///
/// The pure tail of [`prepare`]: everything after the reads. Shared by the
/// single-entity and batch paths so they cannot drift on how a record is
/// built or when an empty overview is refused.
fn build(schema: &Schema, addressed: &Addressed, entity_facts: &[Fact]) -> Result<Prepared> {
    let table = schema.table(addressed.entity.namespace())?;
    let resolved = resolve(entity_facts);
    let record = to_record(
        &resolved,
        table,
        Some((addressed.keytype.as_str(), addressed.value.as_str())),
    );

    // Collect the alias pairs that identify this entity: the URN alias (when it
    // is an alternate key) plus every alternate-key column present in the row.
    // Skip blank values and duplicates throughout.
    let mut aliases: Vec<(String, String)> = Vec::new();
    if !addressed.is_id && !addressed.value.trim().is_empty() {
        aliases.push((addressed.keytype.clone(), addressed.value.clone()));
    }
    for key in &table.alternate_keys {
        let Some(val) = record.get(key) else { continue };
        if is_blank_alias(val) {
            continue;
        }
        let Some(s) = val.as_str() else { continue };
        if !aliases.iter().any(|(kt, kv)| kt == key && kv == s) {
            aliases.push((key.clone(), s.to_string()));
        }
    }

    // Refuse to write an overview built from nothing. A sync that found no
    // facts would otherwise write a row holding only the system `id` — and
    // because tables are `ReplacingMergeTree` stamped at insert, that empty
    // row is newer than the real one and silently replaces it. Nothing about
    // "this entity has no facts" justifies destroying what is already there.
    if entity_facts.is_empty() {
        return Err(Error::NotFound(format!(
            "entity `{}` has no facts; refusing to write an empty overview row",
            addressed.entity
        )));
    }

    Ok(Prepared {
        fact_count: entity_facts.len(),
        item: UpsertItem {
            record,
            aliases,
            id: addressed.is_id.then(|| addressed.value.clone()),
        },
    })
}

/// Collect every fact about `entity`, across all the URN forms it is known by.
///
/// Facts are indexed under the *exact* URN string they were recorded with, so
/// an entity recorded under `urn:businesses:slug:acme` and later addressed as
/// `urn:businesses:id:<uuid>` has facts under one key and a sync looking at the
/// other. Reading only the requested form yields an empty fact set, which
/// resolves to an empty record — and, since every table is a
/// `ReplacingMergeTree` stamped with a fresh version, that empty row *wins*
/// and blanks the entity's real overview.
///
/// So: resolve the incoming URN to its system uuid, ask the alias table what
/// other names point at that uuid, and union the facts from every form
/// (including `id:<uuid>` itself). Facts are deduplicated by document id, since
/// one bundle reached through two aliases is still one bundle.
///
/// An entity with no registered aliases costs one extra query and behaves
/// exactly as before.
async fn gather_facts(
    facts: &dyn FactStore,
    graph: &dyn GraphStore,
    schema: &Schema,
    entity: &Urn,
    keytype: &str,
    value: &str,
    is_id: bool,
) -> Result<Vec<Fact>> {
    let namespace = entity.namespace();

    // The system uuid for this entity, if it already has one. Addressing by
    // `id:` gives it directly; an alias needs a lookup. A miss means the
    // entity is new, so the requested form is the only place facts can be.
    let entity_id = if is_id {
        Some(value.to_string())
    } else {
        graph
            .resolve_alias(schema, namespace, keytype, value)
            .await?
    };

    let Some(entity_id) = entity_id else {
        return facts.facts_for(entity).await;
    };

    // Every URN form this entity answers to: the canonical `id:` form, each
    // registered alias, and the form actually requested (which may not be
    // registered yet). Deduplicated by string, since `Urn` is not `Ord`.
    let mut forms: Vec<Urn> = vec![entity.clone()];
    let mut seen_forms: BTreeSet<String> = BTreeSet::new();
    seen_forms.insert(entity.to_string());

    let push = |urn: Urn, forms: &mut Vec<Urn>, seen: &mut BTreeSet<String>| {
        if seen.insert(urn.to_string()) {
            forms.push(urn);
        }
    };

    if let Ok(canonical) = Urn::new(
        namespace,
        format!("{}:{entity_id}", dkg_config::Table::ID_COLUMN),
    ) {
        push(canonical, &mut forms, &mut seen_forms);
    }
    for (alias_type, alias_value) in graph
        .aliases_for_entity(schema, namespace, &entity_id)
        .await?
    {
        if let Ok(urn) = Urn::new(namespace, format!("{alias_type}:{alias_value}")) {
            push(urn, &mut forms, &mut seen_forms);
        }
    }

    // Union the facts. A bundle's facts all share its document id, so the
    // identity of one stored claim is (document id, dimension) — dedupe on
    // that, or two aliases reaching the same bundle would double every fact
    // in it.
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut out = Vec::new();
    for form in forms {
        for fact in facts.facts_for(&form).await? {
            if seen.insert((fact.id.to_string(), fact.dimension.clone())) {
                out.push(fact);
            }
        }
    }
    Ok(out)
}

/// Read `entity`'s facts, resolve them, and build the row and alias set.
///
/// This is everything [`sync_entity`] does *except* the graph write, factored
/// out so the batch path can prepare many entities before writing them
/// together.
async fn prepare(
    facts: &dyn FactStore,
    graph: &dyn GraphStore,
    schema: &Schema,
    entity: &Urn,
) -> Result<Prepared> {
    let addressed = address(schema, entity)?;
    let entity_facts = gather_facts(
        facts,
        graph,
        schema,
        entity,
        &addressed.keytype,
        &addressed.value,
        addressed.is_id,
    )
    .await?;
    build(schema, &addressed, &entity_facts)
}

/// Prepare a whole batch of entities with a bounded number of round trips.
///
/// The bulk equivalent of calling [`prepare`] per entity, and the reason a
/// batch sync scales. Preparing one entity costs an alias lookup, a reverse
/// alias lookup, and a fact read per URN form it answers to — and a fact read
/// scans its namespace's whole fact collection. Done per entity that is
/// O(entities x collection); done here it is three bulk phases whose cost is
/// independent of the batch size:
///
/// 1. one `resolve_aliases` per table, mapping every non-`id` URN to its uuid;
/// 2. one `aliases_for_entities` per table, giving every form each entity
///    answers to;
/// 3. one `facts_for_many` per namespace, reading each fact collection once
///    and bucketing by URN.
///
/// Results come back in input order, one per entity, with per-entity errors in
/// place — identical in shape to mapping [`prepare`] over the slice.
async fn prepare_batch(
    facts: &dyn FactStore,
    graph: &dyn GraphStore,
    schema: &Schema,
    entities: &[Urn],
) -> Vec<Result<Prepared>> {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    // Phase 0: validate and split every URN. Invalid entities drop out here
    // and keep their slot, so a bad URN cannot fail its neighbours.
    let addressed: Vec<Result<Addressed>> = entities.iter().map(|e| address(schema, e)).collect();

    // Phase 1: resolve every alias-addressed entity to its uuid, one query
    // per table. An `id:`-addressed entity already carries its uuid.
    let mut alias_lookups: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
    for a in addressed.iter().flatten() {
        if !a.is_id {
            alias_lookups
                .entry(a.table.clone())
                .or_default()
                .insert((a.keytype.clone(), a.value.clone()));
        }
    }
    let mut resolved_aliases: HashMap<(String, String), String> = HashMap::new();
    for (table, pairs) in alias_lookups {
        let pairs: Vec<(String, String)> = pairs.into_iter().collect();
        // A lookup failure is a storage fault, not a per-entity one; treat it
        // as "no alias resolved" and let each entity fall back to reading the
        // form it was addressed by, exactly as a fresh entity does.
        if let Ok(found) = graph.resolve_aliases(schema, &table, &pairs).await {
            resolved_aliases.extend(found);
        }
    }

    // The uuid each entity resolves to, by input index. Absent means the
    // entity is new: the requested form is the only place its facts can be.
    let mut entity_ids: Vec<Option<String>> = Vec::with_capacity(addressed.len());
    for a in &addressed {
        entity_ids.push(match a {
            Ok(a) if a.is_id => Some(a.value.clone()),
            Ok(a) => resolved_aliases
                .get(&(a.keytype.clone(), a.value.clone()))
                .cloned(),
            Err(_) => None,
        });
    }

    // Phase 2: every alias registered against those uuids, one query per
    // table — the reverse lookup that finds the other URN forms an entity's
    // facts may have been recorded under.
    let mut reverse_lookups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (a, id) in addressed.iter().zip(&entity_ids) {
        if let (Ok(a), Some(id)) = (a, id) {
            reverse_lookups
                .entry(a.table.clone())
                .or_default()
                .insert(id.clone());
        }
    }
    let mut aliases_by_entity: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (table, ids) in reverse_lookups {
        let ids: Vec<String> = ids.into_iter().collect();
        // As above: a failed reverse lookup degrades to single-form gathering
        // rather than failing the batch.
        if let Ok(found) = graph.aliases_for_entities(schema, &table, &ids).await {
            aliases_by_entity.extend(found);
        }
    }

    // Phase 3: the URN forms to read for each entity, then one bulk fact read
    // covering every form of every entity.
    let forms_per_entity: Vec<Vec<Urn>> = addressed
        .iter()
        .zip(&entity_ids)
        .map(|(a, id)| match a {
            Err(_) => Vec::new(),
            Ok(a) => entity_forms(a, id.as_deref(), &aliases_by_entity),
        })
        .collect();

    let all_forms: Vec<Urn> = {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        forms_per_entity
            .iter()
            .flatten()
            .filter(|u| seen.insert(u.to_string()))
            .cloned()
            .collect()
    };
    let facts_by_urn = facts
        .facts_for_many(&all_forms)
        .await
        .unwrap_or_else(|_| HashMap::new());

    // Assemble: union each entity's forms out of the pre-fetched map and
    // build its row. No further I/O.
    addressed
        .into_iter()
        .zip(forms_per_entity)
        .map(|(a, forms)| {
            let a = a?;
            // A bundle's facts all share its document id, so the identity of
            // one stored claim is (document id, dimension) — dedupe on that,
            // or two aliases reaching the same bundle would double every fact
            // in it.
            let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
            let mut entity_facts = Vec::new();
            for form in &forms {
                let Some(found) = facts_by_urn.get(&form.to_string()) else {
                    continue;
                };
                for fact in found {
                    if seen.insert((fact.id.to_string(), fact.dimension.clone())) {
                        entity_facts.push(fact.clone());
                    }
                }
            }
            build(schema, &a, &entity_facts)
        })
        .collect()
}

/// Every URN form an entity answers to: the form it was addressed by, its
/// canonical `id:` form, and each registered alias.
///
/// The batch counterpart of the form-collection in [`gather_facts`]. An
/// entity with no resolved uuid is new, so the requested form is the only one.
fn entity_forms(
    addressed: &Addressed,
    entity_id: Option<&str>,
    aliases_by_entity: &std::collections::HashMap<String, Vec<(String, String)>>,
) -> Vec<Urn> {
    let namespace = addressed.entity.namespace();
    let mut forms = vec![addressed.entity.clone()];
    let mut seen: BTreeSet<String> = BTreeSet::new();
    seen.insert(addressed.entity.to_string());

    let Some(entity_id) = entity_id else {
        return forms;
    };

    let mut push = |urn: Urn, forms: &mut Vec<Urn>| {
        if seen.insert(urn.to_string()) {
            forms.push(urn);
        }
    };

    if let Ok(canonical) = Urn::new(
        namespace,
        format!("{}:{entity_id}", dkg_config::Table::ID_COLUMN),
    ) {
        push(canonical, &mut forms);
    }
    if let Some(aliases) = aliases_by_entity.get(entity_id) {
        for (alias_type, alias_value) in aliases {
            if let Ok(urn) = Urn::new(namespace, format!("{alias_type}:{alias_value}")) {
                push(urn, &mut forms);
            }
        }
    }
    forms
}

/// Resolve and sync many entities, batching the graph writes.
///
/// Where [`sync_entity`] costs a round trip per alias plus a single-row
/// `INSERT` for each entity, this reads facts concurrently and then writes
/// each table's rows in one `INSERT`, with one alias lookup and one alias
/// `INSERT` for the whole group. Single-row inserts are ClickHouse's worst
/// case, so for bulk sync the difference is large.
///
/// Entities are grouped by target table (their URN namespace), since a batched
/// `INSERT` writes one table. Results come back in input order.
///
/// Every entity is attempted: an entity that fails to prepare yields its error
/// in place, leaving the rest of the batch to sync. If a batched write fails,
/// that table's entities are retried individually so a single bad record does
/// not fail its whole group — the error is attributed to the entity that
/// caused it.
///
/// # Errors
///
/// Individual entity failures are returned in the result vector, not as an
/// overall error. A [`Error::NotFound`] for an entity whose namespace is not a
/// table in `schema` is reported per entity in the same way.
pub async fn sync_entities(
    facts: &dyn FactStore,
    graph: &dyn GraphStore,
    schema: &Schema,
    entities: &[Urn],
) -> Vec<Result<SyncOutcome>> {
    let prepared: Vec<Result<Prepared>> = prepare_batch(facts, graph, schema, entities).await;

    // Outcomes are slotted back by input index. Preparation failures are
    // final; successes are filled in once their write assigns an id.
    let mut outcomes: Vec<Option<Result<SyncOutcome>>> = Vec::with_capacity(prepared.len());
    let mut by_table: std::collections::BTreeMap<String, Vec<(usize, Prepared)>> =
        std::collections::BTreeMap::new();

    for (index, result) in prepared.into_iter().enumerate() {
        match result {
            Ok(p) => {
                // `prepare` resolved this namespace, so the lookup holds.
                match schema.table(entities[index].namespace()) {
                    Ok(table) => {
                        by_table
                            .entry(table.name.clone())
                            .or_default()
                            .push((index, p));
                        outcomes.push(None);
                    }
                    Err(e) => outcomes.push(Some(Err(e))),
                }
            }
            Err(e) => outcomes.push(Some(Err(e))),
        }
    }

    // One batched write per target table.
    for (table_name, group) in by_table {
        let items: Vec<UpsertItem> = group.iter().map(|(_, p)| p.item.clone()).collect();

        match graph.upsert_batch(schema, &table_name, &items).await {
            Ok(ids) if ids.len() == group.len() => {
                for ((index, p), id) in group.into_iter().zip(ids) {
                    outcomes[index] = Some(Ok(SyncOutcome {
                        id,
                        record: p.item.record,
                        fact_count: p.fact_count,
                    }));
                }
            }
            // A short id list would silently mis-attribute ids to entities,
            // so treat it as a failed batch and fall back.
            Ok(_) | Err(_) => {
                // The batch failed as a unit — one bad record fails the whole
                // statement. Retry each entity on its own so the failure is
                // attributed to the entity that caused it and the rest of the
                // group still syncs.
                for (index, p) in group {
                    let result = match &p.item.id {
                        Some(value) => {
                            graph
                                .upsert_by_aliases_with_id(
                                    schema,
                                    &table_name,
                                    &p.item.record,
                                    value,
                                    &p.item.aliases,
                                )
                                .await
                        }
                        None => {
                            graph
                                .upsert_by_aliases(
                                    schema,
                                    &table_name,
                                    &p.item.record,
                                    &p.item.aliases,
                                )
                                .await
                        }
                    };
                    outcomes[index] = Some(result.map(|id| SyncOutcome {
                        id,
                        record: p.item.record,
                        fact_count: p.fact_count,
                    }));
                }
            }
        }
    }

    outcomes
        .into_iter()
        .map(|o| o.expect("every entity has an outcome"))
        .collect()
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
