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

use dkg_core::{Error, Result};
use dkg_facts::{FactStore, Urn};
use dkg_graph::{GraphStore, Record, Schema};

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
    let table = schema.table(entity.namespace())?;
    let (keytype, value) = parse_typed_id(entity.id());

    // The keytype must be `id` or a declared alternate key.
    let is_id = keytype == dkg_config::Table::ID_COLUMN;
    if !is_id && !table.alternate_keys.iter().any(|k| k == keytype) {
        return Err(Error::Invalid(format!(
            "entity `{entity}`: `{keytype}` is not `id` or a declared alternate key of table `{}`",
            table.name
        )));
    }

    let entity_facts = facts.facts_for(entity).await?;
    let resolved = resolve(&entity_facts);
    let record = to_record(&resolved, table, Some((keytype, value)));

    // Collect the alias pairs that identify this entity: the URN alias (when it
    // is an alternate key) plus every alternate-key column present in the row.
    // Skip blank values and duplicates throughout.
    let mut aliases: Vec<(String, String)> = Vec::new();
    if !is_id && !value.trim().is_empty() {
        aliases.push((keytype.to_string(), value.to_string()));
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

    let id = if is_id {
        // Address by physical uuid: write under it, then register any aliases.
        graph
            .upsert_by_aliases_with_id(schema, &table.name, &record, value, &aliases)
            .await?
    } else {
        graph
            .upsert_by_aliases(schema, &table.name, &record, &aliases)
            .await?
    };

    Ok(SyncOutcome {
        id,
        record,
        fact_count: entity_facts.len(),
    })
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
