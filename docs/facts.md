# Facts & overview resolution

The relational schema (feature B) describes the *target shape* of an entity —
the `businesses` table and its columns. **Facts** are the layer beneath it:
where observations land before they are resolved into a row. The schema author
never authors facts and never edits the schema to enable them. Adding a column
to a table simply makes that column a dimension facts can target.

This design follows the prospect-research pattern of many timestamped,
source-stamped observations resolved into one best-value-per-field overview —
ported into DurableKG as two crates: `dkg-facts` (storage) and
`dkg-entity-fact-resolver` (policy).

## What a fact is

A fact is one authority-stamped claim about a single *dimension* of a single
entity:

> the `phone` of `urn:businesses:slug:acme-roofing-co` is `603-555-1234`, per a
> manual entry, confidence 1.0

```jsonc
{
  "entity": "urn:businesses:slug:acme-roofing-co",  // urn:<table>:<keytype>:<value>
  "dimension": "phone",               // a column name on that table
  "value": "603-555-1234",            // opaque JSON
  "authority": {
    "source_type": "manual",          // web_search | website_scrape |
                                      // directory_lookup | manual | inference
    "source_url": null,
    "model": null,
    "confidence": 1.0                 // 0.0..=1.0; higher wins on conflict
  },
  "observed_at": "2026-06-19T10:00:00Z"  // optional; defaults to record time
}
```

Facts are **append-only**. There is no update or delete. A correction is a
newer, higher-confidence fact that out-ranks the old one during resolution.

## Entities and the typed URN

An entity is addressed by a **typed URN** of the form
`urn:<table>:<keytype>:<value>`, where `<keytype>` is either `id` or one of the
table's declared alternate keys (see [relational.md](relational.md)). Examples:

```
urn:businesses:id:0d6c…-…           # the system-owned uuid
urn:businesses:slug:acme-roofing-co  # an alternate key
urn:businesses:domain:acmeroofing.example.com
```

The namespace (`businesses`) tells the resolver which table to project onto.
The keytype/value pair names the row by its system `id` or by an alias that
maps to it — DurableKG resolves the alias to the uuid (see
[Sync](#sync-write-the-overview-into-the-table) below). Unlike before, the URN
value is **never** written straight into an `id` column; `id` is system-owned.

The same entity may carry several aliases — every alternate-key column value
present in its row resolves to the same uuid. These are the `aliases` on a
recorded fact; any of them resolves to the same entity.

## Storage: facts ride on documents

`dkg-facts` has **no backend of its own**. Each fact is persisted as a document
(feature A) in a per-table collection named `facts:<table>`, where `<table>` is
the entity URN's namespace. The resolution-relevant fields are lifted into the
document's queryable metadata, so the three access patterns hit existing
indexes:

| Query | Mechanism |
| ----- | --------- |
| all facts for an entity | metadata filter on `entity` |
| all facts on one dimension | metadata filter on `entity` + `dimension` |
| alternate-id mappings | the URNs recorded with the entity's facts |

One collection per table means an entity filter never scans facts about
unrelated tables, and a table's facts can be listed or cleared on their own.

## Resolution: highest authority per dimension

`resolve(&[Fact]) -> ResolvedEntity` collapses the many facts about an entity
into one best value per dimension. The rule:

1. **Highest `confidence` wins.**
2. **Ties broken by latest `observed_at`.**

Resolution is **per dimension, not per fact** — a low-confidence `phone` and a
high-confidence `industry` both survive. The winning fact's `Authority` is kept
alongside each resolved value (`ResolvedDimension { value, authority }`), so the
overview can still say *where* each field came from. (The original prospect
system discarded that.)

`to_record(entity, resolved, &table)` then projects the resolved entity onto a
concrete table: it keeps only the dimensions that are real columns (dropping
the rest — the "accept any dimension" contract). The overview it produces
carries **no `id`** — `id` is system-owned and assigned by sync, not derived
from the URN. The result is a record ready to insert into the entity's
feature-B table.

## Sync: upsert the overview by alias

`sync_entity(facts, graph, schema, entity)` is the write-back path. It never
writes the URN value into `id`; instead it resolves the entity to a system uuid
by its aliases:

1. gather the entity's facts,
2. resolve them into an overview (which carries no `id`),
3. collect the entity's alias pairs — the URN's own alias plus every
   alternate-key column value present in the resolved row, skipping any
   blank/null/whitespace value,
4. resolve those aliases against the `<table>__aliases` map:
   - **one** existing uuid → replace that row,
   - **several distinct** uuids → **error** with a conflict naming both uuids
     and the disagreeing aliases (`refusing to merge distinct records`),
   - **none** → mint a fresh v4 uuid,
5. insert the row under the resolved uuid and register every alias to point at
   it.

Re-running sync for the same entity **replaces** the row rather than appending
a duplicate, because the keyed table uses `ReplacingMergeTree` (see below) and
the aliases resolve back to the same uuid.

### Why re-sync doesn't duplicate

ClickHouse `MergeTree` does not deduplicate inserts. Every table — keyed on the
system-owned `id` — is therefore created as:

```sql
ENGINE = ReplacingMergeTree(`_dkg_version`) ORDER BY `id`
```

- `_dkg_version` is a **system-managed** `DateTime64(3)` column. Authors never
  declare it; the backend adds it to keyed tables during reconcile and stamps
  it on every insert (the caller's value if supplied, else `now64(3)`). It is
  left visible in `SELECT *` output.
- On the same `id`, the engine keeps the row with the **greatest**
  `_dkg_version` — so the most recent sync wins.
- Collapse happens at background merge time, so reads use `SELECT ... FINAL`
  (applied automatically by the query builder for keyed tables) to see the
  latest row immediately, before a merge has run.

A millisecond tie on `_dkg_version` leaves the choice to the engine, which is
harmless for overview sync (the rows are near-identical).

## Using it

### CLI

```sh
# Record facts (ride on the document store at --database-url / DKG_DOCUMENT_DATABASE_URL)
dkg fact record --entity urn:businesses:slug:acme-roofing-co --dimension phone \
  --value "603-555-1234" --source manual --confidence 1.0
dkg fact record --entity urn:businesses:slug:acme-roofing-co --dimension industry \
  --value "printing" --source inference --confidence 0.4

# List facts (optionally one dimension)
dkg fact list --entity urn:businesses:slug:acme-roofing-co
dkg fact list --entity urn:businesses:slug:acme-roofing-co --dimension phone

# Resolve into the overview record and print it (read-only; needs the schema).
# The printed overview carries no `id`.
dkg fact resolve --entity urn:businesses:slug:acme-roofing-co --config-dir ./examples/configs

# Resolve and upsert the row into the graph table by alias (needs --graph-url)
dkg fact sync --entity urn:businesses:slug:acme-roofing-co \
  --graph-url clickhouse://localhost:8123/kg --config-dir ./examples/configs
```

### HTTP

| Method | Path | Body / query | Description |
| ------ | ---- | ------------ | ----------- |
| POST | `/v1/facts` | a `NewFact` | Record one fact |
| GET  | `/v1/facts` | `?entity=&dimension=` | List an entity's facts |
| POST | `/v1/facts/resolve` | `{ "entity": "urn:..." }` | Resolve → overview record (read-only) |
| POST | `/v1/facts/sync` | `{ "entity": "urn:..." }` | Resolve + write the row |

`resolve` and `sync` require feature B (they need the table shape). Recording
and listing need only the document store.

Example — record then resolve:

```sh
curl -X POST localhost:8080/v1/facts -H 'content-type: application/json' -d '{
  "entity": "urn:businesses:slug:acme-roofing-co",
  "dimension": "phone",
  "value": "603-555-1234",
  "authority": { "source_type": "manual", "confidence": 1.0 }
}'

curl -X POST localhost:8080/v1/facts/resolve -H 'content-type: application/json' \
  -d '{ "entity": "urn:businesses:slug:acme-roofing-co" }'
# → { "phone": "603-555-1234", ... }   (no `id` — it is system-owned)
```

## Relationship to feature D

`sync_entity` is the per-entity form of a feature-D job. The feature-D
`Transform` is one-document-at-a-time and cannot resolve (resolution is
inherently cross-document — it needs all of an entity's facts together). A bulk
runner that groups fact documents by entity and calls the same resolve →
upsert step is the natural next step; until then sync is driven one
entity at a time from the CLI and the HTTP API.

## Crate map

| Crate | Responsibility |
| ----- | -------------- |
| `dkg-facts` | `Fact` / `Authority` / `SourceType`, the `FactStore` trait, `DocumentFactStore` over the document store. Storage only. |
| `dkg-entity-fact-resolver` | `resolve`, `to_record`, `sync_entity`. Policy only. Pure, unit-tested resolution. |
