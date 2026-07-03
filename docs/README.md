# DurableKG documentation

DurableKG is an API-first Rust workspace with four core capabilities plus a
fact layer on top. Each page below goes deeper than the [project
README](../README.md).

## How the pieces fit

```
                facts (claims about entities)
                  │  record / list
                  ▼
        ┌───────────────────────┐     resolve (highest-confidence-per-dimension)
        │  dkg-facts            │ ──────────────┐
        │  facts:<table> docs   │               ▼
        └───────────┬───────────┘     ┌───────────────────────────┐
                    │ rides on        │ dkg-entity-fact-resolver  │
                    ▼                 │ ResolvedEntity → Record   │
        ┌───────────────────────┐     └───────────┬───────────────┘
        │  A — Documents        │                 │ sync (write the overview row)
        │  dkg-documents        │                 ▼
        │  OpenDAL+Fjall+Tantivy│     ┌───────────────────────────┐
        └───────────────────────┘     │ B — Relational records    │
                    ▲                 │ dkg-graph (ClickHouse)    │
        ┌───────────┴───────────┐     │ ReplacingMergeTree rows   │
        │  C — Endpoints        │     └───────────────────────────┘
        │  cache upstreams as   │
        │  documents            │     D — Jobs: bulk A → B transforms
        └───────────────────────┘
```

- **Documents (A)** are the substrate. Facts and cached endpoint results are
  both stored as documents.
- **Relational records (B)** are the queryable, typed shape — the *target* an
  entity's facts resolve into.
- **Endpoints (C)** turn upstream APIs into cached documents.
- **Jobs (D)** bulk-transform documents into records.
- **Facts** sit between A and B: claims land as documents, resolve into one
  overview per entity, and sync into a B table.

## Pages

| Page | What it covers |
| ---- | -------------- |
| [Documents (A)](documents.md) | The document store, URNs, search, durability |
| [Relational records (B)](relational.md) | Tables, relationships, reconcile, ReplacingMergeTree |
| [Endpoints (C)](endpoints.md) | Caching API endpoints, forwarding, upstream auth |
| [Jobs (D)](jobs.md) | Document → record bulk transforms |
| [Facts & overview resolution](facts.md) | Fact storage, authority resolution, sync |
| [Configuration reference](configuration.md) | Env vars, Trongoscript config kinds |

## Conventions

- Config is authored in [Trongoscript](../external/trongoscript) and evaluated
  to JSON, then deserialized into typed schemas.
- Every extension point (storage backends, config kinds, transforms) is an
  [`inventory`](https://docs.rs/inventory) registry — add a variant by
  submitting a registration, no central switch to edit.
