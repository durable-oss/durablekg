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
        │  Documents            │                 │ sync (write the overview row)
        │  dkg-documents        │                 ▼
        │  OpenDAL+Fjall+Tantivy│     ┌───────────────────────────┐
        └───────────────────────┘     │ Relational records        │
                    ▲                 │ dkg-graph (ClickHouse)    │
        ┌───────────┴───────────┐     │ ReplacingMergeTree rows   │
        │  Endpoints            │     └───────────────────────────┘
        │  cache upstreams as   │
        │  documents            │     Jobs: bulk document → record transforms
        └───────────────────────┘
```

- **Documents** are the substrate. Facts and cached endpoint results are
  both stored as documents.
- **Relational records** are the queryable, typed shape — the *target* an
  entity's facts resolve into.
- **Endpoints** turn upstream APIs into cached documents.
- **Jobs** bulk-transform documents into records.
- **Facts** sit between documents and relational records: claims land as documents, resolve into one
  overview per entity, and sync into a relational table.

## Pages

| Page | What it covers |
| ---- | -------------- |
| [Documents](documents.md) | The document store, URNs, search, durability |
| [Relational records](relational.md) | Tables, relationships, reconcile, ReplacingMergeTree |
| [Endpoints](endpoints.md) | Caching API endpoints, forwarding, upstream auth |
| [Jobs](jobs.md) | Document → record bulk transforms |
| [Facts & overview resolution](facts.md) | Fact storage, authority resolution, sync |
| [Configuration reference](configuration.md) | Env vars, Trongoscript config kinds |

## Conventions

- Config is authored in [Trongoscript](../external/trongoscript) and evaluated
  to JSON, then deserialized into typed schemas.
- Every extension point (storage backends, config kinds, transforms) is an
  [`inventory`](https://docs.rs/inventory) registry — add a variant by
  submitting a registration, no central switch to edit.
