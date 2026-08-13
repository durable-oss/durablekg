# Jobs

Jobs run bulk work that processes document data into relational
records. A job reads a collection's worth of documents, runs each
through a registered `Transform`, and inserts the resulting records into the
configured tables.

> **Status: scaffolded.** The `Transform` registry and the job description
> exist; the execution driver is still being built out.

## The Transform registry

A `Transform` maps one document to table-bound records:

```rust
pub struct Transform {
    pub name: &'static str,
    pub apply: fn(&Document) -> Result<Extracted>,
}
```

`Extracted` groups the produced records by destination table — a single
document may populate several tables (e.g. a `businesses` row plus the `people`
rows it mentions).

Transforms are registered through [`inventory`], so a new document-to-record
mapping is added by submitting a registration — no central dispatch table to
edit. A `Job` names a `source_collection` and the `transform` to apply, and
resolves the transform by name at run time.

## Relationship to facts

Fact resolution (see [facts.md](facts.md)) is a job-shaped task that the
current `Transform` interface cannot express on its own: a `Transform` sees one
document at a time, but resolving an entity's overview needs **all** of its
facts together.

So `dkg-entity-fact-resolver` provides `sync_entity`, the per-entity form —
gather → resolve → project → insert — driven one entity at a time from the CLI
and HTTP API. A future bulk-job runner that groups fact documents by entity
before transforming would let the same resolve → project → insert step run as a
bulk job.

[`inventory`]: https://docs.rs/inventory
