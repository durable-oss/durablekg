# Documents

The document store holds semistructured data with rich, searchable metadata.
It is the substrate the rest of DurableKG builds on — facts and cached endpoint
results are both stored as documents.

## The data model

A document is:

```jsonc
{
  "collection": "articles",          // logical grouping; indexed
  "search_ids": ["urn:article:hello"], // caller-assigned URNs to fetch by
  "body": { "...": "opaque JSON" },   // stored as a blob
  "metadata": { "lang": "en" }        // indexed for containment + full-text
}
```

The store assigns `id` (a UUID), `created_at`, and `updated_at`.

### URNs

A `urn:<namespace>:<id>` is a caller-assigned search id — a stable, meaningful
key to fetch a document by, instead of only the store's UUID. The namespace may
not contain a colon (so a namespace prefix is unambiguous); the id may. A URN
must be unique across the store. The namespace prefix `urn:<namespace>:` lists a
whole namespace.

## The composite backend

The default store (`dkg://<path>`) composes three embedded layers:

- **[OpenDAL]** — blob storage for bodies (local filesystem by default, S3 by
  configuration). Lives under `blobs/`.
- **[Fjall]** — an embedded LSM key/value store for metadata, the URN index,
  per-collection listing, and the entity index. **Source of truth for what
  exists.** Under `meta/`.
- **[Tantivy]** — full-text search over body and metadata text. Under `index/`.

### The entity index

One metadata field, `entity`, has a secondary index keyed
`<collection>\0<entity>\0<id>`. Everything else is matched by scanning the
collection and filtering in memory.

It earns the exception because it is how facts are addressed: every fact
bundle lifts its entity URN into `metadata.entity`, and reading one entity's
facts is the hottest query the store serves. Unindexed, that read decodes
every record in the collection — at a few million bundles it saturates the
reader pool and takes every document-store endpoint down with it.

A store written before the index existed has records but no index entries, so
reads check whether a collection is indexed and fall back to the scan when it
is not: slow, but never wrong. The alternative would be reporting "this entity
has no facts", which on the sync path resolves to an empty overview row that
replaces the real one. `migrate()` backfills on boot and is idempotent, so
upgrading is a restart — but the first restart on a large store pays a full
scan per collection before it begins serving.

### Durability

Writes are Fjall-first with a `Pending`/`Indexed` status flag, so a write
interrupted by a crash is detectable. A `repair()` pass re-drives any document
left non-final to completion — run automatically on server boot, or on demand
via `dkg doc repair`.

### Concurrency

Fjall and Tantivy are blocking libraries, so each runs behind a worker pool:
one serialized writer thread plus a pool of readers, fed by bounded in-memory
queues (async-channel, MPMC — each reader clones its own receiver). Async
handlers `await` a reply rather than blocking a Tokio worker on disk I/O.

The bounded queues apply backpressure: when a queue stays full past a short
timeout, the request is shed with `Error::Overloaded` (HTTP `503`) instead of
piling up latency.

A full queue is not the only way a request can stall, so there is a second
ceiling. With a deep queue there is nearly always a free slot, so submission
succeeds immediately and the caller then waits on the job itself — and a job
that is slow, or a pool whose threads are all busy with slow jobs, produces an
unbounded wait rather than a fast failure. `DKG_STORE_EXECUTE_TIMEOUT_MS`
bounds that wait and sheds with `503` too. It bounds the *caller*, not the
worker: the job runs to completion on its thread, because interrupting a
blocking Fjall or Tantivy call midway is not safe. Long-running maintenance
(the entity-index backfill on startup, `repair`) is deliberately exempt.

Tunables: `DKG_STORE_READERS`, `DKG_STORE_QUEUE_DEPTH`,
`DKG_STORE_ENQUEUE_TIMEOUT_MS`, `DKG_STORE_EXECUTE_TIMEOUT_MS`.

On `SIGINT`/`SIGTERM` the server stops accepting connections, lets in-flight
requests finish, then drains the worker pools so no write is lost and the
Tantivy index commits before exit.

## Search

A query is `{ collection?, text?, metadata_contains, limit }`:

- set `text` for full-text search over body and metadata;
- omit `text` to list by collection / metadata containment;
- `metadata_contains` requires the given key/value pairs to be present.

## HTTP & CLI

| Method | Path | Description |
| ------ | ---- | ----------- |
| POST | `/v1/documents` | Create |
| POST | `/v1/documents/batch` | Create many (array of documents) |
| POST | `/v1/documents/search` | Search (full-text or listing) |
| GET | `/v1/documents/by-urn/:urn` | Fetch by URN |
| GET | `/v1/documents/:id` | Fetch by UUID |
| PUT | `/v1/documents/:id` | Replace |
| DELETE | `/v1/documents/:id` | Delete |

`/v1/documents/batch` takes an array of the same body `/v1/documents` takes (a
bare object is accepted as a batch of one).

It is a convenience, not a faster path: the store writes one document per call
either way, so a batch of N costs the same as N requests minus the HTTP
overhead. What it buys you is one round-trip and one error to check.

**Best-effort by default.** Every item is attempted, including the ones after a
failure, and both malformed and rejected items are reported per item:

```json
{ "created": 2,
  "documents": [ {...}, {...} ],
  "failed": 1,
  "errors": [ { "index": 1, "error": "urn already in use: urn:article:x" } ] }
```

`index` is always the item's position in the *request*, so you can correlate the
two lists even though `documents` is shorter. Status is `200` when everything
was written and **`207 Multi-Status`** when anything failed — including when
everything failed, since the request itself was still well-formed. A `400` means
the body was not a batch at all (not an array or object).

A malformed item is just another failed item; it does not reject the request.
One bad row out of a thousand costs you that row.

**`?on_error=stop`** switches to fail-fast: the batch halts at the first failure
and returns `400` with `index` and `succeeded`. Items written before that point
stay committed — there is no transaction spanning the writes, so neither mode is
atomic. `succeeded` is the offset to resume from.

Re-sending an already-created document fails on the duplicate URN rather than
silently double-writing, so a retry of the whole batch is safe but noisy.

```sh
dkg doc put body.json --collection articles --search-id urn:article:hello
dkg doc get urn:article:hello        # or a UUID
dkg doc search --text "embedded search"
dkg doc search --collection articles --limit 20
dkg doc repair
```

[OpenDAL]: https://opendal.apache.org
[Fjall]: https://github.com/fjall-rs/fjall
[Tantivy]: https://github.com/quickwit-oss/tantivy
