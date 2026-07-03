# Documents (feature A)

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
  and per-collection listing. **Source of truth for what exists.** Under
  `meta/`.
- **[Tantivy]** — full-text search over body and metadata text. Under `index/`.

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
piling up latency. Tunables: `DKG_STORE_READERS`, `DKG_STORE_QUEUE_DEPTH`,
`DKG_STORE_ENQUEUE_TIMEOUT_MS`.

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
| POST | `/v1/documents/search` | Search (full-text or listing) |
| GET | `/v1/documents/by-urn/:urn` | Fetch by URN |
| GET | `/v1/documents/:id` | Fetch by UUID |
| PUT | `/v1/documents/:id` | Replace |
| DELETE | `/v1/documents/:id` | Delete |

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
