# DurableKG

DurableKG is an API-first knowledge and document platform from
[Durable Programming](https://durableprogramming.com). It offers four
capabilities, built up across a Rust workspace:

- **A — Documents.** Store semistructured data with rich metadata, in a
  searchable form.
- **B — Relational records.** Define tables (e.g. `people`, `businesses`)
  and the relationships between them, then query each table and traverse
  associations. Backed by ClickHouse.
- **C — Endpoints.** Define API endpoints whose results are automatically
  stored as documents, with user-controlled caching.
- **D — Jobs.** Run bulk jobs that process document data (A) into relational
  records (B).

On top of the relational layer, DurableKG stores **facts** — authority-stamped
claims about an entity (`phone`, `industry`, …), each carrying a source and a
confidence. Resolving an entity's facts yields one best-value-per-dimension
*overview* row, which syncs into its relational table. See
[docs/facts.md](docs/facts.md).

Tables, relationships, and endpoint definitions are authored in
[Trongoscript](external/trongoscript), a small data-templating language —
so a config is data, but an author still gets loops, shared constants, and
helper functions. Each is its own entity: a `table` declares only its own
columns, a `relationship` (`belongs_to` / `has_many` / `many_to_many`) links
two tables by name, and an `endpoint` defines a caching route.

## Status

Feature **A** (documents) is implemented end to end behind an HTTP API and a
CLI. The default store composes three embedded layers:

- **[OpenDAL]** — blob storage for document bodies (local filesystem by
  default; S3 or another object store by configuration).
- **[Fjall]** — an embedded LSM key/value store for metadata, the URN
  search-id index, and per-collection listing. Fjall is the source of truth
  for what exists.
- **[Tantivy]** — full-text search over document body and metadata text.

Writes are Fjall-first with a status flag, so a write interrupted by a crash
is detectable and reconciled by a `repair` pass (run automatically on server
boot, or on demand via `dkg doc repair`).

Fjall and Tantivy are blocking libraries, so each runs behind its own worker
pool — one serialized writer thread (Tantivy has a single `IndexWriter`;
Fjall writes stay ordered) plus a pool of reader threads — fed by bounded
in-memory queues. Async handlers `await` a reply instead of blocking a Tokio
worker on disk I/O. The bounded queues apply backpressure: when a queue stays
full past a short timeout, the request is shed with `Error::Overloaded`
(HTTP `503` + `Retry-After`) rather than piling up unbounded latency. OpenDAL
blob access is already async and stays on the Tokio runtime.

On `SIGINT` / `SIGTERM` the server stops accepting connections, lets in-flight
requests finish, then drains the worker pools — queued jobs run and the
threads join — so no write is lost and the Tantivy index commits before the
process exits.

Feature **B** (relational records) is wired end to end. A schema of `table`
and `relationship` entities is loaded from a config directory, validated once,
and reconciled into ClickHouse **additively** — missing tables, columns, and
many-to-many join tables are created, nothing is ever dropped (cleanup is out
of band). The HTTP API and CLI expose per-table insert and query plus
relationship traversal (`belongs_to` / `has_many` / `many_to_many`), verified
against a live ClickHouse.

Feature **C** (caching endpoints) is wired end to end. Endpoints loaded from
the config directory are mounted at `GET /v1/endpoints/:name`; a call returns
a fresh cached result (`now - created_at < cache_seconds`) or fetches the
upstream, stores it as a document, and returns it. An endpoint config
declares what of the caller's request is forwarded upstream (`:forward`), how
the upstream URL is composed (`:upstream`, with `{param}` templates), and what
varies the cache key (`:cache-key`) — so `?city=portland` and `?city=tokyo`
forward correctly and cache independently. The endpoint can authenticate
itself to its upstream via `:auth` (`bearer` / `header` / `query`), with the
credential referenced by name and resolved from `DKG_SECRET_<name>` at call
time — never stored in config, the cache, or logs. `Authorization`/`Cookie`
headers are never forwarded from the caller. Feature **D** remains scaffolded.

**Facts** are wired end to end on top of features A and B. A fact is recorded
as a document in a per-table `facts:<table>` collection (so they ride on
feature A — no extra backend); `record`, list, and resolve are exposed over
HTTP and the CLI. Resolution picks, per dimension, the highest-confidence fact
(ties broken by recency) and keeps the winning source. Every table is keyed on
a system-owned `id` (a uuid DurableKG mints; authors never declare it) and may
declare unique **alternate keys** (`:alternate-keys ["slug" "domain"]`) that
translate to that id. An entity is addressed by a typed URN
(`urn:businesses:slug:acme-roofing-co`). `sync` resolves the overview and
**upserts** it by alias — minting a uuid on first sight, reusing it when an
alias already maps to a row — and because the table is a `ReplacingMergeTree`
versioned by a system `_dkg_version` timestamp, re-syncing **replaces** the row
instead of duplicating it. See [docs/facts.md](docs/facts.md).

[OpenDAL]: https://opendal.apache.org
[Fjall]: https://github.com/fjall-rs/fjall
[Tantivy]: https://github.com/quickwit-oss/tantivy

## Workspace layout

```
lib/
  dkg-core        Shared Error, Settings, and the inventory-based plugin registry
  dkg-documents   Feature A — DocumentStore trait + composite OpenDAL/Fjall/Tantivy backend
  dkg-config      Trongoscript config loading; typed Table / Relationship / Endpoint schemas
  dkg-graph       Feature B — relational store (Schema loader + ClickHouse backend)
  dkg-endpoints   Feature C — caching API endpoints over the document store
  dkg-jobs        Feature D — document→record bulk transforms
  dkg-facts       Fact storage (authority-stamped claims) over the document store
  dkg-entity-fact-resolver  Resolve facts → overview row; sync into a table
apps/
  dkg-server      axum HTTP API (binary: dkg-server)
  dkg-cli         clap CLI (binary: dkg)
demos/
  prospects-tui   ratatui browser for the businesses/people tables (binary: prospects-tui)
external/
  trongoscript    Vendored config language (its own workspace)
```

## The plugin pattern

DurableKG is built to grow without central switch statements. Every
extension point — document backends, graph backends, config kinds,
document→record transforms — is an [`inventory`](https://docs.rs/inventory)
registry. Adding a variant means submitting one registration; lookup and
enumeration go through `dkg_core::registry`. For example, the composite
document backend registers itself for the `dkg://` URL scheme, and
`dkg_documents::open(url)` dispatches on scheme with no hard-coded wiring.

## Running

Configuration is read from the environment (with local defaults):

| Variable                     | Default                              | Purpose                         |
| ---------------------------- | ------------------------------------ | ------------------------------- |
| `DKG_DOCUMENT_DATABASE_URL`  | `dkg://./data`                       | Document store root (`dkg://<path>`) |
| `DKG_GRAPH_DATABASE_URL`     | *(unset → feature B off)*            | ClickHouse URL, e.g. `clickhouse://localhost:8123/kg` |
| `DKG_CONFIG_DIR`             | *(unset)*                            | Directory of `.trongo` table/relationship configs |
| `DKG_BIND_ADDRESS`           | `127.0.0.1:8080`                     | HTTP server bind address        |
| `DKG_API_KEY`                | *(unset → no inbound auth)*          | Fixed API key every `/v1/*` route requires |
| `DKG_STORE_READERS`          | `4`                                  | Reader threads per blocking subsystem |
| `DKG_STORE_QUEUE_DEPTH`      | `1024`                               | Bounded worker-queue capacity   |
| `DKG_STORE_ENQUEUE_TIMEOUT_MS` | `250`                              | Backpressure wait before `503`  |

The `dkg://<path>` URL opens the composite store rooted at `<path>`, which
holds `blobs/` (bodies), `meta/` (Fjall), and `index/` (Tantivy). The
`DKG_STORE_*` knobs tune the Fjall/Tantivy worker pools. Feature B (relational
records) turns on only when `DKG_GRAPH_DATABASE_URL` is set; on boot the server
loads the `DKG_CONFIG_DIR` tables/relationships and reconciles them into
ClickHouse.

If `DKG_API_KEY` is unset (the default), every route is open — fine on a
private network, not once the port is reachable from outside one. Set it and
every route but `/health` requires the key, via `Authorization: Bearer <key>`
or `X-Api-Key: <key>`; a missing or wrong key gets a `401` with
`{"error": "unauthorized: ..."}`. It is one fixed secret for the whole
server, not per-workspace or per-client credentials — start the server behind
a real auth layer (a gateway, a reverse proxy) if you need those.

Start the API server (reconciles any incomplete document writes, and the
relational schema, on boot):

```sh
DKG_DOCUMENT_DATABASE_URL=dkg://./data \
DKG_GRAPH_DATABASE_URL=clickhouse://localhost:8123/kg \
DKG_CONFIG_DIR=./examples/configs \
DKG_API_KEY=change-me \
  cargo run -p dkg-server

# Every route but /health then requires the key:
curl -H 'Authorization: Bearer change-me' http://localhost:8080/v1/documents/search -d '{"limit":5}'
```

### HTTP API

| Method | Path                              | Description                          |
| ------ | --------------------------------- | ------------------------------------ |
| GET    | `/health`                         | Liveness probe                       |
| POST   | `/v1/config/validate`             | Validate a Trongoscript config       |
| POST   | `/v1/documents`                   | Create a document                    |
| POST   | `/v1/documents/search`            | Search (full-text or listing)        |
| GET    | `/v1/documents/by-urn/:urn`       | Fetch a document by URN search id    |
| GET    | `/v1/documents/:id`               | Fetch a document                     |
| PUT    | `/v1/documents/:id`               | Replace a document                   |
| DELETE | `/v1/documents/:id`               | Delete a document                    |
| POST   | `/v1/tables/:table`               | Insert record(s) into a table (B)    |
| POST   | `/v1/tables/:table/query`         | Query a table (filters, sort, page) (B) |
| POST   | `/v1/tables/:table/count`         | Count rows matching filters (B)      |
| POST   | `/v1/tables/:table/:id/related/:other` | Traverse a relationship (B)     |
| GET    | `/v1/endpoints/:name`             | Call a caching endpoint (C)          |
| POST   | `/v1/facts`                       | Record a fact about an entity        |
| GET    | `/v1/facts?entity=&dimension=`    | List an entity's facts               |
| POST   | `/v1/facts/resolve`               | Resolve facts into an overview row   |
| POST   | `/v1/facts/sync`                  | Resolve + write the row (B)          |

The `/v1/tables/*` routes return `503`-style errors unless feature B is
configured. `/v1/endpoints/:name` requires an endpoint of that name in the
config directory (else `404`); it caches results as documents, so feature A
is always available. A record is a JSON object keyed by column name (insert accepts an
object or an array); a table query body is
`{ filters?: [{column, op, value}], order_by?: {column, direction}, offset?, limit? }`,
where `op` is one of `eq` (default), `ne`, `gt`, `gte`, `lt`, `lte`, `in`
(value is an array), or `like` (value is a case-insensitive substring
pattern); `/count` takes the same `filters` shape. See
[`docs/relational.md`](docs/relational.md) for the full filter reference.

Documents are `{ collection, search_ids, body, metadata }`; `body` is opaque
JSON, `search_ids` is a list of `urn:<namespace>:<id>` strings the document
can be fetched by, and `metadata` is indexed for containment search. A search
query is `{ collection?, text?, metadata_contains, limit }` — set `text` for
full-text search over body and metadata, omit it to list by collection /
metadata.

### CLI

```sh
# Validate Trongoscript configs (offline — no store needed)
cargo run -p dkg-cli -- config validate examples/configs/businesses_table.trongo
cargo run -p dkg-cli -- config kinds

# Documents (open the store at --database-url / DKG_DOCUMENT_DATABASE_URL)
cargo run -p dkg-cli -- doc put body.json --collection articles --search-id urn:article:hello
cargo run -p dkg-cli -- doc get urn:article:hello   # or a UUID
cargo run -p dkg-cli -- doc search --text "embedded search"
cargo run -p dkg-cli -- doc search --collection articles --limit 20
cargo run -p dkg-cli -- doc repair                  # reconcile after a crash

# Relational records (feature B): set --graph-url / --config-dir or the
# DKG_GRAPH_DATABASE_URL / DKG_CONFIG_DIR env vars.
cargo run -p dkg-cli -- schema reconcile
cargo run -p dkg-cli -- table insert businesses biz.json
cargo run -p dkg-cli -- table query people --filter 'name=Ada'
cargo run -p dkg-cli -- table query businesses --filter 'employees_estimate>=50' --order-by -founded_year
cargo run -p dkg-cli -- table count businesses --filter 'state=NH'
cargo run -p dkg-cli -- table related people <uuid> businesses

# Caching endpoints (feature C): needs --config-dir / DKG_CONFIG_DIR.
cargo run -p dkg-cli -- endpoint call weather --query city=portland

# Facts (ride on the document store; resolve/sync also need the schema/graph).
# Entities are addressed by a typed URN: urn:<table>:<keytype>:<value>, where
# keytype is `id` or a declared alternate key (e.g. `slug`).
cargo run -p dkg-cli -- fact record --entity urn:businesses:slug:acme \
  --dimension phone --value "603-555-1234" --source manual --confidence 1.0
cargo run -p dkg-cli -- fact list --entity urn:businesses:slug:acme
cargo run -p dkg-cli -- fact resolve --entity urn:businesses:slug:acme --config-dir ./examples/configs
cargo run -p dkg-cli -- fact sync --entity urn:businesses:slug:acme   # + --graph-url; mints/returns a uuid
```

See [`examples/configs`](examples/configs) for annotated config samples.

### Demos

```sh
# Terminal browser for the businesses/people tables (feature B). Same
# --graph-url / --config-dir (or DKG_GRAPH_DATABASE_URL / DKG_CONFIG_DIR) as
# `dkg table query` — connects to ClickHouse directly, no HTTP server needed.
cargo run -p prospects-tui --release
```

## Documentation

In-depth guides live in [`docs/`](docs/):

- [Overview](docs/README.md) — how the capabilities fit together
- [Documents (A)](docs/documents.md)
- [Relational records (B)](docs/relational.md)
- [Endpoints (C)](docs/endpoints.md)
- [Jobs (D)](docs/jobs.md)
- [Facts & overview resolution](docs/facts.md)
- [Configuration reference](docs/configuration.md)

## Development

```sh
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

## License

MIT OR Apache-2.0. © Durable Programming, LLC.
