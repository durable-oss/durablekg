# Configuration reference

## Environment variables

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `DKG_DOCUMENT_DATABASE_URL` | `dkg://./data` | Document store root (`dkg://<path>`) |
| `DKG_GRAPH_DATABASE_URL` | *(unset → relational records off)* | ClickHouse URL, e.g. `clickhouse://localhost:8123/kg` |
| `DKG_CONFIG_DIR` | *(unset)* | Directory of `.trongo` configs (tables, relationships, endpoints), searched recursively |
| `DKG_BIND_ADDRESS` | `127.0.0.1:8080` | HTTP server bind address |
| `DKG_STORE_READERS` | `4` | Reader threads per blocking subsystem (Fjall, Tantivy) |
| `DKG_STORE_QUEUE_DEPTH` | `1024` | Bounded worker-queue capacity |
| `DKG_STORE_ENQUEUE_TIMEOUT_MS` | `250` | Backpressure wait before shedding with `503` |
| `DKG_STORE_EXECUTE_TIMEOUT_MS` | `30000` | Ceiling on how long one store job may run before the caller sheds with `503`; `0` waits indefinitely |
| `DKG_SECRET_<NAME>` | *(unset)* | Resolves an endpoint's upstream-auth secret named `<NAME>` |
| `DKG_TEST_CLICKHOUSE_URL` | *(unset)* | Enables the live ClickHouse integration tests |

The `dkg://<path>` URL opens the composite store rooted at `<path>`, holding
`blobs/` (bodies), `meta/` (Fjall), and `index/` (Tantivy). Relational records
turn on only when `DKG_GRAPH_DATABASE_URL` is set; on boot the server loads the
`DKG_CONFIG_DIR` tables/relationships and reconciles them.

## Trongoscript config kinds

Config files are authored in [Trongoscript](../external/trongoscript), a small
data-templating language. Each file is one entity carrying a `:kind`
discriminator. Config kinds are an `inventory` registry — `dkg config kinds`
lists the registered ones.

### Directory layout

`DKG_CONFIG_DIR` is searched recursively, so a growing config can be organized
into subdirectories:

```text
config/
  tables/
    businesses.trongo
    people.trongo
  relationships/
    people_belong_to_businesses.trongo
  endpoints/
    weather.trongo
```

**Directory names carry no meaning.** They are for humans; a file's `:kind` is
what decides how it loads, so a `table` filed under `endpoints/` still loads as
a table. Nesting can be any depth, and a flat directory of `.trongo` files works
exactly as before.

Two things are skipped while walking: directories whose names begin with `.`
(so pointing the config dir at a working copy does not descend into `.git`), and
symlinked directories (a symlink cycle would otherwise hang the loader).
Symlinked *files* are read normally.

Files load in sorted full-path order, so evaluation is deterministic. A file
that fails to parse or validate fails startup, and the error names its full
path.

| `:kind` | Capability | Doc |
| ------- | ---------- | --- |
| `table` | Relational records | [relational.md](relational.md) |
| `relationship` | Relational records | [relational.md](relational.md) |
| `endpoint` | Endpoints | [endpoints.md](endpoints.md) |

Validate a config offline:

```sh
dkg config validate examples/configs/businesses_table.trongo
dkg config kinds
```

### A note on hyphenated keys

Trongoscript preserves hyphens in keyword keys, so `:cache-seconds` becomes the
JSON key `"cache-seconds"`. The typed schemas use serde aliases for these
multi-word fields (`:cache-seconds`, `:cache-key`, `:url-template`).

## A minimal local setup

```sh
DKG_DOCUMENT_DATABASE_URL=dkg://./data \
DKG_GRAPH_DATABASE_URL=clickhouse://localhost:8123/kg \
DKG_CONFIG_DIR=./examples/configs \
  cargo run -p dkg-server
```

See [`examples/configs`](../examples/configs) for annotated samples.
