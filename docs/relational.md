# Relational records

Relational records store typed, queryable business records — `people`, `businesses`,
and the like — in ClickHouse. It is the *target shape* that facts resolve into.

This is **not** a node/edge graph. Each table is its own entity declaring only
its own columns; relationships are separate entities that link two tables by
name.

## Authoring a schema

A `table` declares columns with a type and optional `:nullable` / `:array`
modifiers:

```clojure
{:kind "table"
 :name "businesses"
 :columns [{:name "name"    :type "string"}
           {:name "slug"    :type "string"}
           {:name "domain"  :type "string"   :nullable true}
           {:name "founded" :type "datetime" :nullable true}
           {:name "tags"    :type "string"   :array true}]
 :alternate-keys ["slug" "domain"]}
```

Column types: `uuid`, `string`, `int`, `float`, `bool`, `datetime`, `json`.
`uuid` remains a valid column type for foreign keys (e.g. `business_id`); it is
just never declared for a column named `id` — see below.

### The system-owned `id`

Every table is keyed on a system-owned `id` column: a `uuid` DurableKG mints and
keys the table on. Authors **never** declare it. Declaring a column named `id`
is a config error (`column `id` in table `t` is system-managed and must not be
declared`), and `id` cannot be set on insert either (see [Querying](#querying)).

### Alternate keys

A table may declare `:alternate-keys` — a list of already-declared column names
that act as **unique alias keys**. Each alias value maps one-to-one to the
system `id`, giving callers a convenient way to address a row by something
stable (a slug, a domain) without knowing its uuid. They are a lookup
convenience, not a second identity: internally everything is the uuid.

The mapping lives in an auto-created `<table>__aliases` table — columns
`alias_type String`, `alias_value String`, `entity_id UUID`, plus the
`_dkg_version` system column — keyed on `(alias_type, alias_value)` via
`ReplacingMergeTree(_dkg_version)`, so re-registering an alias replaces its
mapping. It is created during reconcile whenever a table has alternate keys.

A `relationship` links two tables. It holds no columns of its own:

```clojure
{:kind "relationship" :type "belongs_to"
 :from "people" :to "businesses" :key "business_id"}
```

- `belongs_to` — `from` holds a foreign-key column referencing one `to` row.
- `has_many` — the inverse; `to` rows reference back to one `from`.
- `many_to_many` — linked through an auto-generated join table named by the two
  table names in sorted order (`posts` ↔ `tags` → `posts_tags`).

`belongs_to` / `has_many` require an explicit `:key`; `many_to_many` does not
(it uses the join table).

## Loading and reconcile

A directory of `.trongo` files is loaded into a bundle, validated **once** as a
whole (every relationship names real tables; foreign-key columns exist; table
names are unique), then reconciled into ClickHouse.

Reconcile is **additive**: missing tables, columns, and join tables are
created; nothing is ever dropped. Removing a table or column from config is a
no-op — cleanup is an out-of-band operation.

## ReplacingMergeTree and the version column

Because every table is keyed on the system-owned `id`, every table is created
as:

```sql
ENGINE = ReplacingMergeTree(`_dkg_version`) ORDER BY `id`
```

so re-inserting a row with an existing `id` **replaces** it rather than
appending a duplicate. This is what makes an entity's overview re-syncable (see
[facts.md](facts.md)).

- `_dkg_version` is a **system-managed** `DateTime64(3)` column — authors never
  declare it. It is added during reconcile and stamped on every insert (the
  caller's value if supplied, else `now64(3)`). It stays visible in `SELECT *`.
- The engine keeps the row with the greatest `_dkg_version` per `id`.
- Collapse is at merge time, so reads use `SELECT ... FINAL` automatically for
  keyed tables (including the `other` side of a many-to-many traversal).

## Querying

| Method | Path | Description |
| ------ | ---- | ----------- |
| POST | `/v1/tables/:table` | Insert record(s) — an object or array |
| POST | `/v1/tables/:table/query` | Query: `{ filters?, order_by?, offset?, limit? }` |
| POST | `/v1/tables/:table/count` | Count: `{ filters? }` → `{ "count": N }` |
| POST | `/v1/tables/:table/:id/related/:other` | Traverse a relationship |

A record is a JSON object keyed by column name. `id` is system-managed: an
insert that sets `id` is rejected (`column `id` is system-managed and cannot be
set on insert`). These routes error unless relational records are configured
(`DKG_GRAPH_DATABASE_URL` set).

`filters` is a list of `{column, op, value}` predicates, ANDed together. `op`
is one of `eq` (default), `ne`, `gt`, `gte`, `lt`, `lte`, `in` (value is an
array), or `like` (value is a string pattern, matched case-insensitively —
`%`/`_` wildcards as in SQL `LIKE`):

```json
{
  "filters": [
    { "column": "industry", "op": "in", "value": ["roofing", "plumbing"] },
    { "column": "employees_estimate", "op": "gte", "value": 50 },
    { "column": "name", "op": "like", "value": "%acme%" }
  ],
  "order_by": { "column": "founded_year", "direction": "desc" },
  "offset": 20,
  "limit": 50
}
```

`order_by.direction` defaults to `asc`. `/count` takes the same `filters`
shape and ignores `order_by`/`offset`/`limit`.

```sh
dkg schema reconcile
dkg table insert businesses biz.json
dkg table query people --filter 'name=Ada'
dkg table query businesses --filter 'employees_estimate>=50' --filter 'industry~roofing,plumbing' --order-by -founded_year --limit 20
dkg table count businesses --filter 'state=NH'
dkg table related people <uuid> businesses
```

CLI filter syntax is `column<op>value`: `=` (default), `!=`, `>=`, `<=`, `>`,
`<`, `~` (in-list, comma-separated), or `*` (substring `LIKE`, e.g.
`name*acme`).

## Notes

- Identifiers (table and column names) are validated as safe SQL identifiers at
  load time, so author-supplied names never inject into generated DDL.
- SQL generation (`ddl.rs` / `dml.rs`) is pure and unit-tested; the live
  ClickHouse integration test is gated on `DKG_TEST_CLICKHOUSE_URL`.
