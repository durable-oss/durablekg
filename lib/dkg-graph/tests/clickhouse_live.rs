//! Live integration tests against a real ClickHouse.
//!
//! These run only when `DKG_TEST_CLICKHOUSE_URL` is set (e.g.
//! `clickhouse://localhost:8123/default`); otherwise each test no-ops so the
//! suite stays green without a database. Each test uses a uniquely-named set
//! of tables so repeated runs and parallel tests do not collide.

use dkg_config::{Column, ColumnType, RelationKind, Relationship, Table};
use dkg_graph::{open, Filter, Op, OrderBy, Record, Schema, SortDirection};
use serde_json::json;

/// The connection URL, or `None` to skip.
fn url() -> Option<String> {
    std::env::var("DKG_TEST_CLICKHOUSE_URL").ok()
}

/// A per-run-unique tag for table names, so re-runs and parallel tests never
/// collide on a `MergeTree` (which does not dedup inserts).
fn unique_tag() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// A fresh UUID string.
fn uid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn col(name: &str, ty: ColumnType, nullable: bool, array: bool) -> Column {
    Column {
        name: name.into(),
        column_type: ty,
        nullable,
        array,
    }
}

fn record(v: serde_json::Value) -> Record {
    v.as_object().unwrap().clone()
}

/// Build a people/businesses schema with table names suffixed by `tag` so
/// each test is isolated.
fn schema(tag: &str) -> (Schema, String, String) {
    let people = format!("people_{tag}");
    let businesses = format!("businesses_{tag}");
    let tables = vec![
        Table {
            name: businesses.clone(),
            columns: vec![col("name", ColumnType::String, false, false)],
            alternate_keys: Vec::new(),
        },
        Table {
            name: people.clone(),
            columns: vec![
                col("name", ColumnType::String, false, false),
                col("business_id", ColumnType::Uuid, true, false),
            ],
            alternate_keys: Vec::new(),
        },
    ];
    let rels = vec![
        Relationship {
            kind: RelationKind::BelongsTo,
            from: people.clone(),
            to: businesses.clone(),
            key: Some("business_id".into()),
        },
        Relationship {
            kind: RelationKind::HasMany,
            from: businesses.clone(),
            to: people.clone(),
            key: Some("business_id".into()),
        },
    ];
    (Schema::load(tables, rels).unwrap(), people, businesses)
}

#[tokio::test]
async fn reconcile_insert_query_and_traverse() {
    let Some(url) = url() else {
        eprintln!("skipping: DKG_TEST_CLICKHOUSE_URL not set");
        return;
    };
    let (schema, people, businesses) = schema(&unique_tag());
    let store = open(&url).await.unwrap();
    store.reconcile(&schema).await.unwrap();

    // Reconcile is idempotent.
    store.reconcile(&schema).await.unwrap();

    let biz_id = uid();
    let ada = uid();
    let bob = uid();
    let (biz_id, ada, bob) = (biz_id.as_str(), ada.as_str(), bob.as_str());

    store
        .insert(
            &schema,
            &businesses,
            &[record(json!({ "id": biz_id, "name": "Durable" }))],
        )
        .await
        .unwrap();
    store
        .insert(
            &schema,
            &people,
            &[
                record(json!({ "id": ada, "name": "Ada", "business_id": biz_id })),
                record(json!({ "id": bob, "name": "Bob", "business_id": biz_id })),
            ],
        )
        .await
        .unwrap();

    // Query a table with a filter.
    let found = store
        .query(
            &schema,
            &people,
            &[Filter::eq("name", json!("Ada"))],
            None,
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].get("id").unwrap(), &json!(ada));

    // Count matches the same filter.
    let n = store
        .count(&schema, &people, &[Filter::eq("name", json!("Ada"))])
        .await
        .unwrap();
    assert_eq!(n, 1);

    // belongs_to: Ada → her business.
    let her_biz = store
        .related(&schema, &people, &json!(ada), &businesses, 10)
        .await
        .unwrap();
    assert_eq!(her_biz.len(), 1);
    assert_eq!(her_biz[0].get("name").unwrap(), &json!("Durable"));

    // has_many: the business → its people (Ada and Bob).
    let staff = store
        .related(&schema, &businesses, &json!(biz_id), &people, 10)
        .await
        .unwrap();
    assert_eq!(staff.len(), 2);
}

#[tokio::test]
async fn reinserting_same_id_replaces_the_row() {
    let Some(url) = url() else {
        eprintln!("skipping: DKG_TEST_CLICKHOUSE_URL not set");
        return;
    };
    let store = open(&url).await.unwrap();

    // A keyed table → ReplacingMergeTree(_dkg_version): re-inserting the same
    // id replaces the row, with the latest version winning.
    let t = format!("overview_{}", unique_tag());
    let schema = Schema::load(
        vec![Table {
            name: t.clone(),
            columns: vec![col("phone", ColumnType::String, true, false)],
            alternate_keys: Vec::new(),
        }],
        vec![],
    )
    .unwrap();
    store.reconcile(&schema).await.unwrap();

    let id = uid();

    // First sync of the overview.
    store
        .insert(
            &schema,
            &t,
            &[record(json!({ "id": id, "phone": "603-555-0001" }))],
        )
        .await
        .unwrap();

    // A later sync of the same entity with an updated value. now64(3) stamps a
    // strictly greater version, so this wins.
    store
        .insert(
            &schema,
            &t,
            &[record(json!({ "id": id, "phone": "603-555-9999" }))],
        )
        .await
        .unwrap();

    // The query path uses FINAL, so it sees exactly one collapsed row holding
    // the newest value — not two, and not the stale one.
    let rows = store.query(&schema, &t, &[], None, 0, 10).await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "ReplacingMergeTree + FINAL should collapse to one row"
    );
    assert_eq!(rows[0].get("id").unwrap(), &json!(id));
    assert_eq!(rows[0].get("phone").unwrap(), &json!("603-555-9999"));
}

#[tokio::test]
async fn additive_reconcile_adds_a_new_column() {
    let Some(url) = url() else {
        return;
    };
    let store = open(&url).await.unwrap();

    // Start with a one-column table.
    let t = format!("widgets_{}", unique_tag());
    let v1 = Schema::load(
        vec![Table {
            name: t.clone(),
            columns: vec![col("placeholder", ColumnType::String, true, false)],
            alternate_keys: Vec::new(),
        }],
        vec![],
    )
    .unwrap();
    store.reconcile(&v1).await.unwrap();

    // Add a column and reconcile again; the ALTER must land so the insert
    // with the new column succeeds.
    let v2 = Schema::load(
        vec![Table {
            name: t.clone(),
            columns: vec![
                col("placeholder", ColumnType::String, true, false),
                col("label", ColumnType::String, true, false),
            ],
            alternate_keys: Vec::new(),
        }],
        vec![],
    )
    .unwrap();
    store.reconcile(&v2).await.unwrap();

    let id = uid();
    store
        .insert(&v2, &t, &[record(json!({ "id": id, "label": "hi" }))])
        .await
        .unwrap();
    let rows = store
        .query(&v2, &t, &[Filter::eq("id", json!(id))], None, 0, 1)
        .await
        .unwrap();
    assert_eq!(rows[0].get("label").unwrap(), &json!("hi"));
}

#[tokio::test]
async fn many_to_many_traversal() {
    let Some(url) = url() else {
        return;
    };
    let tag = unique_tag();
    let people = format!("people_{tag}");
    let projects = format!("projects_{tag}");
    let tables = vec![
        Table {
            name: people.clone(),
            columns: vec![col("name", ColumnType::String, false, false)],
            alternate_keys: Vec::new(),
        },
        Table {
            name: projects.clone(),
            columns: vec![col("name", ColumnType::String, false, false)],
            alternate_keys: Vec::new(),
        },
    ];
    let rel = Relationship {
        kind: RelationKind::ManyToMany,
        from: people.clone(),
        to: projects.clone(),
        key: None,
    };
    let schema = Schema::load(tables, vec![rel.clone()]).unwrap();
    let store = open(&url).await.unwrap();
    store.reconcile(&schema).await.unwrap();

    let ada = uid();
    let proj = uid();
    let (ada, proj) = (ada.as_str(), proj.as_str());
    store
        .insert(
            &schema,
            &people,
            &[record(json!({ "id": ada, "name": "Ada" }))],
        )
        .await
        .unwrap();
    store
        .insert(
            &schema,
            &projects,
            &[record(json!({ "id": proj, "name": "DurableKG" }))],
        )
        .await
        .unwrap();

    // Link them in the auto-generated join table directly (m2m writes are
    // not yet a store method — see TODO). Column order follows the sorted
    // join-table naming.
    let join = rel.join_table();
    let (a_col, b_col) = if people <= projects {
        (format!("{people}_id"), format!("{projects}_id"))
    } else {
        (format!("{projects}_id"), format!("{people}_id"))
    };
    let client = clickhouse_client(&url);
    client
        .query(&format!(
            "INSERT INTO `{join}` (`{a_col}`, `{b_col}`) VALUES (toUUID(?), toUUID(?))"
        ))
        .bind(if people <= projects { ada } else { proj })
        .bind(if people <= projects { proj } else { ada })
        .execute()
        .await
        .unwrap();

    // Traverse: Ada → her projects.
    let projs = store
        .related(&schema, &people, &json!(ada), &projects, 10)
        .await
        .unwrap();
    assert_eq!(projs.len(), 1);
    assert_eq!(projs[0].get("name").unwrap(), &json!("DurableKG"));
}

#[tokio::test]
async fn query_supports_range_in_like_sort_and_offset() {
    let Some(url) = url() else {
        return;
    };
    let tag = unique_tag();
    let t = format!("widgets_{tag}");
    let schema = Schema::load(
        vec![Table {
            name: t.clone(),
            columns: vec![
                col("name", ColumnType::String, false, false),
                col("count", ColumnType::Int, false, false),
            ],
            alternate_keys: Vec::new(),
        }],
        vec![],
    )
    .unwrap();
    let store = open(&url).await.unwrap();
    store.reconcile(&schema).await.unwrap();

    store
        .insert(
            &schema,
            &t,
            &[
                record(json!({ "id": uid(), "name": "alpha", "count": 1 })),
                record(json!({ "id": uid(), "name": "beta", "count": 5 })),
                record(json!({ "id": uid(), "name": "gamma", "count": 10 })),
            ],
        )
        .await
        .unwrap();

    // Range: count >= 5.
    let ge = store
        .query(
            &schema,
            &t,
            &[Filter {
                column: "count".into(),
                op: Op::Gte,
                value: json!(5),
            }],
            None,
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(ge.len(), 2);

    // IN-list: name in (alpha, gamma).
    let in_list = store
        .query(
            &schema,
            &t,
            &[Filter {
                column: "name".into(),
                op: Op::In,
                value: json!(["alpha", "gamma"]),
            }],
            None,
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(in_list.len(), 2);

    // LIKE: substring match, case-insensitive.
    let like = store
        .query(
            &schema,
            &t,
            &[Filter {
                column: "name".into(),
                op: Op::Like,
                value: json!("%ETA%"),
            }],
            None,
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(like.len(), 1);
    assert_eq!(like[0].get("name").unwrap(), &json!("beta"));

    // Sort descending by count, offset past the first row.
    let sorted = store
        .query(
            &schema,
            &t,
            &[],
            Some(&OrderBy {
                column: "count".into(),
                direction: SortDirection::Descending,
            }),
            1,
            10,
        )
        .await
        .unwrap();
    assert_eq!(sorted.len(), 2);
    assert_eq!(sorted[0].get("name").unwrap(), &json!("beta"));
    assert_eq!(sorted[1].get("name").unwrap(), &json!("alpha"));

    // Count matches the same filter as query.
    let n = store
        .count(
            &schema,
            &t,
            &[Filter {
                column: "count".into(),
                op: Op::Gte,
                value: json!(5),
            }],
        )
        .await
        .unwrap();
    assert_eq!(n, 2);
}

/// A raw clickhouse client for test-only setup the store does not expose
/// (linking rows in an m2m join table).
fn clickhouse_client(url: &str) -> clickhouse::Client {
    // Mirror the store's URL parsing minimally: clickhouse:// → http://, then
    // keep just the `host[:port]` part.
    let host = url
        .replacen("clickhouse://", "http://", 1)
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap()
        .to_string();
    clickhouse::Client::default().with_url(format!("http://{host}"))
}
