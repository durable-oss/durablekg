//! HTTP routes for the document store and config validation.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query as AxumQuery, RawQuery, State};
use axum::http::HeaderMap;
use axum::middleware;
use axum::routing::{get, post};
use axum::{Json, Router};
use dkg_config::Endpoint;
use dkg_documents::{Document, DocumentStore, NewDocument, Query, Urn};
use dkg_endpoints::{CallRequest, EnvSecrets, HttpFetcher};
use dkg_entity_fact_resolver::{resolve_entity, sync_entity};
use dkg_facts::{DocumentFactStore, FactStore, NewFact};
use dkg_graph::{Filter, GraphStore, Op, OrderBy, Record, Schema, SortDirection};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::require_api_key;
use crate::error::ApiError;

/// The relational store and its loaded schema (feature B), present only when
/// the server was configured with a graph database and tables.
#[derive(Clone)]
pub struct GraphState {
    /// The relational record store.
    pub store: Arc<dyn GraphStore>,
    /// The schema loaded at boot.
    pub schema: Arc<Schema>,
}

/// User-defined caching endpoints (feature C): the endpoints loaded at boot,
/// keyed by name, plus the shared HTTP client used to fetch their upstreams.
#[derive(Clone)]
pub struct EndpointState {
    /// Configured endpoints by name.
    pub endpoints: Arc<HashMap<String, Endpoint>>,
    /// Shared upstream fetcher.
    pub fetcher: Arc<HttpFetcher>,
    /// Resolves upstream-auth secrets (from `DKG_SECRET_*`).
    pub secrets: Arc<EnvSecrets>,
}

/// Shared application state. Stores are behind trait objects so the server
/// stays backend-agnostic.
#[derive(Clone)]
pub struct AppState {
    /// The configured document store (feature A).
    pub store: Arc<dyn DocumentStore>,
    /// The relational store + schema (feature B), if configured.
    pub graph: Option<GraphState>,
    /// User-defined caching endpoints (feature C).
    pub endpoints: EndpointState,
    /// The fixed API key inbound requests must present, or `None` to accept
    /// every request unauthenticated (see [`crate::auth`]).
    pub api_key: Option<Arc<str>>,
}

impl AppState {
    /// Borrow the graph state or return a `400`-mapped error when feature B
    /// is not configured.
    fn graph(&self) -> Result<&GraphState, ApiError> {
        self.graph.as_ref().ok_or_else(|| {
            ApiError(dkg_core::Error::Invalid(
                "relational store (feature B) is not configured".into(),
            ))
        })
    }

    /// A fact store over the configured document store. Facts ride on feature
    /// A, so this needs no extra state — just a typed view of `store`.
    fn facts(&self) -> DocumentFactStore {
        DocumentFactStore::new(Arc::clone(&self.store))
    }
}

/// Build the application router. Every route requires `state.api_key` (via
/// `Authorization: Bearer <key>` or `X-Api-Key: <key>`) when one is
/// configured, except `/health` — liveness probes should not need a secret.
pub fn router(state: AppState) -> Router {
    let unauthenticated = Router::new().route("/health", get(health));

    let authenticated = Router::new()
        .route("/v1/config/validate", post(validate_config))
        .route("/v1/documents", post(create_document))
        .route("/v1/documents/search", post(search_documents))
        .route("/v1/documents/by-urn/:urn", get(get_document_by_urn))
        .route(
            "/v1/documents/:id",
            get(get_document)
                .put(replace_document)
                .delete(delete_document),
        )
        .route("/v1/tables/:table", post(insert_records))
        .route("/v1/tables/:table/query", post(query_table))
        .route("/v1/tables/:table/count", post(count_table))
        .route(
            "/v1/tables/:table/:id/related/:other",
            post(related_records),
        )
        .route("/v1/endpoints/:name", get(call_endpoint))
        .route("/v1/facts", get(list_facts).post(record_fact))
        .route("/v1/facts/resolve", post(resolve_facts))
        .route("/v1/facts/sync", post(sync_facts))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));

    unauthenticated.merge(authenticated).with_state(state)
}

/// Liveness probe. Always `200` with a small JSON body.
async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": "durablekg" }))
}

/// Validate a Trongoscript config document.
///
/// Body: `{ "source": "<trongoscript>" }`. Evaluates and validates against
/// the registered config kind, returning the evaluated tree on success.
async fn validate_config(Json(payload): Json<Value>) -> Result<Json<Value>, ApiError> {
    let source = payload
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| dkg_core::Error::Invalid("missing `source` field".into()))?;
    let evaluated = dkg_config::eval::evaluate(source)?;
    dkg_config::validate(&evaluated)?;
    Ok(Json(json!({ "valid": true, "config": evaluated })))
}

/// Create a document.
async fn create_document(
    State(state): State<AppState>,
    Json(doc): Json<NewDocument>,
) -> Result<Json<Document>, ApiError> {
    Ok(Json(state.store.create(doc).await?))
}

/// Fetch a document by id.
async fn get_document(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Document>, ApiError> {
    Ok(Json(state.store.get(id).await?))
}

/// Fetch a document by one of its URN search ids. The URN is the full path
/// segment, e.g. `/v1/documents/by-urn/urn:article:hello`.
async fn get_document_by_urn(
    State(state): State<AppState>,
    Path(urn): Path<String>,
) -> Result<Json<Document>, ApiError> {
    let urn: Urn = urn.parse().map_err(|e: dkg_core::Error| ApiError(e))?;
    Ok(Json(state.store.get_by_urn(&urn).await?))
}

/// Replace a document.
async fn replace_document(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(doc): Json<NewDocument>,
) -> Result<Json<Document>, ApiError> {
    Ok(Json(state.store.replace(id, doc).await?))
}

/// Delete a document.
async fn delete_document(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    state.store.delete(id).await?;
    Ok(Json(json!({ "deleted": id })))
}

/// Search documents. The request body is a [`Query`].
async fn search_documents(
    State(state): State<AppState>,
    Json(query): Json<Query>,
) -> Result<Json<Vec<Document>>, ApiError> {
    Ok(Json(state.store.search(query).await?))
}

// ---- Feature B: relational records ----------------------------------------

/// Coerce a JSON body that is either a single object or an array of objects
/// into a list of records.
fn records_from_body(body: Value) -> Result<Vec<Record>, ApiError> {
    let invalid = || {
        ApiError(dkg_core::Error::Invalid(
            "expected a JSON object or array of objects".into(),
        ))
    };
    match body {
        Value::Object(map) => Ok(vec![map]),
        Value::Array(items) => items
            .into_iter()
            .map(|v| match v {
                Value::Object(map) => Ok(map),
                _ => Err(invalid()),
            })
            .collect(),
        _ => Err(invalid()),
    }
}

/// Insert one or more records into `table`. Body is a record object or an
/// array of them.
async fn insert_records(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let graph = state.graph()?;
    let records = records_from_body(body)?;
    let count = records.len();
    graph.store.insert(&graph.schema, &table, &records).await?;
    Ok(Json(json!({ "inserted": count })))
}

/// A table query: any number of `{column, op, value}` filters (ANDed
/// together), an optional sort, an offset, and a limit.
#[derive(serde::Deserialize)]
struct TableQuery {
    #[serde(default)]
    filters: Vec<FilterClause>,
    #[serde(default)]
    order_by: Option<OrderByClause>,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

/// One filter predicate. `op` defaults to `eq` so existing `{column, value}`
/// bodies (from before range/set/substring operators existed) still parse.
#[derive(serde::Deserialize)]
struct FilterClause {
    column: String,
    #[serde(default)]
    op: FilterOp,
    value: Value,
}

#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum FilterOp {
    #[default]
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Like,
}

impl From<FilterOp> for Op {
    fn from(op: FilterOp) -> Self {
        match op {
            FilterOp::Eq => Op::Eq,
            FilterOp::Ne => Op::Ne,
            FilterOp::Gt => Op::Gt,
            FilterOp::Gte => Op::Gte,
            FilterOp::Lt => Op::Lt,
            FilterOp::Lte => Op::Lte,
            FilterOp::In => Op::In,
            FilterOp::Like => Op::Like,
        }
    }
}

impl From<FilterClause> for Filter {
    fn from(f: FilterClause) -> Self {
        Filter {
            column: f.column,
            op: f.op.into(),
            value: f.value,
        }
    }
}

/// A requested sort: `{column, direction}`. `direction` defaults to `asc`.
#[derive(serde::Deserialize)]
struct OrderByClause {
    column: String,
    #[serde(default)]
    direction: SortDirectionClause,
}

#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum SortDirectionClause {
    #[default]
    Asc,
    Desc,
}

impl From<OrderByClause> for OrderBy {
    fn from(o: OrderByClause) -> Self {
        OrderBy {
            column: o.column,
            direction: match o.direction {
                SortDirectionClause::Asc => SortDirection::Ascending,
                SortDirectionClause::Desc => SortDirection::Descending,
            },
        }
    }
}

fn default_limit() -> usize {
    100
}

/// Query `table`, optionally filtered by `filters` (ANDed together), sorted
/// by `order_by`, and paged by `offset`/`limit`.
async fn query_table(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(q): Json<TableQuery>,
) -> Result<Json<Vec<Record>>, ApiError> {
    let graph = state.graph()?;
    let filters: Vec<Filter> = q.filters.into_iter().map(Filter::from).collect();
    let order_by: Option<OrderBy> = q.order_by.map(OrderBy::from);
    let rows = graph
        .store
        .query(
            &graph.schema,
            &table,
            &filters,
            order_by.as_ref(),
            q.offset,
            q.limit,
        )
        .await?;
    Ok(Json(rows))
}

/// Count the rows in `table` matching `filters` (ANDed together), without
/// fetching them.
async fn count_table(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Json(q): Json<TableQuery>,
) -> Result<Json<Value>, ApiError> {
    let graph = state.graph()?;
    let filters: Vec<Filter> = q.filters.into_iter().map(Filter::from).collect();
    let count = graph.store.count(&graph.schema, &table, &filters).await?;
    Ok(Json(json!({ "count": count })))
}

/// Traverse the relationship from `table` (row `id`) to related rows in
/// `other`. The body may set `{ "limit": N }`; default 100.
async fn related_records(
    State(state): State<AppState>,
    Path((table, id, other)): Path<(String, String, String)>,
    body: Option<Json<TableQuery>>,
) -> Result<Json<Vec<Record>>, ApiError> {
    let graph = state.graph()?;
    let limit = body.map(|b| b.limit).unwrap_or_else(default_limit);
    let rows = graph
        .store
        .related(&graph.schema, &table, &json!(id), &other, limit)
        .await?;
    Ok(Json(rows))
}

// ---- Facts: per-entity claims resolved into overview rows -----------------

/// Record one fact. Body is a [`NewFact`] (entity URN, dimension, value,
/// authority). Returns the stored fact.
async fn record_fact(
    State(state): State<AppState>,
    Json(fact): Json<NewFact>,
) -> Result<Json<Value>, ApiError> {
    let stored = state.facts().record(fact).await?;
    Ok(Json(json!(stored)))
}

/// An entity selector: the URN, and an optional dimension to narrow to.
/// Read from the query string, e.g.
/// `GET /v1/facts?entity=urn:businesses:acme&dimension=phone`.
#[derive(serde::Deserialize)]
struct EntityQuery {
    entity: Urn,
    #[serde(default)]
    dimension: Option<String>,
}

/// List facts about an entity, optionally narrowed to one dimension.
async fn list_facts(
    State(state): State<AppState>,
    AxumQuery(q): AxumQuery<EntityQuery>,
) -> Result<Json<Value>, ApiError> {
    let facts = state.facts();
    let found = match q.dimension {
        Some(dim) => facts.facts_on(&q.entity, &dim).await?,
        None => facts.facts_for(&q.entity).await?,
    };
    Ok(Json(json!(found)))
}

/// An entity selector by URN alone.
#[derive(serde::Deserialize)]
struct EntityRef {
    entity: Urn,
}

/// Resolve an entity's facts into its overview record and return it. Reads
/// only; does not write to the graph. Requires feature B (for the table
/// shape).
async fn resolve_facts(
    State(state): State<AppState>,
    Json(req): Json<EntityRef>,
) -> Result<Json<Record>, ApiError> {
    let graph = state.graph()?;
    let table = graph.schema.table(req.entity.namespace())?;
    let entity_facts = state.facts().facts_for(&req.entity).await?;
    Ok(Json(resolve_entity(&req.entity, &entity_facts, table)))
}

/// Resolve an entity's facts and write the overview row into its graph table.
/// Re-syncing replaces the row. Requires feature B.
async fn sync_facts(
    State(state): State<AppState>,
    Json(req): Json<EntityRef>,
) -> Result<Json<Value>, ApiError> {
    let graph = state.graph()?;
    let facts = state.facts();
    let outcome = sync_entity(&facts, graph.store.as_ref(), &graph.schema, &req.entity).await?;
    Ok(Json(json!({
        "entity": req.entity.to_string(),
        "table": req.entity.namespace(),
        "id": outcome.id,
        "fact_count": outcome.fact_count,
        "record": outcome.record,
    })))
}

// ---- Feature C: caching endpoints -----------------------------------------

/// Call a user-defined endpoint by name: return a fresh cached result for
/// this request, or compose+fetch the upstream, cache it, and return it.
///
/// The caller's query string, headers, and body are captured into a
/// [`CallRequest`]; what of them reaches the upstream and the cache key is
/// governed by the endpoint config.
async fn call_endpoint(
    State(state): State<AppState>,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let endpoint = state
        .endpoints
        .endpoints
        .get(&name)
        .ok_or_else(|| ApiError(dkg_core::Error::NotFound(format!("endpoint `{name}`"))))?;

    let request = CallRequest {
        query: parse_query(raw_query.as_deref().unwrap_or("")),
        headers: headers
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|v| (k.as_str().to_string(), v.to_string()))
            })
            .collect(),
        body: if body.is_empty() {
            None
        } else {
            Some(body.to_vec())
        },
    };

    let result = dkg_endpoints::serve(
        state.store.as_ref(),
        state.endpoints.fetcher.as_ref(),
        state.endpoints.secrets.as_ref(),
        endpoint,
        &request,
    )
    .await?;
    Ok(Json(result))
}

/// Parse a raw query string (`a=1&b=2`) into ordered key/value pairs,
/// percent-decoding each component.
fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// Decode percent-escapes and `+`-as-space in a query component.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
