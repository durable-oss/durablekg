//! DurableKG HTTP API server.
//!
//! Boots the document store from `DKG_DOCUMENT_DATABASE_URL`, reconciles any
//! write left incomplete by a prior crash, and serves the document and
//! config-validation API on `DKG_BIND_ADDRESS` (default `127.0.0.1:8080`).
//!
//! If `DKG_API_KEY` is set, every route but `/health` requires it via
//! `Authorization: Bearer <key>` or `X-Api-Key: <key>` (see [`crate::auth`]).
//! Unset, the server is unauthenticated, as it always was.
//!
//! On `SIGINT` / `SIGTERM` the server stops accepting connections, lets
//! in-flight requests finish, then drains the store's worker threads so
//! queued writes flush and indexes commit before the process exits.
//!
//! Routes:
//!   GET  /health                          liveness probe (no API key needed)
//!   POST /v1/config/validate              validate a Trongoscript config
//!   POST /v1/documents                    create a document
//!   POST /v1/documents/search             search (full-text or listing)
//!   GET  /v1/documents/by-urn/:urn        fetch a document by URN search id
//!   GET  /v1/documents/:id                fetch a document
//!   PUT  /v1/documents/:id                replace a document
//!   DELETE /v1/documents/:id              delete a document
//!   POST /v1/tables/:t                    insert record(s) (feature B)
//!   POST /v1/tables/:t/query              query a table (feature B)
//!   POST /v1/tables/:t/count              count rows matching a filter (B)
//!   POST /v1/tables/:t/:id/related/:other traverse a relationship (B)
//!   GET  /v1/endpoints/:name              call a caching endpoint (feature C)
//!   POST /v1/facts                        record a fact about an entity
//!   GET  /v1/facts?entity=&dimension=     list an entity's facts
//!   POST /v1/facts/resolve                resolve facts into an overview row
//!   POST /v1/facts/sync                   resolve + write the row (feature B)

mod auth;
mod error;
mod routes;

use std::sync::Arc;

use anyhow::Context;
use dkg_core::Settings;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::routes::{router, AppState, EndpointState, GraphState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let settings = Settings::from_env();
    tracing::info!(bind = %settings.bind_address, "starting DurableKG server");
    if settings.api_key.is_none() {
        tracing::warn!(
            "DKG_API_KEY is not set; every /v1/* route is unauthenticated. Fine behind a \
             private network, not once the port is reachable from outside one."
        );
    }

    let store = dkg_documents::open(&settings.document_database_url)
        .await
        .context("opening the document store")?;
    store
        .migrate()
        .await
        .context("initializing the document store")?;
    let repaired = store
        .repair()
        .await
        .context("reconciling the document store")?;
    if repaired > 0 {
        tracing::warn!(
            repaired,
            "reconciled documents left incomplete by a prior run"
        );
    }

    let store: Arc<dyn dkg_documents::DocumentStore> = Arc::from(store);

    // Load the config directory once: tables/relationships feed feature B,
    // endpoints feed feature C.
    let bundle = match settings.config_dir.as_deref() {
        Some(dir) => dkg_config::load_dir(dir).context("loading the config directory")?,
        None => dkg_config::ConfigBundle::default(),
    };

    // Feature B: if a graph database is configured, open it and reconcile.
    let graph = build_graph_state(&settings, &bundle).await?;

    // Feature C: index the endpoints by name behind a shared HTTP client.
    let endpoints = build_endpoint_state(bundle.endpoints);

    let state = AppState {
        store: Arc::clone(&store),
        graph,
        endpoints,
        api_key: settings.api_key.clone().map(Arc::from),
    };
    let app = router(state).layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&settings.bind_address)
        .await
        .with_context(|| format!("binding {}", settings.bind_address))?;
    tracing::info!("listening on {}", settings.bind_address);

    // Serve until SIGINT/SIGTERM, then stop accepting connections and let
    // in-flight requests finish before we drain the store.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving")?;

    // The HTTP server has stopped; drain the document store's worker threads
    // so queued writes flush and indexes commit before the process exits.
    tracing::info!("draining the document store");
    store.shutdown().await.context("draining the store")?;
    tracing::info!("shutdown complete");
    Ok(())
}

/// Build the feature-B graph state from the loaded config, if a graph
/// database is configured.
///
/// When `DKG_GRAPH_DATABASE_URL` is set, validates the bundle's
/// tables/relationships into a [`dkg_graph::Schema`], opens the store, and
/// reconciles. Returns `None` when no graph database is configured — the
/// server then runs without feature B. A configured database with no tables
/// is allowed (an empty schema).
async fn build_graph_state(
    settings: &Settings,
    bundle: &dkg_config::ConfigBundle,
) -> anyhow::Result<Option<GraphState>> {
    let Some(graph_url) = settings.graph_database_url.as_deref() else {
        if !bundle.tables.is_empty() {
            tracing::warn!(
                "config defines tables but DKG_GRAPH_DATABASE_URL is not set; feature B disabled"
            );
        }
        return Ok(None);
    };

    let schema = dkg_graph::Schema::load(bundle.tables.clone(), bundle.relationships.clone())
        .context("validating the schema")?;

    let store = dkg_graph::open(graph_url)
        .await
        .context("opening the relational store")?;
    store
        .reconcile(&schema)
        .await
        .context("reconciling the schema into the relational store")?;

    let table_count = schema.tables().count();
    tracing::info!(tables = table_count, "feature B (relational store) ready");
    Ok(Some(GraphState {
        store: Arc::from(store),
        schema: Arc::new(schema),
    }))
}

/// Build the feature-C endpoint state: index endpoints by name behind a
/// shared HTTP client and an env-backed secret store. An empty set is fine
/// (no endpoints configured). Warns about any upstream-auth secret an
/// endpoint references that is not set, so misconfiguration is visible at
/// boot rather than only on the first call.
fn build_endpoint_state(endpoints: Vec<dkg_config::Endpoint>) -> EndpointState {
    use dkg_endpoints::SecretStore;
    let secrets = dkg_endpoints::EnvSecrets::new();

    for ep in &endpoints {
        if let Some(auth) = &ep.auth {
            let name = auth.secret_name();
            if secrets.get(name).is_none() {
                tracing::warn!(
                    endpoint = %ep.name,
                    secret = %name,
                    "upstream-auth secret is not set ({}{name}); calls to this endpoint will fail",
                    dkg_endpoints::EnvSecrets::PREFIX,
                );
            }
        }
    }

    let count = endpoints.len();
    let map = endpoints
        .into_iter()
        .map(|e| (e.name.clone(), e))
        .collect::<std::collections::HashMap<_, _>>();
    if count > 0 {
        tracing::info!(endpoints = count, "feature C (caching endpoints) ready");
    }
    EndpointState {
        endpoints: Arc::new(map),
        fetcher: Arc::new(dkg_endpoints::HttpFetcher::new()),
        secrets: Arc::new(secrets),
    }
}

/// Resolve when the process receives an interrupt (Ctrl-C) or, on Unix, a
/// `SIGTERM` (what orchestrators send to ask a container to stop).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
