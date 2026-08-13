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
use std::time::Duration;

use anyhow::Context;
use dkg_core::{BindAddress, Settings};
use dkg_entity_fact_resolver::SyncQueue;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::routes::{router, AppState, EndpointState, GraphState};

/// Longest the shutdown path waits for the background sync task's final pass.
///
/// That pass syncs whatever is in the dirty set, which after a bulk import can
/// be very large; waiting it out unbounded turns a restart into an indefinite
/// hang. No fact is lost by cutting it short — facts are durable as soon as
/// they are recorded — but the overview rows for entities the pass had not yet
/// written stay stale, and because a pass drains the dirty set up front, those
/// entities are not re-marked. They resync when their facts are next touched,
/// or on an explicit `/v1/facts/sync`. This is the same staleness window the
/// queue already accepts on a crash.
const SYNC_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

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

    // Background sync: recording a fact marks its entity dirty, and a
    // background task rebuilds the overview rows in batches. Only meaningful
    // with feature B configured — without a graph there is nothing to sync
    // into. `DKG_SYNC_INTERVAL_MS=0` turns it off, leaving callers to drive
    // `/v1/facts/sync` themselves.
    let sync_interval = std::env::var("DKG_SYNC_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(dkg_entity_fact_resolver::queue::DEFAULT_INTERVAL);

    let (sync_queue, sync_task, sync_stop) = match (&graph, sync_interval.is_zero()) {
        (Some(graph), false) => {
            let queue = SyncQueue::new();
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let facts: Arc<dyn dkg_facts::FactStore> =
                Arc::new(dkg_facts::DocumentFactStore::new(Arc::clone(&store)));
            let handle = tokio::spawn(queue.clone().run(
                facts,
                Arc::clone(&graph.store),
                Arc::clone(&graph.schema),
                sync_interval,
                stop_rx,
            ));
            tracing::info!(
                interval_ms = sync_interval.as_millis() as u64,
                "background entity sync enabled"
            );
            (Some(queue), Some(handle), Some(stop_tx))
        }
        (None, _) => {
            tracing::debug!("no relational store configured; background sync disabled");
            (None, None, None)
        }
        (Some(_), true) => {
            tracing::info!("DKG_SYNC_INTERVAL_MS=0; background sync disabled");
            (None, None, None)
        }
    };

    let state = AppState {
        store: Arc::clone(&store),
        graph,
        endpoints,
        api_key: settings.api_key.clone().map(Arc::from),
        sync_queue,
    };
    let app = router(state).layer(TraceLayer::new_for_http());

    // Serve until SIGINT/SIGTERM, then stop accepting connections and let
    // in-flight requests finish before we drain the store.
    match settings.bind() {
        BindAddress::Tcp(addr) => {
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .with_context(|| format!("binding {addr}"))?;
            tracing::info!("listening on {addr}");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .context("serving")?;
        }
        BindAddress::Unix(path) => {
            let listener = bind_unix(&path)
                .with_context(|| format!("binding unix:{}", path.display()))?;
            tracing::info!("listening on unix:{}", path.display());
            let result = serve_unix(listener, app).await.context("serving");

            // Remove the socket on the way out, on success or failure: a
            // leftover file makes the next start look like an address clash.
            // A failure to unlink is not worth aborting shutdown over.
            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(error = %e, path = %path.display(), "could not remove the socket");
                }
            }
            result?;
        }
    }

    // Stop the background sync before the store: its final pass reads facts
    // out of the document store, so draining that first would pull the floor
    // out from under it.
    if let (Some(stop), Some(task)) = (sync_stop, sync_task) {
        tracing::info!("draining the background sync queue");
        // A send error means the task already exited; either way the await
        // below settles it.
        let _ = stop.send(true);
        // Bounded, like the connection drain above. The stop flag is only
        // checked between entities, so a pass that is mid-sync of a large
        // table keeps going until it reaches the next check — an unbounded
        // await here blocks the whole shutdown behind it. Abandoning the pass
        // is safe: facts are already durable, and the entities it had not yet
        // reached stay dirty and re-sync on the next run.
        match tokio::time::timeout(SYNC_DRAIN_TIMEOUT, task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "background sync task did not exit cleanly");
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?SYNC_DRAIN_TIMEOUT,
                    "timed out draining the background sync queue; \
                     unsynced entities will be picked up on the next run"
                );
            }
        }
    }

    // The HTTP server has stopped; drain the document store's worker threads
    // so queued writes flush and indexes commit before the process exits.
    tracing::info!("draining the document store");
    store.shutdown().await.context("draining the store")?;
    tracing::info!("shutdown complete");
    Ok(())
}

/// Serve `app` over a Unix listener until SIGINT/SIGTERM.
///
/// `axum::serve` takes a `TcpListener` concretely in axum 0.7 (the `Listener`
/// abstraction that would accept both arrived in 0.8), so the accept loop is
/// written out here rather than bumping the whole workspace's axum for one
/// branch. The behaviour matches the TCP path: stop accepting on the shutdown
/// signal, then let in-flight requests finish.
async fn serve_unix(listener: tokio::net::UnixListener, app: axum::Router) -> anyhow::Result<()> {
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use tower::Service;

    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());

    loop {
        let (stream, _addr) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                // A per-connection accept error (fd exhaustion, a client
                // vanishing mid-handshake) must not take the server down.
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a unix connection");
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };

        // Each connection gets its own clone of the router; axum's Router is
        // cheap to clone (it is an Arc'd service internally).
        let tower_service = app.clone();
        let hyper_service = service_fn(move |request| {
            // `call` needs &mut, and the clone keeps this closure Fn rather
            // than FnOnce so it can serve many requests on one connection.
            tower_service.clone().call(request)
        });

        // The builder must outlive the connection future that borrows it.
        let builder = Builder::new(TokioExecutor::new());
        let connection =
            builder.serve_connection_with_upgrades(TokioIo::new(stream), hyper_service);
        let watched = graceful.watch(connection.into_owned());

        tokio::spawn(async move {
            if let Err(e) = watched.await {
                tracing::debug!(error = %e, "unix connection ended");
            }
        });
    }

    // Give in-flight connections a bounded window to finish, matching the
    // TCP path's graceful drain. A stuck client must not block shutdown
    // forever, so the wait is capped.
    tracing::info!("no longer accepting connections; draining in-flight requests");
    tokio::select! {
        _ = graceful.shutdown() => {}
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            tracing::warn!("timed out draining connections; closing anyway");
        }
    }
    Ok(())
}

/// Bind a Unix domain socket, clearing a stale one left by a previous run.
///
/// A crash leaves the socket file behind, and `bind` then fails with
/// `AddrInUse` even though nothing is listening. Removing it blindly would be
/// worse: it would let a second server steal a socket a healthy one is still
/// serving. So probe first — a successful connect means a live listener, and
/// we refuse rather than take it over.
///
/// The socket is created with `0o600`: a Unix socket's real access control is
/// filesystem permissions, and the default umask would leave the API readable
/// by any local user. Callers wanting group access should widen it
/// deliberately after boot.
fn bind_unix(path: &std::path::Path) -> anyhow::Result<tokio::net::UnixListener> {
    use std::os::unix::fs::PermissionsExt;

    // `sockaddr_un.sun_path` is a fixed-size buffer — 108 bytes on Linux, 104
    // on macOS — and the kernel's own error for overrunning it ("path must be
    // shorter than SUN_LEN") does not say what the limit is or what the path
    // measured. Check up front so the message names both.
    const SUN_PATH_MAX: usize = 100;
    let len = path.as_os_str().len();
    if len > SUN_PATH_MAX {
        anyhow::bail!(
            "socket path is {len} bytes, over the {SUN_PATH_MAX}-byte limit the OS imposes on \
             Unix sockets: {}. Use a shorter path (/tmp or /run), or bind a TCP address instead.",
            path.display()
        );
    }

    if path.exists() {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => anyhow::bail!(
                "another process is already listening on unix:{}",
                path.display()
            ),
            // Nothing on the other end: the file is a leftover, so clear it.
            Err(e) if matches!(e.kind(), std::io::ErrorKind::ConnectionRefused) => {
                tracing::warn!(path = %path.display(), "removing a stale socket from a prior run");
                std::fs::remove_file(path)
                    .with_context(|| format!("removing stale socket {}", path.display()))?;
            }
            // Anything else (a permission error, a non-socket file) is not
            // ours to clean up; let the bind below report it.
            Err(e) => {
                tracing::debug!(error = %e, path = %path.display(), "probing the existing socket");
            }
        }
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
    }

    let listener = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting permissions on {}", path.display()))?;
    Ok(listener)
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
