//! # dkg-endpoints — DurableKG feature C
//!
//! User-definable API endpoints with caching. An endpoint is authored in
//! Trongoscript (loaded as a [`dkg_config::Endpoint`]); when called,
//! DurableKG returns a fresh cached result if one exists, otherwise it
//! fetches the upstream `url`, stores the result as a document (feature A),
//! and returns it.
//!
//! Freshness is `now - created_at < cache_seconds`. `cache_seconds == 0`
//! disables caching, so every call fetches upstream.
//!
//! The upstream fetch is abstracted behind the [`Fetcher`] trait so callers
//! can inject a real HTTP client ([`HttpFetcher`]) or a fake one in tests.
//!
//! ```no_run
//! use dkg_endpoints::{serve, CallRequest, EnvSecrets, HttpFetcher};
//! use dkg_config::Endpoint;
//!
//! # async fn run(store: &dyn dkg_documents::DocumentStore, endpoint: &Endpoint) -> dkg_core::Result<()> {
//! let fetcher = HttpFetcher::new();
//! let secrets = EnvSecrets::new();         // upstream-auth secrets from the env
//! let request = CallRequest::default();    // no query / headers / body
//! let body = serve(store, &fetcher, &secrets, endpoint, &request).await?;
//! # let _ = body;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dkg_config::Endpoint;
use dkg_core::{Error, Result};
use dkg_documents::{Document, DocumentStore, NewDocument, Query};
use serde_json::{json, Value};

mod http;
mod request;
mod secret;

pub use http::HttpFetcher;
pub use request::{cache_key, compose_upstream, CallRequest, UpstreamRequest};
pub use secret::{EnvSecrets, MapSecrets, SecretStore};

/// Fetches a composed upstream request. Abstracted so the HTTP client is
/// injectable (and fakeable in tests).
#[async_trait]
pub trait Fetcher: Send + Sync {
    /// Issue `request` and return the response body as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is unusable.
    async fn fetch(&self, request: &UpstreamRequest) -> Result<Value>;
}

/// Outcome of checking the cache for an endpoint.
#[derive(Debug, Clone, PartialEq)]
pub enum Resolution {
    /// A fresh cached document satisfied the call; carries its body.
    Hit(Value),
    /// No fresh cache entry — the caller should fetch upstream.
    Miss,
}

/// Check the cache for a fresh result for `endpoint`, judging freshness as of
/// `now`.
///
/// Returns [`Resolution::Hit`] only when a cached document exists whose age
/// (`now - created_at`) is under `cache_seconds`. With `cache_seconds == 0`
/// caching is off and this always returns [`Resolution::Miss`].
///
/// # Errors
///
/// Returns an error if the store query fails.
pub async fn resolve(
    store: &dyn DocumentStore,
    endpoint: &Endpoint,
    key: &str,
    now: DateTime<Utc>,
) -> Result<Resolution> {
    if endpoint.cache_seconds == 0 {
        return Ok(Resolution::Miss);
    }

    // Fetch recent cache entries for this endpoint + request key and keep the
    // newest. The store does not filter by time, so freshness is judged here.
    let hits = store
        .search(Query {
            collection: Some(endpoint.target_collection().to_string()),
            metadata_contains: json!({ "endpoint": endpoint.name, "key": key }),
            limit: 50,
            ..Query::default()
        })
        .await?;

    let newest = hits.into_iter().max_by_key(|d| d.created_at);
    match newest {
        Some(doc) if is_fresh(&doc, endpoint.cache_seconds, now) => Ok(Resolution::Hit(doc.body)),
        _ => Ok(Resolution::Miss),
    }
}

/// True if `doc` is younger than `cache_seconds` as of `now`.
fn is_fresh(doc: &Document, cache_seconds: u64, now: DateTime<Utc>) -> bool {
    let max_age = Duration::seconds(cache_seconds as i64);
    now.signed_duration_since(doc.created_at) < max_age
}

/// Store an upstream fetch result as a cached document for `endpoint`.
///
/// # Errors
///
/// Returns an error if the store write fails.
pub async fn store_result(
    store: &dyn DocumentStore,
    endpoint: &Endpoint,
    key: &str,
    body: Value,
) -> Result<Document> {
    store
        .create(NewDocument {
            collection: endpoint.target_collection().to_string(),
            body,
            metadata: json!({ "endpoint": endpoint.name, "key": key }),
            ..NewDocument::default()
        })
        .await
}

/// Serve an endpoint call end to end: return a fresh cached body for this
/// request if one exists, otherwise compose and fetch the upstream, cache the
/// result under the request's cache key, and return it.
///
/// `request` carries the caller's query / headers / body; what of it reaches
/// the upstream and the cache key is governed by the endpoint's `forward` /
/// `upstream` / `cache_key` config.
///
/// # Errors
///
/// Returns an error if composing the upstream request, the cache lookup, the
/// upstream fetch, or the cache write fails.
pub async fn serve(
    store: &dyn DocumentStore,
    fetcher: &dyn Fetcher,
    secrets: &dyn SecretStore,
    endpoint: &Endpoint,
    request: &CallRequest,
) -> Result<Value> {
    serve_at(store, fetcher, secrets, endpoint, request, Utc::now()).await
}

/// [`serve`] with an explicit `now`, for deterministic tests.
///
/// # Errors
///
/// See [`serve`].
pub async fn serve_at(
    store: &dyn DocumentStore,
    fetcher: &dyn Fetcher,
    secrets: &dyn SecretStore,
    endpoint: &Endpoint,
    request: &CallRequest,
    now: DateTime<Utc>,
) -> Result<Value> {
    let key = cache_key(endpoint, request);
    if let Resolution::Hit(body) = resolve(store, endpoint, &key, now).await? {
        return Ok(body);
    }
    let upstream = compose_upstream(endpoint, request, secrets)?;
    let body = fetcher.fetch(&upstream).await?;
    store_result(store, endpoint, &key, body.clone()).await?;
    Ok(body)
}

/// Convenience: validate that `method` is a supported HTTP method, returning
/// it uppercased. Used by [`HttpFetcher`] and worth sharing for any other
/// fetcher.
pub(crate) fn normalize_method(method: &str) -> Result<String> {
    let m = method.to_ascii_uppercase();
    match m.as_str() {
        "GET" | "POST" | "PUT" | "DELETE" | "HEAD" | "PATCH" => Ok(m),
        other => Err(Error::config(format!("unsupported HTTP method `{other}`"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A fetcher that echoes the upstream URL it was given (so tests can see
    /// what was composed) and counts how many times it ran.
    struct CountingFetcher {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Fetcher for CountingFetcher {
        async fn fetch(&self, request: &UpstreamRequest) -> Result<Value> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({ "fetched": request.url }))
        }
    }

    fn endpoint(cache_seconds: u64) -> Endpoint {
        Endpoint {
            name: "weather".into(),
            url: "https://example.com".into(),
            method: "GET".into(),
            cache_seconds,
            collection: None,
            forward: Default::default(),
            upstream: Default::default(),
            cache_key: Default::default(),
            auth: None,
        }
    }

    fn no_request() -> CallRequest {
        CallRequest::default()
    }

    // An in-memory DocumentStore would be ideal, but the real composite store
    // is easy to spin up in a temp dir and exercises the actual search path.
    async fn temp_store() -> (tempfile::TempDir, Box<dyn DocumentStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = dkg_documents::ObjectDocumentStore::open_filesystem(dir.path()).unwrap();
        (dir, Box::new(store))
    }

    #[tokio::test]
    async fn first_call_fetches_then_caches() {
        let (_dir, store) = temp_store().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher = CountingFetcher {
            calls: Arc::clone(&calls),
        };
        let ep = endpoint(900);
        let req = no_request();
        let now = Utc::now();
        let secrets = MapSecrets::default();

        let a = serve_at(store.as_ref(), &fetcher, &secrets, &ep, &req, now)
            .await
            .unwrap();
        assert_eq!(a, json!({ "fetched": "https://example.com" }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Second call within the window is served from cache: no new fetch.
        serve_at(store.as_ref(), &fetcher, &secrets, &ep, &req, now)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_cache_triggers_a_refetch() {
        let (_dir, store) = temp_store().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher = CountingFetcher {
            calls: Arc::clone(&calls),
        };
        let ep = endpoint(60);
        let req = no_request();
        let t0 = Utc::now();
        let secrets = MapSecrets::default();

        serve_at(store.as_ref(), &fetcher, &secrets, &ep, &req, t0)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // 61s later the entry is stale → refetch.
        let later = t0 + Duration::seconds(61);
        serve_at(store.as_ref(), &fetcher, &secrets, &ep, &req, later)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn zero_cache_seconds_always_fetches() {
        let (_dir, store) = temp_store().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher = CountingFetcher {
            calls: Arc::clone(&calls),
        };
        let ep = endpoint(0);
        let req = no_request();
        let now = Utc::now();
        let secrets = MapSecrets::default();

        serve_at(store.as_ref(), &fetcher, &secrets, &ep, &req, now)
            .await
            .unwrap();
        serve_at(store.as_ref(), &fetcher, &secrets, &ep, &req, now)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn distinct_cache_keys_get_separate_entries() {
        let (_dir, store) = temp_store().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher = CountingFetcher {
            calls: Arc::clone(&calls),
        };
        // Forward `city` upstream and vary the cache by it.
        let mut ep = endpoint(900);
        ep.forward.query = vec!["city".into()];
        ep.cache_key.query = vec!["city".into()];
        let now = Utc::now();
        let secrets = MapSecrets::default();

        let portland = CallRequest {
            query: vec![("city".into(), "portland".into())],
            ..Default::default()
        };
        let tokyo = CallRequest {
            query: vec![("city".into(), "tokyo".into())],
            ..Default::default()
        };

        // Two different cities → two upstream fetches, two cache entries.
        let a = serve_at(store.as_ref(), &fetcher, &secrets, &ep, &portland, now)
            .await
            .unwrap();
        let b = serve_at(store.as_ref(), &fetcher, &secrets, &ep, &tokyo, now)
            .await
            .unwrap();
        assert_eq!(a, json!({ "fetched": "https://example.com?city=portland" }));
        assert_eq!(b, json!({ "fetched": "https://example.com?city=tokyo" }));
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // Repeating Portland hits its own cache entry — no third fetch.
        let a2 = serve_at(store.as_ref(), &fetcher, &secrets, &ep, &portland, now)
            .await
            .unwrap();
        assert_eq!(a2, a);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn method_normalization() {
        assert_eq!(normalize_method("get").unwrap(), "GET");
        assert!(normalize_method("TRACE").is_err());
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
