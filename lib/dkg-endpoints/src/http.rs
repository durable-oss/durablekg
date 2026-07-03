//! The reqwest-backed [`Fetcher`].

use async_trait::async_trait;
use dkg_core::{Error, Result};
use serde_json::{json, Value};

use crate::{Fetcher, UpstreamRequest};

/// Fetches endpoint upstreams over HTTP with a shared [`reqwest::Client`].
///
/// Clone freely — clones share the same connection pool.
#[derive(Clone)]
pub struct HttpFetcher {
    client: reqwest::Client,
}

impl HttpFetcher {
    /// Build a fetcher with a default client.
    pub fn new() -> Self {
        HttpFetcher {
            client: reqwest::Client::new(),
        }
    }

    /// Build a fetcher over an existing client (to share a pool / config).
    pub fn with_client(client: reqwest::Client) -> Self {
        HttpFetcher { client }
    }
}

impl Default for HttpFetcher {
    fn default() -> Self {
        HttpFetcher::new()
    }
}

#[async_trait]
impl Fetcher for HttpFetcher {
    async fn fetch(&self, request: &UpstreamRequest) -> Result<Value> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|e| Error::config(format!("invalid method: {e}")))?;

        let mut builder = self.client.request(method, &request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = &request.body {
            builder = builder.body(body.clone());
        }

        let response = builder
            .send()
            .await
            .map_err(|e| Error::Other(Box::new(e)))?;

        let status = response.status();
        if !status.is_success() {
            return Err(Error::Storage(format!(
                "upstream {} returned {}",
                request.url, status
            )));
        }

        // Read the body once, then prefer JSON; if it does not parse, wrap
        // the raw text so the result is always a JSON document.
        let text = response
            .text()
            .await
            .map_err(|e| Error::Other(Box::new(e)))?;
        match serde_json::from_str::<Value>(&text) {
            Ok(value) => Ok(value),
            Err(_) => Ok(json!({ "text": text })),
        }
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
