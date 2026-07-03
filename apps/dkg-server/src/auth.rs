//! Inbound request authentication: a single fixed API key.
//!
//! dkg-server has no notion of users or workspaces — every table and
//! document is reachable to anyone who can reach the port. `DKG_API_KEY`, if
//! set, closes that off with one shared secret: every request but `/health`
//! must present it via `Authorization: Bearer <key>` or `X-Api-Key: <key>`.
//! Unset (the default), the server behaves exactly as before — fine on a
//! private network, not fine once anything reaches it from outside one.

use axum::extract::State;
use axum::http::{HeaderMap, Request};
use axum::middleware::Next;
use axum::response::Response;
use dkg_core::Error;

use crate::error::ApiError;
use crate::routes::AppState;

/// Axum middleware: reject the request unless it presents `state.api_key`
/// (when one is configured). Compares in constant time so response latency
/// cannot be used to guess the key one byte at a time.
pub async fn require_api_key(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(expected) = state.api_key.as_ref() else {
        return Ok(next.run(request).await);
    };
    let expected = expected.as_ref();

    match presented_key(request.headers()) {
        Some(presented) if constant_time_eq(presented.as_bytes(), expected.as_bytes()) => {
            Ok(next.run(request).await)
        }
        _ => Err(ApiError(Error::Unauthorized(
            "missing or invalid API key".into(),
        ))),
    }
}

/// Read the caller's key from `Authorization: Bearer <key>` or
/// `X-Api-Key: <key>`, in that order.
fn presented_key(headers: &HeaderMap) -> Option<String> {
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(s) = auth.to_str() {
            if let Some(key) = s.strip_prefix("Bearer ") {
                return Some(key.to_string());
            }
        }
    }
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Compare two byte strings in time independent of where they first differ.
/// Unequal lengths still short-circuit (the key length is not a secret worth
/// protecting), but a same-length near-match takes the same time as a
/// same-length total mismatch.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_equal_strings() {
        assert!(constant_time_eq(b"secret-key", b"secret-key"));
    }

    #[test]
    fn constant_time_eq_rejects_different_strings() {
        assert!(!constant_time_eq(b"secret-key", b"wrong-key!"));
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"a-much-longer-key"));
    }

    #[test]
    fn presented_key_reads_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer abc123".parse().unwrap(),
        );
        assert_eq!(presented_key(&headers).as_deref(), Some("abc123"));
    }

    #[test]
    fn presented_key_reads_x_api_key_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "abc123".parse().unwrap());
        assert_eq!(presented_key(&headers).as_deref(), Some("abc123"));
    }

    #[test]
    fn presented_key_prefers_bearer_over_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer from-bearer".parse().unwrap(),
        );
        headers.insert("x-api-key", "from-x-api-key".parse().unwrap());
        assert_eq!(presented_key(&headers).as_deref(), Some("from-bearer"));
    }

    #[test]
    fn presented_key_is_none_without_either_header() {
        let headers = HeaderMap::new();
        assert!(presented_key(&headers).is_none());
    }

    #[test]
    fn presented_key_ignores_non_bearer_authorization() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Basic dXNlcjpwYXNz".parse().unwrap(),
        );
        assert!(presented_key(&headers).is_none());
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
