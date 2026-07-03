//! Request passthrough: composing the upstream request and the cache key
//! from a caller's request, per the endpoint's `forward` / `upstream` /
//! `cache_key` config.
//!
//! Everything here is pure (no I/O), so it is unit-tested without a network.
//! The allowlists in the config decide what crosses from the caller into the
//! upstream request and into the cache key; anything not named is dropped.

use std::collections::BTreeMap;

use dkg_config::{AuthSpec, Endpoint};
use dkg_core::{Error, Result};

use crate::SecretStore;

/// Header names that are never forwarded upstream, even if an endpoint's
/// `forward.headers` lists them. Upstream auth is a separate concern (a
/// future secrets mechanism), and forwarding a caller's credentials to a
/// third party is a footgun.
const BLOCKED_HEADERS: [&str; 2] = ["authorization", "cookie"];

/// A caller's incoming request, as the server/CLI captured it. Header and
/// query names are compared case-insensitively for headers and
/// case-sensitively for query params (matching HTTP norms).
#[derive(Debug, Clone, Default)]
pub struct CallRequest {
    /// Query parameters, in arrival order.
    pub query: Vec<(String, String)>,
    /// Request headers, in arrival order. Names are treated case-insensitively.
    pub headers: Vec<(String, String)>,
    /// Request body bytes, if any.
    pub body: Option<Vec<u8>>,
}

impl CallRequest {
    /// First query value for `name` (case-sensitive).
    fn query_get(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// First header value for `name` (case-insensitive).
    fn header_get(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// The composed upstream request an endpoint should issue.
#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamRequest {
    /// Final URL (template substituted and/or query appended).
    pub url: String,
    /// HTTP method (uppercased, validated).
    pub method: String,
    /// Headers to send upstream (allowlisted, auth stripped).
    pub headers: Vec<(String, String)>,
    /// Body to send upstream, if forwarding is enabled and one was provided.
    pub body: Option<Vec<u8>>,
}

/// Compose the upstream request for `endpoint` from the caller's `request`,
/// resolving any upstream-auth secret from `secrets`.
///
/// Applies the `forward` allowlists, fills any `upstream.url_template`
/// placeholders from forwarded query params, appends the remaining forwarded
/// query params, and injects the endpoint's upstream-auth credential (if
/// configured). Blocked caller headers are dropped; the upstream credential
/// is added on top and never enters the cache key.
///
/// # Errors
///
/// Returns [`Error::Invalid`] if a `{param}` placeholder has no matching
/// forwarded query value or the method is unsupported, or
/// [`Error::Config`] if `auth` references a secret that is not configured.
pub fn compose_upstream(
    endpoint: &Endpoint,
    request: &CallRequest,
    secrets: &dyn SecretStore,
) -> Result<UpstreamRequest> {
    let method = crate::normalize_method(&endpoint.method)?;

    // Forwarded query params, in the order the config lists them.
    let forwarded: Vec<(String, String)> = endpoint
        .forward
        .query
        .iter()
        .filter_map(|name| {
            request
                .query_get(name)
                .map(|v| (name.clone(), v.to_string()))
        })
        .collect();

    // Build the base URL: substitute the template if present, else use `url`.
    // Track which params the template consumed so they are not also appended.
    let (mut url, consumed) = match endpoint.upstream.url_template.as_deref() {
        Some(template) => substitute_template(template, &forwarded)?,
        None => (endpoint.url.clone(), Vec::new()),
    };

    // Decide which forwarded params go on the query string. If
    // `upstream.query` is set, only those (minus any consumed by the
    // template); otherwise every forwarded param not consumed.
    let on_query: Vec<&(String, String)> = forwarded
        .iter()
        .filter(|(name, _)| !consumed.contains(name))
        .filter(|(name, _)| {
            endpoint.upstream.query.is_empty() || endpoint.upstream.query.contains(name)
        })
        .collect();
    if !on_query.is_empty() {
        let sep = if url.contains('?') { '&' } else { '?' };
        let qs = on_query
            .iter()
            .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
            .collect::<Vec<_>>()
            .join("&");
        url.push(sep);
        url.push_str(&qs);
    }

    // Forwarded headers: allowlisted and not blocked.
    let mut headers: Vec<(String, String)> = endpoint
        .forward
        .headers
        .iter()
        .filter(|name| !is_blocked_header(name))
        .filter_map(|name| {
            request
                .header_get(name)
                .map(|v| (name.clone(), v.to_string()))
        })
        .collect();

    let body = if endpoint.forward.body {
        request.body.clone()
    } else {
        None
    };

    // Inject the endpoint's own upstream credential (if any). It is added to
    // the upstream request only — never to the cache key — so it does not
    // leak into stored documents or metadata.
    if let Some(auth) = &endpoint.auth {
        apply_auth(auth, secrets, &mut url, &mut headers)?;
    }

    Ok(UpstreamRequest {
        url,
        method,
        headers,
        body,
    })
}

/// Resolve `auth`'s secret and apply the credential to the upstream request:
/// a bearer or custom header is pushed onto `headers`; a query credential is
/// appended to `url`.
fn apply_auth(
    auth: &AuthSpec,
    secrets: &dyn SecretStore,
    url: &mut String,
    headers: &mut Vec<(String, String)>,
) -> Result<()> {
    let name = auth.secret_name();
    let value = secrets.get(name).ok_or_else(|| {
        // Name the missing secret, not its value (there is none to leak).
        Error::config(format!(
            "upstream auth secret `{name}` is not configured (set {}{name})",
            crate::EnvSecrets::PREFIX
        ))
    })?;

    match auth {
        AuthSpec::Bearer { .. } => {
            headers.push(("Authorization".into(), format!("Bearer {value}")));
        }
        AuthSpec::Header { name, .. } => {
            headers.push((name.clone(), value));
        }
        AuthSpec::Query { name, .. } => {
            let sep = if url.contains('?') { '&' } else { '?' };
            url.push(sep);
            url.push_str(&format!("{}={}", urlencode(name), urlencode(&value)));
        }
    }
    Ok(())
}

/// True if `name` is a header that must never be forwarded.
fn is_blocked_header(name: &str) -> bool {
    BLOCKED_HEADERS.iter().any(|b| b.eq_ignore_ascii_case(name))
}

/// Fill `{param}` placeholders in `template` from `params`, returning the
/// filled URL and the names of params that were consumed.
fn substitute_template(
    template: &str,
    params: &[(String, String)],
) -> Result<(String, Vec<String>)> {
    let mut out = String::with_capacity(template.len());
    let mut consumed = Vec::new();
    let mut rest = template;

    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let end = after
            .find('}')
            .ok_or_else(|| Error::Invalid(format!("unclosed `{{` in url-template `{template}`")))?;
        let name = &after[..end];
        let value = params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "url-template placeholder `{{{name}}}` has no forwarded query value"
                ))
            })?;
        out.push_str(&urlencode(value));
        consumed.push(name.to_string());
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok((out, consumed))
}

/// Compute the cache key for a call: a canonical string built from the
/// cache-keyed query params and headers (sorted, so order does not matter),
/// or the empty string when nothing varies the cache (key by endpoint name
/// alone).
pub fn cache_key(endpoint: &Endpoint, request: &CallRequest) -> String {
    // BTreeMap gives a stable, sorted ordering so `a=1&b=2` and `b=2&a=1`
    // produce the same key.
    let mut parts: BTreeMap<String, String> = BTreeMap::new();
    for name in &endpoint.cache_key.query {
        if let Some(v) = request.query_get(name) {
            parts.insert(format!("q:{name}"), v.to_string());
        }
    }
    for name in &endpoint.cache_key.headers {
        if is_blocked_header(name) {
            continue;
        }
        if let Some(v) = request.header_get(name) {
            parts.insert(format!("h:{}", name.to_ascii_lowercase()), v.to_string());
        }
    }
    parts
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Minimal percent-encoding for query values and path segments: keeps the
/// RFC 3986 unreserved set, encodes everything else.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use dkg_config::{CacheKeySpec, ForwardSpec, UpstreamSpec};

    fn endpoint() -> Endpoint {
        Endpoint {
            name: "weather".into(),
            url: "https://api/weather".into(),
            method: "GET".into(),
            cache_seconds: 60,
            collection: None,
            forward: ForwardSpec::default(),
            upstream: UpstreamSpec::default(),
            cache_key: CacheKeySpec::default(),
            auth: None,
        }
    }

    /// An empty secret store, for the no-auth cases.
    fn no_secrets() -> crate::MapSecrets {
        crate::MapSecrets::default()
    }

    fn req(query: &[(&str, &str)], headers: &[(&str, &str)]) -> CallRequest {
        CallRequest {
            query: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: None,
        }
    }

    #[test]
    fn default_forwards_nothing() {
        let ep = endpoint();
        let up = compose_upstream(&ep, &req(&[("city", "tokyo")], &[]), &no_secrets()).unwrap();
        assert_eq!(up.url, "https://api/weather");
        assert!(up.headers.is_empty());
    }

    #[test]
    fn appends_allowlisted_query() {
        let mut ep = endpoint();
        ep.forward.query = vec!["city".into()];
        let up = compose_upstream(
            &ep,
            &req(&[("city", "tokyo"), ("secret", "x")], &[]),
            &no_secrets(),
        )
        .unwrap();
        // `city` forwarded, `secret` dropped (not allowlisted).
        assert_eq!(up.url, "https://api/weather?city=tokyo");
    }

    #[test]
    fn fills_url_template_and_appends_remainder() {
        let mut ep = endpoint();
        ep.forward.query = vec!["city".into(), "units".into()];
        ep.upstream.url_template = Some("https://api/weather/{city}".into());
        let up = compose_upstream(
            &ep,
            &req(&[("city", "tokyo"), ("units", "c")], &[]),
            &no_secrets(),
        )
        .unwrap();
        assert_eq!(up.url, "https://api/weather/tokyo?units=c");
    }

    #[test]
    fn template_missing_param_errors() {
        let mut ep = endpoint();
        ep.forward.query = vec!["city".into()];
        ep.upstream.url_template = Some("https://api/weather/{city}".into());
        let err = compose_upstream(&ep, &req(&[], &[]), &no_secrets()).unwrap_err();
        assert!(matches!(err, Error::Invalid(_)));
    }

    #[test]
    fn auth_header_is_never_forwarded() {
        let mut ep = endpoint();
        ep.forward.headers = vec!["authorization".into(), "accept".into()];
        let up = compose_upstream(
            &ep,
            &req(
                &[],
                &[
                    ("Authorization", "Bearer x"),
                    ("Accept", "application/json"),
                ],
            ),
            &no_secrets(),
        )
        .unwrap();
        assert_eq!(
            up.headers,
            vec![("accept".to_string(), "application/json".to_string())]
        );
    }

    #[test]
    fn cache_key_is_order_independent_and_scoped() {
        let mut ep = endpoint();
        ep.cache_key.query = vec!["city".into(), "units".into()];
        let a = cache_key(&ep, &req(&[("city", "tokyo"), ("units", "c")], &[]));
        let b = cache_key(&ep, &req(&[("units", "c"), ("city", "tokyo")], &[]));
        assert_eq!(a, b);
        assert_eq!(a, "q:city=tokyo&q:units=c");

        // A param not in the cache-key spec does not change the key.
        let c = cache_key(
            &ep,
            &req(&[("city", "tokyo"), ("units", "c"), ("x", "1")], &[]),
        );
        assert_eq!(a, c);
    }

    #[test]
    fn empty_cache_key_when_nothing_configured() {
        let ep = endpoint();
        assert_eq!(cache_key(&ep, &req(&[("city", "tokyo")], &[])), "");
    }

    #[test]
    fn bearer_auth_adds_authorization_header() {
        let mut ep = endpoint();
        ep.auth = Some(AuthSpec::Bearer {
            secret: "TOKEN".into(),
        });
        let secrets = crate::MapSecrets::new([("TOKEN", "sk-live-123")]);
        let up = compose_upstream(&ep, &req(&[], &[]), &secrets).unwrap();
        assert_eq!(
            up.headers,
            vec![(
                "Authorization".to_string(),
                "Bearer sk-live-123".to_string()
            )]
        );
    }

    #[test]
    fn header_auth_adds_named_header() {
        let mut ep = endpoint();
        ep.auth = Some(AuthSpec::Header {
            name: "X-Api-Key".into(),
            secret: "KEY".into(),
        });
        let secrets = crate::MapSecrets::new([("KEY", "abc")]);
        let up = compose_upstream(&ep, &req(&[], &[]), &secrets).unwrap();
        assert_eq!(
            up.headers,
            vec![("X-Api-Key".to_string(), "abc".to_string())]
        );
    }

    #[test]
    fn query_auth_appends_to_url() {
        let mut ep = endpoint();
        ep.forward.query = vec!["city".into()];
        ep.auth = Some(AuthSpec::Query {
            name: "apikey".into(),
            secret: "KEY".into(),
        });
        let secrets = crate::MapSecrets::new([("KEY", "abc")]);
        let up = compose_upstream(&ep, &req(&[("city", "tokyo")], &[]), &secrets).unwrap();
        assert_eq!(up.url, "https://api/weather?city=tokyo&apikey=abc");
    }

    #[test]
    fn missing_secret_is_a_config_error() {
        let mut ep = endpoint();
        ep.auth = Some(AuthSpec::Bearer {
            secret: "TOKEN".into(),
        });
        let err = compose_upstream(&ep, &req(&[], &[]), &no_secrets()).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
    }

    #[test]
    fn auth_secret_does_not_enter_cache_key() {
        // The cache key only reflects cache-keyed request data, never auth.
        let mut ep = endpoint();
        ep.cache_key.query = vec!["city".into()];
        ep.auth = Some(AuthSpec::Bearer {
            secret: "TOKEN".into(),
        });
        let key = cache_key(&ep, &req(&[("city", "tokyo")], &[]));
        assert_eq!(key, "q:city=tokyo");
        assert!(!key.contains("TOKEN"));
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
