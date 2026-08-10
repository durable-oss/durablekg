//! Mapping DurableKG errors onto HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use dkg_core::Error;
use serde_json::{json, Value};

/// Wrapper so a [`dkg_core::Error`] can be returned directly from handlers.
///
/// `?` in a handler that returns `Result<T, ApiError>` converts any
/// [`dkg_core::Error`] into the right status code: `404` for not-found,
/// `400` for bad input, `500` otherwise. The body is always a JSON object
/// with an `error` message.
pub struct ApiError(pub Error);

impl From<Error> for ApiError {
    fn from(err: Error) -> Self {
        ApiError(err)
    }
}

/// A batch request that stopped partway through, under `?on_error=stop`.
///
/// The items written before the failure are already committed and are *not*
/// rolled back — the document store has no transaction spanning several
/// writes. So the error body reports both what went wrong and how far the
/// batch got, letting a caller resume from `succeeded` instead of re-sending
/// items that are already stored.
///
/// Only the opt-in stop mode produces this. The default best-effort mode
/// reports per-item failures in a `200`/`207` body instead; see
/// [`BatchOutcome`].
pub struct BatchError {
    /// The underlying failure. Determines the response status.
    pub source: Error,
    /// Zero-based index of the request item that failed.
    pub index: usize,
    /// How many request items completed before the failure. Equal to `index`,
    /// carried explicitly because it is the offset a caller resumes from.
    /// Counts *input* items, so for a fact batch it is bundles, not the facts
    /// they expanded into.
    pub succeeded: usize,
}

impl BatchError {
    /// Record a failure at `index`; every earlier item is already committed.
    pub fn at(index: usize, source: Error) -> Self {
        BatchError {
            source,
            index,
            succeeded: index,
        }
    }
}

impl IntoResponse for BatchError {
    fn into_response(self) -> Response {
        let status = ApiError::status_for(&self.source);
        let body = Json(json!({
            "error": format!("item {}: {}", self.index, self.source),
            "index": self.index,
            "succeeded": self.succeeded,
        }));
        (status, body).into_response()
    }
}

/// The result of a best-effort batch: everything that was written, plus the
/// per-item failures.
///
/// Successes and failures are kept in separate lists rather than one entry per
/// request item, so the common all-succeeded case stays compact. Correlate the
/// two by `index`, which is always the item's position in the *request*.
///
/// The status is `200` when every item was written and `207 Multi-Status` when
/// any failed, so a caller can branch before parsing. Note that "all items
/// failed" is still `207`: the request itself was well-formed, only its
/// contents were rejected. A `400` means the body was not a batch at all.
pub struct BatchOutcome<T> {
    /// Items written, in request order. Shorter than the request when some
    /// failed.
    pub written: Vec<T>,
    /// One entry per failed item: its request `index` and the error.
    pub errors: Vec<(usize, Error)>,
    /// JSON key for the written list, e.g. `"documents"` or `"facts"`.
    pub written_key: &'static str,
    /// JSON key for the written count, e.g. `"created"` or `"recorded"`.
    pub count_key: &'static str,
}

impl<T: serde::Serialize> IntoResponse for BatchOutcome<T> {
    fn into_response(self) -> Response {
        // A partial batch is still a batch that ran; only a malformed body is
        // a 400. 207 lets a caller detect failures without parsing the body.
        let status = if self.errors.is_empty() {
            StatusCode::OK
        } else {
            StatusCode::MULTI_STATUS
        };
        let errors: Vec<Value> = self
            .errors
            .iter()
            .map(|(index, err)| json!({ "index": index, "error": err.to_string() }))
            .collect();
        let body = Json(json!({
            self.count_key: self.written.len(),
            self.written_key: self.written,
            "failed": errors.len(),
            "errors": errors,
        }));
        (status, body).into_response()
    }
}

impl ApiError {
    /// The status code a given error maps onto. Shared with [`BatchError`] so
    /// both render the same error the same way.
    fn status_for(err: &Error) -> StatusCode {
        match err {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Invalid(_) | Error::Config(_) | Error::Serialization(_) => {
                StatusCode::BAD_REQUEST
            }
            Error::UnknownBackend(_) => StatusCode::BAD_REQUEST,
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::Overloaded(_) => StatusCode::SERVICE_UNAVAILABLE,
            Error::Storage(_) | Error::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = Self::status_for(&self.0);
        let body = Json(json!({ "error": self.0.to_string() }));
        // Tell well-behaved clients to back off before retrying a shed
        // request.
        if matches!(self.0, Error::Overloaded(_)) {
            (status, [("Retry-After", "1")], body).into_response()
        } else {
            (status, body).into_response()
        }
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
