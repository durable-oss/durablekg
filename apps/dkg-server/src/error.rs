//! Mapping DurableKG errors onto HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use dkg_core::Error;
use serde_json::json;

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

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Invalid(_) | Error::Config(_) | Error::Serialization(_) => {
                StatusCode::BAD_REQUEST
            }
            Error::UnknownBackend(_) => StatusCode::BAD_REQUEST,
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::Overloaded(_) => StatusCode::SERVICE_UNAVAILABLE,
            Error::Storage(_) | Error::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
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
