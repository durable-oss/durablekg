//! The shared DurableKG error type.
//!
//! Every library and application crate in the workspace surfaces errors as
//! a [`Error`] (or wraps its own narrower error and converts at the
//! boundary). Keeping one error enum at the core means HTTP handlers and
//! the CLI can map failures to status codes / exit codes in one place.

use thiserror::Error;

/// Errors raised anywhere in DurableKG.
#[derive(Error, Debug)]
pub enum Error {
    /// A requested entity (document, config, endpoint, …) does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// The caller supplied a value that failed validation.
    #[error("invalid input: {0}")]
    Invalid(String),

    /// A configuration file (Trongoscript or otherwise) could not be loaded
    /// or did not match the expected shape.
    #[error("configuration error: {0}")]
    Config(String),

    /// The caller did not present a valid credential where one is required.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// A storage backend failed. The string carries the backend's own
    /// message; the variant deliberately does not leak the backend type so
    /// callers treat all backends uniformly.
    #[error("storage error: {0}")]
    Storage(String),

    /// No backend was registered under the requested name.
    #[error("unknown backend `{0}`")]
    UnknownBackend(String),

    /// A subsystem's work queue is saturated and the request could not be
    /// accepted within the allowed wait. Callers should back off and retry;
    /// HTTP surfaces this as `503 Service Unavailable`.
    #[error("overloaded: {0}")]
    Overloaded(String),

    /// Serialization or deserialization of a value failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// A catch-all for errors that do not (yet) have a dedicated variant.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Convenient `Result` alias used throughout DurableKG.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Build an [`Error::Storage`] from any displayable backend error.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_core::Error;
    ///
    /// let err = Error::storage("connection refused");
    /// assert_eq!(err.to_string(), "storage error: connection refused");
    /// ```
    pub fn storage(msg: impl std::fmt::Display) -> Self {
        Error::Storage(msg.to_string())
    }

    /// Build an [`Error::Config`] from any displayable message.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_core::Error;
    ///
    /// let err = Error::config("missing :name key");
    /// assert_eq!(err.to_string(), "configuration error: missing :name key");
    /// ```
    pub fn config(msg: impl std::fmt::Display) -> Self {
        Error::Config(msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_renders_with_subject() {
        let err = Error::NotFound("doc 42".into());
        assert_eq!(err.to_string(), "not found: doc 42");
    }

    #[test]
    fn storage_helper_prefixes_message() {
        assert_eq!(Error::storage("boom").to_string(), "storage error: boom");
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
