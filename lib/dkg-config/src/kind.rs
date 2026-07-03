//! The config-kind registry.
//!
//! Every config document carries a `:kind` discriminator. A [`ConfigKind`]
//! registration says how to recognise and validate one kind. New kinds
//! (beyond the built-in graph-shape and endpoint kinds) are added by
//! submitting a [`ConfigKind`] with [`inventory`] — no central match arm to
//! edit.

use dkg_core::registry::Named;
use dkg_core::{Error, Result};
use trongoscript::Value;

/// A registered kind of configuration document.
///
/// `validate` is handed the evaluated config tree and should return `Ok` if
/// it is well-formed for this kind, or an [`Error::Config`] describing the
/// problem. It does not need to fully deserialize — callers do that with
/// the concrete typed structs — but it is the right place for cross-field
/// checks a `serde` derive cannot express.
pub struct ConfigKind {
    /// The value of the `:kind` field this registration handles.
    pub kind: &'static str,
    /// One-line human description, shown in listings and errors.
    pub summary: &'static str,
    /// Structural validation hook.
    pub validate: fn(&Value) -> Result<()>,
}

impl Named for ConfigKind {
    fn name(&self) -> &str {
        self.kind
    }
}

inventory::collect!(ConfigKind);

/// Read the `:kind` discriminator from an evaluated config tree.
///
/// # Errors
///
/// Returns [`Error::Config`] if the tree is not an object or has no string
/// `kind` field.
pub fn kind_of(value: &Value) -> Result<&str> {
    value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::config("config is missing a string `:kind` field"))
}

/// Validate an evaluated config tree against its registered kind.
///
/// # Errors
///
/// Returns [`Error::Config`] if the `:kind` is missing, unregistered, or
/// the kind's own `validate` hook rejects the tree.
///
/// # Examples
///
/// ```
/// use dkg_config::{eval::evaluate, kind::validate};
///
/// let cfg = evaluate(r#"{:kind "endpoint" :name "weather" :url "https://example.com"}"#).unwrap();
/// validate(&cfg).unwrap();
/// ```
pub fn validate(value: &Value) -> Result<()> {
    let kind = kind_of(value)?;
    let registered = dkg_core::registry::find::<ConfigKind>(kind).ok_or_else(|| {
        let known = dkg_core::registry::names::<ConfigKind>().join(", ");
        Error::config(format!(
            "unknown config kind `{kind}` (known kinds: {known})"
        ))
    })?;
    (registered.validate)(value)
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
