//! Evaluating Trongoscript configuration sources.
//!
//! DurableKG configs — knowledge-graph shapes and API endpoint definitions
//! — are authored in Trongoscript rather than static JSON/YAML. That buys
//! authors computation (loops, shared constants, helper functions) while
//! the result is still a plain data tree we can deserialize.
//!
//! Evaluation runs with the *default* (capability-free) Trongoscript
//! configuration: no filesystem, network, or subprocess access. A config
//! file therefore cannot read other files or reach the network during
//! evaluation, which keeps loading untrusted configs safe.

use dkg_core::{Error, Result};
use trongoscript::{eval_source, Context, EvalConfig, Value};

/// Evaluate a Trongoscript config source to a JSON value.
///
/// # Errors
///
/// Returns [`Error::Config`] if the source fails to parse or evaluate.
///
/// # Examples
///
/// ```
/// use dkg_config::eval::evaluate;
///
/// let v = evaluate(r#"{:kind "endpoint" :name "weather"}"#).unwrap();
/// assert_eq!(v["name"], serde_json::json!("weather"));
/// ```
pub fn evaluate(source: &str) -> Result<Value> {
    let mut ctx = Context::new(EvalConfig::default());
    eval_source(source, &mut ctx).map_err(Error::config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluates_computed_values() {
        // Trongoscript can compute config values, not just hold literals.
        let v = evaluate(r#"{:kind "graph-shape" :edge-count (+ 1 2)}"#).unwrap();
        assert_eq!(v["edge-count"], serde_json::json!(3));
    }

    #[test]
    fn surfaces_parse_errors_as_config_errors() {
        let err = evaluate("{:kind (unterminated").unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
