//! # dkg-config — Trongoscript configuration
//!
//! DurableKG's user-authored configuration — relational tables and their
//! relationships (feature B) and API endpoint definitions (feature C) — is
//! written in [Trongoscript], a small data-templating language, rather than
//! static JSON or YAML. Authors get computation and reuse; the result is
//! still a plain data tree.
//!
//! Each entity is its own config: a [`Table`] declares only its own columns,
//! a [`Relationship`] links two tables by name, and an [`Endpoint`] defines
//! a caching route.
//!
//! The flow is: [`eval::evaluate`] a source string to a JSON value, then
//! [`kind::validate`] it against its registered [`kind::ConfigKind`], then
//! deserialize into a typed schema such as [`Table`] or [`Endpoint`]. New
//! config kinds register through [`inventory`], so the set of supported
//! kinds grows without changing this crate's core.
//!
//! ```
//! use dkg_config::{eval::evaluate, kind::validate, Endpoint};
//!
//! let cfg = evaluate(
//!     r#"{:kind "endpoint"
//!         :name "weather"
//!         :url "https://api.example.com/weather"
//!         :cache-seconds (* 60 15)}"#,
//! )
//! .unwrap();
//! validate(&cfg).unwrap();
//! let ep = Endpoint::from_value(&cfg).unwrap();
//! assert_eq!(ep.cache_seconds, 900);
//! ```
//!
//! [Trongoscript]: trongoscript

pub mod eval;
pub mod kind;
pub mod loader;
pub mod schema;

pub use kind::{validate, ConfigKind};
pub use loader::{load_dir, ConfigBundle};
pub use schema::{
    AuthSpec, CacheKeySpec, Column, ColumnType, Endpoint, ForwardSpec, RelationKind, Relationship,
    Table, UpstreamSpec,
};

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
