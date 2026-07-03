//! Loading a directory of Trongoscript config entities.
//!
//! A deployment's configuration is a directory of `.trongo` files, each one
//! entity (a table, a relationship, or an endpoint). [`load_dir`] evaluates
//! and validates every file once, then groups the results by kind into a
//! [`ConfigBundle`] the rest of the system consumes.

use std::path::Path;

use dkg_core::{Error, Result};

use crate::eval::evaluate;
use crate::kind::kind_of;
use crate::schema::{Endpoint, Relationship, Table};

/// The typed result of loading a config directory: every entity, grouped by
/// kind.
#[derive(Debug, Default)]
pub struct ConfigBundle {
    /// Table definitions (feature B).
    pub tables: Vec<Table>,
    /// Relationships between tables (feature B).
    pub relationships: Vec<Relationship>,
    /// Caching API endpoints (feature C).
    pub endpoints: Vec<Endpoint>,
}

/// Load, evaluate, and validate every `.trongo` file in `dir`, grouping the
/// entities by kind.
///
/// Files are processed in sorted path order for determinism. A non-existent
/// directory yields an empty bundle (a deployment may legitimately have no
/// schema yet); any present file that fails to evaluate or validate is an
/// error naming the file.
///
/// # Errors
///
/// Returns [`Error::Config`] (with the offending file path) if a file fails
/// to read, evaluate, or validate, or carries an unrecognized `:kind`.
pub fn load_dir(dir: impl AsRef<Path>) -> Result<ConfigBundle> {
    let dir = dir.as_ref();
    if !dir.exists() {
        return Ok(ConfigBundle::default());
    }

    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| Error::config(format!("reading config dir {}: {e}", dir.display())))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|ext| ext == "trongo"))
        .collect();
    paths.sort();

    let mut bundle = ConfigBundle::default();
    for path in paths {
        let source = std::fs::read_to_string(&path)
            .map_err(|e| Error::config(format!("reading {}: {e}", path.display())))?;
        let value = evaluate(&source)
            .map_err(|e| Error::config(format!("evaluating {}: {e}", path.display())))?;

        let kind =
            kind_of(&value).map_err(|e| Error::config(format!("in {}: {e}", path.display())))?;
        match kind {
            "table" => bundle.tables.push(Table::from_value(&value)?),
            "relationship" => bundle.relationships.push(Relationship::from_value(&value)?),
            "endpoint" => bundle.endpoints.push(Endpoint::from_value(&value)?),
            other => {
                return Err(Error::config(format!(
                    "in {}: unknown config kind `{other}`",
                    path.display()
                )))
            }
        }
    }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &std::path::Path, name: &str, body: &str) {
        std::fs::write(dir.join(name), body).unwrap();
    }

    #[test]
    fn loads_and_groups_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "people.trongo",
            r#"{:kind "table" :name "people" :columns [{:name "name" :type "string"}]}"#,
        );
        write(
            dir.path(),
            "businesses.trongo",
            r#"{:kind "table" :name "businesses" :columns [{:name "name" :type "string"}]}"#,
        );
        write(
            dir.path(),
            "rel.trongo",
            r#"{:kind "relationship" :type "many_to_many" :from "people" :to "businesses"}"#,
        );
        write(
            dir.path(),
            "weather.trongo",
            r#"{:kind "endpoint" :name "weather" :url "https://x"}"#,
        );
        // A non-.trongo file is ignored.
        write(dir.path(), "notes.txt", "ignore me");

        let bundle = load_dir(dir.path()).unwrap();
        assert_eq!(bundle.tables.len(), 2);
        assert_eq!(bundle.relationships.len(), 1);
        assert_eq!(bundle.endpoints.len(), 1);
    }

    #[test]
    fn missing_dir_is_empty_not_an_error() {
        let bundle = load_dir("/nonexistent/dkg/config/dir").unwrap();
        assert!(bundle.tables.is_empty());
        assert!(bundle.endpoints.is_empty());
    }

    #[test]
    fn invalid_file_names_the_path() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "bad.trongo", r#"{:kind "table" :name "ok"}}}"#);
        let err = load_dir(dir.path()).unwrap_err();
        assert!(err.to_string().contains("bad.trongo"), "got {err}");
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
