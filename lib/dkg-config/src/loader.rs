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

/// Load, evaluate, and validate every `.trongo` file under `dir`, grouping the
/// entities by kind.
///
/// The search is recursive, so a config directory may be organized into
/// subdirectories:
///
/// ```text
/// config/
///   tables/         people.trongo, businesses.trongo
///   relationships/  membership.trongo
///   endpoints/      weather.trongo
/// ```
///
/// Directory names carry no meaning — they are for humans. A file's `:kind`
/// alone decides what it is, so a table declared in `endpoints/` still loads
/// as a table. A flat directory of `.trongo` files works exactly as before.
///
/// Files are processed in sorted full-path order for determinism. A
/// non-existent directory yields an empty bundle (a deployment may legitimately
/// have no schema yet); any present file that fails to evaluate or validate is
/// an error naming the file.
///
/// # Errors
///
/// Returns [`Error::Config`] (with the offending file path) if a directory or
/// file cannot be read, or if a file fails to evaluate or validate, or carries
/// an unrecognized `:kind`.
pub fn load_dir(dir: impl AsRef<Path>) -> Result<ConfigBundle> {
    let dir = dir.as_ref();
    if !dir.exists() {
        return Ok(ConfigBundle::default());
    }

    let mut paths = Vec::new();
    collect_configs(dir, &mut paths)?;
    // Sort on the full path so nesting order is stable regardless of the order
    // the filesystem hands back entries.
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

/// Recursively collect every `.trongo` file under `dir` into `out`.
///
/// Skips dot-directories (`.git` and friends) so pointing the config dir at a
/// working copy does not walk its history, and does not follow symlinked
/// directories, which would otherwise let a cycle hang the loader. Symlinked
/// *files* are still read normally.
fn collect_configs(dir: &Path, out: &mut Vec<std::path::PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| Error::config(format!("reading config dir {}: {e}", dir.display())))?;

    for entry in entries {
        let entry = entry.map_err(|e| Error::config(format!("reading {}: {e}", dir.display())))?;
        let path = entry.path();
        // `file_type` does not traverse symlinks, so a linked directory
        // reports as a symlink here and falls through both arms.
        let file_type = entry
            .file_type()
            .map_err(|e| Error::config(format!("inspecting {}: {e}", path.display())))?;

        if file_type.is_dir() {
            let hidden = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with('.'));
            if !hidden {
                collect_configs(&path, out)?;
            }
        } else if path.extension().is_some_and(|ext| ext == "trongo") {
            out.push(path);
        }
    }
    Ok(())
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
    fn loads_nested_subdirectories() {
        let dir = tempfile::tempdir().unwrap();
        let tables = dir.path().join("tables");
        let rels = dir.path().join("relationships");
        let endpoints = dir.path().join("endpoints/external");
        std::fs::create_dir_all(&tables).unwrap();
        std::fs::create_dir_all(&rels).unwrap();
        std::fs::create_dir_all(&endpoints).unwrap();

        write(
            &tables,
            "people.trongo",
            r#"{:kind "table" :name "people" :columns [{:name "name" :type "string"}]}"#,
        );
        write(
            &tables,
            "businesses.trongo",
            r#"{:kind "table" :name "businesses" :columns [{:name "name" :type "string"}]}"#,
        );
        write(
            &rels,
            "membership.trongo",
            r#"{:kind "relationship" :type "many_to_many" :from "people" :to "businesses"}"#,
        );
        // Nested more than one level deep.
        write(
            &endpoints,
            "weather.trongo",
            r#"{:kind "endpoint" :name "weather" :url "https://x"}"#,
        );
        // A file at the root still loads, so flat layouts keep working.
        write(
            dir.path(),
            "root.trongo",
            r#"{:kind "endpoint" :name "root" :url "https://y"}"#,
        );

        let bundle = load_dir(dir.path()).unwrap();
        assert_eq!(bundle.tables.len(), 2);
        assert_eq!(bundle.relationships.len(), 1);
        assert_eq!(bundle.endpoints.len(), 2);
    }

    #[test]
    fn directory_name_does_not_imply_kind() {
        // Subdirectories are organizational only: `:kind` decides, so an
        // endpoint filed under tables/ still loads as an endpoint.
        let dir = tempfile::tempdir().unwrap();
        let tables = dir.path().join("tables");
        std::fs::create_dir_all(&tables).unwrap();
        write(
            &tables,
            "misfiled.trongo",
            r#"{:kind "endpoint" :name "misfiled" :url "https://x"}"#,
        );

        let bundle = load_dir(dir.path()).unwrap();
        assert!(bundle.tables.is_empty());
        assert_eq!(bundle.endpoints.len(), 1);
    }

    #[test]
    fn skips_dot_directories() {
        let dir = tempfile::tempdir().unwrap();
        let git = dir.path().join(".git");
        std::fs::create_dir_all(&git).unwrap();
        // Invalid on purpose: if the walker descended here it would error.
        write(&git, "junk.trongo", "not valid trongoscript {{{");

        let bundle = load_dir(dir.path()).unwrap();
        assert!(bundle.tables.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_directory_cycle_does_not_hang() {
        let dir = tempfile::tempdir().unwrap();
        let tables = dir.path().join("tables");
        std::fs::create_dir_all(&tables).unwrap();
        write(
            &tables,
            "people.trongo",
            r#"{:kind "table" :name "people" :columns [{:name "name" :type "string"}]}"#,
        );
        // A link back to the root: following it would recurse forever.
        std::os::unix::fs::symlink(dir.path(), dir.path().join("loop")).unwrap();

        let bundle = load_dir(dir.path()).unwrap();
        assert_eq!(bundle.tables.len(), 1);
    }

    #[test]
    fn nested_invalid_file_names_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let tables = dir.path().join("tables");
        std::fs::create_dir_all(&tables).unwrap();
        write(&tables, "bad.trongo", r#"{:kind "table" :name "ok"}}}"#);

        let err = load_dir(dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("bad.trongo"), "got {msg}");
        // The path should locate the file within the tree, not just name it.
        assert!(msg.contains("tables"), "got {msg}");
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
