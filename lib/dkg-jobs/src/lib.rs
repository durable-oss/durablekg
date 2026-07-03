//! # dkg-jobs — DurableKG feature D (scaffold)
//!
//! Bulk jobs that process document data (feature A) into relational records
//! (feature B). A job reads a query's worth of documents, transforms each
//! into table-bound records with a registered [`Transform`], and inserts the
//! result into the configured tables.
//!
//! Transforms are registered through [`inventory`], so new
//! document-to-record mappings are added without editing a dispatch table.
//! This crate defines the [`Transform`] registry and the job description;
//! the execution driver lands as the runner is built out.

use dkg_core::registry::Named;
use dkg_core::Result;
use dkg_documents::Document;
use dkg_graph::Record;

/// One destination table and the records a transform produced for it.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRecords {
    /// Target table name (must exist in the loaded schema).
    pub table: String,
    /// Records to insert into that table.
    pub records: Vec<Record>,
}

/// One document's worth of extracted records, grouped by destination table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Extracted {
    /// Records to insert, grouped by table. A document may populate several
    /// tables (e.g. a `businesses` row plus the `people` rows it mentions).
    pub tables: Vec<TableRecords>,
}

/// A registered transform from a document to relational records.
///
/// Submit one with [`inventory::submit!`] to teach DurableKG how a class of
/// documents maps into tables.
pub struct Transform {
    /// Unique transform name, referenced by a job description.
    pub name: &'static str,
    /// Turn a single document into table-bound records.
    pub apply: fn(&Document) -> Result<Extracted>,
}

impl Named for Transform {
    fn name(&self) -> &str {
        self.name
    }
}

inventory::collect!(Transform);

/// A bulk job: read documents from `source_collection`, run them through
/// `transform`, and insert the resulting records into the configured tables.
#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    /// Document collection to read from (feature A).
    pub source_collection: String,
    /// Name of the registered [`Transform`] to apply.
    pub transform: String,
}

impl Job {
    /// Look up the [`Transform`] this job names.
    ///
    /// # Errors
    ///
    /// Returns [`dkg_core::Error::NotFound`] if no transform is registered
    /// under [`Job::transform`].
    pub fn resolve_transform(&self) -> Result<&'static Transform> {
        dkg_core::registry::find::<Transform>(&self.transform)
            .ok_or_else(|| dkg_core::Error::NotFound(format!("transform `{}`", self.transform)))
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
