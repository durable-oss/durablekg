//! The document data model.
//!
//! A [`Document`] is an opaque JSON `body` plus queryable JSON `metadata`,
//! grouped into a named `collection`, and optionally reachable by one or
//! more caller-assigned [`Urn`] search ids. The composite store keeps the
//! body in object storage, the metadata and URN indexes in an embedded
//! key/value store, and a full-text index over the body for text search.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use dkg_core::Error;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A Uniform Resource Name used as a caller-assigned search id, e.g.
/// `urn:article:2026-06-18-hello`.
///
/// URNs let callers fetch a document by a stable, meaningful key of their
/// own rather than only by the store-assigned [`Uuid`]. The store keeps a
/// `urn → id` index, and the `urn:<namespace>:` prefix supports listing a
/// whole namespace.
///
/// Only the three-part `urn:<namespace>:<id>` shape is accepted. `namespace`
/// and `id` must be non-empty; `namespace` may not contain a colon (so the
/// namespace prefix is unambiguous), while `id` may.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Urn {
    namespace: String,
    id: String,
}

impl Urn {
    /// Construct a URN from its namespace and id parts.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] if either part is empty or the namespace
    /// contains a colon.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_documents::Urn;
    ///
    /// let urn = Urn::new("article", "2026-06-18-hello").unwrap();
    /// assert_eq!(urn.to_string(), "urn:article:2026-06-18-hello");
    /// assert_eq!(urn.namespace(), "article");
    /// ```
    pub fn new(namespace: impl Into<String>, id: impl Into<String>) -> Result<Self, Error> {
        let namespace = namespace.into();
        let id = id.into();
        if namespace.is_empty() || id.is_empty() {
            return Err(Error::Invalid(
                "URN namespace and id must be non-empty".into(),
            ));
        }
        if namespace.contains(':') {
            return Err(Error::Invalid(
                "URN namespace must not contain a colon".into(),
            ));
        }
        Ok(Urn { namespace, id })
    }

    /// The namespace component.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The id component.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The key prefix that selects every URN in this URN's namespace:
    /// `urn:<namespace>:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_documents::Urn;
    ///
    /// let urn = Urn::new("article", "x").unwrap();
    /// assert_eq!(Urn::namespace_prefix("article"), "urn:article:");
    /// assert!(urn.to_string().starts_with(&Urn::namespace_prefix("article")));
    /// ```
    pub fn namespace_prefix(namespace: &str) -> String {
        format!("urn:{namespace}:")
    }
}

impl fmt::Display for Urn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "urn:{}:{}", self.namespace, self.id)
    }
}

impl FromStr for Urn {
    type Err = Error;

    /// Parse a `urn:<namespace>:<id>` string.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] if the string is not a well-formed URN.
    fn from_str(s: &str) -> Result<Self, Error> {
        // Split into exactly three parts: "urn", namespace, and the rest
        // (which may itself contain colons).
        let mut parts = s.splitn(3, ':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("urn"), Some(namespace), Some(id)) => Urn::new(namespace, id),
            _ => Err(Error::Invalid(format!(
                "not a urn:namespace:id value: `{s}`"
            ))),
        }
    }
}

impl Serialize for Urn {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Urn {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// A stored document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Document {
    /// Stable store-assigned identifier.
    pub id: Uuid,
    /// Logical grouping, e.g. a tenant or dataset name. Indexed.
    pub collection: String,
    /// Caller-assigned search ids this document is reachable by.
    #[serde(default)]
    pub search_ids: Vec<Urn>,
    /// The document payload. Opaque to the store, stored as a blob.
    pub body: serde_json::Value,
    /// Queryable annotations. Indexed for metadata and full-text search.
    pub metadata: serde_json::Value,
    /// When the document was first stored.
    pub created_at: DateTime<Utc>,
    /// When the document was last replaced.
    pub updated_at: DateTime<Utc>,
}

/// The fields a caller provides when creating or replacing a document.
///
/// The store owns `id`, `created_at`, and `updated_at`; everything a caller
/// controls lives here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct NewDocument {
    /// Logical grouping the document belongs to.
    pub collection: String,
    /// Caller-assigned search ids. Each must be unique across the store;
    /// creating a document with a URN already in use is rejected.
    #[serde(default)]
    pub search_ids: Vec<Urn>,
    /// The document payload.
    pub body: serde_json::Value,
    /// Queryable annotations. Defaults to an empty object when omitted.
    #[serde(default = "empty_object")]
    pub metadata: serde_json::Value,
}

fn empty_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// A search request against the document store.
///
/// Criteria are ANDed. With no `text` and an empty `metadata_contains`, the
/// query lists documents (optionally within a collection) up to `limit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    /// Restrict to a single collection. `None` searches all collections.
    pub collection: Option<String>,
    /// Full-text query over document body and metadata text. `None` skips
    /// the full-text index and lists by metadata/collection instead.
    #[serde(default)]
    pub text: Option<String>,
    /// Require these key/value pairs to be present in `metadata`.
    #[serde(default = "empty_object")]
    pub metadata_contains: serde_json::Value,
    /// Maximum number of results to return.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    50
}

impl Default for Query {
    /// A list-everything query with the default limit and a match-all
    /// (empty-object) metadata filter. Hand-written rather than derived so
    /// `limit` is the real default (not `0`) and `metadata_contains` is an
    /// empty object (not JSON `null`), which is what the containment check
    /// treats as match-all.
    fn default() -> Self {
        Query {
            collection: None,
            text: None,
            metadata_contains: empty_object(),
            limit: default_limit(),
        }
    }
}

impl Query {
    /// A query that lists everything in `collection`, up to the default
    /// limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_documents::Query;
    ///
    /// let q = Query::in_collection("articles");
    /// assert_eq!(q.collection.as_deref(), Some("articles"));
    /// assert_eq!(q.limit, 50);
    /// ```
    pub fn in_collection(collection: impl Into<String>) -> Self {
        Query {
            collection: Some(collection.into()),
            text: None,
            metadata_contains: empty_object(),
            limit: default_limit(),
        }
    }

    /// A full-text query.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_documents::Query;
    ///
    /// let q = Query::full_text("rust embedded database");
    /// assert_eq!(q.text.as_deref(), Some("rust embedded database"));
    /// ```
    pub fn full_text(text: impl Into<String>) -> Self {
        Query {
            collection: None,
            text: Some(text.into()),
            metadata_contains: empty_object(),
            limit: default_limit(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urn_round_trips_through_string() {
        let urn = Urn::new("article", "a:b:c").unwrap();
        assert_eq!(urn.to_string(), "urn:article:a:b:c");
        let parsed: Urn = "urn:article:a:b:c".parse().unwrap();
        assert_eq!(parsed, urn);
        assert_eq!(parsed.id(), "a:b:c");
    }

    #[test]
    fn urn_rejects_malformed_input() {
        assert!("article:x".parse::<Urn>().is_err());
        assert!("urn:article".parse::<Urn>().is_err());
        assert!(Urn::new("", "x").is_err());
        assert!(Urn::new("a:b", "x").is_err());
    }

    #[test]
    fn urn_serializes_as_a_plain_string() {
        let urn = Urn::new("article", "hello").unwrap();
        let json = serde_json::to_string(&urn).unwrap();
        assert_eq!(json, "\"urn:article:hello\"");
        let back: Urn = serde_json::from_str(&json).unwrap();
        assert_eq!(back, urn);
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
