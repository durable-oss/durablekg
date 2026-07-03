//! A loaded, validated relational schema.
//!
//! A [`Schema`] is the whole set of [`Table`]s and [`Relationship`]s for a
//! deployment, assembled from individually-authored Trongoscript entities
//! and validated together **once on load**. Validation is the only place
//! cross-entity rules are checked: that every relationship names tables that
//! exist, that a `belongs_to` / `has_many` foreign key is a real column, and
//! that table names are unique.
//!
//! The schema is additive at runtime. New tables, columns, and relationships
//! are reconciled into storage on load; nothing is ever dropped here.
//! Removing a table or column from config is a no-op — cleanup is an
//! out-of-band operation.

use std::collections::HashMap;

use dkg_config::{Relationship, Table};
use dkg_core::{Error, Result};

/// A validated collection of tables and the relationships among them.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    tables: HashMap<String, Table>,
    relationships: Vec<Relationship>,
}

impl Schema {
    /// Build and validate a schema from its tables and relationships.
    ///
    /// This is the single load-time validation point. It checks:
    ///
    /// - no two tables share a name;
    /// - every relationship's `from` and `to` name a defined table;
    /// - a `belongs_to` foreign key column exists on `from`, and a
    ///   `has_many` foreign key column exists on `to`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] describing the first problem found.
    pub fn load(tables: Vec<Table>, relationships: Vec<Relationship>) -> Result<Self> {
        let mut by_name: HashMap<String, Table> = HashMap::with_capacity(tables.len());
        for table in tables {
            if by_name.insert(table.name.clone(), table.clone()).is_some() {
                return Err(Error::config(format!("duplicate table `{}`", table.name)));
            }
        }

        for rel in &relationships {
            let from = by_name.get(&rel.from).ok_or_else(|| {
                Error::config(format!(
                    "relationship references unknown table `{}`",
                    rel.from
                ))
            })?;
            let to = by_name.get(&rel.to).ok_or_else(|| {
                Error::config(format!(
                    "relationship references unknown table `{}`",
                    rel.to
                ))
            })?;

            use dkg_config::RelationKind::*;
            match rel.kind {
                BelongsTo => require_key_column(from, rel.key.as_deref(), &rel.from)?,
                HasMany => require_key_column(to, rel.key.as_deref(), &rel.to)?,
                ManyToMany => {} // join table is generated; no FK column needed
            }
        }

        Ok(Schema {
            tables: by_name,
            relationships,
        })
    }

    /// Look up a table by name.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if no such table is defined.
    pub fn table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| Error::NotFound(format!("table `{name}`")))
    }

    /// All tables, in unspecified order.
    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.values()
    }

    /// All relationships.
    pub fn relationships(&self) -> &[Relationship] {
        &self.relationships
    }

    /// Find the relationship from `from` to `to`, if one is defined (in
    /// either declared direction for many-to-many).
    pub fn relationship(&self, from: &str, to: &str) -> Option<&Relationship> {
        self.relationships.iter().find(|r| {
            (r.from == from && r.to == to)
                || (matches!(r.kind, dkg_config::RelationKind::ManyToMany)
                    && r.from == to
                    && r.to == from)
        })
    }
}

/// Ensure `key` names an existing column on `table`.
fn require_key_column(table: &Table, key: Option<&str>, table_name: &str) -> Result<()> {
    let key = key.ok_or_else(|| Error::config("relationship is missing its `:key` column"))?;
    if table.columns.iter().any(|c| c.name == key) {
        Ok(())
    } else {
        Err(Error::config(format!(
            "relationship key `{key}` is not a column on table `{table_name}`"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dkg_config::{Column, ColumnType, RelationKind};

    fn col(name: &str, ty: ColumnType) -> Column {
        Column {
            name: name.into(),
            column_type: ty,
            nullable: false,
            array: false,
        }
    }

    fn people() -> Table {
        Table {
            name: "people".into(),
            columns: vec![
                col("name", ColumnType::String),
                col("business_id", ColumnType::Uuid),
            ],
            alternate_keys: Vec::new(),
        }
    }

    fn businesses() -> Table {
        Table {
            name: "businesses".into(),
            columns: vec![col("name", ColumnType::String)],
            alternate_keys: Vec::new(),
        }
    }

    #[test]
    fn loads_valid_schema() {
        let schema = Schema::load(
            vec![people(), businesses()],
            vec![Relationship {
                kind: RelationKind::BelongsTo,
                from: "people".into(),
                to: "businesses".into(),
                key: Some("business_id".into()),
            }],
        )
        .unwrap();
        assert_eq!(schema.table("people").unwrap().columns.len(), 2);
        assert!(schema.relationship("people", "businesses").is_some());
    }

    #[test]
    fn rejects_relationship_to_unknown_table() {
        let err = Schema::load(
            vec![people()],
            vec![Relationship {
                kind: RelationKind::BelongsTo,
                from: "people".into(),
                to: "businesses".into(),
                key: Some("business_id".into()),
            }],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_belongs_to_with_missing_fk_column() {
        let err = Schema::load(
            vec![people(), businesses()],
            vec![Relationship {
                kind: RelationKind::BelongsTo,
                from: "people".into(),
                to: "businesses".into(),
                key: Some("nonexistent".into()),
            }],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn rejects_duplicate_table() {
        let err = Schema::load(vec![people(), people()], vec![]).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn m2m_relationship_resolves_either_direction() {
        let schema = Schema::load(
            vec![people(), businesses()],
            vec![Relationship {
                kind: RelationKind::ManyToMany,
                from: "people".into(),
                to: "businesses".into(),
                key: None,
            }],
        )
        .unwrap();
        assert!(schema.relationship("businesses", "people").is_some());
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
