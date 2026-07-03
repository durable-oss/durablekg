//! Projecting a resolved entity onto a graph table.
//!
//! [`resolve`](crate::resolve::resolve) produces a dimension→value map that
//! does not know or care about the schema. [`to_record`] is where that map
//! meets the target [`Table`]: it keeps only the dimensions that are real
//! columns and drops the rest (facts may claim dimensions a table has not
//! declared — that is the "accept any dimension" contract). The result is a
//! [`Record`](dkg_graph::Record) ready to be upserted.
//!
//! The `id` column is **not** written here. `id` is system-owned: it is minted
//! or resolved from the entity's aliases at the sync boundary (see
//! [`crate::sync`]), never taken from the URN. A user-supplied alias carried in
//! the URN (e.g. `urn:businesses:slug:acme`) is folded into the record when its
//! keytype is a declared alternate-key column and its value is non-blank.

use dkg_config::Table;
use dkg_graph::Record;
use serde_json::Value;

use crate::resolve::ResolvedEntity;

/// True if `value` is a blank alias: JSON null, or a string that is empty or
/// whitespace-only. Blank aliases are never written or registered (prospect
/// data is full of `""` placeholders, which would otherwise all collide).
pub fn is_blank_alias(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.trim().is_empty(),
        _ => false,
    }
}

/// Project a resolved entity onto `table`, returning a record of every resolved
/// dimension that is a declared column of `table`. The system `id` is not
/// written (it is assigned at the sync boundary).
///
/// Dimensions with no matching column are silently dropped — a fact about a
/// dimension the schema does not (yet) model is stored and resolvable, it just
/// does not reach the row until a column for it exists.
///
/// `url_alias` is the entity URN's typed alias `(keytype, value)` (e.g.
/// `("slug", "acme")`). When `keytype` is a declared alternate-key column of
/// `table` and `value` is non-blank, it is folded into the record so addressing
/// a row by `urn:businesses:slug:acme` seeds `slug=acme` even with no slug
/// fact. A `keytype` of `id` is ignored here (the system owns `id`).
///
/// # Examples
///
/// ```
/// use dkg_config::{Column, ColumnType, Table};
/// use dkg_entity_fact_resolver::{resolve, to_record};
/// use dkg_facts::{Authority, Fact, SourceType, Urn};
/// use chrono::{TimeZone, Utc};
/// use uuid::Uuid;
///
/// let table = Table {
///     name: "businesses".into(),
///     columns: vec![
///         Column { name: "slug".into(), column_type: ColumnType::String, nullable: false, array: false },
///         Column { name: "phone".into(), column_type: ColumnType::String, nullable: true, array: false },
///     ],
///     alternate_keys: vec!["slug".into()],
/// };
/// let fact = Fact {
///     id: Uuid::nil(),
///     entity: Urn::new("businesses", "slug:acme").unwrap(),
///     dimension: "phone".into(),
///     value: serde_json::json!("603-555-1234"),
///     authority: Authority { source_type: SourceType::Manual, source_url: None, model: None, confidence: 1.0 },
///     observed_at: Utc.timestamp_opt(0, 0).unwrap(),
///     recorded_at: Utc.timestamp_opt(0, 0).unwrap(),
/// };
/// let resolved = resolve(&[fact]);
///
/// // The URN alias `slug:acme` is folded in; the phone fact is kept; `id` is
/// // not written (it is assigned at the sync boundary).
/// let record = to_record(&resolved, &table, Some(("slug", "acme")));
/// assert_eq!(record.get("slug").unwrap(), &serde_json::json!("acme"));
/// assert_eq!(record.get("phone").unwrap(), &serde_json::json!("603-555-1234"));
/// assert!(!record.contains_key("id"));
/// ```
pub fn to_record(
    resolved: &ResolvedEntity,
    table: &Table,
    url_alias: Option<(&str, &str)>,
) -> Record {
    let mut record = Record::new();

    for column in &table.columns {
        if let Some(dim) = resolved.dimensions.get(&column.name) {
            record.insert(column.name.clone(), dim.value.clone());
        }
    }

    // Fold the URN's typed alias into the record when it names an alternate-key
    // column and is non-blank. A resolved fact for the same column wins (it is
    // already present), so this only fills a column the facts left empty.
    if let Some((keytype, value)) = url_alias {
        if keytype != Table::ID_COLUMN
            && table.alternate_keys.iter().any(|k| k == keytype)
            && !value.trim().is_empty()
            && !record.contains_key(keytype)
        {
            record.insert(keytype.to_string(), Value::String(value.to_string()));
        }
    }

    record
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::ResolvedDimension;
    use dkg_config::{Column, ColumnType};
    use dkg_facts::{Authority, SourceType};

    fn col(name: &str, ty: ColumnType) -> Column {
        Column {
            name: name.into(),
            column_type: ty,
            nullable: true,
            array: false,
        }
    }

    fn dim(value: serde_json::Value) -> ResolvedDimension {
        ResolvedDimension {
            value,
            authority: Authority {
                source_type: SourceType::Manual,
                source_url: None,
                model: None,
                confidence: 1.0,
            },
        }
    }

    fn businesses() -> Table {
        Table {
            name: "businesses".into(),
            columns: vec![
                col("slug", ColumnType::String),
                col("phone", ColumnType::String),
                col("industry", ColumnType::String),
            ],
            alternate_keys: vec!["slug".into()],
        }
    }

    #[test]
    fn keeps_columns_and_drops_unknown_dimensions() {
        let mut resolved = ResolvedEntity::default();
        resolved
            .dimensions
            .insert("phone".into(), dim(serde_json::json!("603-555-1234")));
        // Not a column on the table:
        resolved
            .dimensions
            .insert("founder".into(), dim(serde_json::json!("Jane")));

        let record = to_record(&resolved, &businesses(), None);

        assert_eq!(
            record.get("phone").unwrap(),
            &serde_json::json!("603-555-1234")
        );
        // `industry` had no fact; `founder` had no column — neither appears.
        assert!(!record.contains_key("industry"));
        assert!(!record.contains_key("founder"));
        // `id` is never written here.
        assert!(!record.contains_key("id"));
    }

    #[test]
    fn folds_url_alias_when_it_is_an_alternate_key() {
        let record = to_record(
            &ResolvedEntity::default(),
            &businesses(),
            Some(("slug", "acme")),
        );
        assert_eq!(record.get("slug").unwrap(), &serde_json::json!("acme"));
    }

    #[test]
    fn ignores_url_alias_that_is_id_or_blank_or_not_a_key() {
        // `id` keytype is never folded.
        let r1 = to_record(&ResolvedEntity::default(), &businesses(), Some(("id", "x")));
        assert!(r1.is_empty());
        // blank value is skipped.
        let r2 = to_record(
            &ResolvedEntity::default(),
            &businesses(),
            Some(("slug", "  ")),
        );
        assert!(!r2.contains_key("slug"));
        // a keytype that is not a declared alternate key is skipped.
        let r3 = to_record(
            &ResolvedEntity::default(),
            &businesses(),
            Some(("phone", "x")),
        );
        assert!(!r3.contains_key("phone"));
    }

    #[test]
    fn fact_wins_over_url_alias_for_same_column() {
        let mut resolved = ResolvedEntity::default();
        resolved
            .dimensions
            .insert("slug".into(), dim(serde_json::json!("from-fact")));
        let record = to_record(&resolved, &businesses(), Some(("slug", "from-url")));
        assert_eq!(record.get("slug").unwrap(), &serde_json::json!("from-fact"));
    }

    #[test]
    fn is_blank_alias_detects_empty_and_null() {
        assert!(is_blank_alias(&Value::Null));
        assert!(is_blank_alias(&serde_json::json!("")));
        assert!(is_blank_alias(&serde_json::json!("   ")));
        assert!(!is_blank_alias(&serde_json::json!("acme")));
        assert!(!is_blank_alias(&serde_json::json!(42)));
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
