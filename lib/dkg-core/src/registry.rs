//! Inventory-backed plugin registries.
//!
//! DurableKG is built to grow: new document-storage backends, new
//! knowledge-graph backends, new config kinds, and new API output formats
//! should all be addable without editing a central match arm. We lean on
//! the [`inventory`] crate for this. Each extension point declares a
//! *registration* struct and submits instances with [`inventory::submit!`];
//! lookups collect those instances at startup.
//!
//! This module provides the small shared machinery: the [`Named`] trait
//! that every registration implements, and the generic [`find`] / [`names`]
//! helpers that work over any inventory-collected registration type.
//!
//! # Defining an extension point
//!
//! ```
//! use dkg_core::registry::Named;
//!
//! /// One registered greeting strategy.
//! pub struct Greeter {
//!     pub name: &'static str,
//!     pub greet: fn(&str) -> String,
//! }
//!
//! impl Named for Greeter {
//!     fn name(&self) -> &str {
//!         self.name
//!     }
//! }
//!
//! inventory::collect!(Greeter);
//!
//! // Elsewhere (often in another crate), register an implementation:
//! inventory::submit! {
//!     Greeter { name: "polite", greet: |who| format!("Hello, {who}.") }
//! }
//!
//! // And look it up by name:
//! let g = dkg_core::registry::find::<Greeter>("polite").unwrap();
//! assert_eq!((g.greet)("world"), "Hello, world.");
//! ```

/// Implemented by every registration struct so the generic lookup helpers
/// can ask "what name does this register under?".
pub trait Named: Sync + 'static {
    /// The unique name this registration is found by.
    fn name(&self) -> &str;
}

/// Find the registration of type `T` submitted under `name`.
///
/// Returns `None` if nothing was registered under that name. When two
/// registrations share a name the first one collected wins; registering
/// duplicate names is a programming error.
///
/// # Examples
///
/// See the [module-level example](self).
pub fn find<T>(name: &str) -> Option<&'static T>
where
    T: Named,
    inventory::iter<T>: IntoIterator<Item = &'static T>,
{
    inventory::iter::<T>.into_iter().find(|r| r.name() == name)
}

/// List the names of every registration of type `T`, in collection order.
///
/// Useful for help text, validation messages ("known backends: …"), and
/// admin endpoints that enumerate available plugins.
///
/// # Examples
///
/// ```
/// use dkg_core::registry::{names, Named};
///
/// struct Format {
///     id: &'static str,
/// }
/// impl Named for Format {
///     fn name(&self) -> &str {
///         self.id
///     }
/// }
/// inventory::collect!(Format);
/// inventory::submit! { Format { id: "json" } }
/// inventory::submit! { Format { id: "yaml" } }
///
/// let mut got = names::<Format>();
/// got.sort();
/// assert_eq!(got, vec!["json", "yaml"]);
/// ```
pub fn names<T>() -> Vec<&'static str>
where
    T: Named,
    inventory::iter<T>: IntoIterator<Item = &'static T>,
{
    inventory::iter::<T>.into_iter().map(|r| r.name()).collect()
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
