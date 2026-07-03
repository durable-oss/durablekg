//! Resolving upstream-auth secrets by name.
//!
//! An endpoint's `auth` config references a credential by *name*, never by
//! value. A [`SecretStore`] maps that name to the actual secret at call time,
//! so credentials stay out of the config (and out of version control, logs,
//! and the cached document).

use std::collections::HashMap;

/// Resolves a secret name to its value.
///
/// Implementations should treat misses as "not configured" (return `None`),
/// not as an error — the caller decides what a missing secret means (here, a
/// failed endpoint call rather than an unauthenticated upstream request).
pub trait SecretStore: Send + Sync {
    /// The secret value for `name`, or `None` if unset.
    fn get(&self, name: &str) -> Option<String>;
}

/// Reads secrets from environment variables, namespaced under `DKG_SECRET_`.
///
/// A config `secret` of `"WEATHER_TOKEN"` resolves to the env var
/// `DKG_SECRET_WEATHER_TOKEN`. The prefix keeps endpoint configs from
/// reaching arbitrary process environment.
#[derive(Debug, Clone, Default)]
pub struct EnvSecrets;

impl EnvSecrets {
    /// The environment-variable prefix.
    pub const PREFIX: &'static str = "DKG_SECRET_";

    /// Construct an env-backed secret store.
    pub fn new() -> Self {
        EnvSecrets
    }
}

impl SecretStore for EnvSecrets {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(format!("{}{name}", Self::PREFIX)).ok()
    }
}

/// An in-memory secret store, handy for tests and for callers that source
/// secrets themselves.
#[derive(Debug, Clone, Default)]
pub struct MapSecrets {
    secrets: HashMap<String, String>,
}

impl MapSecrets {
    /// Build from name/value pairs.
    pub fn new<K, V>(items: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        MapSecrets {
            secrets: items
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl SecretStore for MapSecrets {
    fn get(&self, name: &str) -> Option<String> {
        self.secrets.get(name).cloned()
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
