//! Process-level configuration shared by the server and CLI.
//!
//! This is *deployment* configuration (where the databases live), distinct
//! from the user-authored Trongoscript configs handled by the `dkg-config`
//! crate. Values are read from the environment so the same binary runs in
//! dev and production without recompilation.

/// Connection settings for the backing stores.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Document store URL (feature A). The default `dkg://<path>` scheme
    /// opens the composite OpenDAL/Fjall/Tantivy store rooted at `<path>`.
    pub document_database_url: String,
    /// ClickHouse URL for the relational store (feature B), e.g.
    /// `clickhouse://localhost:8123/kg`. `None` disables feature B — the
    /// server runs documents-only.
    pub graph_database_url: Option<String>,
    /// Directory of Trongoscript config entities (tables, relationships,
    /// endpoints) to load on boot. `None` skips config loading.
    pub config_dir: Option<String>,
    /// Address the HTTP server binds to, e.g. `0.0.0.0:8080`.
    pub bind_address: String,
    /// A fixed API key inbound requests must present, or `None` to disable
    /// inbound auth (the historical default: anyone who can reach the port
    /// can read/write every table). Set via `DKG_API_KEY`. `/health` is
    /// exempt regardless.
    pub api_key: Option<String>,
}

impl Settings {
    /// Load settings from the environment.
    ///
    /// Reads `DKG_DOCUMENT_DATABASE_URL`, `DKG_GRAPH_DATABASE_URL`,
    /// `DKG_CONFIG_DIR`, `DKG_BIND_ADDRESS`, and `DKG_API_KEY`, applying
    /// sensible local-development defaults for any that are unset. The graph
    /// URL, config directory, and API key are optional — unset means that
    /// feature is off.
    ///
    /// # Examples
    ///
    /// ```
    /// // With nothing in the environment, defaults apply.
    /// std::env::remove_var("DKG_BIND_ADDRESS");
    /// std::env::remove_var("DKG_GRAPH_DATABASE_URL");
    /// std::env::remove_var("DKG_API_KEY");
    /// let settings = dkg_core::Settings::from_env();
    /// assert_eq!(settings.bind_address, "127.0.0.1:8080");
    /// assert!(settings.graph_database_url.is_none());
    /// assert!(settings.api_key.is_none());
    /// ```
    pub fn from_env() -> Self {
        Settings {
            document_database_url: env_or("DKG_DOCUMENT_DATABASE_URL", "dkg://./data"),
            graph_database_url: std::env::var("DKG_GRAPH_DATABASE_URL").ok(),
            config_dir: std::env::var("DKG_CONFIG_DIR").ok(),
            bind_address: env_or("DKG_BIND_ADDRESS", "127.0.0.1:8080"),
            api_key: std::env::var("DKG_API_KEY").ok().filter(|k| !k.is_empty()),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
