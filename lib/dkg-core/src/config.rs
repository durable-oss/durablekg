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
    /// Address the HTTP server binds to: either `host:port` (e.g.
    /// `0.0.0.0:8080`) or `unix:<path>` for a Unix domain socket. Parse it
    /// with [`Settings::bind`].
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

    /// Parse [`bind_address`](Self::bind_address) into what the server should
    /// listen on.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_core::BindAddress;
    /// let mut settings = dkg_core::Settings::from_env();
    /// settings.bind_address = "unix:/tmp/dkg.sock".into();
    /// assert_eq!(
    ///     settings.bind(),
    ///     BindAddress::Unix(std::path::PathBuf::from("/tmp/dkg.sock"))
    /// );
    /// ```
    pub fn bind(&self) -> BindAddress {
        BindAddress::parse(&self.bind_address)
    }
}

/// Where the HTTP server listens.
///
/// A Unix socket avoids the loopback TCP stack and, more usefully, gets its
/// access control from the filesystem: the socket's directory permissions
/// decide who can reach the API, which is a stronger default than an open
/// port on a shared host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindAddress {
    /// A TCP `host:port`, resolved by the listener.
    Tcp(String),
    /// A Unix domain socket at this path.
    Unix(std::path::PathBuf),
}

impl BindAddress {
    /// Parse a bind string. `unix:<path>` selects a Unix socket; anything
    /// else is treated as a TCP address and validated when the listener
    /// binds, so an unparseable host:port fails there with its own error
    /// rather than being silently reinterpreted here.
    ///
    /// # Examples
    ///
    /// ```
    /// use dkg_core::BindAddress;
    /// assert_eq!(
    ///     BindAddress::parse("0.0.0.0:8080"),
    ///     BindAddress::Tcp("0.0.0.0:8080".into())
    /// );
    /// assert_eq!(
    ///     BindAddress::parse("unix:./run/dkg.sock"),
    ///     BindAddress::Unix("./run/dkg.sock".into())
    /// );
    /// ```
    pub fn parse(value: &str) -> Self {
        match value.strip_prefix("unix:") {
            Some(path) => BindAddress::Unix(std::path::PathBuf::from(path)),
            None => BindAddress::Tcp(value.to_string()),
        }
    }
}

impl std::fmt::Display for BindAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindAddress::Tcp(addr) => write!(f, "{addr}"),
            BindAddress::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_host_port_is_tcp() {
        assert_eq!(
            BindAddress::parse("127.0.0.1:8080"),
            BindAddress::Tcp("127.0.0.1:8080".into())
        );
    }

    #[test]
    fn the_unix_prefix_selects_a_socket_path() {
        assert_eq!(
            BindAddress::parse("unix:/var/run/dkg.sock"),
            BindAddress::Unix("/var/run/dkg.sock".into())
        );
    }

    #[test]
    fn a_socket_path_may_be_relative() {
        assert_eq!(
            BindAddress::parse("unix:dkg.sock"),
            BindAddress::Unix("dkg.sock".into())
        );
    }

    #[test]
    fn a_path_containing_a_colon_survives_parsing() {
        // Only the first `unix:` is a prefix; the rest is the path verbatim.
        assert_eq!(
            BindAddress::parse("unix:/tmp/a:b/dkg.sock"),
            BindAddress::Unix("/tmp/a:b/dkg.sock".into())
        );
    }

    #[test]
    fn display_round_trips_through_parse() {
        for raw in ["0.0.0.0:8080", "unix:/tmp/dkg.sock"] {
            let parsed = BindAddress::parse(raw);
            assert_eq!(parsed.to_string(), raw);
            assert_eq!(BindAddress::parse(&parsed.to_string()), parsed);
        }
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
