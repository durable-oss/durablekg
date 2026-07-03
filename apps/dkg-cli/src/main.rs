//! DurableKG CLI (`dkg`).
//!
//! Subcommands:
//!   dkg config validate <FILE>      Evaluate and validate a Trongoscript config.
//!   dkg config kinds                List the registered config kinds.
//!   dkg doc put <FILE> [--collection C] [--search-id URN ...]   Store a JSON document.
//!   dkg doc get <ID|URN>            Fetch a document by id or URN search id.
//!   dkg doc search [--text Q] [--collection C] [--limit N]      Search documents.
//!   dkg doc repair                  Reconcile the store after a crash.
//!   dkg schema reconcile            Reconcile the config dir into ClickHouse.
//!   dkg table insert <T> <FILE>     Insert records into a table.
//!   dkg table query <T> [--filter c<op>v ...] [--order-by [-]c] [--offset N]   Query a table.
//!   dkg table count <T> [--filter c<op>v ...]   Count rows matching a filter.
//!   dkg table related <T> <ID> <OTHER>   Traverse a relationship.
//!   dkg endpoint call <NAME> [--query k=v] [--header N:v]   Call a caching endpoint (C).
//!
//! Document subcommands open the document store at `--database-url` (or
//! `DKG_DOCUMENT_DATABASE_URL`, default `dkg://./data`). Table/schema
//! subcommands open ClickHouse at `--graph-url` (`DKG_GRAPH_DATABASE_URL`)
//! and load tables from `--config-dir` (`DKG_CONFIG_DIR`). Config validate
//! subcommands are offline.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use dkg_documents::{NewDocument, Query, Urn};
use uuid::Uuid;

/// Shared flags for relational (feature B) commands.
#[derive(clap::Args, Debug)]
struct GraphOpts {
    /// ClickHouse URL, e.g. `clickhouse://localhost:8123/kg`.
    #[arg(long, env = "DKG_GRAPH_DATABASE_URL")]
    graph_url: String,
    /// Directory of `.trongo` table/relationship configs to load.
    #[arg(long, env = "DKG_CONFIG_DIR")]
    config_dir: PathBuf,
}

impl GraphOpts {
    /// Load + validate the schema and open the store.
    async fn connect(&self) -> Result<(Box<dyn dkg_graph::GraphStore>, dkg_graph::Schema)> {
        let store = dkg_graph::open(&self.graph_url).await?;
        Ok((store, self.schema()?))
    }

    /// Load + validate the schema without opening the store. Used by commands
    /// that only need the table shapes (e.g. resolving facts to a record).
    fn schema(&self) -> Result<dkg_graph::Schema> {
        let bundle = dkg_config::load_dir(&self.config_dir)?;
        Ok(dkg_graph::Schema::load(
            bundle.tables,
            bundle.relationships,
        )?)
    }
}

#[derive(Parser, Debug)]
#[command(name = "dkg", version, about = "DurableKG command-line interface")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Work with Trongoscript configuration files.
    #[command(subcommand)]
    Config(ConfigCmd),
    /// Work with documents in the store.
    #[command(subcommand)]
    Doc(DocCmd),
    /// Work with relational tables (feature B).
    #[command(subcommand)]
    Table(TableCmd),
    /// Manage the relational schema (feature B).
    #[command(subcommand)]
    Schema(SchemaCmd),
    /// Call a user-defined caching endpoint (feature C).
    #[command(subcommand)]
    Endpoint(EndpointCmd),
    /// Record, inspect, and resolve facts about graph entities.
    #[command(subcommand)]
    Fact(FactCmd),
}

#[derive(Subcommand, Debug)]
enum EndpointCmd {
    /// Call an endpoint by name: return a fresh cached result or fetch the
    /// upstream, cache it, and return it.
    Call {
        /// Endpoint name (must exist in the config directory).
        name: String,
        /// Query param as `key=value` (repeatable).
        #[arg(long = "query")]
        query: Vec<String>,
        /// Request header as `Name: value` (repeatable).
        #[arg(long = "header")]
        header: Vec<String>,
        /// Directory of `.trongo` endpoint configs.
        #[arg(long, env = "DKG_CONFIG_DIR")]
        config_dir: PathBuf,
        /// Document store root URL (where results are cached).
        #[arg(
            long,
            env = "DKG_DOCUMENT_DATABASE_URL",
            default_value = "dkg://./data"
        )]
        database_url: String,
    },
}

/// The document store URL facts are kept in (facts ride on feature A).
#[derive(clap::Args, Debug)]
struct DocStoreOpts {
    /// Store root URL.
    #[arg(
        long,
        env = "DKG_DOCUMENT_DATABASE_URL",
        default_value = "dkg://./data"
    )]
    database_url: String,
}

#[derive(Subcommand, Debug)]
enum FactCmd {
    /// Record one fact about an entity.
    Record {
        /// Entity URN, `urn:<table>:<id>` (e.g. `urn:businesses:acme`).
        #[arg(long)]
        entity: Urn,
        /// The dimension (column) the fact claims, e.g. `phone`.
        #[arg(long)]
        dimension: String,
        /// The claimed value, parsed as JSON when it parses, else a string.
        #[arg(long)]
        value: String,
        /// Source type: web_search | website_scrape | directory_lookup |
        /// manual | inference.
        #[arg(long, default_value = "manual")]
        source: String,
        /// Confidence in `0.0..=1.0`. Higher wins on conflict.
        #[arg(long, default_value_t = 1.0)]
        confidence: f32,
        /// Originating URL, when there is one.
        #[arg(long)]
        source_url: Option<String>,
        /// Model that produced the fact, when machine-generated.
        #[arg(long)]
        model: Option<String>,
        #[command(flatten)]
        store: DocStoreOpts,
    },
    /// Record many facts at once from NDJSON bundles (one JSON object per
    /// line), reading a file or stdin. Each bundle records several dimensions
    /// of one entity under a single shared authority and observation time, so
    /// the entity URN, provenance, and timestamps are stored once per source
    /// instead of once per column — the efficient path for bulk loads.
    ///
    /// Each line is an object:
    ///   {"entity":"urn:businesses:slug:acme","source":"manual",
    ///    "confidence":0.8,"source_url":null,"model":null,"observed_at":null,
    ///    "dimensions":{"name":"Acme Co","phone":"603-555-1234"}}
    /// `source`/`confidence`/`source_url`/`model`/`observed_at` are optional
    /// (defaults: manual / 1.0 / none / none / record-time).
    RecordBatch {
        /// NDJSON file to read; `-` or omitted reads stdin.
        #[arg(long)]
        input: Option<PathBuf>,
        #[command(flatten)]
        store: DocStoreOpts,
    },
    /// List facts about an entity, optionally narrowed to one dimension.
    List {
        /// Entity URN.
        #[arg(long)]
        entity: Urn,
        /// Restrict to one dimension.
        #[arg(long)]
        dimension: Option<String>,
        #[command(flatten)]
        store: DocStoreOpts,
    },
    /// Resolve an entity's facts into its overview record and print it.
    /// Reads only; does not write to the graph.
    Resolve {
        /// Entity URN. Its `<table>` namespace selects the target table.
        #[arg(long)]
        entity: Urn,
        #[command(flatten)]
        store: DocStoreOpts,
        /// Config dir to load the schema from (for the table shape).
        #[arg(long, env = "DKG_CONFIG_DIR")]
        config_dir: PathBuf,
    },
    /// Resolve an entity's facts and write the overview row into its graph
    /// table (feature B). Re-syncing replaces the row.
    Sync {
        /// Entity URN. Its `<table>` namespace selects the target table.
        #[arg(long)]
        entity: Urn,
        #[command(flatten)]
        store: DocStoreOpts,
        #[command(flatten)]
        graph: GraphOpts,
    },
}

#[derive(Subcommand, Debug)]
enum SchemaCmd {
    /// Load the config directory and reconcile it into ClickHouse
    /// (additive: create tables/columns/join tables, never drop).
    Reconcile {
        #[command(flatten)]
        graph: GraphOpts,
    },
}

#[derive(Subcommand, Debug)]
enum TableCmd {
    /// Insert records into a table from a JSON file (an object or array).
    Insert {
        /// Target table name.
        table: String,
        /// JSON file holding one record object or an array of them.
        file: PathBuf,
        #[command(flatten)]
        graph: GraphOpts,
    },
    /// Query a table, optionally filtered by one or more predicates (ANDed
    /// together).
    Query {
        /// Table name.
        table: String,
        /// Filter as `column<op>value`, where `<op>` is one of `=`, `!=`,
        /// `>=`, `<=`, `>`, `<`, `~` (in-list, value is comma-separated), or
        /// `*` (substring match, value may contain `%`/`_` wildcards). Value
        /// is parsed as JSON when it parses, else kept as a string. May be
        /// repeated to AND multiple predicates.
        #[arg(long)]
        filter: Vec<String>,
        /// Sort by `column` (ascending) or `-column` (descending).
        #[arg(long)]
        order_by: Option<String>,
        /// Rows to skip before collecting `limit` of them.
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Maximum rows.
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[command(flatten)]
        graph: GraphOpts,
    },
    /// Count the rows in a table matching zero or more predicates, without
    /// fetching them.
    Count {
        /// Table name.
        table: String,
        /// Filter as `column<op>value`; see `query --help` for the syntax.
        /// May be repeated to AND multiple predicates.
        #[arg(long)]
        filter: Vec<String>,
        #[command(flatten)]
        graph: GraphOpts,
    },
    /// Traverse a relationship from one row to related rows in another table.
    Related {
        /// Source table.
        table: String,
        /// Source row id (UUID).
        id: String,
        /// Related table to fetch.
        other: String,
        /// Maximum rows.
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[command(flatten)]
        graph: GraphOpts,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCmd {
    /// Evaluate and validate a Trongoscript config file.
    Validate {
        /// Path to the `.trongo` config file.
        file: PathBuf,
    },
    /// List the registered config kinds.
    Kinds,
}

#[derive(Subcommand, Debug)]
enum DocCmd {
    /// Store a JSON document read from a file.
    Put {
        /// Path to a JSON file holding the document body.
        file: PathBuf,
        /// Collection to store the document under.
        #[arg(long, default_value = "default")]
        collection: String,
        /// Caller-assigned URN search id (repeatable), e.g.
        /// `--search-id urn:article:hello`.
        #[arg(long = "search-id")]
        search_id: Vec<Urn>,
        /// Store root URL.
        #[arg(
            long,
            env = "DKG_DOCUMENT_DATABASE_URL",
            default_value = "dkg://./data"
        )]
        database_url: String,
    },
    /// Fetch a document by id or by URN search id.
    Get {
        /// The document id (UUID) or a `urn:namespace:id` search id.
        key: String,
        /// Store root URL.
        #[arg(
            long,
            env = "DKG_DOCUMENT_DATABASE_URL",
            default_value = "dkg://./data"
        )]
        database_url: String,
    },
    /// Search documents (full-text or by collection listing).
    Search {
        /// Full-text query over body and metadata. Omit to list.
        #[arg(long)]
        text: Option<String>,
        /// Restrict to a collection.
        #[arg(long)]
        collection: Option<String>,
        /// Maximum results.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Store root URL.
        #[arg(
            long,
            env = "DKG_DOCUMENT_DATABASE_URL",
            default_value = "dkg://./data"
        )]
        database_url: String,
    },
    /// Reconcile the store after an interrupted write or crash.
    Repair {
        /// Store root URL.
        #[arg(
            long,
            env = "DKG_DOCUMENT_DATABASE_URL",
            default_value = "dkg://./data"
        )]
        database_url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Config(cmd) => run_config(cmd),
        Command::Doc(cmd) => run_doc(cmd).await,
        Command::Table(cmd) => run_table(cmd).await,
        Command::Schema(cmd) => run_schema(cmd).await,
        Command::Endpoint(cmd) => run_endpoint(cmd).await,
        Command::Fact(cmd) => run_fact(cmd).await,
    }
}

async fn run_endpoint(cmd: EndpointCmd) -> Result<()> {
    match cmd {
        EndpointCmd::Call {
            name,
            query,
            header,
            config_dir,
            database_url,
        } => {
            let bundle = dkg_config::load_dir(&config_dir)?;
            let endpoint = bundle
                .endpoints
                .into_iter()
                .find(|e| e.name == name)
                .with_context(|| {
                    format!("no endpoint named `{name}` in {}", config_dir.display())
                })?;

            let request = dkg_endpoints::CallRequest {
                query: query
                    .iter()
                    .map(|s| split_pair(s, '='))
                    .collect::<Result<_>>()?,
                headers: header
                    .iter()
                    .map(|s| split_pair(s, ':'))
                    .collect::<Result<_>>()?,
                body: None,
            };

            let store = dkg_documents::open(&database_url).await?;
            let fetcher = dkg_endpoints::HttpFetcher::new();
            let secrets = dkg_endpoints::EnvSecrets::new();
            let body =
                dkg_endpoints::serve(store.as_ref(), &fetcher, &secrets, &endpoint, &request)
                    .await?;
            store.shutdown().await?;
            println!("{}", serde_json::to_string_pretty(&body)?);
            Ok(())
        }
    }
}

/// Parse a `--source` value into a [`SourceType`].
fn parse_source(s: &str) -> Result<dkg_facts::SourceType> {
    use dkg_facts::SourceType::*;
    Ok(match s {
        "web_search" => WebSearch,
        "website_scrape" => WebsiteScrape,
        "directory_lookup" => DirectoryLookup,
        "manual" => Manual,
        "inference" => Inference,
        other => anyhow::bail!(
            "unknown source `{other}` (expected one of: web_search, \
             website_scrape, directory_lookup, manual, inference)"
        ),
    })
}

/// One line of `fact record-batch` input: an entity, a shared authority, and
/// a flat map of dimension → value. Mirrors [`dkg_facts::NewBundle`] but takes
/// the authority fields flat (and `source` as a string) so the input is the
/// plain JSON an LLM or export script naturally emits.
#[derive(serde::Deserialize)]
struct BatchBundle {
    entity: Urn,
    #[serde(default)]
    aliases: Vec<Urn>,
    #[serde(default = "default_source")]
    source: String,
    #[serde(default = "default_confidence")]
    confidence: f32,
    #[serde(default)]
    source_url: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
    dimensions: serde_json::Map<String, serde_json::Value>,
}

fn default_source() -> String {
    "manual".to_string()
}
fn default_confidence() -> f32 {
    1.0
}

/// Parse one NDJSON line into a [`dkg_facts::NewBundle`], translating the flat
/// `source` string into a typed [`dkg_facts::SourceType`] and rejecting a
/// bundle that claims no dimensions.
fn parse_bundle(line: &str) -> Result<dkg_facts::NewBundle> {
    use dkg_facts::{Authority, NewBundle};
    let raw: BatchBundle = serde_json::from_str(line)?;
    if raw.dimensions.is_empty() {
        anyhow::bail!("bundle for `{}` claims no dimensions", raw.entity);
    }
    Ok(NewBundle {
        entity: raw.entity,
        aliases: raw.aliases,
        authority: Authority {
            source_type: parse_source(&raw.source)?,
            source_url: raw.source_url,
            model: raw.model,
            confidence: raw.confidence,
        },
        observed_at: raw.observed_at,
        dimensions: raw.dimensions,
    })
}

/// Parse a CLI string value as JSON when it parses, else as a bare string —
/// so `--value 42` is a number and `--value "Acme Co"` is a string.
fn parse_value(raw: &str) -> serde_json::Value {
    serde_json::from_str(raw).unwrap_or_else(|_| serde_json::Value::String(raw.to_string()))
}

async fn run_fact(cmd: FactCmd) -> Result<()> {
    use dkg_facts::{Authority, DocumentFactStore, FactStore, NewFact};
    use std::sync::Arc;

    match cmd {
        FactCmd::Record {
            entity,
            dimension,
            value,
            source,
            confidence,
            source_url,
            model,
            store,
        } => {
            let docs: Arc<dyn dkg_documents::DocumentStore> =
                Arc::from(dkg_documents::open(&store.database_url).await?);
            let facts = DocumentFactStore::new(Arc::clone(&docs));
            let recorded = facts
                .record(NewFact {
                    entity,
                    aliases: Vec::new(),
                    dimension,
                    value: parse_value(&value),
                    authority: Authority {
                        source_type: parse_source(&source)?,
                        source_url,
                        model,
                        confidence,
                    },
                    observed_at: None,
                })
                .await?;
            docs.shutdown().await?;
            println!("{}", recorded.id);
            Ok(())
        }
        FactCmd::RecordBatch { input, store } => {
            let docs: Arc<dyn dkg_documents::DocumentStore> =
                Arc::from(dkg_documents::open(&store.database_url).await?);
            let facts = DocumentFactStore::new(Arc::clone(&docs));

            let reader: Box<dyn std::io::BufRead> = match input.as_deref() {
                Some(p) if p != std::path::Path::new("-") => Box::new(std::io::BufReader::new(
                    std::fs::File::open(p)
                        .with_context(|| format!("opening batch input {}", p.display()))?,
                )),
                _ => Box::new(std::io::BufReader::new(std::io::stdin())),
            };

            let mut bundles = 0usize;
            let mut dimensions = 0usize;
            for (lineno, line) in std::io::BufRead::lines(reader).enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let bundle = parse_bundle(&line)
                    .with_context(|| format!("parsing bundle on line {}", lineno + 1))?;
                let recorded = facts.record_bundle(bundle).await?;
                dimensions += recorded.len();
                bundles += 1;
            }
            docs.shutdown().await?;
            println!("recorded {bundles} bundle(s), {dimensions} fact(s)");
            Ok(())
        }
        FactCmd::List {
            entity,
            dimension,
            store,
        } => {
            let docs: Arc<dyn dkg_documents::DocumentStore> =
                Arc::from(dkg_documents::open(&store.database_url).await?);
            let facts = DocumentFactStore::new(Arc::clone(&docs));
            let found = match dimension {
                Some(dim) => facts.facts_on(&entity, &dim).await?,
                None => facts.facts_for(&entity).await?,
            };
            docs.shutdown().await?;
            println!("{}", serde_json::to_string_pretty(&found)?);
            Ok(())
        }
        FactCmd::Resolve {
            entity,
            store,
            config_dir,
        } => {
            let bundle = dkg_config::load_dir(&config_dir)?;
            let schema = dkg_graph::Schema::load(bundle.tables, bundle.relationships)?;
            let table = schema.table(entity.namespace())?;

            let docs: Arc<dyn dkg_documents::DocumentStore> =
                Arc::from(dkg_documents::open(&store.database_url).await?);
            let facts = DocumentFactStore::new(Arc::clone(&docs));
            let entity_facts = facts.facts_for(&entity).await?;
            docs.shutdown().await?;

            let record =
                dkg_entity_fact_resolver::transform::resolve_entity(&entity, &entity_facts, table);
            println!("{}", serde_json::to_string_pretty(&record)?);
            Ok(())
        }
        FactCmd::Sync {
            entity,
            store,
            graph,
        } => {
            let (graph_store, schema) = graph.connect().await?;
            let docs: Arc<dyn dkg_documents::DocumentStore> =
                Arc::from(dkg_documents::open(&store.database_url).await?);
            let facts = DocumentFactStore::new(Arc::clone(&docs));

            let outcome = dkg_entity_fact_resolver::sync_entity(
                &facts,
                graph_store.as_ref(),
                &schema,
                &entity,
            )
            .await?;
            docs.shutdown().await?;
            println!(
                "synced {} into `{}` as id {} from {} fact(s)",
                entity,
                entity.namespace(),
                outcome.id,
                outcome.fact_count
            );
            Ok(())
        }
    }
}

async fn run_schema(cmd: SchemaCmd) -> Result<()> {
    match cmd {
        SchemaCmd::Reconcile { graph } => {
            let (store, schema) = graph.connect().await?;
            store.reconcile(&schema).await?;
            println!("reconciled {} table(s)", schema.tables().count());
            Ok(())
        }
    }
}

/// Split a `key<sep>value` string into a trimmed pair (used for `--query`
/// `k=v` and `--header` `Name: value`).
fn split_pair(s: &str, sep: char) -> Result<(String, String)> {
    let (k, v) = s
        .split_once(sep)
        .with_context(|| format!("expected `key{sep}value`, got `{s}`"))?;
    Ok((k.trim().to_string(), v.trim().to_string()))
}

/// Parse a `column<op>value` filter into a [`dkg_graph::Filter`]. `<op>` is
/// one of `!=`, `>=`, `<=`, `>`, `<`, `~` (in-list, comma-separated), `*`
/// (substring `LIKE`), or bare `=` (the default, checked last since every
/// other operator's own character would otherwise never be reached).
///
/// For `=`/`!=`/`>=`/`<=`/`>`/`<`, the value is parsed as JSON when it
/// parses (so `42` is a number, `"x"` a string) and otherwise kept as a bare
/// string. `~` always splits its value on commas into a JSON array of
/// strings; `*` always keeps its value as a literal string pattern.
fn parse_filter(s: &str) -> Result<dkg_graph::Filter> {
    let scalar = |raw: &str| -> serde_json::Value {
        serde_json::from_str(raw).unwrap_or_else(|_| serde_json::Value::String(raw.to_string()))
    };

    // Longest-operator-first so `!=`/`>=`/`<=` are not mistaken for `>`/`<`.
    if let Some((col, raw)) = s.split_once("!=") {
        return Ok(dkg_graph::Filter {
            column: col.trim().to_string(),
            op: dkg_graph::Op::Ne,
            value: scalar(raw.trim()),
        });
    }
    if let Some((col, raw)) = s.split_once(">=") {
        return Ok(dkg_graph::Filter {
            column: col.trim().to_string(),
            op: dkg_graph::Op::Gte,
            value: scalar(raw.trim()),
        });
    }
    if let Some((col, raw)) = s.split_once("<=") {
        return Ok(dkg_graph::Filter {
            column: col.trim().to_string(),
            op: dkg_graph::Op::Lte,
            value: scalar(raw.trim()),
        });
    }
    if let Some((col, raw)) = s.split_once('~') {
        let items: Vec<serde_json::Value> = raw
            .split(',')
            .map(|v| serde_json::Value::String(v.trim().to_string()))
            .collect();
        return Ok(dkg_graph::Filter {
            column: col.trim().to_string(),
            op: dkg_graph::Op::In,
            value: serde_json::Value::Array(items),
        });
    }
    if let Some((col, raw)) = s.split_once('*') {
        return Ok(dkg_graph::Filter {
            column: col.trim().to_string(),
            op: dkg_graph::Op::Like,
            value: serde_json::Value::String(raw.trim().to_string()),
        });
    }
    if let Some((col, raw)) = s.split_once('>') {
        return Ok(dkg_graph::Filter {
            column: col.trim().to_string(),
            op: dkg_graph::Op::Gt,
            value: scalar(raw.trim()),
        });
    }
    if let Some((col, raw)) = s.split_once('<') {
        return Ok(dkg_graph::Filter {
            column: col.trim().to_string(),
            op: dkg_graph::Op::Lt,
            value: scalar(raw.trim()),
        });
    }
    let (col, raw) = s
        .split_once('=')
        .context("filter must be `column<op>value`, e.g. `age>=21` or `name=Ada`")?;
    Ok(dkg_graph::Filter {
        column: col.trim().to_string(),
        op: dkg_graph::Op::Eq,
        value: scalar(raw.trim()),
    })
}

/// Parse `--order-by`: a bare `column` (ascending) or `-column` (descending).
fn parse_order_by(s: &str) -> dkg_graph::OrderBy {
    match s.strip_prefix('-') {
        Some(col) => dkg_graph::OrderBy {
            column: col.to_string(),
            direction: dkg_graph::SortDirection::Descending,
        },
        None => dkg_graph::OrderBy {
            column: s.to_string(),
            direction: dkg_graph::SortDirection::Ascending,
        },
    }
}

async fn run_table(cmd: TableCmd) -> Result<()> {
    match cmd {
        TableCmd::Insert { table, file, graph } => {
            let (store, schema) = graph.connect().await?;
            let body: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&file)
                    .with_context(|| format!("reading {}", file.display()))?,
            )
            .context("parsing records JSON")?;
            let records: Vec<dkg_graph::Record> = match body {
                serde_json::Value::Object(map) => vec![map],
                serde_json::Value::Array(items) => items
                    .into_iter()
                    .map(|v| {
                        v.as_object()
                            .cloned()
                            .context("each record must be a JSON object")
                    })
                    .collect::<Result<_>>()?,
                _ => anyhow::bail!("expected a JSON object or array of objects"),
            };
            let n = records.len();
            store.insert(&schema, &table, &records).await?;
            println!("inserted {n} record(s)");
            Ok(())
        }
        TableCmd::Query {
            table,
            filter,
            order_by,
            offset,
            limit,
            graph,
        } => {
            let (store, schema) = graph.connect().await?;
            let filters = filter
                .iter()
                .map(|f| parse_filter(f))
                .collect::<Result<Vec<_>>>()?;
            let order_by = order_by.as_deref().map(parse_order_by);
            let rows = store
                .query(&schema, &table, &filters, order_by.as_ref(), offset, limit)
                .await?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
            Ok(())
        }
        TableCmd::Count {
            table,
            filter,
            graph,
        } => {
            let (store, schema) = graph.connect().await?;
            let filters = filter
                .iter()
                .map(|f| parse_filter(f))
                .collect::<Result<Vec<_>>>()?;
            let count = store.count(&schema, &table, &filters).await?;
            println!("{count}");
            Ok(())
        }
        TableCmd::Related {
            table,
            id,
            other,
            limit,
            graph,
        } => {
            let (store, schema) = graph.connect().await?;
            let rows = store
                .related(&schema, &table, &serde_json::json!(id), &other, limit)
                .await?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
            Ok(())
        }
    }
}

fn run_config(cmd: ConfigCmd) -> Result<()> {
    match cmd {
        ConfigCmd::Validate { file } => {
            let source = std::fs::read_to_string(&file)
                .with_context(|| format!("reading {}", file.display()))?;
            let evaluated = dkg_config::eval::evaluate(&source)?;
            dkg_config::validate(&evaluated)?;
            let kind = dkg_config::kind::kind_of(&evaluated)?;
            println!("ok: valid `{kind}` config");
            println!("{}", serde_json::to_string_pretty(&evaluated)?);
            Ok(())
        }
        ConfigCmd::Kinds => {
            for kind in dkg_core::registry::names::<dkg_config::ConfigKind>() {
                println!("{kind}");
            }
            Ok(())
        }
    }
}

async fn run_doc(cmd: DocCmd) -> Result<()> {
    match cmd {
        DocCmd::Put {
            file,
            collection,
            search_id,
            database_url,
        } => {
            let body: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&file)
                    .with_context(|| format!("reading {}", file.display()))?,
            )
            .context("parsing document JSON")?;
            let store = dkg_documents::open(&database_url).await?;
            let doc = store
                .create(NewDocument {
                    collection,
                    search_ids: search_id,
                    body,
                    metadata: serde_json::json!({}),
                })
                .await?;
            // Drain so the write's index commit flushes before we exit.
            store.shutdown().await?;
            println!("{}", doc.id);
            Ok(())
        }
        DocCmd::Get { key, database_url } => {
            let store = dkg_documents::open(&database_url).await?;
            // A `urn:...` key is a search id; anything else is a UUID.
            let doc = if key.starts_with("urn:") {
                store.get_by_urn(&key.parse::<Urn>()?).await?
            } else {
                store
                    .get(key.parse::<Uuid>().context("invalid document id")?)
                    .await?
            };
            store.shutdown().await?;
            println!("{}", serde_json::to_string_pretty(&doc)?);
            Ok(())
        }
        DocCmd::Search {
            text,
            collection,
            limit,
            database_url,
        } => {
            let store = dkg_documents::open(&database_url).await?;
            let docs = store
                .search(Query {
                    collection,
                    text,
                    metadata_contains: serde_json::json!({}),
                    limit,
                })
                .await?;
            store.shutdown().await?;
            println!("{}", serde_json::to_string_pretty(&docs)?);
            Ok(())
        }
        DocCmd::Repair { database_url } => {
            let store = dkg_documents::open(&database_url).await?;
            let n = store.repair().await?;
            store.shutdown().await?;
            println!("repaired {n} document(s)");
            Ok(())
        }
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
