//! Command-line interface definitions for saci-service.
//!
//! Uses clap 4 derive to define the full CLI shape. Every subcommand has its
//! own args struct; global options (config path, address, log overrides) are
//! collected in [`GlobalOpts`] and flattened into the root [`Cli`] struct.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// SACI (Self-orchestrated Autonomous Compute Interface) distributed batch processing service.
#[derive(Parser, Debug)]
#[command(
    name = "saci-service",
    version,
    about = "SACI (Self-orchestrated Autonomous Compute Interface) distributed batch processing service"
)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalOpts,
    #[command(subcommand)]
    pub cmd: Command,
}

/// Options that apply to every subcommand.
#[derive(clap::Args, Debug, Clone)]
pub struct GlobalOpts {
    /// Path to the service config file.
    ///
    /// Defaults to `saci.kdl` in the current directory, so `saci-service serve`
    /// works with no flags. A missing file surfaces as
    /// `Configuration error: reading config file saci.kdl: ...`.
    #[arg(
        long,
        short = 'c',
        env = "SACI_CONFIG",
        default_value = "saci.kdl",
        global = true
    )]
    pub config: PathBuf,

    /// HTTP control-plane address to query (for status/cluster commands).
    #[arg(long, env = "SACI_ADDR", global = true)]
    pub addr: Option<String>,

    /// Log format override.
    #[arg(long, env = "SACI_LOG_FORMAT", global = true, value_enum)]
    pub log_format: Option<LogFormatArg>,

    /// Log level override applied to the tracing filter.
    #[arg(long, env = "SACI_LOG_LEVEL", global = true)]
    pub log_level: Option<String>,

    /// OTLP/HTTP collector base URL for span export. Empty disables it.
    #[arg(long, env = "SACI_OTLP_ENDPOINT", global = true)]
    pub otlp_endpoint: Option<String>,
}

/// Top-level subcommands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the service (standalone or cluster, determined by config).
    Serve(ServeArgs),
    /// Validate a config file without starting the service.
    Validate(ValidateArgs),
    /// Query the status of a running service instance via its HTTP API.
    Status(StatusArgs),
    /// Cluster management subcommands.
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
}

/// Arguments for the `serve` subcommand.
#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Override the node ID (useful when deploying the same config to all nodes).
    #[arg(long, env = "SACI_NODE_ID")]
    pub node_id: Option<u64>,

    /// Override the HTTP bind port (0 = OS-assigned ephemeral port).
    ///
    /// Takes precedence over the `http.bind` port in the config file.
    /// Useful for testing: pass `--port 0` and read the bound address from
    /// stdout (`saci-service listening on <addr>`).
    #[arg(long, env = "SACI_HTTP_PORT")]
    pub port: Option<u16>,
}

/// Arguments for the `validate` subcommand.
#[derive(clap::Args, Debug)]
pub struct ValidateArgs {
    // Config path comes from GlobalOpts --config.
    /// Treat unknown factory types as errors rather than warnings.
    ///
    /// A type no crate here provides (a user-defined factory registered at
    /// serve time) warns and the command exits 0; `--strict` makes it a
    /// non-zero exit. A type that is a built-in connector this binary was
    /// built without is an error in both modes, since no serve-time
    /// registration can supply it.
    #[arg(long)]
    pub strict: bool,
    /// Build only sources, sinks and transformers; skip processor nodes and
    /// the workflow-graph check that needs their built components.
    ///
    /// For a config naming a processor module not present on disk (a
    /// template, or one built by a separate step), this still exercises
    /// every connector's own `deny_unknown_fields` config and its
    /// `validate()`, with no live service and no processor artifact needed.
    #[arg(long)]
    pub connectors_only: bool,
}

/// Arguments for the `status` subcommand.
#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// Fetch the full /status JSON (default: show a summary).
    #[arg(long)]
    pub full: bool,
}

/// Cluster management subcommands.
#[derive(Subcommand, Debug)]
pub enum ClusterCmd {
    /// Initialize a new cluster on this node. Only run once per cluster.
    Init,
    /// Join an existing cluster by contacting the leader.
    Join {
        /// HTTP address of the leader node (e.g., http://10.0.0.1:8080).
        #[arg(long)]
        leader: String,
    },
    /// Leave the cluster gracefully. Removes this node from Raft membership.
    Leave,
    /// Show cluster status (membership, roles, commit index, etc.).
    Status,
}

/// Log format selection for CLI override.
#[derive(ValueEnum, Clone, Debug)]
pub enum LogFormatArg {
    Pretty,
    Json,
}
