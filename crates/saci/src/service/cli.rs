//! The `saci-service` command line, as a library entry point.
//!
//! [`main`] is the whole `saci-service` binary. A custom server is a binary of
//! its own that depends on `saci` with the features it wants and calls
//! [`main`], or [`main_with`] to register its own factories.
//!
//! clap 4 derive defines the CLI shape. Every subcommand has its own args
//! struct; the one global option (the config path) is [`GlobalOpts`],
//! flattened into the root [`Cli`] struct.

pub mod commands;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use crate::service::ServiceBuilder;

/// Run the `saci-service` command line with the built-in connectors.
pub fn main() -> ExitCode {
    main_with(|builder| builder)
}

/// Run the `saci-service` command line, handing the [`ServiceBuilder`] that
/// `serve` and `validate` build from to `customize` after the built-in
/// connectors and codecs are registered.
///
/// Parses the process arguments, loads a `.env` file from the working
/// directory if one exists (an existing variable is never overridden), and
/// prints any error on stderr before returning [`ExitCode::FAILURE`].
///
/// ```no_run
/// use saci::SaciError;
/// use saci::connector::{ConfigValue, Connector, ConnectorContext, Role};
/// use saci::io::Source;
///
/// struct MyConnector;
///
/// impl Connector for MyConnector {
///     fn name(&self) -> &'static str {
///         "my-source"
///     }
///
///     fn roles(&self) -> &'static [Role] {
///         &[Role::Source]
///     }
///
///     fn source(
///         &self,
///         _config: &ConfigValue,
///         _ctx: &ConnectorContext,
///     ) -> Result<Box<dyn Source>, SaciError> {
///         Err(SaciError::configuration("my-source is not implemented yet"))
///     }
/// }
///
/// fn main() -> std::process::ExitCode {
///     saci::service::cli::main_with(|builder| builder.connector(MyConnector))
/// }
/// ```
pub fn main_with<F>(customize: F) -> ExitCode
where
    F: Fn(ServiceBuilder) -> ServiceBuilder,
{
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("error: starting the tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async {
        // Load a `.env` so `${VAR}` placeholders in configs and cloud
        // credentials resolve without a manual export. Optional: an absent or
        // unreadable file is ignored, and `dotenvy::dotenv` never overrides a
        // variable already present in the environment.
        let _ = dotenvy::dotenv();
        let parsed = Cli::parse();
        // `status`/`cluster status` build a reqwest client; reqwest 0.13's
        // `rustls-no-provider` needs the crypto provider installed first.
        crate::service::install_ring_provider();
        let result = match &parsed.cmd {
            Command::Serve(args) => commands::serve::run(&parsed.global, args, &customize).await,
            Command::Validate(args) => {
                commands::validate::run(&parsed.global, args, &customize).await
            }
            Command::Status(args) => commands::status::run(&parsed.global, args).await,
            Command::Cluster { cmd } => commands::cluster::run(&parsed.global, cmd).await,
            Command::Backfill(args) => {
                commands::backfill::run_backfill(&parsed.global, args, &customize).await
            }
            Command::Cursor { cmd } => commands::backfill::run_cursor(&parsed.global, cmd).await,
            Command::Reload(args) => commands::backfill::run_reload(&parsed.global, args).await,
        };
        match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

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
    /// Trigger a parameterized historical backfill for one workflow.
    Backfill(BackfillArgs),
    /// Inspect or reset persisted source cursors and processor state priors.
    Cursor {
        #[command(subcommand)]
        cmd: CursorCmd,
    },
    /// Live-reload workflow and service configuration without restarting.
    Reload(ReloadArgs),
}

/// Arguments for the `serve` subcommand.
#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Override the node ID (useful when deploying the same config to all nodes).
    #[arg(long, env = "SACI_NODE_ID")]
    pub node_id: Option<u64>,

    /// Override the HTTP bind address (`host:port`; port 0 = OS-assigned).
    ///
    /// Replaces `http.bind` from the config file whole. Useful for testing:
    /// pass `--bind 127.0.0.1:0` and read the bound address from stdout
    /// (`saci-service listening on <addr>`).
    #[arg(long, env = "SACI_HTTP_BIND", value_name = "ADDR")]
    pub bind: Option<String>,

    /// Log format override.
    #[arg(long, env = "SACI_LOG_FORMAT", value_enum)]
    pub log_format: Option<LogFormatArg>,

    /// Log level override applied to the tracing filter.
    #[arg(long, env = "SACI_LOG_LEVEL")]
    pub log_level: Option<String>,

    /// OTLP/HTTP collector base URL for span export. Empty disables it.
    #[arg(long, env = "SACI_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
}

/// Arguments for the `validate` subcommand.
#[derive(clap::Args, Debug)]
pub struct ValidateArgs {
    // Config path comes from GlobalOpts --config.
    /// Treat unknown connector names as errors rather than warnings.
    ///
    /// A name no crate here provides (a user-defined connector registered at
    /// serve time) warns and the command exits 0; `--strict` makes it a
    /// non-zero exit. A name that is a built-in connector this binary was
    /// built without is an error in both modes, since no serve-time
    /// registration can supply it.
    #[arg(long)]
    pub strict: bool,
    /// Build only sources, sinks and codecs; skip processor nodes and
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

    /// Base URL of the node to query (for example `http://localhost:8080`).
    ///
    /// Defaults to `http://{http.bind}` from `--config`.
    #[arg(long, env = "SACI_URL")]
    pub url: Option<String>,
}

/// Cluster management subcommands.
#[derive(Subcommand, Debug)]
pub enum ClusterCmd {
    /// Initialize a new cluster on this node. Only run once per cluster.
    Init,
    /// Show cluster status from a running node's HTTP API.
    Status {
        /// Base URL of the node (for example `http://10.0.0.1:8080`).
        ///
        /// Defaults to `http://{http.bind}` from `--config`.
        #[arg(long, env = "SACI_URL")]
        url: Option<String>,
    },
}

/// Log format selection for CLI override.
#[derive(ValueEnum, Clone, Debug)]
pub enum LogFormatArg {
    Pretty,
    Json,
}

/// Arguments for the `backfill` subcommand.
#[derive(clap::Args, Debug)]
pub struct BackfillArgs {
    /// Target workflow identifier.
    pub workflow: String,
    /// Optional inclusive start timestamp (`YYYY-MM-DD`, RFC3339, or unix_ms).
    #[arg(long)]
    pub start: Option<String>,
    /// Optional inclusive end timestamp (`YYYY-MM-DD`, RFC3339, or unix_ms).
    #[arg(long)]
    pub end: Option<String>,
    /// Event-time column used to filter records to `[--start, --end]`.
    #[arg(long)]
    pub time_field: Option<String>,
    /// Custom `KEY=VALUE` substitution parameter (repeatable).
    #[arg(long = "param", value_name = "KEY=VALUE")]
    pub params: Vec<String>,
    /// Start with fresh processor state instead of loading persisted priors.
    #[arg(long)]
    pub reset_state: bool,
    /// Base URL of a running service node (for example `http://localhost:8080`).
    ///
    /// Defaults to `http://{http.bind}` from `--config`. Ignored with `--local`.
    #[arg(long, env = "SACI_URL")]
    pub url: Option<String>,
    /// Execute one local backfill pass directly from `--config` instead of
    /// calling a running service HTTP endpoint.
    #[arg(long)]
    pub local: bool,
}

/// Cursor inspection and time-travel reset subcommands.
#[derive(Subcommand, Debug)]
pub enum CursorCmd {
    /// Show current cursors and historical checkpoint timestamps for a workflow.
    List {
        /// Target workflow identifier.
        workflow: String,
        /// Base URL of the running service node.
        #[arg(long, env = "SACI_URL")]
        url: Option<String>,
    },
    /// Roll back cursors/priors to a historical checkpoint or reset to origin.
    Reset {
        /// Target workflow identifier.
        workflow: String,
        /// Reset only this source node ID (default: all sources in workflow).
        #[arg(long)]
        source: Option<String>,
        /// Roll back to the newest checkpoint at or before this timestamp
        /// (`YYYY-MM-DD`, RFC3339, or unix_ms).
        #[arg(long, value_name = "TIMESTAMP")]
        to: Option<String>,
        /// Explicit Unix millisecond checkpoint timestamp.
        #[arg(long, value_name = "UNIX_MS")]
        to_unix_ms: Option<u64>,
        /// Explicit `items_processed` value to set on the reset cursor(s).
        #[arg(long)]
        items_processed: Option<u64>,
        /// Keep existing processor state priors rather than rolling them back or clearing them.
        #[arg(long)]
        keep_priors: bool,
        /// Base URL of the running service node.
        #[arg(long, env = "SACI_URL")]
        url: Option<String>,
    },
}

/// Arguments for the `reload` subcommand.
#[derive(clap::Args, Debug)]
pub struct ReloadArgs {
    /// Path to a replacement KDL config file to upload (default: ask the service
    /// to reload from its startup `--config` path on disk).
    #[arg(long)]
    pub file: Option<PathBuf>,
    /// Base URL of the running service node.
    #[arg(long, env = "SACI_URL")]
    pub url: Option<String>,
}
