//! `saci-service cluster`: cluster management subcommands.
//!
//! The whole body of this module is gated on `feature = "service-cluster"`.
//! In a `service`-only build the CLI still recognises the `cluster` subcommand,
//! because the `clap` types in `cli.rs` are unconditional, but invoking it
//! returns a clear "not built with service-cluster" error instead of a nonsense
//! "unrecognised subcommand" failure.
//!
//! `cluster init` validates the config and confirms `cluster { bootstrap #true }`.
//! `cluster status` reads `/status` from `--url`, or from the `http.bind` of
//! the config at `--config`, and prints the node's identity line.

use crate::SaciError;
#[cfg(feature = "service-cluster")]
use crate::service::config::{ServiceConfig, ServiceMode};

use crate::service::cli::{ClusterCmd, GlobalOpts};

/// Entry point for the `cluster` subcommand.
#[cfg(feature = "service-cluster")]
pub async fn run(global: &GlobalOpts, cmd: &ClusterCmd) -> Result<(), SaciError> {
    match cmd {
        ClusterCmd::Init => cmd_init(global).await,
        ClusterCmd::Status { url } => cmd_status(global, url.as_deref()).await,
    }
}

/// Fallback entry point used in `service`-only builds.
///
/// Returns a clear error explaining that cluster support was not compiled in,
/// keeping the subcommand visible in `--help` so operators do not mistake its
/// absence for a CLI typo.
#[cfg(not(feature = "service-cluster"))]
pub async fn run(_global: &GlobalOpts, _cmd: &ClusterCmd) -> Result<(), SaciError> {
    Err(SaciError::configuration(
        "this binary was built without the `service-cluster` feature — \
         rebuild with `--features service-cluster` to use cluster subcommands",
    ))
}

/// Validate the config and confirm it is bootstrap-ready.
///
/// A pre-flight check only: it loads the config, confirms `mode "cluster"` and
/// `cluster { bootstrap #true }`, then prints next-step instructions. Bootstrapping itself
/// happens when `saci-service serve --config <path>` runs with the same file.
#[cfg(feature = "service-cluster")]
async fn cmd_init(global: &GlobalOpts) -> Result<(), SaciError> {
    let config_path = &global.config;
    let config = ServiceConfig::load(config_path)?;

    match &config.mode {
        ServiceMode::Cluster {
            config: cluster_cfg,
        } => {
            if !cluster_cfg.bootstrap {
                return Err(SaciError::configuration(
                    "`bootstrap` is #false in the config's cluster block. \
                     Set `cluster { bootstrap #true }` to initialise a new cluster.",
                ));
            }
            println!("OK: config is valid and cluster {{ bootstrap #true }}");
            println!("  node.id:  {}", config.node.id);
            println!("  peers:    {}", cluster_cfg.peers.len());
            println!();
            println!(
                "To bootstrap the cluster, start this node with:\n  \
                 saci-service serve --config {}",
                config_path.display()
            );
            println!();
            println!(
                "IMPORTANT: run `saci-service serve` on ONE node first. \
                 After the leader is elected, start the remaining nodes \
                 with `bootstrap #false`."
            );
        }
        ServiceMode::Standalone { .. } => {
            return Err(SaciError::configuration(
                "config is in standalone mode. cluster init requires mode \"cluster\"",
            ));
        }
    }

    Ok(())
}

/// Show cluster status from the running node's HTTP API.
///
/// Queries `/status` and prints the node's identity line.
#[cfg(feature = "service-cluster")]
async fn cmd_status(global: &GlobalOpts, url: Option<&str>) -> Result<(), SaciError> {
    let addr = super::status::resolve_url(global, url)?;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/status"))
        .send()
        .await
        .map_err(|e| SaciError::generic(format!("failed to reach {addr}: {e}")))?;

    if !resp.status().is_success() {
        return Err(SaciError::generic(format!(
            "status endpoint returned HTTP {}",
            resp.status()
        )));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| SaciError::generic(format!("failed to parse /status JSON: {e}")))?;

    let node_id = body.get("node_id").and_then(|v| v.as_u64()).unwrap_or(0);
    let mode = body
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    if mode != "cluster" {
        println!("node {node_id} is running in {mode} mode (not cluster)");
        return Ok(());
    }

    println!("node {node_id}  mode=cluster");

    Ok(())
}
