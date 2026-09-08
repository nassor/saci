//! `saci-service cluster`: cluster management subcommands.
//!
//! The whole body of this module is gated on `feature = "service-cluster"`.
//! In a `service`-only build the CLI still recognises the `cluster` subcommand,
//! because the `clap` types in `cli.rs` are unconditional, but invoking it
//! returns a clear "not built with service-cluster" error instead of a nonsense
//! "unrecognised subcommand" failure.
//!
//! `cluster init` validates the config and confirms `bootstrap = true`.
//! `cluster status` reads the `cluster` field of the `/status` endpoint.
//! `cluster join` and `cluster leave` print the manual membership steps,
//! because the HTTP control plane has no membership endpoint.

use saci_service::SaciError;
#[cfg(feature = "service-cluster")]
use saci_service::service::config::{ServiceConfig, ServiceMode};

use crate::cli::{ClusterCmd, GlobalOpts};

/// Entry point for the `cluster` subcommand.
#[cfg(feature = "service-cluster")]
pub async fn run(global: &GlobalOpts, cmd: &ClusterCmd) -> Result<(), SaciError> {
    match cmd {
        ClusterCmd::Init => cmd_init(global).await,
        ClusterCmd::Join { leader } => cmd_join(global, leader).await,
        ClusterCmd::Leave => cmd_leave(global).await,
        ClusterCmd::Status => cmd_status(global).await,
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
/// A pre-flight check only: it loads the config, confirms `mode = cluster` and
/// `bootstrap = true`, then prints next-step instructions. Bootstrapping itself
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
                    "cluster.bootstrap is false in the config. \
                     Set bootstrap: true to initialise a new cluster.",
                ));
            }
            println!("OK: config is valid and cluster.bootstrap = true");
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
                 with bootstrap: false."
            );
        }
        ServiceMode::Standalone { .. } => {
            return Err(SaciError::configuration(
                "config is in standalone mode. cluster init requires mode: cluster",
            ));
        }
    }

    Ok(())
}

/// Join an existing cluster.
///
/// The HTTP control plane exposes no `/cluster/membership` endpoint, so
/// membership changes are manual: add the node to the `peers` list in the
/// config on every node, set `bootstrap: false` on the new node, and restart
/// all nodes.
#[cfg(feature = "service-cluster")]
async fn cmd_join(_global: &GlobalOpts, leader: &str) -> Result<(), SaciError> {
    eprintln!(
        "Note: cluster join via HTTP is not yet implemented in v1.\n\
         Leader address provided: {leader}\n\
         \n\
         To add a node to an existing cluster:\n\
         1. Add the new node's entry to the 'peers' list in the config on all nodes.\n\
         2. Set 'bootstrap: false' on the new node.\n\
         3. Restart all nodes."
    );
    Ok(())
}

/// Remove this node from the cluster gracefully.
///
/// As with `cluster join` there is no HTTP membership endpoint. Remove the node
/// from `peers` in the config on the remaining nodes and restart them.
#[cfg(feature = "service-cluster")]
async fn cmd_leave(_global: &GlobalOpts) -> Result<(), SaciError> {
    eprintln!(
        "Note: cluster leave via HTTP is not yet implemented in v1.\n\
         \n\
         To remove a node from the cluster:\n\
         1. Stop the node process.\n\
         2. Remove the node's entry from the 'peers' list in the config on all remaining nodes.\n\
         3. Restart the remaining nodes."
    );
    Ok(())
}

/// Show cluster status from the running node's HTTP API.
///
/// Queries `/status` and extracts the `cluster` field, which is null when the
/// node has no cluster probe wired.
#[cfg(feature = "service-cluster")]
async fn cmd_status(global: &GlobalOpts) -> Result<(), SaciError> {
    let addr = global.addr.as_ref().ok_or_else(|| {
        SaciError::configuration(
            "--addr is required for cluster status (e.g., http://localhost:8080)",
        )
    })?;

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

    match body.get("cluster") {
        Some(cluster) if !cluster.is_null() => {
            println!(
                "{}",
                serde_json::to_string_pretty(cluster).unwrap_or_else(|_| cluster.to_string())
            );
        }
        _ => {
            // The serve command wires no cluster probe.
            println!("node {node_id}  mode=cluster");
            println!(
                "Note: cluster details are not available in v1. \
                 Full Raft metrics integration is planned for v1.1."
            );
        }
    }

    Ok(())
}
