//! Distributed order fulfillment: a multi-node SACI example.
//!
//! Runs a 3-node Raft cluster where each node processes `Order` batches
//! through a 4-stage pipeline, producing JSON invoice files.
//!
//! ```
//! # node 1: bootstrap and generator
//! distributed_fulfillment --node-id 1 --bootstrap \
//!     --data-dir /tmp/node1 --output-dir /tmp/output/node1 \
//!     --listen 127.0.0.1:9001 \
//!     --peers 127.0.0.1:9002,127.0.0.1:9003
//!
//! # node 2
//! distributed_fulfillment --node-id 2 \
//!     --data-dir /tmp/node2 --output-dir /tmp/output/node2 \
//!     --listen 127.0.0.1:9002 \
//!     --peers 127.0.0.1:9001,127.0.0.1:9003
//!
//! # node 3
//! distributed_fulfillment --node-id 3 \
//!     --data-dir /tmp/node3 --output-dir /tmp/output/node3 \
//!     --listen 127.0.0.1:9003 \
//!     --peers 127.0.0.1:9001,127.0.0.1:9002
//! ```

mod components;
mod generator;
mod resources;
mod store;
mod systems;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use saci_service::SaciError;
use saci_service::SaciResult;
use saci_service::distributed::RedbSharedStore;
use saci_service::distributed::consensus::{ArrowRaftDriver, ArrowRaftDriverConfig};
use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
use saci_service::distributed::strategy::CheckpointStrategy;
use saci_service::service::config::{LogFormat, ObservabilityConfig};
use saci_service::service::logging::init_logging;

use crate::store::FulfillmentStore;
use crate::systems::build_pipeline;

#[derive(Debug)]
struct Args {
    node_id: u64,
    listen: SocketAddr,
    /// Peers as "host:port" strings. Hostnames such as Docker service names are
    /// resolved lazily at connection time by the transport layer.
    peers: HashMap<u64, String>,
    data_dir: PathBuf,
    output_dir: PathBuf,
    bootstrap: bool,
    log_json: bool,
    generator_interval_secs: u64,
}

fn parse_args() -> Result<Args, String> {
    let args: Vec<String> = std::env::args().collect();
    let mut node_id: Option<u64> = None;
    let mut listen: Option<SocketAddr> = None;
    let mut peers_raw: Vec<String> = Vec::new();
    let mut data_dir: Option<PathBuf> = None;
    let mut output_dir: Option<PathBuf> = None;
    let mut bootstrap = false;
    let mut log_json = false;
    let mut generator_interval_secs: u64 = 2;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--node-id" => {
                i += 1;
                node_id = Some(args[i].parse().map_err(|e| format!("--node-id: {e}"))?);
            }
            "--listen" => {
                i += 1;
                listen = Some(args[i].parse().map_err(|e| format!("--listen: {e}"))?);
            }
            "--peers" => {
                i += 1;
                peers_raw = args[i].split(',').map(|s| s.trim().to_string()).collect();
            }
            "--data-dir" => {
                i += 1;
                data_dir = Some(PathBuf::from(&args[i]));
            }
            "--output-dir" => {
                i += 1;
                output_dir = Some(PathBuf::from(&args[i]));
            }
            "--bootstrap" => bootstrap = true,
            "--log-json" => log_json = true,
            "--generator-interval" => {
                i += 1;
                generator_interval_secs = args[i]
                    .parse()
                    .map_err(|e| format!("--generator-interval: {e}"))?;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        i += 1;
    }

    let node_id = node_id.ok_or("--node-id required")?;
    let listen = listen.unwrap_or_else(|| {
        let port = 9000 + node_id as u16;
        format!("0.0.0.0:{port}").parse().unwrap()
    });

    // Peers stay strings: deferring DNS to connection time lets a peer such as
    // "node2:9002" start later than this node.
    let mut peers: HashMap<u64, String> = HashMap::new();
    let mut peer_id = 1u64;
    for raw in peers_raw {
        if peer_id == node_id {
            peer_id += 1;
        }
        peers.insert(peer_id, raw);
        peer_id += 1;
    }

    let data_dir = data_dir.ok_or("--data-dir required")?;
    let output_dir = output_dir.unwrap_or_else(|| data_dir.join("output"));

    Ok(Args {
        node_id,
        listen,
        peers,
        data_dir,
        output_dir,
        bootstrap,
        log_json,
        generator_interval_secs,
    })
}

#[tokio::main]
async fn main() -> SaciResult<()> {
    let args = parse_args().map_err(SaciError::configuration)?;

    let obs_cfg = ObservabilityConfig {
        log_format: if args.log_json {
            LogFormat::Json
        } else {
            LogFormat::Pretty
        },
        log_level: std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        ..ObservabilityConfig::default()
    };
    // The example has no HTTP control plane, so nothing here reads the
    // inspector handle: the capture layer is installed either way.
    let (telemetry, _inspector) = init_logging(&obs_cfg, args.node_id)?;

    #[cfg(feature = "tracing")]
    tracing::info!(
        node_id = args.node_id,
        listen = %args.listen,
        bootstrap = args.bootstrap,
        peers = ?args.peers,
        "distributed_fulfillment starting"
    );

    std::fs::create_dir_all(&args.data_dir)
        .map_err(|e| SaciError::configuration(format!("create data_dir: {e}")))?;
    std::fs::create_dir_all(&args.output_dir)
        .map_err(|e| SaciError::configuration(format!("create output_dir: {e}")))?;

    let raft_cfg = ArrowRaftDriverConfig {
        node_id: args.node_id,
        listen_addr: args.listen,
        peers: args.peers.clone(),
        heartbeat_interval_ms: 50,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        snapshot_log_interval: 10_000,
    };

    let log_db = args.data_dir.join("raft_log.redb");
    let app_db_path = args.data_dir.join("app.redb");

    let (raft_handle, raft_task) = ArrowRaftDriver::start(raft_cfg, &log_db, &app_db_path).await?;

    // The driver wires only outbound connections, so the listener for inbound
    // Raft RPCs has to be started separately. It binds before it accepts, so a
    // taken address fails here.
    let _tcp_server = raft_handle.spawn_tcp_server(args.listen).await?;

    if args.bootstrap {
        use openraft::BasicNode;
        let mut members = std::collections::BTreeMap::new();
        members.insert(
            args.node_id,
            BasicNode {
                addr: args.listen.to_string(),
            },
        );
        for (peer_id, peer_addr) in &args.peers {
            members.insert(
                *peer_id,
                BasicNode {
                    addr: peer_addr.to_string(),
                },
            );
        }
        raft_handle.initialize(members).await?;
        #[cfg(feature = "tracing")]
        tracing::info!(node_id = args.node_id, "Raft cluster initialized");
    }

    let app_db = raft_handle.app_db().clone();
    let inner_store = RedbSharedStore::multi_node(app_db.clone(), raft_handle.clone());
    let store = Arc::new(FulfillmentStore::new(Arc::new(inner_store), app_db));

    wait_for_leader(raft_handle.clone(), Duration::from_secs(30)).await?;

    if args.bootstrap && args.node_id == 1 {
        let gen_store = Arc::clone(&store);
        let interval = Duration::from_secs(args.generator_interval_secs);
        tokio::spawn(async move {
            generator::run_generator(gen_store, 1, interval).await;
        });
        #[cfg(feature = "tracing")]
        tracing::info!(node_id = args.node_id, "generator task started");
    }

    let pipeline = build_pipeline();

    let runner_cfg = RunnerConfig {
        instance_id: uuid::Uuid::now_v7(),
        checkpoint_strategy: CheckpointStrategy::EveryNStages(2),
        schema_id: generator::SCHEMA_ID,
        max_batches: None,
        lease_renewal_check_interval_millis: 5_000,
        workflow_id: "fulfillment".to_string(),
        processor_id: "fulfillment".to_string(),
        #[cfg(feature = "windows")]
        partition_mask: None,
    };

    let node_id = args.node_id;
    let output_dir = args.output_dir.clone();
    let world_factory = {
        let store_ref = Arc::clone(&store);
        move || {
            // FulfillmentStore::world_factory holds the hydration logic, and
            // calling it per claim picks up that claim's IPC.
            (store_ref.world_factory(node_id))()
        }
    };

    let runner = DistributedRunner::new((*store).clone(), Box::new(pipeline), runner_cfg);

    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigint = signal(SignalKind::interrupt()).unwrap();
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.unwrap();
        }
    };

    #[cfg(feature = "tracing")]
    tracing::info!(node_id, "runner started — waiting for batches");

    let node_id_log = args.node_id;

    // The shutdown future is pinned so it survives every loop iteration.
    // `runner.run()` returns `Ok(0)` on an empty claim queue, so the loop
    // sleeps and retries to pick up batches produced later by any node. Only a
    // signal or a real runner error leaves the loop.
    tokio::pin!(shutdown);

    let mut total_batches: usize = 0;
    let run_result: SaciResult<usize> = loop {
        tokio::select! {
            r = runner.run(&world_factory) => match r {
                Ok(n) => {
                    total_batches += n;
                    if n == 0 {
                        // No pending work: idle briefly, but let a signal
                        // interrupt the sleep.
                        tokio::select! {
                            () = tokio::time::sleep(Duration::from_secs(2)) => {}
                            () = &mut shutdown => {
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    node_id = node_id_log,
                                    "shutdown signal received during idle wait"
                                );
                                break Ok(total_batches);
                            }
                        }
                        continue;
                    }
                    continue;
                }
                Err(e) => {
                    // Propose timeouts and forwarding hiccups during leader
                    // re-election are transient: sleep and retry once the
                    // cluster stabilises.
                    let msg = e.to_string();
                    if msg.contains("cluster propose timeout")
                        || msg.contains("forward_proposal")
                        || msg.contains("not leader")
                    {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            node_id = node_id_log,
                            error = %e,
                            "runner: transient cluster error — retrying"
                        );
                        tokio::select! {
                            () = tokio::time::sleep(Duration::from_secs(3)) => {}
                            () = &mut shutdown => break Ok(total_batches),
                        }
                        continue;
                    }
                    break Err(e);
                }
            },
            () = &mut shutdown => {
                #[cfg(feature = "tracing")]
                tracing::info!(node_id = node_id_log, "shutdown signal received");
                break Ok(total_batches);
            }
        }
    };

    match run_result {
        Ok(n) => {
            #[cfg(feature = "tracing")]
            tracing::info!(node_id = node_id_log, batches = n, "runner finished");
        }
        Err(ref e) => {
            #[cfg(feature = "tracing")]
            tracing::error!(node_id = node_id_log, error = %e, "runner error");
        }
    }

    raft_handle.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), raft_task).await;

    let _ = output_dir; // a real sink would take this through pipeline resources
    telemetry.shutdown().await;
    run_result.map(|_| ())
}

/// Wait until the cluster has a leader and a committed log entry, or time out.
///
/// An elected leader still needs quorum to commit its initial blank entry
/// before it accepts application writes, so `last_applied.is_some()` is the
/// signal that proposals will succeed.
async fn wait_for_leader(
    handle: saci_service::distributed::consensus::ArrowRaftDriverHandle,
    timeout: Duration,
) -> SaciResult<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let metrics = handle.metrics();
        if metrics.current_leader.is_some() && metrics.last_applied.is_some() {
            #[cfg(feature = "tracing")]
            tracing::info!(
                leader = ?metrics.current_leader,
                last_applied = ?metrics.last_applied,
                "cluster has leader"
            );
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(SaciError::configuration(
                "timed out waiting for Raft cluster to be ready (30s)",
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
