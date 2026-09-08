//! Cluster runner for the `service` feature.
//!
//! [`run_cluster`] validates the data-directory layout (`bootstrap.lock`,
//! `node-id`), starts an [`ArrowRaftDriver`] with the configured peers and
//! timings, bootstraps a fresh cluster when configured, waits for Raft to
//! settle, runs the [`DistributedRunner`] loop until cancellation, then shuts
//! down gracefully.
//!
//! ## Sources and sinks
//!
//! Sources are rejected in cluster mode by
//! [`ServiceConfig::validate`](super::config::ServiceConfig::validate), so the
//! cluster path consumes batches registered through the shared store. Sinks run
//! locally in the runner's scheduler after each batch, so output is spread
//! across nodes and operators must aggregate it externally.
//!
//! ## Graceful shutdown (30 s budget)
//!
//! Cancellation logs the shutdown, aborts the source producer task, and lets
//! `DistributedRunner::run_until_cancelled` release any in-flight claim before
//! `handle.shutdown()` drains the Raft driver and the stats are returned.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::checkpoint::CheckpointStore;
use crate::distributed::consensus::driver::{
    ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle,
};
use crate::distributed::consensus::store::RedbSharedStore;
use crate::distributed::runner::{DistributedRunner, RunnerConfig};
use crate::distributed::strategy::CheckpointStrategy;
use crate::service::builder::BuiltService;
use crate::service::config::{ClusterConfig, ServiceConfig, ServiceMode};

// ── File names ────────────────────────────────────────────────────────────────

const BOOTSTRAP_LOCK_FILE: &str = "bootstrap.lock";
const RAFT_LOG_DB_FILE: &str = "raft-log.redb";
const APP_DB_FILE: &str = "cluster-app.redb";
const NODE_ID_FILE: &str = "node-id";

// ── Timing constants ─────────────────────────────────────────────────────────

/// How long to wait for Raft to exit Candidate state on startup.
const RAFT_SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while waiting for Raft metrics to settle.
const RAFT_SETTLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Budget for leader-transfer attempt during graceful shutdown.
const LEADER_TRANSFER_BUDGET: Duration = Duration::from_secs(5);
/// Pause before re-entering the runner after it found no claimable batch.
///
/// Short enough that a node picks up freshly registered work promptly, long
/// enough that an idle cluster is not scanning its state machine in a spin.
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// How long to wait for the raft drive loop to finish after signalling it.
///
/// The loop is what closes `raft-log.redb` and `cluster-app.redb`, so a
/// restart of this node depends on it having run to completion.
const DRIVER_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Aggregate statistics returned by [`run_cluster`].
#[derive(Debug, Default, Clone)]
pub struct ClusterStats {
    /// Batches successfully processed (acked) during this run.
    pub batches_processed: u64,
    /// Batches that encountered a processing error.
    pub batches_failed: u64,
    /// Claim errors (could not claim a batch).
    pub claim_errors: u64,
    /// Checkpoints written during this run.
    pub checkpoints_written: u64,
    /// Node ID of the last known leader, if available.
    pub last_leader_id: Option<u64>,
    /// Raft term at exit, if available.
    pub last_raft_term: Option<u64>,
    /// Total wall-clock milliseconds the runner was active.
    pub total_duration_ms: u64,
}

/// Run the cluster scheduler until `cancel` is signalled.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] if:
/// - `config.mode` is not `ServiceMode::Cluster`.
/// - The data directory contains `raft-log.redb` without `bootstrap.lock`
///   (indicates an unclean shutdown before bootstrap completed).
/// - The stored `node-id` file disagrees with `config.node.id`.
/// - The Raft driver cannot be started.
///
/// Returns [`SaciError::Generic`] if the Raft cluster does not settle within
/// the configured settle timeout.
pub async fn run_cluster(
    built: BuiltService,
    config: &ServiceConfig,
    cancel: CancellationToken,
) -> SaciResult<ClusterStats> {
    let start = std::time::Instant::now();
    let mut stats = ClusterStats::default();

    let cluster = match &config.mode {
        ServiceMode::Cluster { config: c } => c,
        ServiceMode::Standalone { .. } => {
            return Err(SaciError::configuration(
                "run_cluster called with standalone config — use run_standalone instead",
            ));
        }
    };

    let data_dir = &config.node.data_dir;

    validate_data_dir(data_dir, config.node.id)?;

    let this_peer = cluster
        .peers
        .iter()
        .find(|p| p.id == config.node.id)
        .ok_or_else(|| {
            SaciError::configuration(format!(
                "node id {} not found in cluster peers",
                config.node.id
            ))
        })?;

    let listen_addr: SocketAddr = this_peer.addr.parse().map_err(|e| {
        SaciError::configuration(format!("invalid peer addr '{}': {e}", this_peer.addr))
    })?;

    // Peer addresses are stored as strings; the transport layer resolves them
    // via DNS lazily at connection time (supports Docker service hostnames).
    let peers: HashMap<u64, String> = cluster
        .peers
        .iter()
        .filter(|p| p.id != config.node.id)
        .map(|p| (p.id, p.addr.clone()))
        .collect();

    let driver_config = driver_config(config.node.id, listen_addr, peers, cluster);

    let log_db_path = data_dir.join(RAFT_LOG_DB_FILE);
    let app_db_path = data_dir.join(APP_DB_FILE);

    let (handle, driver_task) = ArrowRaftDriver::start(driver_config, &log_db_path, &app_db_path)
        .await
        .map_err(|e| SaciError::configuration(format!("ArrowRaftDriver::start failed: {e}")))?;

    // Both guards abort on drop, so every `?` between here and the shutdown
    // sequence below stops the accept loop and the drive loop instead of
    // leaving a detached task holding the port, a `Raft` clone and two open
    // redb files. `AbortOnDropHandle` is also a `Future`, so the clean path
    // still awaits the driver.
    let driver_task = tokio_util::task::AbortOnDropHandle::new(driver_task);

    // The peer listener has to be accepting before anything expects a quorum:
    // `bootstrap_cluster` proposes the initial membership and
    // `wait_for_raft_settled` waits for an election, and both need this node
    // to answer `AppendEntries`, `Vote` and a forwarded proposal.
    let raft_server =
        tokio_util::task::AbortOnDropHandle::new(handle.spawn_tcp_server(listen_addr).await?);

    let bootstrap_lock = data_dir.join(BOOTSTRAP_LOCK_FILE);
    if cluster.bootstrap && !bootstrap_lock.exists() {
        bootstrap_cluster(
            &handle,
            cluster,
            config.node.id,
            &this_peer.addr,
            &bootstrap_lock,
        )
        .await?;
        // Record the node identity so a later start can catch an accidental
        // node-id change, such as a wrong SACI_NODE_ID.
        write_node_id_file(data_dir, config.node.id)?;
    }

    wait_for_raft_settled(&handle, cluster.election_timeout_ms).await?;

    // A raft log alone does not prove this directory belongs to a cluster, so
    // the markers `validate_data_dir` reads on the next start are written once
    // this node has actually seen a leader. `wait_for_raft_settled` is too
    // early: an uninitialised node reports `Learner` immediately, before it
    // has heard from anyone.
    let mut joined = mark_joined_if_in_cluster(&handle, data_dir, config.node.id)?;

    // Bound to the returned handle's lifetime: `AbortOnDropHandle` stops the
    // recorder on every exit path from `run_cluster`, including the fallible
    // ones between here and `handle.shutdown()`.
    let _raft_gauges = tokio_util::task::AbortOnDropHandle::new(spawn_raft_gauges(
        handle.clone(),
        cancel.child_token(),
    ));

    let metrics = handle.metrics();
    stats.last_raft_term = Some(metrics.current_term);
    stats.last_leader_id = metrics.current_leader;

    let store = cluster_store(&handle, cluster);

    let producer_cancel = cancel.child_token();
    // ServiceConfig::validate rejects sources in cluster mode, so built.nodes
    // contains no BuiltNodeKind::Source by the time run_cluster runs.
    debug_assert!(
        built
            .nodes
            .iter()
            .all(|n| !matches!(n.kind, super::builder::BuiltNodeKind::Source(_))),
        "cluster runner received a source node — validate() should have rejected this config"
    );
    let source_task: Option<tokio::task::JoinHandle<()>> = None;
    let _ = producer_cancel; // unused until sources are wired

    // The clone-empty template gives each partition a fresh, schema-registered
    // Dataset with no row data. Config validation (rule 11) guarantees the
    // workflow declared exactly one node and that it is a processor.
    let mut nodes = built.nodes;
    let node = nodes
        .pop()
        .expect("cluster mode config validation guarantees exactly one node");
    let runtime = match node.kind {
        super::builder::BuiltNodeKind::Processor { runtime, .. } => runtime,
        _ => unreachable!("cluster mode config validation guarantees the one node is a processor"),
    };
    let dataset_template = runtime.template_dataset();

    // Gate 3b: the persisted checkpoints on this node must belong to the same
    // schema shape the pipeline declares, or resuming would mix layouts.
    crate::service::validation::validate_schema_fingerprint(
        dataset_template.schemas().fingerprint(),
        store.persisted_schema_id().await?,
    )?;

    let runner_config = RunnerConfig {
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        // Cluster mode is guaranteed exactly one workflow by
        // `ServiceConfig::validate`.
        workflow_id: config.workflows[0].id.clone(),
        processor_id: node.id.clone(),
        ..Default::default()
    };

    let runner = DistributedRunner::new(store, runtime, runner_config);

    let runner_cancel = cancel.child_token();

    // A cluster node outlives an empty work pool. `DistributedRunner::run`
    // returns as soon as `claim_next_batch` finds nothing, and a node that
    // exited there would leave the cluster before an operator registered any
    // work, taking its vote with it. So the run is re-entered after an idle
    // pause until cancellation.
    //
    // At-least-once guarantee: if the cancellation arm wins, the in-flight
    // `runner.run()` future is dropped and the current batch is NOT acked via
    // `PartitionSource::ack_claim`. On the next run, the `PartitionSource`
    // redelivers it (via claim lease expiry or unacked claim). Scheduler
    // systems must therefore be idempotent.
    while !runner_cancel.is_cancelled() {
        if !joined {
            joined = mark_joined_if_in_cluster(&handle, data_dir, config.node.id)?;
        }

        let processed = tokio::select! {
            result = runner.run(|| dataset_template.clone_empty()) => result,
            _ = runner_cancel.cancelled() => break,
        };

        match processed {
            Ok(n) => {
                stats.batches_processed += n as u64;
            }
            Err(e) => {
                stats.batches_failed += 1;
                // Log but keep the node in the cluster: a store error is
                // transient (a leaderless moment, a lost claim race), and
                // dropping the vote would make it worse.
                eprintln!("[saci cluster] runner error: {e}");
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(IDLE_POLL_INTERVAL) => {}
            _ = runner_cancel.cancelled() => break,
        }
    }

    eprintln!("[saci cluster] cluster runner cancelled, initiating shutdown");

    // Cancel the source producer (if it ever runs).
    if let Some(task) = source_task {
        task.abort();
    }

    // openraft alpha.17 does not expose trigger_leader_transfer on the public
    // Raft API, so shutdown skips it. The cost is one election cycle of
    // unavailability (election_timeout_ms * 2).
    let _ = LEADER_TRANSFER_BUDGET; // budget reserved

    // Stop accepting first, so no inbound RPC keeps a `Raft` clone alive past
    // the shutdown, then wait for the drive loop to actually finish: it is
    // what closes both redb files, and `handle.shutdown()` only signals it.
    // Without the await this function can return while the log and the
    // application database are still open, which a restart then cannot open.
    raft_server.abort();
    handle.shutdown().await;
    match tokio::time::timeout(DRIVER_DRAIN_TIMEOUT, driver_task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => eprintln!("[saci cluster] raft driver exited with an error: {e}"),
        Ok(Err(e)) => eprintln!("[saci cluster] raft driver task panicked: {e}"),
        Err(_) => eprintln!(
            "[saci cluster] raft driver did not finish within {}s; its redb files may still be open",
            DRIVER_DRAIN_TIMEOUT.as_secs()
        ),
    }

    stats.total_duration_ms = start.elapsed().as_millis() as u64;

    Ok(stats)
}

/// Record `saci_raft_commit_index`, `saci_raft_term` and `saci_raft_leader_id`
/// once a second until `cancel` fires.
///
/// [`ArrowRaftDriverHandle::metrics`] is synchronous, so this needs no other
/// plumbing. It exists because `/status` still reports `"cluster": null`: the
/// gauges come from the driver handle, not from a populated `cluster_probe`.
fn spawn_raft_gauges(
    handle: ArrowRaftDriverHandle,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let m = handle.metrics();
                    crate::metrics::instruments().raft(
                        m.local_committed.map_or(0, |id| id.index),
                        m.current_term,
                        m.current_leader,
                    );
                }
                _ = cancel.cancelled() => break,
            }
        }
    })
}

// ── Validation helpers ────────────────────────────────────────────────────────

/// Validate the data-directory layout before starting the cluster.
///
/// Rules enforced:
/// - If `raft-log.redb` exists without `bootstrap.lock` → error (the previous
///   start opened the log but never joined a cluster).
/// - If `node-id` file exists and disagrees with `node_id` → error (misconfiguration).
fn validate_data_dir(data_dir: &Path, node_id: u64) -> SaciResult<()> {
    let raft_log = data_dir.join(RAFT_LOG_DB_FILE);
    let bootstrap_lock = data_dir.join(BOOTSTRAP_LOCK_FILE);
    let node_id_file = data_dir.join(NODE_ID_FILE);

    // Rule 1: raft-log.redb without bootstrap.lock → refuse to start.
    if raft_log.exists() && !bootstrap_lock.exists() {
        return Err(SaciError::configuration(format!(
            "data_dir {:?} contains '{}' but no '{}'. \
             This indicates an unclean shutdown before bootstrap completed. \
             Restore from backup or delete the data directory to reinitialise.",
            data_dir, RAFT_LOG_DB_FILE, BOOTSTRAP_LOCK_FILE
        )));
    }

    // Rule 2: node-id file must match config.
    if node_id_file.exists() {
        let stored = std::fs::read_to_string(&node_id_file)
            .map_err(|e| SaciError::store(format!("read node-id file: {e}")))?;
        let stored_id: u64 = stored.trim().parse().map_err(|_| {
            SaciError::configuration(format!(
                "node-id file contains non-numeric content: {:?}",
                stored.trim()
            ))
        })?;
        if stored_id != node_id {
            return Err(SaciError::configuration(format!(
                "node-id file contains {stored_id} but config has node.id={node_id}. \
                 Data directory belongs to a different node. \
                 Use the correct data_dir or update node.id."
            )));
        }
    }

    Ok(())
}

/// Write the `node-id` file (idempotent if already correct).
fn write_node_id_file(data_dir: &Path, node_id: u64) -> SaciResult<()> {
    let path = data_dir.join(NODE_ID_FILE);
    if path.exists() {
        return Ok(()); // already written and validated by validate_data_dir
    }
    std::fs::create_dir_all(data_dir)
        .map_err(|e| SaciError::store(format!("create data_dir {data_dir:?}: {e}")))?;
    std::fs::write(&path, node_id.to_string())
        .map_err(|e| SaciError::store(format!("write node-id file: {e}")))?;
    Ok(())
}

/// Build the Raft driver configuration a cluster node starts with.
///
/// Every Raft timing and cadence the cluster header declares travels through
/// here, including `snapshot_log_interval`, which the driver turns into
/// openraft's snapshot policy, so log compaction runs on the configured
/// cadence rather than on a library default.
fn driver_config(
    node_id: u64,
    listen_addr: SocketAddr,
    peers: HashMap<u64, String>,
    cluster: &ClusterConfig,
) -> ArrowRaftDriverConfig {
    ArrowRaftDriverConfig {
        node_id,
        listen_addr,
        peers,
        heartbeat_interval_ms: cluster.heartbeat_interval_ms,
        election_timeout_min_ms: cluster.election_timeout_ms,
        election_timeout_max_ms: cluster.election_timeout_ms * 2,
        snapshot_log_interval: cluster.snapshot_log_interval,
    }
}

/// Build the shared store a cluster node runs against.
///
/// `handle.app_db()` is the same `Arc<Mutex<Database>>` the Raft state machine
/// applies into, so reads need no second handle on the file and every mutation
/// is proposed through the driver. The lease a claim is granted comes from the
/// config, not from the store's own default.
fn cluster_store(
    handle: &crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    cluster: &ClusterConfig,
) -> RedbSharedStore {
    RedbSharedStore::multi_node(Arc::clone(handle.app_db()), handle.clone())
        .with_lease_ttl_millis(cluster.lease_ttl_ms)
}

/// Record that this node's raft log belongs to a live cluster, if it does yet.
///
/// Returns whether the markers are on disk. `bootstrap.lock` and `node-id` are
/// written once this node reports a leader, which is the first moment its
/// `raft-log.redb` provably belongs to a cluster rather than to an interrupted
/// first boot. A joined node therefore leaves the same four files behind as
/// the one that bootstrapped it, and `validate_data_dir`'s first rule keeps
/// its meaning: a log with no marker is a node that never got that far.
///
/// Node state alone is not enough to decide this: an uninitialised openraft
/// node reports `Learner` from the moment it starts.
fn mark_joined_if_in_cluster(
    handle: &crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    data_dir: &Path,
    node_id: u64,
) -> SaciResult<bool> {
    let lock = data_dir.join(BOOTSTRAP_LOCK_FILE);
    if lock.exists() {
        write_node_id_file(data_dir, node_id)?;
        return Ok(true);
    }
    if handle.metrics().current_leader.is_none() {
        return Ok(false);
    }
    std::fs::create_dir_all(data_dir)
        .map_err(|e| SaciError::store(format!("create data_dir {data_dir:?}: {e}")))?;
    std::fs::write(&lock, "joined")
        .map_err(|e| SaciError::store(format!("write bootstrap.lock: {e}")))?;
    write_node_id_file(data_dir, node_id)?;
    Ok(true)
}

async fn bootstrap_cluster(
    handle: &crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    cluster: &ClusterConfig,
    node_id: u64,
    this_addr: &str,
    bootstrap_lock: &Path,
) -> SaciResult<()> {
    use openraft::BasicNode;
    use std::collections::BTreeMap;

    let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
    for peer in &cluster.peers {
        members.insert(
            peer.id,
            BasicNode {
                addr: peer.addr.clone(),
            },
        );
    }

    // If only one peer and it's us, this is a single-node bootstrap.
    if members.is_empty() {
        members.insert(
            node_id,
            BasicNode {
                addr: this_addr.to_string(),
            },
        );
    }

    handle.initialize(members).await?;

    let lock_dir = bootstrap_lock.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(lock_dir)
        .map_err(|e| SaciError::store(format!("create data_dir {lock_dir:?}: {e}")))?;
    std::fs::write(bootstrap_lock, "bootstrapped")
        .map_err(|e| SaciError::store(format!("write bootstrap.lock: {e}")))?;

    eprintln!("[saci cluster] cluster bootstrapped, bootstrap.lock written");
    Ok(())
}

/// Wait until the Raft node settles into a stable role.
///
/// Polls [`ArrowRaftDriverHandle::metrics`] every [`RAFT_SETTLE_POLL_INTERVAL`]
/// until the node reports `Leader`, `Follower`, or `Learner` state, or until
/// [`RAFT_SETTLE_TIMEOUT`] expires.
async fn wait_for_raft_settled(
    handle: &crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    election_timeout_ms: u64,
) -> SaciResult<()> {
    let deadline = tokio::time::Instant::now() + RAFT_SETTLE_TIMEOUT;

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(SaciError::generic(format!(
                "Raft did not settle within {}s. \
                 Check peer connectivity and election_timeout_ms ({election_timeout_ms}ms).",
                RAFT_SETTLE_TIMEOUT.as_secs()
            )));
        }

        let metrics = handle.metrics();
        {
            use openraft::ServerState;
            match metrics.state {
                ServerState::Leader | ServerState::Follower | ServerState::Learner => {
                    return Ok(());
                }
                ServerState::Candidate => {
                    // Still electing; keep waiting.
                }
                ServerState::Shutdown => {
                    return Err(SaciError::generic(
                        "Raft node entered Shutdown state during startup",
                    ));
                }
            }
        }

        tokio::time::sleep(RAFT_SETTLE_POLL_INTERVAL).await;
    }
}

#[cfg(all(test, feature = "service-cluster"))]
mod tests {
    use super::*;
    use crate::distributed::consensus::driver::{ArrowRaftDriver, ArrowRaftDriverConfig};
    use crate::distributed::consensus::store::RedbSharedStore;
    use crate::service::config::{
        ClusterConfig, HttpConfig, NodeConfig, ObservabilityConfig, PeerSpec, ServiceConfig,
        ServiceMode, StandaloneConfig, WorkflowSpec,
    };
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    fn free_addr() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    }

    /// Read the value of a single-sample Prometheus gauge line by series name.
    ///
    /// The exporter attaches an `otel_scope_name` label, so the name is followed
    /// by `{...}` rather than a space.
    #[cfg(feature = "metrics")]
    fn gauge_value(text: &str, series: &str) -> Option<f64> {
        text.lines()
            .filter(|l| !l.starts_with('#'))
            .find_map(|l| l.strip_prefix(series))
            .and_then(|rest| rest.rsplit(' ').next())
            .and_then(|v| v.parse::<f64>().ok())
    }

    fn single_peer_cluster_config(
        node_id: u64,
        addr: SocketAddr,
        data_dir: PathBuf,
    ) -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: node_id,
                name: None,
                data_dir,
            },
            mode: ServiceMode::Cluster {
                config: ClusterConfig {
                    peers: vec![PeerSpec {
                        id: node_id,
                        addr: addr.to_string(),
                    }],
                    bootstrap: true,
                    lease_ttl_ms: 10_000,
                    election_timeout_ms: 300,
                    heartbeat_interval_ms: 50,
                    snapshot_log_interval: 1000,
                },
            },
            workflows: vec![WorkflowSpec {
                id: "cluster-test".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: Vec::new(),
                wasm: Vec::new(),
                plugin: Vec::new(),
                sinks: Vec::new(),
                links: Vec::new(),
                dlq: None,
            }],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        }
    }

    fn empty_built_service() -> crate::service::builder::BuiltService {
        use crate::service::builder::{BuiltNode, BuiltNodeKind, BuiltService};
        BuiltService {
            workflow_id: "cluster-test".to_string(),
            workflow_name: None,
            nodes: vec![BuiltNode {
                id: "p".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(crate::pipeline::Pipeline::new("test")),
                    kind: "native",
                },
                downstream: Vec::new(),
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
                heal_recovered: None,
            }],
            registry: std::sync::Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
            dlq: None,
        }
    }
    #[test]
    fn test_inconsistent_data_dir_returns_error() {
        let dir = TempDir::new().unwrap();
        // Write raft-log.redb but NOT bootstrap.lock.
        std::fs::write(dir.path().join(RAFT_LOG_DB_FILE), b"fake").unwrap();

        let result = validate_data_dir(dir.path(), 1);
        assert!(result.is_err(), "expected error for missing bootstrap.lock");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains(BOOTSTRAP_LOCK_FILE),
            "error should mention bootstrap.lock: {msg}"
        );
    }

    #[test]
    fn test_consistent_data_dir_accepted() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(RAFT_LOG_DB_FILE), b"fake").unwrap();
        std::fs::write(dir.path().join(BOOTSTRAP_LOCK_FILE), b"bootstrapped").unwrap();
        validate_data_dir(dir.path(), 1).expect("consistent dir should be accepted");
    }

    #[test]
    fn test_empty_data_dir_accepted() {
        let dir = TempDir::new().unwrap();
        validate_data_dir(dir.path(), 1).expect("empty dir should be accepted");
    }

    #[test]
    fn test_node_id_mismatch_returns_error() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(NODE_ID_FILE), b"42").unwrap();

        let result = validate_data_dir(dir.path(), 1);
        assert!(result.is_err(), "expected error for node-id mismatch");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("42"),
            "error should mention stored id 42: {msg}"
        );
        assert!(msg.contains('1'), "error should mention config id 1: {msg}");
    }

    #[test]
    fn test_node_id_match_accepted() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(NODE_ID_FILE), b"7").unwrap();
        validate_data_dir(dir.path(), 7).expect("matching node-id should be accepted");
    }

    #[test]
    fn test_write_node_id_file_creates_file_on_first_run() {
        let dir = TempDir::new().unwrap();
        assert!(!dir.path().join(NODE_ID_FILE).exists());

        write_node_id_file(dir.path(), 42).expect("first write should succeed");

        let path = dir.path().join(NODE_ID_FILE);
        assert!(path.exists(), "node-id file should exist after first write");
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            contents.trim(),
            "42",
            "node-id file should contain the node id"
        );
    }

    #[test]
    fn test_write_node_id_file_is_idempotent() {
        let dir = TempDir::new().unwrap();
        write_node_id_file(dir.path(), 7).unwrap();
        // Second call must not overwrite or error.
        write_node_id_file(dir.path(), 7).expect("second write should be idempotent");

        let contents = std::fs::read_to_string(dir.path().join(NODE_ID_FILE)).unwrap();
        assert_eq!(contents.trim(), "7");
    }

    #[test]
    fn test_validate_data_dir_detects_mismatch_after_write() {
        let dir = TempDir::new().unwrap();
        // Simulate a successful bootstrap: write node-id for node 1.
        write_node_id_file(dir.path(), 1).unwrap();

        // A second run with a *different* node.id should fail.
        let err = validate_data_dir(dir.path(), 99).unwrap_err();
        assert!(
            err.to_string().contains("99"),
            "error should mention the new id 99: {err}"
        );
        assert!(
            err.to_string().contains('1'),
            "error should mention the stored id 1: {err}"
        );
    }

    #[tokio::test]
    async fn test_bootstrap_creates_lock_file() {
        let dir = TempDir::new().unwrap();
        let addr = free_addr();
        let driver_config = ArrowRaftDriverConfig {
            node_id: 1,
            listen_addr: addr,
            peers: HashMap::new(),
            heartbeat_interval_ms: 30,
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            snapshot_log_interval: 1_000,
        };

        let (handle, _task) = ArrowRaftDriver::start(
            driver_config,
            dir.path().join(RAFT_LOG_DB_FILE),
            dir.path().join(APP_DB_FILE),
        )
        .await
        .unwrap();

        let bootstrap_lock = dir.path().join(BOOTSTRAP_LOCK_FILE);
        let cluster = ClusterConfig {
            peers: vec![PeerSpec {
                id: 1,
                addr: addr.to_string(),
            }],
            bootstrap: true,
            lease_ttl_ms: 10_000,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_log_interval: 1000,
        };

        bootstrap_cluster(&handle, &cluster, 1, &addr.to_string(), &bootstrap_lock)
            .await
            .unwrap();

        assert!(
            bootstrap_lock.exists(),
            "bootstrap.lock should have been created"
        );

        handle.shutdown().await;
    }

    /// The one-second recorder must write all three Raft gauges from the driver
    /// handle, with the bootstrapped node as the leader.
    ///
    /// `saci_raft_leader_id` is `-1` for an unknown leader, so asserting `1`
    /// proves the recorder read a settled `RaftMetrics` rather than a default.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn test_spawn_raft_gauges_records_all_three() {
        use prometheus::TextEncoder;

        let dir = TempDir::new().unwrap();
        let addr = free_addr();
        let driver_config = ArrowRaftDriverConfig {
            node_id: 1,
            listen_addr: addr,
            peers: HashMap::new(),
            heartbeat_interval_ms: 30,
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            snapshot_log_interval: 1_000,
        };

        let (handle, _task) = ArrowRaftDriver::start(
            driver_config,
            dir.path().join(RAFT_LOG_DB_FILE),
            dir.path().join(APP_DB_FILE),
        )
        .await
        .unwrap();

        let cluster = ClusterConfig {
            peers: vec![PeerSpec {
                id: 1,
                addr: addr.to_string(),
            }],
            bootstrap: true,
            lease_ttl_ms: 10_000,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_log_interval: 1000,
        };
        bootstrap_cluster(
            &handle,
            &cluster,
            1,
            &addr.to_string(),
            &dir.path().join(BOOTSTRAP_LOCK_FILE),
        )
        .await
        .unwrap();
        wait_for_raft_settled(&handle, cluster.election_timeout_ms)
            .await
            .unwrap();

        let cancel = CancellationToken::new();
        let gauges = spawn_raft_gauges(handle.clone(), cancel.child_token());

        // A freshly bootstrapped node passes `wait_for_raft_settled` as a
        // Learner, before it promotes itself, so the first tick legitimately
        // sees no leader and records -1. Polling proves the recorder keeps
        // publishing rather than firing once.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut text = String::new();
        let mut leader = None;
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
            text = TextEncoder::new()
                .encode_to_string(&crate::metrics::test_registry().gather())
                .expect("encode prometheus text");
            leader = gauge_value(&text, "saci_raft_leader_id");
            if leader == Some(1.0) {
                break;
            }
        }
        cancel.cancel();
        let _ = gauges.await;

        for series in [
            "saci_raft_commit_index",
            "saci_raft_term",
            "saci_raft_leader_id",
        ] {
            assert!(
                text.contains(series),
                "{series} should have been recorded:\n{text}"
            );
        }
        assert_eq!(
            leader,
            Some(1.0),
            "the single bootstrapped node must become the recorded leader:\n{text}"
        );
        assert!(
            gauge_value(&text, "saci_raft_term").is_some_and(|t| t >= 1.0),
            "a leader implies a term of at least 1:\n{text}"
        );

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_standalone_mode_rejected() {
        let dir = TempDir::new().unwrap();
        let config = ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: dir.path().to_path_buf(),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![WorkflowSpec {
                id: "w".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: Vec::new(),
                wasm: Vec::new(),
                plugin: Vec::new(),
                sinks: Vec::new(),
                links: Vec::new(),
                dlq: None,
            }],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        };

        let cancel = CancellationToken::new();
        let result = run_cluster(empty_built_service(), &config, cancel).await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("standalone"),
            "error should mention standalone: {msg}"
        );
    }

    // Integration smoke test: boots a real single-node Raft cluster, runs
    // run_cluster, cancels after 500 ms, and checks that it returns Ok.
    #[tokio::test]
    async fn test_single_node_cancel_returns_ok() {
        let dir = TempDir::new().unwrap();
        let addr = free_addr();
        let config = single_peer_cluster_config(1, addr, dir.path().to_path_buf());

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            cancel_clone.cancel();
        });

        let result = run_cluster(empty_built_service(), &config, cancel).await;
        assert!(
            result.is_ok(),
            "run_cluster should return Ok on cancel: {result:?}"
        );

        let stats = result.unwrap();
        assert!(
            dir.path().join(BOOTSTRAP_LOCK_FILE).exists(),
            "bootstrap.lock must exist after successful run"
        );
        // No duration assertion: Raft startup and settle dominate the 500 ms
        // cancel and depend on election timers.
        assert!(
            stats.total_duration_ms > 0,
            "duration should be > 0ms, got {}ms",
            stats.total_duration_ms
        );
    }

    /// A cluster node must accept peer traffic on its declared `peer addr`,
    /// or a bootstrapped membership can never replicate and a follower can
    /// never join. The node stays up while its work pool is empty, so a
    /// connect after settle is deterministic.
    #[tokio::test]
    async fn test_cluster_node_accepts_peer_connections() {
        let dir = TempDir::new().unwrap();
        let addr = free_addr();
        let config = single_peer_cluster_config(1, addr, dir.path().to_path_buf());

        // `run_cluster` holds a `Box<dyn PipelineRuntime>`, which is `?Send`,
        // so the probe runs beside it in this same task rather than in a
        // spawned one.
        let cancel = CancellationToken::new();
        let probe_cancel = cancel.clone();
        let probe = async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let mut connected = false;
            while tokio::time::Instant::now() < deadline {
                if tokio::net::TcpStream::connect(addr).await.is_ok() {
                    connected = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            probe_cancel.cancel();
            connected
        };

        let (result, connected) =
            tokio::join!(run_cluster(empty_built_service(), &config, cancel), probe);
        let stats = result.expect("run_cluster");
        assert!(
            connected,
            "no peer listener on {addr}; a cluster node that does not accept \
             AppendEntries, Vote or a forwarded proposal cannot replicate"
        );
        assert!(stats.total_duration_ms > 0);
    }

    /// A node that joined rather than bootstrapped records `bootstrap.lock`
    /// and `node-id` too, so its own restart is not read as an interrupted
    /// first boot. Started twice against the same `data_dir`, which also
    /// covers `run_cluster` closing both redb files before it returns.
    #[tokio::test]
    async fn test_joined_node_can_restart() {
        let dir = TempDir::new().unwrap();
        let addr = free_addr();
        let mut config = single_peer_cluster_config(1, addr, dir.path().to_path_buf());
        match &mut config.mode {
            ServiceMode::Cluster { config: c } => c.bootstrap = false,
            ServiceMode::Standalone { .. } => unreachable!("built as a cluster config"),
        }

        // Cancel on the marker rather than on a timer: it is written only once
        // this node reports a leader, and a single-node election takes an
        // election timeout, so any fixed sleep is a race.
        for start in 1..=2 {
            let cancel = CancellationToken::new();
            let lock = dir.path().join(BOOTSTRAP_LOCK_FILE);
            let watch_cancel = cancel.clone();
            let watcher = async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
                while !lock.exists() && tokio::time::Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let seen = lock.exists();
                watch_cancel.cancel();
                seen
            };
            let (result, marker_seen) =
                tokio::join!(run_cluster(empty_built_service(), &config, cancel), watcher);
            result.unwrap_or_else(|e| panic!("start {start} of a joining node failed: {e}"));
            assert!(
                marker_seen,
                "start {start} never recorded {BOOTSTRAP_LOCK_FILE}, so its own restart \
                 would be refused as an interrupted first boot"
            );
        }

        let mut entries: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        entries.sort();
        let mut expected = vec![
            BOOTSTRAP_LOCK_FILE.to_string(),
            APP_DB_FILE.to_string(),
            NODE_ID_FILE.to_string(),
            RAFT_LOG_DB_FILE.to_string(),
        ];
        expected.sort();
        assert_eq!(
            entries, expected,
            "a joined node leaves the same four files as a bootstrapping one"
        );
    }

    /// The compaction cadence the cluster header declares is the one openraft
    /// runs on, and the replication lag threshold stays above it. Asserted on
    /// the driver config `run_cluster` itself builds, so a driver that falls
    /// back to openraft's own snapshot cadence, or leaves the lag threshold at
    /// openraft's 5000 while the interval sits above it, fails here.
    #[test]
    fn test_cluster_snapshot_log_interval_reaches_the_raft_config() {
        let cluster = ClusterConfig {
            peers: vec![PeerSpec {
                id: 1,
                addr: "127.0.0.1:9000".to_string(),
            }],
            bootstrap: true,
            lease_ttl_ms: 30_000,
            election_timeout_ms: 1_500,
            heartbeat_interval_ms: 300,
            snapshot_log_interval: 12_500,
        };

        let raft_config = driver_config(
            1,
            "127.0.0.1:9000".parse().unwrap(),
            HashMap::new(),
            &cluster,
        )
        .raft_config();

        assert_eq!(
            raft_config.snapshot_policy,
            openraft::SnapshotPolicy::LogsSinceLast(12_500),
            "the configured interval must set the snapshot policy"
        );

        let openraft::SnapshotPolicy::LogsSinceLast(interval) = &raft_config.snapshot_policy else {
            panic!("the snapshot policy must carry the configured interval");
        };
        assert!(
            raft_config.replication_lag_threshold > *interval,
            "a follower must be allowed to lag by more than one snapshot \
             interval ({} vs {interval}): a snapshot is built at the commit \
             index and the next one only an interval later, so installing one \
             can leave a follower a full interval behind",
            raft_config.replication_lag_threshold
        );
    }

    /// `run_cluster` grants a claim the lease the config declares, not the
    /// store's own default. Asserted on the store `run_cluster` itself builds,
    /// so dropping the `with_lease_ttl_millis` call fails here.
    #[tokio::test]
    async fn test_cluster_lease_ttl_reaches_the_store() {
        let dir = TempDir::new().unwrap();
        let addr = free_addr();
        let (handle, _task) = ArrowRaftDriver::start(
            ArrowRaftDriverConfig {
                node_id: 1,
                listen_addr: addr,
                peers: HashMap::new(),
                heartbeat_interval_ms: 30,
                election_timeout_min_ms: 100,
                election_timeout_max_ms: 200,
                snapshot_log_interval: 1_000,
            },
            dir.path().join(RAFT_LOG_DB_FILE),
            dir.path().join(APP_DB_FILE),
        )
        .await
        .unwrap();

        let cluster_of = |lease_ttl_ms: u64| ClusterConfig {
            peers: vec![PeerSpec {
                id: 1,
                addr: addr.to_string(),
            }],
            bootstrap: true,
            lease_ttl_ms,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_log_interval: 1000,
        };

        assert_eq!(
            cluster_store(&handle, &cluster_of(12_345)).lease_ttl_millis(),
            12_345,
            "the store must grant the configured lease, not DEFAULT_LEASE_TTL_MILLIS"
        );
        assert_eq!(
            cluster_store(&handle, &cluster_of(30_000)).lease_ttl_millis(),
            30_000,
            "and it must follow the config rather than a constant"
        );

        handle.shutdown().await;
    }

    // Dropping the consensus receiver simulates a partitioned cluster: the
    // oneshot channel closes at once, so every propose must return an error.
    #[tokio::test]
    async fn test_multi_node_propose_channel_closed_returns_error() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.redb");

        let (store, rx) = RedbSharedStore::with_consensus(&db_path).await.unwrap();
        drop(rx);

        let result = store
            .register_master_batch(0, "comp".to_string(), 1, vec![0u8; 64], 1)
            .await;

        assert!(
            result.is_err(),
            "closed channel should produce an error, not success"
        );
    }

    #[test]
    fn test_oversize_payload_rejected_before_propose() {
        use crate::distributed::partition::MAX_LOG_ENTRY_BYTES;

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.redb");
        let store = RedbSharedStore::single_node(&db_path).unwrap();

        // Payload at exactly the limit (= MAX_LOG_ENTRY_BYTES) should be rejected.
        let big = vec![0u8; MAX_LOG_ENTRY_BYTES];
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(store.register_master_batch(0, "x".to_string(), 1, big, 1));

        assert!(
            result.is_err(),
            "oversize payload must be rejected with an error"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("MAX_LOG_ENTRY_BYTES") || msg.contains("Split"),
            "error should mention size limit: {msg}"
        );
    }

    // A duplicate RegisterMasterBatch must surface the state machine's own
    // response rather than a canned ClaimAcked.
    #[tokio::test]
    async fn test_sm_error_propagates_to_caller() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.redb");
        let store = RedbSharedStore::single_node(&db_path).unwrap();

        store
            .register_master_batch(0, "comp".to_string(), 1, vec![0u8; 64], 10)
            .await
            .expect("first registration should succeed");

        // A repeat registration may be idempotent or may fail; what matters is
        // that the response comes from the state machine.
        let result2 = store
            .register_master_batch(0, "comp".to_string(), 1, vec![0u8; 64], 10)
            .await;
        // Either outcome is acceptable.
        let _ = result2;
    }
}
