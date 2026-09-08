//! Arrow-IPC Raft node driver.
//!
//! [`ArrowRaftDriver`] manages an openraft node parameterised with
//! [`SaciTypeConfig`](crate::distributed::consensus::types::SaciTypeConfig). It:
//!
//! 1. Initialises the Raft node (single-node cluster or provided peers).
//! 2. Receives [`ConsensusCommand`](crate::distributed::consensus::ConsensusCommand) proposals.
//! 3. Calls `Raft::client_write` directly with the command, with no intermediate
//!    string encoding, because `D = ConsensusCommand` for `SaciTypeConfig`.
//! 4. Returns the [`ConsensusResponse`](crate::distributed::consensus::ConsensusResponse) via a oneshot reply channel.

#[cfg(feature = "distributed-raft")]
pub(crate) mod raft_impl {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;

    use openraft::BasicNode;
    use openraft::Config as RaftConfig;
    use openraft::Raft;
    use openraft::SnapshotPolicy;
    use openraft::error::InitializeError;
    use openraft::storage::{RaftLogStorage, RaftStateMachine};
    use tokio::sync::{mpsc, oneshot};

    use crate::SaciError;
    use crate::SaciResult;
    use crate::distributed::consensus::storage::raft_impl::{
        ArrowRedbLogStore, ArrowRedbStateMachine, validate_store_consistency,
    };
    use crate::distributed::consensus::transport::TcpNetworkFactory;
    use crate::distributed::consensus::types::{
        ConsensusCommand as SmConsensusCommand, ConsensusResponse, SaciTypeConfig,
    };

    pub type ArrowSaciRaft = Raft<SaciTypeConfig, ArrowRedbStateMachine>;

    /// Configuration for [`ArrowRaftDriver`].
    #[derive(Debug, Clone)]
    pub struct ArrowRaftDriverConfig {
        pub node_id: u64,
        pub listen_addr: SocketAddr,
        /// Peer addresses as `"host:port"` strings. These may be IP literals or
        /// hostnames resolved lazily at connection time, such as Docker service names.
        /// An empty map triggers single-node auto-initialisation.
        pub peers: HashMap<u64, String>,
        pub heartbeat_interval_ms: u64,
        pub election_timeout_min_ms: u64,
        pub election_timeout_max_ms: u64,
        /// Take a snapshot once this many log entries have been committed
        /// since the last one, which is what purges the log behind it.
        /// Carried into [`SnapshotPolicy::LogsSinceLast`]; must be at least 1.
        pub snapshot_log_interval: u64,
    }

    impl Default for ArrowRaftDriverConfig {
        fn default() -> Self {
            Self {
                node_id: 1,
                listen_addr: "127.0.0.1:7101".parse().unwrap(),
                peers: HashMap::new(),
                heartbeat_interval_ms: 50,
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
                snapshot_log_interval: 10_000,
            }
        }
    }

    impl ArrowRaftDriverConfig {
        /// Build the openraft configuration this node runs with.
        ///
        /// `snapshot_log_interval` becomes the snapshot policy, so compaction
        /// follows the configured cadence. It also derives
        /// `replication_lag_threshold` as twice itself, rather than leaving
        /// openraft's default of 5000 beside an interval that may exceed it.
        /// `SnapshotPolicy::LogsSinceLast` builds a snapshot once the commit
        /// index has advanced one interval past the last one, so the newest
        /// snapshot a leader can transmit is up to a full interval behind its
        /// own log tail, and installing it leaves the receiver that far
        /// behind. A threshold below the interval would therefore call a
        /// follower lagging in a state that transmitting a snapshot cannot
        /// improve. Openraft derives the same doubling in its own fuzz
        /// harness, whose field comment reads "must exceed
        /// snapshot_logs_threshold"; the second interval is headroom for the
        /// entries committed while a snapshot is in flight. The multiply is
        /// `saturating_mul`, because the interval is an operator-supplied
        /// `u64`: it pins at `u64::MAX` instead of wrapping to a threshold
        /// below the interval. Openraft's `Config::validate` checks no
        /// relation between the two fields, so an incoherent pair would start
        /// silently. This openraft version reads the threshold only in the
        /// blocking wait of `add_learner`, which this driver does not call, so
        /// the derivation keeps the pair coherent for whenever a join path
        /// does.
        ///
        /// `max_in_snapshot_log_to_keep` and `purge_batch_size` stay at
        /// openraft's defaults: they bound how much of an already-snapshotted
        /// log is retained and how eagerly it is trimmed, which is a separate
        /// decision from how often a snapshot is taken.
        pub(crate) fn raft_config(&self) -> RaftConfig {
            RaftConfig {
                cluster_name: "arrow-saci-cluster".into(),
                heartbeat_interval: self.heartbeat_interval_ms,
                election_timeout_min: self.election_timeout_min_ms,
                election_timeout_max: self.election_timeout_max_ms,
                snapshot_policy: SnapshotPolicy::LogsSinceLast(self.snapshot_log_interval),
                replication_lag_threshold: self.snapshot_log_interval.saturating_mul(2),
                ..Default::default()
            }
        }
    }

    /// Handle for submitting proposals and requesting shutdown.
    #[derive(Clone)]
    pub struct ArrowRaftDriverHandle {
        proposal_tx: mpsc::Sender<(
            SmConsensusCommand,
            oneshot::Sender<SaciResult<ConsensusResponse>>,
        )>,
        shutdown_tx: Arc<tokio::sync::Mutex<Option<oneshot::Sender<()>>>>,
        /// The underlying Raft instance; exposes `metrics()` and `initialize()`.
        raft: ArrowSaciRaft,
        /// The redb database shared with the Raft state machine.
        ///
        /// Used by `RedbSharedStore::multi_node` to open read-only queries
        /// against the same file that the state machine writes to.
        app_db: Arc<std::sync::Mutex<redb::Database>>,
    }

    impl ArrowRaftDriverHandle {
        pub async fn propose(&self, cmd: SmConsensusCommand) -> SaciResult<ConsensusResponse> {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.proposal_tx
                .send((cmd, reply_tx))
                .await
                .map_err(|_| SaciError::generic("ArrowRaftDriver: proposal channel closed"))?;
            reply_rx
                .await
                .map_err(|_| SaciError::generic("ArrowRaftDriver: reply channel closed"))?
        }

        pub async fn shutdown(&self) {
            let mut guard = self.shutdown_tx.lock().await;
            if let Some(tx) = guard.take() {
                let _ = tx.send(());
            }
        }

        /// Return the redb database shared with the Raft state machine.
        pub fn app_db(&self) -> &Arc<std::sync::Mutex<redb::Database>> {
            &self.app_db
        }

        /// Return the latest Raft metrics snapshot.
        pub fn metrics(&self) -> openraft::RaftMetrics<SaciTypeConfig> {
            use openraft::async_runtime::WatchReceiver;
            self.raft.metrics().borrow_watched().clone()
        }

        /// Bind `listen_addr` and serve inbound Raft RPCs from cluster peers.
        ///
        /// Call this once per node after `ArrowRaftDriver::start` in multi-node
        /// mode. Without it, other nodes cannot deliver heartbeats, votes, or
        /// log entries here, and the cluster fails to elect a leader.
        ///
        /// The bind happens before the accept task is spawned, so a taken
        /// address is reported here instead of leaving the node running as a
        /// member no peer can reach. The returned `JoinHandle` runs the accept
        /// loop until it is aborted or the process exits.
        ///
        /// # Errors
        ///
        /// Returns [`SaciError::Store`] when `listen_addr` cannot be bound.
        pub async fn spawn_tcp_server(
            &self,
            listen_addr: std::net::SocketAddr,
        ) -> SaciResult<tokio::task::JoinHandle<std::io::Result<()>>> {
            use crate::distributed::consensus::transport::RaftTcpServer;
            RaftTcpServer::new(self.raft.clone(), listen_addr)
                .bind_and_spawn()
                .await
                .map_err(|e| SaciError::store(format!("bind raft listener on {listen_addr}: {e}")))
        }

        /// Bootstrap the Raft cluster with the given initial membership.
        ///
        /// This is a no-op if the cluster is already initialised
        /// (`NotAllowed` error is swallowed).
        pub async fn initialize(
            &self,
            members: std::collections::BTreeMap<u64, BasicNode>,
        ) -> SaciResult<()> {
            match self.raft.initialize(members).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    if matches!(
                        e,
                        openraft::error::RaftError::APIError(InitializeError::NotAllowed(_))
                    ) {
                        Ok(()) // already initialised
                    } else {
                        Err(SaciError::configuration(format!("raft initialize: {e}")))
                    }
                }
            }
        }
    }

    pub struct ArrowRaftDriver;

    impl ArrowRaftDriver {
        pub async fn start(
            config: ArrowRaftDriverConfig,
            log_db_path: impl Into<PathBuf>,
            app_db_path: impl Into<PathBuf>,
        ) -> SaciResult<(
            ArrowRaftDriverHandle,
            tokio::task::JoinHandle<SaciResult<()>>,
        )> {
            let log_db_path = log_db_path.into();
            let app_db_path = app_db_path.into();

            let raft_config = Arc::new(
                config
                    .raft_config()
                    .validate()
                    .map_err(|e| SaciError::configuration(format!("openraft config: {e}")))?,
            );

            let mut log_store = ArrowRedbLogStore::open(&log_db_path)?;
            let app_db = Arc::new(std::sync::Mutex::new(
                redb::Database::create(&app_db_path)
                    .map_err(|e| SaciError::store(format!("open app_db: {e}")))?,
            ));
            let mut state_machine = ArrowRedbStateMachine::open(app_db.clone())
                .map_err(|e| SaciError::store(format!("open state machine: {e}")))?;

            // Both halves of the node directory are open; refuse to start if the
            // state machine is behind what the log store already purged. Without
            // this a node restored from mismatched backups would silently
            // diverge instead of failing loudly.
            {
                let log_state = log_store
                    .get_log_state()
                    .await
                    .map_err(|e| SaciError::store(format!("read log state: {e}")))?;
                let (last_applied, _membership) = state_machine
                    .applied_state()
                    .await
                    .map_err(|e| SaciError::store(format!("read applied state: {e}")))?;
                validate_store_consistency(log_state.last_purged_log_id, last_applied)?;
            }

            let peers_basic: HashMap<u64, BasicNode> = config
                .peers
                .iter()
                .map(|(id, addr)| (*id, BasicNode { addr: addr.clone() }))
                .collect();
            let network = TcpNetworkFactory::from_basic_nodes(&peers_basic);

            let raft: ArrowSaciRaft = Raft::new(
                config.node_id,
                raft_config,
                network,
                log_store,
                state_machine,
            )
            .await
            .map_err(|e| SaciError::configuration(format!("Raft::new: {e}")))?;

            if config.peers.is_empty() {
                let mut members = std::collections::BTreeMap::new();
                members.insert(
                    config.node_id,
                    BasicNode {
                        addr: config.listen_addr.to_string(),
                    },
                );
                match raft.initialize(members).await {
                    Ok(()) => {}
                    Err(e) => {
                        if !matches!(
                            e,
                            openraft::error::RaftError::APIError(InitializeError::NotAllowed(_))
                        ) {
                            return Err(SaciError::configuration(format!("raft initialize: {e}")));
                        }
                    }
                }
            }

            // `run_loop` takes ownership for leader forwarding.
            let peers = config.peers;

            let (proposal_tx, proposal_rx) = mpsc::channel::<(
                SmConsensusCommand,
                oneshot::Sender<SaciResult<ConsensusResponse>>,
            )>(128);
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

            let handle = ArrowRaftDriverHandle {
                proposal_tx,
                shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
                raft: raft.clone(),
                app_db,
            };

            let raft_clone = raft.clone();
            let join =
                tokio::spawn(
                    async move { run_loop(raft_clone, peers, proposal_rx, shutdown_rx).await },
                );

            Ok((handle, join))
        }
    }

    async fn run_loop(
        raft: ArrowSaciRaft,
        peers: HashMap<u64, String>,
        mut proposal_rx: mpsc::Receiver<(
            SmConsensusCommand,
            oneshot::Sender<SaciResult<ConsensusResponse>>,
        )>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> SaciResult<()> {
        loop {
            tokio::select! {
                proposal_opt = proposal_rx.recv() => {
                    match proposal_opt {
                        Some((cmd, reply_tx)) => {
                            let result = write_command(&raft, &peers, cmd).await;
                            let _ = reply_tx.send(result);
                        }
                        None => break,
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
        raft.shutdown()
            .await
            .map_err(|e| SaciError::generic(format!("raft shutdown: {e}")))?;
        Ok(())
    }

    async fn write_command(
        raft: &ArrowSaciRaft,
        peers: &HashMap<u64, String>,
        cmd: SmConsensusCommand,
    ) -> SaciResult<ConsensusResponse> {
        // `D = ConsensusCommand`, `R = ConsensusResponse` on `SaciTypeConfig`.
        // The state machine returns a `ConsensusResponse` via the responder.
        use openraft::error::{ClientWriteError, RaftError};
        match raft.client_write(cmd.clone()).await {
            Ok(r) => Ok(r.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                // This node is a follower; locate the leader and forward.
                // Prefer the address from our peers map (configured at startup)
                // since the ForwardToLeader node info may be incomplete.
                let leader_addr: Option<String> = fwd
                    .leader_id
                    .and_then(|id| peers.get(&id).cloned())
                    .or_else(|| fwd.leader_node.map(|n| n.addr.clone()));

                match leader_addr {
                    Some(addr) => {
                        use crate::distributed::consensus::transport::forward_proposal;
                        forward_proposal(&addr, cmd).await
                    }
                    None => {
                        // No leader elected yet, so let the caller back off and retry.
                        Ok(ConsensusResponse::NoBatchAvailable)
                    }
                }
            }
            Err(e) => Err(SaciError::generic(format!("client_write: {e}"))),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::time::Duration;
        use tempfile::TempDir;

        fn free_addr() -> SocketAddr {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        }

        #[tokio::test]
        async fn test_arrow_driver_starts_and_shuts_down() {
            let dir = TempDir::new().unwrap();
            let addr = free_addr();
            let config = ArrowRaftDriverConfig {
                node_id: 1,
                listen_addr: addr,
                peers: HashMap::new(),
                heartbeat_interval_ms: 30,
                election_timeout_min_ms: 100,
                election_timeout_max_ms: 200,
                snapshot_log_interval: 1_000,
            };

            let (handle, task) = ArrowRaftDriver::start(
                config,
                dir.path().join("arrow_log.redb"),
                dir.path().join("arrow_app.redb"),
            )
            .await
            .unwrap();

            tokio::time::sleep(Duration::from_millis(300)).await;
            handle.shutdown().await;
            let result = tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("driver should stop within 3s");
            assert!(result.is_ok());
        }

        /// A log store that has purged past an empty state machine is the exact
        /// mismatched-backup shape `validate_store_consistency` describes.
        /// `start` must refuse rather than come up and diverge.
        #[tokio::test]
        async fn test_start_refuses_inconsistent_node_directory() {
            use openraft::vote::RaftLeaderId;

            let dir = TempDir::new().unwrap();
            let log_path = dir.path().join("arrow_log.redb");
            let app_path = dir.path().join("arrow_app.redb");

            // Purge the log up to index 10 while leaving the state machine file
            // untouched, so `last_applied` stays `None`.
            {
                let mut log_store = ArrowRedbLogStore::open(&log_path).unwrap();
                let purged = openraft::LogId::new(
                    openraft::impls::leader_id_adv::LeaderId::new(1u64, 1u64),
                    10,
                );
                log_store.purge(purged).await.unwrap();
            }

            let config = ArrowRaftDriverConfig {
                node_id: 1,
                listen_addr: free_addr(),
                peers: HashMap::new(),
                heartbeat_interval_ms: 30,
                election_timeout_min_ms: 100,
                election_timeout_max_ms: 200,
                snapshot_log_interval: 1_000,
            };

            let err = ArrowRaftDriver::start(config, &log_path, &app_path)
                .await
                .err()
                .expect("start must refuse an inconsistent node directory");
            let msg = err.to_string();
            assert!(
                msg.contains("store consistency violation"),
                "error must name the consistency check; got: {msg}"
            );
        }
    }
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::{ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle, ArrowSaciRaft};
