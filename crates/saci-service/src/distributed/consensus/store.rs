//! [`RedbSharedStore`]: Arrow-IPC-native distributed partition source and checkpoint
//! store backed by redb, with optional Raft consensus.
//!
//! ## Type topology
//!
//! `RedbSharedStore` is an enum with two variants:
//!
//! - `SingleNode` wraps [`SingleNodeStore`], which owns an `Arc<Database>` and applies
//!   commands directly to the local redb file. An optional `propose_tx` channel lets
//!   tests route commands through a mock Raft driver (the
//!   [`with_consensus`](RedbSharedStore::with_consensus) path).
//!
//! - `MultiNode` wraps [`MultiNodeStore`] (enabled by `distributed-raft`), which holds
//!   a reference to the state machine's `Arc<Mutex<Database>>` for read-only queries
//!   and routes all mutations through a live `ArrowRaftDriverHandle`. It owns no
//!   `Arc<Database>`, so the type system enforces the invariant.
//!
//! The 1 MiB cap on `ipc_bytes` is enforced at the propose boundary.
//!
//! ## Network-partition safety
//!
//! Every multi-node propose is wrapped in
//! `tokio::time::timeout(CLUSTER_PROPOSE_TIMEOUT, …)`. If the leader is unreachable
//! and the timeout fires, the call returns the retryable
//! `SaciError::generic("cluster propose timeout (30s)")`.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redb::Database;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::checkpoint::{Checkpoint, CheckpointStore};
use crate::distributed::consensus::state_machine::{
    apply as sm_apply, find_data_checkpoint_schema_id as sm_find_data_checkpoint_schema_id,
    find_first_pending_batch, read_checkpoint as sm_read_checkpoint,
};
use crate::distributed::consensus::types::{ConsensusCommand, ConsensusResponse};
use crate::distributed::partition::{BatchClaim, MAX_LOG_ENTRY_BYTES, PartitionSource};

/// Read the current unix-millis wall-clock time on the **propose** path.
///
/// Stamped into [`ConsensusCommand`] variants that carry `now_at_propose` so the state
/// machine's apply handlers stay deterministic. This helper MUST NOT be called from
/// `apply_*`. Determinism is a single-site invariant: the leader stamps time once at
/// propose, followers reuse the stamped value on replay.
pub(super) fn propose_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Max number of claim retries before giving up with a distributed error.
const MAX_CONFLICT_RETRIES: usize = 8;

/// Classify whether a [`ConsensusResponse::Error`] message reports a *transient*
/// row-range overlap (another instance won the race) that the caller should retry,
/// rather than a hard failure.
///
/// The state machine's `apply_claim_row_range` emits an error message containing the
/// substring "overlaps" when the targeted range collides with an existing
/// Claimed/Completed entry.
fn is_transient_overlap_conflict(msg: &str) -> bool {
    msg.contains("overlaps")
}

/// Default lease duration in milliseconds.
///
/// Must stay well above election_timeout × 3.
const DEFAULT_LEASE_TTL_MILLIS: u64 = 90_000; // 90 s

/// Timeout for each Raft propose call in multi-node mode.
///
/// Must cover leader election and a quorum commit even at cold start, when every node
/// starts within seconds of the others. Election alone can take up to
/// `election_timeout_max × 3` ≈ 900 ms and log replication adds a round trip, so 30 s
/// leaves a comfortable margin.
const CLUSTER_PROPOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Internal storage for the single-node variant.
///
/// Owns the redb `Database`. Mutations are applied directly to `db` unless a
/// `propose_tx` channel is present (the `with_consensus` test path).
pub struct SingleNodeStore {
    /// The redb database. Public within the crate so test helpers can seed
    /// data via `sm_apply(&inner.db, …)`.
    pub(crate) db: Arc<Database>,
    /// `None` in production single-node mode. `Some` in tests that use
    /// [`RedbSharedStore::with_consensus`] to drive a mock Raft driver.
    propose_tx: Option<mpsc::Sender<(ConsensusCommand, oneshot::Sender<ConsensusResponse>)>>,
    lease_ttl_millis: u64,
}

impl SingleNodeStore {
    async fn propose(&self, command: ConsensusCommand) -> SaciResult<ConsensusResponse> {
        if let Some(tx) = &self.propose_tx {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send((command, resp_tx))
                .await
                .map_err(|_| SaciError::generic("Raft proposal channel closed"))?;
            tokio::time::timeout(CLUSTER_PROPOSE_TIMEOUT, resp_rx)
                .await
                .map_err(|_| {
                    SaciError::generic(format!(
                        "cluster propose timeout ({}s)",
                        CLUSTER_PROPOSE_TIMEOUT.as_secs()
                    ))
                })?
                .map_err(|_| SaciError::generic("Raft response channel closed"))
        } else {
            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || sm_apply(&db, command))
                .await
                .map_err(|e| SaciError::generic(format!("spawn_blocking panicked: {e}")))?
        }
    }

    async fn scan_pending(&self) -> SaciResult<Option<(u64, (u32, u32))>> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || find_first_pending_batch(&db))
            .await
            .map_err(|e| SaciError::generic(format!("spawn_blocking: {e}")))?
    }

    async fn load_checkpoint_record(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<crate::distributed::consensus::state_machine::CheckpointRecord>> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || sm_read_checkpoint(&db, claim_id, stage_idx))
            .await
            .map_err(|e| SaciError::generic(format!("spawn_blocking: {e}")))?
    }

    async fn persisted_schema_id(&self) -> SaciResult<Option<u32>> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || sm_find_data_checkpoint_schema_id(&db))
            .await
            .map_err(|e| SaciError::generic(format!("spawn_blocking: {e}")))?
    }
}

/// Internal storage for the multi-node (Raft) variant.
///
/// Owns no `Database`. It holds the shared `Arc<Mutex<Database>>` that the Raft state
/// machine owns, used only for read-only queries; all mutations route through the Raft
/// driver handle.
#[cfg(feature = "distributed-raft")]
pub struct MultiNodeStore {
    /// Shared database handle, the same instance the Raft state machine uses. Read-only
    /// queries only; mutations go through `handle`.
    read_db: Arc<std::sync::Mutex<Database>>,
    handle: crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    lease_ttl_millis: u64,
}

#[cfg(feature = "distributed-raft")]
impl MultiNodeStore {
    async fn propose(&self, command: ConsensusCommand) -> SaciResult<ConsensusResponse> {
        tokio::time::timeout(CLUSTER_PROPOSE_TIMEOUT, self.handle.propose(command))
            .await
            .map_err(|_| {
                SaciError::generic(format!(
                    "cluster propose timeout ({}s)",
                    CLUSTER_PROPOSE_TIMEOUT.as_secs()
                ))
            })?
    }

    async fn scan_pending(&self) -> SaciResult<Option<(u64, (u32, u32))>> {
        let db = Arc::clone(&self.read_db);
        tokio::task::spawn_blocking(move || {
            let guard = db
                .lock()
                .map_err(|_| SaciError::store("read DB mutex poisoned"))?;
            find_first_pending_batch(&guard)
        })
        .await
        .map_err(|e| SaciError::generic(format!("spawn_blocking: {e}")))?
    }

    async fn load_checkpoint_record(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<crate::distributed::consensus::state_machine::CheckpointRecord>> {
        let db = Arc::clone(&self.read_db);
        tokio::task::spawn_blocking(move || {
            let db = db
                .lock()
                .map_err(|_| SaciError::store("read DB mutex poisoned"))?;
            sm_read_checkpoint(&db, claim_id, stage_idx)
        })
        .await
        .map_err(|e| SaciError::generic(format!("spawn_blocking: {e}")))?
    }

    async fn persisted_schema_id(&self) -> SaciResult<Option<u32>> {
        let db = Arc::clone(&self.read_db);
        tokio::task::spawn_blocking(move || {
            let db = db
                .lock()
                .map_err(|_| SaciError::store("read DB mutex poisoned"))?;
            sm_find_data_checkpoint_schema_id(&db)
        })
        .await
        .map_err(|e| SaciError::generic(format!("spawn_blocking: {e}")))?
    }
}

/// Distributed partition source and checkpoint store backed by redb.
///
/// Constructed via:
///
/// - [`RedbSharedStore::single_node`]: single instance, no Raft.
/// - [`RedbSharedStore::with_consensus`]: test path with a single redb file and a mock
///   Raft channel returned to the caller for manual driving.
/// - [`RedbSharedStore::multi_node`]: production cluster path (requires
///   `distributed-raft`). No local `Database` ownership; all mutations route through
///   the live Raft driver.
pub enum RedbSharedStore {
    /// Commands applied directly to the local database (with optional mock
    /// Raft channel in the `with_consensus` test path).
    SingleNode(SingleNodeStore),
    /// All mutations proposed through the Raft driver; reads use the shared
    /// state-machine database. This variant holds **no** `Arc<Database>`.
    #[cfg(feature = "distributed-raft")]
    MultiNode(MultiNodeStore),
}

impl RedbSharedStore {
    /// Create a **single-node** store (no Raft).
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] if the database cannot be opened.
    pub fn single_node(db_path: &Path) -> SaciResult<Self> {
        let db = Database::create(db_path)
            .map_err(|e| SaciError::store(format!("open redb at {db_path:?}: {e}")))?;
        Ok(Self::SingleNode(SingleNodeStore {
            db: Arc::new(db),
            propose_tx: None,
            lease_ttl_millis: DEFAULT_LEASE_TTL_MILLIS,
        }))
    }

    /// Create a store wired to a mock Raft consensus channel (test path).
    ///
    /// Returns `(store, proposal_rx)`. The caller drives `proposal_rx` through
    /// a mock Raft driver. This path keeps a real local redb file so test
    /// helpers can seed data via `sm_apply`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] if the database cannot be opened.
    pub async fn with_consensus(
        db_path: &Path,
    ) -> SaciResult<(
        Self,
        mpsc::Receiver<(ConsensusCommand, oneshot::Sender<ConsensusResponse>)>,
    )> {
        let db = Database::create(db_path)
            .map_err(|e| SaciError::store(format!("open redb at {db_path:?}: {e}")))?;
        let (propose_tx, propose_rx) = mpsc::channel(256);
        let store = Self::SingleNode(SingleNodeStore {
            db: Arc::new(db),
            propose_tx: Some(propose_tx),
            lease_ttl_millis: DEFAULT_LEASE_TTL_MILLIS,
        });
        Ok((store, propose_rx))
    }

    /// Create a **multi-node** store backed by a live `ArrowRaftDriverHandle`.
    ///
    /// The production cluster path. All mutations route through the Raft driver.
    /// Read-only operations lock `app_db` directly, the same `Arc<Mutex<Database>>` the
    /// Raft state machine uses, which avoids a second open file handle. This variant
    /// owns no `Arc<Database>`, so no code can apply commands directly to redb in
    /// multi-node mode.
    #[cfg(feature = "distributed-raft")]
    pub fn multi_node(
        app_db: Arc<std::sync::Mutex<Database>>,
        handle: crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    ) -> Self {
        Self::MultiNode(MultiNodeStore {
            read_db: app_db,
            handle,
            lease_ttl_millis: DEFAULT_LEASE_TTL_MILLIS,
        })
    }

    /// Override the lease TTL for testing.
    pub fn with_lease_ttl_millis(self, millis: u64) -> Self {
        match self {
            Self::SingleNode(mut s) => {
                s.lease_ttl_millis = millis;
                Self::SingleNode(s)
            }
            #[cfg(feature = "distributed-raft")]
            Self::MultiNode(mut m) => {
                m.lease_ttl_millis = millis;
                Self::MultiNode(m)
            }
        }
    }

    /// Register a master RecordBatch in the replicated state.
    ///
    /// `ipc_bytes` must be <[`MAX_LOG_ENTRY_BYTES`]. An oversize payload returns an
    /// error; callers must split before proposing.
    pub async fn register_master_batch(
        &self,
        batch_id: u64,
        component: String,
        schema_id: u32,
        ipc_bytes: Vec<u8>,
        total_rows: u32,
    ) -> SaciResult<()> {
        if ipc_bytes.len() > MAX_LOG_ENTRY_BYTES {
            return Err(SaciError::generic(format!(
                "register_master_batch: ipc_bytes ({} bytes) > MAX_LOG_ENTRY_BYTES ({}). \
                 Split the batch before proposing.",
                ipc_bytes.len(),
                MAX_LOG_ENTRY_BYTES
            )));
        }
        let cmd = ConsensusCommand::RegisterMasterBatch {
            batch_id,
            component,
            schema_id,
            ipc_bytes,
            total_rows,
            now_at_propose: propose_now_millis(),
        };
        match self.propose(cmd).await? {
            ConsensusResponse::MasterBatchRegistered { .. } => Ok(()),
            ConsensusResponse::Error { message } => Err(SaciError::generic(format!(
                "register_master_batch: {message}"
            ))),
            other => Err(SaciError::generic(format!(
                "register_master_batch unexpected response: {other:?}"
            ))),
        }
    }

    /// Sweep expired leases by proposing `ReclaimExpired` with the given unix-millis
    /// timestamp.
    ///
    /// Any `Claimed` row-range whose `lease_expires_at < now_millis` is reset to
    /// `Pending`, making it available for the next runner. Returns the number of
    /// claims freed.
    ///
    /// Callers pass the current wall-clock unix-millis. The value is embedded in the
    /// Raft log entry so all replicas apply the same expiry boundary.
    pub async fn propose_reclaim_expired(&self, now_millis: u64) -> SaciResult<u32> {
        let cmd = ConsensusCommand::ReclaimExpired {
            now_at_propose: now_millis,
        };
        match self.propose(cmd).await? {
            ConsensusResponse::ExpiredReclaimed { reclaimed_count } => Ok(reclaimed_count),
            ConsensusResponse::Error { message } => Err(SaciError::generic(format!(
                "propose_reclaim_expired: {message}"
            ))),
            other => Err(SaciError::generic(format!(
                "propose_reclaim_expired unexpected response: {other:?}"
            ))),
        }
    }

    /// Lease, in milliseconds, that this store grants a claim it hands out.
    ///
    /// Set by [`with_lease_ttl_millis`](Self::with_lease_ttl_millis), otherwise
    /// `DEFAULT_LEASE_TTL_MILLIS`.
    pub fn lease_ttl_millis(&self) -> u64 {
        match self {
            Self::SingleNode(s) => s.lease_ttl_millis,
            #[cfg(feature = "distributed-raft")]
            Self::MultiNode(m) => m.lease_ttl_millis,
        }
    }

    async fn propose(&self, command: ConsensusCommand) -> SaciResult<ConsensusResponse> {
        match self {
            Self::SingleNode(s) => s.propose(command).await,
            #[cfg(feature = "distributed-raft")]
            Self::MultiNode(m) => m.propose(command).await,
        }
    }

    async fn scan_pending(&self) -> SaciResult<Option<(u64, (u32, u32))>> {
        match self {
            Self::SingleNode(s) => s.scan_pending().await,
            #[cfg(feature = "distributed-raft")]
            Self::MultiNode(m) => m.scan_pending().await,
        }
    }

    async fn load_checkpoint_record(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<crate::distributed::consensus::state_machine::CheckpointRecord>> {
        match self {
            Self::SingleNode(s) => s.load_checkpoint_record(claim_id, stage_idx).await,
            #[cfg(feature = "distributed-raft")]
            Self::MultiNode(m) => m.load_checkpoint_record(claim_id, stage_idx).await,
        }
    }
}

#[async_trait]
impl PartitionSource for RedbSharedStore {
    /// Claim the next pending row-range, retrying on transient overlap conflicts
    /// caused by racing instances.
    ///
    /// The retry loop is bounded and covers transient overlap errors only; any other
    /// error propagates as a hard failure. Returning `Ok(None)` for an overlap would
    /// let the losing runner exit as if the cluster were idle.
    async fn claim_next_batch(&self, instance_id: Uuid) -> SaciResult<Option<BatchClaim>> {
        let lease_ttl_millis = self.lease_ttl_millis();
        for attempt in 0..MAX_CONFLICT_RETRIES {
            let range = self.scan_pending().await?;

            let (batch_id, (row_start, row_end)) = match range {
                None => return Ok(None),
                Some(r) => r,
            };

            let claim_id = Uuid::now_v7();
            let cmd = ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: row_start,
                row_range_end: row_end,
                claim_id,
                instance_id,
                lease_ttl_millis,
                now_at_propose: propose_now_millis(),
            };

            match self.propose(cmd).await? {
                ConsensusResponse::BatchClaimed {
                    batch_id,
                    component,
                    row_range_start,
                    row_range_end,
                    schema_id,
                    claim_id,
                    instance_id,
                    lease_expires_at,
                } => {
                    return Ok(Some(BatchClaim {
                        batch_id,
                        component,
                        row_range: row_range_start..row_range_end,
                        schema_id,
                        claim_id,
                        instance_id,
                        lease_expires_at,
                        lease_ttl_millis,
                        claimed_at: Instant::now(),
                    }));
                }
                ConsensusResponse::NoBatchAvailable => return Ok(None),
                ConsensusResponse::Error { message } => {
                    if is_transient_overlap_conflict(&message) {
                        // Exponential backoff with jitter: base 10ms × 2^attempt, capped at 1s.
                        let base_ms = 10u64 * (1u64 << attempt.min(6));
                        let jitter_ms = (claim_id.as_u128() as u64 >> 32) % (base_ms / 2 + 1);
                        let delay = Duration::from_millis(base_ms + jitter_ms);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(SaciError::generic(format!(
                        "claim_next_batch hard failure: {message}"
                    )));
                }
                other => {
                    return Err(SaciError::generic(format!(
                        "claim_next_batch unexpected response: {other:?}"
                    )));
                }
            }
        }

        Err(SaciError::generic(format!(
            "claim_next_batch: exhausted retries after {MAX_CONFLICT_RETRIES} transient overlap conflicts"
        )))
    }

    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
        let cmd = ConsensusCommand::RenewClaim {
            claim_id,
            instance_id,
            lease_ttl_millis: self.lease_ttl_millis(),
            now_at_propose: propose_now_millis(),
        };
        match self.propose(cmd).await? {
            ConsensusResponse::ClaimRenewed { expires_at } => Ok(expires_at),
            ConsensusResponse::Error { message } => {
                Err(SaciError::generic(format!("renew_claim failed: {message}")))
            }
            other => Err(SaciError::generic(format!(
                "renew_claim unexpected response: {other:?}"
            ))),
        }
    }

    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        match self
            .propose(ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            })
            .await?
        {
            ConsensusResponse::ClaimAcked => Ok(()),
            ConsensusResponse::Error { message } => {
                Err(SaciError::generic(format!("ack_claim failed: {message}")))
            }
            other => Err(SaciError::generic(format!(
                "ack_claim unexpected response: {other:?}"
            ))),
        }
    }

    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        match self
            .propose(ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            })
            .await?
        {
            ConsensusResponse::ClaimReleased => Ok(()),
            ConsensusResponse::Error { message } => Err(SaciError::generic(format!(
                "release_claim failed: {message}"
            ))),
            other => Err(SaciError::generic(format!(
                "release_claim unexpected response: {other:?}"
            ))),
        }
    }

    async fn reclaim_expired(&self, now_millis: u64) -> SaciResult<u32> {
        self.propose_reclaim_expired(now_millis).await
    }
}

#[async_trait]
impl CheckpointStore for RedbSharedStore {
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> SaciResult<()> {
        if ipc_bytes.len() > MAX_LOG_ENTRY_BYTES {
            return Err(SaciError::generic(format!(
                "save_checkpoint: ipc_bytes ({} bytes) > MAX_LOG_ENTRY_BYTES. Split per component.",
                ipc_bytes.len()
            )));
        }
        let cmd = ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx,
            ipc_bytes,
            schema_id,
            now_at_propose: propose_now_millis(),
        };
        match self.propose(cmd).await? {
            ConsensusResponse::CheckpointWritten { .. } => Ok(()),
            ConsensusResponse::Error { message } => Err(SaciError::generic(format!(
                "save_checkpoint failed: {message}"
            ))),
            other => Err(SaciError::generic(format!(
                "save_checkpoint unexpected response: {other:?}"
            ))),
        }
    }

    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<Checkpoint>> {
        let record = self.load_checkpoint_record(claim_id, stage_idx).await?;
        Ok(record.map(|r| Checkpoint {
            batch_id: r.batch_id,
            stage_idx: r.stage_idx,
            payload: r.ipc_bytes,
            schema_id: r.schema_id,
            created_at: r.created_at,
        }))
    }

    /// Schema fingerprint recorded by any persisted data checkpoint on this
    /// node, read from `arrow_checkpoints`.
    async fn persisted_schema_id(&self) -> SaciResult<Option<u32>> {
        match self {
            Self::SingleNode(s) => s.persisted_schema_id().await,
            #[cfg(feature = "distributed-raft")]
            Self::MultiNode(m) => m.persisted_schema_id().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::state_machine::apply as sm_apply;
    use std::path::PathBuf;

    fn temp_path() -> PathBuf {
        let dir = std::env::temp_dir();
        dir.join(format!("saci_arrow_store_test_{}.db", Uuid::now_v7()))
    }

    fn make_store(path: &Path) -> RedbSharedStore {
        RedbSharedStore::single_node(path).expect("single_node store")
    }

    /// Extract the `Arc<Database>` from a `SingleNode` store for test seeding.
    ///
    /// Panics on a `MultiNode` store: tests that need direct DB access must use the
    /// single-node constructor.
    fn store_db(store: &RedbSharedStore) -> Arc<Database> {
        match store {
            RedbSharedStore::SingleNode(s) => Arc::clone(&s.db),
            #[cfg(feature = "distributed-raft")]
            RedbSharedStore::MultiNode(_) => {
                panic!("store_db called on MultiNode store — use single_node() in tests")
            }
        }
    }

    fn seed_batch(store: &RedbSharedStore, batch_id: u64, total_rows: u32) {
        sm_apply(
            &store_db(store),
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 64],
                total_rows,
                now_at_propose: 0,
            },
        )
        .expect("seed_batch");
    }

    #[tokio::test]
    async fn test_single_node_claim_and_ack() {
        let path = temp_path();
        let store = make_store(&path);
        seed_batch(&store, 0, 100);

        let instance = Uuid::now_v7();
        let claim = store
            .claim_next_batch(instance)
            .await
            .expect("claim")
            .expect("should have claim");

        assert_eq!(claim.batch_id, 0);
        assert_eq!(claim.row_range, 0..100);

        store
            .ack_claim(claim.claim_id, claim.instance_id)
            .await
            .expect("ack");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_single_node_release_and_reclaim() {
        let path = temp_path();
        let store = make_store(&path);
        seed_batch(&store, 0, 50);

        let instance = Uuid::now_v7();
        let claim = store.claim_next_batch(instance).await.unwrap().unwrap();

        store
            .release_claim(claim.claim_id, claim.instance_id)
            .await
            .unwrap();

        // Re-claim should succeed.
        let claim2 = store.claim_next_batch(instance).await.unwrap().unwrap();
        assert_eq!(claim2.batch_id, 0);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_single_node_checkpoint_round_trip() {
        let path = temp_path();
        let store = make_store(&path);
        seed_batch(&store, 0, 100);

        let instance = Uuid::now_v7();
        let claim = store.claim_next_batch(instance).await.unwrap().unwrap();

        store
            .save_checkpoint(claim.claim_id, 1, vec![0xDE, 0xAD], 1)
            .await
            .unwrap();

        let cp = store
            .load_checkpoint(claim.claim_id, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.payload, vec![0xDE, 0xAD]);
        assert_eq!(cp.stage_idx, 1);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_multi_node_mock_raft_driver() {
        let path = temp_path();
        let (store, mut rx) = RedbSharedStore::with_consensus(&path).await.unwrap();

        // Seed a batch directly into the shared DB.
        let db = store_db(&store);
        sm_apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 0,
                component: "orders".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 64],
                total_rows: 200,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Spawn mock Raft driver that applies commands directly.
        tokio::spawn(async move {
            while let Some((cmd, resp_tx)) = rx.recv().await {
                let response = sm_apply(&db, cmd).unwrap_or(ConsensusResponse::Error {
                    message: "sm_apply failed".to_string(),
                });
                let _ = resp_tx.send(response);
            }
        });

        let instance = Uuid::now_v7();
        let claim = store
            .claim_next_batch(instance)
            .await
            .expect("claim via mock Raft")
            .expect("should have claim");
        assert_eq!(claim.batch_id, 0);

        store
            .ack_claim(claim.claim_id, claim.instance_id)
            .await
            .unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_register_oversize_rejected_at_propose_boundary() {
        let path = temp_path();
        let store = make_store(&path);

        // Exactly MAX_LOG_ENTRY_BYTES is allowed; one byte over is rejected.
        let big_ipc = vec![0u8; MAX_LOG_ENTRY_BYTES + 1];
        let result = store
            .register_master_batch(0, "x".to_string(), 1, big_ipc, 1)
            .await;
        assert!(
            result.is_err(),
            "should reject payload > MAX_LOG_ENTRY_BYTES"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_checkpoint_oversize_rejected_at_propose_boundary() {
        let path = temp_path();
        let store = make_store(&path);
        seed_batch(&store, 0, 10);
        let instance = Uuid::now_v7();
        let claim = store.claim_next_batch(instance).await.unwrap().unwrap();

        // One byte over the limit must be rejected.
        let big = vec![0u8; MAX_LOG_ENTRY_BYTES + 1];
        let result = store.save_checkpoint(claim.claim_id, 0, big, 1).await;
        assert!(
            result.is_err(),
            "should reject checkpoint > MAX_LOG_ENTRY_BYTES"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_is_transient_overlap_conflict_classifier() {
        assert!(is_transient_overlap_conflict(
            "row_range [0, 100) overlaps an existing active claim"
        ));
        assert!(!is_transient_overlap_conflict("batch_id 7 not found"));
        assert!(!is_transient_overlap_conflict("claim_id X not found"));
    }

    /// `claim_next_batch` must retry on transient overlap conflicts rather than
    /// silently returning `Ok(None)`, which would make a racing cluster quiesce as if
    /// it were idle.
    ///
    /// The `with_consensus` mock-Raft path drives the first propose to a synthetic
    /// overlap error, then lets the second attempt succeed via the real state machine.
    #[tokio::test]
    async fn test_claim_next_batch_retries_on_transient_overlap_conflict() {
        let path = temp_path();
        let (store, mut rx) = RedbSharedStore::with_consensus(&path).await.unwrap();

        // Seed a batch directly into the shared DB so scan_pending finds it.
        let db = store_db(&store);
        sm_apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 0,
                component: "orders".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Mock Raft driver: inject a transient overlap error on attempt #1,
        // then apply normally on attempt #2.
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            let mut attempt: usize = 0;
            while let Some((cmd, resp_tx)) = rx.recv().await {
                attempt += 1;
                if attempt == 1 {
                    let _ = resp_tx.send(ConsensusResponse::Error {
                        message: "row_range [0, 100) overlaps an existing active claim".to_string(),
                    });
                } else {
                    let response = sm_apply(&db_clone, cmd).unwrap_or(ConsensusResponse::Error {
                        message: "sm_apply failed".to_string(),
                    });
                    let _ = resp_tx.send(response);
                }
            }
        });

        let instance = Uuid::now_v7();
        let claim = store
            .claim_next_batch(instance)
            .await
            .expect("claim_next_batch should retry and succeed")
            .expect("expected a claim after retry");
        assert_eq!(claim.batch_id, 0);
        let _ = std::fs::remove_file(&path);
    }
}
