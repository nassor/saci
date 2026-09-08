//! Arrow-IPC-native checkpoint store traits and types.
//!
//! [`CheckpointStore`] persists intermediate pipeline state as Arrow IPC bytes
//! so that a runner can resume from the last completed stage after a crash or
//! lease expiry.

use async_trait::async_trait;
use uuid::Uuid;

use crate::SaciResult;

/// Sentinel `stage_idx` value used to store the window accumulator checkpoint.
///
/// Regular pipeline stages number from 0 and stay in the dozens, so `u32::MAX` cannot
/// collide with a real stage index. The state machine has no upper bound on
/// `stage_idx`, so this value is safe to use directly.
pub const ACCUMULATOR_STAGE_SENTINEL: u32 = u32::MAX;

/// Sentinel `stage_idx` used to store a runtime's opaque internal-state blob.
///
/// Distinct from [`ACCUMULATOR_STAGE_SENTINEL`] (`u32::MAX`) and far above any real
/// stage index, which number from 0 and stay in the dozens. The payload is whatever
/// [`PipelineRuntime::run_on_with_state`](saci_core::runtime::PipelineRuntime::run_on_with_state)
/// returned; the host never interprets it.
pub const PROCESSOR_STATE_STAGE_SENTINEL: u32 = u32::MAX - 1;

/// A persisted intermediate snapshot of pipeline state for one claim stage.
///
/// The `payload` field holds Arrow IPC bytes serialised from the
/// [`Dataset`](crate::Dataset) at the checkpoint boundary.
/// It is empty (`vec![]`) when the checkpoint strategy is
/// [`CheckpointStrategy::None`](crate::distributed::strategy::CheckpointStrategy::None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// Stable identifier for the master batch this checkpoint belongs to.
    pub batch_id: u64,
    /// Dataset stage index after which this checkpoint was taken.
    pub stage_idx: u32,
    /// Arrow IPC bytes for the intermediate pipeline state; empty if no snapshot.
    pub payload: Vec<u8>,
    /// Schema version of the Arrow data in `payload`.
    pub schema_id: u32,
    /// Unix milliseconds when this checkpoint was created.
    pub created_at: u64,
}

/// Persistent storage for Arrow-IPC pipeline checkpoints.
///
/// In multi-node mode each mutation is committed through Raft before
/// returning. In single-node mode mutations are applied directly to the local
/// database.
///
/// Reads always go to the local replica. Eventual consistency is acceptable
/// for checkpoint data: the worst case is re-processing one stage.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Save a checkpoint for `claim_id` at `stage_idx`.
    ///
    /// `ipc_bytes` must be less than
    /// [`MAX_LOG_ENTRY_BYTES`](crate::distributed::partition::MAX_LOG_ENTRY_BYTES).
    /// The caller is responsible for splitting across multiple checkpoints if
    /// the pipeline state is larger.
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> SaciResult<()>;

    /// Load the latest checkpoint for `claim_id` at `stage_idx`, if any.
    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<Checkpoint>>;

    /// Largest `ipc_bytes` this store accepts per checkpoint.
    ///
    /// The trait default is [`MAX_LOG_ENTRY_BYTES`](crate::distributed::partition::MAX_LOG_ENTRY_BYTES),
    /// which is what a checkpoint travelling inside a raft log entry allows.
    /// A store with a larger envelope may raise it.
    /// Pre-check helpers (`save_accumulator_state`, `save_processor_state`)
    /// consult this instead of hard-coding the cap, so a store with a larger
    /// envelope does not needlessly reject state.
    fn max_checkpoint_bytes(&self) -> usize {
        crate::distributed::partition::MAX_LOG_ENTRY_BYTES
    }

    /// Schema version recorded by any persisted data-stage checkpoint, or
    /// `Ok(None)` when the store holds no data checkpoints yet.
    ///
    /// Used at startup to refuse a redeployed pipeline whose schema
    /// fingerprint does not match the state it would resume against.
    async fn persisted_schema_id(&self) -> SaciResult<Option<u32>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_checkpoint_fields() {
        let cp = Checkpoint {
            batch_id: 42,
            stage_idx: 3,
            payload: vec![0xAA, 0xBB],
            schema_id: 1,
            created_at: 1_700_000_000_000,
        };
        assert_eq!(cp.batch_id, 42);
        assert_eq!(cp.stage_idx, 3);
        assert_eq!(cp.payload, vec![0xAA, 0xBB]);
        assert_eq!(cp.schema_id, 1);
        assert_eq!(cp.created_at, 1_700_000_000_000);
    }

    #[test]
    fn test_arrow_checkpoint_empty_payload() {
        let cp = Checkpoint {
            batch_id: 1,
            stage_idx: 0,
            payload: vec![],
            schema_id: 0,
            created_at: 0,
        };
        assert!(cp.payload.is_empty());
    }

    #[test]
    fn test_arrow_checkpoint_clone_eq() {
        let cp = Checkpoint {
            batch_id: 7,
            stage_idx: 2,
            payload: vec![1, 2, 3],
            schema_id: 5,
            created_at: 999,
        };
        assert_eq!(cp.clone(), cp);
    }
}
