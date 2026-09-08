//! Table definitions and stored record types for the consensus state machine.
//!
//! Every redb table the state machine touches is declared here, together with
//! the JSON-encoded record structs those tables hold. Handlers, queries, and
//! snapshot I/O all address the tables through these constants so there is one
//! definition of each table name and each on-disk shape.

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

use crate::distributed::consensus::types::ClaimStatus;

pub(super) const MASTER_BATCHES: TableDefinition<u64, &[u8]> =
    TableDefinition::new("arrow_master_batches");
pub(super) const CLAIMS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("arrow_claims");
/// Secondary index: key = `batch_id_be8 ++ claim_id_16` (24 bytes),
/// value = `start_row_be4 ++ end_row_be4 ++ status_byte` (9 bytes).
///
/// Allows O(k) range scans for a single batch without touching `arrow_claims`.
pub(super) const CLAIMS_BY_BATCH: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("arrow_claims_by_batch");
pub(super) const CHECKPOINTS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("arrow_checkpoints");
pub(super) const INSTANCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("arrow_instances");
/// Secondary index: set of batch_ids that still have at least one pending claim.
///
/// Key = `batch_id: u64`, value = empty slice `&[]`.
/// Inserted by `apply_register_master_batch`; removed by `apply_ack_claim` when
/// all rows of the batch are covered by Completed claims. Enables O(k) scan
/// over pending batches instead of an O(N) sweep of all batch_ids.
pub(super) const PENDING_BATCHES: TableDefinition<u64, &[u8]> =
    TableDefinition::new("arrow_pending_batches");

/// SM metadata table. Defined here so `restore_state` can write watermarks
/// in the same transaction as the snapshot data, for one commit and one
/// fsync. The same table is declared in `storage/mod.rs` for the openraft
/// state machine.
pub(super) const SM_META_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("arrow_sm_meta");

/// SM metadata key for `last_applied`; must match the table in `storage/mod.rs`.
pub(crate) const KEY_SM_LAST_APPLIED: &str = "sm_last_applied";
/// SM metadata key for `last_membership`; must match the table in `storage/mod.rs`.
pub(crate) const KEY_SM_LAST_MEMBERSHIP: &str = "sm_last_membership";

/// Eligibility status of a master batch.
///
/// Records written without this field decode as `Active` via `#[serde(default)]` on
/// [`MasterBatchRecord::status`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchStatus {
    /// Batch is eligible for claiming. Default for newly registered batches and for
    /// records decoded without the field.
    #[default]
    Active,
    /// Batch has been permanently disqualified by a runner-side release-cap
    /// trip. `claim_next_batch` will never return this batch again. Operators
    /// can observe poisoned batches via `/status` and must re-register a new
    /// batch if they want to retry the same data.
    Poisoned,
}

/// Stored master batch record.
///
/// ## Schema evolution
///
/// New fields use `#[serde(default)]`. Records are encoded with `serde_json`, so a
/// field missing from an on-disk record decodes to its `Default` value, giving a
/// deterministic upgrade path on every node and every replay. Malformed JSON still
/// returns `Err` from `dec()` and halts the state machine, preserving the
/// halt-on-decode-failure invariant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterBatchRecord {
    pub batch_id: u64,
    pub component: String,
    pub schema_id: u32,
    /// Arrow IPC bytes for the full master RecordBatch.
    pub ipc_bytes: Vec<u8>,
    pub total_rows: u32,
    pub created_at: u64,
    /// Next checkpoint counter (incremented on each successful Checkpoint apply).
    pub checkpoint_seq: u64,
    /// Consecutive release-attempt counter.
    ///
    /// Incremented by `apply_release_claim` and `apply_reclaim_expired`,
    /// reset to 0 by `apply_ack_claim`. When a runner observes this value
    /// crossing `RunnerConfig::max_claim_releases`, it proposes a
    /// `PoisonBatch` command to disqualify the batch.
    #[serde(default)]
    pub release_attempts: u32,
    /// Batch eligibility status. Defaults to `Active` when absent on disk.
    #[serde(default)]
    pub status: BatchStatus,
    /// Unix epoch milliseconds at which the batch was poisoned, or `None`
    /// while the batch is `Active`. Set by `apply_poison_batch` and never
    /// mutated again (first-writer wins on races).
    #[serde(default)]
    pub poisoned_at: Option<u64>,
}

/// Stored claim record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub batch_id: u64,
    pub row_range_start: u32,
    pub row_range_end: u32,
    pub instance_id: [u8; 16],
    pub lease_expires_at: u64,
    pub status: ClaimStatus,
}

/// Stored checkpoint record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub batch_id: u64,
    pub stage_idx: u32,
    pub ipc_bytes: Vec<u8>,
    pub schema_id: u32,
    pub created_at: u64,
    /// FNV-1a hash of `claim_id_bytes || stage_idx_be4 || ipc_bytes`. Two
    /// checkpoint applies for the same (claim_id, stage_idx) with identical
    /// body produce the same hash, so a retry with a fresh `now_at_propose`
    /// is still detected as a duplicate.
    pub content_hash: u64,
}

/// Per-instance heartbeat record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceRecord {
    pub last_heartbeat_at: u64,
}

#[cfg(test)]
mod tests {
    use super::super::keys::dec;
    use super::*;

    /// Records written without the `release_attempts` / `status` / `poisoned_at`
    /// fields must decode cleanly via `#[serde(default)]`.
    #[test]
    fn test_master_batch_record_decodes_legacy_json() {
        // Record JSON missing release_attempts/status/poisoned_at.
        let legacy = br#"{
            "batch_id": 1,
            "component": "legacy",
            "schema_id": 1,
            "ipc_bytes": [1,2,3],
            "total_rows": 10,
            "created_at": 0,
            "checkpoint_seq": 0
        }"#;
        let record: MasterBatchRecord = dec(legacy).unwrap();
        assert_eq!(record.batch_id, 1);
        assert_eq!(record.component, "legacy");
        assert_eq!(
            record.release_attempts, 0,
            "missing field must default to 0"
        );
        assert_eq!(
            record.status,
            BatchStatus::Active,
            "missing field must default to Active"
        );
        assert_eq!(
            record.poisoned_at, None,
            "missing field must default to None"
        );
    }
}
