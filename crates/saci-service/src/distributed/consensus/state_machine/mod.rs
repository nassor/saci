//! Arrow-IPC-aware deterministic state machine for SACI distributed consensus.
//!
//! `apply(db, command)` is the single entry point. It opens one redb
//! `WriteTransaction`, performs all mutations, and commits, so every write is
//! fate-shared with a single fsync.
//!
//! ## Tables
//!
//! | Table | Key | Value |
//! |-------|-----|-------|
//! | `arrow_master_batches` | `batch_id: u64` | JSON-encoded [`MasterBatchRecord`] |
//! | `arrow_claims` | `claim_id: [u8; 16]` | JSON-encoded [`ClaimRecord`] |
//! | `arrow_claims_by_batch` | `(batch_id_be8 ++ claim_id_16): [u8; 24]` | `start_be4 ++ end_be4 ++ status_byte`: 9 bytes |
//! | `arrow_checkpoints` | `(claim_id_bytes ++ stage_u32_be): [u8; 20]` | JSON-encoded [`CheckpointRecord`] |
//! | `arrow_instances` | `instance_id: [u8; 16]` | JSON-encoded [`InstanceRecord`] |
//!
//! ## Secondary index for per-batch claim scans
//!
//! `arrow_claims_by_batch` is a secondary index kept in lockstep with
//! `arrow_claims`. Its key prefix-encodes `batch_id` big-endian, so a bounded range
//! scan returns just the claims of one batch in O(k) where k is claims in the batch,
//! not O(total_claims). The value carries the row range and status byte, so overlap
//! checks never touch the primary `arrow_claims` table.
//!
//! ## Two-step claim check (TOCTOU safety)
//!
//! `apply_claim_row_range` prechecks the target batch in a `ReadTransaction` and
//! returns early, without writing, when the batch is missing or the range overlaps
//! a `Claimed`/`Completed` entry. It then repeats the check inside the
//! `WriteTransaction` that performs the insert. redb serialises writers, so a claim
//! inserted between the two scans is already committed and visible to the second
//! scan under the write lock.
//!
//! ## Determinism invariant
//!
//! Apply handlers must not read wall-clock time, random numbers, or any other
//! ambient state. Time-dependent fields ride on the [`ConsensusCommand`] via
//! `now_at_propose`, populated by the leader at propose time. Two replicas applying
//! the same committed log entry therefore produce byte-identical database state.

use redb::Database;

use crate::SaciResult;

use super::types::{ConsensusCommand, ConsensusResponse};

mod handlers;
mod keys;
mod queries;
mod records;
mod snapshot_io;

use handlers::{
    apply_ack_claim, apply_checkpoint, apply_claim_row_range, apply_heartbeat, apply_poison_batch,
    apply_reclaim_expired, apply_register_master_batch, apply_release_claim, apply_renew_claim,
};

pub use queries::{
    count_completed_claims, find_data_checkpoint_schema_id, find_first_pending_batch,
    find_first_pending_claim, read_checkpoint, read_claim, read_master_batch,
};
pub use records::{BatchStatus, CheckpointRecord, ClaimRecord, InstanceRecord, MasterBatchRecord};
pub use snapshot_io::{DumpedState, dump_state, restore_state};

// Consumed by the openraft state-machine store, which only exists with Raft.
#[cfg(feature = "distributed-raft")]
pub(crate) use records::{KEY_SM_LAST_APPLIED, KEY_SM_LAST_MEMBERSHIP};

/// Apply a committed Raft log entry to the redb application tables.
///
/// This function is **deterministic**: the same sequence of commands applied
/// to the same initial state always produces the same final state.
///
/// Application-level conditions (batch not found, claim overlaps, etc.) are
/// returned as `Ok(ConsensusResponse::Error { .. })` rather than `Err(...)`.
/// Only I/O failures or serialization errors produce `Err(SaciError)`.
///
/// # Errors
///
/// Returns [`SaciError`](crate::SaciError) for I/O failures or serialisation errors.
pub fn apply(db: &Database, command: ConsensusCommand) -> SaciResult<ConsensusResponse> {
    match command {
        ConsensusCommand::RegisterMasterBatch {
            batch_id,
            component,
            schema_id,
            ipc_bytes,
            total_rows,
            now_at_propose,
        } => apply_register_master_batch(
            db,
            batch_id,
            component,
            schema_id,
            ipc_bytes,
            total_rows,
            now_at_propose,
        ),

        ConsensusCommand::ClaimRowRange {
            batch_id,
            row_range_start,
            row_range_end,
            claim_id,
            instance_id,
            lease_ttl_millis,
            now_at_propose,
        } => apply_claim_row_range(
            db,
            batch_id,
            row_range_start,
            row_range_end,
            claim_id,
            instance_id,
            lease_ttl_millis,
            now_at_propose,
        ),

        ConsensusCommand::RenewClaim {
            claim_id,
            instance_id,
            lease_ttl_millis,
            now_at_propose,
        } => apply_renew_claim(db, claim_id, instance_id, lease_ttl_millis, now_at_propose),

        ConsensusCommand::AckClaim {
            claim_id,
            instance_id,
        } => apply_ack_claim(db, claim_id, instance_id),

        ConsensusCommand::ReleaseClaim {
            claim_id,
            instance_id,
        } => apply_release_claim(db, claim_id, instance_id),

        ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx,
            ipc_bytes,
            schema_id,
            now_at_propose,
        } => apply_checkpoint(
            db,
            claim_id,
            stage_idx,
            ipc_bytes,
            schema_id,
            now_at_propose,
        ),

        ConsensusCommand::Heartbeat { instance_id, at } => apply_heartbeat(db, instance_id, at),

        ConsensusCommand::ReclaimExpired { now_at_propose } => {
            apply_reclaim_expired(db, now_at_propose)
        }

        ConsensusCommand::PoisonBatch {
            batch_id,
            now_at_propose,
        } => apply_poison_batch(db, batch_id, now_at_propose),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn temp_db() -> (Database, tempfile::TempPath) {
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        let path = file.into_temp_path();
        let db = Database::create(&path).expect("redb create");
        (db, path)
    }

    pub(super) fn small_ipc() -> Vec<u8> {
        vec![0xAB; 64]
    }

    pub(super) fn seed_claimed_guard(db: &Database) -> (uuid::Uuid, uuid::Uuid) {
        let batch_id = 99u64;
        let claim_id = uuid::Uuid::now_v7();
        let instance_id = uuid::Uuid::now_v7();
        apply(
            db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "guard_test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0xAB; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let resp = apply(
            db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 50,
                claim_id,
                instance_id,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::BatchClaimed { .. }),
            "seed_claimed_guard failed: {resp:?}"
        );
        (claim_id, instance_id)
    }

    /// Applies one fixed command sequence into two independent redb databases and
    /// asserts the resulting state dumps are byte-identical. A wall-clock read
    /// inside an apply handler would give divergent `created_at` /
    /// `lease_expires_at` fields and fail here.
    #[test]
    fn test_state_machine_apply_is_deterministic_across_replicas() {
        let (db_a, _path_a) = temp_db();
        let (db_b, _path_b) = temp_db();

        // Fixed UUIDs so both replicas apply byte-identical input.
        let claim1 = uuid::Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
        let claim2 = uuid::Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);
        let inst = uuid::Uuid::from_u128(0xAAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA);

        let cmds = vec![
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 10,
                component: "alpha".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0xA1; 32],
                total_rows: 300,
                now_at_propose: 1_700_000_000_000,
            },
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 11,
                component: "beta".to_string(),
                schema_id: 2,
                ipc_bytes: vec![0xB2; 48],
                total_rows: 200,
                now_at_propose: 1_700_000_000_100,
            },
            ConsensusCommand::ClaimRowRange {
                batch_id: 10,
                row_range_start: 0,
                row_range_end: 150,
                claim_id: claim1,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 1_700_000_000_200,
            },
            ConsensusCommand::ClaimRowRange {
                batch_id: 10,
                row_range_start: 150,
                row_range_end: 300,
                claim_id: claim2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 1_700_000_000_300,
            },
            ConsensusCommand::Checkpoint {
                claim_id: claim1,
                stage_idx: 0,
                ipc_bytes: vec![0xCA, 0xFE],
                schema_id: 1,
                now_at_propose: 1_700_000_000_400,
            },
            ConsensusCommand::Checkpoint {
                claim_id: claim1,
                stage_idx: 1,
                ipc_bytes: vec![0xDE, 0xAD],
                schema_id: 1,
                now_at_propose: 1_700_000_000_500,
            },
            ConsensusCommand::AckClaim {
                claim_id: claim1,
                instance_id: inst,
            },
            ConsensusCommand::RenewClaim {
                claim_id: claim2,
                instance_id: inst,
                lease_ttl_millis: 45_000,
                now_at_propose: 1_700_000_000_600,
            },
            ConsensusCommand::Heartbeat {
                instance_id: inst,
                at: 1_700_000_000_700,
            },
            ConsensusCommand::ReleaseClaim {
                claim_id: claim2,
                instance_id: inst,
            },
        ];

        // A wall-clock gap between the two runs exposes any latent SystemTime read.
        for cmd in &cmds {
            apply(&db_a, cmd.clone()).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
        for cmd in &cmds {
            apply(&db_b, cmd.clone()).unwrap();
        }

        let dump_a = dump_state(&db_a).unwrap();
        let dump_b = dump_state(&db_b).unwrap();

        // serde_json gives a byte comparison that surfaces field-level differences,
        // timestamps included.
        let json_a = serde_json::to_vec(&(
            &dump_a.0,
            &dump_a.1,
            &dump_a
                .2
                .iter()
                .map(|(k, v)| (k.to_vec(), v))
                .collect::<Vec<_>>(),
            &dump_a.3,
        ))
        .unwrap();
        let json_b = serde_json::to_vec(&(
            &dump_b.0,
            &dump_b.1,
            &dump_b
                .2
                .iter()
                .map(|(k, v)| (k.to_vec(), v))
                .collect::<Vec<_>>(),
            &dump_b.3,
        ))
        .unwrap();
        assert_eq!(
            json_a, json_b,
            "state machine apply must be deterministic across replicas"
        );
    }
}
