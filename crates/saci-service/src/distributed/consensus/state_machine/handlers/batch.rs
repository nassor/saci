//! Master-batch lifecycle handlers: registration, poisoning, and the
//! `release_attempts` counter that feeds the claim-level retry cap.

use redb::{Database, ReadableTable};

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::consensus::types::ConsensusResponse;
use crate::distributed::partition::MAX_LOG_ENTRY_BYTES;

use super::super::keys::{dec, enc};
use super::super::records::{BatchStatus, MASTER_BATCHES, MasterBatchRecord, PENDING_BATCHES};

pub(crate) fn apply_register_master_batch(
    db: &Database,
    batch_id: u64,
    component: String,
    schema_id: u32,
    ipc_bytes: Vec<u8>,
    total_rows: u32,
    now_at_propose: u64,
) -> SaciResult<ConsensusResponse> {
    // Enforce the 1 MiB hard cap.
    if ipc_bytes.len() >= MAX_LOG_ENTRY_BYTES {
        return Ok(ConsensusResponse::Error {
            message: format!(
                "RegisterMasterBatch: ipc_bytes ({} bytes) exceeds MAX_LOG_ENTRY_BYTES ({})",
                ipc_bytes.len(),
                MAX_LOG_ENTRY_BYTES
            ),
        });
    }

    let record = MasterBatchRecord {
        batch_id,
        component,
        schema_id,
        ipc_bytes,
        total_rows,
        created_at: now_at_propose,
        checkpoint_seq: 0,
        release_attempts: 0,
        status: BatchStatus::Active,
        poisoned_at: None,
    };
    let bytes = enc(&record)?;
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;
    {
        let mut table = txn
            .open_table(MASTER_BATCHES)
            .map_err(|e| SaciError::generic(format!("open master_batches: {e}")))?;
        table
            .insert(batch_id, bytes.as_slice())
            .map_err(|e| SaciError::generic(format!("insert master_batch: {e}")))?;

        // Mark as pending in the secondary index so scan_pending is O(k).
        let mut pending_table = txn
            .open_table(PENDING_BATCHES)
            .map_err(|e| SaciError::generic(format!("open pending_batches: {e}")))?;
        pending_table
            .insert(batch_id, [].as_slice())
            .map_err(|e| SaciError::generic(format!("insert pending_batches: {e}")))?;
    }
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;
    Ok(ConsensusResponse::MasterBatchRegistered { batch_id })
}

/// Permanently disqualify a master batch.
///
/// Idempotent: a batch already at `BatchStatus::Poisoned` returns `Ok` without
/// mutating, so the first-writer `poisoned_at` timestamp survives cross-node poison
/// races. Raft serialises the competing proposals, the first wins, the second is a
/// no-op.
///
/// First application sets `status = Poisoned`, stamps `poisoned_at = now_at_propose`,
/// writes the master batch back, and removes it from the `PENDING_BATCHES` secondary
/// index so `find_first_pending_batch` never returns it again. Existing claim records
/// stay in place for `/status` audit and clear when their lease expires or the
/// operator deletes the batch.
pub(crate) fn apply_poison_batch(
    db: &Database,
    batch_id: u64,
    now_at_propose: u64,
) -> SaciResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn (poison): {e}")))?;
    let response = {
        let mut table = txn
            .open_table(MASTER_BATCHES)
            .map_err(|e| SaciError::generic(format!("open master_batches (poison): {e}")))?;
        let existing: Option<MasterBatchRecord> = {
            let raw = table
                .get(batch_id)
                .map_err(|e| SaciError::generic(format!("get master_batch (poison): {e}")))?
                .map(|g| g.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("PoisonBatch: batch_id {batch_id} not found"),
            },
            Some(record) if record.status == BatchStatus::Poisoned => {
                // Idempotent no-op: preserve first-writer poisoned_at.
                ConsensusResponse::BatchPoisoned {
                    batch_id,
                    poisoned_at: record.poisoned_at.unwrap_or(now_at_propose),
                }
            }
            Some(mut record) => {
                record.status = BatchStatus::Poisoned;
                record.poisoned_at = Some(now_at_propose);
                let bytes = enc(&record)?;
                table.insert(batch_id, bytes.as_slice()).map_err(|e| {
                    SaciError::generic(format!("update master_batch (poison): {e}"))
                })?;
                // Drop the master_batches borrow before opening PENDING_BATCHES
                // in the same write txn.
                drop(table);
                let mut pending_table = txn.open_table(PENDING_BATCHES).map_err(|e| {
                    SaciError::generic(format!("open pending_batches (poison): {e}"))
                })?;
                pending_table.remove(batch_id).map_err(|e| {
                    SaciError::generic(format!("remove pending_batches (poison): {e}"))
                })?;
                ConsensusResponse::BatchPoisoned {
                    batch_id,
                    poisoned_at: now_at_propose,
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit (poison): {e}")))?;
    Ok(response)
}

/// Increment `MasterBatchRecord.release_attempts` in the given write transaction.
///
/// A missing master batch is a silent no-op: the caller has already validated the
/// claim, and an absent parent batch is an orphaned-claim bug that should not be
/// masked as a release error.
pub(super) fn increment_release_attempts(
    txn: &redb::WriteTransaction,
    batch_id: u64,
) -> SaciResult<()> {
    let mut table = txn
        .open_table(MASTER_BATCHES)
        .map_err(|e| SaciError::generic(format!("open master_batches (bump): {e}")))?;
    let existing: Option<MasterBatchRecord> = {
        let raw = table
            .get(batch_id)
            .map_err(|e| SaciError::generic(format!("get master_batch (bump): {e}")))?
            .map(|g| g.value().to_vec());
        raw.map(|bytes| dec(&bytes)).transpose()?
    };
    if let Some(mut record) = existing {
        record.release_attempts = record.release_attempts.saturating_add(1);
        let bytes = enc(&record)?;
        table
            .insert(batch_id, bytes.as_slice())
            .map_err(|e| SaciError::generic(format!("update master_batch (bump): {e}")))?;
    }
    Ok(())
}

/// Reset `MasterBatchRecord.release_attempts` to 0 once a claim completes, so later
/// failures on the same batch count consecutively rather than over the batch
/// lifetime. A missing batch is a silent no-op, as in [`increment_release_attempts`].
pub(super) fn reset_release_attempts(
    txn: &redb::WriteTransaction,
    batch_id: u64,
) -> SaciResult<()> {
    let mut table = txn
        .open_table(MASTER_BATCHES)
        .map_err(|e| SaciError::generic(format!("open master_batches (reset): {e}")))?;
    let existing: Option<MasterBatchRecord> = {
        let raw = table
            .get(batch_id)
            .map_err(|e| SaciError::generic(format!("get master_batch (reset): {e}")))?
            .map(|g| g.value().to_vec());
        raw.map(|bytes| dec(&bytes)).transpose()?
    };
    if let Some(mut record) = existing
        && record.release_attempts != 0
    {
        record.release_attempts = 0;
        let bytes = enc(&record)?;
        table
            .insert(batch_id, bytes.as_slice())
            .map_err(|e| SaciError::generic(format!("update master_batch (reset): {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::state_machine::tests::{
        seed_claimed_guard, small_ipc, temp_db,
    };
    use crate::distributed::consensus::state_machine::{
        apply, find_first_pending_batch, read_master_batch,
    };
    use crate::distributed::consensus::types::ConsensusCommand;

    #[test]
    fn test_register_master_batch_succeeds() {
        let (db, _path) = temp_db();
        let resp = apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "orders".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(
                resp,
                ConsensusResponse::MasterBatchRegistered { batch_id: 1 }
            ),
            "{resp:?}"
        );
        let record = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(record.batch_id, 1);
        assert_eq!(record.component, "orders");
        assert_eq!(record.total_rows, 100);
    }

    #[test]
    fn test_register_master_batch_oversize_rejected() {
        let (db, _path) = temp_db();
        let big_ipc = vec![0u8; MAX_LOG_ENTRY_BYTES]; // exactly at limit → rejected
        let resp = apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 2,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: big_ipc,
                total_rows: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error, got {resp:?}"
        );
    }

    /// `apply_release_claim` increments `release_attempts`.
    #[test]
    fn test_release_claim_increments_release_attempts() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);

        // Initial state: counter is zero.
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.release_attempts, 0);

        // Releasing the claim bumps the counter to 1.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.release_attempts, 1);
    }

    /// `apply_ack_claim` resets `release_attempts` to 0.
    #[test]
    fn test_ack_claim_resets_release_attempts() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);

        // Call the helper directly to build up attempts; simpler than driving
        // expired reclaims in-test.
        let w = db.begin_write().unwrap();
        increment_release_attempts(&w, 99).unwrap();
        increment_release_attempts(&w, 99).unwrap();
        increment_release_attempts(&w, 99).unwrap();
        w.commit().unwrap();
        assert_eq!(
            read_master_batch(&db, 99)
                .unwrap()
                .unwrap()
                .release_attempts,
            3
        );

        // Acking the original claim resets the counter.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.release_attempts, 0, "ack must reset counter");
    }

    /// `apply_reclaim_expired` increments `release_attempts`.
    #[test]
    fn test_reclaim_expired_increments_release_attempts() {
        let (db, _path) = temp_db();
        let (_claim_id, _instance_id) = seed_claimed_guard(&db);

        // The claim's `lease_expires_at` is `now_at_propose (1_000) + lease_ttl
        // (90_000)` = 91_000, so propose ReclaimExpired later to make the lease
        // visibly expired.
        apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200_000,
            },
        )
        .unwrap();
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(
            batch.release_attempts, 1,
            "reclaim_expired must bump release_attempts just like an explicit release"
        );
    }

    /// A late `ReleaseClaim` after `ReclaimExpired` must not bump the counter. The
    /// status check in `apply_release_claim` blocks it: the retry-cap wiring lives
    /// inside the success branch, so late deliveries hit the guard first.
    #[test]
    fn test_late_release_after_reclaim_does_not_double_count() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);

        // Reclaim the lease (bumps counter to 1).
        apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200_000,
            },
        )
        .unwrap();
        assert_eq!(
            read_master_batch(&db, 99)
                .unwrap()
                .unwrap()
                .release_attempts,
            1
        );

        // The late ReleaseClaim hits the "not in Claimed state" guard in
        // apply_release_claim and returns Error without mutating, so the counter
        // stays at 1.
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "late release against Pending claim must be rejected"
        );
        assert_eq!(
            read_master_batch(&db, 99)
                .unwrap()
                .unwrap()
                .release_attempts,
            1,
            "release_attempts must NOT double-count on late ReleaseClaim after ReclaimExpired"
        );
    }

    /// `apply_poison_batch` marks status, stamps `poisoned_at`, and removes the batch
    /// from `PENDING_BATCHES` so future `claim_next_batch` calls never see it.
    #[test]
    fn test_poison_batch_marks_status_and_removes_from_pending() {
        let (db, _path) = temp_db();
        let _ = seed_claimed_guard(&db);

        let resp = apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 99,
                now_at_propose: 42_000,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::BatchPoisoned {
                batch_id,
                poisoned_at,
            } => {
                assert_eq!(batch_id, 99);
                assert_eq!(poisoned_at, 42_000);
            }
            other => panic!("expected BatchPoisoned, got {other:?}"),
        }

        // Master batch record reflects the new state.
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.status, BatchStatus::Poisoned);
        assert_eq!(batch.poisoned_at, Some(42_000));

        // `find_first_pending_batch` skips the poisoned batch, and seed_claimed_guard
        // registered only batch 99, so the result is None.
        let result = find_first_pending_batch(&db).unwrap();
        assert!(
            result.is_none(),
            "poisoned batch must not be returned by find_first_pending_batch, got {result:?}"
        );
    }

    /// `PoisonBatch` is idempotent and preserves the first-writer `poisoned_at`
    /// timestamp. A concurrent second proposer must not overwrite it, or `/status`
    /// would report jittering "how long poisoned" values as Raft serialises races.
    #[test]
    fn test_poison_batch_idempotent_preserves_first_writer_timestamp() {
        let (db, _path) = temp_db();
        let _ = seed_claimed_guard(&db);

        // First poison at timestamp 100.
        apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 99,
                now_at_propose: 100,
            },
        )
        .unwrap();
        // Second poison at timestamp 200 must be a no-op.
        let resp = apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 99,
                now_at_propose: 200,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::BatchPoisoned {
                batch_id,
                poisoned_at,
            } => {
                assert_eq!(batch_id, 99);
                assert_eq!(
                    poisoned_at, 100,
                    "second PoisonBatch must return first-writer poisoned_at"
                );
            }
            other => panic!("expected BatchPoisoned (idempotent), got {other:?}"),
        }
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(
            batch.poisoned_at,
            Some(100),
            "first-writer timestamp must be preserved on double-poison"
        );
    }

    /// `PoisonBatch` against a non-existent batch returns Error.
    #[test]
    fn test_poison_batch_unknown_id_returns_error() {
        let (db, _path) = temp_db();
        let resp = apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 12345,
                now_at_propose: 100,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "PoisonBatch on unknown batch_id must return Error"
        );
    }

    /// N consecutive `ReleaseClaim`s accumulate `release_attempts` with no ack in
    /// between, so the counter resets only on an explicit ack. Each iteration needs a
    /// fresh claim because `ReleaseClaim` moves the claim to Pending.
    #[test]
    fn test_n_releases_accumulate_release_attempts() {
        let (db, _path) = temp_db();
        let batch_id = 77u64;
        let instance_id = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "nrel".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x01; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        for i in 0..5 {
            let claim_id = uuid::Uuid::now_v7();
            apply(
                &db,
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
            apply(
                &db,
                ConsensusCommand::ReleaseClaim {
                    claim_id,
                    instance_id,
                },
            )
            .unwrap();
            let batch = read_master_batch(&db, batch_id).unwrap().unwrap();
            assert_eq!(
                batch.release_attempts,
                (i + 1) as u32,
                "after {} releases counter must be {}",
                i + 1,
                i + 1
            );
        }
    }

    /// A claim acked after some failures leaves no stale counter: the next failure
    /// starts from 1, not N+1. Combining reset-on-ack with a later failure shows the
    /// reset is real and not just a zero-check.
    #[test]
    fn test_reset_on_ack_then_new_failure_starts_from_one() {
        let (db, _path) = temp_db();
        let batch_id = 88u64;
        let instance_id = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "cycle".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x02; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Three failures: claim → release × 3.
        for _ in 0..3 {
            let claim_id = uuid::Uuid::now_v7();
            apply(
                &db,
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
            apply(
                &db,
                ConsensusCommand::ReleaseClaim {
                    claim_id,
                    instance_id,
                },
            )
            .unwrap();
        }
        assert_eq!(
            read_master_batch(&db, batch_id)
                .unwrap()
                .unwrap()
                .release_attempts,
            3
        );

        // One success: claim → ack.
        let ok_claim = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: ok_claim,
                instance_id,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id: ok_claim,
                instance_id,
            },
        )
        .unwrap();
        assert_eq!(
            read_master_batch(&db, batch_id)
                .unwrap()
                .unwrap()
                .release_attempts,
            0,
            "ack resets counter"
        );

        // Row range [0,50) is marked Completed on the secondary index by the
        // successful ack, so the new-failure path needs a second batch with a fresh
        // row range.
        let fresh_batch = 89u64;
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: fresh_batch,
                component: "cycle2".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x03; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: fresh_batch,
                row_range_start: 0,
                row_range_end: 50,
                claim_id,
                instance_id,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert_eq!(
            read_master_batch(&db, fresh_batch)
                .unwrap()
                .unwrap()
                .release_attempts,
            1,
            "new batch starts from zero and counts its first failure as 1"
        );
    }
}
