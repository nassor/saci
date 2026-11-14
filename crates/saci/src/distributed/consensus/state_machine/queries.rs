//! Read-only queries over the state machine tables.
//!
//! Point reads for the stored records plus the scans that pick the next
//! claimable row range. Callers outside the state machine use these instead of
//! opening redb tables themselves.

use redb::{Database, ReadableDatabase, ReadableTable};

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::consensus::types::ClaimStatus;

use super::keys::{
    batch_range_end, batch_range_start, checkpoint_key, dec, decode_claims_by_batch_value,
};
use super::records::{
    CHECKPOINTS, CLAIMS, CLAIMS_BY_BATCH, CheckpointRecord, ClaimRecord, MASTER_BATCHES,
    MasterBatchRecord, PENDING_BATCHES,
};

/// Read a [`MasterBatchRecord`] by `batch_id`. Returns `Ok(None)` if absent.
pub fn read_master_batch(db: &Database, batch_id: u64) -> SaciResult<Option<MasterBatchRecord>> {
    let txn = db
        .begin_read()
        .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;
    let table = match txn.open_table(MASTER_BATCHES) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(SaciError::generic(format!("open master_batches: {e}"))),
    };
    match table
        .get(batch_id)
        .map_err(|e| SaciError::generic(format!("get master_batch: {e}")))?
    {
        None => Ok(None),
        Some(guard) => dec(guard.value()).map(Some),
    }
}

/// Read a [`ClaimRecord`] by `claim_id`. Returns `Ok(None)` if absent.
pub fn read_claim(db: &Database, claim_id: uuid::Uuid) -> SaciResult<Option<ClaimRecord>> {
    let txn = db
        .begin_read()
        .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;
    let table = match txn.open_table(CLAIMS) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(SaciError::generic(format!("open claims: {e}"))),
    };
    match table
        .get(claim_id.as_bytes().as_slice())
        .map_err(|e| SaciError::generic(format!("get claim: {e}")))?
    {
        None => Ok(None),
        Some(guard) => dec(guard.value()).map(Some),
    }
}

/// Read a [`CheckpointRecord`] by `(claim_id, stage_idx)`. Returns `Ok(None)` if absent.
pub fn read_checkpoint(
    db: &Database,
    claim_id: uuid::Uuid,
    stage_idx: u32,
) -> SaciResult<Option<CheckpointRecord>> {
    let txn = db
        .begin_read()
        .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;
    let table = match txn.open_table(CHECKPOINTS) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(SaciError::generic(format!("open checkpoints: {e}"))),
    };
    let key = checkpoint_key(claim_id.as_bytes(), stage_idx);
    match table
        .get(key.as_slice())
        .map_err(|e| SaciError::generic(format!("get checkpoint: {e}")))?
    {
        None => Ok(None),
        Some(guard) => dec(guard.value()).map(Some),
    }
}

/// Return the `schema_id` of any persisted *data* checkpoint, or `Ok(None)` when
/// none exists.
///
/// Data checkpoints record `Dataset::schemas().fingerprint()` as their `schema_id`,
/// so this is the fingerprint the node's persisted state belongs to. Reserved
/// sentinel stages are skipped: their `schema_id` slot carries a component schema
/// *version*, not a fingerprint.
///
/// The scan stops at the first match. Every data checkpoint on a node shares one
/// fingerprint, because a fingerprint change means a redeployed pipeline.
pub fn find_data_checkpoint_schema_id(db: &Database) -> SaciResult<Option<u32>> {
    use crate::distributed::checkpoint::PROCESSOR_STATE_STAGE_SENTINEL;

    let txn = db
        .begin_read()
        .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;
    let table = match txn.open_table(CHECKPOINTS) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(SaciError::generic(format!("open checkpoints: {e}"))),
    };
    let iter = table
        .iter()
        .map_err(|e| SaciError::generic(format!("iter checkpoints: {e}")))?;
    for item in iter {
        let (_k, v) = item.map_err(|e| SaciError::generic(format!("checkpoint item: {e}")))?;
        let record: CheckpointRecord = dec(v.value())?;
        if record.stage_idx < PROCESSOR_STATE_STAGE_SENTINEL {
            return Ok(Some(record.schema_id));
        }
    }
    Ok(None)
}

/// Find the first pending (unclaimed) row range for `batch_id`.
///
/// Returns `Ok(None)` when no pending range exists: the batch is missing, empty, or
/// every row is already covered by a Claimed or Completed claim. Completed claims are
/// occupied and never re-issuable, so they share the exclusion set with Claimed.
///
/// Uses the `arrow_claims_by_batch` secondary index for an O(k) scan where k
/// is the number of claims for this batch, rather than O(total_claims).
pub fn find_first_pending_claim(db: &Database, batch_id: u64) -> SaciResult<Option<(u32, u32)>> {
    let batch = match read_master_batch(db, batch_id)? {
        None => return Ok(None),
        Some(b) => b,
    };

    // Collect occupied ranges using the secondary index (O(k) for this batch).
    let txn = db
        .begin_read()
        .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;

    let mut claimed: Vec<(u32, u32)> = Vec::new();

    let idx_open = txn.open_table(CLAIMS_BY_BATCH);
    match idx_open {
        Ok(idx_table) => {
            let lo = batch_range_start(batch_id);
            let range_iter = match batch_range_end(batch_id) {
                Some(hi) => idx_table.range(lo.as_slice()..hi.as_slice()),
                None => idx_table.range(lo.as_slice()..),
            }
            .map_err(|e| SaciError::generic(format!("claims_by_batch range: {e}")))?;

            for item in range_iter {
                let (_k, v) =
                    item.map_err(|e| SaciError::generic(format!("claims_by_batch item: {e}")))?;
                let (start, end, status) = decode_claims_by_batch_value(v.value())
                    .ok_or_else(|| SaciError::generic("claims_by_batch: malformed value"))?;
                if matches!(status, ClaimStatus::Claimed | ClaimStatus::Completed) {
                    claimed.push((start, end));
                }
            }
        }
        // Secondary index table absent, so no claims exist and the whole batch is free.
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(SaciError::generic(format!("open claims_by_batch: {e}"))),
    }

    if claimed.is_empty() {
        if batch.total_rows > 0 {
            return Ok(Some((0, batch.total_rows)));
        }
        return Ok(None);
    }

    // Find the first row not covered by any occupied range.
    claimed.sort_by_key(|&(s, _)| s);
    let mut cursor: u32 = 0;
    for (start, end) in &claimed {
        if cursor < *start {
            return Ok(Some((cursor, *start)));
        }
        if *end > cursor {
            cursor = *end;
        }
    }
    if cursor < batch.total_rows {
        return Ok(Some((cursor, batch.total_rows)));
    }

    Ok(None)
}

/// Count `Completed` claims in `db` via the `arrow_claims_by_batch` secondary index.
/// Returns 0 if the table or any read fails; callers are tests that treat errors as
/// "not yet populated".
pub fn count_completed_claims(db: &Database) -> usize {
    use redb::ReadableTable as _;
    let Ok(txn) = db.begin_read() else {
        return 0;
    };
    let Ok(table) = txn.open_table(CLAIMS_BY_BATCH) else {
        return 0;
    };
    let Ok(iter) = table.iter() else {
        return 0;
    };
    iter.filter_map(|item| item.ok())
        .filter(|(_, v)| {
            decode_claims_by_batch_value(v.value())
                .is_some_and(|(_, _, s)| matches!(s, ClaimStatus::Completed))
        })
        .count()
}

/// Find the first batch_id that still has pending work, using the `PENDING_BATCHES`
/// secondary index for O(k) lookup where k is the number of pending batches.
/// Collects candidates under a single read transaction, then queries each with
/// [`find_first_pending_claim`]. Stale index entries (batch fully consumed but
/// index not yet cleaned up) are silently skipped.
pub fn find_first_pending_batch(db: &Database) -> SaciResult<Option<(u64, (u32, u32))>> {
    let candidates: Vec<u64> = {
        let txn = db
            .begin_read()
            .map_err(|e| SaciError::generic(format!("redb read txn (pending_batch): {e}")))?;
        match txn.open_table(PENDING_BATCHES) {
            Ok(table) => {
                let mut ids = Vec::new();
                for item in table
                    .iter()
                    .map_err(|e| SaciError::generic(format!("iter pending_batches: {e}")))?
                {
                    let (k, _) =
                        item.map_err(|e| SaciError::generic(format!("pending_batches item: {e}")))?;
                    ids.push(k.value());
                }
                ids
            }
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(SaciError::generic(format!("open pending_batches: {e}"))),
        }
    };
    for batch_id in candidates {
        if let Some(range) = find_first_pending_claim(db, batch_id)? {
            return Ok(Some((batch_id, range)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::state_machine::apply;
    use crate::distributed::consensus::state_machine::tests::{small_ipc, temp_db};
    use crate::distributed::consensus::types::{ConsensusCommand, ConsensusResponse};

    /// `find_first_pending_claim` must treat Completed as occupied, and
    /// `apply_claim_row_range` must reject overlaps with Completed ranges.
    #[test]
    fn test_find_first_pending_claim_skips_completed_ranges() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let claim_id = uuid::Uuid::now_v7();
        let inst = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Ack the claim so its status becomes Completed.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Completed);

        // No pending rows should be reported.
        let pending = find_first_pending_claim(&db, 1).unwrap();
        assert!(
            pending.is_none(),
            "Completed claim must not be reported as pending; got {pending:?}"
        );

        // A fresh claim on the same range must be rejected as overlapping.
        let c2 = uuid::Uuid::now_v7();
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id: c2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::Error { message } => {
                assert!(
                    message.contains("overlaps"),
                    "expected overlap error, got {message}"
                );
            }
            other => panic!("expected Error variant, got {other:?}"),
        }
    }

    /// Seeds N batches, completes N-1, asserts find_first_pending_batch
    /// visits only the one remaining pending batch.
    #[test]
    fn test_pending_batches_index_skips_completed() {
        const N: u64 = 10;
        let (db, _path) = temp_db();
        let inst = uuid::Uuid::now_v7();
        let mut claim_ids = Vec::new();

        // Register N batches and claim+ack N-1.
        for batch_id in 0..N {
            apply(
                &db,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id,
                    component: format!("comp_{batch_id}"),
                    schema_id: 1,
                    ipc_bytes: small_ipc(),
                    total_rows: 10,
                    now_at_propose: 0,
                },
            )
            .unwrap();

            let claim_id = uuid::Uuid::now_v7();
            claim_ids.push((batch_id, claim_id));
            apply(
                &db,
                ConsensusCommand::ClaimRowRange {
                    batch_id,
                    row_range_start: 0,
                    row_range_end: 10,
                    claim_id,
                    instance_id: inst,
                    lease_ttl_millis: 90_000,
                    now_at_propose: 1_000,
                },
            )
            .unwrap();
        }

        // Ack all except batch 7 (keep that one pending by releasing it).
        for (batch_id, claim_id) in &claim_ids {
            if *batch_id == 7 {
                apply(
                    &db,
                    ConsensusCommand::ReleaseClaim {
                        claim_id: *claim_id,
                        instance_id: inst,
                    },
                )
                .unwrap();
            } else {
                apply(
                    &db,
                    ConsensusCommand::AckClaim {
                        claim_id: *claim_id,
                        instance_id: inst,
                    },
                )
                .unwrap();
            }
        }

        // find_first_pending_batch must find batch 7.
        let result = find_first_pending_batch(&db).unwrap();
        assert!(result.is_some(), "must find the pending batch");
        let (found_batch_id, _range) = result.unwrap();
        assert_eq!(
            found_batch_id, 7,
            "must find batch 7, got batch {found_batch_id}"
        );
    }
}
