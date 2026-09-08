//! Snapshot dump and restore for the state machine tables.
//!
//! `dump_state` reads every table into owned records for Arrow IPC
//! serialization; `restore_state` replaces the current state with a dump and
//! rebuilds the `arrow_claims_by_batch` secondary index, optionally writing the
//! openraft watermarks in the same commit.

use redb::{Database, ReadableDatabase, ReadableTable};

use crate::SaciError;
use crate::SaciResult;

use super::keys::{claims_by_batch_key, claims_by_batch_value, dec, enc};
use super::records::{
    CHECKPOINTS, CLAIMS, CLAIMS_BY_BATCH, CheckpointRecord, ClaimRecord, INSTANCES, InstanceRecord,
    KEY_SM_LAST_APPLIED, KEY_SM_LAST_MEMBERSHIP, MASTER_BATCHES, MasterBatchRecord,
    PENDING_BATCHES, SM_META_TABLE,
};

/// Full snapshot of all state machine tables.
///
/// Returned by [`dump_state`]: `(master_batches, claims, checkpoints, instances)`.
pub type DumpedState = (
    Vec<MasterBatchRecord>,
    Vec<(uuid::Uuid, ClaimRecord)>,
    Vec<([u8; 20], CheckpointRecord)>,
    Vec<(uuid::Uuid, InstanceRecord)>,
);

/// Dump all tables from `db` into a snapshot representation for Arrow IPC
/// serialization. Returns `(master_batches, claims, checkpoints, instances)`.
pub fn dump_state(db: &Database) -> SaciResult<DumpedState> {
    let txn = db
        .begin_read()
        .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;

    let mut batches = Vec::new();
    if let Ok(table) = txn.open_table(MASTER_BATCHES) {
        for item in table
            .iter()
            .map_err(|e| SaciError::generic(format!("master_batches iter: {e}")))?
        {
            let (_k, v) =
                item.map_err(|e| SaciError::generic(format!("master_batches iter item: {e}")))?;
            batches.push(dec(v.value())?);
        }
    }

    let mut claims = Vec::new();
    if let Ok(table) = txn.open_table(CLAIMS) {
        for item in table
            .iter()
            .map_err(|e| SaciError::generic(format!("claims iter: {e}")))?
        {
            let (k, v) = item.map_err(|e| SaciError::generic(format!("claims iter item: {e}")))?;
            let id_bytes: [u8; 16] = k
                .value()
                .try_into()
                .map_err(|_| SaciError::generic("claim key is not 16 bytes"))?;
            let id = uuid::Uuid::from_bytes(id_bytes);
            claims.push((id, dec(v.value())?));
        }
    }

    let mut checkpoints = Vec::new();
    if let Ok(table) = txn.open_table(CHECKPOINTS) {
        for item in table
            .iter()
            .map_err(|e| SaciError::generic(format!("checkpoints iter: {e}")))?
        {
            let (k, v) =
                item.map_err(|e| SaciError::generic(format!("checkpoints iter item: {e}")))?;
            let key_bytes: [u8; 20] = k
                .value()
                .try_into()
                .map_err(|_| SaciError::generic("checkpoint key is not 20 bytes"))?;
            checkpoints.push((key_bytes, dec(v.value())?));
        }
    }

    let mut instances = Vec::new();
    if let Ok(table) = txn.open_table(INSTANCES) {
        for item in table
            .iter()
            .map_err(|e| SaciError::generic(format!("instances iter: {e}")))?
        {
            let (k, v) =
                item.map_err(|e| SaciError::generic(format!("instances iter item: {e}")))?;
            let id_bytes: [u8; 16] = k
                .value()
                .try_into()
                .map_err(|_| SaciError::generic("instance key is not 16 bytes"))?;
            let id = uuid::Uuid::from_bytes(id_bytes);
            instances.push((id, dec(v.value())?));
        }
    }

    Ok((batches, claims, checkpoints, instances))
}

/// Restore all tables from a dump produced by [`dump_state`].
///
/// Clears existing state, then installs the snapshot content. Optional watermarks
/// (`last_applied_bytes`, `last_membership_bytes`) are written in the same
/// transaction, so install and watermark are fate-shared in one commit and fsync.
///
/// Rebuilds the `arrow_claims_by_batch` secondary index from the claims.
pub fn restore_state(
    db: &Database,
    batches: Vec<MasterBatchRecord>,
    claims: Vec<(uuid::Uuid, ClaimRecord)>,
    checkpoints: Vec<([u8; 20], CheckpointRecord)>,
    instances: Vec<(uuid::Uuid, InstanceRecord)>,
    sm_meta: Option<(&[u8], &[u8])>, // (last_applied_bytes, last_membership_bytes)
) -> SaciResult<()> {
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;
    {
        // Drop existing tables so the snapshot replaces rather than merges with
        // current state. `open_table` below recreates them.
        macro_rules! drop_table {
            ($def:expr) => {
                match txn.delete_table($def) {
                    Ok(_) => {}
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(e) => {
                        return Err(SaciError::generic(format!(
                            "delete table {}: {e}",
                            <_ as redb::TableHandle>::name(&$def)
                        )));
                    }
                }
            };
        }
        drop_table!(MASTER_BATCHES);
        drop_table!(CLAIMS);
        drop_table!(CLAIMS_BY_BATCH);
        drop_table!(CHECKPOINTS);
        drop_table!(INSTANCES);
        drop_table!(PENDING_BATCHES);

        let mut batch_table = txn
            .open_table(MASTER_BATCHES)
            .map_err(|e| SaciError::generic(format!("open master_batches: {e}")))?;
        let mut claim_table = txn
            .open_table(CLAIMS)
            .map_err(|e| SaciError::generic(format!("open claims: {e}")))?;
        let mut idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| SaciError::generic(format!("open claims_by_batch: {e}")))?;
        let mut cp_table = txn
            .open_table(CHECKPOINTS)
            .map_err(|e| SaciError::generic(format!("open checkpoints: {e}")))?;
        let mut inst_table = txn
            .open_table(INSTANCES)
            .map_err(|e| SaciError::generic(format!("open instances: {e}")))?;
        let mut pending_table = txn
            .open_table(PENDING_BATCHES)
            .map_err(|e| SaciError::generic(format!("open pending_batches: {e}")))?;

        for record in batches {
            let bytes = enc(&record)?;
            batch_table
                .insert(record.batch_id, bytes.as_slice())
                .map_err(|e| SaciError::generic(format!("insert batch: {e}")))?;
            // All registered batches start as pending; acks will remove them.
            pending_table
                .insert(record.batch_id, [].as_slice())
                .map_err(|e| {
                    SaciError::generic(format!("insert pending_batches (restore): {e}"))
                })?;
        }

        // Stale pending_table entries are harmless: find_first_pending_batch
        // delegates to find_first_pending_claim which checks CLAIMS_BY_BATCH
        // authoritatively and skips fully-completed batches.
        for (id, record) in claims {
            let bytes = enc(&record)?;
            claim_table
                .insert(id.as_bytes().as_slice(), bytes.as_slice())
                .map_err(|e| SaciError::generic(format!("insert claim: {e}")))?;

            let id_bytes = *id.as_bytes();
            let sec_key = claims_by_batch_key(record.batch_id, &id_bytes);
            let sec_val =
                claims_by_batch_value(record.row_range_start, record.row_range_end, record.status);
            idx_table
                .insert(sec_key.as_slice(), sec_val.as_slice())
                .map_err(|e| SaciError::generic(format!("insert claims_by_batch: {e}")))?;
        }

        for (key, record) in checkpoints {
            let bytes = enc(&record)?;
            cp_table
                .insert(key.as_slice(), bytes.as_slice())
                .map_err(|e| SaciError::generic(format!("insert checkpoint: {e}")))?;
        }

        for (id, record) in instances {
            let bytes = enc(&record)?;
            inst_table
                .insert(id.as_bytes().as_slice(), bytes.as_slice())
                .map_err(|e| SaciError::generic(format!("insert instance: {e}")))?;
        }

        // Write sm_meta watermarks in the same transaction so install and
        // watermark commit atomically.
        if let Some((applied_bytes, membership_bytes)) = sm_meta {
            let mut meta_table = txn
                .open_table(SM_META_TABLE)
                .map_err(|e| SaciError::generic(format!("open sm_meta: {e}")))?;
            meta_table
                .insert(KEY_SM_LAST_APPLIED, applied_bytes)
                .map_err(|e| SaciError::generic(format!("write sm_last_applied: {e}")))?;
            meta_table
                .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes)
                .map_err(|e| SaciError::generic(format!("write sm_last_membership: {e}")))?;
        }
    }
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::state_machine::keys::decode_claims_by_batch_value;
    use crate::distributed::consensus::state_machine::tests::{small_ipc, temp_db};
    use crate::distributed::consensus::state_machine::{
        apply, read_checkpoint, read_claim, read_master_batch,
    };
    use crate::distributed::consensus::types::{ClaimStatus, ConsensusCommand, ConsensusResponse};

    #[test]
    fn test_snapshot_dump_restore_round_trip() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "orders".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 500,
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
        apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: vec![1, 2, 3],
                schema_id: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::Heartbeat {
                instance_id: inst,
                at: 12345,
            },
        )
        .unwrap();

        let (batches, claims, checkpoints, instances) = dump_state(&db).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(claims.len(), 1);
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(instances.len(), 1);

        let (db2, _path2) = temp_db();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        let batch = read_master_batch(&db2, 1).unwrap().unwrap();
        assert_eq!(batch.component, "orders");
        assert_eq!(batch.total_rows, 500);

        let claim = read_claim(&db2, claim_id).unwrap().unwrap();
        assert_eq!(claim.row_range_start, 0);
        assert_eq!(claim.row_range_end, 100);

        let cp = read_checkpoint(&db2, claim_id, 0).unwrap().unwrap();
        assert_eq!(cp.ipc_bytes, vec![1, 2, 3]);
    }

    /// Verify that `restore_state` rebuilds the secondary index so overlap checks
    /// work after a snapshot install, with no command replay.
    #[test]
    fn test_restore_state_rebuilds_secondary_index() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 200,
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
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();

        let (batches, claims, checkpoints, instances) = dump_state(&db).unwrap();
        let (db2, _path2) = temp_db();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        // The secondary index must be populated in db2.
        {
            let txn = db2.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim_id.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index must be rebuilt by restore_state");
            let (start, end, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!((start, end, status), (0, 100, ClaimStatus::Completed));
        }

        // Overlap check should reject re-claiming the same range even after restore.
        let c2 = uuid::Uuid::now_v7();
        let resp = apply(
            &db2,
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
                    "expected overlap error after restore, got {message}"
                );
            }
            other => panic!("expected Error after restore, got {other:?}"),
        }
    }

    #[test]
    fn install_snapshot_atomic_clear() {
        // db1 has {c1, c2} and {batch 1, 2}
        let (db1, _p1) = temp_db();
        for i in 1u64..=2 {
            apply(
                &db1,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id: i,
                    component: format!("comp_{i}"),
                    schema_id: 1,
                    ipc_bytes: small_ipc(),
                    total_rows: 100,
                    now_at_propose: 0,
                },
            )
            .unwrap();
        }
        let inst = uuid::Uuid::now_v7();
        let c1 = uuid::Uuid::now_v7();
        apply(
            &db1,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c1,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // db2 has {c4, batch 3}, state that the snapshot from db1 must replace.
        let (db2, _p2) = temp_db();
        apply(
            &db2,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 3,
                component: "old_comp".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 50,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let c4 = uuid::Uuid::now_v7();
        apply(
            &db2,
            ConsensusCommand::ClaimRowRange {
                batch_id: 3,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c4,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Dump db1 → restore into db2.
        let (batches, claims, checkpoints, instances) = dump_state(&db1).unwrap();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        // db2 must contain exactly {batch 1, 2} and claim c1, not batch 3 or c4.
        assert!(
            read_master_batch(&db2, 1).unwrap().is_some(),
            "batch 1 must be present"
        );
        assert!(
            read_master_batch(&db2, 2).unwrap().is_some(),
            "batch 2 must be present"
        );
        assert!(
            read_master_batch(&db2, 3).unwrap().is_none(),
            "old batch 3 must be purged by snapshot install"
        );
        assert!(
            read_claim(&db2, c1).unwrap().is_some(),
            "claim c1 must be present"
        );
        assert!(
            read_claim(&db2, c4).unwrap().is_none(),
            "old claim c4 must be purged by snapshot install"
        );
    }
}
