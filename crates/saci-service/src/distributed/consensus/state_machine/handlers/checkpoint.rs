//! Checkpoint write handler.
//!
//! Writes a stage checkpoint and bumps the owning master batch's
//! `checkpoint_seq` in one transaction, with content-hash idempotency so a
//! replayed log entry does not double-count.

use redb::{Database, ReadableDatabase, ReadableTable};

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::consensus::types::ConsensusResponse;
use crate::distributed::partition::MAX_LOG_ENTRY_BYTES;

use super::super::keys::{checkpoint_content_hash, checkpoint_key, dec, enc};
use super::super::records::{
    CHECKPOINTS, CLAIMS, CheckpointRecord, ClaimRecord, MASTER_BATCHES, MasterBatchRecord,
};

pub(crate) fn apply_checkpoint(
    db: &Database,
    claim_id: uuid::Uuid,
    stage_idx: u32,
    ipc_bytes: Vec<u8>,
    schema_id: u32,
    now_at_propose: u64,
) -> SaciResult<ConsensusResponse> {
    // Enforce 1 MiB cap.
    if ipc_bytes.len() >= MAX_LOG_ENTRY_BYTES {
        return Ok(ConsensusResponse::Error {
            message: format!(
                "Checkpoint ipc_bytes ({} bytes) exceeds MAX_LOG_ENTRY_BYTES ({})",
                ipc_bytes.len(),
                MAX_LOG_ENTRY_BYTES
            ),
        });
    }

    // Look up the batch_id from the claim for the checkpoint record.
    let batch_id = {
        let txn = db
            .begin_read()
            .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        let table = match txn.open_table(CLAIMS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(ConsensusResponse::Error {
                    message: format!("claim_id {claim_id} not found (table missing)"),
                });
            }
            Err(e) => return Err(SaciError::generic(format!("open claims: {e}"))),
        };
        match table
            .get(key)
            .map_err(|e| SaciError::generic(format!("get claim: {e}")))?
        {
            None => {
                return Ok(ConsensusResponse::Error {
                    message: format!("claim_id {claim_id} not found"),
                });
            }
            Some(guard) => {
                let record: ClaimRecord = dec(guard.value())?;
                record.batch_id
            }
        }
    };

    let content_hash = checkpoint_content_hash(claim_id.as_bytes(), stage_idx, &ipc_bytes);
    let record = CheckpointRecord {
        batch_id,
        stage_idx,
        ipc_bytes,
        schema_id,
        created_at: now_at_propose,
        content_hash,
    };
    let bytes = enc(&record)?;
    let key = checkpoint_key(claim_id.as_bytes(), stage_idx);

    // Write the checkpoint and increment the master batch's checkpoint_seq atomically.
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;
    let checkpoint_id = {
        // Idempotency: an existing (claim_id, stage_idx) with the same content_hash
        // returns success without re-incrementing checkpoint_seq. Keying on
        // content_hash catches a retry that carries a fresh now_at_propose.
        let already_exists = {
            let cp_table = txn
                .open_table(CHECKPOINTS)
                .map_err(|e| SaciError::generic(format!("open checkpoints (idempotency): {e}")))?;
            let raw = cp_table
                .get(key.as_slice())
                .map_err(|e| SaciError::generic(format!("get checkpoint (idempotency): {e}")))?
                .map(|g| g.value().to_vec());
            if let Some(existing_bytes) = raw {
                let existing: CheckpointRecord = dec(&existing_bytes)?;
                existing.content_hash == content_hash
            } else {
                false
            }
        };

        if already_exists {
            // Return the current checkpoint_seq without incrementing it.
            let batch_table = txn.open_table(MASTER_BATCHES).map_err(|e| {
                SaciError::generic(format!("open master_batches (idempotency): {e}"))
            })?;
            let raw = batch_table
                .get(batch_id)
                .map_err(|e| SaciError::generic(format!("get master_batch (idempotency): {e}")))?
                .map(|g| g.value().to_vec());
            let seq = match raw {
                Some(b) => dec::<MasterBatchRecord>(&b)?.checkpoint_seq,
                None => 0,
            };
            return Ok(ConsensusResponse::CheckpointWritten { checkpoint_id: seq });
        }

        let seq = {
            let mut batch_table = txn
                .open_table(MASTER_BATCHES)
                .map_err(|e| SaciError::generic(format!("open master_batches: {e}")))?;
            // Read and drop the guard before inserting.
            let existing: Option<MasterBatchRecord> = {
                let raw = batch_table
                    .get(batch_id)
                    .map_err(|e| SaciError::generic(format!("get master_batch: {e}")))?
                    .map(|guard| guard.value().to_vec());
                raw.map(|bytes| dec(&bytes)).transpose()?
            };
            match existing {
                None => 0u64,
                Some(mut br) => {
                    br.checkpoint_seq += 1;
                    let seq = br.checkpoint_seq;
                    let updated = enc(&br)?;
                    batch_table
                        .insert(batch_id, updated.as_slice())
                        .map_err(|e| SaciError::generic(format!("update master_batch: {e}")))?;
                    seq
                }
            }
        };

        {
            let mut cp_table = txn
                .open_table(CHECKPOINTS)
                .map_err(|e| SaciError::generic(format!("open checkpoints: {e}")))?;
            cp_table
                .insert(key.as_slice(), bytes.as_slice())
                .map_err(|e| SaciError::generic(format!("insert checkpoint: {e}")))?;
        }

        seq
    };

    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;
    Ok(ConsensusResponse::CheckpointWritten { checkpoint_id })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::state_machine::tests::{small_ipc, temp_db};
    use crate::distributed::consensus::state_machine::{apply, read_checkpoint, read_master_batch};
    use crate::distributed::consensus::types::ConsensusCommand;

    #[test]
    fn test_checkpoint_written_and_read() {
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

        let resp = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 2,
                ipc_bytes: vec![0xCA, 0xFE],
                schema_id: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::CheckpointWritten { .. }),
            "{resp:?}"
        );

        let cp = read_checkpoint(&db, claim_id, 2).unwrap().unwrap();
        assert_eq!(cp.stage_idx, 2);
        assert_eq!(cp.ipc_bytes, vec![0xCA, 0xFE]);
    }

    #[test]
    fn test_checkpoint_oversize_rejected() {
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

        let big = vec![0u8; MAX_LOG_ENTRY_BYTES]; // at limit → rejected
        let resp = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: big,
                schema_id: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(matches!(resp, ConsensusResponse::Error { .. }), "{resp:?}");
    }

    #[test]
    fn checkpoint_replay_idempotent() {
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

        let cp_cmd = ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx: 0,
            ipc_bytes: vec![0xCA, 0xFE],
            schema_id: 1,
            now_at_propose: 999, // deterministic identity key
        };

        let resp1 = apply(&db, cp_cmd.clone()).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq1,
        } = resp1
        else {
            panic!("expected CheckpointWritten, got {resp1:?}");
        };

        // Replay must not double-increment checkpoint_seq.
        let resp2 = apply(&db, cp_cmd).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq2,
        } = resp2
        else {
            panic!("expected CheckpointWritten on replay, got {resp2:?}");
        };
        assert_eq!(
            seq2, seq1,
            "checkpoint_seq must not increment on idempotent replay"
        );

        let batch = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(batch.checkpoint_seq, seq1, "master batch seq must be seq1");
    }

    /// Applying the same checkpoint body twice (even with different
    /// `now_at_propose`) must increment `checkpoint_seq` exactly once.
    #[test]
    fn test_checkpoint_replay_idempotent_across_retries() {
        let (db, _path) = temp_db();
        let instance = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "comp".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: instance,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();

        let checkpoint_body = vec![0xCC; 128];

        // First apply: new checkpoint, seq should be 1.
        let resp1 = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: checkpoint_body.clone(),
                schema_id: 1,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        let seq1 = match resp1 {
            ConsensusResponse::CheckpointWritten { checkpoint_id } => checkpoint_id,
            other => panic!("expected CheckpointWritten, got {other:?}"),
        };
        assert_eq!(seq1, 1, "first checkpoint must set seq to 1");

        // Second apply: same body, different now_at_propose (retry scenario).
        // checkpoint_seq must NOT increment again.
        let resp2 = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: checkpoint_body.clone(),
                schema_id: 1,
                now_at_propose: 99_999, // fresh timestamp: must be ignored
            },
        )
        .unwrap();
        let seq2 = match resp2 {
            ConsensusResponse::CheckpointWritten { checkpoint_id } => checkpoint_id,
            other => panic!("expected CheckpointWritten on retry, got {other:?}"),
        };
        assert_eq!(
            seq2, seq1,
            "retry with fresh now_at_propose must not increment checkpoint_seq"
        );
        let batch = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(
            batch.checkpoint_seq, 1,
            "checkpoint_seq must be exactly 1 after idempotent retry"
        );
    }
}
