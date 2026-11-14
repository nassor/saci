//! Claim lifecycle handlers: acquire, renew, ack, release, reclaim, and the
//! per-instance heartbeat.
//!
//! These handlers own the `arrow_claims` table and keep the
//! `arrow_claims_by_batch` secondary index in lockstep with it, so the
//! overlap checks never have to sweep the primary table.

use redb::{Database, ReadableDatabase, ReadableTable};

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::consensus::types::{ClaimStatus, ConsensusResponse};

use super::super::keys::{
    batch_range_end, batch_range_start, claims_by_batch_key, claims_by_batch_value, dec,
    decode_claims_by_batch_value, enc,
};
use super::super::records::{
    CLAIMS, CLAIMS_BY_BATCH, ClaimRecord, INSTANCES, InstanceRecord, MASTER_BATCHES,
    MasterBatchRecord, PENDING_BATCHES,
};

use super::batch::{increment_release_attempts, reset_release_attempts};

/// Scan `arrow_claims_by_batch` for a specific `batch_id` and check whether
/// `[row_range_start, row_range_end)` overlaps any `Claimed` or `Completed`
/// entry.
///
/// Returns `Ok(true)` if an overlap is found. Works on any readable table type
/// that implements `ReadableTable<&[u8], &[u8]>`.
fn has_batch_overlap<T>(
    table: &T,
    batch_id: u64,
    row_range_start: u32,
    row_range_end: u32,
) -> SaciResult<bool>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let lo = batch_range_start(batch_id);
    // Inclusive start and exclusive end confine the scan to this batch's entries.
    // For batch_id == u64::MAX, scan to the end of the table.
    let range_iter = match batch_range_end(batch_id) {
        Some(hi) => table.range(lo.as_slice()..hi.as_slice()),
        None => table.range(lo.as_slice()..),
    }
    .map_err(|e| SaciError::generic(format!("claims_by_batch range: {e}")))?;

    for item in range_iter {
        let (_k, v) = item.map_err(|e| SaciError::generic(format!("claims_by_batch item: {e}")))?;
        let (start, end, status) = decode_claims_by_batch_value(v.value())
            .ok_or_else(|| SaciError::generic("claims_by_batch: malformed value (not 9 bytes)"))?;
        if matches!(status, ClaimStatus::Claimed | ClaimStatus::Completed) {
            // [rs, re) overlaps [a, b) iff rs < b && re > a
            if row_range_start < end && row_range_end > start {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)] // one-shot internal apply handler; all args come from a single ConsensusCommand::ClaimRowRange variant
pub(crate) fn apply_claim_row_range(
    db: &Database,
    batch_id: u64,
    row_range_start: u32,
    row_range_end: u32,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
    lease_ttl_millis: u64,
    now_at_propose: u64,
) -> SaciResult<ConsensusResponse> {
    // Range validity: pure arithmetic, no I/O.
    if row_range_start >= row_range_end {
        return Ok(ConsensusResponse::Error {
            message: format!("invalid row_range: start {row_range_start} >= end {row_range_end}"),
        });
    }

    // Replay idempotency: a claim_id that was already applied (crash between apply
    // and watermark persist, then raft replays the entry) returns success instead of
    // the phase-1 overlap error.
    {
        let rtxn_idem = db
            .begin_read()
            .map_err(|e| SaciError::generic(format!("redb read txn (idempotency): {e}")))?;
        let claim_table_opt = match rtxn_idem.open_table(CLAIMS) {
            Ok(t) => Some(t),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(e) => {
                return Err(SaciError::generic(format!(
                    "open claims (idempotency): {e}"
                )));
            }
        };
        if let Some(claim_table) = claim_table_opt {
            let raw = claim_table
                .get(claim_id.as_bytes().as_slice())
                .map_err(|e| SaciError::generic(format!("get claim (idempotency): {e}")))?
                .map(|g| g.value().to_vec());
            if let Some(bytes) = raw {
                let existing: ClaimRecord = dec(&bytes)?;
                if existing.batch_id == batch_id
                    && existing.row_range_start == row_range_start
                    && existing.row_range_end == row_range_end
                    && existing.instance_id == *instance_id.as_bytes()
                {
                    // Read the batch fresh for the response; it may have changed.
                    let batch_for_response = match rtxn_idem.open_table(MASTER_BATCHES) {
                        Ok(t) => match t.get(batch_id).map_err(|e| {
                            SaciError::generic(format!("get batch (idempotency): {e}"))
                        })? {
                            Some(g) => dec::<MasterBatchRecord>(g.value())?,
                            None => {
                                return Ok(ConsensusResponse::Error {
                                    message: format!(
                                        "batch_id {batch_id} not found on idempotent replay"
                                    ),
                                });
                            }
                        },
                        Err(e) => {
                            return Err(SaciError::generic(format!(
                                "open batches (idempotency): {e}"
                            )));
                        }
                    };
                    return Ok(ConsensusResponse::BatchClaimed {
                        batch_id,
                        component: batch_for_response.component,
                        row_range_start,
                        row_range_end,
                        schema_id: batch_for_response.schema_id,
                        claim_id,
                        instance_id,
                        lease_expires_at: existing.lease_expires_at,
                    });
                }
            }
        }
    }

    // Read precheck: validate the proposed range against persisted state using only
    // a ReadTransaction. The common rejection path (batch missing, range occupied)
    // returns here without ever taking the write lock.

    let precheck_result: Result<Option<MasterBatchRecord>, ConsensusResponse> = {
        let rtxn = db
            .begin_read()
            .map_err(|e| SaciError::generic(format!("redb read txn: {e}")))?;

        let batch_opt: Option<MasterBatchRecord> = {
            let batch_table = match rtxn.open_table(MASTER_BATCHES) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(ConsensusResponse::Error {
                        message: format!("batch_id {batch_id} not found"),
                    });
                }
                Err(e) => return Err(SaciError::generic(format!("open master_batches: {e}"))),
            };
            match batch_table
                .get(batch_id)
                .map_err(|e| SaciError::generic(format!("get master_batch: {e}")))?
            {
                None => None,
                Some(guard) => Some(dec(guard.value())?),
            }
        };

        match &batch_opt {
            None => Err(ConsensusResponse::Error {
                message: format!("batch_id {batch_id} not found"),
            }),
            Some(batch) => {
                if row_range_end > batch.total_rows {
                    return Ok(ConsensusResponse::Error {
                        message: format!(
                            "row_range [{row_range_start}, {row_range_end}) exceeds total_rows {}",
                            batch.total_rows
                        ),
                    });
                }

                // Overlap check via the secondary index: O(k) for this batch.
                let overlap = match rtxn.open_table(CLAIMS_BY_BATCH) {
                    Ok(idx_table) => {
                        has_batch_overlap(&idx_table, batch_id, row_range_start, row_range_end)?
                    }
                    // Table absent, so no claims exist and nothing can overlap.
                    Err(redb::TableError::TableDoesNotExist(_)) => false,
                    Err(e) => {
                        return Err(SaciError::generic(format!("open claims_by_batch: {e}")));
                    }
                };

                if overlap {
                    Err(ConsensusResponse::Error {
                        message: format!(
                            "row_range [{row_range_start}, {row_range_end}) overlaps an existing active claim"
                        ),
                    })
                } else {
                    Ok(batch_opt)
                }
            }
        }
    };

    let batch = match precheck_result {
        Err(early_response) => return Ok(early_response),
        Ok(None) => {
            return Ok(ConsensusResponse::Error {
                message: format!("batch_id {batch_id} not found"),
            });
        }
        Ok(Some(b)) => b,
    };

    // Write confirmation: re-check overlaps under the write lock to close the TOCTOU
    // window before the insert. redb serialises writers, so nothing can slip in
    // between acquiring the lock and this scan.

    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;

    // open_table on a WriteTransaction creates the table when it is missing. A fresh
    // table is empty, so the range scan reports no overlap.
    let overlap_under_lock = {
        let idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| SaciError::generic(format!("open claims_by_batch (write): {e}")))?;
        has_batch_overlap(&idx_table, batch_id, row_range_start, row_range_end)?
    };

    if overlap_under_lock {
        // Dropping the write txn aborts it; nothing to persist.
        return Ok(ConsensusResponse::Error {
            message: format!(
                "row_range [{row_range_start}, {row_range_end}) overlaps an existing active claim"
            ),
        });
    }

    let lease_expires_at = now_at_propose + lease_ttl_millis;
    let record = ClaimRecord {
        batch_id,
        row_range_start,
        row_range_end,
        instance_id: *instance_id.as_bytes(),
        lease_expires_at,
        status: ClaimStatus::Claimed,
    };
    let bytes = enc(&record)?;
    let claim_id_bytes = *claim_id.as_bytes();
    let secondary_key = claims_by_batch_key(batch_id, &claim_id_bytes);
    let secondary_val = claims_by_batch_value(row_range_start, row_range_end, ClaimStatus::Claimed);

    {
        let mut claim_table = txn
            .open_table(CLAIMS)
            .map_err(|e| SaciError::generic(format!("open claims: {e}")))?;
        claim_table
            .insert(claim_id_bytes.as_slice(), bytes.as_slice())
            .map_err(|e| SaciError::generic(format!("insert claim: {e}")))?;

        let mut idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| SaciError::generic(format!("open claims_by_batch: {e}")))?;
        idx_table
            .insert(secondary_key.as_slice(), secondary_val.as_slice())
            .map_err(|e| SaciError::generic(format!("insert claims_by_batch: {e}")))?;
    }

    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;

    Ok(ConsensusResponse::BatchClaimed {
        batch_id,
        component: batch.component,
        row_range_start,
        row_range_end,
        schema_id: batch.schema_id,
        claim_id,
        instance_id,
        lease_expires_at,
    })
}

pub(crate) fn apply_renew_claim(
    db: &Database,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
    lease_ttl_millis: u64,
    now_at_propose: u64,
) -> SaciResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;
    let response = {
        let mut table = txn
            .open_table(CLAIMS)
            .map_err(|e| SaciError::generic(format!("open claims: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        // Read the record and drop the guard before mutating.
        let existing: Option<ClaimRecord> = {
            let raw = table
                .get(key)
                .map_err(|e| SaciError::generic(format!("get claim: {e}")))?
                .map(|guard| guard.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("claim {claim_id} not found"),
            },
            Some(mut record) => {
                if record.status != ClaimStatus::Claimed {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} is not in Claimed state"),
                    }
                } else if record.instance_id != *instance_id.as_bytes() {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} held by different instance"),
                    }
                } else if record.lease_expires_at < now_at_propose {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} lease has already expired"),
                    }
                } else {
                    let new_expires = now_at_propose + lease_ttl_millis;
                    // max() guarantees monotonicity against out-of-order proposals.
                    record.lease_expires_at = record.lease_expires_at.max(new_expires);
                    let expires_at = record.lease_expires_at;
                    let bytes = enc(&record)?;
                    table
                        .insert(key, bytes.as_slice())
                        .map_err(|e| SaciError::generic(format!("update claim: {e}")))?;
                    ConsensusResponse::ClaimRenewed { expires_at }
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;
    Ok(response)
}

pub(crate) fn apply_ack_claim(
    db: &Database,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
) -> SaciResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;
    let response = {
        let mut table = txn
            .open_table(CLAIMS)
            .map_err(|e| SaciError::generic(format!("open claims: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        // Read and drop the guard before inserting.
        let existing: Option<ClaimRecord> = {
            let raw = table
                .get(key)
                .map_err(|e| SaciError::generic(format!("get claim: {e}")))?
                .map(|guard| guard.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("claim {claim_id} not found"),
            },
            Some(mut record) => {
                if record.status != ClaimStatus::Claimed {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} not in Claimed state"),
                    }
                } else if record.instance_id != *instance_id.as_bytes() {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} held by different instance"),
                    }
                } else {
                    let batch_id = record.batch_id;
                    let row_range_start = record.row_range_start;
                    let row_range_end = record.row_range_end;
                    record.status = ClaimStatus::Completed;
                    record.lease_expires_at = 0;
                    let bytes = enc(&record)?;
                    table
                        .insert(key, bytes.as_slice())
                        .map_err(|e| SaciError::generic(format!("update claim: {e}")))?;

                    // Keep secondary index in sync.
                    let claim_id_bytes = *claim_id.as_bytes();
                    let sec_key = claims_by_batch_key(batch_id, &claim_id_bytes);
                    let sec_val = claims_by_batch_value(
                        row_range_start,
                        row_range_end,
                        ClaimStatus::Completed,
                    );
                    let mut idx_table = txn
                        .open_table(CLAIMS_BY_BATCH)
                        .map_err(|e| SaciError::generic(format!("open claims_by_batch: {e}")))?;
                    idx_table
                        .insert(sec_key.as_slice(), sec_val.as_slice())
                        .map_err(|e| SaciError::generic(format!("update claims_by_batch: {e}")))?;

                    // Remove from PENDING_BATCHES if all claims for this batch are now Completed.
                    let all_complete = {
                        let lo = batch_range_start(batch_id);
                        let range_iter = match batch_range_end(batch_id) {
                            Some(hi) => idx_table.range(lo.as_slice()..hi.as_slice()),
                            None => idx_table.range(lo.as_slice()..),
                        }
                        .map_err(|e| {
                            SaciError::generic(format!("claims_by_batch range (ack): {e}"))
                        })?;
                        let mut complete = true;
                        for item in range_iter {
                            let (_k, v) = item.map_err(|e| {
                                SaciError::generic(format!("claims_by_batch item (ack): {e}"))
                            })?;
                            let (_, _, status) = decode_claims_by_batch_value(v.value())
                                .ok_or_else(|| {
                                    SaciError::generic("claims_by_batch: malformed value (ack)")
                                })?;
                            if !matches!(status, ClaimStatus::Completed) {
                                complete = false;
                                break;
                            }
                        }
                        complete
                    };
                    if all_complete {
                        let mut pending_table = txn.open_table(PENDING_BATCHES).map_err(|e| {
                            SaciError::generic(format!("open pending_batches (ack): {e}"))
                        })?;
                        pending_table.remove(batch_id).map_err(|e| {
                            SaciError::generic(format!("remove pending_batches (ack): {e}"))
                        })?;
                    }

                    // A successful ack resets the consecutive-failure counter.
                    reset_release_attempts(&txn, batch_id)?;

                    ConsensusResponse::ClaimAcked
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;
    Ok(response)
}

pub(crate) fn apply_release_claim(
    db: &Database,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
) -> SaciResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;
    let response = {
        let mut table = txn
            .open_table(CLAIMS)
            .map_err(|e| SaciError::generic(format!("open claims: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        // Read and drop the guard before inserting.
        let existing: Option<ClaimRecord> = {
            let raw = table
                .get(key)
                .map_err(|e| SaciError::generic(format!("get claim: {e}")))?
                .map(|guard| guard.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("claim {claim_id} not found"),
            },
            Some(mut record) => {
                if record.status != ClaimStatus::Claimed {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} not in Claimed state"),
                    }
                } else if record.instance_id != *instance_id.as_bytes() {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} held by different instance"),
                    }
                } else {
                    let batch_id = record.batch_id;
                    let row_range_start = record.row_range_start;
                    let row_range_end = record.row_range_end;
                    record.status = ClaimStatus::Pending;
                    record.lease_expires_at = 0;
                    record.instance_id = [0u8; 16];
                    let bytes = enc(&record)?;
                    table
                        .insert(key, bytes.as_slice())
                        .map_err(|e| SaciError::generic(format!("update claim: {e}")))?;

                    // Keep secondary index in sync.
                    let claim_id_bytes = *claim_id.as_bytes();
                    let sec_key = claims_by_batch_key(batch_id, &claim_id_bytes);
                    let sec_val =
                        claims_by_batch_value(row_range_start, row_range_end, ClaimStatus::Pending);
                    // Drop the CLAIMS borrow before opening CLAIMS_BY_BATCH
                    // or MASTER_BATCHES in the same write txn.
                    drop(table);
                    let mut idx_table = txn
                        .open_table(CLAIMS_BY_BATCH)
                        .map_err(|e| SaciError::generic(format!("open claims_by_batch: {e}")))?;
                    idx_table
                        .insert(sec_key.as_slice(), sec_val.as_slice())
                        .map_err(|e| SaciError::generic(format!("update claims_by_batch: {e}")))?;
                    drop(idx_table);

                    // The release_attempts bump sits inside the Claimed→Pending
                    // success branch. A late ReleaseClaim against an already-Pending
                    // claim (beaten by ReclaimExpired) hits the status guard above
                    // and never reaches this increment, so it cannot double-count.
                    increment_release_attempts(&txn, batch_id)?;

                    ConsensusResponse::ClaimReleased
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;
    Ok(response)
}

pub(crate) fn apply_heartbeat(
    db: &Database,
    instance_id: uuid::Uuid,
    at: u64,
) -> SaciResult<ConsensusResponse> {
    let record = InstanceRecord {
        last_heartbeat_at: at,
    };
    let bytes = enc(&record)?;
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn: {e}")))?;
    {
        let mut table = txn
            .open_table(INSTANCES)
            .map_err(|e| SaciError::generic(format!("open instances: {e}")))?;
        table
            .insert(instance_id.as_bytes().as_slice(), bytes.as_slice())
            .map_err(|e| SaciError::generic(format!("insert instance: {e}")))?;
    }
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit: {e}")))?;
    Ok(ConsensusResponse::HeartbeatRecorded)
}

/// Sweep expired leases: flip `Claimed → Pending` for every claim whose
/// `lease_expires_at < now_at_propose`. Returns the count of reclaimed entries.
///
/// Both `CLAIMS` and `CLAIMS_BY_BATCH` are updated in a single transaction.
pub(crate) fn apply_reclaim_expired(
    db: &Database,
    now_at_propose: u64,
) -> SaciResult<ConsensusResponse> {
    // Collect expired claim_ids under a read transaction first.
    let expired: Vec<([u8; 16], ClaimRecord)> = {
        let rtxn = db
            .begin_read()
            .map_err(|e| SaciError::generic(format!("redb read txn (reclaim): {e}")))?;
        let table = match rtxn.open_table(CLAIMS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 });
            }
            Err(e) => return Err(SaciError::generic(format!("open claims (reclaim): {e}"))),
        };
        let mut out = Vec::new();
        for item in table
            .iter()
            .map_err(|e| SaciError::generic(format!("iter claims (reclaim): {e}")))?
        {
            let (k, v) =
                item.map_err(|e| SaciError::generic(format!("claims iter item (reclaim): {e}")))?;
            let key_bytes: [u8; 16] = k
                .value()
                .try_into()
                .map_err(|_| SaciError::generic("claim key not 16 bytes"))?;
            let record: ClaimRecord = dec(v.value())?;
            if record.status == ClaimStatus::Claimed && record.lease_expires_at < now_at_propose {
                out.push((key_bytes, record));
            }
        }
        out
    };

    if expired.is_empty() {
        return Ok(ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 });
    }

    let count = expired.len() as u32;
    let txn = db
        .begin_write()
        .map_err(|e| SaciError::generic(format!("redb write txn (reclaim): {e}")))?;

    // Collect the parent batch_ids while reclaiming. The `release_attempts` bump
    // cannot interleave with the claim-table writes because
    // `increment_release_attempts` opens a different table in the same txn.
    let mut bumped_batch_ids: Vec<u64> = Vec::with_capacity(expired.len());

    {
        let mut claim_table = txn
            .open_table(CLAIMS)
            .map_err(|e| SaciError::generic(format!("open claims (reclaim write): {e}")))?;
        let mut idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| SaciError::generic(format!("open claims_by_batch (reclaim): {e}")))?;

        for (key_bytes, mut record) in expired {
            record.status = ClaimStatus::Pending;
            record.lease_expires_at = 0;
            let updated = enc(&record)?;
            claim_table
                .insert(key_bytes.as_slice(), updated.as_slice())
                .map_err(|e| SaciError::generic(format!("update claim (reclaim): {e}")))?;

            // Sync secondary index.
            let sec_key = claims_by_batch_key(record.batch_id, &key_bytes);
            let sec_val = claims_by_batch_value(
                record.row_range_start,
                record.row_range_end,
                ClaimStatus::Pending,
            );
            idx_table
                .insert(sec_key.as_slice(), sec_val.as_slice())
                .map_err(|e| {
                    SaciError::generic(format!("update claims_by_batch (reclaim): {e}"))
                })?;

            bumped_batch_ids.push(record.batch_id);
        }
    }

    // Bump release_attempts on every reclaimed claim's parent batch. For retry-cap
    // purposes a crashed runner is indistinguishable from an explicit ReleaseClaim;
    // without this bump a runner that keeps crashing on a poison batch would never
    // trip the cap.
    for batch_id in bumped_batch_ids {
        increment_release_attempts(&txn, batch_id)?;
    }
    txn.commit()
        .map_err(|e| SaciError::generic(format!("commit (reclaim): {e}")))?;
    Ok(ConsensusResponse::ExpiredReclaimed {
        reclaimed_count: count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::state_machine::tests::{
        seed_claimed_guard, small_ipc, temp_db,
    };
    use crate::distributed::consensus::state_machine::{apply, read_claim};
    use crate::distributed::consensus::types::ConsensusCommand;

    #[test]
    fn test_claim_row_range_happy_path() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "items".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 200,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let claim_id = uuid::Uuid::now_v7();
        let instance_id = uuid::Uuid::now_v7();
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 50,
                claim_id,
                instance_id,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        assert!(
            matches!(
                resp,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "{resp:?}"
        );

        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.row_range_start, 0);
        assert_eq!(record.row_range_end, 50);
        assert_eq!(record.status, ClaimStatus::Claimed);
    }

    #[test]
    fn test_claim_overlap_rejected() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "items".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 200,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let c1 = uuid::Uuid::now_v7();
        let c2 = uuid::Uuid::now_v7();
        let inst = uuid::Uuid::now_v7();

        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id: c1,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Overlapping range should fail.
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 50,
                row_range_end: 150,
                claim_id: c2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error for overlapping claim, got {resp:?}"
        );
    }

    #[test]
    fn test_ack_claim_sets_completed() {
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
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        assert!(matches!(resp, ConsensusResponse::ClaimAcked), "{resp:?}");

        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.status, ClaimStatus::Completed);
    }

    #[test]
    fn test_release_claim_returns_to_pending() {
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
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        assert!(matches!(resp, ConsensusResponse::ClaimReleased), "{resp:?}");
        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.status, ClaimStatus::Pending);
    }

    #[test]
    fn test_heartbeat_stores_instance() {
        let (db, _path) = temp_db();
        let inst = uuid::Uuid::now_v7();
        let resp = apply(
            &db,
            ConsensusCommand::Heartbeat {
                instance_id: inst,
                at: 99999,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::HeartbeatRecorded),
            "{resp:?}"
        );
    }

    #[test]
    fn test_renew_claim_updates_expiry() {
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
        let original = read_claim(&db, claim_id).unwrap().unwrap().lease_expires_at;

        let resp = apply(
            &db,
            ConsensusCommand::RenewClaim {
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::ClaimRenewed { expires_at } => {
                assert!(expires_at >= original, "new expiry should be >= original");
            }
            _ => panic!("expected ClaimRenewed, got {resp:?}"),
        }
    }

    /// Verify that `CLAIMS_BY_BATCH` secondary index is populated on claim
    /// and correctly reflects status changes on ack/release.
    #[test]
    fn test_secondary_index_populated_and_updated() {
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

        // Read the secondary index directly and verify the entry.
        {
            let txn = db.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim_id.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index entry missing");
            let (start, end, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!((start, end, status), (0, 100, ClaimStatus::Claimed));
        }

        // Ack → status must flip to Completed in secondary index.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        {
            let txn = db.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim_id.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index entry missing");
            let (_, _, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!(status, ClaimStatus::Completed);
        }

        // Claim a second range.
        let claim2 = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 100,
                row_range_end: 200,
                claim_id: claim2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Release second claim → status must flip to Pending in secondary index.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id: claim2,
                instance_id: inst,
            },
        )
        .unwrap();
        {
            let txn = db.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim2.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index entry missing");
            let (_, _, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!(status, ClaimStatus::Pending);
        }
    }

    /// Verify that the secondary-index range scan isolates claims across batches: a
    /// claim in batch 2 must not be visible when scanning for batch 1.
    #[test]
    fn test_secondary_index_batch_isolation() {
        let (db, _path) = temp_db();
        let inst = uuid::Uuid::now_v7();

        for batch_id in 1u64..=3 {
            apply(
                &db,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id,
                    component: format!("comp_{batch_id}"),
                    schema_id: 1,
                    ipc_bytes: small_ipc(),
                    total_rows: 100,
                    now_at_propose: 0,
                },
            )
            .unwrap();
        }

        // Claim rows 0..50 in batch 2.
        let c2 = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 2,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Claiming the same rows in batch 1 must succeed (different batch).
        let c1 = uuid::Uuid::now_v7();
        let resp = apply(
            &db,
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
        assert!(
            matches!(resp, ConsensusResponse::BatchClaimed { .. }),
            "batch isolation failed: expected BatchClaimed, got {resp:?}"
        );

        // Claiming the same rows in batch 2 must be rejected (overlap).
        let c2b = uuid::Uuid::now_v7();
        let resp2 = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 2,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c2b,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp2, ConsensusResponse::Error { .. }),
            "expected overlap rejection within same batch, got {resp2:?}"
        );
    }

    #[test]
    fn claim_row_range_replay_idempotent() {
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
        let cmd = ConsensusCommand::ClaimRowRange {
            batch_id: 1,
            row_range_start: 0,
            row_range_end: 50,
            claim_id,
            instance_id: inst,
            lease_ttl_millis: 30_000,
            now_at_propose: 1000,
        };

        let resp1 = apply(&db, cmd.clone()).unwrap();
        assert!(
            matches!(
                resp1,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "first apply should succeed: {resp1:?}"
        );

        // Second apply of the same command must succeed (idempotent replay).
        let resp2 = apply(&db, cmd).unwrap();
        assert!(
            matches!(
                resp2,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "replay should be idempotent: {resp2:?}"
        );

        // There should be exactly one claim record.
        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.status, ClaimStatus::Claimed);
        assert_eq!(record.row_range_start, 0);
        assert_eq!(record.row_range_end, 50);
    }

    #[test]
    fn renew_claim_monotonic() {
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
        // Claim at t=1000, ttl=60_000 → expires at 61_000.
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();

        // Renew with stale now_at_propose=500 (before the claim was even made).
        // new_expires = 500 + 60_000 = 60_500 < 61_000 → max() keeps 61_000.
        let resp = apply(
            &db,
            ConsensusCommand::RenewClaim {
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 500,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::ClaimRenewed { expires_at } => {
                assert_eq!(
                    expires_at, 61_000,
                    "stale renew must not move expiry backwards"
                );
            }
            ConsensusResponse::Error { message } => {
                // The impl only rejects dead leases, where lease_expires_at <
                // now_at_propose; 61_000 < 500 is false, so reaching this branch means
                // monotonicity was violated.
                panic!("unexpected error: {message}");
            }
            other => panic!("unexpected response: {other:?}"),
        }

        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            record.lease_expires_at, 61_000,
            "lease_expires_at must not regress"
        );
    }

    #[test]
    fn reclaim_expired_frees_ranges() {
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
        // Claim at t=0 with ttl=100 → expires at 100.
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Before expiry: the range is still blocked.
        let resp_before =
            apply(&db, ConsensusCommand::ReclaimExpired { now_at_propose: 50 }).unwrap();
        assert!(
            matches!(
                resp_before,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 }
            ),
            "nothing should be reclaimed before expiry: {resp_before:?}"
        );
        let rec_before = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec_before.status, ClaimStatus::Claimed);

        // After expiry: sweep frees the range.
        let resp_after = apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200,
            },
        )
        .unwrap();
        assert!(
            matches!(
                resp_after,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 1 }
            ),
            "one claim should be reclaimed: {resp_after:?}"
        );

        let rec_after = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            rec_after.status,
            ClaimStatus::Pending,
            "claim must be Pending after reclaim"
        );
        assert_eq!(rec_after.lease_expires_at, 0);

        // The range must now be claimable again by a different claim_id.
        let claim_id2 = uuid::Uuid::now_v7();
        let resp_reclaim = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id: claim_id2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 200,
            },
        )
        .unwrap();
        assert!(
            matches!(resp_reclaim, ConsensusResponse::BatchClaimed { .. }),
            "range should be claimable after reclaim: {resp_reclaim:?}"
        );
    }

    #[test]
    fn test_ack_claim_rejects_pending_status() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        // Release the claim first, moving its status to Pending.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        // Acking a Pending claim must be rejected.
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "ack of Pending claim must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending, "must remain Pending");
    }

    #[test]
    fn test_release_claim_rejects_completed_status() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        // Ack to Completed first.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        // Releasing a Completed claim must be rejected.
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
            "release of Completed claim must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Completed, "must remain Completed");
    }

    /// `apply_release_claim` must reject a claim already in `ClaimStatus::Pending`,
    /// for instance when `ReclaimExpired` raced the runner and reclaimed the lease.
    /// The `release_attempts` bump lives inside the successful release branch and must
    /// not fire on this rejection path, otherwise a late `ReleaseClaim` arriving after
    /// a reclaim would double-count the attempt.
    #[test]
    fn test_release_claim_rejects_pending_status() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        // First release transitions the claim to Pending.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending);

        // A second release against the now-Pending claim is rejected by the status
        // guard: no mutation, no redb transaction error, just an Error response to the
        // caller.
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
            "release of Pending claim must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending, "must remain Pending");
    }

    #[test]
    fn test_claim_row_range_idempotency_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let batch_id = 77u64;
        let claim_id = uuid::Uuid::now_v7();
        let instance_a = uuid::Uuid::now_v7();
        let instance_b = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "idem_test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x77; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        // Instance A claims the range.
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: instance_a,
                lease_ttl_millis: 90_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        // Replay the same claim_id with instance_b. The idempotency check must not
        // return success because instance_id does not match.
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: instance_b,
                lease_ttl_millis: 90_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        // Must fall through to the overlap check, which rejects it, rather than
        // returning BatchClaimed.
        assert!(
            !matches!(resp, ConsensusResponse::BatchClaimed { instance_id, .. } if instance_id == instance_b),
            "wrong-instance idempotency replay must not mint a claim for instance_b, got {resp:?}"
        );
    }

    #[test]
    fn test_ack_claim_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let (claim_id, _correct) = seed_claimed_guard(&db);
        let wrong_instance = uuid::Uuid::now_v7();
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: wrong_instance,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error for wrong instance_id, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            rec.status,
            ClaimStatus::Claimed,
            "claim must remain Claimed"
        );
    }

    #[test]
    fn test_release_claim_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let (claim_id, _correct) = seed_claimed_guard(&db);
        let wrong_instance = uuid::Uuid::now_v7();
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id: wrong_instance,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error for wrong instance_id, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            rec.status,
            ClaimStatus::Claimed,
            "claim must remain Claimed"
        );
    }

    #[test]
    fn test_ack_claim_correct_instance_succeeds() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::ClaimAcked),
            "expected ClaimAcked, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Completed);
    }

    #[test]
    fn test_release_claim_correct_instance_succeeds() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::ClaimReleased),
            "expected ClaimReleased, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending);
    }

    #[test]
    fn test_late_ack_after_reclaim_rejected() {
        let (db, _path) = temp_db();
        let batch_id = 88u64;
        let claim_a = uuid::Uuid::now_v7();
        let instance_a = uuid::Uuid::now_v7();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "late_ack".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0xAB; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: claim_a,
                instance_id: instance_a,
                lease_ttl_millis: 1_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let swept = apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 2_000,
            },
        )
        .unwrap();
        assert!(
            matches!(
                swept,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 1 }
            ),
            "{swept:?}"
        );
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id: claim_a,
                instance_id: instance_a,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "late ack must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_a).unwrap().unwrap();
        assert_eq!(
            rec.status,
            ClaimStatus::Pending,
            "must remain Pending after late ack"
        );
    }

    #[test]
    fn test_renew_claim_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let (claim_id, _correct) = seed_claimed_guard(&db);
        let wrong_instance = uuid::Uuid::now_v7();
        let resp = apply(
            &db,
            ConsensusCommand::RenewClaim {
                claim_id,
                instance_id: wrong_instance,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "renew with wrong instance must be rejected, got {resp:?}"
        );
    }
}
