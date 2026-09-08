//! Encoding helpers: JSON record serialization, composite redb keys, and the
//! packed secondary-index value.
//!
//! Nothing here touches a database. These are the pure functions that decide
//! how a record, a key, or a status byte is laid out on disk, so the handlers
//! and queries agree byte-for-byte on every encoding.

use serde::{Deserialize, Serialize};

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::consensus::types::ClaimStatus;

pub(super) fn enc<T: Serialize>(v: &T) -> SaciResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| SaciError::generic(format!("state machine encode: {e}")))
}

pub(super) fn dec<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> SaciResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|e| SaciError::generic(format!("state machine decode: {e}")))
}

/// Compute the checkpoint content hash (FNV-1a 64) from its identifying inputs.
/// The body is streamed through the hasher without building an intermediate
/// buffer, so a 1 MiB checkpoint does not pay for a 1 MiB allocation + memcpy.
pub(super) fn checkpoint_content_hash(
    claim_id: &[u8; 16],
    stage_idx: u32,
    ipc_bytes: &[u8],
) -> u64 {
    use std::hash::Hasher as _;
    let mut h = fnv::FnvHasher::default();
    h.write(claim_id);
    h.write(&stage_idx.to_be_bytes());
    h.write(ipc_bytes);
    h.finish()
}

/// Build a 20-byte composite key: `claim_id_bytes (16) || stage_idx_be (4)`.
pub(super) fn checkpoint_key(claim_id: &[u8; 16], stage_idx: u32) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[..16].copy_from_slice(claim_id);
    k[16..].copy_from_slice(&stage_idx.to_be_bytes());
    k
}

/// Build a 24-byte secondary index key: `batch_id_be8 (8) || claim_id_16 (16)`.
///
/// The `batch_id_be8` prefix enables efficient batch-scoped range queries.
pub(super) fn claims_by_batch_key(batch_id: u64, claim_id: &[u8; 16]) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[..8].copy_from_slice(&batch_id.to_be_bytes());
    k[8..].copy_from_slice(claim_id);
    k
}

/// Build the 8-byte lower-bound key for a batch range scan: `batch_id_be8`.
pub(super) fn batch_range_start(batch_id: u64) -> [u8; 8] {
    batch_id.to_be_bytes()
}

/// Build the 8-byte exclusive upper-bound key for a batch range scan:
/// `(batch_id + 1)_be8`. Returns `None` if `batch_id == u64::MAX`.
pub(super) fn batch_range_end(batch_id: u64) -> Option<[u8; 8]> {
    batch_id.checked_add(1).map(|n| n.to_be_bytes())
}

/// Encode the secondary index value: `start_row_be4 ++ end_row_be4 ++ status_byte`.
pub(super) fn claims_by_batch_value(start: u32, end: u32, status: ClaimStatus) -> [u8; 9] {
    let mut v = [0u8; 9];
    v[..4].copy_from_slice(&start.to_be_bytes());
    v[4..8].copy_from_slice(&end.to_be_bytes());
    v[8] = status_byte(status);
    v
}

/// Decode the secondary index value into `(start_row, end_row, status)`.
///
/// Returns `None` if the slice is not exactly 9 bytes or the status byte is
/// unrecognised.
pub(super) fn decode_claims_by_batch_value(v: &[u8]) -> Option<(u32, u32, ClaimStatus)> {
    if v.len() != 9 {
        return None;
    }
    let start = u32::from_be_bytes(v[..4].try_into().ok()?);
    let end = u32::from_be_bytes(v[4..8].try_into().ok()?);
    let status = status_from_byte(v[8])?;
    Some((start, end, status))
}

/// Encode a [`ClaimStatus`] as a single byte.
fn status_byte(s: ClaimStatus) -> u8 {
    match s {
        ClaimStatus::Pending => 0,
        ClaimStatus::Claimed => 1,
        ClaimStatus::Completed => 2,
    }
}

/// Decode a [`ClaimStatus`] from a single byte. Returns `None` for unknown bytes.
fn status_from_byte(b: u8) -> Option<ClaimStatus> {
    match b {
        0 => Some(ClaimStatus::Pending),
        1 => Some(ClaimStatus::Claimed),
        2 => Some(ClaimStatus::Completed),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the secondary index value encoding/decoding round-trips correctly.
    #[test]
    fn test_secondary_index_encoding_round_trip() {
        for status in [
            ClaimStatus::Pending,
            ClaimStatus::Claimed,
            ClaimStatus::Completed,
        ] {
            let encoded = claims_by_batch_value(42, 84, status);
            let (start, end, decoded_status) =
                decode_claims_by_batch_value(&encoded).expect("decode must succeed");
            assert_eq!(start, 42);
            assert_eq!(end, 84);
            assert_eq!(decoded_status, status);
        }

        // Malformed (wrong length) must return None.
        assert!(decode_claims_by_batch_value(&[0u8; 8]).is_none());
        assert!(decode_claims_by_batch_value(&[0u8; 10]).is_none());
        assert!(decode_claims_by_batch_value(&[]).is_none());
    }
}
