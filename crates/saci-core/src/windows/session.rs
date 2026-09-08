//! Session assignment: turning event timestamps into per-key session IDs.
//!
//! The geometry a session window is declared with lives in
//! [`crate::window_spec::WindowSpec`], outside this feature, because a host
//! parses the declaration whether or not it carries the engine.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_select::take::take;

use crate::error::SaciError;

use super::hash::compute_key_hash;

/// Assign session IDs to every row in `batch`.
///
/// Algorithm:
/// 1. Compute a per-row key hash from `key_cols` (empty slice = global, all rows same key).
/// 2. Sort rows by `(key_hash, ts_ms)` using Arrow's lexicographic sort.
/// 3. Scan the sorted order: start a new session whenever the key changes
///    or the gap to the previous event exceeds `gap_ms`.
/// 4. Map sorted session IDs back to the original row order.
///
/// Returns an `Int64Array` of length `batch.num_rows()` where `result[i]` is
/// the session ID for original row `i`.  Session IDs are zero-based and
/// monotonically increasing in the sort order.
///
/// # Errors
///
/// Returns `SaciError::Generic` if key-hashing, sorting, or index-mapping fails.
pub fn assign_sessions(
    ts_ms: &Int64Array,
    key_cols: &[&ArrayRef],
    gap_ms: i64,
) -> Result<Int64Array, SaciError> {
    let n = ts_ms.len();
    if n == 0 {
        return Ok(Int64Array::from(Vec::<i64>::new()));
    }

    let key_hash = if key_cols.is_empty() {
        Int64Array::from(vec![0i64; n])
    } else {
        compute_key_hash(key_cols)?
    };

    let sort_cols = vec![
        SortColumn {
            values: Arc::new(key_hash.clone()) as ArrayRef,
            options: None,
        },
        SortColumn {
            values: Arc::new(ts_ms.clone()) as ArrayRef,
            options: None,
        },
    ];
    let sorted_indices = lexsort_to_indices(&sort_cols, None)
        .map_err(|e| SaciError::generic(format!("assign_sessions: sort error: {e}")))?;

    // Reorder key_hash and ts_ms to the sorted order.
    let sorted_key_hash = take(&key_hash as &dyn Array, &sorted_indices, None)
        .map_err(|e| SaciError::generic(format!("assign_sessions: take key_hash: {e}")))?;
    let sorted_ts = take(ts_ms as &dyn Array, &sorted_indices, None)
        .map_err(|e| SaciError::generic(format!("assign_sessions: take ts: {e}")))?;

    let sorted_key_hash = sorted_key_hash
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| SaciError::generic("assign_sessions: downcast key_hash failed"))?;
    let sorted_ts = sorted_ts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| SaciError::generic("assign_sessions: downcast ts failed"))?;

    let mut sorted_session_ids = vec![0i64; n];
    let mut session_id = 0i64;

    // Iterate consecutive (prev, curr) pairs starting at 1. Raw indices are
    // needed to call `.value(i)` on the Arrow arrays, so zip an offset-by-one
    // range instead of indexing `sorted_session_ids` directly.
    let keys = sorted_key_hash.values();
    let timestamps = sorted_ts.values();
    for (prev_idx, slot) in sorted_session_ids[1..].iter_mut().enumerate() {
        let curr_idx = prev_idx + 1;
        if keys[curr_idx] != keys[prev_idx]
            || (timestamps[curr_idx] - timestamps[prev_idx]) > gap_ms
        {
            session_id += 1;
        }
        *slot = session_id;
    }

    // sorted_indices[sort_pos] = original_row_idx
    // We need result[original_row_idx] = sorted_session_ids[sort_pos]
    let mut result = vec![0i64; n];
    for (sort_pos, &orig_row) in sorted_indices.values().iter().enumerate() {
        result[orig_row as usize] = sorted_session_ids[sort_pos];
    }

    Ok(Int64Array::from(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};

    use super::*;

    fn str_col(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    /// Single key, two clear sessions separated by a large gap.
    #[test]
    fn test_assign_sessions_single_key_two_sessions() {
        // gap_ms = 1000; events at 0, 100, 200 then 5000, 5100 → 2 sessions
        let ts = Int64Array::from(vec![0i64, 100, 200, 5000, 5100]);
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.len(), 5);
        // All first three in session 0, last two in session 1
        assert_eq!(ids.value(0), ids.value(1));
        assert_eq!(ids.value(1), ids.value(2));
        assert_ne!(ids.value(2), ids.value(3));
        assert_eq!(ids.value(3), ids.value(4));
    }

    /// Single key, all events close together → one session.
    #[test]
    fn test_assign_sessions_single_key_one_session() {
        let ts = Int64Array::from(vec![0i64, 500, 999]);
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids.value(0), ids.value(1));
        assert_eq!(ids.value(1), ids.value(2));
    }

    /// Exact gap boundary: gap == gap_ms is NOT a new session; gap > gap_ms IS.
    #[test]
    fn test_assign_sessions_exact_gap_boundary() {
        // gap_ms = 1000; ts diff of 1000 is not > 1000 so same session
        let ts = Int64Array::from(vec![0i64, 1000, 2001]);
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.value(0), ids.value(1), "gap==gap_ms: same session");
        assert_ne!(ids.value(1), ids.value(2), "gap>gap_ms: new session");
    }

    /// Multiple keys: each key gets its own independent session numbering.
    #[test]
    fn test_assign_sessions_multi_key() {
        // key A: ts 0, 100, 5000   (gap after 100 → new session for A)
        // key B: ts 200, 300       (one session for B)
        // Interleaved in original order: A0, B0, A1, B1, A2
        let ts = Int64Array::from(vec![0i64, 200, 100, 300, 5000]);
        let key_col = str_col(&["A", "B", "A", "B", "A"]);
        let key_ref: ArrayRef = key_col;
        let ids = assign_sessions(&ts, &[&key_ref], 1000).unwrap();
        assert_eq!(ids.len(), 5);

        // A events are at original indices 0, 2, 4
        let a0 = ids.value(0); // ts=0
        let a1 = ids.value(2); // ts=100 (same session as ts=0, gap=100 <= 1000)
        let a2 = ids.value(4); // ts=5000 (new session, gap=4900 > 1000)
        assert_eq!(a0, a1, "A: ts=0 and ts=100 same session");
        assert_ne!(a1, a2, "A: ts=5000 is a new session");

        // B events are at original indices 1, 3
        let b0 = ids.value(1); // ts=200
        let b1 = ids.value(3); // ts=300 (gap=100 <= 1000, same session)
        assert_eq!(b0, b1, "B: ts=200 and ts=300 same session");
    }

    /// Single event → always session 0.
    #[test]
    fn test_assign_sessions_single_event() {
        let ts = Int64Array::from(vec![42i64]);
        let ids = assign_sessions(&ts, &[], 500).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.value(0), 0);
    }

    /// Empty input → empty output.
    #[test]
    fn test_assign_sessions_empty() {
        let ts = Int64Array::from(Vec::<i64>::new());
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.len(), 0);
    }
}
