//! Key-hash computation for windowed grouping.
//!
//! Produces a stable `Int64Array` holding the FNV-1a hash of each row's key
//! columns. A zero-length `cols` slice means no key (global window) and every
//! row hashes to `0`.

use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_cast::cast;
use arrow_schema::DataType;

use crate::error::SaciError;

fn fnv1a(data: &[u8]) -> i64 {
    use std::hash::Hasher as _;
    let mut h = fnv::FnvHasher::default();
    h.write(data);
    h.finish() as i64
}

/// Compute a per-row FNV-1a hash over a set of key columns.
///
/// Each column in `cols` is cast to `Utf8` via `arrow_cast`, then the row's
/// values are concatenated with `'\x00'` separators and hashed. A null is
/// encoded as the single prefix byte `\x01` and a present value as `\x02`
/// followed by its UTF-8 bytes, so a null key can never collide with the literal
/// string `"null"` or any other real value.
///
/// Pass an empty slice for a non-keyed window, where every row maps to the same
/// global bucket.
///
/// # Errors
///
/// Returns `SaciError::Generic` if `arrow_cast::cast` fails for any column.
pub fn compute_key_hash(cols: &[&ArrayRef]) -> Result<Int64Array, SaciError> {
    let n_rows = if cols.is_empty() {
        // Non-keyed callers pass an empty slice. The row count is unknown here,
        // so this returns a 0-length array and the caller uses the pipeline row
        // count when building the window ID column.
        return Ok(Int64Array::from(vec![0i64; 0]));
    } else {
        cols[0].len()
    };

    // Cast every column to Utf8 once so we pay the cast cost per-column, not
    // per-row-per-column.
    let string_cols: Vec<StringArray> = cols
        .iter()
        .map(|col| {
            let casted = cast(col.as_ref(), &DataType::Utf8).map_err(|e| {
                SaciError::generic(format!("failed to cast key column to Utf8: {e}"))
            })?;
            casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SaciError::generic("downcast to StringArray failed after cast"))
                .cloned()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut hashes = Vec::with_capacity(n_rows);
    let mut buf = String::new();

    for row_idx in 0..n_rows {
        buf.clear();
        for (col_idx, col) in string_cols.iter().enumerate() {
            if col_idx > 0 {
                buf.push('\x00');
            }
            if col.is_null(row_idx) {
                buf.push('\x01');
            } else {
                buf.push('\x02');
                buf.push_str(col.value(row_idx));
            }
        }
        hashes.push(fnv1a(buf.as_bytes()));
    }

    Ok(Int64Array::from(hashes))
}

/// All-zeros `Int64Array` of length `n_rows`, for global (non-keyed) windows.
pub fn compute_global_hash(n_rows: usize) -> Int64Array {
    Int64Array::from(vec![0i64; n_rows])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};

    fn str_col(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn i32_col(values: &[i32]) -> ArrayRef {
        Arc::new(Int32Array::from(values.to_vec()))
    }

    #[test]
    fn test_no_key_returns_empty() {
        let result = compute_key_hash(&[]).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_global_hash_all_zeros() {
        let result = compute_global_hash(4);
        assert_eq!(result.len(), 4);
        assert!(result.values().iter().all(|&v| v == 0));
    }

    #[test]
    fn test_single_key_column_distinct_rows() {
        let col = str_col(&[Some("a"), Some("b"), Some("c")]);
        let result = compute_key_hash(&[&col]).unwrap();
        assert_eq!(result.len(), 3);
        assert_ne!(result.value(0), result.value(1));
        assert_ne!(result.value(1), result.value(2));
    }

    #[test]
    fn test_single_key_column_same_value() {
        let col = str_col(&[Some("x"), Some("x"), Some("x")]);
        let result = compute_key_hash(&[&col]).unwrap();
        assert_eq!(result.value(0), result.value(1));
        assert_eq!(result.value(1), result.value(2));
    }

    #[test]
    fn test_single_key_column_null_does_not_collide_with_null_string() {
        // A null key and the literal string "null" must hash differently.
        let col = str_col(&[None, Some("null")]);
        let result = compute_key_hash(&[&col]).unwrap();
        assert_ne!(
            result.value(0),
            result.value(1),
            "null key must not collide with the string \"null\""
        );
    }

    #[test]
    fn test_single_key_column_null_keys_group_together() {
        // Two null keys must hash to the same value so they land in the same
        // window group.
        let col = str_col(&[None, Some("x"), None]);
        let result = compute_key_hash(&[&col]).unwrap();
        assert_eq!(
            result.value(0),
            result.value(2),
            "two null keys must produce the same hash"
        );
        assert_ne!(
            result.value(0),
            result.value(1),
            "null key must not collide with non-null key"
        );
    }

    #[test]
    fn test_single_key_non_string_column() {
        // Int32 column is cast to Utf8 before hashing.
        let col = i32_col(&[1, 2, 1]);
        let result = compute_key_hash(&[&col]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), result.value(2));
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_multi_key_same_row_different_partition() {
        let col_a = str_col(&[Some("us"), Some("eu"), Some("us")]);
        let col_b = str_col(&[Some("prod"), Some("prod"), Some("dev")]);

        let result = compute_key_hash(&[&col_a, &col_b]).unwrap();
        assert_eq!(result.len(), 3);

        // Rows 0, 1, 2 are (us, prod), (eu, prod), (us, dev).
        assert_ne!(result.value(0), result.value(1));
        assert_ne!(result.value(0), result.value(2));
    }

    #[test]
    fn test_multi_key_identical_rows_same_hash() {
        let col_a = str_col(&[Some("x"), Some("x")]);
        let col_b = str_col(&[Some("y"), Some("y")]);

        let result = compute_key_hash(&[&col_a, &col_b]).unwrap();
        assert_eq!(result.value(0), result.value(1));
    }

    #[test]
    fn test_multi_key_separator_prevents_collision() {
        // Without the '\x00' separator "ab","c" and "a","bc" would collide.
        let col_a1 = str_col(&[Some("ab")]);
        let col_b1 = str_col(&[Some("c")]);

        let col_a2 = str_col(&[Some("a")]);
        let col_b2 = str_col(&[Some("bc")]);

        let r1 = compute_key_hash(&[&col_a1, &col_b1]).unwrap();
        let r2 = compute_key_hash(&[&col_a2, &col_b2]).unwrap();
        assert_ne!(r1.value(0), r2.value(0));
    }
}
