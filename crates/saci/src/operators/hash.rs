//! Key-hash and canonical key-string helpers shared by grouping, join,
//! deduplication, and windowed aggregation operators.

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

/// Cast each key column to `Utf8` once per column, then invoke `on_row` with
/// each row's canonical null-safe key string.
///
/// Null values encode as `\x01` and present values as `\x02` followed by their
/// UTF-8 representation, separated by `\x00`. A null key therefore never
/// collides with `"null"` or an empty string, and multi-column `("ab", "c")`
/// never collides with `("a", "bc")`.
pub(crate) fn for_each_row_key(
    cols: &[&ArrayRef],
    mut on_row: impl FnMut(usize, &str),
) -> Result<(), SaciError> {
    if cols.is_empty() {
        return Ok(());
    }
    let n_rows = cols[0].len();
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
        on_row(row_idx, &buf);
    }
    Ok(())
}

/// Compute a per-row FNV-1a hash over a set of key columns.
///
/// Each column in `cols` is cast to `Utf8` via `arrow_cast`, then the row's
/// values are concatenated with `'\x00'` separators and hashed. A null is
/// encoded as the single prefix byte `\x01` and a present value as `\x02`
/// followed by its UTF-8 bytes, so a null key can never collide with the literal
/// string `"null"` or any other real value.
///
/// Pass an empty slice for a non-keyed operation, returning an empty array.
///
/// # Errors
///
/// Returns `SaciError::Generic` if `arrow_cast::cast` fails for any column.
pub fn compute_key_hash(cols: &[&ArrayRef]) -> Result<Int64Array, SaciError> {
    if cols.is_empty() {
        return Ok(Int64Array::from(vec![0i64; 0]));
    }
    let mut hashes = Vec::with_capacity(cols[0].len());
    for_each_row_key(cols, |_row_idx, key| {
        hashes.push(fnv1a(key.as_bytes()));
    })?;
    Ok(Int64Array::from(hashes))
}

/// All-zeros `Int64Array` of length `n_rows`, for global (non-keyed) groups.
pub fn compute_global_hash(n_rows: usize) -> Int64Array {
    Int64Array::from(vec![0i64; n_rows])
}

/// Materialize `(hash, canonical_key)` pairs per row over `cols`.
pub(crate) fn compute_row_keys(cols: &[&ArrayRef]) -> Result<Vec<(i64, String)>, SaciError> {
    if cols.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(cols[0].len());
    for_each_row_key(cols, |_row_idx, key| {
        out.push((fnv1a(key.as_bytes()), key.to_string()));
    })?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};

    #[test]
    fn canonical_keys_distinguish_null_and_boundaries() {
        let a: ArrayRef = Arc::new(StringArray::from(vec![None, Some("null"), Some("ab")]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), Some("x"), Some("c")]));
        let c: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 2]));
        let keys = compute_row_keys(&[&a, &b, &c]).unwrap();
        assert_eq!(keys.len(), 3);
        assert_ne!(keys[0].1, keys[1].1);
        assert_ne!(keys[0].0, keys[1].0);
    }
}
