//! Key-hash computation for windowed grouping.
//!
//! Re-exports the shared null-safe FNV-1a key-hash helpers from
//! [`crate::operators::hash`].

pub use crate::operators::hash::{compute_global_hash, compute_key_hash};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, StringArray};

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
        let col = str_col(&[None, Some("null")]);
        let result = compute_key_hash(&[&col]).unwrap();
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_single_key_column_null_keys_group_together() {
        let col = str_col(&[None, Some("x"), None]);
        let result = compute_key_hash(&[&col]).unwrap();
        assert_eq!(result.value(0), result.value(2));
        assert_ne!(result.value(0), result.value(1));
    }

    #[test]
    fn test_single_key_non_string_column() {
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
        let col_a1 = str_col(&[Some("ab")]);
        let col_b1 = str_col(&[Some("c")]);
        let col_a2 = str_col(&[Some("a")]);
        let col_b2 = str_col(&[Some("bc")]);

        let r1 = compute_key_hash(&[&col_a1, &col_b1]).unwrap();
        let r2 = compute_key_hash(&[&col_a2, &col_b2]).unwrap();
        assert_ne!(r1.value(0), r2.value(0));
    }
}
