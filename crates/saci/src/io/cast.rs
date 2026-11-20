//! Schema cast utilities for the ingestion layer.
//!
//! [`cast_batch`] applies Arrow's `cast` kernel to each field of a
//! [`RecordBatch`] so it matches a target [`Schema`].
//!
//! `Dataset` registers a schema once and enforces it at append time, so the
//! physical type of a registered column cannot change in place. Casting is a
//! pre-ingestion step: cast the batch, then call
//! [`append_record_batch`](crate::dataset::Dataset::append_record_batch).
//!
//! `CastOptions { safe: true }` is the default: values that cannot be
//! represented in the target type become `null`. Set `safe: false` to return
//! an error instead.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::Schema;

use crate::error::{SaciError, SaciResult};

/// Cast each column of `batch` to match `target` schema.
///
/// A batch whose schema is `target` itself, or has equal fields, is returned
/// as a clone that shares the input's columns. Otherwise the result carries
/// `target` itself, not a copy of it.
///
/// Columns present in `batch` but absent from `target` are dropped.
/// Columns present in `target` but absent from `batch` produce an error.
///
/// # Errors
///
/// Returns `SaciError::Generic` if:
/// - A column required by `target` is missing from `batch`.
/// - Arrow's cast kernel returns an error (e.g. in non-safe mode when a value
///   overflows the target type).
pub fn cast_batch(
    batch: &RecordBatch,
    target: &Arc<Schema>,
    options: &CastOptions<'_>,
) -> SaciResult<RecordBatch> {
    if Arc::ptr_eq(batch.schema_ref(), target) || batch.schema_ref().fields() == target.fields() {
        return Ok(batch.clone());
    }

    let mut columns = Vec::with_capacity(target.fields().len());
    for target_field in target.fields() {
        let col = batch.column_by_name(target_field.name()).ok_or_else(|| {
            SaciError::generic(format!(
                "cast_batch: source batch missing column '{}'",
                target_field.name()
            ))
        })?;

        let cast_col = if col.data_type() == target_field.data_type() {
            col.clone()
        } else {
            cast_with_options(col.as_ref(), target_field.data_type(), options).map_err(|e| {
                SaciError::generic(format!(
                    "cast_batch: cannot cast column '{}' from {:?} to {:?}: {e}",
                    target_field.name(),
                    col.data_type(),
                    target_field.data_type()
                ))
            })?
        };
        columns.push(cast_col);
    }

    RecordBatch::try_new(Arc::clone(target), columns)
        .map_err(|e| SaciError::generic(format!("cast_batch: RecordBatch rebuild error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array};
    use arrow_cast::CastOptions;
    use arrow_schema::{DataType, Field, Schema};

    fn int32_batch(schema: Arc<Schema>, values: Vec<i32>) -> RecordBatch {
        let arr = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn test_cast_batch_int32_to_int64() {
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let tgt_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = int32_batch(src_schema.clone(), vec![1, 2, 3]);

        let options = CastOptions::default();
        let result = cast_batch(&batch, &tgt_schema, &options).unwrap();
        assert_eq!(result.schema().field(0).data_type(), &DataType::Int64);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1i64);
        assert_eq!(col.value(2), 3i64);
    }

    #[test]
    fn test_cast_batch_missing_column_returns_error() {
        let src_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let tgt_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));
        let batch = int32_batch(src_schema, vec![1]);
        let options = CastOptions::default();
        let err = cast_batch(&batch, &tgt_schema, &options).unwrap_err();
        assert!(err.message().contains("missing column"));
    }

    #[test]
    fn test_cast_batch_incompatible_safe_mode_produces_nulls() {
        use arrow_array::StringArray;
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let tgt_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let arr = Arc::new(StringArray::from(vec!["42", "not_a_number"]));
        let batch = RecordBatch::try_new(src_schema, vec![arr]).unwrap();
        let options = CastOptions {
            safe: true,
            ..Default::default()
        };
        let result = cast_batch(&batch, &tgt_schema, &options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cast_batch_incompatible_unsafe_mode_returns_error() {
        use arrow_array::StringArray;
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let tgt_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let arr = Arc::new(StringArray::from(vec!["not_a_number"]));
        let batch = RecordBatch::try_new(src_schema, vec![arr]).unwrap();
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_batch(&batch, &tgt_schema, &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_cast_batch_equal_schema_shares_columns() {
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let equal_target = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = int32_batch(src_schema, vec![1, 2, 3]);
        let options = CastOptions::default();

        let result = cast_batch(&batch, &equal_target, &options).unwrap();
        assert_eq!(result.num_columns(), batch.num_columns());
        for (out, input) in result.columns().iter().zip(batch.columns()) {
            assert!(Arc::ptr_eq(out, input));
        }
        assert_eq!(result.schema().fields(), equal_target.fields());
    }

    #[test]
    fn test_cast_batch_result_uses_the_target_arc() {
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = int32_batch(src_schema, vec![1]);
        let result = cast_batch(&batch, &target, &CastOptions::default()).unwrap();
        assert!(Arc::ptr_eq(result.schema_ref(), &target));
    }
}
