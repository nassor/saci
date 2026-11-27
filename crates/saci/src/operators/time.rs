//! Timestamp-to-milliseconds conversion shared by streaming operators.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};

use crate::error::{SaciError, SaciResult};

/// Convert an `Int64` (milliseconds) or Arrow `Timestamp` column into an
/// `Int64Array` of Unix milliseconds, preserving nulls.
pub(crate) fn to_ms_array(col: &ArrayRef) -> SaciResult<Int64Array> {
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .cloned()
            .ok_or_else(|| SaciError::generic("downcast Int64Array failed")),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| SaciError::generic("downcast TimestampMillisecondArray failed"))?;
            Ok(Int64Array::from_iter(arr.iter()))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| SaciError::generic("downcast TimestampSecondArray failed"))?;
            Ok(Int64Array::from_iter(
                arr.iter().map(|v| v.map(|s| s.saturating_mul(1_000))),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| SaciError::generic("downcast TimestampMicrosecondArray failed"))?;
            Ok(Int64Array::from_iter(
                arr.iter().map(|v| v.map(|us| us.div_euclid(1_000))),
            ))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| SaciError::generic("downcast TimestampNanosecondArray failed"))?;
            Ok(Int64Array::from_iter(
                arr.iter().map(|v| v.map(|ns| ns.div_euclid(1_000_000))),
            ))
        }
        other => Err(SaciError::generic(format!(
            "time field must be Int64 or Timestamp, got {other:?}"
        ))),
    }
}

/// Serialize a `RecordBatch` into an Arrow IPC stream byte vector.
pub(crate) fn batch_to_ipc(batch: &RecordBatchWrap<'_>) -> SaciResult<Vec<u8>> {
    use arrow_ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.0.schema_ref())
            .map_err(|e| SaciError::generic(format!("operator IPC writer init: {e}")))?;
        writer
            .write(batch.0)
            .map_err(|e| SaciError::generic(format!("operator IPC writer write: {e}")))?;
        writer
            .finish()
            .map_err(|e| SaciError::generic(format!("operator IPC writer finish: {e}")))?;
    }
    Ok(buf)
}

pub(crate) struct RecordBatchWrap<'a>(pub(crate) &'a arrow_array::RecordBatch);

/// Deserialize a `RecordBatch` from Arrow IPC stream bytes (`None` if empty).
pub(crate) fn ipc_to_batch(bytes: &[u8]) -> SaciResult<Option<arrow_array::RecordBatch>> {
    use arrow_ipc::reader::StreamReader;
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| SaciError::generic(format!("operator IPC reader init: {e}")))?;
    match reader.next() {
        Some(Ok(batch)) => Ok(Some(batch)),
        Some(Err(e)) => Err(SaciError::generic(format!("operator IPC reader read: {e}"))),
        None => Ok(None),
    }
}

/// Concatenate non-empty `RecordBatch` slices into a single batch with `schema`.
pub(crate) fn concat_non_empty(
    schema: &Arc<arrow_schema::Schema>,
    batches: &[arrow_array::RecordBatch],
) -> SaciResult<arrow_array::RecordBatch> {
    let non_empty: Vec<&arrow_array::RecordBatch> =
        batches.iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        return Ok(arrow_array::RecordBatch::new_empty(schema.clone()));
    }
    arrow_select::concat::concat_batches(schema, non_empty)
        .map_err(|e| SaciError::generic(format!("concat_non_empty error: {e}")))
}
