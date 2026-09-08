//! Time column utilities: casting timestamps to milliseconds since epoch.

use arrow_array::{
    Array, ArrayRef, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::DataType;

use crate::error::SaciError;

/// Convert a time column to Int64 milliseconds since epoch.
///
/// Supports:
/// - `Int64`: passed through as-is (assumed to be milliseconds)
/// - `TimestampMillisecond`: converted directly
/// - `TimestampSecond`: multiplied by 1000
/// - `TimestampMicrosecond`: divided by 1000 (truncates)
/// - `TimestampNanosecond`: divided by 1_000_000 (truncates)
pub fn to_ms_array(col: &ArrayRef) -> Result<Int64Array, SaciError> {
    let data_type = col.data_type();

    match data_type {
        DataType::Int64 => Ok(col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| SaciError::generic("failed to downcast Int64Array"))?
            .clone()),
        DataType::Timestamp(unit, _) => {
            use arrow_schema::TimeUnit;
            match unit {
                TimeUnit::Second => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            SaciError::generic("failed to downcast TimestampSecondArray")
                        })?;
                    let ms: Int64Array = arr
                        .iter()
                        .map(|opt_ts| opt_ts.map(|ts| ts * 1000))
                        .collect();
                    Ok(ms)
                }
                TimeUnit::Millisecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            SaciError::generic("failed to downcast TimestampMillisecondArray")
                        })?;
                    let ms: Int64Array = arr.iter().collect();
                    Ok(ms)
                }
                TimeUnit::Microsecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            SaciError::generic("failed to downcast TimestampMicrosecondArray")
                        })?;
                    let ms: Int64Array = arr
                        .iter()
                        .map(|opt_ts| opt_ts.map(|ts| ts / 1000))
                        .collect();
                    Ok(ms)
                }
                TimeUnit::Nanosecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            SaciError::generic("failed to downcast TimestampNanosecondArray")
                        })?;
                    let ms: Int64Array = arr
                        .iter()
                        .map(|opt_ts| opt_ts.map(|ts| ts / 1_000_000))
                        .collect();
                    Ok(ms)
                }
            }
        }
        _ => Err(SaciError::generic(format!(
            "time column must be Int64 or Timestamp variant, got {data_type:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int64Builder, TimestampMillisecondBuilder, TimestampSecondBuilder};

    fn arc(arr: impl arrow_array::Array + 'static) -> ArrayRef {
        std::sync::Arc::new(arr)
    }

    #[test]
    fn test_int64_passthrough() {
        let mut builder = Int64Builder::new();
        builder.append_value(1000);
        builder.append_value(2000);
        builder.append_null();
        let col = arc(builder.finish());

        let result = to_ms_array(&col).unwrap();
        assert_eq!(result.value(0), 1000);
        assert_eq!(result.value(1), 2000);
        assert!(result.is_null(2));
    }

    #[test]
    fn test_timestamp_millisecond() {
        let mut builder = TimestampMillisecondBuilder::new();
        builder.append_value(1000);
        builder.append_value(2000);
        builder.append_null();
        let col = arc(builder.finish());

        let result = to_ms_array(&col).unwrap();
        assert_eq!(result.value(0), 1000);
        assert_eq!(result.value(1), 2000);
        assert!(result.is_null(2));
    }

    #[test]
    fn test_timestamp_second() {
        let mut builder = TimestampSecondBuilder::new();
        builder.append_value(1); // 1 second = 1000 ms
        builder.append_value(2); // 2 seconds = 2000 ms
        builder.append_null();
        let col = arc(builder.finish());

        let result = to_ms_array(&col).unwrap();
        assert_eq!(result.value(0), 1000);
        assert_eq!(result.value(1), 2000);
        assert!(result.is_null(2));
    }
}
