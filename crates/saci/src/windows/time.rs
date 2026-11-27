//! Time column utilities: casting timestamps to milliseconds since epoch.

use arrow_array::{ArrayRef, Int64Array};

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
    crate::operators::time::to_ms_array(col)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
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
