//! The timestamped object key layout.

use chrono::{DateTime, Utc};
use object_store::path::Path;

/// `{prefix}/{YYYYMMDDTHHMMSS.mmmZ}-{NNNNNN}{suffix}`.
///
/// The timestamp is UTC and fixed-width, so key order is upload order, which is
/// what [`crate::source::S3Source`]'s sorted listing relies on to replay
/// objects in the order they were written. `seq` is the sink's own flush
/// counter, zero-padded to six digits, which keeps two objects opened inside
/// one millisecond distinct.
pub(crate) fn object_key(prefix: &str, suffix: &str, opened_at: DateTime<Utc>, seq: u64) -> Path {
    let stem = format!(
        "{}-{seq:06}{suffix}",
        opened_at.format("%Y%m%dT%H%M%S%.3fZ")
    );
    Path::from(prefix).join(stem)
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;

    /// 2025-01-15T10:40:00.123Z, spelled out so the expected key is readable.
    fn opened() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 1, 15, 10, 40, 0).unwrap() + chrono::Duration::milliseconds(123)
    }

    #[test]
    fn renders_the_documented_shape() {
        assert_eq!(
            object_key("orders", ".csv", opened(), 7).as_ref(),
            "orders/20250115T104000.123Z-000007.csv"
        );
        assert_eq!(
            object_key("", "", opened(), 0).as_ref(),
            "20250115T104000.123Z-000000"
        );
    }

    #[test]
    fn keys_one_millisecond_apart_sort_in_time_order() {
        let a = object_key("orders", ".csv", opened(), 0);
        let b = object_key(
            "orders",
            ".csv",
            opened() + chrono::Duration::milliseconds(1),
            0,
        );
        assert!(a.as_ref() < b.as_ref());
    }

    #[test]
    fn same_timestamp_keys_sort_by_seq() {
        let a = object_key("orders", ".csv", opened(), 0);
        let b = object_key("orders", ".csv", opened(), 1);
        assert!(a.as_ref() < b.as_ref());
    }

    #[test]
    fn a_key_crossing_a_second_boundary_sorts_after_one_before_it() {
        let a = object_key(
            "orders",
            ".csv",
            opened() + chrono::Duration::milliseconds(999),
            9,
        );
        let b = object_key("orders", ".csv", opened() + chrono::Duration::seconds(1), 0);
        assert!(a.as_ref() < b.as_ref());
    }
}
