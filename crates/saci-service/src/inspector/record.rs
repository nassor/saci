//! The shapes the inspector captures.
//!
//! Every one of them is defined in [`saci_inspector_wire`] and re-exported here,
//! not redefined: the buffers hold exactly what `/api/*` serves, so there is no
//! capture-to-wire conversion step and no pair of structs to keep in sync. The
//! string fields are [`Cow<'static, str>`](std::borrow::Cow) for that reason:
//! capture borrows the `&'static str` a `tracing` callsite already owns, and the
//! browser deserializes the same field into an owned `String`.
//!
//! `trace_id` and `span_id` are `tracing`'s own [`span::Id`](tracing::span::Id)
//! values, not W3C ids. This telemetry never leaves the process, and minting
//! 128-bit ids would need a second id map for no local benefit; the tradeoff is
//! that an id here cannot be correlated with one at an OTLP collector.

pub use saci_inspector_wire::{
    LogRecord, MetricSample, Pair, SeriesKind, SeriesPoint, SpanRecord, TraceDetail, TraceSummary,
};

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds since the Unix epoch.
///
/// A clock stepped behind the epoch reports 0 rather than failing: a bad
/// timestamp must not drop the record that carries it.
pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The uppercase name `/api/logs` reports for `level`.
pub(crate) fn level_name(level: &tracing::Level) -> &'static str {
    match *level {
        tracing::Level::ERROR => "ERROR",
        tracing::Level::WARN => "WARN",
        tracing::Level::INFO => "INFO",
        tracing::Level::DEBUG => "DEBUG",
        tracing::Level::TRACE => "TRACE",
    }
}

/// Severity rank, `0` most severe.
///
/// Ordering, not identity: `/api/logs?level=warn` keeps every record whose rank
/// is at or below `WARN`'s. An unrecognised name ranks least severe so a filter
/// never silently discards records it cannot classify.
pub(crate) fn level_rank(level: &str) -> u8 {
    match level {
        "ERROR" | "error" => 0,
        "WARN" | "warn" => 1,
        "INFO" | "info" => 2,
        "DEBUG" | "debug" => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_names_match_tracing_levels() {
        assert_eq!(level_name(&tracing::Level::ERROR), "ERROR");
        assert_eq!(level_name(&tracing::Level::TRACE), "TRACE");
    }

    #[test]
    fn level_rank_orders_by_severity_and_accepts_both_cases() {
        assert!(level_rank("ERROR") < level_rank("WARN"));
        assert!(level_rank("WARN") < level_rank("INFO"));
        assert_eq!(level_rank("warn"), level_rank("WARN"));
        assert_eq!(level_rank("nonsense"), 4);
    }

    #[test]
    fn now_unix_ms_is_after_2020() {
        assert!(now_unix_ms() > 1_577_836_800_000);
    }
}
