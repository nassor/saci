//! [`InspectorConfig`]: how much telemetry the inspector keeps.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default retention window, one hour.
fn default_retention_ms() -> u64 {
    3_600_000
}

/// Default metric export interval, one second.
fn default_sample_interval_ms() -> u64 {
    1_000
}

fn default_max_spans() -> usize {
    10_000
}

fn default_max_logs() -> usize {
    10_000
}

/// One hour of samples at the default one-second interval.
fn default_max_samples() -> usize {
    3600
}

/// Cap on retained flow-control decisions.
///
/// Two orders of magnitude below the span and log caps, because the buffer
/// holds one record per *episode* per source, not one per guard trip: a
/// source closes at most one adjustment epoch per `adjust_interval_ms` (one
/// minute by default), and however hard a guard is tripping it contributes
/// one record until the target lands somewhere else or the cause changes.
/// A thousand covers a whole hour of retention for more sources than a
/// workflow declares, and [`BufferStats::dropped`](crate::inspector_wire::BufferStats::dropped)
/// reports it if not.
/// `record_flow_decision` in
/// [`runner`](crate::service::runner) owns the episode rule.
fn default_max_flow_decisions() -> usize {
    1_000
}

fn default_true() -> bool {
    true
}

/// How much telemetry the inspector keeps, and whether it runs at all.
///
/// Every field carries a `serde` default, so `[observability.inspector]` can be
/// omitted entirely or given one key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InspectorConfig {
    /// Master switch. `false` installs no layer, attaches no metric reader, and
    /// merges no routes, so `/api/*` and `/ui` return 404.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Whether to serve the dashboard at `/ui`. `false` keeps the JSON API.
    #[serde(default = "default_true")]
    pub ui: bool,
    /// How long a record stays readable, in milliseconds.
    #[serde(default = "default_retention_ms")]
    pub retention_ms: u64,
    /// How often metrics are copied out of the SDK, in milliseconds.
    #[serde(default = "default_sample_interval_ms")]
    pub sample_interval_ms: u64,
    /// Hard cap on retained span records.
    #[serde(default = "default_max_spans")]
    pub max_spans: usize,
    /// Hard cap on retained log records.
    #[serde(default = "default_max_logs")]
    pub max_logs: usize,
    /// Hard cap on retained metric samples.
    #[serde(default = "default_max_samples")]
    pub max_samples: usize,
    /// Hard cap on retained flow-control decisions.
    #[serde(default = "default_max_flow_decisions")]
    pub max_flow_decisions: usize,
}

impl Default for InspectorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ui: true,
            retention_ms: default_retention_ms(),
            sample_interval_ms: default_sample_interval_ms(),
            max_spans: default_max_spans(),
            max_logs: default_max_logs(),
            max_samples: default_max_samples(),
            max_flow_decisions: default_max_flow_decisions(),
        }
    }
}

impl InspectorConfig {
    /// Retention as a [`Duration`].
    pub fn retention(&self) -> Duration {
        Duration::from_millis(self.retention_ms)
    }

    /// Export interval as a [`Duration`], never below one millisecond.
    ///
    /// A zero interval would make the `PeriodicReader` collect in a tight loop.
    pub fn sample_interval(&self) -> Duration {
        Duration::from_millis(self.sample_interval_ms.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_documented_config() {
        let cfg = InspectorConfig::default();
        assert!(cfg.enabled);
        assert!(cfg.ui);
        assert_eq!(cfg.retention_ms, 3_600_000);
        assert_eq!(cfg.sample_interval(), Duration::from_secs(1));
        assert_eq!(
            InspectorConfig {
                sample_interval_ms: 0,
                ..InspectorConfig::default()
            }
            .sample_interval(),
            Duration::from_millis(1),
            "a zero interval must not make the reader spin"
        );
    }

    #[test]
    fn the_duration_keys_are_milliseconds_and_the_seconds_spelling_is_gone() {
        let cfg = InspectorConfig::deserialize(serde_json::json!({
            "retention_ms": 5_000,
            "sample_interval_ms": 250,
        }))
        .expect("the millisecond keys parse");
        assert_eq!(cfg.retention(), Duration::from_millis(5_000));
        assert_eq!(cfg.sample_interval(), Duration::from_millis(250));

        let err = InspectorConfig::deserialize(serde_json::json!({ "retention_secs": 5 }))
            .expect_err("`retention_secs` was replaced by `retention_ms`");
        assert!(err.to_string().contains("retention_secs"), "{err}");
    }
}
