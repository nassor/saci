//! The `observability` block: logging, tracing and the inspector.

use serde::{Deserialize, Serialize};

use crate::error::{SaciError, SaciResult};

/// Log output format.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-readable output for TTY / development.
    #[default]
    Pretty,
    /// Structured JSON for production log aggregators.
    Json,
}

fn default_log_level() -> String {
    "error".to_string()
}

fn default_sample_ratio() -> f64 {
    1.0
}

/// Observability (logging) configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Log output format.
    #[serde(default)]
    pub log_format: LogFormat,
    /// Tracing level filter string (`"info"`, `"debug"`, etc.).
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// OTLP/HTTP collector base URL, for example
    /// `http://127.0.0.1:4318`. `None` disables span export.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Fraction of spans and events below ERROR that `log_level` admits and
    /// the subscriber keeps, 0.0 to 1.0. Decided once per root span or root
    /// event; children follow their root.
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
    /// Fraction of ERROR-level spans and events kept, 0.0 to 1.0. Independent
    /// of `sample_ratio`: an error inside a trace `sample_ratio` dropped is
    /// still rolled against this ratio.
    #[serde(default = "default_sample_ratio")]
    pub error_sample_ratio: f64,
    /// In-process telemetry capture, its JSON API and the `/ui` dashboard.
    #[serde(default)]
    pub inspector: crate::inspector::InspectorConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Pretty,
            log_level: default_log_level(),
            otlp_endpoint: None,
            sample_ratio: default_sample_ratio(),
            error_sample_ratio: default_sample_ratio(),
            inspector: crate::inspector::InspectorConfig::default(),
        }
    }
}

impl ObservabilityConfig {
    /// Validate the two sampling ratios.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the first key whose value
    /// falls outside `0.0..=1.0`. NaN fails the range check too, so it is
    /// rejected rather than silently treated as "never".
    pub fn validate(&self) -> SaciResult<()> {
        for (key, value) in [
            ("sample_ratio", self.sample_ratio),
            ("error_sample_ratio", self.error_sample_ratio),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(SaciError::configuration(format!(
                    "observability.{key} must be between 0.0 and 1.0"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use crate::service::config::test_support::{minimal_standalone_kdl, parse};

    /// A sampling ratio outside `0.0..=1.0` fails validation naming its own
    /// key, so an operator sees which of the two is wrong.
    #[test]
    fn test_out_of_range_sampling_ratios_are_rejected() {
        let base = minimal_standalone_kdl();

        let too_large = parse(&format!("{base}\nobservability sample_ratio=1.5\n")).expect("parse");
        let err = too_large.validate().expect_err("1.5 is not a ratio");
        assert!(
            err.message().contains("observability.sample_ratio"),
            "error should name the key: {err}"
        );

        let negative =
            parse(&format!("{base}\nobservability error_sample_ratio=-0.1\n")).expect("parse");
        let err = negative.validate().expect_err("-0.1 is not a ratio");
        assert!(
            err.message().contains("observability.error_sample_ratio"),
            "error should name the key: {err}"
        );
    }
}
