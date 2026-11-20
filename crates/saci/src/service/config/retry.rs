//! The `retry` block of a source or sink node.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::SaciError;
use crate::retry::RetryMode;

/// Default total attempts (initial + retries) for a source or sink node.
fn default_retry_attempts() -> u32 {
    4
}
/// Default delay before the second attempt.
fn default_retry_base_ms() -> u64 {
    100
}
/// Default growth factor applied per attempt.
fn default_retry_mult() -> f64 {
    2.0
}
/// Default ceiling on the computed delay.
fn default_retry_max_ms() -> u64 {
    30_000
}
/// Default fraction of the delay randomised.
fn default_retry_jitter() -> f64 {
    0.1
}
/// Retry policy for a source or sink node.
///
/// An omitted `retry` block retries a connector call 3 times (4 attempts)
/// with 100 ms base delay, 2.0x multiplier, 30 s cap and 0.1 jitter, the
/// parameters of `RetryMode::exponential(3)`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Total attempts including the initial one; 1 disables retrying.
    #[serde(default = "default_retry_attempts")]
    pub max_attempts: u32,
    /// Delay before the second attempt.
    #[serde(default = "default_retry_base_ms")]
    pub base_delay_ms: u64,
    /// Growth factor applied per attempt.
    #[serde(default = "default_retry_mult")]
    pub multiplier: f64,
    /// Ceiling on the computed delay.
    #[serde(default = "default_retry_max_ms")]
    pub max_delay_ms: u64,
    /// Fraction of the delay randomised, in `0.0..=1.0`.
    #[serde(default = "default_retry_jitter")]
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_retry_attempts(),
            base_delay_ms: default_retry_base_ms(),
            multiplier: default_retry_mult(),
            max_delay_ms: default_retry_max_ms(),
            jitter: default_retry_jitter(),
        }
    }
}

impl RetryConfig {
    pub(super) fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.max_attempts < 1 {
            return Err(SaciError::configuration(format!(
                "{what}: retry.max_attempts must be at least 1"
            )));
        }
        if self.multiplier < 1.0 || self.multiplier.is_nan() {
            return Err(SaciError::configuration(format!(
                "{what}: retry.multiplier must be at least 1.0, got {}",
                self.multiplier
            )));
        }
        if !(0.0..=1.0).contains(&self.jitter) {
            return Err(SaciError::configuration(format!(
                "{what}: retry.jitter must be within 0.0..=1.0, got {}",
                self.jitter
            )));
        }
        Ok(())
    }

    /// The engine's retry policy this config drives.
    pub fn to_retry_mode(&self) -> RetryMode {
        if self.max_attempts == 1 {
            RetryMode::None
        } else {
            RetryMode::ExponentialBackoff {
                max_retries: (self.max_attempts - 1) as usize,
                base_delay: Duration::from_millis(self.base_delay_ms),
                multiplier: self.multiplier,
                max_delay: Duration::from_millis(self.max_delay_ms),
                jitter: self.jitter,
            }
        }
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::service::config::test_support::{minimal_standalone_kdl, parse};

    // ------------------------------------------------------------ retry

    /// A standalone workflow whose source node carries `retry` as a child
    /// node, so a test can vary the retry block's properties.
    fn source_retry_kdl(retry: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

workflow "w" {{
    source "in" connector="noop" component="X" {{
        {retry}
    }}
    sink "out" connector="noop" component="X"
    link from="in" to="out"
}}
"#
        )
    }

    fn assert_exponential_backoff(
        mode: RetryMode,
        max_retries: usize,
        base_delay_ms: u64,
        multiplier: f64,
        max_delay_ms: u64,
        jitter: f64,
    ) {
        match mode {
            RetryMode::ExponentialBackoff {
                max_retries: got_retries,
                base_delay,
                multiplier: got_multiplier,
                max_delay,
                jitter: got_jitter,
            } => {
                assert_eq!(got_retries, max_retries);
                assert_eq!(base_delay, Duration::from_millis(base_delay_ms));
                assert_eq!(got_multiplier, multiplier);
                assert_eq!(max_delay, Duration::from_millis(max_delay_ms));
                assert_eq!(got_jitter, jitter);
            }
            other => panic!("expected ExponentialBackoff, got {other:?}"),
        }
    }

    #[test]
    fn retry_defaults_when_the_block_is_omitted() {
        let cfg = parse(&minimal_standalone_kdl()).expect("parse should succeed");
        let wf = &cfg.workflows[0];
        assert_eq!(wf.sources[0].retry, RetryConfig::default());
        assert_eq!(wf.sinks[0].retry, RetryConfig::default());
        assert_exponential_backoff(
            wf.sources[0].retry.to_retry_mode(),
            3,
            100,
            2.0,
            30_000,
            0.1,
        );
    }

    #[test]
    fn a_retry_block_parses() {
        let cfg = parse(&source_retry_kdl(
            "retry max_attempts=8 base_delay_ms=500 multiplier=2.0 max_delay_ms=10000 jitter=0.1",
        ))
        .expect("parse should succeed");
        let retry = &cfg.workflows[0].sources[0].retry;
        assert_eq!(retry.max_attempts, 8);
        assert_eq!(retry.base_delay_ms, 500);
        assert_eq!(retry.multiplier, 2.0);
        assert_eq!(retry.max_delay_ms, 10_000);
        assert_eq!(retry.jitter, 0.1);
    }

    #[test]
    fn max_attempts_one_disables_retrying() {
        let cfg = parse(&source_retry_kdl("retry max_attempts=1")).expect("parse should succeed");
        let mode = cfg.workflows[0].sources[0].retry.to_retry_mode();
        assert!(matches!(mode, RetryMode::None));
        assert_eq!(mode.max_attempts(), 1);
    }

    #[test]
    fn custom_params_map_to_an_exponential_backoff() {
        let cfg = parse(&source_retry_kdl(
            "retry max_attempts=8 base_delay_ms=500 multiplier=2.0 max_delay_ms=10000 jitter=0.1",
        ))
        .expect("parse should succeed");
        let sys = cfg.workflows[0].sources[0].retry.to_retry_mode();
        assert_exponential_backoff(sys, 7, 500, 2.0, 10_000, 0.1);
    }

    #[test]
    fn retry_max_attempts_zero_is_a_configuration_error() {
        let cfg = parse(&source_retry_kdl("retry max_attempts=0")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("retry.max_attempts must be at least 1"),
            "got: {err}"
        );
        assert!(err.contains("source 'in'"), "got: {err}");
    }

    #[test]
    fn retry_multiplier_below_one_is_a_configuration_error() {
        let cfg = parse(&source_retry_kdl("retry multiplier=0.5")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("retry.multiplier must be at least 1.0, got 0.5"),
            "got: {err}"
        );
    }

    #[test]
    fn retry_jitter_out_of_range_is_a_configuration_error() {
        let cfg = parse(&source_retry_kdl("retry jitter=1.5")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("retry.jitter must be within 0.0..=1.0, got 1.5"),
            "got: {err}"
        );
    }
}
