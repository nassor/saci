//! Retry strategies and the drivers that apply them.
//!
//! [`RetryMode`] picks the strategy: `None` fails on the first error, `Fixed`
//! retries a set number of times with a constant delay, and
//! `ExponentialBackoff` grows the delay and can add jitter. [`SystemConfig`]
//! wraps a mode and adds builder methods. `Pipeline`'s stage executor runs
//! every system through [`run_with_retries`] or
//! [`run_with_retries_blocking`].
//!
//! ## Example
//!
//! ```rust
//! use saci_core::retry::{RetryMode, SystemConfig};
//! use std::time::Duration;
//!
//! let config = SystemConfig::new()
//!     .with_exponential_retry(3);
//! assert_eq!(config.retry_mode.max_attempts(), 4);
//! ```

use crate::error::SaciError;
use rand::RngExt;
use std::time::Duration;
#[cfg(feature = "tracing")]
use tracing::{Instrument, error, info_span, warn};

/// Retry strategy for task execution.
#[derive(Debug, Clone, Copy)]
pub enum RetryMode {
    /// Fail immediately on the first error.
    None,

    /// Fixed number of retries with constant delay
    ///
    /// # Fields
    /// - `retries`: Number of retry attempts
    /// - `delay`: Fixed delay between attempts
    Fixed { retries: usize, delay: Duration },

    /// Exponential backoff with optional jitter
    ///
    /// Implements exponential backoff: delay = base_delay * multiplier^attempt + jitter
    ///
    /// # Fields
    /// - `max_retries`: Maximum number of retry attempts
    /// - `base_delay`: Initial delay duration
    /// - `multiplier`: Exponential multiplier (typically 2.0)
    /// - `max_delay`: Maximum delay cap to prevent excessive waits
    /// - `jitter`: Add randomness to prevent thundering herd (0.0 to 1.0)
    ExponentialBackoff {
        max_retries: usize,
        base_delay: Duration,
        multiplier: f64,
        max_delay: Duration,
        jitter: f64,
    },
}

impl RetryMode {
    /// Create a fixed retry mode with specified retries and delay
    pub fn fixed(retries: usize, delay: Duration) -> Self {
        Self::Fixed { retries, delay }
    }

    /// Create an exponential backoff retry mode with sensible defaults
    ///
    /// Uses base_delay=100ms, multiplier=2.0, max_delay=30s, jitter=0.1
    pub fn exponential(max_retries: usize) -> Self {
        Self::ExponentialBackoff {
            max_retries,
            base_delay: Duration::from_millis(100),
            multiplier: 2.0,
            max_delay: Duration::from_secs(30),
            jitter: 0.1,
        }
    }

    /// Create a custom exponential backoff retry mode
    pub fn exponential_custom(
        max_retries: usize,
        base_delay: Duration,
        multiplier: f64,
        max_delay: Duration,
        jitter: f64,
    ) -> Self {
        Self::ExponentialBackoff {
            max_retries,
            base_delay,
            multiplier,
            max_delay,
            jitter: jitter.clamp(0.0, 1.0),
        }
    }

    /// Get the maximum number of attempts (initial + retries)
    pub fn max_attempts(&self) -> usize {
        match self {
            Self::None => 1,
            Self::Fixed { retries, .. } => retries + 1,
            Self::ExponentialBackoff { max_retries, .. } => max_retries + 1,
        }
    }

    /// Calculate delay for a specific attempt number (0-based)
    ///
    /// Returns `None` when attempts are exhausted or when no delay applies.
    pub fn delay_for_attempt(&self, attempt: usize) -> Option<Duration> {
        match self {
            Self::None => None,
            Self::Fixed { retries, delay } => {
                if attempt < *retries {
                    Some(*delay)
                } else {
                    None
                }
            }
            Self::ExponentialBackoff {
                max_retries,
                base_delay,
                multiplier,
                max_delay,
                jitter,
            } => {
                if attempt < *max_retries {
                    let base_ms = base_delay.as_millis() as f64;
                    let exponential_delay = base_ms * multiplier.powi(attempt as i32);
                    let capped_delay = exponential_delay.min(max_delay.as_millis() as f64);

                    // Add jitter: delay * (1 ± jitter * random_factor)
                    let jitter_factor = if *jitter > 0.0 {
                        let mut rng = rand::rng();
                        let random_factor: f64 = rng.random_range(-1.0..=1.0);
                        1.0 + (jitter * random_factor)
                    } else {
                        1.0
                    };

                    let final_delay = (capped_delay * jitter_factor).max(0.0) as u64;
                    Some(Duration::from_millis(final_delay))
                } else {
                    None
                }
            }
        }
    }
}

impl Default for RetryMode {
    fn default() -> Self {
        Self::ExponentialBackoff {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            multiplier: 2.0,
            max_delay: Duration::from_secs(30),
            jitter: 0.1,
        }
    }
}

/// System configuration for retry behavior
///
/// Wraps a [`RetryMode`] and exposes builder methods for common configurations.
///
/// # Example
///
/// ```rust
/// use saci_core::retry::{RetryMode, SystemConfig};
/// use std::time::Duration;
///
/// // Default: exponential backoff with 3 retries
/// let config = SystemConfig::new();
/// assert_eq!(config.retry_mode.max_attempts(), 4);
///
/// // No retries
/// let minimal = SystemConfig::minimal();
/// assert_eq!(minimal.retry_mode.max_attempts(), 1);
///
/// // Fixed retry
/// let fixed = SystemConfig::new()
///     .with_fixed_retry(2, Duration::from_millis(50));
/// assert_eq!(fixed.retry_mode.max_attempts(), 3);
/// ```
#[must_use]
#[derive(Clone, Copy, Default)]
pub struct SystemConfig {
    /// Retry strategy for failed executions
    pub retry_mode: RetryMode,
}

impl SystemConfig {
    /// Create a new `SystemConfig` with default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a minimal configuration with no retries
    ///
    /// Useful for tasks that should fail fast without any retry attempts.
    pub fn minimal() -> Self {
        Self {
            retry_mode: RetryMode::None,
        }
    }

    /// Set the retry mode for this configuration
    pub fn with_retry(mut self, retry_mode: RetryMode) -> Self {
        self.retry_mode = retry_mode;
        self
    }

    /// Convenience method for fixed retry configuration
    pub fn with_fixed_retry(self, retries: usize, delay: Duration) -> Self {
        self.with_retry(RetryMode::fixed(retries, delay))
    }

    /// Convenience method for exponential backoff retry configuration
    pub fn with_exponential_retry(self, max_retries: usize) -> Self {
        self.with_retry(RetryMode::exponential(max_retries))
    }
}

/// Attempt bookkeeping shared by [`run_with_retries`] and
/// [`run_with_retries_blocking`].
///
/// The two drivers differ only in how they wait, so the attempt counter, the
/// exhaustion check, the
/// [`SaciError::RetryExhausted`](crate::SaciError::RetryExhausted) construction
/// and the delay lookup all live here once.
struct Attempts<'a> {
    config: &'a SystemConfig,
    max_attempts: usize,
    attempt: usize,
}

impl<'a> Attempts<'a> {
    fn new(config: &'a SystemConfig) -> Self {
        Self {
            config,
            max_attempts: config.retry_mode.max_attempts(),
            attempt: 0,
        }
    }

    /// Number of retries consumed so far (`0` = the first attempt succeeded).
    fn retries(&self) -> u32 {
        self.attempt as u32
    }

    /// The span covering the attempt about to run, or a disabled span for the
    /// first attempt.
    ///
    /// Innermost of the four spans `saci-core` opens, below `system.execute`: a
    /// stage that looks slow is usually a system being retried. A first attempt
    /// carries no such explanation, and a subscriber that captures every span
    /// pays for one per operation whatever the outcome, so the first attempt
    /// runs under [`Span::none`](tracing::Span::none) and only a retry opens a
    /// recorded span.
    #[cfg(feature = "tracing")]
    fn span(&self) -> tracing::Span {
        if self.attempt == 0 {
            return tracing::Span::none();
        }
        info_span!(
            "task_attempt",
            attempt = self.attempt + 1,
            max_attempts = self.max_attempts
        )
    }

    /// Fold a failed attempt.
    ///
    /// `Ok(Some(delay))`: wait that long, then retry.
    /// `Ok(None)`: retry immediately, the mode declares no delay.
    /// `Err(_)`: attempts exhausted, `e` is wrapped in `RetryExhausted`.
    fn failed(&mut self, e: SaciError) -> Result<Option<Duration>, SaciError> {
        self.attempt += 1;
        if self.attempt >= self.max_attempts {
            #[cfg(feature = "tracing")]
            error!(
                error = %e,
                attempts = self.attempt,
                "retry exhausted, giving up"
            );
            return Err(SaciError::retry_exhausted(e, self.attempt));
        }
        let delay = self.config.retry_mode.delay_for_attempt(self.attempt - 1);
        #[cfg(feature = "tracing")]
        warn!(
            error = %e,
            attempt = self.attempt,
            max_attempts = self.max_attempts,
            delay_ms = delay.unwrap_or_default().as_millis(),
            "attempt failed, retrying"
        );
        Ok(delay)
    }
}

/// Execute a fallible async operation with retry logic from [`SystemConfig`].
///
/// Runs `run_fn` up to `config.retry_mode.max_attempts()` times. Returns
/// `(value, retries)` on the first success, where `retries` is `0` if the first
/// attempt succeeded, or
/// [`SaciError::RetryExhausted`] once every
/// attempt is consumed.
///
/// Waits with `tokio::time::sleep` under the `runtime` feature. Processor builds
/// have no async timer, so delays are skipped and retries are immediate; use
/// [`run_with_retries_blocking`] from a blocking thread.
///
/// Each retry after the first attempt runs inside a `task_attempt` span
/// carrying `attempt` and `max_attempts` under the `tracing` feature; a first
/// attempt that succeeds opens no span.
///
/// # Example
///
/// ```rust,no_run,ignore
/// use saci_core::error::SaciError;
/// use saci_core::retry::{SystemConfig, run_with_retries};
///
/// # #[tokio::main]
/// # async fn main() {
/// let config = SystemConfig::minimal();
/// let (value, retries) =
///     run_with_retries(&config, async || Ok::<&str, SaciError>("done"))
///         .await
///         .unwrap();
/// assert_eq!(value, "done");
/// assert_eq!(retries, 0);
/// # }
/// ```
pub async fn run_with_retries<T>(
    config: &SystemConfig,
    mut run_fn: impl AsyncFnMut() -> Result<T, SaciError>,
) -> Result<(T, u32), SaciError> {
    let mut attempts = Attempts::new(config);
    loop {
        // The span must wrap the future rather than be `enter`ed around an
        // await: a guard held across a yield attributes another task's work to
        // this attempt.
        #[cfg(feature = "tracing")]
        let result = run_fn().instrument(attempts.span()).await;
        #[cfg(not(feature = "tracing"))]
        let result = run_fn().await;

        match result {
            Ok(value) => return Ok((value, attempts.retries())),
            Err(e) => {
                if let Some(delay) = attempts.failed(e)? {
                    #[cfg(feature = "runtime")]
                    tokio::time::sleep(delay).await;
                    // Processor builds have no timer; retry immediately.
                    #[cfg(not(feature = "runtime"))]
                    let _ = delay;
                }
            }
        }
    }
}

/// [`run_with_retries`] for a synchronous operation already running on a
/// blocking thread.
///
/// Waits with `std::thread::sleep`, which is correct there and would stall the
/// async executor anywhere else.
pub fn run_with_retries_blocking<T>(
    config: &SystemConfig,
    mut run_fn: impl FnMut() -> Result<T, SaciError>,
) -> Result<(T, u32), SaciError> {
    let mut attempts = Attempts::new(config);
    loop {
        // No await here, so a plain guard is correct.
        #[cfg(feature = "tracing")]
        let span = attempts.span();
        #[cfg(feature = "tracing")]
        let guard = span.enter();

        let result = run_fn();

        #[cfg(feature = "tracing")]
        drop(guard);

        match result {
            Ok(value) => return Ok((value, attempts.retries())),
            Err(e) => {
                if let Some(delay) = attempts.failed(e)? {
                    std::thread::sleep(delay);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "runtime")]
    use std::sync::Arc;
    #[cfg(feature = "runtime")]
    use std::sync::atomic::{AtomicUsize, Ordering};

    // ── RetryMode tests ──────────────────────────────────────────────────────

    #[test]
    fn test_retry_mode_none_max_attempts_is_one() {
        assert_eq!(RetryMode::None.max_attempts(), 1);
    }

    #[test]
    fn test_retry_mode_fixed_max_attempts() {
        let mode = RetryMode::fixed(3, Duration::from_millis(10));
        assert_eq!(mode.max_attempts(), 4);
    }

    #[test]
    fn test_retry_mode_fixed_returns_correct_delay() {
        let delay = Duration::from_millis(50);
        let mode = RetryMode::fixed(2, delay);
        assert_eq!(mode.delay_for_attempt(0), Some(delay));
        assert_eq!(mode.delay_for_attempt(1), Some(delay));
        assert_eq!(mode.delay_for_attempt(2), None); // exhausted
    }

    #[test]
    fn test_retry_mode_exponential_max_attempts() {
        let mode = RetryMode::exponential(5);
        assert_eq!(mode.max_attempts(), 6);
    }

    #[test]
    fn test_retry_mode_exponential_returns_increasing_delays() {
        // Use zero jitter so delays are deterministic
        let mode = RetryMode::exponential_custom(
            4,
            Duration::from_millis(100),
            2.0,
            Duration::from_secs(30),
            0.0,
        );
        let d0 = mode.delay_for_attempt(0).unwrap();
        let d1 = mode.delay_for_attempt(1).unwrap();
        let d2 = mode.delay_for_attempt(2).unwrap();
        assert!(d1 > d0, "delay should grow: {d0:?} < {d1:?}");
        assert!(d2 > d1, "delay should grow: {d1:?} < {d2:?}");
    }

    #[test]
    fn test_delay_for_attempt_returns_none_when_exhausted() {
        let mode = RetryMode::fixed(2, Duration::from_millis(10));
        // attempt index 2 is beyond the 2 retries allowed
        assert_eq!(mode.delay_for_attempt(2), None);
    }

    #[test]
    fn test_retry_mode_none_delay_is_none() {
        assert_eq!(RetryMode::None.delay_for_attempt(0), None);
    }

    #[test]
    fn test_retry_mode_exponential_max_delay_cap() {
        let mode = RetryMode::exponential_custom(
            10,
            Duration::from_millis(1000),
            10.0,
            Duration::from_millis(500), // cap below what exponent would produce
            0.0,
        );
        for i in 0..10 {
            if let Some(d) = mode.delay_for_attempt(i) {
                assert!(
                    d <= Duration::from_millis(500),
                    "attempt {i}: delay {d:?} exceeds cap"
                );
            }
        }
    }

    // ── SystemConfig tests ───────────────────────────────────────────────────

    #[test]
    fn test_system_config_minimal_has_no_retries() {
        let cfg = SystemConfig::minimal();
        assert_eq!(cfg.retry_mode.max_attempts(), 1);
    }

    #[test]
    fn test_system_config_default_has_exponential_backoff() {
        let cfg = SystemConfig::new();
        // Default is ExponentialBackoff with max_retries=3 → 4 attempts
        assert_eq!(cfg.retry_mode.max_attempts(), 4);
    }

    #[test]
    fn test_system_config_with_fixed_retry() {
        let cfg = SystemConfig::new().with_fixed_retry(2, Duration::from_millis(10));
        assert_eq!(cfg.retry_mode.max_attempts(), 3);
    }

    #[test]
    fn test_system_config_with_exponential_retry() {
        let cfg = SystemConfig::new().with_exponential_retry(5);
        assert_eq!(cfg.retry_mode.max_attempts(), 6);
    }

    #[test]
    fn test_system_config_with_retry_builder() {
        let cfg = SystemConfig::new().with_retry(RetryMode::None);
        assert_eq!(cfg.retry_mode.max_attempts(), 1);
    }

    // ── retry driver tests ───────────────────────────────────────────────────

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_run_with_retries_succeeds_on_first_try() {
        let config = SystemConfig::minimal();
        let result = run_with_retries(&config, async || Ok::<&str, SaciError>("success")).await;
        assert_eq!(result.unwrap(), ("success", 0));
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_run_with_retries_retries_on_failure() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let config = SystemConfig::new().with_fixed_retry(2, Duration::ZERO);

        let cc = Arc::clone(&call_count);
        let result = run_with_retries(&config, async || {
            let n = cc.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(SaciError::generic("transient"))
            } else {
                Ok("recovered")
            }
        })
        .await;

        assert_eq!(
            result.unwrap(),
            ("recovered", 2),
            "two retries preceded the success"
        );
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_run_with_retries_returns_retry_exhausted_after_max_attempts() {
        let config = SystemConfig::new().with_fixed_retry(2, Duration::ZERO);

        let result = run_with_retries(&config, async || {
            Err::<(), SaciError>(SaciError::generic("always fails"))
        })
        .await;

        let err = result.unwrap_err();
        assert!(
            matches!(err, SaciError::RetryExhausted { .. }),
            "expected RetryExhausted, got {err:?}"
        );
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_run_with_retries_no_retry_on_first_failure() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let config = SystemConfig::minimal();

        let cc = Arc::clone(&call_count);
        let result = run_with_retries(&config, async || {
            cc.fetch_add(1, Ordering::SeqCst);
            Err::<(), SaciError>(SaciError::generic("fail"))
        })
        .await;

        assert!(matches!(
            result.unwrap_err(),
            SaciError::RetryExhausted { .. }
        ));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_retry_exhausted_preserves_source_error_and_attempt_count() {
        // 2 retries → 3 total attempts
        let config = SystemConfig::new().with_fixed_retry(2, Duration::ZERO);

        let result = run_with_retries(&config, async || {
            Err::<(), SaciError>(SaciError::system_execution("root cause"))
        })
        .await;

        match result.unwrap_err() {
            SaciError::RetryExhausted { source, attempts } => {
                assert_eq!(attempts, 3, "expected 3 attempts");
                assert!(
                    matches!(*source, SaciError::SystemExecution(_)),
                    "source should be SystemExecution, got {source:?}"
                );
                assert_eq!(source.message(), "root cause");
            }
            other => panic!("expected RetryExhausted, got {other:?}"),
        }
    }

    /// The blocking driver counts attempts and builds the exhaustion error
    /// exactly like the async one; both share `Attempts`.
    #[test]
    fn test_run_with_retries_blocking_matches_the_async_driver() {
        let config = SystemConfig::new().with_fixed_retry(2, Duration::ZERO);

        let mut calls = 0usize;
        let ok = run_with_retries_blocking(&config, || {
            calls += 1;
            if calls < 3 {
                Err(SaciError::generic("transient"))
            } else {
                Ok("recovered")
            }
        });
        assert_eq!(ok.unwrap(), ("recovered", 2));
        assert_eq!(calls, 3);

        let err = run_with_retries_blocking(&config, || {
            Err::<(), SaciError>(SaciError::system_execution("root cause"))
        })
        .unwrap_err();
        match err {
            SaciError::RetryExhausted { source, attempts } => {
                assert_eq!(attempts, 3);
                assert_eq!(source.message(), "root cause");
            }
            other => panic!("expected RetryExhausted, got {other:?}"),
        }
    }
}
