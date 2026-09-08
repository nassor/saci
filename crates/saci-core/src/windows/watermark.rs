//! Watermark tracking for streaming semantics.
//!
//! A watermark is a monotonically increasing timestamp that signals "all events
//! with timestamps earlier than this value have been observed". Event-time window
//! systems use it to decide when a window is complete and can be emitted.
//!
//! [`WatermarkState`] holds the current watermark, the high-water mark across all
//! observed event timestamps, plus an `allowed_lateness` tolerance. A row whose
//! event timestamp falls before `current_watermark - allowed_lateness` is beyond
//! the lateness budget and goes to a side-output instead of being reprocessed.
//!
//! The watermark does **not** advance automatically; callers drive it via
//! [`WatermarkState::advance`].

/// Watermark tracking state for event-time streaming windows.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "windows")]
/// # {
/// use saci_core::windows::watermark::WatermarkState;
///
/// let mut wm = WatermarkState::new(500); // 500 ms of allowed lateness
/// wm.advance(1_000);
/// assert_eq!(wm.current_watermark(), 1_000);
/// assert!(!wm.is_beyond_lateness(600)); // 1000 - 600 = 400 <= 500 → still late but accepted
/// assert!(wm.is_beyond_lateness(400));  // 1000 - 400 = 600 > 500 → dropped
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WatermarkState {
    /// Current watermark: the highest event-timestamp seen so far (ms since epoch).
    ///
    /// Initialised to `i64::MIN` so that the first real timestamp always advances it.
    current_watermark: i64,
    /// Maximum allowed lateness in milliseconds.
    ///
    /// A row with `ts >= current_watermark - allowed_lateness` is still eligible
    /// for late-data re-firing. Anything earlier is beyond the lateness budget.
    allowed_lateness: i64,
}

impl WatermarkState {
    /// Create a new [`WatermarkState`] with the given allowed lateness.
    ///
    /// The initial watermark is `i64::MIN`. `allowed_lateness` is the largest
    /// number of milliseconds a row's timestamp may fall below the current
    /// watermark and still be eligible for late firing. Pass `0` to drop all
    /// out-of-order data immediately.
    pub fn new(allowed_lateness: i64) -> Self {
        Self {
            current_watermark: i64::MIN,
            allowed_lateness,
        }
    }

    /// Advance the watermark to `ts` if `ts` is greater than the current value.
    ///
    /// Watermarks are monotonically non-decreasing; calling this with a value
    /// smaller than the current watermark has no effect.
    pub fn advance(&mut self, ts: i64) {
        if ts > self.current_watermark {
            self.current_watermark = ts;
        }
    }

    /// The current watermark (highest event-timestamp observed so far).
    pub fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    /// The configured allowed-lateness tolerance (milliseconds).
    pub fn allowed_lateness(&self) -> i64 {
        self.allowed_lateness
    }

    /// Returns `true` when `ts` is strictly before the lateness threshold.
    ///
    /// The threshold is `current_watermark - allowed_lateness`. A row below it
    /// should be routed to a side-output or dropped.
    ///
    /// Returns `false` when the watermark has not yet been set (`i64::MIN`).
    pub fn is_beyond_lateness(&self, ts: i64) -> bool {
        if self.current_watermark == i64::MIN {
            return false;
        }
        // allowed_lateness >= current_watermark puts the threshold at or below
        // zero, which is infinite tolerance.
        if self.allowed_lateness >= self.current_watermark {
            return false;
        }
        // Subtraction is safe because allowed_lateness < current_watermark.
        let threshold = self.current_watermark - self.allowed_lateness;
        ts < threshold
    }

    /// Returns `true` when `ts` is below the watermark but still inside the
    /// allowed-lateness window, so eligible for late-data re-firing.
    ///
    /// ```text
    /// current_watermark - allowed_lateness  ≤  ts  <  current_watermark
    /// ```
    pub fn is_late_but_acceptable(&self, ts: i64) -> bool {
        if self.current_watermark == i64::MIN {
            return false;
        }
        ts < self.current_watermark && !self.is_beyond_lateness(ts)
    }

    /// Returns `true` when `ts` arrived on-time (≥ current watermark).
    pub fn is_on_time(&self, ts: i64) -> bool {
        self.current_watermark == i64::MIN || ts >= self.current_watermark
    }
}

/// The host-side watermark for one windowed processor node, as a [`Dataset`](crate::Dataset)
/// resource.
///
/// The service runners track a watermark per processor node whose config
/// declares a `window` block — the maximum event timestamp observed across all
/// of the node's inbound data — and insert it into the batch dataset before
/// calling the runtime. A native pipeline (or any in-process runtime) reads it
/// through [`Dataset::get_resource`](crate::dataset::Dataset::get_resource) to
/// see what the host believes about stream completeness.
///
/// Resources never cross the Arrow IPC boundary, so a WASM processor or a
/// native plugin never sees this value; those runtimes derive their own
/// watermark from the merged input rows instead. The host-side value is still
/// what the dashboard and the `saci_window_watermark_seconds` series report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowWatermark(pub i64);

impl WindowWatermark {
    /// The watermark in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn as_ms(self) -> i64 {
        self.0
    }

    /// The watermark as fractional seconds since the Unix epoch.
    #[must_use]
    pub const fn as_seconds(self) -> f64 {
        self.0 as f64 / 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_watermark_is_min() {
        let wm = WatermarkState::new(1000);
        assert_eq!(wm.current_watermark(), i64::MIN);
    }

    #[test]
    fn test_advance_increases_watermark() {
        let mut wm = WatermarkState::new(0);
        wm.advance(500);
        assert_eq!(wm.current_watermark(), 500);
        wm.advance(300); // backward, no effect
        assert_eq!(wm.current_watermark(), 500);
        wm.advance(1000);
        assert_eq!(wm.current_watermark(), 1000);
    }

    #[test]
    fn test_is_beyond_lateness_no_watermark_set() {
        let wm = WatermarkState::new(100);
        // No watermark yet → nothing is beyond lateness.
        assert!(!wm.is_beyond_lateness(-99999));
        assert!(!wm.is_beyond_lateness(0));
    }

    #[test]
    fn test_is_beyond_lateness_zero_allowed() {
        let mut wm = WatermarkState::new(0);
        wm.advance(1000);
        // threshold = 1000 - 0 = 1000; ts=999 < 1000 → beyond
        assert!(wm.is_beyond_lateness(999));
        // ts=1000 is not beyond (< 1000 is false)
        assert!(!wm.is_beyond_lateness(1000));
    }

    #[test]
    fn test_is_beyond_lateness_with_allowance() {
        let mut wm = WatermarkState::new(500);
        wm.advance(1000);
        // threshold = 1000 - 500 = 500
        assert!(!wm.is_beyond_lateness(500)); // exactly at threshold → not beyond
        assert!(wm.is_beyond_lateness(499));
        assert!(!wm.is_beyond_lateness(600)); // late but within window
        assert!(!wm.is_beyond_lateness(1000)); // on-time
    }

    #[test]
    fn test_is_late_but_acceptable() {
        let mut wm = WatermarkState::new(500);
        wm.advance(1000);
        // late but acceptable: 500 <= ts < 1000
        assert!(wm.is_late_but_acceptable(500));
        assert!(wm.is_late_but_acceptable(999));
        assert!(!wm.is_late_but_acceptable(499)); // beyond lateness
        assert!(!wm.is_late_but_acceptable(1000)); // on time, not late
    }

    #[test]
    fn test_is_on_time() {
        let mut wm = WatermarkState::new(500);
        // Before any advance, everything is on-time.
        assert!(wm.is_on_time(-1000));
        wm.advance(1000);
        assert!(wm.is_on_time(1000));
        assert!(wm.is_on_time(2000));
        assert!(!wm.is_on_time(999)); // late
    }

    #[test]
    fn test_watermark_large_allowed_lateness_does_not_panic() {
        // When allowed_lateness >= current_watermark, no data is beyond lateness
        // (effectively infinite tolerance).

        // Case 1: watermark=100, allowed_lateness=i64::MAX (>> 100).
        let mut wm = WatermarkState::new(i64::MAX);
        wm.advance(100);
        assert!(!wm.is_beyond_lateness(i64::MIN)); // allowed_lateness >= watermark → false
        assert!(!wm.is_beyond_lateness(0));
        assert!(!wm.is_beyond_lateness(100));

        // Case 2: watermark=i64::MAX, allowed_lateness=i64::MAX.
        let mut wm2 = WatermarkState::new(i64::MAX);
        wm2.advance(i64::MAX);
        assert!(!wm2.is_beyond_lateness(-1)); // allowed_lateness >= watermark → false
        assert!(!wm2.is_beyond_lateness(0));
        assert!(!wm2.is_beyond_lateness(i64::MAX));

        // Case 3: Normal case with reasonable values.
        // watermark=1000, allowed_lateness=500 → threshold=500.
        let mut wm3 = WatermarkState::new(500);
        wm3.advance(1000);
        assert!(wm3.is_beyond_lateness(499)); // 499 < 500 → beyond
        assert!(!wm3.is_beyond_lateness(500)); // 500 not < 500 → not beyond
        assert!(!wm3.is_beyond_lateness(1000)); // on-time
    }
}
