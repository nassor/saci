//! The window geometry: tumbling, sliding and session.
//!
//! This is data, not machinery, so it is compiled in every build. A host
//! parses a `window` declaration in every build and answers the capability
//! question separately; the engine that turns a geometry into assigned rows is
//! [`crate::windows`], which the `windows` feature gates.

use crate::error::{SaciError, SaciResult};

/// A window specification defining the geometry and boundaries of windows.
///
/// Serializes as an internally tagged object on the `kind` key
/// (`{"kind":"tumbling","size_ms":30000}`), which is how a KDL `window` node
/// reaches the host configuration and how the service topology describes a
/// windowed processor node. `offset_ms` is optional on the wire and defaults
/// to zero.
///
/// ```rust
/// use saci::window::WindowSpec;
///
/// let spec = WindowSpec::sliding(60_000, 15_000).with_offset(5_000);
/// assert_eq!(spec.kind(), "sliding");
/// assert!(spec.validate().is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowSpec {
    /// Fixed-size, non-overlapping windows.
    /// Each record belongs to exactly one window.
    /// Window boundaries: `floor_div(ts - offset, size)`
    Tumbling {
        /// Window size in milliseconds.
        size_ms: i64,
        /// Alignment offset in milliseconds (default 0).
        #[serde(default, skip_serializing_if = "is_zero")]
        offset_ms: i64,
    },
    /// Gap-based session windows.
    ///
    /// A new session begins whenever the gap between consecutive events within
    /// the same key exceeds `gap_ms` milliseconds. All events in one unbroken
    /// run belong to the same session.
    Session {
        /// Inactivity gap in milliseconds that delimits session boundaries.
        gap_ms: i64,
    },
    /// Overlapping fixed-size windows that advance by a slide interval.
    ///
    /// Each record belongs to `k = ceil(size_ms / slide_ms)` windows.
    /// Window boundaries follow the same floor-division alignment as
    /// tumbling windows but with step size `slide_ms` instead of `size_ms`.
    Sliding {
        /// Window size in milliseconds.
        size_ms: i64,
        /// Slide (advance) interval in milliseconds. Must be ≤ `size_ms`.
        slide_ms: i64,
        /// Alignment offset in milliseconds (default 0).
        #[serde(default, skip_serializing_if = "is_zero")]
        offset_ms: i64,
    },
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

impl WindowSpec {
    /// Tumbling windows of `size_ms`, aligned at zero.
    pub fn tumbling(size_ms: i64) -> Self {
        Self::Tumbling {
            size_ms,
            offset_ms: 0,
        }
    }

    /// Sliding windows of `size_ms` advancing by `slide_ms`, aligned at zero.
    pub fn sliding(size_ms: i64, slide_ms: i64) -> Self {
        Self::Sliding {
            size_ms,
            slide_ms,
            offset_ms: 0,
        }
    }

    /// Session windows closed by an inactivity gap of `gap_ms`.
    pub fn session(gap_ms: i64) -> Self {
        Self::Session { gap_ms }
    }

    /// Shift the window alignment by `offset_ms`. A session window has no
    /// alignment and is returned unchanged.
    pub fn with_offset(self, offset_ms: i64) -> Self {
        match self {
            Self::Tumbling { size_ms, .. } => Self::Tumbling { size_ms, offset_ms },
            Self::Sliding {
                size_ms, slide_ms, ..
            } => Self::Sliding {
                size_ms,
                slide_ms,
                offset_ms,
            },
            session @ Self::Session { .. } => session,
        }
    }

    /// `"tumbling"`, `"sliding"` or `"session"`, the `kind` tag of the serde
    /// shape.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Tumbling { .. } => "tumbling",
            Self::Sliding { .. } => "sliding",
            Self::Session { .. } => "session",
        }
    }

    /// Whether the geometry is sane enough to build windows from.
    ///
    /// Checks what the window maths needs to stay well-defined: a strictly
    /// positive `size_ms`, a strictly positive `slide_ms` no larger than
    /// `size_ms`, and a strictly positive session `gap_ms`. The host runs this
    /// once at config load and `WindowedSystem` at construction, so a nonsense
    /// geometry is a configuration error, not a division by zero at run time.
    ///
    /// # Errors
    ///
    /// [`SaciError::Configuration`] naming the offending value.
    pub fn validate(&self) -> SaciResult<()> {
        let problem = match self {
            Self::Tumbling { size_ms, .. } if *size_ms <= 0 => {
                format!("tumbling window size_ms must be > 0, got {size_ms}")
            }
            Self::Session { gap_ms } if *gap_ms <= 0 => {
                format!("session window gap_ms must be > 0, got {gap_ms}")
            }
            Self::Sliding { size_ms, .. } if *size_ms <= 0 => {
                format!("sliding window size_ms must be > 0, got {size_ms}")
            }
            Self::Sliding { slide_ms, .. } if *slide_ms <= 0 => {
                format!("sliding window slide_ms must be > 0, got {slide_ms}")
            }
            Self::Sliding {
                size_ms, slide_ms, ..
            } if slide_ms > size_ms => {
                format!("sliding window slide_ms ({slide_ms}) must be <= size_ms ({size_ms})")
            }
            _ => return Ok(()),
        };
        Err(SaciError::configuration(problem))
    }
}

/// The tumbling window id of `ts` (milliseconds): `floor((ts - offset_ms) /
/// size_ms)`.
///
/// Uses true floor division. Standard `/` truncates toward zero, so
/// `-5 / 3 = -1` where `floor(-5 / 3) = -2`.
pub fn tumbling_id(ts: i64, size_ms: i64, offset_ms: i64) -> i64 {
    (ts - offset_ms).div_euclid(size_ms)
}

/// The `k = ceil(size_ms / slide_ms)` sliding window ids that contain `ts`.
///
/// Each window of a sliding specification is identified by the tumbling
/// window id computed with step size `slide_ms`:
///
/// ```text
/// window_id[j] = tumbling_id(ts - j * slide_ms, slide_ms, offset_ms)
///                for j in 0..k
/// ```
///
/// Yields exactly `k` ids, newest first. The caller must ensure
/// `slide_ms > 0`.
pub fn sliding_ids(
    ts: i64,
    size_ms: i64,
    slide_ms: i64,
    offset_ms: i64,
) -> impl Iterator<Item = i64> {
    let k = (size_ms + slide_ms - 1) / slide_ms;
    (0..k).map(move |j| tumbling_id(ts - j * slide_ms, slide_ms, offset_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_assign_positive_ts() {
        assert_eq!(tumbling_id(1500, 1000, 0), 1);
    }

    #[test]
    fn test_tumbling_assign_with_offset() {
        // floor((1500 - 500) / 1000) = 1, floor((1200 - 500) / 1000) = 0
        assert_eq!(tumbling_id(1500, 1000, 500), 1);
        assert_eq!(tumbling_id(1200, 1000, 500), 0);
    }

    #[test]
    fn test_tumbling_assign_negative_ts() {
        // floor(-1.5) = -2, floor(-0.5) = -1
        assert_eq!(tumbling_id(-1500, 1000, 0), -2);
        assert_eq!(tumbling_id(-500, 1000, 0), -1);
    }

    #[test]
    fn test_tumbling_assign_boundary() {
        assert_eq!(tumbling_id(0, 1000, 0), 0);
        assert_eq!(tumbling_id(999, 1000, 0), 0);
        assert_eq!(tumbling_id(1000, 1000, 0), 1);
        assert_eq!(tumbling_id(1999, 1000, 0), 1);
        assert_eq!(tumbling_id(2000, 1000, 0), 2);
    }

    fn ids(ts: i64, size_ms: i64, slide_ms: i64, offset_ms: i64) -> Vec<i64> {
        sliding_ids(ts, size_ms, slide_ms, offset_ms).collect()
    }

    /// size=10, slide=5 → k=2; each record belongs to exactly 2 windows.
    #[test]
    fn test_assign_sliding_basic() {
        assert_eq!(ids(7, 10, 5, 0), [1, 0]);
    }

    /// size=15, slide=5 → k=3.
    #[test]
    fn test_assign_sliding_k_three() {
        assert_eq!(ids(12, 15, 5, 0), [2, 1, 0]);
    }

    /// size == slide → k=1, identical to tumbling.
    #[test]
    fn test_assign_sliding_equals_tumbling_when_size_eq_slide() {
        assert_eq!(ids(1500, 1000, 1000, 0), [tumbling_id(1500, 1000, 0)]);
    }

    /// With offset: same floor-division shift as tumbling.
    #[test]
    fn test_assign_sliding_with_offset() {
        assert_eq!(ids(600, 1000, 500, 100), [1, 0]);
    }

    /// Negative timestamps use floor division, same as tumbling.
    #[test]
    fn test_assign_sliding_negative_ts() {
        assert_eq!(ids(-3, 10, 5, 0), [-1, -2]);
    }

    /// Non-divisible size/slide: k rounds up correctly.
    #[test]
    fn test_assign_sliding_non_divisible_ceil() {
        assert_eq!(ids(5, 7, 3, 0).len(), 3);
    }

    #[test]
    fn constructors_build_the_variants() {
        assert_eq!(
            WindowSpec::tumbling(1000).with_offset(5),
            WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 5
            }
        );
        assert_eq!(
            WindowSpec::sliding(10, 5),
            WindowSpec::Sliding {
                size_ms: 10,
                slide_ms: 5,
                offset_ms: 0
            }
        );
        assert_eq!(
            WindowSpec::session(30).with_offset(9),
            WindowSpec::Session { gap_ms: 30 }
        );
        assert_eq!(WindowSpec::session(30).kind(), "session");
    }

    #[test]
    fn validate_accepts_sane_geometries() {
        WindowSpec::tumbling(1000)
            .validate()
            .expect("positive size");
        WindowSpec::session(500).validate().expect("positive gap");
        WindowSpec::sliding(10_000, 5_000)
            .validate()
            .expect("size >= slide");
    }

    #[test]
    fn validate_rejects_nonsense_geometries() {
        assert!(WindowSpec::tumbling(0).validate().is_err());
        assert!(WindowSpec::tumbling(-100).validate().is_err());
        assert!(WindowSpec::session(0).validate().is_err());
        assert!(WindowSpec::sliding(1_000, 0).validate().is_err());
        let err = WindowSpec::sliding(1_000, 2_000).validate().unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.to_string().contains("slide_ms"), "got: {err}");
    }

    /// The serde shape is what the KDL `window` node and the topology use:
    /// internally tagged on `kind`, snake_case variants, `offset_ms` optional.
    #[test]
    fn spec_round_trips_through_its_internally_tagged_shape() {
        let spec = WindowSpec::tumbling(30_000);
        let json = serde_json::to_string(&spec).expect("serialize");
        assert!(
            json.contains(r#""kind":"tumbling""#) && json.contains(r#""size_ms":30000"#),
            "got: {json}"
        );
        let back: WindowSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, spec);

        let sliding: WindowSpec =
            serde_json::from_str(r#"{"kind":"sliding","size_ms":60000,"slide_ms":10000}"#)
                .expect("offset_ms is optional");
        assert_eq!(sliding, WindowSpec::sliding(60_000, 10_000));
    }
}
