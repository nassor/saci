//! The window geometry enum: tumbling, sliding and session.
//!
//! This is data, not machinery, so it lives outside the `windows` feature.
//! A host parses a `window` declaration in every build and answers the
//! capability question separately; the engine that turns a geometry into
//! assigned rows is [`crate::windows`], which the feature gates.

/// A window specification defining the geometry and boundaries of windows.
///
/// Serializes as an internally tagged object on the `kind` key
/// (`{"kind":"tumbling","size_ms":30000}`), which is how a KDL `window` node
/// reaches the host configuration and how the service topology describes a
/// windowed processor node. `offset_ms` is optional on the wire and defaults
/// to zero.
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
        #[serde(default)]
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
        #[serde(default)]
        offset_ms: i64,
    },
}

impl WindowSpec {
    /// Assign a window_id to a timestamp (milliseconds since epoch).
    ///
    /// For Tumbling: `floor_div(ts - offset, size)`.
    ///
    /// Uses true floor division. Standard `/` truncates toward zero, so
    /// `-5 / 3 = -1` where `floor(-5 / 3) = -2`.
    pub fn assign_tumbling(ts: i64, size_ms: i64, offset_ms: i64) -> i64 {
        let ts = ts - offset_ms;
        // Floor division: subtract one when the remainder is negative.
        let q = ts / size_ms;
        let r = ts % size_ms;
        if r < 0 { q - 1 } else { q }
    }

    /// Compute the `k = ceil(size_ms / slide_ms)` window IDs that contain `ts`.
    ///
    /// Each window in a sliding specification is identified by the tumbling
    /// window ID computed with step size `slide_ms`:
    ///
    /// ```text
    /// window_id[j] = assign_tumbling(ts - j * slide_ms, slide_ms, offset_ms)
    ///                for j in 0..k
    /// ```
    ///
    /// The returned `Vec` has exactly `k` elements (may contain duplicates at
    /// boundaries when `size_ms` is not a multiple of `slide_ms`).
    ///
    /// The caller must ensure `slide_ms > 0`.
    pub fn assign_sliding(ts: i64, size_ms: i64, slide_ms: i64, offset_ms: i64) -> Vec<i64> {
        // k = ceil(size_ms / slide_ms)
        let k = (size_ms + slide_ms - 1) / slide_ms;
        (0..k)
            .map(|j| Self::assign_tumbling(ts - j * slide_ms, slide_ms, offset_ms))
            .collect()
    }

    /// Whether the geometry is sane enough to build windows from.
    ///
    /// Checks what the window maths needs to stay well-defined: a strictly
    /// positive `size_ms`, a strictly positive `slide_ms` no larger than
    /// `size_ms`, and a strictly positive session `gap_ms`. The host runs this
    /// once at config load so a nonsense geometry is a configuration error,
    /// not a division-by-zero at run time.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            WindowSpec::Tumbling { size_ms, .. } => {
                if *size_ms <= 0 {
                    return Err(format!(
                        "tumbling window size_ms must be > 0, got {size_ms}"
                    ));
                }
            }
            WindowSpec::Session { gap_ms } => {
                if *gap_ms <= 0 {
                    return Err(format!("session window gap_ms must be > 0, got {gap_ms}"));
                }
            }
            WindowSpec::Sliding {
                size_ms, slide_ms, ..
            } => {
                if *size_ms <= 0 {
                    return Err(format!("sliding window size_ms must be > 0, got {size_ms}"));
                }
                if *slide_ms <= 0 {
                    return Err(format!(
                        "sliding window slide_ms must be > 0, got {slide_ms}"
                    ));
                }
                if slide_ms > size_ms {
                    return Err(format!(
                        "sliding window slide_ms ({slide_ms}) must be <= size_ms ({size_ms})"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_assign_positive_ts() {
        // Timestamp 1500 ms, window size 1000 ms, no offset
        // floor(1500 / 1000) = 1
        assert_eq!(WindowSpec::assign_tumbling(1500, 1000, 0), 1);
    }

    #[test]
    fn test_tumbling_assign_with_offset() {
        // Timestamp 1500 ms, window size 1000 ms, offset 500 ms
        // floor((1500 - 500) / 1000) = floor(1000 / 1000) = 1
        assert_eq!(WindowSpec::assign_tumbling(1500, 1000, 500), 1);

        // Timestamp 1200 ms, window size 1000 ms, offset 500 ms
        // floor((1200 - 500) / 1000) = floor(700 / 1000) = 0
        assert_eq!(WindowSpec::assign_tumbling(1200, 1000, 500), 0);
    }

    #[test]
    fn test_tumbling_assign_negative_ts() {
        // Timestamp -1500 ms, window size 1000 ms, no offset
        // floor(-1500 / 1000) = floor(-1.5) = -2
        assert_eq!(WindowSpec::assign_tumbling(-1500, 1000, 0), -2);

        // Timestamp -500 ms, window size 1000 ms, no offset
        // floor(-500 / 1000) = floor(-0.5) = -1
        assert_eq!(WindowSpec::assign_tumbling(-500, 1000, 0), -1);
    }

    #[test]
    fn test_tumbling_assign_boundary() {
        // Window boundaries with size 1000:
        // [0, 1000), [1000, 2000), [2000, 3000), etc.
        assert_eq!(WindowSpec::assign_tumbling(0, 1000, 0), 0);
        assert_eq!(WindowSpec::assign_tumbling(999, 1000, 0), 0);
        assert_eq!(WindowSpec::assign_tumbling(1000, 1000, 0), 1);
        assert_eq!(WindowSpec::assign_tumbling(1999, 1000, 0), 1);
        assert_eq!(WindowSpec::assign_tumbling(2000, 1000, 0), 2);
    }

    /// size=10, slide=5 → k=2; each record belongs to exactly 2 windows.
    #[test]
    fn test_assign_sliding_basic() {
        // ts=7, size=10, slide=5, offset=0
        // j=0: assign_tumbling(7, 5, 0) = floor(7/5) = 1
        // j=1: assign_tumbling(7 - 5, 5, 0) = assign_tumbling(2, 5, 0) = floor(2/5) = 0
        let ids = WindowSpec::assign_sliding(7, 10, 5, 0);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 1);
        assert_eq!(ids[1], 0);
    }

    /// size=15, slide=5 → k=3.
    #[test]
    fn test_assign_sliding_k_three() {
        // ts=12, size=15, slide=5
        // j=0: floor(12/5) = 2
        // j=1: floor(7/5)  = 1
        // j=2: floor(2/5)  = 0
        let ids = WindowSpec::assign_sliding(12, 15, 5, 0);
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], 2);
        assert_eq!(ids[1], 1);
        assert_eq!(ids[2], 0);
    }

    /// size == slide → k=1, identical to tumbling.
    #[test]
    fn test_assign_sliding_equals_tumbling_when_size_eq_slide() {
        let ids = WindowSpec::assign_sliding(1500, 1000, 1000, 0);
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], WindowSpec::assign_tumbling(1500, 1000, 0));
    }

    /// With offset: same floor-division shift as tumbling.
    #[test]
    fn test_assign_sliding_with_offset() {
        // ts=600, size=1000, slide=500, offset=100
        // k=2
        // j=0: assign_tumbling(600, 500, 100) = floor((600-100)/500) = floor(500/500) = 1
        // j=1: assign_tumbling(100, 500, 100) = floor((100-100)/500) = floor(0/500) = 0
        let ids = WindowSpec::assign_sliding(600, 1000, 500, 100);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 1);
        assert_eq!(ids[1], 0);
    }

    /// Negative timestamps use floor division, same as tumbling.
    #[test]
    fn test_assign_sliding_negative_ts() {
        // ts=-3, size=10, slide=5, offset=0
        // k=2
        // j=0: assign_tumbling(-3, 5, 0) = floor(-3/5) = -1
        // j=1: assign_tumbling(-8, 5, 0) = floor(-8/5) = -2
        let ids = WindowSpec::assign_sliding(-3, 10, 5, 0);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], -1);
        assert_eq!(ids[1], -2);
    }

    /// Non-divisible size/slide: k rounds up correctly.
    #[test]
    fn test_assign_sliding_non_divisible_ceil() {
        // size=7, slide=3 → k = ceil(7/3) = 3
        let ids = WindowSpec::assign_sliding(5, 7, 3, 0);
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn validate_accepts_sane_geometries() {
        WindowSpec::Tumbling {
            size_ms: 1000,
            offset_ms: 0,
        }
        .validate()
        .expect("positive size");
        WindowSpec::Session { gap_ms: 500 }
            .validate()
            .expect("positive gap");
        WindowSpec::Sliding {
            size_ms: 10_000,
            slide_ms: 5_000,
            offset_ms: 0,
        }
        .validate()
        .expect("size >= slide");
    }

    #[test]
    fn validate_rejects_nonsense_geometries() {
        assert!(
            WindowSpec::Tumbling {
                size_ms: 0,
                offset_ms: 0,
            }
            .validate()
            .is_err()
        );
        assert!(
            WindowSpec::Tumbling {
                size_ms: -100,
                offset_ms: 0,
            }
            .validate()
            .is_err()
        );
        assert!(WindowSpec::Session { gap_ms: 0 }.validate().is_err());
        assert!(
            WindowSpec::Sliding {
                size_ms: 1_000,
                slide_ms: 0,
                offset_ms: 0,
            }
            .validate()
            .is_err()
        );
        let err = WindowSpec::Sliding {
            size_ms: 1_000,
            slide_ms: 2_000,
            offset_ms: 0,
        }
        .validate()
        .unwrap_err();
        assert!(err.contains("slide_ms"), "got: {err}");
    }

    /// The serde shape is what the KDL `window` node and the topology use:
    /// internally tagged on `kind`, snake_case variants, `offset_ms` optional.
    #[test]
    fn spec_round_trips_through_its_internally_tagged_shape() {
        let spec = WindowSpec::Tumbling {
            size_ms: 30_000,
            offset_ms: 0,
        };
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
        assert_eq!(
            sliding,
            WindowSpec::Sliding {
                size_ms: 60_000,
                slide_ms: 10_000,
                offset_ms: 0,
            }
        );
    }
}
