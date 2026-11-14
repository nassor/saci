//! [`WindowedSystemBuilder`]: fluent construction and validation of a
//! [`WindowedSystem`].
//!
//! Collects the source component, time field, key fields, window geometry, and
//! window function, then checks that every required piece is present before
//! handing back a ready system.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::error::SaciError;
use crate::system::SystemMeta;

use super::function::WindowFunction;
use super::result::WindowResults;
use super::system::WindowedSystem;
use super::watermark::WatermarkState;
use crate::window_spec::WindowSpec;

/// Builder for [`WindowedSystem`].
///
/// All required fields (`source`, `window`, `function`) must be set before
/// calling [`build`](Self::build), which returns a `SaciError::Configuration`
/// for any missing required field.
///
/// # Example
///
/// ```ignore
/// let sys = WindowedSystemBuilder::new()
///     .source("Trade", "timestamp_ms")
///     .keyed_by(&["symbol"])
///     .window(WindowSpec::Tumbling { size_ms: 60_000, offset_ms: 0 })
///     .function(WindowFunction::Reduce {
///         input_field: "price",
///         aggregate: ReduceAggregate::Sum,
///     })
///     .build()
///     .unwrap();
/// ```
pub struct WindowedSystemBuilder {
    source_component: Option<&'static str>,
    time_field: Option<&'static str>,
    key_fields: Vec<&'static str>,
    spec: Option<WindowSpec>,
    function: Option<WindowFunction>,
    /// Allowed lateness in milliseconds. `None` disables watermark tracking.
    allowed_lateness_ms: Option<i64>,
}

impl Default for WindowedSystemBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowedSystemBuilder {
    /// Create a new builder with all fields unset.
    ///
    /// Watermark tracking is disabled by default. Call
    /// [`allowed_lateness`](Self::allowed_lateness) to enable it.
    pub fn new() -> Self {
        Self {
            source_component: None,
            time_field: None,
            key_fields: vec![],
            spec: None,
            function: None,
            allowed_lateness_ms: None,
        }
    }

    /// Enable watermark tracking with the given allowed-lateness budget.
    ///
    /// The system advances a watermark from observed event timestamps and routes
    /// each row by it. `ts >= watermark` is processed normally.
    /// `watermark - allowed_lateness <= ts < watermark` re-fires the window into
    /// [`WindowResults::late_batches`]. Anything earlier lands in
    /// `WindowResults::side_output`, a
    /// [`SideOutput<DroppedLate>`](super::result::SideOutput) resource.
    ///
    /// Pass `0` to drop all out-of-order data immediately, with no late firings.
    pub fn allowed_lateness(mut self, ms: i64) -> Self {
        self.allowed_lateness_ms = Some(ms);
        self
    }

    /// Set the source component name and the name of its time field.
    ///
    /// The time field must be `Int64` or a `Timestamp` variant accepted by
    /// [`to_ms_array`](super::time::to_ms_array): second, millisecond,
    /// microsecond, or nanosecond.
    pub fn source(mut self, component: &'static str, time_field: &'static str) -> Self {
        self.source_component = Some(component);
        self.time_field = Some(time_field);
        self
    }

    /// Set the key fields for grouped (keyed) windows.
    ///
    /// Pass an empty slice or omit this call for a non-keyed (global) window
    /// where all rows share the same bucket.
    pub fn keyed_by(mut self, fields: &[&'static str]) -> Self {
        self.key_fields = fields.to_vec();
        self
    }

    /// Set the window specification (geometry).
    pub fn window(mut self, spec: WindowSpec) -> Self {
        self.spec = Some(spec);
        self
    }

    /// Set the window function (aggregate or custom process).
    pub fn function(mut self, f: WindowFunction) -> Self {
        self.function = Some(f);
        self
    }

    /// Build the [`WindowedSystem`], validating that all required fields are set.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `source`, `window`, or
    /// `function` have not been set.
    pub fn build(self) -> Result<WindowedSystem, SaciError> {
        let source_component = self.source_component.ok_or_else(|| {
            SaciError::configuration(
                "WindowedSystemBuilder: source component not set; call .source()",
            )
        })?;
        let time_field = self.time_field.ok_or_else(|| {
            SaciError::configuration("WindowedSystemBuilder: time field not set; call .source()")
        })?;
        let spec = self.spec.ok_or_else(|| {
            SaciError::configuration("WindowedSystemBuilder: window spec not set; call .window()")
        })?;
        let function = self.function.ok_or_else(|| {
            SaciError::configuration(
                "WindowedSystemBuilder: window function not set; call .function()",
            )
        })?;

        let meta = SystemMeta::new("windowed")
            .read_component(source_component)
            .write_resource::<WindowResults>();

        let watermark = self
            .allowed_lateness_ms
            .map(|ms| Mutex::new(WatermarkState::new(ms)));

        Ok(WindowedSystem {
            source_component,
            time_field,
            key_fields: self.key_fields,
            spec,
            function,
            meta,
            watermark,
            emitted_windows: Mutex::new(HashSet::new()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::system::System;

    use super::super::function::ReduceAggregate;

    #[test]
    fn test_builder_missing_source_returns_error() {
        let result = WindowedSystemBuilder::new()
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "configuration");
    }

    #[test]
    fn test_builder_missing_window_returns_error() {
        let result = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "configuration");
    }

    #[test]
    fn test_builder_missing_function_returns_error() {
        let result = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "configuration");
    }

    #[test]
    fn test_builder_success_sets_meta_name() {
        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();
        assert_eq!(sys.meta().name, "windowed");
    }

    #[test]
    fn test_builder_meta_reads_component_and_writes_resource() {
        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();
        let meta = sys.meta();
        assert!(meta.reads_components.contains(&"Trade"));
        assert!(!meta.writes_resources.is_empty());
    }
}
