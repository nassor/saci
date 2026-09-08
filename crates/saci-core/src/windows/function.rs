//! Window function definitions: reduce aggregates and custom process functions.
//!
//! `WindowFunction::Reduce` applies a built-in columnar aggregate (`Sum`, `Min`,
//! `Max`, `Count`, `Mean`) over one numeric field within each window group.
//! `WindowFunction::Process` delegates to a user-supplied [`ProcessWindowFn`] that
//! receives the full window [`RecordBatch`], the escape hatch for logic that is
//! not a single-field aggregate. Evaluation over grouped data lives in
//! `system.rs`; this module is data definitions only.

use arrow_array::RecordBatch;

use crate::error::SaciError;

/// Contextual metadata passed to [`ProcessWindowFn::process`] for each window group.
///
/// The context carries timing and watermark state so the function can implement
/// time-aware logic, such as late-data annotations or conditional early firing.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "windows")]
/// # {
/// use saci_core::windows::function::{ProcessWindowFn, WindowContext};
/// use arrow_array::RecordBatch;
/// use saci_core::error::SaciError;
///
/// struct MyFn;
/// impl ProcessWindowFn for MyFn {
///     fn process(&self, ctx: &WindowContext, batch: &RecordBatch) -> Result<RecordBatch, SaciError> {
///         // Inspect ctx.is_late_firing to annotate late results differently.
///         Ok(batch.clone())
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WindowContext {
    /// Window bucket identifier (e.g. floor-division index for tumbling windows).
    pub window_id: i64,
    /// Inclusive start of the window in milliseconds since epoch.
    pub window_start: i64,
    /// Exclusive end of the window in milliseconds since epoch.
    pub window_end: i64,
    /// Whether this invocation is a late-data re-firing (the window had already
    /// been emitted once and a late row arrived within the allowed-lateness window).
    pub is_late_firing: bool,
    /// The current watermark at the time this window group is processed (ms
    /// since epoch).  `i64::MIN` when no watermark has been set yet.
    pub watermark: i64,
}

/// Built-in aggregate operations for [`WindowFunction::Reduce`].
///
/// Each variant reduces all values of a single numeric field within a window
/// group into one scalar output row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReduceAggregate {
    /// Arithmetic sum of all non-null values.
    Sum,
    /// Minimum non-null value.
    Min,
    /// Maximum non-null value.
    Max,
    /// Count of all rows (including nulls).
    Count,
    /// Arithmetic mean of all non-null values.
    Mean,
}

/// A user-defined function that processes all rows belonging to one window.
///
/// Implement this trait when the built-in [`ReduceAggregate`] variants are not
/// enough. The system calls [`ProcessWindowFn::process`] once per
/// `(window_id, key_hash)` group with that group's rows as a [`RecordBatch`] and
/// a [`WindowContext`] carrying timing and watermark information.
///
/// Implementations must be `Send + Sync` so parallel window execution can invoke
/// them from several threads concurrently.
pub trait ProcessWindowFn: Send + Sync {
    /// Compute the output for a single window group.
    ///
    /// `ctx` carries the window boundaries, the late-firing flag, and the current
    /// watermark. `batch` holds every row assigned to this group in its original
    /// order. The returned batch is the result for this window.
    fn process(&self, ctx: &WindowContext, batch: &RecordBatch) -> Result<RecordBatch, SaciError>;
}

/// The window function to apply when computing window results.
///
/// Either a built-in columnar aggregate over one field, or a fully custom
/// function that receives the entire window [`RecordBatch`].
pub enum WindowFunction {
    /// Apply a built-in aggregate to a single field.
    ///
    /// # Example
    ///
    /// ```ignore
    /// WindowFunction::Reduce {
    ///     input_field: "price",
    ///     aggregate: ReduceAggregate::Sum,
    /// }
    /// ```
    Reduce {
        /// Name of the numeric field to aggregate.
        input_field: &'static str,
        /// The aggregate operation to apply.
        aggregate: ReduceAggregate,
    },

    /// Delegate to a custom processing function.
    ///
    /// The boxed [`ProcessWindowFn`] receives the full window [`RecordBatch`]
    /// and can return an output with an arbitrary schema.
    Process(Box<dyn ProcessWindowFn>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reduce_aggregate_eq() {
        assert_eq!(ReduceAggregate::Sum, ReduceAggregate::Sum);
        assert_ne!(ReduceAggregate::Sum, ReduceAggregate::Min);
    }

    #[test]
    fn test_reduce_aggregate_copy() {
        let agg = ReduceAggregate::Max;
        let agg2 = agg; // Copy
        assert_eq!(agg, agg2);
    }

    #[test]
    fn test_reduce_aggregate_debug() {
        assert_eq!(format!("{:?}", ReduceAggregate::Count), "Count");
        assert_eq!(format!("{:?}", ReduceAggregate::Mean), "Mean");
    }

    #[test]
    fn test_window_function_reduce_constructible() {
        let wf = WindowFunction::Reduce {
            input_field: "amount",
            aggregate: ReduceAggregate::Sum,
        };
        match wf {
            WindowFunction::Reduce {
                input_field,
                aggregate,
            } => {
                assert_eq!(input_field, "amount");
                assert_eq!(aggregate, ReduceAggregate::Sum);
            }
            WindowFunction::Process(_) => panic!("unexpected variant"),
        }
    }

    struct NoopFn;
    impl ProcessWindowFn for NoopFn {
        fn process(
            &self,
            _ctx: &WindowContext,
            batch: &RecordBatch,
        ) -> Result<RecordBatch, SaciError> {
            Ok(batch.clone())
        }
    }

    #[test]
    fn test_window_function_process_constructible() {
        let wf = WindowFunction::Process(Box::new(NoopFn));
        assert!(matches!(wf, WindowFunction::Process(_)));
    }

    #[test]
    fn test_window_context_fields() {
        let ctx = WindowContext {
            window_id: 3,
            window_start: 3000,
            window_end: 4000,
            is_late_firing: true,
            watermark: 5000,
        };
        assert_eq!(ctx.window_id, 3);
        assert_eq!(ctx.window_start, 3000);
        assert_eq!(ctx.window_end, 4000);
        assert!(ctx.is_late_firing);
        assert_eq!(ctx.watermark, 5000);
    }

    #[test]
    fn test_window_context_clone() {
        let ctx = WindowContext {
            window_id: 1,
            window_start: 0,
            window_end: 1000,
            is_late_firing: false,
            watermark: i64::MIN,
        };
        let ctx2 = ctx.clone();
        assert_eq!(ctx2.window_id, ctx.window_id);
        assert_eq!(ctx2.is_late_firing, ctx.is_late_firing);
    }
}
