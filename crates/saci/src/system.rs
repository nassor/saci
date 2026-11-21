//! [`System`]: the processing unit for the Arrow-backed pipeline.
//!
//! `System` operates on [`Dataset`] and declares data access at **field
//! granularity** through [`SystemMeta`]. Field-level declarations let two
//! systems that write different fields of the same component share a pipeline
//! stage.
//!
//! ## Quick start
//!
//! ```rust
//! use saci::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Component, Serialize, Deserialize)]
//! struct Price { value: f64 }
//!
//! struct PrintPriceSystem;
//!
//! #[async_trait]
//! impl System for PrintPriceSystem {
//!     fn meta(&self) -> SystemMeta {
//!         SystemMeta::new("print_price").reads(Price::VALUE)
//!     }
//!     async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
//!         Ok(())
//!     }
//! }
//! ```

mod closure;
mod field;
mod meta;
mod write_set;

pub use closure::{parallel_fn, system_fn};
pub use field::{FieldAccess, FieldRef};
pub use meta::SystemMeta;
#[cfg(feature = "engine")]
pub(crate) use write_set::merge_entries;
pub use write_set::{SliceWriteSet, WriteSet};

use std::ops::Range;

use async_trait::async_trait;

use crate::dataset::Dataset;
use crate::error::SaciError;
use crate::retry::RetryMode;

/// A processing unit that operates on a [`Dataset`] and declares field-level
/// data access via [`SystemMeta`].
///
/// Only [`run`](Self::run) is required. Override [`meta`](Self::meta) to
/// declare field-level read/write dependencies for the scheduler, and
/// [`retry`](Self::retry) to retry a transient failure.
///
/// Override [`run_sync`](Self::run_sync) to return `Some(result)` when the
/// system body is purely synchronous: the scheduler then calls it directly and
/// skips the `Box<dyn Future>` allocation that `#[async_trait]` imposes.
/// Systems built with [`system_fn`] always provide `run_sync`.
///
/// # Example
///
/// ```rust
/// use saci::prelude::*;
/// use arrow_array::Float64Array;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Component, Serialize, Deserialize)]
/// struct Score { value: f64 }
///
/// struct SumScoreSystem;
///
/// #[async_trait]
/// impl System for SumScoreSystem {
///     fn meta(&self) -> SystemMeta {
///         SystemMeta::new("sum_score").reads(Score::VALUE)
///     }
///     async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
///         let col = data.column(Score::VALUE)?;
///         let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
///         let _sum: f64 = arr.values().iter().sum();
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait System: Send + Sync {
    /// Returns metadata describing this system's name and data access patterns.
    ///
    /// The default returns an empty [`SystemMeta`] whose name is the type
    /// name of the concrete implementing type. Override to declare field-level
    /// read/write dependencies for automatic stage ordering.
    fn meta(&self) -> SystemMeta {
        SystemMeta::new(std::any::type_name::<Self>())
    }

    /// How a failed run is retried. The default is [`RetryMode::None`]; only
    /// an error whose [`SaciError::is_retryable`] is true is ever retried.
    fn retry(&self) -> RetryMode {
        RetryMode::None
    }

    /// Execute this system against the given dataset.
    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError>;

    /// Synchronous fast-path for systems that do not need `.await`.
    ///
    /// When this returns `Some(result)`, the scheduler uses `result` directly
    /// and skips the async state machine. The default returns `None`.
    ///
    /// Systems created with [`system_fn`] automatically implement this.
    fn run_sync(&self, _data: &mut Dataset) -> Option<Result<(), SaciError>> {
        None
    }
}

/// Minimum row count below which slice parallelism is bypassed in favour of
/// single-threaded execution. Prevents rayon overhead from dominating on
/// small batches.
pub const SLICE_PARALLEL_THRESHOLD: u32 = 100_000;

/// Minimum row count at or above which a stage containing multiple
/// [`ParallelSystem`]s dispatches its systems concurrently, one
/// `spawn_blocking` each. Below this the systems in the stage run sequentially
/// inline, because dispatch plus write-set merging costs more than the work it
/// parallelises. The crossover sits between 65 536 and 131 072 rows, the same
/// order as [`SLICE_PARALLEL_THRESHOLD`].
pub const STAGE_INLINE_THRESHOLD: u32 = 100_000;

/// A processing unit that operates on an **immutable** [`Dataset`] view and
/// produces a [`WriteSet`] for atomic write-back.
///
/// Where [`System`] takes `&mut Dataset`, `ParallelSystem` takes `&Dataset`, so
/// systems with disjoint field writes can run concurrently in one stage.
///
/// Implement either [`run`](Self::run), or [`sliced`](Self::sliced) and
/// [`run_slice`](Self::run_slice).
///
/// ## Slice parallelism
///
/// A system whose [`sliced`](Self::sliced) returns `true` writes its kernel
/// once in [`run_slice`](Self::run_slice): the scheduler splits
/// `data.row_range()` into `num_cpus` chunks, runs each chunk in parallel via
/// rayon, then calls [`merge_slices`](Self::merge_slices) to combine the
/// results. Below [`SLICE_PARALLEL_THRESHOLD`] rows, the default `run` calls
/// `run_slice` once over the whole range.
///
/// ## Resource updates
///
/// The dataset is immutable during parallel execution, so resource mutations
/// travel on the returned `WriteSet` ([`WriteSet::with_resource`]). The
/// scheduler applies them on the main thread after columns are committed.
///
/// ## Example
///
/// ```rust
/// use std::sync::Arc;
/// use saci::prelude::*;
/// use arrow_array::Float64Array;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Component, Serialize, Deserialize)]
/// struct Score { value: f64 }
///
/// struct DoubleScoreSystem;
///
/// #[async_trait]
/// impl ParallelSystem for DoubleScoreSystem {
///     fn meta(&self) -> SystemMeta {
///         SystemMeta::new("double_score").reads(Score::VALUE).writes(Score::VALUE)
///     }
///     async fn run(&self, data: &Dataset) -> SaciResult<WriteSet> {
///         let col = data.column(Score::VALUE)?;
///         let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
///         let doubled: Vec<f64> = arr.values().iter().map(|&v| v * 2.0).collect();
///         Ok(WriteSet::new().put(Score::VALUE, Arc::new(Float64Array::from(doubled))))
///     }
/// }
/// ```
#[async_trait]
pub trait ParallelSystem: Send + Sync {
    /// Returns metadata describing this system's name and data access patterns.
    fn meta(&self) -> SystemMeta;

    /// How a failed run is retried. The default is [`RetryMode::None`].
    fn retry(&self) -> RetryMode {
        RetryMode::None
    }

    /// Execute this system against an **immutable** dataset view, producing a
    /// [`WriteSet`] to be applied atomically after the stage completes.
    ///
    /// The default runs [`run_slice`](Self::run_slice) once over every row and
    /// passes the result through [`merge_slices`](Self::merge_slices).
    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        self.merge_slices(vec![self.run_slice(data, data.row_range())?])
    }

    /// Synchronous fast-path: when this returns `Some(result)`, the scheduler
    /// uses it and never builds the async state machine of
    /// [`run`](Self::run). The default returns `None`.
    ///
    /// Systems created with [`parallel_fn`] automatically implement this.
    fn run_sync(&self, _data: &Dataset) -> Option<Result<WriteSet, SaciError>> {
        None
    }

    /// `true` when [`run_slice`](Self::run_slice) is implemented and the
    /// scheduler may split a large dataset across threads. Defaults to `false`.
    fn sliced(&self) -> bool {
        false
    }

    /// Execute this system over one row-range slice.
    ///
    /// Called only when [`sliced`](Self::sliced) returns `true`, from rayon
    /// inside `spawn_blocking`. Results are merged with
    /// [`merge_slices`](Self::merge_slices).
    fn run_slice(&self, _data: &Dataset, _rows: Range<u32>) -> Result<SliceWriteSet, SaciError> {
        Err(SaciError::configuration(
            "ParallelSystem: implement `run`, or `sliced` and `run_slice`",
        ))
    }

    /// Merge per-slice results into one [`WriteSet`].
    ///
    /// The default implementation concatenates each field's array segments
    /// in order using [`arrow_select::concat::concat`]; a single slice is
    /// moved through without copying.
    ///
    /// Override when a system needs special merging, such as reducing
    /// per-slice statistics to a single scalar.
    fn merge_slices(&self, mut slices: Vec<SliceWriteSet>) -> Result<WriteSet, SaciError> {
        use arrow_select::concat::concat;

        if slices.len() == 1
            && let Some(only) = slices.pop()
        {
            let mut ws = WriteSet::new();
            for (component, field, array) in only.iter() {
                ws = ws.put_named(component, field, array.clone());
            }
            return Ok(ws);
        }
        let Some(first) = slices.first() else {
            return Ok(WriteSet::new());
        };

        let mut ws = WriteSet::new();
        for (component, field, _) in first.iter() {
            let arrays: Vec<&dyn arrow_array::Array> = slices
                .iter()
                .filter_map(|s| s.get_named(component, field))
                .map(|a| a.as_ref())
                .collect();
            let merged = concat(&arrays)
                .map_err(|e| SaciError::generic(format!("merge_slices concat error: {e}")))?;
            ws = ws.put_named(component, field, merged);
        }

        Ok(ws)
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::error::SaciError;

    #[derive(Serialize, Deserialize)]
    struct Order {
        id: u64,
        total: f64,
    }

    impl Component for Order {
        fn name() -> &'static str {
            "Order"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    struct MyResource(#[allow(dead_code)] u32);

    struct ReadIdSystem;

    #[async_trait]
    impl System for ReadIdSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("read_id").reads_named("Order", "id")
        }
        async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
            Ok(())
        }
    }

    struct SyncSystem;

    #[async_trait]
    impl System for SyncSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("sync_sys")
        }
        async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
            Ok(())
        }
        fn run_sync(&self, _data: &mut Dataset) -> Option<Result<(), SaciError>> {
            Some(Ok(()))
        }
    }

    #[test]
    fn test_meta_name_is_set() {
        let sys = ReadIdSystem;
        assert_eq!(sys.meta().name(), "read_id");
    }

    #[test]
    fn test_meta_reads_field() {
        let meta = SystemMeta::new("test")
            .reads_named("Order", "id")
            .reads_named("Order", "total");
        assert_eq!(meta.reads.len(), 2);
        assert_eq!(meta.reads[0].component, "Order");
        assert_eq!(meta.reads[0].field, "id");
    }

    #[test]
    fn test_meta_writes_field() {
        let meta = SystemMeta::new("test").writes_named("Order", "total");
        assert_eq!(meta.writes.len(), 1);
        assert_eq!(meta.writes[0].field, "total");
    }

    #[test]
    fn test_meta_reads_all() {
        let meta = SystemMeta::new("test").reads_all::<Order>();
        assert_eq!(meta.reads_components, vec!["Order"]);
    }

    #[test]
    fn test_meta_writes_all_named() {
        let meta = SystemMeta::new("test").writes_all_named("Order");
        assert_eq!(meta.writes_components, vec!["Order"]);
    }

    #[test]
    fn test_meta_resources() {
        let meta = SystemMeta::new("test")
            .reads_resource::<MyResource>()
            .writes_resource::<MyResource>();
        assert_eq!(meta.reads_resources.len(), 1);
        assert_eq!(meta.writes_resources.len(), 1);
        assert_eq!(meta.reads_resources[0], TypeId::of::<MyResource>());
    }

    #[test]
    fn test_default_retry_is_none() {
        assert!(matches!(ReadIdSystem.retry(), RetryMode::None));
    }

    #[test]
    fn test_default_meta_uses_type_name() {
        struct AnonymousSystem;
        #[async_trait]
        impl System for AnonymousSystem {
            async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
                Ok(())
            }
        }
        let meta = AnonymousSystem.meta();
        assert!(
            meta.name().contains("AnonymousSystem"),
            "expected type name, got: {}",
            meta.name()
        );
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_system_run_is_callable() {
        let mut data = Dataset::new();
        data.register::<Order>().unwrap();
        let sys = ReadIdSystem;
        assert!(sys.run(&mut data).await.is_ok());
    }

    #[test]
    fn test_run_sync_default_returns_none() {
        let mut data = Dataset::new();
        let sys = ReadIdSystem;
        assert!(sys.run_sync(&mut data).is_none());
    }

    #[test]
    fn test_run_sync_override_returns_some() {
        let mut data = Dataset::new();
        let sys = SyncSystem;
        assert!(matches!(sys.run_sync(&mut data), Some(Ok(()))));
    }

    impl Order {
        const AMOUNT: FieldRef<Order> = FieldRef::new("amount");
        const STATUS: FieldRef<Order> = FieldRef::new("status");
    }

    #[test]
    fn field_ref_reads_overload() {
        let meta = SystemMeta::new("test").reads(Order::AMOUNT);
        assert_eq!(meta.reads.len(), 1);
        assert_eq!(
            meta.reads[0],
            FieldAccess {
                component: "Order",
                field: "amount"
            }
        );
    }

    #[test]
    fn field_ref_writes_overload() {
        let meta = SystemMeta::new("test").writes(Order::STATUS);
        assert_eq!(meta.writes.len(), 1);
        assert_eq!(
            meta.writes[0],
            FieldAccess {
                component: "Order",
                field: "status"
            }
        );
    }

    #[test]
    fn field_ref_is_copy() {
        let f = Order::AMOUNT;
        let _ = f;
        let _ = f;
    }
}
