//! [`System`]: the processing unit for the Arrow-backed pipeline.
//!
//! `System` operates on [`Dataset`] and declares data access at **field
//! granularity** via [`FieldAccess`] pairs `(component_name, field_name)`.
//! Field-level declarations let two systems that write different fields of the
//! same component share a pipeline stage.
//!
//! ## Quick start
//!
//! ```rust
//! #
//! # {
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use async_trait::async_trait;
//! use saci_core::component::Component;
//! use saci_core::system::{System, SystemMeta};
//! use saci_core::dataset::Dataset;
//! use saci_core::SaciError;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Price { value: f64 }
//! impl Component for Price {
//!     fn name() -> &'static str { "Price" }
//!     fn schema() -> Arc<Schema> {
//!         Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]))
//!     }
//! }
//!
//! struct PrintPriceSystem;
//!
//! #[async_trait]
//! impl System for PrintPriceSystem {
//!     fn meta(&self) -> SystemMeta {
//!         SystemMeta::new("print_price")
//!             .read("Price", "value")
//!     }
//!     async fn run(&self, _data: &mut Dataset) -> Result<(), saci_core::SaciError> {
//!         Ok(())
//!     }
//! }
//! # }
//! ```

pub mod closure;
pub mod field;
pub mod meta;
pub mod write_set;

pub use closure::{FnSystem, system_fn};
pub use field::{FieldAccess, FieldRef};
pub use meta::SystemMeta;
pub use write_set::{ResourceUpdate, SliceWriteSet, WriteSet};

use async_trait::async_trait;

use crate::dataset::Dataset;
use crate::error::SaciError;
use crate::retry::SystemConfig;

/// A processing unit that operates on a [`Dataset`] and declares field-level
/// data access via [`SystemMeta`].
///
/// Only [`run`](Self::run) is required. Override [`meta`](Self::meta) to
/// declare field-level read/write dependencies for the scheduler, and
/// [`config`](Self::config) to change retry behaviour.
///
/// Override [`run_sync`](Self::run_sync) to return `Some(result)` when the
/// system body is purely synchronous: the scheduler then calls it directly and
/// skips the `Box<dyn Future>` allocation that `#[async_trait]` imposes.
/// Systems built with [`system_fn`] always provide `run_sync`.
///
/// # Example
///
/// ```rust
/// #
/// # {
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use arrow_array::Float64Array;
/// use async_trait::async_trait;
/// use saci_core::component::Component;
/// use saci_core::system::{System, SystemMeta};
/// use saci_core::dataset::Dataset;
/// use saci_core::SaciError;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Score { value: f64 }
/// impl Component for Score {
///     fn name() -> &'static str { "Score" }
///     fn schema() -> Arc<Schema> {
///         Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]))
///     }
/// }
///
/// struct SumScoreSystem;
///
/// #[async_trait]
/// impl System for SumScoreSystem {
///     fn meta(&self) -> SystemMeta {
///         SystemMeta::new("sum_score").read("Score", "value")
///     }
///     async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
///         if let Some(col) = data.column::<Score>("value") {
///             let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
///             let _sum: f64 = arr.values().iter().sum();
///         }
///         Ok(())
///     }
/// }
/// # }
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

    /// Returns the retry configuration for this system.
    ///
    /// Defaults to [`SystemConfig::default()`] (exponential backoff, 3 retries).
    fn config(&self) -> SystemConfig {
        SystemConfig::default()
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
/// ## Slice parallelism
///
/// Override [`run_slice`](Self::run_slice) to opt into intra-system slice
/// parallelism: the scheduler splits `data.row_range()` into `num_cpus` chunks,
/// runs each chunk in parallel via rayon, then calls
/// [`merge_slices`](Self::merge_slices) to combine the results. Returning
/// `None`, the default, opts out.
///
/// ## Resource updates
///
/// The dataset is immutable during parallel execution, so resource mutations
/// travel as [`ResourceUpdate`] values attached to the returned `WriteSet`. The
/// scheduler applies them on the main thread after columns are committed.
///
/// ## Example
///
/// ```rust
/// #
/// # {
/// use std::sync::Arc;
/// use std::collections::HashMap;
/// use arrow_array::{ArrayRef, Float64Array};
/// use arrow_schema::{DataType, Field, Schema};
/// use async_trait::async_trait;
/// use saci_core::component::Component;
/// use saci_core::system::{ParallelSystem, SystemMeta, WriteSet};
/// use saci_core::dataset::Dataset;
/// use saci_core::SaciError;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Score { value: f64 }
/// impl Component for Score {
///     fn name() -> &'static str { "Score" }
///     fn schema() -> Arc<Schema> {
///         Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]))
///     }
/// }
///
/// struct DoubleScoreSystem;
///
/// #[async_trait]
/// impl ParallelSystem for DoubleScoreSystem {
///     fn meta(&self) -> SystemMeta {
///         SystemMeta::new("double_score")
///             .read("Score", "value")
///             .write("Score", "value")
///     }
///     async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
///         let col = data.column::<Score>("value")
///             .ok_or_else(|| SaciError::generic("Score.value not found"))?;
///         let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
///         let doubled: Vec<f64> = arr.values().iter().map(|&v| v * 2.0).collect();
///         Ok(WriteSet::new().put("Score", "value", Arc::new(Float64Array::from(doubled))))
///     }
/// }
/// # }
/// ```
#[async_trait]
pub trait ParallelSystem: Send + Sync {
    /// Returns metadata describing this system's name and data access patterns.
    fn meta(&self) -> SystemMeta;

    /// Returns the retry configuration for this system.
    fn config(&self) -> SystemConfig {
        SystemConfig::default()
    }

    /// Execute this system against an **immutable** dataset view, producing a
    /// [`WriteSet`] to be applied atomically after the stage completes.
    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError>;

    /// Execute this system over one row-range slice.
    ///
    /// When this returns `Some(result)`, the scheduler splits the row range
    /// into chunks (one per CPU core) and executes each chunk in parallel via
    /// rayon (inside `spawn_blocking`). Results are merged with
    /// [`merge_slices`](Self::merge_slices).
    ///
    /// Return `None` to opt out of slice parallelism (the default).
    fn run_slice(
        &self,
        _data: &Dataset,
        _rows: std::ops::Range<u32>,
    ) -> Option<Result<SliceWriteSet, SaciError>> {
        None
    }

    /// Merge per-slice results into one [`WriteSet`].
    ///
    /// The default implementation concatenates each field's array segments
    /// in order using [`arrow_select::concat::concat`].
    ///
    /// Override when a system needs special merging, such as reducing
    /// per-slice statistics to a single scalar.
    fn merge_slices(&self, slices: Vec<SliceWriteSet>) -> Result<WriteSet, SaciError> {
        use arrow_select::concat::concat;

        if slices.is_empty() {
            return Ok(WriteSet::new());
        }

        let keys: Vec<(&'static str, &'static str)> = slices[0].fields.keys().cloned().collect();

        let mut ws = WriteSet::new();
        for key in keys {
            let arrays: Vec<&dyn arrow_array::Array> = slices
                .iter()
                .filter_map(|s| s.fields.get(&key))
                .map(|a| a.as_ref())
                .collect();
            if arrays.is_empty() {
                continue;
            }
            let merged = concat(&arrays)
                .map_err(|e| SaciError::generic(format!("merge_slices concat error: {e}")))?;
            ws = ws.put(key.0, key.1, merged);
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
    use crate::retry::SystemConfig;

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
            SystemMeta::new("read_id").read("Order", "id")
        }
        async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
            Ok(())
        }
    }

    #[allow(dead_code)]
    struct WriteTotalSystem;

    #[async_trait]
    impl System for WriteTotalSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("write_total").write("Order", "total")
        }
        async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
            Ok(())
        }
    }

    #[allow(dead_code)]
    struct ReadComponentSystem;

    #[async_trait]
    impl System for ReadComponentSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("read_order").read_component("Order")
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
        assert_eq!(sys.meta().name, "read_id");
    }

    #[test]
    fn test_meta_reads_field() {
        let meta = SystemMeta::new("test")
            .read("Order", "id")
            .read("Order", "total");
        assert_eq!(meta.reads.len(), 2);
        assert_eq!(meta.reads[0].component, "Order");
        assert_eq!(meta.reads[0].field, "id");
    }

    #[test]
    fn test_meta_writes_field() {
        let meta = SystemMeta::new("test").write("Order", "total");
        assert_eq!(meta.writes.len(), 1);
        assert_eq!(meta.writes[0].field, "total");
    }

    #[test]
    fn test_meta_read_component() {
        let meta = SystemMeta::new("test").read_component("Order");
        assert_eq!(meta.reads_components, vec!["Order"]);
    }

    #[test]
    fn test_meta_write_component() {
        let meta = SystemMeta::new("test").write_component("Order");
        assert_eq!(meta.writes_components, vec!["Order"]);
    }

    #[test]
    fn test_meta_resources() {
        let meta = SystemMeta::new("test")
            .read_resource::<MyResource>()
            .write_resource::<MyResource>();
        assert_eq!(meta.reads_resources.len(), 1);
        assert_eq!(meta.writes_resources.len(), 1);
        assert_eq!(meta.reads_resources[0], TypeId::of::<MyResource>());
    }

    #[test]
    fn test_default_config_is_exponential_backoff() {
        let sys = ReadIdSystem;
        let config = sys.config();
        assert_eq!(
            config.retry_mode.max_attempts(),
            SystemConfig::default().retry_mode.max_attempts()
        );
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
            meta.name.contains("AnonymousSystem"),
            "expected type name, got: {}",
            meta.name
        );
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_system_run_is_callable() {
        let mut data = Dataset::new();
        data.register_component::<Order>().unwrap();
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
