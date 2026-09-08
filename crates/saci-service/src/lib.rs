pub use saci_core::column;
pub use saci_core::component;
pub use saci_core::dataset;
pub use saci_core::error;
pub use saci_core::partition;
pub use saci_core::pipeline;
pub use saci_core::resource;
pub use saci_core::retry;
pub use saci_core::row;
pub use saci_core::scheduler;
pub use saci_core::schema;
pub use saci_core::system;

#[cfg(feature = "windows")]
pub use saci_core::windows;

pub use saci_core::Component;
pub use saci_core::Row;
pub use saci_core::SchemaRegistry;
pub use saci_core::{BackpressureSpec, DependencyKind, PipelineConfig, Scheduler};
pub use saci_core::{Dataset, Pipeline, PipelineBuilder, RunStats};
pub use saci_core::{
    FieldAccess, FieldRef, ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta,
    WriteSet, system_fn,
};
pub use saci_core::{RetryMode, SystemConfig};
pub use saci_core::{SaciError, SaciResult};

// The service's own OpenTelemetry instruments. Not gated: the no-op impl keeps
// every call site free of `#[cfg]`.
pub mod metrics;

// In-process telemetry. Sibling of `metrics`, not part of `service`, so a
// library embedder can capture spans and samples without the axum control
// plane.
#[cfg(feature = "inspector")]
pub mod inspector;

#[cfg(feature = "distributed")]
pub mod distributed;

// Shared by the two out-of-process pipeline runtimes: both decode declared
// component schemas into a template dataset the same way.
#[cfg(any(feature = "wasm", feature = "plugin"))]
mod descriptor;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "plugin")]
pub mod plugin;

#[cfg(feature = "service")]
pub mod service;

/// Convenience re-exports of the most commonly used types and traits.
///
/// `use saci_service::prelude::*;`
pub mod prelude {
    pub use crate::{
        BackpressureSpec, Component, Dataset, DependencyKind, FieldAccess, ParallelSystem,
        Pipeline, PipelineBuilder, PipelineConfig, ResourceUpdate, RetryMode, Row, RunStats,
        SaciError, SaciResult, Scheduler, SchemaRegistry, SliceWriteSet, System, SystemConfig,
        SystemMeta, WriteSet, system_fn,
    };

    pub use crate::column::ComponentView;
    pub use crate::dataset::DatasetBuilder;
    pub use crate::system::FieldRef;

    pub use async_trait::async_trait;

    #[cfg(feature = "windows")]
    pub use crate::windows::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator};

    pub use crate::partition::KeyPartition;
}
