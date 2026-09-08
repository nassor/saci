pub mod error;
pub mod retry;

pub mod column;
pub mod component;
pub mod dataset;
pub mod partition;
pub mod pipeline;
pub mod resource;
pub mod row;
pub mod scheduler;
pub mod schema;
pub mod sdk;
pub mod system;

#[cfg(feature = "runtime")]
pub mod runtime;

#[cfg(feature = "io")]
pub mod io;

/// The `window` geometry enum. Declared in every build: a host parses a
/// window declaration whether or not it carries the engine that serves one.
pub mod window_spec;

#[cfg(feature = "windows")]
pub mod windows;

pub use error::{SaciError, SaciResult};
pub use retry::{RetryMode, SystemConfig};

pub use component::Component;
pub use dataset::{Dataset, DatasetBuilder};
pub use partition::KeyPartition;
pub use pipeline::{Pipeline, PipelineBuilder, RunStats};
pub use row::Row;
pub use scheduler::{BackpressureSpec, DependencyKind, PipelineConfig, Scheduler};
pub use schema::SchemaRegistry;
pub use system::{
    FieldAccess, FieldRef, ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta,
    WriteSet, system_fn,
};

#[cfg(feature = "runtime")]
pub use runtime::{PipelineRuntime, RuntimeDescriptorInfo, RuntimeOutput};

pub mod prelude {
    #[cfg(feature = "runtime")]
    pub use crate::runtime::RuntimeOutput;
    pub use crate::{
        BackpressureSpec, Component, Dataset, DependencyKind, FieldAccess, KeyPartition,
        ParallelSystem, Pipeline, PipelineBuilder, PipelineConfig, ResourceUpdate, RetryMode, Row,
        RunStats, SaciError, SaciResult, Scheduler, SchemaRegistry, SliceWriteSet, System,
        SystemConfig, SystemMeta, WriteSet, system_fn,
    };

    pub use crate::column::ComponentView;
    pub use crate::dataset::DatasetBuilder;
    pub use crate::system::FieldRef;

    pub use async_trait::async_trait;

    #[cfg(feature = "windows")]
    pub use crate::windows::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator, WindowWatermark};
}
