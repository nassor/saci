//! [`Pipeline`]: a self-contained workload of data, systems, DAG plan and optional IO.

pub(crate) use dag::SystemEntry;

use std::cell::Cell;
use std::sync::{Arc, OnceLock};

use crate::dataset::{Dataset, DatasetBuilder};
use crate::error::SaciError;
use crate::retry::SystemConfig;

use self::dag::ExpandedMeta;

#[cfg(feature = "io")]
use crate::io::{Sink, Source};

/// Statistics produced by the most recent [`run`](Pipeline::run) or
/// [`run_with_io`](Pipeline::run_with_io) call.
#[derive(Copy, Clone, Debug, Default)]
pub struct RunStats {
    /// Net change in live rows (positive = rows added, negative = rows deleted).
    pub rows_produced: isize,
    /// Number of systems that ran during the tick.
    pub systems_run: usize,
    /// Wall-clock milliseconds for the tick.
    pub duration_millis: u64,
    /// Total number of system retry attempts that occurred during this batch
    /// (sum across all systems; one retry = one failed attempt before a
    /// subsequent success). Populated by both `run()` and `run_on()`.
    pub retries_this_batch: u32,
}

/// A self-contained workload: columnar data, systems, DAG plan, and optional
/// IO sources/sinks.
///
/// `Pipeline` is the primary entry point for processing work. Build one with
/// [`Pipeline::new`], register components on `data()`, add systems, then call
/// [`run`](Self::run) or [`run_with_io`](Self::run_with_io).
///
/// For running many pipelines from one process, see `Scheduler`.
pub struct Pipeline {
    name: Arc<str>,
    /// Owned columnar data for this workload.
    pub data: Dataset,
    systems: Vec<SystemEntry>,
    stages: OnceLock<Result<Vec<Vec<usize>>, SaciError>>,
    expanded_metas: OnceLock<Result<Vec<ExpandedMeta>, SaciError>>,
    configs: OnceLock<Vec<SystemConfig>>,
    last_stats: Cell<RunStats>,
    #[cfg(feature = "io")]
    sources: Vec<(&'static str, Box<dyn Source>)>,
    #[cfg(feature = "io")]
    sinks: Vec<(&'static str, Box<dyn Sink>)>,
}

impl Pipeline {
    /// Create an empty pipeline with the given name.
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            data: Dataset::new(),
            systems: Vec::new(),
            stages: OnceLock::new(),
            expanded_metas: OnceLock::new(),
            configs: OnceLock::new(),
            last_stats: Cell::new(RunStats::default()),
            #[cfg(feature = "io")]
            sources: Vec::new(),
            #[cfg(feature = "io")]
            sinks: Vec::new(),
        }
    }

    /// Stats from the most recent [`run`](Self::run), [`run_on`](Self::run_on),
    /// or [`run_with_io`](Self::run_with_io) call.
    ///
    /// Returns `RunStats::default()` before the first run.
    pub fn last_stats(&self) -> RunStats {
        self.last_stats.get()
    }

    /// Return the pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Shared reference to the underlying dataset.
    pub fn data(&self) -> &Dataset {
        &self.data
    }

    /// Exclusive reference to the underlying dataset.
    pub fn data_mut(&mut self) -> &mut Dataset {
        &mut self.data
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new("default")
    }
}

/// Fluent builder for [`Pipeline`].
pub struct PipelineBuilder {
    pub(super) name: Arc<str>,
    pub(super) data: DatasetBuilder,
    pub(super) systems: Vec<SystemEntry>,
    #[cfg(feature = "io")]
    pub(super) sources: Vec<(&'static str, Box<dyn Source>)>,
    #[cfg(feature = "io")]
    pub(super) sinks: Vec<(&'static str, Box<dyn Sink>)>,
}

mod builder;
mod dag;
mod execution;
mod registration;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_name() {
        let p = Pipeline::new("my-pipeline");
        assert_eq!(p.name(), "my-pipeline");
    }

    #[test]
    fn test_data_mut_accessible() {
        use crate::component::Component;
        use std::sync::Arc;

        use arrow_schema::{DataType, Field, Schema};
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        struct RootOrder {
            id: u64,
        }
        impl Component for RootOrder {
            fn name() -> &'static str {
                "RootOrder"
            }
            fn schema() -> Arc<Schema> {
                Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
            }
        }

        let mut p = Pipeline::new("data-test");
        p.data_mut().register_component::<RootOrder>().unwrap();
        p.data_mut()
            .append::<RootOrder>(&[RootOrder { id: 1 }])
            .unwrap();
        assert_eq!(p.data().rows(), 1);
    }
}
