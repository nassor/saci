use std::sync::Arc;

use crate::system::{ParallelSystem, System};

#[cfg(feature = "io")]
use crate::io::{Sink, Source};

use super::dag::SystemEntry;
use super::{Pipeline, PipelineBuilder};
use crate::dataset::{Dataset, DatasetBuilder};

impl Pipeline {
    /// Create a [`PipelineBuilder`].
    pub fn builder(name: impl Into<Arc<str>>) -> PipelineBuilder {
        PipelineBuilder {
            name: name.into(),
            data: DatasetBuilder(Dataset::new()),
            systems: Vec::new(),
            #[cfg(feature = "io")]
            sources: Vec::new(),
            #[cfg(feature = "io")]
            sinks: Vec::new(),
        }
    }
}

impl PipelineBuilder {
    /// Register component `C` in the dataset.
    pub fn with<C: crate::component::Component>(mut self) -> Self {
        self.data = self.data.with::<C>();
        self
    }

    /// Insert a resource into the dataset.
    pub fn with_resource<R: Send + Sync + 'static>(mut self, r: R) -> Self {
        self.data = self.data.with_resource(r);
        self
    }

    /// Add a sequential system.
    pub fn with_system<S: System + 'static>(mut self, system: S) -> Self {
        self.systems.push(SystemEntry::Sequential(Box::new(system)));
        self
    }

    /// Add a parallel system.
    pub fn with_parallel_system<S: ParallelSystem + 'static>(mut self, system: S) -> Self {
        self.systems.push(SystemEntry::Parallel(Arc::new(system)));
        self
    }

    /// Add a source.
    #[cfg(feature = "io")]
    pub fn with_source<S: Source + 'static>(mut self, component: &'static str, source: S) -> Self {
        self.sources.push((component, Box::new(source)));
        self
    }

    /// Add a sink.
    #[cfg(feature = "io")]
    pub fn with_sink<K: Sink + 'static>(mut self, component: &'static str, sink: K) -> Self {
        self.sinks.push((component, Box::new(sink)));
        self
    }

    /// Consume the builder and return the configured [`Pipeline`].
    pub fn build(self) -> Pipeline {
        let mut p = Pipeline::new(self.name);
        p.data = self.data.build();
        p.systems = self.systems;
        #[cfg(feature = "io")]
        {
            p.sources = self.sources;
            p.sinks = self.sinks;
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::pipeline::Pipeline;
    use crate::system::{SystemMeta, system_fn};

    use crate::dataset::Dataset;

    #[derive(Serialize, Deserialize)]
    struct BuilderOrder {
        id: u64,
    }

    impl Component for BuilderOrder {
        fn name() -> &'static str {
            "BuilderOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
        }
    }

    #[test]
    fn test_pipeline_builder_with_system() {
        let p = Pipeline::builder("built")
            .with::<BuilderOrder>()
            .with_system(system_fn(
                SystemMeta::new("noop").read_component("BuilderOrder"),
                |_data: &mut Dataset| Ok(()),
            ))
            .build();

        assert_eq!(p.name(), "built");
        assert!(p.data.schemas().contains("BuilderOrder"));
    }
}
