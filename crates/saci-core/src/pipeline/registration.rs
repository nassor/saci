use std::sync::Arc;

use crate::system::{ParallelSystem, System};

#[cfg(feature = "io")]
use crate::io::{Sink, Source};

use super::Pipeline;
use super::dag::SystemEntry;

impl Pipeline {
    fn invalidate_cache(&mut self) {
        self.stages = std::sync::OnceLock::new();
        self.expanded_metas = std::sync::OnceLock::new();
        self.configs = std::sync::OnceLock::new();
    }

    /// Add a sequential [`System`].
    pub fn add_system<S: System + 'static>(&mut self, system: S) -> &mut Self {
        self.invalidate_cache();
        self.systems.push(SystemEntry::Sequential(Box::new(system)));
        self
    }

    /// Add a parallel [`ParallelSystem`].
    pub fn add_parallel_system<S: ParallelSystem + 'static>(&mut self, system: S) -> &mut Self {
        self.invalidate_cache();
        self.systems.push(SystemEntry::Parallel(Arc::new(system)));
        self
    }

    /// Add a pre-boxed sequential [`System`].
    pub fn add_system_boxed(&mut self, system: Box<dyn System>) -> &mut Self {
        self.invalidate_cache();
        self.systems.push(SystemEntry::Sequential(system));
        self
    }

    /// Add a pre-boxed parallel [`ParallelSystem`].
    pub fn add_parallel_system_boxed(&mut self, system: Box<dyn ParallelSystem>) -> &mut Self {
        self.invalidate_cache();
        self.systems.push(SystemEntry::Parallel(Arc::from(system)));
        self
    }

    /// Register a [`Source`] to drain into `component` before each
    /// [`run_with_io`](Self::run_with_io) call.
    #[cfg(feature = "io")]
    pub fn add_source<S: Source + 'static>(
        &mut self,
        component: &'static str,
        source: S,
    ) -> &mut Self {
        self.sources.push((component, Box::new(source)));
        self
    }

    /// Register a [`Sink`] to receive all rows of `component` after each
    /// [`run_with_io`](Self::run_with_io) call.
    #[cfg(feature = "io")]
    pub fn add_sink<K: Sink + 'static>(&mut self, component: &'static str, sink: K) -> &mut Self {
        self.sinks.push((component, Box::new(sink)));
        self
    }

    /// Query the [`pending_rows`](crate::io::sink::Sink::pending_rows) value
    /// for the sink registered under `component`, if any.
    ///
    /// Returns `None` if no sink is registered for `component` or if the sink
    /// does not implement backpressure probing.
    #[cfg(feature = "io")]
    pub fn sink_pending_rows(&self, component: &str) -> Option<usize> {
        self.sinks
            .iter()
            .find(|(comp, _)| *comp == component)
            .and_then(|(_, sink)| sink.pending_rows())
    }
}
