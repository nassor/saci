//! [`Pipeline`]: a self-contained workload of data, systems, DAG plan and optional IO.

pub(crate) use dag::SystemEntry;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow_schema::Schema;

use crate::component::Component;
use crate::dataset::Dataset;
#[cfg(feature = "engine")]
use crate::dataset::intern_component_name;
use crate::error::{SaciError, SaciResult};
use crate::schema::SchemaRegistry;
use crate::system::{ParallelSystem, System};

use self::dag::Plan;

#[cfg(feature = "engine")]
use crate::io::{Sink, Source};

/// Statistics of one [`run`](Pipeline::run), [`run_on`](Pipeline::run_on)
/// or [`run_stream`](Pipeline::run_stream) call.
#[derive(Copy, Clone, Debug, Default)]
pub struct RunStats {
    /// Net change in live rows (positive = rows added, negative = rows deleted).
    pub rows_produced: isize,
    /// Number of systems that ran.
    pub systems_run: usize,
    /// Wall-clock time of the call.
    pub elapsed: Duration,
    /// Total number of system retries during the call (sum across all
    /// systems; one retry = one failed attempt before a later success).
    pub retries: u32,
}

/// A self-contained workload: columnar data, systems, DAG plan, and optional
/// IO sources and sinks.
///
/// Build one with a chain from [`Pipeline::new`], then call
/// [`run`](Self::run):
///
/// ```rust
/// use saci::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Component, Serialize, Deserialize)]
/// struct Order { id: u64 }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> SaciResult<()> {
/// let mut pipeline = Pipeline::new("orders")
///     .component::<Order>()
///     .system(system_fn(SystemMeta::new("noop").reads(Order::ID), |_| Ok(())));
/// pipeline.data_mut().append(&[Order { id: 1 }])?;
/// let stats = pipeline.run().await?;
/// assert_eq!(stats.systems_run, 1);
/// # Ok(())
/// # }
/// ```
///
/// For running many pipelines from one process, see `Scheduler`.
pub struct Pipeline {
    name: Arc<str>,
    data: Dataset,
    systems: Vec<SystemEntry>,
    plan: OnceLock<Result<Plan, SaciError>>,
    /// The first construction error, returned by every later `plan`,
    /// `validate` and `run*` call.
    pending_error: Option<SaciError>,
    /// The one component carried across batches, when declared.
    state: Option<StateHooks>,
    #[cfg(feature = "engine")]
    sources: Vec<(&'static str, Box<dyn Source>)>,
    #[cfg(feature = "engine")]
    sinks: Vec<(&'static str, Box<dyn Sink>)>,
}

/// How a declared state component crosses a batch boundary.
struct StateHooks {
    component: &'static str,
    restore: fn(&mut Dataset, Option<&[u8]>) -> SaciResult<()>,
    capture: fn(&Dataset) -> SaciResult<Option<Vec<u8>>>,
}

impl Pipeline {
    /// Create an empty pipeline with the given name.
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            data: Dataset::new(),
            systems: Vec::new(),
            plan: OnceLock::new(),
            pending_error: None,
            state: None,
            #[cfg(feature = "engine")]
            sources: Vec::new(),
            #[cfg(feature = "engine")]
            sinks: Vec::new(),
        }
    }

    /// Keep the first construction error; later ones are consequences of it.
    fn record(&mut self, result: SaciResult<()>) {
        if let Err(e) = result
            && self.pending_error.is_none()
        {
            self.pending_error = Some(e);
        }
    }

    /// Register component `C` in the pipeline's dataset. A failure surfaces
    /// from the next [`validate`](Self::validate), [`plan`](Self::plan) or
    /// run.
    pub fn component<C: Component>(mut self) -> Self {
        if self
            .state
            .as_ref()
            .is_some_and(|state| state.component == C::name())
        {
            self.record(Err(state_is_component(C::name())));
            return self;
        }
        let result = self.data.register::<C>();
        self.record(result);
        self.plan = OnceLock::new();
        self
    }

    /// Declare `C` as the one component whose rows survive across batches.
    ///
    /// Before each batch the rows the previous one left behind arrive as a
    /// [`sdk::State<C>`](crate::sdk::State) resource; afterwards whatever the
    /// systems left there is the checkpoint
    /// ([`restore_state`](Self::restore_state),
    /// [`capture_state`](Self::capture_state)). `C` is state, not a batch
    /// component: registering it with [`component`](Self::component) too, or
    /// declaring a second state type, is a configuration error.
    pub fn state<C>(mut self) -> Self
    where
        C: Component + serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        if self.state.is_some() {
            self.record(Err(SaciError::configuration(
                "a pipeline carries one state type",
            )));
            return self;
        }
        if self.data.schemas().contains(C::name()) {
            self.record(Err(state_is_component(C::name())));
            return self;
        }
        self.state = Some(StateHooks {
            component: C::name(),
            restore: crate::sdk::state::restore::<C>,
            capture: crate::sdk::state::capture::<C>,
        });
        self
    }

    /// `true` when the pipeline declared a [`state`](Self::state) component.
    pub fn is_stateful(&self) -> bool {
        self.state.is_some()
    }

    /// Insert the state rows `prior` carries into `data`. A no-op for a
    /// pipeline with no state.
    ///
    /// # Errors
    ///
    /// `prior` does not decode as the state component.
    pub fn restore_state(&self, data: &mut Dataset, prior: Option<&[u8]>) -> SaciResult<()> {
        match &self.state {
            Some(state) => (state.restore)(data, prior),
            None => Ok(()),
        }
    }

    /// The checkpoint blob of the state rows the systems left on `data`;
    /// `None` for a pipeline with no state.
    ///
    /// # Errors
    ///
    /// The state resource is gone from `data`, or the rows do not encode.
    pub fn capture_state(&self, data: &Dataset) -> SaciResult<Option<Vec<u8>>> {
        match &self.state {
            Some(state) => (state.capture)(data),
            None => Ok(None),
        }
    }

    /// Register a component by name with a schema known only at runtime.
    pub fn schema(mut self, name: &str, schema: Arc<Schema>) -> Self {
        let result = self.data.register_schema(name, schema);
        self.record(result);
        self.plan = OnceLock::new();
        self
    }

    /// Insert (or replace) resource `R` in the pipeline's dataset.
    pub fn resource<R: Send + Sync + 'static>(mut self, r: R) -> Self {
        self.data.insert_resource(r);
        self
    }

    /// Add a sequential [`System`].
    pub fn system<S: System + 'static>(self, system: S) -> Self {
        self.system_boxed(Box::new(system))
    }

    /// Add a [`ParallelSystem`].
    pub fn parallel_system<S: ParallelSystem + 'static>(self, system: S) -> Self {
        self.push(SystemEntry::Parallel(Arc::new(system)))
    }

    /// Add a pre-boxed sequential [`System`].
    pub fn system_boxed(self, system: Box<dyn System>) -> Self {
        self.push(SystemEntry::Sequential(system))
    }

    /// Add a pre-boxed [`ParallelSystem`].
    pub fn parallel_system_boxed(self, system: Box<dyn ParallelSystem>) -> Self {
        self.push(SystemEntry::Parallel(Arc::from(system)))
    }

    fn push(mut self, entry: SystemEntry) -> Self {
        self.systems.push(entry);
        self.plan = OnceLock::new();
        self
    }

    /// Drain `source` into `component` at the start of every
    /// [`run`](Self::run).
    #[cfg(feature = "engine")]
    pub fn source<S: Source + 'static>(mut self, component: &str, source: S) -> Self {
        self.sources
            .push((intern_component_name(component), Box::new(source)));
        self
    }

    /// Write every row of `component` to `sink` at the end of every
    /// [`run`](Self::run), then finish it.
    #[cfg(feature = "engine")]
    pub fn sink<K: Sink + 'static>(mut self, component: &str, sink: K) -> Self {
        self.sinks
            .push((intern_component_name(component), Box::new(sink)));
        self
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

    /// Validate every declared field access against the registered schemas.
    ///
    /// # Errors
    ///
    /// The first construction error, or a `Configuration` error naming the
    /// access that matches no registered field.
    pub fn validate(&self) -> SaciResult<()> {
        self.plan().map(|_| ())
    }

    /// The execution plan: stages of system indices, each stage's systems
    /// free of conflicting access.
    ///
    /// # Errors
    ///
    /// As [`validate`](Self::validate).
    pub fn plan(&self) -> SaciResult<&[Vec<usize>]> {
        ensure_plan(
            &self.plan,
            self.pending_error.as_ref(),
            &self.systems,
            self.data.schemas(),
        )
        .map(|plan| plan.stages.as_slice())
    }

    /// The [`pending_rows`](crate::io::Sink::pending_rows) value of the sink
    /// attached to `component`, if any.
    ///
    /// `None` if no sink is attached for `component` or the sink does not
    /// implement backpressure probing.
    #[cfg(feature = "engine")]
    pub fn sink_pending_rows(&self, component: &str) -> Option<usize> {
        self.sinks
            .iter()
            .find(|(comp, _)| *comp == component)
            .and_then(|(_, sink)| sink.pending_rows())
    }
}

fn state_is_component(name: &str) -> SaciError {
    SaciError::configuration(format!(
        "{name} is this pipeline's state; do not also register it as a component"
    ))
}

/// The cached plan, built from `systems` and `schemas` on first use.
fn ensure_plan<'a>(
    cell: &'a OnceLock<Result<Plan, SaciError>>,
    pending_error: Option<&SaciError>,
    systems: &[SystemEntry],
    schemas: &SchemaRegistry,
) -> SaciResult<&'a Plan> {
    if let Some(e) = pending_error {
        return Err(e.clone());
    }
    cell.get_or_init(|| Plan::build(systems, schemas))
        .as_ref()
        .map_err(Clone::clone)
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new("default")
    }
}

mod dag;
mod execution;

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{DataType, Field};
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

    #[derive(Serialize, Deserialize)]
    struct LateOrder {
        id: u64,
    }
    impl Component for LateOrder {
        fn name() -> &'static str {
            "LateOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
        }
    }

    #[test]
    fn test_pipeline_name() {
        let p = Pipeline::new("my-pipeline");
        assert_eq!(p.name(), "my-pipeline");
    }

    #[test]
    fn test_data_mut_accessible() {
        let mut p = Pipeline::new("data-test").component::<RootOrder>();
        p.data_mut()
            .append::<RootOrder>(&[RootOrder { id: 1 }])
            .unwrap();
        assert_eq!(p.data().rows(), 1);
    }

    /// Registering a component once rows exist fails inside the chain; the
    /// error waits for `validate` and comes back from every later call.
    #[test]
    fn a_registration_error_surfaces_from_validate() {
        let mut p = Pipeline::new("late").component::<RootOrder>();
        p.data_mut()
            .append::<RootOrder>(&[RootOrder { id: 1 }])
            .unwrap();
        let p = p.component::<LateOrder>();
        let err = p.validate().unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.to_string().contains("LateOrder"), "{err}");
        assert_eq!(p.plan().unwrap_err(), err);
    }

    #[test]
    fn declared_state_round_trips_through_capture_and_restore() {
        let p = Pipeline::new("stateful").state::<RootOrder>();
        assert!(p.is_stateful());

        let mut data = Dataset::new();
        p.restore_state(&mut data, None).unwrap();
        data.resource_mut::<crate::sdk::State<RootOrder>>()
            .unwrap()
            .rows
            .push(RootOrder { id: 9 });
        let blob = p
            .capture_state(&data)
            .unwrap()
            .expect("a stateful pipeline checkpoints");

        let mut next = Dataset::new();
        p.restore_state(&mut next, Some(&blob)).unwrap();
        let rows = &next
            .resource::<crate::sdk::State<RootOrder>>()
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 9);
    }

    #[test]
    fn a_stateless_pipeline_captures_nothing() {
        let p = Pipeline::new("stateless");
        let mut data = Dataset::new();
        p.restore_state(&mut data, Some(&[1u8, 2, 3][..])).unwrap();
        assert_eq!(p.capture_state(&data).unwrap(), None);
        assert!(!p.is_stateful());
    }

    #[test]
    fn state_that_is_also_a_component_fails_validate() {
        for p in [
            Pipeline::new("a")
                .state::<RootOrder>()
                .component::<RootOrder>(),
            Pipeline::new("b")
                .component::<RootOrder>()
                .state::<RootOrder>(),
        ] {
            let err = p.validate().unwrap_err();
            assert!(
                err.to_string().contains("is this pipeline's state"),
                "{err}"
            );
        }
        let err = Pipeline::new("c")
            .state::<RootOrder>()
            .state::<LateOrder>()
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("one state type"), "{err}");
    }
}
