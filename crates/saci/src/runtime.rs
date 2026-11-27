//! [`PipelineRuntime`], the host-side seam for swappable pipeline implementations.
//!
//! A host (standalone runner, `DistributedRunner`, service builder) holds
//! `Box<dyn PipelineRuntime>` instead of a concrete [`Pipeline`]. Three impls exist:
//! [`Pipeline`] in this crate restores its state, runs [`Pipeline::run_on`] and
//! captures its state again, and in the host `WasmPipelineRuntime` serializes the
//! dataset to Arrow IPC and calls a processor component's `run-batch` export
//! through wasmtime, while `NativePluginRuntime` calls the `run_batch` slot of a
//! dlopened cdylib's `crate::plugin_abi` vtable.
//!
//! The trait only exists when the `engine` feature is enabled. Processor builds
//! (`--no-default-features --features processor`) use [`Pipeline`] directly through
//! the sync executor in the processor SDK and never construct a `dyn PipelineRuntime`.

use async_trait::async_trait;

use crate::sdk::RouteDecision;
use crate::{Dataset, Pipeline, SaciResult};

/// What a runtime reports about itself beyond [`PipelineRuntime::name`].
///
/// A host holding `Box<dyn PipelineRuntime>` has erased the concrete runtime,
/// so the out-of-process runtimes' self-description (a processor component's
/// `describe()` record, a native plugin's manifest) is otherwise unreachable.
/// This is the one generic way to read it.
///
/// A native [`Pipeline`] has no self-description concept beyond
/// [`PipelineRuntime::name`], so it keeps the empty default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeDescriptorInfo {
    /// The identity the runtime declares for **itself**, empty when it declares
    /// none.
    ///
    /// Not the same thing as [`PipelineRuntime::name`], which is whatever the
    /// host passed at construction: the declared node id for a config-loaded
    /// WASM node, the manifest name for a native plugin, the pipeline's own
    /// name for a native [`Pipeline`]. A WASM processor's `describe()` reports
    /// the name the processor author chose, and that is what identifies which
    /// artifact is actually loaded.
    pub name: String,
    /// Version string the runtime reports, empty when it reports none.
    pub version: String,
    /// Whether the runtime carries state across batch boundaries.
    pub stateful: bool,
    /// Fingerprint of the runtime's component schemas, empty when it reports
    /// none.
    pub schema_fingerprint: String,
}

/// What a [`PipelineRuntime::run_batch`] call produced.
#[derive(Debug, Default, Clone)]
pub struct RuntimeOutput {
    /// Opaque state blob, persisted verbatim and handed back as the next
    /// call's `prior`.
    pub state: Option<Vec<u8>>,
    /// Branch names this batch's output is delivered to. `None` preserves
    /// legacy behaviour: the host multicasts to every downstream link.
    /// `Some(vec![])` routes the output nowhere.
    pub routes: Option<Vec<String>>,
}

/// Swappable host-side execution backend for a single pipeline.
///
/// Host components hold a `Box<dyn PipelineRuntime>` so they can drive either a
/// native [`Pipeline`] or a WASM processor module through the same call site.
///
/// Implementations must be `Send` so the box can move between threads. Futures
/// produced by `run_batch` need not be `Send`: the host always drives them to
/// completion from the task that owns the runtime, and `Pipeline` holds
/// `Box<dyn Sink>` which may not be `Sync`, so `Send` futures would require
/// `Pipeline: Sync`.
#[async_trait(?Send)]
pub trait PipelineRuntime: Send {
    /// Human-readable name for logging, metrics, and status endpoints.
    fn name(&self) -> &str;

    /// Execute all systems against the provided dataset, carrying opaque
    /// runtime-internal state, and return the batch's state and routing
    /// decision.
    ///
    /// `data` is owned by the caller. Sources and sinks are **not** drained
    /// here; the host layer handles IO before and after this call.
    ///
    /// `prior` is the blob this runtime returned in [`RuntimeOutput::state`]
    /// from the previous call for the same logical partition, or `None` on
    /// first run. The host persists the returned blob verbatim and hands it
    /// back as the next `prior`; only the runtime knows the layout. A runtime
    /// whose state lives entirely in the `Dataset` ignores `prior` and returns
    /// [`RuntimeOutput::default`], which carries no state and no routes.
    async fn run_batch(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput>;

    /// Component names this runtime expects to find in the dataset.
    ///
    /// Called at load and validation time only. Hosts use the list to check the
    /// `component` a source or sink node declares in the config file against
    /// the components the runtime handles, before the first `run_batch` call.
    ///
    /// The default returns an empty slice, which suffices for runtimes that
    /// validate internally.
    fn declared_components(&self) -> Vec<&str> {
        Vec::new()
    }

    /// What this runtime reports about itself beyond its name.
    ///
    /// Called by status and inspection surfaces, never on the execution path.
    /// The default returns an empty [`RuntimeDescriptorInfo`], which is what a
    /// runtime with no separate self-description has to report.
    fn descriptor_info(&self) -> RuntimeDescriptorInfo {
        RuntimeDescriptorInfo::default()
    }

    /// Return a schema-only, empty [`Dataset`] matching this runtime's components.
    ///
    /// Called once by host runners at construction time to seed the dataset used
    /// in each processing iteration. Every component schema must be registered in
    /// the returned dataset's `SchemaRegistry`; resources and row data are not
    /// required.
    ///
    /// There is no default body: a schemaless fallback would surface later as an
    /// opaque `ensure_plan` failure. A runtime that cannot build a valid template
    /// (e.g. corrupted processor describe) must fail in its constructor instead.
    fn template_dataset(&self) -> Dataset;
}

/// A [`PipelineRuntime`] whose state lives entirely in the [`Dataset`].
///
/// Most runtimes carry no state between batches: they read the dataset, write
/// the dataset, and ignore the `prior` blob and the routing decision that
/// [`PipelineRuntime::run_batch`] exposes. Implement this trait instead of
/// `PipelineRuntime` for those: a blanket impl supplies `run_batch`, passing
/// `data` to [`run`](Self::run) and returning [`RuntimeOutput::default`], which
/// carries no state and no routes.
///
/// A runtime that persists state across batches or routes rows to branches
/// implements `PipelineRuntime` directly instead. A type must not implement
/// both traits: the blanket impl would conflict with the direct one, and the
/// compiler rejects it.
///
/// The bounds match [`PipelineRuntime`]: implementations are `Send`, and the
/// future returned by `run` need not be.
///
/// ```
/// use saci::prelude::*;
/// use saci::runtime::StatelessRuntime;
///
/// struct Idle;
///
/// #[async_trait(?Send)]
/// impl StatelessRuntime for Idle {
///     fn name(&self) -> &str {
///         "idle"
///     }
///
///     async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
///         Ok(())
///     }
///
///     fn template_dataset(&self) -> Dataset {
///         Dataset::new()
///     }
/// }
///
/// let runtime: Box<dyn PipelineRuntime> = Box::new(Idle);
/// assert_eq!(runtime.name(), "idle");
/// ```
///
/// `name`, `declared_components`, `descriptor_info` and `template_dataset`
/// exist on both traits, so a scope that imports both and calls one of them
/// on the concrete type is ambiguous (E0034). Import only the trait you call
/// through, or call `PipelineRuntime::run_batch(&runtime, ..)` by path.
#[async_trait(?Send)]
pub trait StatelessRuntime: Send {
    /// Human-readable name for logging, metrics, and status endpoints.
    ///
    /// Same contract as [`PipelineRuntime::name`].
    fn name(&self) -> &str;

    /// Execute all systems against the provided dataset.
    ///
    /// `data` is owned by the caller. Sources and sinks are **not** drained
    /// here; the host layer handles IO before and after this call.
    async fn run(&self, data: &mut Dataset) -> SaciResult<()>;

    /// Component names this runtime expects to find in the dataset.
    ///
    /// Same contract as [`PipelineRuntime::declared_components`]; the default
    /// returns an empty list.
    fn declared_components(&self) -> Vec<&str> {
        Vec::new()
    }

    /// What this runtime reports about itself beyond its name.
    ///
    /// Same contract as [`PipelineRuntime::descriptor_info`]; the default
    /// returns an empty [`RuntimeDescriptorInfo`].
    fn descriptor_info(&self) -> RuntimeDescriptorInfo {
        RuntimeDescriptorInfo::default()
    }

    /// Return a schema-only, empty [`Dataset`] matching this runtime's components.
    ///
    /// Same contract as [`PipelineRuntime::template_dataset`]; there is no
    /// default body.
    fn template_dataset(&self) -> Dataset;
}

#[async_trait(?Send)]
impl<T: StatelessRuntime> PipelineRuntime for T {
    fn name(&self) -> &str {
        StatelessRuntime::name(self)
    }

    async fn run_batch(
        &self,
        data: &mut Dataset,
        _prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput> {
        self.run(data).await?;
        Ok(RuntimeOutput::default())
    }

    fn declared_components(&self) -> Vec<&str> {
        StatelessRuntime::declared_components(self)
    }

    fn descriptor_info(&self) -> RuntimeDescriptorInfo {
        StatelessRuntime::descriptor_info(self)
    }

    fn template_dataset(&self) -> Dataset {
        StatelessRuntime::template_dataset(self)
    }
}

#[async_trait(?Send)]
impl PipelineRuntime for Pipeline {
    fn name(&self) -> &str {
        Pipeline::name(self)
    }

    async fn run_batch(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput> {
        self.restore_state(data, prior)?;
        Pipeline::run_on(self, data).await?;
        let state = self.capture_state(data)?;
        let routes = data
            .take_resource::<RouteDecision>()
            .map(RouteDecision::into_branches);
        Ok(RuntimeOutput { state, routes })
    }

    fn declared_components(&self) -> Vec<&str> {
        self.data().schemas().iter().map(|(k, _)| *k).collect()
    }

    fn template_dataset(&self) -> Dataset {
        self.data().clone_empty()
    }

    fn descriptor_info(&self) -> RuntimeDescriptorInfo {
        RuntimeDescriptorInfo {
            stateful: self.is_stateful(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::Component;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    struct Alpha;
    impl Component for Alpha {
        fn name() -> &'static str {
            "Alpha"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
        }
    }

    struct Beta;
    impl Component for Beta {
        fn name() -> &'static str {
            "Beta"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]))
        }
    }

    struct Gamma;
    impl Component for Gamma {
        fn name() -> &'static str {
            "Gamma"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]))
        }
    }

    #[test]
    fn pipeline_impls_runtime_name() {
        let p = Pipeline::new("example");
        let rt: &dyn PipelineRuntime = &p;
        assert_eq!(rt.name(), "example");
    }

    #[test]
    fn descriptor_info_defaults_to_empty_for_native_pipeline() {
        let p = Pipeline::new("example");
        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let info = rt.descriptor_info();
        assert_eq!(
            info,
            RuntimeDescriptorInfo::default(),
            "a native pipeline declares no identity of its own beyond name()"
        );
        assert_eq!(rt.name(), "example", "name() is still the host's name");
    }

    #[test]
    fn declared_components_lists_registered_schemas() {
        let mut p = Pipeline::new("trio");
        p.data_mut().register::<Alpha>().unwrap();
        p.data_mut().register::<Beta>().unwrap();
        p.data_mut().register::<Gamma>().unwrap();

        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let mut names = rt.declared_components();
        names.sort();
        assert_eq!(names, vec!["Alpha", "Beta", "Gamma"]);
    }

    #[tokio::test]
    async fn run_batch_dispatches_through_trait_object() {
        let p = Pipeline::new("empty");
        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let mut data = Dataset::new();
        let out = rt.run_batch(&mut data, None).await.unwrap();
        assert!(out.state.is_none());
        assert!(out.routes.is_none());
    }

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Counter {
        n: i64,
    }
    impl Component for Counter {
        fn name() -> &'static str {
            "Counter"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
        }
    }

    /// Counts batches in its declared state and routes every even batch to
    /// the `even` branch.
    fn counting_pipeline() -> Pipeline {
        use crate::sdk::State;
        use crate::{SystemMeta, system_fn};

        Pipeline::new("counting")
            .state::<Counter>()
            .system(system_fn(
                SystemMeta::new("count").writes_resource::<State<Counter>>(),
                |data| {
                    let n = {
                        let counter = State::<Counter>::get_or_insert_default(data)?;
                        counter.n += 1;
                        counter.n
                    };
                    if n % 2 == 0 {
                        data.insert_resource(RouteDecision::to(["even"]));
                    }
                    Ok(())
                },
            ))
    }

    #[tokio::test]
    async fn pipeline_run_batch_threads_state_and_reports_the_route_decision() {
        let rt: Box<dyn PipelineRuntime> = Box::new(counting_pipeline());

        let mut first = Dataset::new();
        let out = rt.run_batch(&mut first, None).await.unwrap();
        let blob = out.state.expect("a stateful pipeline checkpoints");
        assert_eq!(out.routes, None, "no decision on the first batch");

        let mut second = Dataset::new();
        let out = rt.run_batch(&mut second, Some(&blob)).await.unwrap();
        assert_eq!(out.routes, Some(vec!["even".to_string()]));
        assert!(
            second.take_resource::<RouteDecision>().is_none(),
            "the decision leaves the dataset with the output"
        );

        let reader = counting_pipeline();
        let mut restored = Dataset::new();
        reader
            .restore_state(&mut restored, out.state.as_deref())
            .unwrap();
        let counter = restored
            .resource::<crate::sdk::State<Counter>>()
            .unwrap()
            .rows
            .first()
            .map(|c| c.n);
        assert_eq!(
            counter,
            Some(2),
            "the checkpoint carries the second batch's count"
        );
    }

    #[test]
    fn template_dataset_preserves_registered_schemas() {
        let mut p = Pipeline::new("trio");
        p.data_mut().register::<Alpha>().unwrap();
        p.data_mut().register::<Beta>().unwrap();
        p.data_mut().register::<Gamma>().unwrap();

        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let tmpl = rt.template_dataset();

        assert!(tmpl.schemas().contains("Alpha"));
        assert!(tmpl.schemas().contains("Beta"));
        assert!(tmpl.schemas().contains("Gamma"));
        assert_eq!(tmpl.rows(), 0);
    }

    struct Counting {
        seen: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait(?Send)]
    impl StatelessRuntime for Counting {
        fn name(&self) -> &str {
            "counting"
        }

        async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
            self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        fn declared_components(&self) -> Vec<&str> {
            vec!["Alpha"]
        }

        fn template_dataset(&self) -> Dataset {
            Dataset::new().with::<Alpha>().unwrap()
        }
    }

    #[tokio::test]
    async fn stateless_runtime_is_a_pipeline_runtime_that_ignores_prior() {
        let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let rt: Box<dyn PipelineRuntime> = Box::new(Counting {
            seen: std::sync::Arc::clone(&seen),
        });
        assert_eq!(rt.name(), "counting");
        assert_eq!(rt.declared_components(), vec!["Alpha"]);
        assert_eq!(rt.descriptor_info(), RuntimeDescriptorInfo::default());
        assert!(rt.template_dataset().schemas().contains("Alpha"));

        let mut data = rt.template_dataset();
        let out = rt.run_batch(&mut data, Some(b"ignored")).await.unwrap();
        assert_eq!(out.state, None);
        assert_eq!(out.routes, None);
        assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
