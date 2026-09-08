//! [`PipelineRuntime`], the host-side seam for swappable pipeline implementations.
//!
//! A host (standalone runner, `DistributedRunner`, service builder) holds
//! `Box<dyn PipelineRuntime>` instead of a concrete [`Pipeline`]. Three impls exist:
//! [`Pipeline`] in this crate wraps [`Pipeline::run_on`], and in `saci-service`
//! `WasmPipelineRuntime` serializes the dataset to Arrow IPC and calls a processor
//! component's `run-batch` export through wasmtime, while `NativePluginRuntime`
//! calls the `run_batch` slot of a dlopened cdylib's `saci-plugin-abi` vtable.
//!
//! The trait only exists when the `runtime` feature is enabled. Processor builds
//! (`--no-default-features --features processor`) use [`Pipeline`] directly through
//! the sync executor in the processor SDK and never construct a `dyn PipelineRuntime`.

use async_trait::async_trait;

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

/// What a [`PipelineRuntime::run_on_with_state_and_routes`] call produced.
#[derive(Debug, Default, Clone)]
pub struct RuntimeOutput {
    /// Opaque state blob, persisted verbatim — the value
    /// [`run_on_with_state`](PipelineRuntime::run_on_with_state) returns.
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
/// produced by `run_on` need not be `Send`: the host always drives them to
/// completion from the task that owns the runtime, and `Pipeline` holds
/// `Box<dyn Sink>` which may not be `Sync`, so `Send` futures would require
/// `Pipeline: Sync`.
#[async_trait(?Send)]
pub trait PipelineRuntime: Send {
    /// Human-readable name for logging, metrics, and status endpoints.
    fn name(&self) -> &str;

    /// Execute all systems against the provided dataset.
    ///
    /// `data` is owned by the caller. Sources and sinks are **not** drained
    /// here; the host layer handles IO before and after this call.
    async fn run_on(&self, data: &mut Dataset) -> SaciResult<()>;

    /// Execute against `data`, carrying opaque runtime-internal state.
    ///
    /// `prior` is the blob this runtime returned from the previous call for the
    /// same logical partition, or `None` on first run. The returned blob is
    /// persisted verbatim by the host and handed back as the next `prior`. Hosts
    /// must treat it as opaque; only the runtime knows the layout.
    ///
    /// The default implementation ignores `prior`, delegates to
    /// [`run_on`](Self::run_on), and returns `Ok(None)`, which is correct for any
    /// runtime whose state lives entirely in the `Dataset`.
    async fn run_on_with_state(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<Option<Vec<u8>>> {
        let _ = prior;
        self.run_on(data).await?;
        Ok(None)
    }

    /// Execute against `data`, carrying opaque state, and return the batch's
    /// routing decision alongside it.
    ///
    /// The default implementation delegates to [`run_on_with_state`](Self::run_on_with_state)
    /// and reports no routes, which is correct for any runtime that cannot route.
    async fn run_on_with_state_and_routes(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput> {
        let state = self.run_on_with_state(data, prior).await?;
        Ok(RuntimeOutput {
            state,
            routes: None,
        })
    }

    /// Component names this runtime expects to find in the dataset.
    ///
    /// Called at load and validation time only. Hosts use the list to check the
    /// `component` a source or sink node declares in the config file against
    /// the components the runtime handles, before the first `run_on` call.
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

#[async_trait(?Send)]
impl PipelineRuntime for Pipeline {
    fn name(&self) -> &str {
        Pipeline::name(self)
    }

    async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
        Pipeline::run_on(self, data).await
    }

    fn declared_components(&self) -> Vec<&str> {
        self.data.schemas().iter().map(|(k, _)| *k).collect()
    }

    fn template_dataset(&self) -> Dataset {
        self.data.clone_empty()
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
        p.data_mut().register_component::<Alpha>().unwrap();
        p.data_mut().register_component::<Beta>().unwrap();
        p.data_mut().register_component::<Gamma>().unwrap();

        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let mut names = rt.declared_components();
        names.sort();
        assert_eq!(names, vec!["Alpha", "Beta", "Gamma"]);
    }

    #[tokio::test]
    async fn run_on_dispatches_through_trait_object() {
        let p = Pipeline::new("empty");
        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let mut data = Dataset::new();
        rt.run_on(&mut data).await.unwrap();
    }

    #[test]
    fn template_dataset_preserves_registered_schemas() {
        let mut p = Pipeline::new("trio");
        p.data_mut().register_component::<Alpha>().unwrap();
        p.data_mut().register_component::<Beta>().unwrap();
        p.data_mut().register_component::<Gamma>().unwrap();

        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let tmpl = rt.template_dataset();

        assert!(tmpl.schemas().contains("Alpha"));
        assert!(tmpl.schemas().contains("Beta"));
        assert!(tmpl.schemas().contains("Gamma"));
        assert_eq!(tmpl.rows(), 0);
    }
}
