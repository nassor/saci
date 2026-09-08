//! [`ServiceBuilder`]: assembles a [`BuiltService`] per workflow from config
//! and registry.
//!
//! `ServiceBuilder` is the integration point between the configuration file and
//! the SACI runtime. It holds a [`Registry`] of user-provided IO factories and,
//! given a [`ServiceConfig`], instantiates every declared node of every
//! declared `workflow`: sources, sinks, transformers, and `wasm`/`plugin`
//! processors.
//!
//! ## Usage
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use saci_service::service::builder::ServiceBuilder;
//! use saci_service::pipeline::Pipeline;
//!
//! let pipeline = Pipeline::new("my_pipeline");
//! let _builder = ServiceBuilder::new().with_runtime("my-processor", Box::new(pipeline));
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{SaciError, SaciResult};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_core::runtime::PipelineRuntime;

use super::config::{
    HealConfig, NodeKind, RunMode, ServiceConfig, ServiceMode, TransformerSpec, WorkflowSpec,
};
use super::dlq::{DeadLetterQueue, DlqRegistry, DlqShared};
use super::factories::{missing_factory_error, missing_transformer_error};
use super::heal::{HealSettings, HealingSink, HealingSource, Rebuilder};
#[cfg(feature = "wasm")]
use super::loader::{LocalModuleResolver, PipelineRuntimeLoader};
#[cfg(feature = "plugin")]
use super::plugin_loader::load_plugin_runtime;
use super::registry::{Registry, SinkFactory, SourceFactory, TransformerFactory};
use crate::inspector::Inspector;
#[cfg(feature = "wasm")]
use crate::wasm::WasmEngine;
use saci_connector::{ChannelBridge, NodeIdentity};
use saci_transformer::Transformer;

use super::topology::build_topology;

// The "no runtime" help text names how a processor node can be given a
// runtime, so it has one arm per combination of `wasm` and `plugin`. With
// neither feature there is no `wasm`/`plugin` node to build, so neither of the
// two error paths that word this help text is compiled in.
#[cfg(all(feature = "wasm", not(feature = "plugin")))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::with_runtime(id, ..) or set 'module' on the wasm node";
#[cfg(all(feature = "plugin", not(feature = "wasm")))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::with_runtime(id, ..) \
     or set 'library' on the plugin node";
#[cfg(all(feature = "wasm", feature = "plugin"))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::with_runtime(id, ..) \
     or set 'module'/'library' on the node";

/// Whether a config-driven source is handed to the runner wrapped in
/// [`RetryingSource`](saci_core::io::retry::RetryingSource).
///
/// Sinks always take their wrapper: a `write_batch` the runner cannot re-drive
/// is lost rows. A source's answer depends on who owns its error policy.
///
/// - Batch modes (`one_shot`, `interval`, `continuous`) pull each source
///   through `next_batch` themselves, under that source's admission credit: an
///   error there logs `source drain error (continuing)`, counts one
///   `iteration_errors`, marks the source failed and ends its drain for that
///   pass. The wrapper is what turns a transient failure into a retried read
///   instead of a source contributing nothing to the iteration. Cluster mode
///   declares no source node at all, so its answer never applies.
/// - `run_mode kind="stream"` is itself the retry loop: `run_stream` polls the
///   source once per item, and on an error logs it, counts it in
///   `iteration_errors` and re-polls after a cancellable
///   `SOURCE_ERROR_BACKOFF`. Wrapping there nests a second retry loop inside
///   the first, on the item path, with a first backoff an order of magnitude
///   longer than the runner's own, so a stream source is handed over
///   unwrapped and the runner's documented policy is the only one that runs.
fn sources_take_retry_wrapper(config: &ServiceConfig) -> bool {
    !matches!(
        &config.mode,
        ServiceMode::Standalone { config: sc } if sc.run_mode == RunMode::Stream
    )
}

/// Why a declared workflow cannot be torn down and built a second time, or
/// `None` when it can.
///
/// The lifecycle control plane reports this as
/// `WorkflowStatus::restart_blocked_reason` and refuses `start`, `stop` and
/// `restart` for such a workflow, `stop` included, because stopping something
/// that cannot be started again leaves the operator with no way back.
/// `pause`/`resume` stay available: they keep the runner and every resource it
/// holds alive.
pub fn rebuild_blocker(workflow: &WorkflowSpec) -> Option<&'static str> {
    if workflow.wasm.iter().any(|spec| spec.module.is_none()) {
        return Some(INJECTED_RUNTIME_BLOCKER);
    }
    if workflow.plugin.iter().any(|spec| spec.library.is_none()) {
        return Some(INJECTED_RUNTIME_BLOCKER);
    }
    let channel_source = workflow
        .sources
        .iter()
        .any(|spec| spec.type_name == "ChannelSource");
    let channel_sink = workflow
        .sinks
        .iter()
        .any(|spec| spec.type_name == "ChannelSink");
    if channel_source || channel_sink {
        return Some(CHANNEL_BLOCKER);
    }
    None
}

/// A processor node with no artifact took its runtime from
/// [`ServiceBuilder::with_runtime`], and `build_processor_node` *removes* it
/// from the builder, so the second build of that workflow has nothing to give
/// it.
///
/// Unconditional because `rebuild_blocker` reads both processor vectors in
/// every build, the same way the config declares them.
const INJECTED_RUNTIME_BLOCKER: &str = "a processor node declares no module or library, so its \
     runtime was supplied through ServiceBuilder::with_runtime and exists only once";

/// `ChannelRegistry` latches each named half as it is built and refuses a
/// second one, and the sink being the channel's only `Sender` is what gives
/// the consumer a real EOF.
const CHANNEL_BLOCKER: &str =
    "a ChannelSource or ChannelSink is bound to one mpsc pair created once per process";

/// The self-healing policy one node runs under, or `None` when it is not
/// wrapped at all.
///
/// `rebuildable` is what the node's factory answered for its own config.
/// Healing is on by default, so a connector that cannot be rebuilt is simply
/// left alone under the inherited policy: a channel half appears in most
/// multi-workflow configs and is not an event. A node that declares its
/// **own** `heal` block asked for it by name, so the same answer is an error
/// there instead: a block that quietly does nothing is worse than a refusal.
/// A node-level block setting `enabled #false` resolves to disabled and never
/// reaches that arm.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] naming the workflow, the node and the
/// factory's own reason when a node-level `heal` block asks for what its
/// connector cannot give.
fn resolve_heal(
    local: Option<&HealConfig>,
    global: &HealConfig,
    rebuildable: Result<(), &'static str>,
    workflow_id: &str,
    role: &str,
    node_id: &str,
    type_name: &str,
) -> SaciResult<Option<HealSettings>> {
    let settings = local
        .cloned()
        .unwrap_or_default()
        .resolve(global, HealSettings::default());
    if !settings.enabled {
        return Ok(None);
    }
    match rebuildable {
        Ok(()) => Ok(Some(settings)),
        Err(reason) if local.is_some() => Err(SaciError::configuration(format!(
            "workflow '{workflow_id}' {role} '{node_id}': heal is not available for \
             type '{type_name}': {reason}"
        ))),
        Err(_) => Ok(None),
    }
}

/// What both node builders read that is the same for every node of one
/// workflow build.
///
/// Grouped rather than passed one by one, because a source node's builder
/// already carries its own id, spec and retry decision and these four are the
/// shared half.
struct NodeBuild<'a> {
    registry: &'a Arc<Registry>,
    transformers: &'a HashMap<String, Arc<dyn Transformer>>,
    heal: &'a HealConfig,
    /// `node.label()`, the service name a peer-facing connector announces.
    service: &'a str,
}

impl NodeBuild<'_> {
    /// Where the node `id` of `workflow` sits, as its connectors see it.
    fn identity(&self, workflow: &WorkflowSpec, id: &str) -> NodeIdentity {
        NodeIdentity {
            service: self.service.to_string(),
            workflow: workflow.id.clone(),
            node: id.to_string(),
        }
    }
}

/// A [`ServiceBuilder`] that has published its [`Registry`] and can build any
/// declared workflow again, which is what the lifecycle plane's `start` and
/// `restart` need.
///
/// [`ServiceBuilder::build_all`] consumes the builder, so nothing it produced
/// can be built twice; this keeps both halves alive instead.
pub struct ServiceFactory {
    builder: ServiceBuilder,
    registry: Arc<Registry>,
    retry_sources: bool,
    heal: HealConfig,
    /// `node.data_dir`, where a store that writes to local disk (today, a
    /// `dlq "redb"` block) puts its file.
    data_dir: std::path::PathBuf,
    /// `node.label()`, the service name a peer-facing connector announces.
    service: String,
}

impl ServiceFactory {
    /// Build one declared workflow.
    ///
    /// Calling this twice for the same workflow is only sound once the
    /// previous [`BuiltService`] has been dropped: a source or sink may hold
    /// an exclusive resource such as a listening port. See
    /// [`rebuild_blocker`] for the workflows where even that is not enough.
    ///
    /// # Errors
    ///
    /// Whatever [`ServiceBuilder::build_all`] would return for this workflow.
    pub fn build(&mut self, workflow: &WorkflowSpec) -> Result<BuiltService, SaciError> {
        self.builder.build_one(
            &self.registry,
            workflow,
            self.retry_sources,
            &self.heal,
            &self.data_dir,
            &self.service,
        )
    }

    /// Publish the topology of `built` into the inspector, if one is attached.
    pub fn publish_topology(&self, config: &ServiceConfig, built: &[BuiltService]) {
        if let Some(inspector) = &self.builder.inspector {
            let node_slices: Vec<&[BuiltNode]> = built.iter().map(|b| b.nodes.as_slice()).collect();
            inspector.set_topology(build_topology(
                config,
                &node_slices,
                inspector.topology().version + 1,
            ));
        }
    }

    /// The frozen registry every build shares.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
}

/// Intern `name` to a process-lifetime `&'static str`, deduplicating by
/// content so the same component name declared on several nodes leaks once.
///
/// `Dataset::append_record_batch`/`Dataset::batch_for` key components by
/// `&'static str`, matching every compile-time `Component::name()` in the
/// codebase; this is the one adapter from a KDL-declared, therefore runtime,
/// component name to that contract. Growth is bounded by the number of
/// distinct component names the config declares, which is fixed for the life
/// of the process. This mirrors `saci_core::dataset`'s own (crate-private)
/// component-name interner for the identical reason.
fn intern_component_name(name: &str) -> &'static str {
    static INTERNED: OnceLock<Mutex<std::collections::HashSet<&'static str>>> = OnceLock::new();
    let set = INTERNED.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
    let mut set = set
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(&existing) = set.get(name) {
        return existing;
    }
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    set.insert(leaked);
    leaked
}

/// What kind of execution backend a [`BuiltNode::kind`] holds.
pub enum BuiltNodeKind {
    /// A constructed IO source.
    Source(Box<dyn Source>),
    /// A constructed processor runtime.
    Processor {
        /// The execution backend.
        runtime: Box<dyn PipelineRuntime>,
        /// `"wasm"`, `"plugin"` or `"native"`.
        kind: &'static str,
    },
    /// A constructed IO sink.
    Sink(Box<dyn Sink>),
}

/// One declared outbound edge of a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltEdge {
    /// Index into [`BuiltService::nodes`]. Always greater than the owning
    /// node's index, because `nodes` is in topological order.
    pub node: usize,
    /// Branch name this edge carries; `None` for an unlabelled link.
    pub branch: Option<String>,
}

/// Whether a routing decision selects an edge. `None` routes (legacy) select
/// everything; otherwise only edges whose branch is named are selected.
pub(crate) fn edge_selected(routes: &Option<Vec<String>>, branch: &Option<String>) -> bool {
    match routes {
        None => true,
        Some(routes) => match branch {
            Some(name) => routes.iter().any(|r| r == name),
            None => false,
        },
    }
}

/// One assembled node of the workflow graph.
pub struct BuiltNode {
    /// The declared id.
    pub id: String,
    /// Declared name, absent when the config named none.
    pub name: Option<String>,
    /// Connector `type` for a source or sink; the runtime kind
    /// (`"wasm"`/`"plugin"`/`"native"`) for a processor.
    pub type_name: String,
    /// Component a source writes or a sink reads. `None` for a processor.
    ///
    /// Leaked to `'static` once at build time: it is a span field and a
    /// `Dataset` key on every iteration.
    pub component: Option<&'static str>,
    /// The constructed execution backend.
    pub kind: BuiltNodeKind,
    /// Outbound edges into [`BuiltService::nodes`]. Always greater than this
    /// node's own index, because `nodes` is in topological order.
    pub downstream: Vec<BuiltEdge>,
    /// Artifact path for a `wasm`/`plugin` processor, for the topology
    /// detail. `None` for a source, a sink, or a processor whose runtime was
    /// supplied through [`ServiceBuilder::with_runtime`].
    pub artifact: Option<String>,
    /// A healed sink node's recovered flag: latched `true` the moment a
    /// rebuilt instance's first write succeeds, and cleared by whoever reads
    /// it. `None` for an unhealed sink, and for every source and processor
    /// node. The dead letter queue reads it to replay the moment a sink
    /// comes back rather than waiting out its own backoff.
    pub heal_recovered: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// The node's windowing declaration, when its config declared a `window`
    /// block. `None` for a non-windowed processor and for every source or
    /// sink. The runners use it to track the node's watermark; the topology
    /// uses it to describe the node.
    #[cfg(feature = "windows")]
    pub window: Option<super::config::WindowConfig>,
}

/// All runtime artifacts produced by [`ServiceBuilder::build_all`] for one
/// workflow.
///
/// The caller owns these and drives them with a runner function
/// (`run_standalone`, `run_cluster`).
///
/// `registry` is shared (via `Arc`) across every workflow one `build_all` call
/// produces, so that factory-allocated resources the sources and sinks point
/// back to stay alive for the service lifetime.
///
/// The `Debug` implementation reports counts only, because the node trait
/// objects are not `Debug`.
pub struct BuiltService {
    /// The workflow's declared id.
    pub workflow_id: String,
    /// The workflow's declared name, absent when the config named none.
    pub workflow_name: Option<String>,
    /// Every declared node, in topological order: a node always follows every
    /// node that links into it.
    pub nodes: Vec<BuiltNode>,
    /// The registry that built this service, shared across every workflow
    /// `build_all` produced and retained for lifetime management.
    pub registry: Arc<Registry>,
    /// The inspector this build published its topology into, when enabled.
    /// Runners hand it to the HTTP layer, and record one
    /// [`FlowDecision`](saci_inspector_wire::FlowDecision) into it on each pass
    /// a source's admission target moved.
    pub inspector: Option<Inspector>,
    /// This workflow's dead letter queue, when it declared a `dlq` block.
    /// The runner owns it: `record` and `replay` both run inline between
    /// passes, so nothing else may touch it.
    pub dlq: Option<DeadLetterQueue>,
}

impl std::fmt::Debug for BuiltService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltService")
            .field("workflow_id", &self.workflow_id)
            .field("nodes_count", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

/// Assembles one [`BuiltService`] per declared workflow from a
/// [`ServiceConfig`] and a populated [`Registry`].
///
/// Register IO factories first, optionally supply native runtimes for
/// artifact-less processor nodes via [`with_runtime`](Self::with_runtime),
/// then call [`build_all`](Self::build_all) with a loaded config.
///
/// ## Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::builder::ServiceBuilder;
/// use saci_service::pipeline::Pipeline;
///
/// let pipeline = Pipeline::new("my_pipeline");
/// let _builder = ServiceBuilder::new().with_runtime("my-processor", Box::new(pipeline));
/// // builder.build_all(&config) would return Ok(vec![BuiltService { ... }])
/// # }
/// ```
pub struct ServiceBuilder {
    registry: Registry,
    /// Native runtimes keyed by the processor node id they back. Consumed by
    /// [`build_all`](Self::build_all) for a `wasm`/`plugin` node declared with
    /// no artifact.
    runtimes: HashMap<String, Box<dyn PipelineRuntime>>,
    #[cfg(feature = "wasm")]
    wasm_engine: Option<WasmEngine>,
    inspector: Option<Inspector>,
    channels: Option<Arc<dyn ChannelBridge>>,
    /// The shared halves of every declared dead letter queue, so the HTTP
    /// control plane and the runner see one summary and one request slot per
    /// workflow. `None` for a library embedder, which gets a queue with a
    /// private shared half.
    dlq_registry: Option<Arc<DlqRegistry>>,
}

impl ServiceBuilder {
    /// Create a new builder with an empty registry and no runtimes.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            runtimes: HashMap::new(),
            #[cfg(feature = "wasm")]
            wasm_engine: None,
            inspector: None,
            channels: None,
            dlq_registry: None,
        }
    }

    /// Share every declared dead letter queue's summary and request slot
    /// with `registry`, which the HTTP control plane reads.
    ///
    /// Without it a queue still records and replays; only
    /// `GET /api/dlq` and the replay endpoints have nothing to address.
    pub fn with_dlq_registry(mut self, registry: Arc<DlqRegistry>) -> Self {
        self.dlq_registry = Some(registry);
        self
    }

    /// Supply the runtime for the processor node declared with id
    /// `processor_id` and no `module`/`library` key.
    ///
    /// Any `Box<dyn PipelineRuntime>` is accepted, typically `Box::new(pipeline)`
    /// for a native [`Pipeline`](crate::pipeline::Pipeline).
    pub fn with_runtime(
        mut self,
        processor_id: impl Into<String>,
        runtime: Box<dyn PipelineRuntime>,
    ) -> Self {
        self.runtimes.insert(processor_id.into(), runtime);
        self
    }

    /// Publish the built topology into `inspector` during
    /// [`build_all`](Self::build_all).
    ///
    /// The builder is the only place that knows both the concrete runtime kind
    /// (before the `Box<dyn PipelineRuntime>` erases it) and the configured
    /// source and sink sets, which is exactly what the topology is.
    pub fn with_inspector(mut self, inspector: Inspector) -> Self {
        self.inspector = Some(inspector);
        self
    }

    /// Register the shared channel bridge every `ChannelSource`/`ChannelSink`
    /// node resolves its named half through.
    ///
    /// `register_builtin_factories` attaches a default
    /// `saci_connector_channel::ChannelRegistry` automatically when
    /// `connector-channel` is enabled; call this to supply a different
    /// instance (for example, to share one registry across two independently
    /// built services).
    pub fn with_channel_bridge(mut self, channels: Arc<dyn ChannelBridge>) -> Self {
        self.channels = Some(channels);
        self
    }

    /// Set the [`WasmEngine`] used to load every `wasm` node's module. If not
    /// set and the workflow declares one, `build_all` creates a default engine
    /// automatically and shares it across every `wasm` node.
    #[cfg(feature = "wasm")]
    pub fn with_wasm_engine(mut self, engine: WasmEngine) -> Self {
        self.wasm_engine = Some(engine);
        self
    }

    /// Register a source factory (builder-style chaining).
    pub fn register_source<F: SourceFactory>(mut self, factory: F) -> Self {
        self.registry.register_source(factory);
        self
    }

    /// Register a sink factory (builder-style chaining).
    pub fn register_sink<F: SinkFactory>(mut self, factory: F) -> Self {
        self.registry.register_sink(factory);
        self
    }

    /// Register a transformer factory (builder-style chaining).
    pub fn register_transformer<F: TransformerFactory>(mut self, factory: F) -> Self {
        self.registry.register_transformer(factory);
        self
    }

    /// Mutably register a source factory.
    pub fn register_source_mut<F: SourceFactory>(&mut self, factory: F) -> &mut Self {
        self.registry.register_source(factory);
        self
    }

    /// Mutably register a sink factory.
    pub fn register_sink_mut<F: SinkFactory>(&mut self, factory: F) -> &mut Self {
        self.registry.register_sink(factory);
        self
    }

    /// Mutably register a transformer factory.
    pub fn register_transformer_mut<F: TransformerFactory>(&mut self, factory: F) -> &mut Self {
        self.registry.register_transformer(factory);
        self
    }

    /// Access the inner registry (for inspection or passing to helpers).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Assemble one [`BuiltService`] per workflow declared in `config`, in
    /// declaration order, sharing one [`Registry`] and, when enabled, one
    /// [`Inspector`] topology across all of them.
    ///
    /// Every sink is wrapped in a
    /// [`RetryingSink`](saci_core::io::retry::RetryingSink). Sources are
    /// wrapped in a [`RetryingSource`](saci_core::io::retry::RetryingSource)
    /// too, except under `run_mode kind="stream"`, where the stream runner's
    /// own re-poll is already the retry loop.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if any workflow fails to build; see
    /// `build_one` for the full list of failure modes.
    pub fn build_all(self, config: &ServiceConfig) -> Result<Vec<BuiltService>, SaciError> {
        let mut factory = self.into_factory(config);

        let mut out = Vec::with_capacity(config.workflows.len());
        for workflow in &config.workflows {
            out.push(factory.build(workflow)?);
        }

        factory.publish_topology(config, &out);

        Ok(out)
    }

    /// Freeze the registry and keep the builder, so any declared workflow can
    /// be built again later.
    ///
    /// [`build_all`](Self::build_all) is this plus one
    /// [`ServiceFactory::build`] per declared workflow and one
    /// [`ServiceFactory::publish_topology`]; the lifecycle supervisor keeps the
    /// factory instead, because `start` and `restart` rebuild one workflow at a
    /// time.
    pub fn into_factory(mut self, config: &ServiceConfig) -> ServiceFactory {
        let registry = Arc::new(std::mem::replace(&mut self.registry, Registry::new()));
        let retry_sources = sources_take_retry_wrapper(config);
        ServiceFactory {
            builder: self,
            registry,
            retry_sources,
            heal: config.heal.clone(),
            data_dir: config.node.data_dir.clone(),
            service: config.node.label(),
        }
    }

    /// Build every declared source, sink and transformer across every
    /// workflow, skipping processor nodes and the workflow-graph check that
    /// needs their built components.
    ///
    /// Exercises the same connector factories [`build_all`](Self::build_all)
    /// does: each node's `config` is deserialized into its connector-specific,
    /// `deny_unknown_fields` struct and run through that struct's own
    /// `validate()`. Every built-in connector's factory does this
    /// synchronously with no network or broker connection (`serve` connects
    /// lazily, on the first read or write), so this check needs no live
    /// service and, unlike `build_all`, no processor artifact on disk either,
    /// which is what lets a template config naming a `pipelines/*.wasm`
    /// placeholder still have its connectors checked.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the workflow, node id and
    /// offending key for the first source, sink or transformer that fails to
    /// build. An unregistered factory type is one such error, exactly as it
    /// is in `build_all`; the caller distinguishes it the same way
    /// (`saci-service validate`'s `is_unknown_factory_error`).
    pub fn build_connectors_only(mut self, config: &ServiceConfig) -> Result<(), SaciError> {
        let registry = Arc::new(std::mem::replace(&mut self.registry, Registry::new()));
        let service = config.node.label();
        for workflow in &config.workflows {
            let transformers = build_transformers(&registry, &workflow.transformers)?;
            let build = NodeBuild {
                registry: &registry,
                transformers: &transformers,
                heal: &config.heal,
                service: &service,
            };
            for spec in &workflow.sources {
                self.build_source_node(&spec.id, workflow, &build, false)?;
            }
            for spec in &workflow.sinks {
                self.build_sink_node(&spec.id, workflow, &build)?;
            }
            // A store this binary carries no connector for, or a `nats`
            // store on core NATS, is a defect in the file: `validate
            // --connectors-only` is where an operator finds that out, not
            // the first sink failure in production.
            self.build_dlq(workflow, &registry, &config.node.data_dir)?;
        }
        Ok(())
    }

    /// Assemble a [`BuiltService`] for one declared `workflow`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if:
    /// - A transformer names an unregistered format.
    /// - A source or sink names an unregistered connector `type`.
    /// - A source or sink factory returns an error.
    /// - A `wasm`/`plugin` node names a module/library that cannot be
    ///   resolved, compiled, or described.
    /// - A `wasm` node names a module and the wasmtime engine this builder
    ///   would create for it cannot be created, which on a 64-bit host means
    ///   the pooling allocator's address-space reservation was refused.
    /// - A `wasm`/`plugin` node declares neither an artifact nor a runtime
    ///   registered for its id through [`with_runtime`](Self::with_runtime).
    /// - The links do not carry matching components and schemas end to end
    ///   (see [`validate_workflow_graph`](super::validation::validate_workflow_graph)).
    fn build_one(
        &mut self,
        registry: &Arc<Registry>,
        workflow: &WorkflowSpec,
        retry_sources: bool,
        heal: &HealConfig,
        data_dir: &std::path::Path,
        service: &str,
    ) -> Result<BuiltService, SaciError> {
        let transformers = build_transformers(registry, &workflow.transformers)?;
        let build = NodeBuild {
            registry,
            transformers: &transformers,
            heal,
            service,
        };
        let order = workflow.topological_order()?;
        let nodes_meta = workflow.nodes();

        // Natural (declaration) index -> topological position, so `downstream`
        // can be filled by looking up a link's endpoints.
        let id_to_topo_index: HashMap<&str, usize> = order
            .iter()
            .enumerate()
            .map(|(topo_idx, &nat_idx)| (nodes_meta[nat_idx].0, topo_idx))
            .collect();

        let mut nodes: Vec<BuiltNode> = Vec::with_capacity(order.len());
        for &nat_idx in &order {
            let (id, kind) = nodes_meta[nat_idx];
            let built = match kind {
                NodeKind::Source => self.build_source_node(id, workflow, &build, retry_sources)?,
                NodeKind::Sink => self.build_sink_node(id, workflow, &build)?,
                NodeKind::Processor => self.build_processor_node(id, workflow)?,
            };
            nodes.push(built);
        }

        for link in &workflow.links {
            let &from = id_to_topo_index.get(link.from.as_str()).ok_or_else(|| {
                SaciError::configuration(format!(
                    "workflow '{}': link names undeclared node '{}'",
                    workflow.id, link.from
                ))
            })?;
            let &to = id_to_topo_index.get(link.to.as_str()).ok_or_else(|| {
                SaciError::configuration(format!(
                    "workflow '{}': link names undeclared node '{}'",
                    workflow.id, link.to
                ))
            })?;
            nodes[from].downstream.push(BuiltEdge {
                node: to,
                branch: link.branch.clone(),
            });
        }

        super::validation::validate_workflow_graph(&workflow.id, &nodes)?;

        // After the nodes: a store that cannot be opened refuses the
        // workflow the way a sink node does, and it is worth hearing about
        // the graph first.
        let dlq = self.build_dlq(workflow, registry, data_dir)?;

        Ok(BuiltService {
            workflow_id: workflow.id.clone(),
            workflow_name: workflow.name.clone(),
            nodes,
            registry: registry.clone(),
            inspector: self.inspector.clone(),
            dlq,
        })
    }

    /// Build the workflow's dead letter queue, or `None` when it declares no
    /// `dlq` block.
    ///
    /// The shared half comes from the registry when one was attached, so the
    /// HTTP control plane and this queue are two views of one state. With no
    /// registry the queue gets a private one, which is what a library
    /// embedder driving [`ServiceBuilder`] directly holds.
    ///
    /// # Errors
    ///
    /// Whatever [`DeadLetterQueue::build`] refused the block with.
    fn build_dlq(
        &self,
        workflow: &WorkflowSpec,
        registry: &Arc<Registry>,
        data_dir: &std::path::Path,
    ) -> SaciResult<Option<DeadLetterQueue>> {
        let Some(dlq) = workflow.dlq.as_ref() else {
            return Ok(None);
        };
        let shared = match self.dlq_registry.as_ref().and_then(|r| r.get(&workflow.id)) {
            Some(shared) => Arc::clone(shared),
            None => Arc::new(DlqShared::new(&workflow.id, &dlq.0)),
        };
        DeadLetterQueue::build(workflow, data_dir, registry, shared).map(Some)
    }

    fn build_source_node(
        &self,
        id: &str,
        workflow: &WorkflowSpec,
        build: &NodeBuild<'_>,
        retry_sources: bool,
    ) -> SaciResult<BuiltNode> {
        let spec = workflow
            .sources
            .iter()
            .find(|s| s.id == id)
            .expect("nodes() id must resolve to a declared source");
        let factory = build
            .registry
            .source(&spec.type_name)
            .ok_or_else(|| missing_factory_error("source", &spec.type_name, &spec.id))?;
        let settings = resolve_heal(
            spec.heal.as_ref(),
            build.heal,
            factory.rebuildable(&spec.config),
            &workflow.id,
            "source",
            &spec.id,
            &spec.type_name,
        )?;
        let bound = spec
            .transformer
            .as_deref()
            .map(|tid| build.transformers[tid].clone());
        let rebuilder = Rebuilder::new(
            build.registry.clone(),
            &spec.type_name,
            &spec.id,
            spec.config.clone(),
            bound,
            self.channels.clone(),
            retry_sources.then(|| spec.retry.to_system_config()),
        )
        .with_identity(build.identity(workflow, &spec.id));
        let built = rebuilder.build_source()?;
        let source: Box<dyn Source> = match settings {
            Some(settings) => Box::new(HealingSource::new(
                built,
                rebuilder,
                settings,
                &workflow.id,
                &spec.id,
            )),
            None => built,
        };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: spec.type_name.clone(),
            component: Some(intern_component_name(&spec.component)),
            kind: BuiltNodeKind::Source(source),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        })
    }

    fn build_sink_node(
        &self,
        id: &str,
        workflow: &WorkflowSpec,
        build: &NodeBuild<'_>,
    ) -> SaciResult<BuiltNode> {
        let spec = workflow
            .sinks
            .iter()
            .find(|s| s.id == id)
            .expect("nodes() id must resolve to a declared sink");
        let factory = build
            .registry
            .sink(&spec.type_name)
            .ok_or_else(|| missing_factory_error("sink", &spec.type_name, &spec.id))?;
        let settings = resolve_heal(
            spec.heal.as_ref(),
            build.heal,
            factory.rebuildable(&spec.config),
            &workflow.id,
            "sink",
            &spec.id,
            &spec.type_name,
        )?;
        let bound = spec
            .transformer
            .as_deref()
            .map(|tid| build.transformers[tid].clone());
        let rebuilder = Rebuilder::new(
            build.registry.clone(),
            &spec.type_name,
            &spec.id,
            spec.config.clone(),
            bound,
            self.channels.clone(),
            Some(spec.retry.to_system_config()),
        )
        .with_identity(build.identity(workflow, &spec.id));
        let built = rebuilder.build_sink()?;
        let (sink, heal_recovered): (Box<dyn Sink>, _) = match settings {
            Some(settings) => {
                let healing = HealingSink::new(built, rebuilder, settings, &workflow.id, &spec.id);
                let recovered = healing.recovered_flag();
                (Box::new(healing), Some(recovered))
            }
            None => (built, None),
        };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: spec.type_name.clone(),
            component: Some(intern_component_name(&spec.component)),
            kind: BuiltNodeKind::Sink(sink),
            downstream: Vec::new(),
            artifact: None,
            heal_recovered,
            #[cfg(feature = "windows")]
            window: None,
        })
    }

    /// Dispatch to whichever of `workflow.wasm` / `workflow.plugin` declared
    /// `id`.
    ///
    /// Both vectors exist in every build, so a [`NodeKind::Processor`] id
    /// always comes from exactly one of them. Whether this build can *build*
    /// what it finds is the second question: with the node's host compiled
    /// out, the matching `#[cfg(not(...))]` arm refuses by name. Reaching one
    /// of those arms means something bypassed
    /// [`validate_build_capabilities`](super::validation::validate_build_capabilities),
    /// which the binary runs on the loaded config first. A library embedder
    /// driving `ServiceBuilder` directly is the usual way, and this is the
    /// message they get.
    ///
    /// A `window` block this build carries no engine for is the same kind of
    /// refusal, raised one level down in `build_wasm_node` and
    /// `build_plugin_node`. Putting it there rather than ahead of this
    /// dispatch is what keeps a node's own host answered first, the order
    /// `validate_build_capabilities` reports in.
    ///
    /// The final error is left for an id that is in neither vector, which
    /// `nodes()` makes unreachable through `build_one` and is a caller error
    /// through any other path.
    fn build_processor_node(&mut self, id: &str, workflow: &WorkflowSpec) -> SaciResult<BuiltNode> {
        #[cfg(feature = "wasm")]
        if let Some(spec) = workflow.wasm.iter().find(|w| w.id == id) {
            return self.build_wasm_node(spec, &workflow.id);
        }
        #[cfg(not(feature = "wasm"))]
        if let Some(spec) = workflow.wasm.iter().find(|w| w.id == id) {
            return Err(super::factories::missing_wasm_host_error(
                &spec.id,
                &workflow.id,
            ));
        }
        #[cfg(feature = "plugin")]
        if let Some(spec) = workflow.plugin.iter().find(|p| p.id == id) {
            return self.build_plugin_node(spec, &workflow.id);
        }
        #[cfg(not(feature = "plugin"))]
        if let Some(spec) = workflow.plugin.iter().find(|p| p.id == id) {
            return Err(super::factories::missing_plugin_host_error(
                &spec.id,
                &workflow.id,
            ));
        }
        Err(SaciError::configuration(format!(
            "workflow '{}': processor '{id}' names neither a wasm nor a plugin node",
            workflow.id
        )))
    }

    #[cfg(feature = "wasm")]
    fn build_wasm_node(
        &mut self,
        spec: &super::config::WasmSpec,
        workflow_id: &str,
    ) -> SaciResult<BuiltNode> {
        // The embedder-facing half of the windowing capability question, the
        // counterpart of the `#[cfg(not(feature = "wasm"))]` arm in
        // `build_processor_node`. It sits here, inside the host-gated
        // builder, so the host is always answered before the block it
        // carries, matching `validate_build_capabilities`. Reads `cfg!`
        // rather than `#[cfg]`: there is no call to compile out.
        if !cfg!(feature = "windows") && spec.window.is_some() {
            return Err(super::factories::missing_windows_engine_error(
                "wasm",
                &spec.id,
                workflow_id,
            ));
        }
        // The window geometry is injected into the config table as `window.*`
        // keys, so the guest's `get-config` answers one source of truth: the
        // block the operator wrote, not a copy in the `config` node.
        let spec = config_with_window(spec.clone());
        let (runtime, kind, artifact): (Box<dyn PipelineRuntime>, &'static str, Option<String>) =
            match &spec.module {
                Some(module) => {
                    // Built at most once per builder and only for a node that
                    // names a `module`, so a config with no module-backed wasm
                    // node still reserves no address space. Fallible since the
                    // pooling instance allocator reserves ~1 TiB of address
                    // space per engine, which a process resource limit can
                    // refuse: a library embedder gets this error rather than a
                    // panic.
                    let engine = match self.wasm_engine.clone() {
                        Some(engine) => engine,
                        None => {
                            let engine = WasmEngine::new().map_err(|e| {
                                SaciError::configuration(format!(
                                    "processor '{}': wasmtime engine creation failed: {e}. \
                                     The pooling instance allocator reserves ~1 TiB of address \
                                     space per engine, so this is usually a process \
                                     address-space limit (RLIMIT_AS / `ulimit -v`, common in \
                                     containers and CI sandboxes) or too many live engines in \
                                     one process; share one with \
                                     `ServiceBuilder::with_wasm_engine`",
                                    spec.id
                                ))
                            })?;
                            self.wasm_engine = Some(engine.clone());
                            engine
                        }
                    };
                    let loader = PipelineRuntimeLoader::new(engine, LocalModuleResolver::new());
                    let runtime = loader
                        .load(&spec.id, &spec)?
                        .with_identity(workflow_id.to_string(), spec.id.clone());
                    (Box::new(runtime), "wasm", Some(module.clone()))
                }
                None => {
                    let runtime = self.runtimes.remove(&spec.id).ok_or_else(|| {
                        SaciError::configuration(format!(
                            "processor '{}': {RUNTIME_SOURCE_HELP}",
                            spec.id
                        ))
                    })?;
                    (runtime, "native", None)
                }
            };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: kind.to_string(),
            component: None,
            kind: BuiltNodeKind::Processor { runtime, kind },
            downstream: Vec::new(),
            artifact,
            #[cfg(feature = "windows")]
            window: spec.window.clone(),
            heal_recovered: None,
        })
    }

    #[cfg(feature = "plugin")]
    fn build_plugin_node(
        &mut self,
        spec: &super::config::PluginSpec,
        workflow_id: &str,
    ) -> SaciResult<BuiltNode> {
        // Same placement and reasoning as the wasm path above: host first,
        // then the block it carries.
        if !cfg!(feature = "windows") && spec.window.is_some() {
            return Err(super::factories::missing_windows_engine_error(
                "plugin",
                &spec.id,
                workflow_id,
            ));
        }
        // Same `window.*` config injection as the wasm path: the plugin's
        // `get_config` callback answers the geometry the operator declared.
        let spec = config_with_window(spec.clone());
        let (runtime, kind, artifact): (Box<dyn PipelineRuntime>, &'static str, Option<String>) =
            match &spec.library {
                Some(library) => {
                    let runtime = load_plugin_runtime(&spec, None)?
                        .with_identity(workflow_id.to_string(), spec.id.clone());
                    (Box::new(runtime), "plugin", Some(library.clone()))
                }
                None => {
                    let runtime = self.runtimes.remove(&spec.id).ok_or_else(|| {
                        SaciError::configuration(format!(
                            "processor '{}': {RUNTIME_SOURCE_HELP}",
                            spec.id
                        ))
                    })?;
                    (runtime, "native", None)
                }
            };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: kind.to_string(),
            component: None,
            kind: BuiltNodeKind::Processor { runtime, kind },
            downstream: Vec::new(),
            artifact,
            #[cfg(feature = "windows")]
            window: spec.window.clone(),
            heal_recovered: None,
        })
    }
}

/// Inject a node's `window` block into its `config` table as `window.*` keys.
///
/// The block and the table are two KDL nodes but one contract: the host tracks
/// the watermark from the block, and the processor or plugin reads the same
/// geometry back through `get-config`. Building the enriched spec keeps every
/// call site (loader, plugin host) reading `spec.config` unchanged. Both call
/// sites are the `wasm` and `plugin` node builders, so the helper is compiled
/// in exactly when one of those two features is.
#[cfg(all(feature = "windows", any(feature = "wasm", feature = "plugin")))]
fn config_with_window<T>(mut spec: T) -> T
where
    T: WindowConfigCarrier,
{
    if let Some(window) = spec.window_config() {
        for (key, value) in window.config_pairs() {
            spec.config_mut().entry(key).or_insert(value);
        }
    }
    spec
}

#[cfg(all(not(feature = "windows"), any(feature = "wasm", feature = "plugin")))]
fn config_with_window<T>(spec: T) -> T {
    spec
}

/// The two processor node specs, unified for [`config_with_window`].
#[cfg(all(feature = "windows", any(feature = "wasm", feature = "plugin")))]
trait WindowConfigCarrier {
    fn window_config(&self) -> Option<&super::config::WindowConfig>;
    fn config_mut(&mut self) -> &mut HashMap<String, String>;
}

#[cfg(all(feature = "windows", feature = "wasm"))]
impl WindowConfigCarrier for super::config::WasmSpec {
    fn window_config(&self) -> Option<&super::config::WindowConfig> {
        self.window.as_ref()
    }
    fn config_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.config
    }
}

#[cfg(all(feature = "windows", feature = "plugin"))]
impl WindowConfigCarrier for super::config::PluginSpec {
    fn window_config(&self) -> Option<&super::config::WindowConfig> {
        self.window.as_ref()
    }
    fn config_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.config
    }
}

impl Default for ServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve every declared `transformer` node against `registry`, building one
/// instance per declaration.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a transformer names an
/// unregistered format, or when its factory rejects its `options`. The
/// unregistered-format wording, feature hint included, belongs to
/// [`missing_transformer_error`].
fn build_transformers(
    registry: &Registry,
    specs: &[TransformerSpec],
) -> SaciResult<HashMap<String, Arc<dyn Transformer>>> {
    let mut out = HashMap::with_capacity(specs.len());
    for spec in specs {
        let factory = registry.transformers().get(&spec.format).ok_or_else(|| {
            missing_transformer_error(&spec.id, &spec.format, &registry.transformers().formats())
        })?;
        out.insert(spec.id.clone(), factory.build(&spec.options)?);
    }
    Ok(out)
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::dataset::Dataset;
    use crate::pipeline::Pipeline;
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, RetryConfig, ServiceMode, SinkSpec,
        SourceSpec, StandaloneConfig, WorkflowSpec,
    };
    use crate::service::registry::{SinkFactory, SourceFactory};
    use crate::system::{System, SystemMeta};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use saci_connector::{ConfigMap, ConfigValue, ConnectorContext, REBUILD_UNDECLARED};
    #[cfg(feature = "connector-saci")]
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[cfg(feature = "connector-saci")]
    use tempfile::NamedTempFile;

    // ── Test helpers ──────────────────────────────────────────────────────────

    struct NoopSystem;

    #[async_trait]
    impl System for NoopSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("noop")
        }
        async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
            Ok(())
        }
    }

    struct NoopSourceFactory;
    impl SourceFactory for NoopSourceFactory {
        fn type_name(&self) -> &'static str {
            "NoopSource"
        }
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            use saci_connector_channel::ChannelSource;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (_tx, src) = ChannelSource::new(schema, 1);
            Ok(Box::new(src))
        }
    }

    struct NoopSinkFactory;
    impl SinkFactory for NoopSinkFactory {
        fn type_name(&self) -> &'static str {
            "NoopSink"
        }
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, SaciError> {
            use saci_connector_channel::ChannelSink;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (sink, _rx) = ChannelSink::new(schema, 1);
            Ok(Box::new(sink))
        }
    }

    fn base_config(workflow: WorkflowSpec) -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/saci-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![workflow],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        }
    }

    fn empty_workflow(id: &str) -> WorkflowSpec {
        WorkflowSpec {
            id: id.to_string(),
            name: None,
            transformers: Vec::new(),
            sources: Vec::new(),
            wasm: Vec::new(),
            plugin: Vec::new(),
            sinks: Vec::new(),
            links: Vec::new(),
            dlq: None,
        }
    }

    #[test]
    fn test_source_straight_to_sink_builds_with_no_processor() {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            heal: None,
            flow_control: None,
            id: "src1".to_string(),
            name: None,
            type_name: "NoopSource".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            heal: None,
            id: "sink1".to_string(),
            name: None,
            type_name: "NoopSink".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
        });
        let config = base_config(workflow);

        let service = ServiceBuilder::new()
            .register_source(NoopSourceFactory)
            .register_sink(NoopSinkFactory)
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        assert_eq!(service.workflow_id, "w");
        assert_eq!(service.nodes.len(), 2);
        assert_eq!(service.nodes[0].id, "src1");
        assert_eq!(service.nodes[0].component, Some("comp1"));
        assert!(matches!(service.nodes[0].kind, BuiltNodeKind::Source(_)));
        assert_eq!(service.nodes[1].id, "sink1");
        assert!(matches!(service.nodes[1].kind, BuiltNodeKind::Sink(_)));
        assert_eq!(
            service.nodes[0].downstream,
            vec![BuiltEdge {
                node: 1,
                branch: None
            }]
        );
    }

    #[test]
    fn test_unknown_source_factory_returns_error() {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            heal: None,
            flow_control: None,
            id: "bad_src".to_string(),
            name: None,
            type_name: "GhostSource".to_string(),
            transformer: None,
            component: "comp".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("GhostSource"));
    }

    /// A declared `plugin` node in a binary with no plugin host is refused by
    /// name, through `build_all` rather than through the binary's gate.
    ///
    /// `validate_build_capabilities` is what an operator hits, and its own
    /// tests cover it. This covers the other caller: a library embedder
    /// driving `ServiceBuilder` directly, for whom
    /// `build_processor_node`'s `#[cfg(not(feature = "plugin"))]` arm is the
    /// only refusal there is. Without this test, that arm could fall through
    /// to the trailing "names neither a wasm nor a plugin node" error, the
    /// message a reader would be told after deleting the arm, and every
    /// existing test would still pass, because
    /// `every_host_refusal_names_the_feature_that_supplies_it` only calls
    /// the producer directly and never asks whether the builder reaches it.
    ///
    /// `plugin` is not in the default bundle, so this runs under a plain
    /// `cargo test -p saci-service`.
    #[cfg(not(feature = "plugin"))]
    #[test]
    fn test_plugin_node_without_the_host_is_refused_by_the_builder() {
        let mut workflow = empty_workflow("w");
        workflow.plugin.push(super::super::config::PluginSpec {
            id: "audit".to_string(),
            name: None,
            library: Some("libaudit.so".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("--features plugin"),
            "the builder must name the flag that would host the node, not report \
             the id as unresolvable: {}",
            err.message()
        );
        assert!(
            err.message().contains("audit"),
            "the refusal must name the offending node: {}",
            err.message()
        );
    }

    /// A `window` block in a binary with no windowing engine is refused by
    /// the builder too, for the same reason the plugin arm above is: an
    /// embedder driving `ServiceBuilder` never reaches
    /// `validate_build_capabilities`, and without this the block would be
    /// dropped on the floor while the node built and ran unwindowed.
    ///
    /// Needs `wasm`, because a missing host is refused before the window is
    /// looked at. `cargo nextest run -p saci-service --no-default-features
    /// --features service,wasm --lib` is what exercises it.
    #[cfg(all(not(feature = "windows"), feature = "wasm"))]
    #[test]
    fn test_a_window_block_without_the_engine_is_refused_by_the_builder() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "aggregate".to_string(),
            name: None,
            module: Some("aggregate.wasm".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: Some(super::super::config::WindowConfig {
                spec: saci_core::window_spec::WindowSpec::Tumbling {
                    size_ms: 30_000,
                    offset_ms: 0,
                },
                time_field: "timestamp_ms".to_string(),
                key_fields: Vec::new(),
                allowed_lateness_ms: 0,
            }),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("--features windows"),
            "the builder must name the flag that would serve the block: {}",
            err.message()
        );
        assert!(
            err.message().contains("aggregate"),
            "the refusal must name the offending node: {}",
            err.message()
        );
    }

    /// With neither the host nor the engine, the builder names the host,
    /// the same one `validate_build_capabilities` names for that config.
    ///
    /// The two refusals live in different places, the host in
    /// `build_processor_node` and the window one inside `build_wasm_node`,
    /// so nothing but their nesting keeps them in that order. Hoisting the
    /// window check ahead of the dispatch would flip it and hand an embedder
    /// a different message than the binary gives for the same file.
    #[cfg(all(not(feature = "wasm"), not(feature = "windows")))]
    #[test]
    fn test_a_missing_host_is_named_before_a_missing_windowing_engine() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "aggregate".to_string(),
            name: None,
            module: Some("aggregate.wasm".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: Some(super::super::config::WindowConfig {
                spec: saci_core::window_spec::WindowSpec::Tumbling {
                    size_ms: 30_000,
                    offset_ms: 0,
                },
                time_field: "timestamp_ms".to_string(),
                key_fields: Vec::new(),
                allowed_lateness_ms: 0,
            }),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert!(
            err.message().contains("--features wasm"),
            "the host this build lacks outranks the block it would carry: {}",
            err.message()
        );
    }

    #[test]
    fn test_unknown_sink_factory_returns_error() {
        let mut workflow = empty_workflow("w");
        workflow.sinks.push(SinkSpec {
            heal: None,
            id: "bad_sink".to_string(),
            name: None,
            type_name: "GhostSink".to_string(),
            transformer: None,
            component: "comp".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("GhostSink"));
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_node_with_no_module_and_no_registered_runtime_is_an_error() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "p".to_string(),
            name: None,
            module: None,
            sha3_256: None,
            config: std::collections::HashMap::new(),
            window: None,
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("processor 'p'"), "got: {err}");
        assert!(
            err.message().contains("ServiceBuilder::with_runtime"),
            "got: {err}"
        );
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_node_with_no_module_uses_the_registered_native_runtime() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "p".to_string(),
            name: None,
            module: None,
            sha3_256: None,
            config: std::collections::HashMap::new(),
            window: None,
        });
        let config = base_config(workflow);

        let service = ServiceBuilder::new()
            .with_runtime("p", Box::new(Pipeline::new("native-p")))
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        assert_eq!(service.nodes.len(), 1);
        match &service.nodes[0].kind {
            BuiltNodeKind::Processor { runtime, kind } => {
                assert_eq!(*kind, "native");
                assert_eq!(runtime.name(), "native-p");
            }
            _ => panic!("expected a processor node"),
        }
        assert_eq!(service.nodes[0].artifact, None);
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_connectors_only_runs_every_connector_but_skips_a_missing_processor_module() {
        /// A minimal stand-in for a real connector's `deny_unknown_fields`
        /// config, so this test exercises the same failure mode a real
        /// connector's factory does: an unrecognised key is a hard error,
        /// not something dropped silently.
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct StrictSourceConfig {
            #[serde(default)]
            #[allow(dead_code)]
            greeting: String,
        }

        struct StrictSourceFactory;
        impl SourceFactory for StrictSourceFactory {
            fn type_name(&self) -> &'static str {
                "StrictSource"
            }
            fn build(
                &self,
                config: &ConfigValue,
                _ctx: &ConnectorContext,
            ) -> Result<Box<dyn Source>, SaciError> {
                serde_json::from_value::<StrictSourceConfig>(config.clone())
                    .map_err(|e| SaciError::configuration(format!("StrictSource config: {e}")))?;
                use saci_connector_channel::ChannelSource;
                let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
                let (_tx, src) = ChannelSource::new(schema, 1);
                Ok(Box::new(src))
            }
        }

        // Source, processor and sink linked end to end, the same shape
        // `build_all` walks, but the wasm node names a module this test
        // never writes to disk, so `build_all` would fail trying to read it.
        fn workflow_naming_a_missing_module(source_config: ConfigValue) -> WorkflowSpec {
            let mut workflow = empty_workflow("w");
            workflow.sources.push(SourceSpec {
                heal: None,
                flow_control: None,
                id: "src1".to_string(),
                name: None,
                type_name: "StrictSource".to_string(),
                transformer: None,
                component: "comp1".to_string(),
                retry: RetryConfig::default(),
                config: source_config,
            });
            workflow.sinks.push(SinkSpec {
                heal: None,
                id: "sink1".to_string(),
                name: None,
                type_name: "NoopSink".to_string(),
                transformer: None,
                component: "comp1".to_string(),
                retry: RetryConfig::default(),
                config: ConfigValue::Object(ConfigMap::new()),
            });
            workflow.wasm.push(super::super::config::WasmSpec {
                id: "p".to_string(),
                name: None,
                module: Some("pipelines/does-not-exist.wasm".to_string()),
                sha3_256: None,
                config: std::collections::HashMap::new(),
                window: None,
            });
            workflow.links.push(super::super::config::LinkSpec {
                from: "src1".to_string(),
                to: "p".to_string(),
                branch: None,
            });
            workflow.links.push(super::super::config::LinkSpec {
                from: "p".to_string(),
                to: "sink1".to_string(),
                branch: None,
            });
            workflow
        }

        // Half 1: a valid connector config plus a processor artifact that
        // does not exist on disk. This is the whole reason
        // `build_connectors_only` exists: it must succeed here, where
        // `build_all` would fail on the missing module before ever reaching
        // the sink.
        let mut valid = ConfigMap::new();
        valid.insert(
            "greeting".to_string(),
            ConfigValue::String("hi".to_string()),
        );
        let config = base_config(workflow_naming_a_missing_module(ConfigValue::Object(valid)));
        ServiceBuilder::new()
            .register_source(StrictSourceFactory)
            .register_sink(NoopSinkFactory)
            .build_connectors_only(&config)
            .unwrap_or_else(|e| {
                panic!("connectors-only build should skip the missing module: {e}")
            });

        // Half 2: the same shape, but the source's own config carries a key
        // its factory does not recognise. `build_connectors_only` must still
        // run that connector's own validation and name it in the error:
        // skipping the processor must not mean skipping the connectors too.
        let mut bad = ConfigMap::new();
        bad.insert(
            "no_such_key".to_string(),
            ConfigValue::String("x".to_string()),
        );
        let config = base_config(workflow_naming_a_missing_module(ConfigValue::Object(bad)));
        let err = ServiceBuilder::new()
            .register_source(StrictSourceFactory)
            .register_sink(NoopSinkFactory)
            .build_connectors_only(&config)
            .expect_err(
                "an unknown key in a connector's config must fail connectors-only validation",
            );
        assert!(
            err.message().contains("StrictSource") && err.message().contains("no_such_key"),
            "error should name the connector and the offending key: {err}"
        );
    }

    /// `build_connectors_only` walks the same `build_source_node`/
    /// `build_sink_node` path `build_all` does, so a `saci` source and sink
    /// pair gets the node identity `ConnectorContext::identity` requires
    /// without ever reaching the "no identity" refusal.
    #[cfg(feature = "connector-saci")]
    #[test]
    fn build_connectors_only_binds_the_node_identity_for_a_saci_pair() {
        let raw = r#"
mode "standalone"

node id=1 name="svc-a" data_dir="/tmp/saci-test"

run_mode kind="stream"

workflow "w" {
    source "in" type="saci" component="Tick" {
        config {
            bind "127.0.0.1:0"
            schema_fields "v" type="Int64" nullable=#false
        }
    }
    sink "out" type="saci" component="Tick" {
        config {
            connect "127.0.0.1:9"
            schema_fields "v" type="Int64" nullable=#false
        }
    }
    link from="in" to="out"
}
"#;
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(raw.as_bytes()).expect("write config");

        let config = ServiceConfig::load(file.path()).expect("config loads");
        crate::service::register_builtin_factories(ServiceBuilder::new())
            .build_connectors_only(&config)
            .expect("the host binds the node identity for both saci halves");
    }

    #[test]
    fn test_boxed_system_runs_on_runtime() {
        let mut pipeline = Pipeline::new("test");
        pipeline.add_system_boxed(Box::new(NoopSystem));
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { pipeline.run().await }).unwrap();
    }

    #[test]
    fn test_build_all_returns_one_built_service_per_workflow() {
        let config = ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/saci-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![empty_workflow("a"), empty_workflow("b")],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        };

        let built = ServiceBuilder::new()
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"));

        assert_eq!(built.len(), 2);
        assert_eq!(built[0].workflow_id, "a");
        assert_eq!(built[1].workflow_id, "b");
    }

    // ── Retry wrappers ────────────────────────────────────────────────────────

    /// A source that fails the first `failures` `next_batch` calls, then
    /// yields one 1-row batch. `calls` counts every `next_batch` invocation.
    struct FlakySource {
        failures_left: usize,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Source for FlakySource {
        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.failures_left > 0 {
                self.failures_left -= 1;
                Err(SaciError::generic("flaky source failure"))
            } else {
                let batch = RecordBatch::try_new(
                    self.schema(),
                    vec![Arc::new(Int32Array::from(vec![1_i32]))],
                )
                .expect("schema should build a batch");
                Ok(Some(batch))
            }
        }
    }

    struct FlakySourceFactory {
        failures: usize,
        calls: Arc<AtomicUsize>,
    }

    impl SourceFactory for FlakySourceFactory {
        fn type_name(&self) -> &'static str {
            "FlakySource"
        }

        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            Ok(Box::new(FlakySource {
                failures_left: self.failures,
                calls: Arc::clone(&self.calls),
            }))
        }
    }

    /// A sink whose `write_batch` fails `failures` times, then succeeds.
    struct FlakySink {
        write_failures_left: usize,
        write_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Sink for FlakySink {
        async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<(), SaciError> {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            if self.write_failures_left > 0 {
                self.write_failures_left -= 1;
                Err(SaciError::generic("flaky sink write failure"))
            } else {
                Ok(())
            }
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
        }
    }

    struct FlakySinkFactory {
        write_failures: usize,
        write_calls: Arc<AtomicUsize>,
    }

    impl SinkFactory for FlakySinkFactory {
        fn type_name(&self) -> &'static str {
            "FlakySink"
        }

        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, SaciError> {
            Ok(Box::new(FlakySink {
                write_failures_left: self.write_failures,
                write_calls: Arc::clone(&self.write_calls),
            }))
        }
    }

    /// A one-link workflow whose flaky source feeds a flaky sink, so
    /// `build_all` produces a valid graph with both nodes retry-wrapped.
    fn flaky_workflow(source_retry: RetryConfig, sink_retry: RetryConfig) -> WorkflowSpec {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            heal: None,
            flow_control: None,
            id: "src1".to_string(),
            name: None,
            type_name: "FlakySource".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: source_retry,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            heal: None,
            id: "sink1".to_string(),
            name: None,
            type_name: "FlakySink".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: sink_retry,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
        });
        workflow
    }

    #[tokio::test]
    async fn a_flaky_source_is_retried_until_it_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(
            RetryConfig {
                base_delay_ms: 1,
                ..Default::default()
            },
            RetryConfig::default(),
        );
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 2,
                calls: Arc::clone(&calls),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build_all(&base_config(workflow))
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(0);
        let BuiltNodeKind::Source(mut source) = node.kind else {
            panic!("expected a source node");
        };
        let out = source.next_batch().await.expect("retry should recover");
        assert_eq!(out.unwrap().num_rows(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two failures then success");
    }

    #[tokio::test]
    async fn a_source_with_retry_disabled_surfaces_the_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(
            RetryConfig {
                max_attempts: 1,
                ..Default::default()
            },
            RetryConfig::default(),
        );
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 2,
                calls: Arc::clone(&calls),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build_all(&base_config(workflow))
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(0);
        let BuiltNodeKind::Source(mut source) = node.kind else {
            panic!("expected a source node");
        };
        let err = source.next_batch().await.unwrap_err();
        let SaciError::RetryExhausted { attempts, .. } = err else {
            panic!("expected the first error wrapped as RetryExhausted");
        };
        assert_eq!(attempts, 1, "single attempt");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "single attempt");
    }

    /// `run_mode kind="stream"` hands the source over unwrapped: `run_stream`
    /// is itself the retry loop, so a second one inside `next_batch` would put
    /// its 100 ms first backoff on the item path in front of the runner's own
    /// 10 ms re-poll. The error must reach the runner as the connector raised
    /// it, on the first attempt, not wrapped in `RetryExhausted`.
    #[tokio::test]
    async fn a_stream_mode_source_is_not_retry_wrapped() {
        let calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(RetryConfig::default(), RetryConfig::default());
        let mut config = base_config(workflow);
        config.mode = ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::Stream,
            },
        };
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 2,
                calls: Arc::clone(&calls),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(0);
        let BuiltNodeKind::Source(mut source) = node.kind else {
            panic!("expected a source node");
        };
        let err = source.next_batch().await.unwrap_err();
        assert!(
            !matches!(err, SaciError::RetryExhausted { .. }),
            "a stream source's error reaches the runner unwrapped, got {err}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no retry inside next_batch"
        );
    }

    /// The sink keeps its wrapper in stream mode: nothing re-drives a dropped
    /// `write_batch`.
    #[tokio::test]
    async fn a_stream_mode_sink_keeps_its_retry_wrapper() {
        let write_calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(RetryConfig::default(), RetryConfig::default());
        let mut config = base_config(workflow);
        config.mode = ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::Stream,
            },
        };
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 0,
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 1,
                write_calls: Arc::clone(&write_calls),
            })
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(1);
        let BuiltNodeKind::Sink(mut sink) = node.kind else {
            panic!("expected a sink node");
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .expect("schema should build a batch");
        sink.write_batch(&batch)
            .await
            .expect("retry should recover");
        assert_eq!(
            write_calls.load(Ordering::SeqCst),
            2,
            "one failure then success"
        );
    }

    #[tokio::test]
    async fn a_flaky_sink_is_retried_until_it_succeeds() {
        let write_calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(
            RetryConfig::default(),
            RetryConfig {
                base_delay_ms: 1,
                ..Default::default()
            },
        );
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 0,
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 1,
                write_calls: Arc::clone(&write_calls),
            })
            .build_all(&base_config(workflow))
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(1);
        let BuiltNodeKind::Sink(mut sink) = node.kind else {
            panic!("expected a sink node");
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .expect("schema should build a batch");
        sink.write_batch(&batch)
            .await
            .expect("retry should recover");
        assert_eq!(
            write_calls.load(Ordering::SeqCst),
            2,
            "one failure then success"
        );
    }

    // ── Self-healing ─────────────────────────────────────────────────────────

    /// A source that always fails, from a factory that declares a rebuild
    /// sound and counts how many instances it has handed out.
    struct HealableSourceFactory(Arc<AtomicUsize>);

    struct AlwaysFailingSource;

    #[async_trait]
    impl Source for AlwaysFailingSource {
        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            Err(SaciError::generic("the connector is down"))
        }
    }

    impl SourceFactory for HealableSourceFactory {
        fn type_name(&self) -> &'static str {
            "HealableSource"
        }

        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(AlwaysFailingSource))
        }

        fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
            Ok(())
        }
    }

    /// `FlakySink` reused as a sink whose factory refuses a rebuild, which is
    /// the trait default: it declares nothing, so the host never heals it.
    fn heal_workflow(
        source_heal: Option<HealConfig>,
        sink_heal: Option<HealConfig>,
    ) -> WorkflowSpec {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            heal: source_heal,
            flow_control: None,
            id: "src1".to_string(),
            name: None,
            type_name: "HealableSource".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: RetryConfig {
                max_attempts: 1,
                ..Default::default()
            },
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            heal: sink_heal,
            id: "sink1".to_string(),
            name: None,
            type_name: "FlakySink".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
        });
        workflow
    }

    fn build_heal_service(
        config: &ServiceConfig,
        builds: &Arc<AtomicUsize>,
    ) -> Result<BuiltService, SaciError> {
        Ok(ServiceBuilder::new()
            .register_source(HealableSourceFactory(Arc::clone(builds)))
            .register_sink(FlakySinkFactory {
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build_all(config)?
            .remove(0))
    }

    /// The whole point of healing by default: a connector that declares a
    /// rebuild sound is replaced with no `heal` block anywhere in the config.
    #[tokio::test(start_paused = true)]
    async fn a_rebuildable_source_heals_under_the_default_policy() {
        let builds = Arc::new(AtomicUsize::new(0));
        let mut config = base_config(heal_workflow(None, None));
        // Only the schedule is shortened; `enabled` is left to the default.
        config.heal = HealConfig {
            after_failures: Some(1),
            base_delay_ms: Some(1),
            max_delay_ms: Some(2),
            ..Default::default()
        };
        let mut service = build_heal_service(&config, &builds).expect("builds");

        let node = service.nodes.remove(0);
        let BuiltNodeKind::Source(mut source) = node.kind else {
            panic!("expected a source node");
        };
        assert!(source.next_batch().await.is_err(), "the handle is dead");
        assert_eq!(builds.load(Ordering::SeqCst), 1, "no rebuild yet");

        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        assert!(source.next_batch().await.is_err(), "still down");
        assert_eq!(
            builds.load(Ordering::SeqCst),
            2,
            "the default policy rebuilt it with no heal block declared"
        );
    }

    /// A node that names `heal` on a connector that cannot be rebuilt is a
    /// load-time error: a block that quietly does nothing is worse.
    #[test]
    fn a_heal_block_on_a_connector_that_cannot_be_rebuilt_is_refused() {
        let builds = Arc::new(AtomicUsize::new(0));
        let config = base_config(heal_workflow(
            None,
            Some(HealConfig {
                after_failures: Some(2),
                ..Default::default()
            }),
        ));
        let err = build_heal_service(&config, &builds).expect_err("must refuse");
        let text = err.to_string();
        assert!(
            text.contains("heal is not available for type 'FlakySink'"),
            "the refusal names the node and its type: {text}"
        );
        assert!(
            text.contains(REBUILD_UNDECLARED),
            "the refusal carries the factory's own reason: {text}"
        );
    }

    /// The same node opting out builds, and so does the same node with no
    /// block under a blanket top-level one: healing is skipped for it rather
    /// than refused, because an inherited policy is not a request by name.
    #[test]
    fn a_connector_that_cannot_be_rebuilt_is_skipped_rather_than_refused() {
        let builds = Arc::new(AtomicUsize::new(0));
        let opted_out = base_config(heal_workflow(
            None,
            Some(HealConfig {
                enabled: Some(false),
                ..Default::default()
            }),
        ));
        build_heal_service(&opted_out, &builds).expect("an opted-out node builds");

        let mut inherited = base_config(heal_workflow(None, None));
        inherited.heal = HealConfig {
            after_failures: Some(2),
            ..Default::default()
        };
        build_heal_service(&inherited, &builds).expect("an inherited policy skips the node");
    }
}
