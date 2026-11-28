//! [`ServiceBuilder`]: assembles a [`BuiltWorkflow`] per workflow from config
//! and registry.
//!
//! `ServiceBuilder` is the integration point between the configuration file and
//! the SACI runtime. It holds a [`Registry`] of user-provided IO factories and,
//! given a [`ServiceConfig`], instantiates every declared node of every
//! declared `workflow`: sources, sinks, codecs, and `wasm`/`plugin`
//! processors.
//!
//! ## Usage
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use saci::service::ServiceBuilder;
//! use saci::pipeline::Pipeline;
//!
//! let pipeline = Pipeline::new("my_pipeline");
//! let _builder = ServiceBuilder::new().runtime("my-processor", pipeline);
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{SaciError, SaciResult};
use crate::io::{Sink, Source};
use crate::runtime::PipelineRuntime;

use super::alert::AlertDispatcher;
#[cfg(feature = "crypto")]
use super::config::encryption::resolve_cipher;
use super::config::{
    CodecSpec, CorruptRecordPolicy, HealConfig, NodeKind, RunMode, SchemaSpec, ServiceConfig,
    ServiceEncryptionConfig, ServiceMode, StoreConfig, WindowConfig, WorkflowSpec,
};
use super::dlq::{DeadLetterQueue, DlqRegistry, DlqShared};
use super::factories::{lookup_connector, missing_codec_error};
use super::flow::FlowPlan;
use super::heal::{HealSettings, HealingSink, HealingSource, Rebuilder};
use super::lifecycle::RunControl;
#[cfg(feature = "wasm-host")]
use super::loader::{LocalModuleResolver, PipelineRuntimeLoader};
use super::operator_builder::build_operator_node;
#[cfg(feature = "plugin-host")]
use super::plugin_loader::load_plugin_runtime;
use super::redb_state::RedbStateClient;
use super::registry::{CodecFactory, Connector, Rebuild, Registry, Role};
use super::standalone::WorkflowStats;
use crate::codec::Codec;
use crate::connector::{ChannelBridge, ConfigValue, NodeIdentity};
#[cfg(feature = "crypto")]
use crate::crypto::{
    DecryptingSource, EncryptedCodec, EncryptingSink, EncryptionMode, FieldEncoding, KeyProvider,
    PAYLOAD_COLUMN, wire_schema,
};
use crate::inspector::Inspector;
use crate::schema::EvolutionPolicy;
#[cfg(feature = "wasm-host")]
use crate::wasm_host::WasmEngine;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::topology::build_topology;

// The "no runtime" help text names how a processor node can be given a
// runtime, so it has one arm per combination of `wasm` and `plugin`. With
// neither feature there is no `wasm`/`plugin` node to build, so neither of the
// two error paths that word this help text is compiled in.
#[cfg(all(feature = "wasm-host", not(feature = "plugin-host")))]
const RUNTIME_SOURCE_HELP: &str =
    "no runtime provided: call ServiceBuilder::runtime(id, ..) or set 'path' on the wasm node";
#[cfg(all(feature = "plugin-host", not(feature = "wasm-host")))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::runtime(id, ..) \
     or set 'path' on the plugin node";
#[cfg(all(feature = "wasm-host", feature = "plugin-host"))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::runtime(id, ..) \
     or set 'path' on the node";

/// Whether a declared workflow cannot be torn down and built a second time, or
/// `None` when it can.
///
/// Channel bridges and injected runtimes are both rebuildable across
/// lifecycle restarts and live configuration reloads, so this now always returns
/// `None`.
pub fn rebuild_blocker(_workflow: &WorkflowSpec) -> Option<&'static str> {
    None
}

/// Clonable wrapper around a caller-injected [`PipelineRuntime`] so workflows
/// registered via [`ServiceBuilder::runtime`] can be stopped, started,
/// restarted, and hot-reloaded without losing their runtime instance.
#[derive(Clone)]
struct SharedRuntime {
    name: String,
    components: Vec<String>,
    descriptor: crate::runtime::RuntimeDescriptorInfo,
    template: Arc<std::sync::Mutex<crate::dataset::Dataset>>,
    inner: Arc<tokio::sync::Mutex<Box<dyn PipelineRuntime>>>,
}

impl SharedRuntime {
    fn new(runtime: Box<dyn PipelineRuntime>) -> Self {
        let name = runtime.name().to_string();
        let components = runtime
            .declared_components()
            .into_iter()
            .map(str::to_string)
            .collect();
        let descriptor = runtime.descriptor_info();
        let template = runtime.template_dataset();
        Self {
            name,
            components,
            descriptor,
            template: Arc::new(std::sync::Mutex::new(template)),
            inner: Arc::new(tokio::sync::Mutex::new(runtime)),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl PipelineRuntime for SharedRuntime {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run_batch(
        &self,
        data: &mut crate::dataset::Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<crate::runtime::RuntimeOutput> {
        self.inner.lock().await.run_batch(data, prior).await
    }

    fn declared_components(&self) -> Vec<&str> {
        self.components.iter().map(String::as_str).collect()
    }

    fn descriptor_info(&self) -> crate::runtime::RuntimeDescriptorInfo {
        self.descriptor.clone()
    }

    fn template_dataset(&self) -> crate::dataset::Dataset {
        self.template
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone_empty()
    }
}

/// The self-healing policy one node runs under, or `None` when it is not
/// wrapped at all.
///
/// `rebuild` is what the node's connector answered for its own config.
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
    rebuild: Rebuild,
    workflow_id: &str,
    role: &str,
    node_id: &str,
    connector: &str,
) -> SaciResult<Option<HealSettings>> {
    let settings = local
        .cloned()
        .unwrap_or_default()
        .resolve(global, HealSettings::default());
    if !settings.enabled {
        return Ok(None);
    }
    match rebuild {
        Rebuild::Yes => Ok(Some(settings)),
        Rebuild::No(reason) if local.is_some() => Err(SaciError::configuration(format!(
            "workflow '{workflow_id}' {role} '{node_id}': heal is not available for \
             connector '{connector}': {reason}"
        ))),
        Rebuild::No(_) => Ok(None),
    }
}

/// Refuse a workflow that declares a live source outside `run "stream"`.
///
/// [`WorkflowSpec::validate`] already refuses one built from a connector this
/// crate ships, because it has no registry to ask. This is the same rule
/// asked of `registry`, so a connector an embedder registered answers for
/// itself through [`Connector::live`].
///
/// # Errors
///
/// [`SaciError::Configuration`] naming the first source whose connector
/// reports itself live while `run` is not [`RunMode::Stream`].
fn refuse_live_sources(
    registry: &Registry,
    workflow: &WorkflowSpec,
    run: &RunMode,
) -> SaciResult<()> {
    if *run == RunMode::Stream {
        return Ok(());
    }
    match workflow.sources.iter().find(|spec| {
        registry
            .connector(&spec.connector)
            .is_some_and(|connector| connector.live(&spec.config))
    }) {
        Some(spec) => Err(SaciError::configuration(format!(
            "workflow '{}' source '{}': connector '{}' never reaches EOF; it requires \
             standalone mode with run \"stream\"",
            workflow.id, spec.id, spec.connector
        ))),
        None => Ok(()),
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
    codecs: &'a HashMap<String, Arc<dyn Codec>>,
    heal: &'a HealConfig,
    /// `node.label()`, the service name a peer-facing connector announces.
    service: &'a str,
    encryption: Option<&'a ServiceEncryptionConfig>,
    #[cfg(feature = "crypto")]
    cached_provider: std::cell::RefCell<Option<Arc<dyn KeyProvider>>>,
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
/// [`ServiceBuilder::build`] consumes the builder, so nothing it produced
/// can be built twice; this keeps both halves alive instead.
pub struct WorkflowFactory {
    builder: ServiceBuilder,
    registry: Arc<Registry>,
    heal: HealConfig,
    /// `node.data_dir`, where a store that writes to local disk (today, a
    /// `dlq "redb"` block) puts its file.
    data_dir: std::path::PathBuf,
    /// `node.label()`, the service name a peer-facing connector announces.
    service: String,
    /// How every built workflow is run: the config's `run` node, or
    /// [`RunMode::Continuous`] in cluster mode, where it is not read.
    run: RunMode,
    /// The flow-control policy the config resolves for every source.
    flow: FlowPlan,
    /// Whether a batch run threads processor state through the redb store
    /// (`store "redb" { batch_resume #true }`).
    batch_resume: bool,
    encryption: Option<ServiceEncryptionConfig>,
}

impl WorkflowFactory {
    /// Update factory settings from a newly reloaded [`ServiceConfig`].
    pub(crate) fn update_config(&mut self, config: &ServiceConfig) {
        self.heal = config.heal.clone();
        self.data_dir = config.node.data_dir.clone();
        self.service = config.node.label();
        self.run = match &config.mode {
            ServiceMode::Standalone { config: standalone } => standalone.run.clone(),
            ServiceMode::Cluster { .. } => RunMode::Continuous,
        };
        self.flow = FlowPlan::from_config(config);
        self.batch_resume = matches!(
            &config.store,
            Some(StoreConfig::Redb {
                batch_resume: true,
                ..
            })
        );
        self.encryption = config.encryption.clone();
        self.builder.alerts.update_config(&config.alerts);
    }

    /// Refresh lineage graphs on `self.builder.lineage`, when present.
    pub(crate) fn publish_lineage(&self, config: &ServiceConfig, built: &[BuiltWorkflow]) {
        if let Some(lineage) = &self.builder.lineage {
            lineage.update_graphs(config, Some(built));
        }
    }

    /// Snapshot current factory settings so a failed hot-reload can roll back.
    pub(crate) fn snapshot_settings(
        &self,
    ) -> (
        HealConfig,
        std::path::PathBuf,
        String,
        RunMode,
        FlowPlan,
        bool,
        Option<ServiceEncryptionConfig>,
    ) {
        (
            self.heal.clone(),
            self.data_dir.clone(),
            self.service.clone(),
            self.run.clone(),
            self.flow.clone(),
            self.batch_resume,
            self.encryption.clone(),
        )
    }

    /// Restore factory settings from a snapshot after a failed hot-reload.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_settings(
        &mut self,
        snap: (
            HealConfig,
            std::path::PathBuf,
            String,
            RunMode,
            FlowPlan,
            bool,
            Option<ServiceEncryptionConfig>,
        ),
    ) {
        self.heal = snap.0;
        self.data_dir = snap.1;
        self.service = snap.2;
        self.run = snap.3;
        self.flow = snap.4;
        self.batch_resume = snap.5;
        self.encryption = snap.6;
    }
    /// Build one declared workflow.
    ///
    /// Calling this twice for the same workflow is only sound once the
    /// previous [`BuiltWorkflow`] has been dropped: a source or sink may hold
    /// an exclusive resource such as a listening port. See
    /// [`rebuild_blocker`] for the workflows where even that is not enough.
    ///
    /// # Errors
    ///
    /// Whatever [`ServiceBuilder::build`] would return for this workflow.
    pub fn build(&mut self, workflow: &WorkflowSpec) -> Result<BuiltWorkflow, SaciError> {
        let effective_run = workflow.run.as_ref().unwrap_or(&self.run).clone();
        refuse_live_sources(&self.registry, workflow, &effective_run)?;
        let retry_sources = effective_run != RunMode::Stream;
        let mut built = self.builder.build_one(
            &self.registry,
            workflow,
            retry_sources,
            &self.heal,
            &self.data_dir,
            &self.service,
            self.encryption.as_ref(),
        )?;
        built.run = effective_run;
        built.flow = self.flow.clone();
        built.batch_resume = self.batch_resume;
        Ok(built)
    }

    /// Publish the topology of `built` into the inspector, if one is attached.
    pub fn publish_topology(&self, config: &ServiceConfig, built: &[BuiltWorkflow]) {
        if let Some(inspector) = &self.builder.inspector {
            let node_slices: Vec<&[BuiltNode]> = built.iter().map(|b| b.nodes.as_slice()).collect();
            inspector.set_topology(build_topology(
                config,
                &self.registry,
                &node_slices,
                inspector.topology().version + 1,
            ));
        }
    }

    /// The frozen registry every build shares.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// The inspector attached to the inner builder, if any.
    pub(crate) fn inspector(&self) -> Option<&Inspector> {
        self.builder.inspector.as_ref()
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
/// of the process. This mirrors `crate::dataset`'s own (crate-private)
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
    /// Index into [`BuiltWorkflow::nodes`]. Always greater than the owning
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
#[non_exhaustive]
pub struct BuiltNode {
    /// The declared id.
    pub id: String,
    /// Declared name, absent when the config named none.
    pub name: Option<String>,
    /// The connector name for a source or sink (`file`, `kafka`, ..); the
    /// runtime kind (`"wasm"`/`"plugin"`/`"native"`) for a processor. A node
    /// added through [`BuiltWorkflowBuilder`] carries `"native"` (processor)
    /// or `"custom"` (source or sink).
    pub connector: String,
    /// Component a source writes or a sink reads. `None` for a processor.
    ///
    /// Leaked to `'static` once at build time: it is a span field and a
    /// `Dataset` key on every iteration.
    pub component: Option<&'static str>,
    /// The constructed execution backend.
    pub kind: BuiltNodeKind,
    /// Outbound edges into [`BuiltWorkflow::nodes`]. Always greater than this
    /// node's own index, because `nodes` is in topological order.
    pub downstream: Vec<BuiltEdge>,
    /// Artifact path for a `wasm`/`plugin` processor, for the topology
    /// detail. `None` for a source, a sink, or a processor whose runtime was
    /// supplied through [`ServiceBuilder::runtime`].
    pub artifact: Option<String>,
    /// A healed sink node's recovered flag: latched `true` the moment a
    /// rebuilt instance's first write succeeds, and cleared by whoever reads
    /// it. `None` for an unhealed sink, and for every source and processor
    /// node. The dead letter queue reads it to replay the moment a sink
    /// comes back rather than waiting out its own backoff.
    pub heal_recovered: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// The node's windowing declaration, when its config declared a `window`
    /// block. `None` for a non-windowed processor and for every source or
    /// sink. The runners read it to track the node's watermark when the
    /// `windows` feature is on; the topology uses it to describe the node.
    pub window: Option<WindowConfig>,
    /// Node-level schema evolution policy.
    pub(crate) evolution: EvolutionPolicy,
    /// Per-outbound-edge evolution policy (parallel to [`downstream`](Self::downstream)).
    pub(crate) edge_evolutions: Vec<EvolutionPolicy>,
    /// Policy for corrupt source records (`fail`, `drop`, `dlq`).
    pub(crate) on_corrupt: CorruptRecordPolicy,
}

impl BuiltNode {
    /// Effective [`EvolutionPolicy`] for outbound edge `edge_idx`.
    pub(crate) fn edge_evolution(&self, edge_idx: usize) -> EvolutionPolicy {
        self.edge_evolutions
            .get(edge_idx)
            .copied()
            .unwrap_or(self.evolution)
    }
}

/// All runtime artifacts produced by [`ServiceBuilder::build`] for one
/// workflow.
///
/// The caller owns these and drives them with [`run`](Self::run), or with
/// `run_cluster` in cluster mode. A workflow assembled in code rather than
/// from a config is built with [`BuiltWorkflow::builder`].
///
/// `registry` is shared (via `Arc`) across every workflow one `build` call
/// produces, so that factory-allocated resources the sources and sinks point
/// back to stay alive for the service lifetime.
///
/// The `Debug` implementation reports counts only, because the node trait
/// objects are not `Debug`.
#[non_exhaustive]
pub struct BuiltWorkflow {
    /// The workflow's declared id.
    pub workflow_id: String,
    /// The workflow's declared name, absent when the config named none.
    pub workflow_name: Option<String>,
    /// Every declared node, in topological order: a node always follows every
    /// node that links into it.
    pub nodes: Vec<BuiltNode>,
    /// The registry that built this service, shared across every workflow
    /// `build` produced and retained for lifetime management.
    pub registry: Arc<Registry>,
    /// The inspector this build published its topology into, when enabled.
    /// Runners hand it to the HTTP layer, and record one
    /// [`FlowDecision`](crate::inspector_wire::FlowDecision) into it on each pass
    /// a source's admission target moved.
    pub inspector: Option<Inspector>,
    /// This workflow's dead letter queue, when it declared a `dlq` block.
    /// The runner owns it: `record` and `replay` both run inline between
    /// passes, so nothing else may touch it.
    pub dlq: Option<DeadLetterQueue>,
    /// How [`run`](Self::run) drives the workflow, from the config's
    /// `run` node. Not read in cluster mode.
    pub(super) run: RunMode,
    /// The flow-control policy each source node runs under.
    pub(super) flow: FlowPlan,
    /// Whether a batch run threads processor state through the redb store.
    pub(super) batch_resume: bool,
    /// OpenLineage event dispatcher shared with the runner and control plane.
    pub(super) lineage: Option<Arc<super::lineage::LineageDispatcher>>,
}

impl std::fmt::Debug for BuiltWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltWorkflow")
            .field("workflow_id", &self.workflow_id)
            .field("nodes_count", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

impl BuiltWorkflow {
    /// Start assembling a workflow by hand, node by node.
    ///
    /// ```rust
    /// # #[cfg(all(feature = "service", feature = "connector-channel"))]
    /// # {
    /// use saci::connector::channel::{ChannelSink, ChannelSource};
    /// use saci::service::BuiltWorkflow;
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use std::sync::Arc;
    /// # let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    /// let (source, _tx) = ChannelSource::new(schema.clone(), 8);
    /// let (sink, _rx) = ChannelSink::new(schema, 8);
    /// let workflow = BuiltWorkflow::builder("w")
    ///     .source("in", "T", source)
    ///     .sink("out", "T", sink)
    ///     .link("in", "out")
    ///     .build()
    ///     .unwrap();
    /// // workflow.run(cancel).await drives it.
    /// # let _ = workflow;
    /// # }
    /// ```
    pub fn builder(id: impl Into<String>) -> BuiltWorkflowBuilder {
        BuiltWorkflowBuilder {
            id: id.into(),
            name: None,
            nodes: Vec::new(),
            links: Vec::new(),
            windows: Vec::new(),
            run: RunMode::default(),
            flow: None,
            batch_resume: false,
            inspector: None,
            dlq: None,
            evolution: EvolutionPolicy::strict(),
        }
    }

    /// Drive the workflow until its run mode finishes it or `options` cancel
    /// it.
    ///
    /// [`RunMode::Stream`] runs one pass per admitted chunk of each arriving
    /// source batch; every other mode runs repeated passes over every node in
    /// topological order, paced by the mode. Returns [`WorkflowStats`]
    /// on success, including on cancellation, which is a clean exit; `Err` is
    /// reserved for an unrecoverable condition such as a stream workflow with
    /// no source.
    ///
    /// `options` is a [`RunOptions`], or anything that converts into one: a
    /// bare [`CancellationToken`] for an uncontrolled run, a [`RunControl`]
    /// when the caller owns a pause gate.
    ///
    /// # Errors
    ///
    /// [`SaciError::Configuration`] for a stream workflow with no source, and
    /// any error loading processor state from the store.
    pub async fn run(self, options: impl Into<RunOptions>) -> SaciResult<WorkflowStats> {
        let options = options.into();
        match self.run {
            RunMode::Stream => super::stream::run(self, options).await,
            _ => super::standalone::run(self, options).await,
        }
    }
}

/// What a [`BuiltWorkflow::run`] call reads besides the workflow itself: the
/// runner's cancellation and pause control, where it publishes live counters,
/// and the store it persists processor state to.
///
/// A [`CancellationToken`] or a [`RunControl`] converts into one with
/// everything else unset, so an uncontrolled run is `workflow.run(cancel)`.
/// The default carries a token nothing cancels.
#[derive(Default)]
pub struct RunOptions {
    pub(super) control: RunControl,
    pub(super) live_stats: Option<Arc<RwLock<WorkflowStats>>>,
    pub(super) state: Option<Arc<RedbStateClient>>,
}

impl RunOptions {
    /// Options that run under `control` and publish and persist nothing.
    pub fn new(control: impl Into<RunControl>) -> Self {
        Self {
            control: control.into(),
            live_stats: None,
            state: None,
        }
    }

    /// Publish the run's counters into `stats` as it goes, so `GET /status`
    /// sees current progress rather than only the final tally.
    pub fn live_stats(mut self, stats: Arc<RwLock<WorkflowStats>>) -> Self {
        self.live_stats = Some(stats);
        self
    }

    /// Persist processor state and source cursors through `state`.
    pub fn state(mut self, state: Arc<RedbStateClient>) -> Self {
        self.state = Some(state);
        self
    }
}

impl From<CancellationToken> for RunOptions {
    fn from(cancel: CancellationToken) -> Self {
        Self::new(cancel)
    }
}

impl From<RunControl> for RunOptions {
    fn from(control: RunControl) -> Self {
        Self::new(control)
    }
}

/// Assembles a [`BuiltWorkflow`] by hand: declare nodes in flow order, link
/// them, choose a run mode. Obtained from [`BuiltWorkflow::builder`].
///
/// Nodes keep declaration order, so a node must be declared after every node
/// that links into it. A node added here is a native one: a source or sink
/// is never wrapped in a retry or heal wrapper, and the workflow's registry
/// is empty. [`build`](Self::build) does not run the graph validation
/// [`ServiceBuilder::build`] applies to a configured workflow.
pub struct BuiltWorkflowBuilder {
    id: String,
    name: Option<String>,
    nodes: Vec<BuiltNode>,
    links: Vec<(String, String, Option<String>, Option<EvolutionPolicy>)>,
    windows: Vec<(String, WindowConfig)>,
    run: RunMode,
    flow: Option<FlowPlan>,
    batch_resume: bool,
    inspector: Option<Inspector>,
    dlq: Option<DeadLetterQueue>,
    evolution: EvolutionPolicy,
}

impl BuiltWorkflowBuilder {
    /// Set the workflow's declared name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    fn node(
        mut self,
        id: &str,
        connector: &str,
        component: Option<&'static str>,
        kind: BuiltNodeKind,
    ) -> Self {
        self.nodes.push(BuiltNode {
            id: id.to_string(),
            name: None,
            connector: connector.to_string(),
            component,
            kind,
            downstream: Vec::new(),
            artifact: None,
            heal_recovered: None,
            window: None,
            evolution: self.evolution,
            edge_evolutions: Vec::new(),
            on_corrupt: CorruptRecordPolicy::default(),
        });
        self
    }

    /// Declare a source node `id` that writes `component`.
    pub fn source(self, id: &str, component: &str, source: impl Source + 'static) -> Self {
        self.node(
            id,
            "custom",
            Some(intern_component_name(component)),
            BuiltNodeKind::Source(Box::new(source)),
        )
    }

    /// Declare a processor node `id` backed by `runtime`.
    pub fn processor(self, id: &str, runtime: impl PipelineRuntime + 'static) -> Self {
        self.node(
            id,
            "native",
            None,
            BuiltNodeKind::Processor {
                runtime: Box::new(runtime),
                kind: "native",
            },
        )
    }

    /// Declare a sink node `id` that reads `component`.
    pub fn sink(self, id: &str, component: &str, sink: impl Sink + 'static) -> Self {
        self.node(
            id,
            "custom",
            Some(intern_component_name(component)),
            BuiltNodeKind::Sink(Box::new(sink)),
        )
    }

    /// Attach a `window` declaration to the processor node `processor_id`,
    /// which the runners read to track that node's watermark when the
    /// `windows` feature is on.
    pub fn window(mut self, processor_id: &str, window: WindowConfig) -> Self {
        self.windows.push((processor_id.to_string(), window));
        self
    }

    /// Link node `from` to node `to`.
    pub fn link(mut self, from: &str, to: &str) -> Self {
        self.links
            .push((from.to_string(), to.to_string(), None, None));
        self
    }

    /// Link node `from` to node `to` with an explicit schema evolution policy.
    pub fn link_evolved(mut self, from: &str, to: &str, policy: EvolutionPolicy) -> Self {
        self.links
            .push((from.to_string(), to.to_string(), None, Some(policy)));
        self
    }

    /// Link node `from` to node `to` on the named `branch`, which a
    /// processor's routing decision selects by name.
    pub fn link_branch(mut self, from: &str, to: &str, branch: &str) -> Self {
        self.links.push((
            from.to_string(),
            to.to_string(),
            Some(branch.to_string()),
            None,
        ));
        self
    }

    /// Set the default schema evolution policy for links in this hand-built workflow.
    pub fn schema_evolution(mut self, policy: EvolutionPolicy) -> Self {
        self.evolution = policy;
        self
    }

    /// How [`BuiltWorkflow::run`] drives the workflow. The default is
    /// [`RunMode::Continuous`].
    pub fn run_mode(mut self, run: RunMode) -> Self {
        self.run = run;
        self
    }

    /// The flow-control policy for each source. Without one, a stream
    /// workflow gets [`FlowPlan::stream_default`] and every other run mode the
    /// batch defaults.
    pub fn flow(mut self, flow: FlowPlan) -> Self {
        self.flow = Some(flow);
        self
    }

    /// Whether a batch run threads processor state through the redb store
    /// handed to [`RunOptions::state`]. Off by default.
    pub fn batch_resume(mut self, on: bool) -> Self {
        self.batch_resume = on;
        self
    }

    /// Record flow-control decisions into `inspector`.
    pub fn inspector(mut self, inspector: Inspector) -> Self {
        self.inspector = Some(inspector);
        self
    }

    /// Give the workflow a dead letter queue.
    pub fn dlq(mut self, dlq: DeadLetterQueue) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Resolve every link and window to node indices and assemble the
    /// workflow.
    ///
    /// # Errors
    ///
    /// [`SaciError::Configuration`] for a duplicate node id, a link or window
    /// that names an undeclared node, a window on a node that is not a
    /// processor, a link from a node to itself, and a link whose target was
    /// declared before its origin.
    pub fn build(self) -> SaciResult<BuiltWorkflow> {
        let Self {
            id,
            name,
            mut nodes,
            links,
            windows,
            run,
            flow,
            batch_resume,
            inspector,
            dlq,
            evolution,
        } = self;

        let mut index: HashMap<String, usize> = HashMap::with_capacity(nodes.len());
        for (i, node) in nodes.iter_mut().enumerate() {
            if node.evolution.is_strict() && !evolution.is_strict() {
                node.evolution = evolution;
            }
            if index.insert(node.id.clone(), i).is_some() {
                return Err(SaciError::configuration(format!(
                    "workflow '{id}': duplicate node id '{}'",
                    node.id
                )));
            }
        }
        let position = |node: &str| {
            index.get(node).copied().ok_or_else(|| {
                SaciError::configuration(format!(
                    "workflow '{id}': link or window names undeclared node '{node}'"
                ))
            })
        };

        for (processor_id, window) in windows {
            let i = position(&processor_id)?;
            if !matches!(nodes[i].kind, BuiltNodeKind::Processor { .. }) {
                return Err(SaciError::configuration(format!(
                    "workflow '{id}': node '{processor_id}' is not a processor and takes no window"
                )));
            }
            nodes[i].window = Some(window);
        }

        for (from, to, branch, edge_policy) in links {
            let (f, t) = (position(&from)?, position(&to)?);
            if f == t {
                return Err(SaciError::configuration(format!(
                    "workflow '{id}': node '{from}' cannot link to itself"
                )));
            }
            if t < f {
                return Err(SaciError::configuration(format!(
                    "workflow '{id}': declare nodes in flow order: '{to}' comes before '{from}'"
                )));
            }
            let effective = edge_policy.unwrap_or(evolution);
            nodes[f].downstream.push(BuiltEdge { node: t, branch });
            nodes[f].edge_evolutions.push(effective);
        }

        let flow = flow.unwrap_or_else(|| match run {
            RunMode::Stream => FlowPlan::stream_default(),
            _ => FlowPlan::default(),
        });
        Ok(BuiltWorkflow {
            workflow_id: id,
            workflow_name: name,
            nodes,
            registry: Arc::new(Registry::new()),
            inspector,
            dlq,
            run,
            flow,
            batch_resume,
            lineage: None,
        })
    }
}

/// Assembles one [`BuiltWorkflow`] per declared workflow from a
/// [`ServiceConfig`] and a populated [`Registry`].
///
/// [`new`](Self::new) registers every compiled-in connector and codec;
/// [`empty`](Self::empty) registers nothing. Optionally supply native runtimes
/// for artifact-less processor nodes via [`runtime`](Self::runtime), then call
/// [`build`](Self::build) with a loaded config.
///
/// ## Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci::service::ServiceBuilder;
/// use saci::pipeline::Pipeline;
///
/// let pipeline = Pipeline::new("my_pipeline");
/// let _builder = ServiceBuilder::new().runtime("my-processor", pipeline);
/// // builder.build(&config) would return Ok(vec![BuiltWorkflow { ... }])
/// # }
/// ```
pub struct ServiceBuilder {
    registry: Registry,
    /// Native runtimes keyed by the processor node id they back. Reused across
    /// builds/restarts via [`SharedRuntime`].
    runtimes: HashMap<String, SharedRuntime>,
    #[cfg(feature = "wasm-host")]
    wasm_engine: Option<WasmEngine>,
    inspector: Option<Inspector>,
    channels: Option<Arc<dyn ChannelBridge>>,
    /// The shared halves of every declared dead letter queue, so the HTTP
    /// control plane and the runner see one summary and one request slot per
    /// workflow. `None` for a library embedder, which gets a queue with a
    /// private shared half.
    dlq_registry: Option<Arc<DlqRegistry>>,
    #[cfg(feature = "crypto")]
    key_provider: Option<Arc<dyn KeyProvider>>,
    /// Outbound alert dispatcher shared with healed connectors and DLQs.
    alerts: AlertDispatcher,
    /// OpenLineage event dispatcher shared with workflow runners.
    lineage: Option<Arc<super::lineage::LineageDispatcher>>,
}

impl ServiceBuilder {
    /// Create a builder with every compiled-in connector and codec registered,
    /// plus the default channel registry when `connector-channel` is enabled.
    pub fn new() -> Self {
        super::factories::register_builtin_factories(Self::empty())
    }

    /// Create a builder with an empty registry and no runtimes.
    pub fn empty() -> Self {
        Self {
            registry: Registry::new(),
            runtimes: HashMap::new(),
            #[cfg(feature = "wasm-host")]
            wasm_engine: None,
            inspector: None,
            channels: None,
            dlq_registry: None,
            #[cfg(feature = "crypto")]
            key_provider: None,
            alerts: AlertDispatcher::noop(),
            lineage: None,
        }
    }

    /// Attach an outbound [`AlertDispatcher`] for connector heal-exhaustion and DLQ alerts.
    pub fn alerts(mut self, alerts: AlertDispatcher) -> Self {
        self.alerts = alerts;
        self
    }

    /// Attach an OpenLineage [`LineageDispatcher`](super::lineage::LineageDispatcher)
    /// for emitting workflow run lifecycle events.
    pub fn lineage(mut self, lineage: Arc<super::lineage::LineageDispatcher>) -> Self {
        self.lineage = Some(lineage);
        self
    }

    /// Supply a key provider for declarative `encryption`, `encrypt`, and
    /// `decrypt` blocks without embedding keys directly in KDL config.
    #[cfg(feature = "crypto")]
    pub fn key_provider(mut self, provider: Arc<dyn KeyProvider>) -> Self {
        self.key_provider = Some(provider);
        self
    }

    /// Share every declared dead letter queue's summary and request slot
    /// with `registry`, which the HTTP control plane reads.
    ///
    /// Without it a queue still records and replays; only
    /// `GET /api/dlq` and the replay endpoints have nothing to address.
    pub fn dlq_registry(mut self, registry: Arc<DlqRegistry>) -> Self {
        self.dlq_registry = Some(registry);
        self
    }

    /// Supply the runtime for the processor node declared with id
    /// `processor_id` and no `path` key.
    ///
    /// Any [`PipelineRuntime`] is accepted, typically a native
    /// [`Pipeline`](crate::pipeline::Pipeline).
    pub fn runtime(
        mut self,
        processor_id: impl Into<String>,
        runtime: impl PipelineRuntime + 'static,
    ) -> Self {
        self.runtimes
            .insert(processor_id.into(), SharedRuntime::new(Box::new(runtime)));
        self
    }

    /// Publish the built topology into `inspector` during
    /// [`build`](Self::build).
    ///
    /// The builder is the only place that knows both the concrete runtime kind
    /// (before the `Box<dyn PipelineRuntime>` erases it) and the configured
    /// source and sink sets, which is exactly what the topology is.
    pub fn inspector(mut self, inspector: Inspector) -> Self {
        self.inspector = Some(inspector);
        self
    }

    /// Register the shared channel bridge every `ChannelSource`/`ChannelSink`
    /// node resolves its named half through.
    ///
    /// [`new`](Self::new) attaches a default
    /// `crate::connector::channel::ChannelRegistry` automatically when
    /// `connector-channel` is enabled; call this to supply a different
    /// instance (for example, to share one registry across two independently
    /// built services).
    pub fn channels(mut self, channels: Arc<dyn ChannelBridge>) -> Self {
        self.channels = Some(channels);
        self
    }

    /// Set the [`WasmEngine`] used to load every `wasm` node's module. If not
    /// set and the workflow declares one, `build` creates a default engine
    /// automatically and shares it across every `wasm` node.
    #[cfg(feature = "wasm-host")]
    pub fn wasm_engine(mut self, engine: WasmEngine) -> Self {
        self.wasm_engine = Some(engine);
        self
    }

    /// Register a connector (builder-style chaining).
    ///
    /// A connector registered under a name that is already taken replaces it.
    pub fn connector<C: Connector>(mut self, connector: C) -> Self {
        self.registry.register(connector);
        self
    }

    /// Register a codec factory (builder-style chaining).
    pub fn codec<F: CodecFactory>(mut self, factory: F) -> Self {
        self.registry.register_codec(factory);
        self
    }

    /// Access the inner registry (for inspection or passing to helpers).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The inner registry, for the built-in registration in
    /// `register_builtin_factories`.
    pub(crate) fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
    }

    /// Assemble one [`BuiltWorkflow`] per workflow declared in `config`, in
    /// declaration order, sharing one [`Registry`] and, when enabled, one
    /// [`Inspector`] topology across all of them.
    ///
    /// Every sink is wrapped in a
    /// [`RetryingSink`](crate::io::RetryingSink). Sources are
    /// wrapped in a [`RetryingSource`](crate::io::RetryingSource)
    /// too, except under `run "stream"`, where the stream runner's
    /// own re-poll is already the retry loop.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if any workflow fails to build; see
    /// `build_one` for the full list of failure modes.
    pub fn build(self, config: &ServiceConfig) -> Result<Vec<BuiltWorkflow>, SaciError> {
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
    /// [`build`](Self::build) is this plus one
    /// [`WorkflowFactory::build`] per declared workflow and one
    /// [`WorkflowFactory::publish_topology`]; the lifecycle supervisor keeps the
    /// factory instead, because `start` and `restart` rebuild one workflow at a
    /// time.
    pub fn into_factory(mut self, config: &ServiceConfig) -> WorkflowFactory {
        let registry = Arc::new(std::mem::replace(&mut self.registry, Registry::new()));
        let run = match &config.mode {
            ServiceMode::Standalone { config: standalone } => standalone.run.clone(),
            ServiceMode::Cluster { .. } => RunMode::Continuous,
        };
        let batch_resume = matches!(
            &config.store,
            Some(StoreConfig::Redb {
                batch_resume: true,
                ..
            })
        );
        WorkflowFactory {
            builder: self,
            registry,
            heal: config.heal.clone(),
            data_dir: config.node.data_dir.clone(),
            service: config.node.label(),
            run,
            flow: FlowPlan::from_config(config),
            batch_resume,
            encryption: config.encryption.clone(),
        }
    }

    /// Build every declared source, sink and codec across every
    /// workflow, skipping processor nodes and the workflow-graph check that
    /// needs their built components.
    ///
    /// Exercises the same connector factories [`build`](Self::build)
    /// does: each node's `config` is deserialized into its connector-specific,
    /// `deny_unknown_fields` struct and run through that struct's own
    /// `validate()`. Every built-in connector's factory does this
    /// synchronously with no network or broker connection (`serve` connects
    /// lazily, on the first read or write), so this check needs no live
    /// service and, unlike `build`, no processor artifact on disk either,
    /// which is what lets a template config naming a `pipelines/*.wasm`
    /// placeholder still have its connectors checked.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the workflow, node id and
    /// offending key for the first source, sink or codec that fails to
    /// build. An unregistered connector name is one such error, exactly as it
    /// is in `build`; the caller distinguishes it the same way
    /// (`saci-service validate`'s `is_unknown_factory_error`).
    pub fn build_connectors_only(mut self, config: &ServiceConfig) -> Result<(), SaciError> {
        let registry = Arc::new(std::mem::replace(&mut self.registry, Registry::new()));
        let service = config.node.label();
        for workflow in &config.workflows {
            #[cfg(feature = "crypto")]
            let mut cached_provider = None;
            let codecs = build_codecs(
                &registry,
                &workflow.codecs,
                config.encryption.as_ref(),
                #[cfg(feature = "crypto")]
                self.key_provider.as_ref(),
                #[cfg(feature = "crypto")]
                &mut cached_provider,
            )?;
            let build = NodeBuild {
                registry: &registry,
                codecs: &codecs,
                heal: &config.heal,
                service: &service,
                encryption: config.encryption.as_ref(),
                #[cfg(feature = "crypto")]
                cached_provider: std::cell::RefCell::new(cached_provider),
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

    /// Assemble a [`BuiltWorkflow`] for one declared `workflow`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if:
    /// - A codec names an unregistered format.
    /// - A source or sink names an unregistered connector `type`.
    /// - A source or sink factory returns an error.
    /// - A `wasm`/`plugin` node names a module/library that cannot be
    ///   resolved, compiled, or described.
    /// - A `wasm` node names a module and the wasmtime engine this builder
    ///   would create for it cannot be created, which on a 64-bit host means
    ///   the pooling allocator's address-space reservation was refused.
    /// - A `wasm`/`plugin` node declares neither an artifact nor a runtime
    ///   registered for its id through [`runtime`](Self::runtime).
    /// - The links do not carry matching components and schemas end to end
    ///   (see [`validate_workflow_graph`](super::validation::validate_workflow_graph)).
    #[allow(clippy::too_many_arguments)]
    fn build_one(
        &mut self,
        registry: &Arc<Registry>,
        workflow: &WorkflowSpec,
        retry_sources: bool,
        heal: &HealConfig,
        data_dir: &std::path::Path,
        service: &str,
        encryption: Option<&ServiceEncryptionConfig>,
    ) -> Result<BuiltWorkflow, SaciError> {
        #[cfg(feature = "crypto")]
        let mut cached_provider = None;
        let codecs = build_codecs(
            registry,
            &workflow.codecs,
            encryption,
            #[cfg(feature = "crypto")]
            self.key_provider.as_ref(),
            #[cfg(feature = "crypto")]
            &mut cached_provider,
        )?;
        let build = NodeBuild {
            registry,
            codecs: &codecs,
            heal,
            service,
            encryption,
            #[cfg(feature = "crypto")]
            cached_provider: std::cell::RefCell::new(cached_provider),
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
                NodeKind::Processor => self.build_processor_node(id, workflow, &nodes)?,
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
            let edge_evolution = link
                .schema_evolution
                .map(|c| c.policy())
                .or_else(|| {
                    if !nodes[from].evolution.is_strict() {
                        Some(nodes[from].evolution)
                    } else if !nodes[to].evolution.is_strict() {
                        Some(nodes[to].evolution)
                    } else {
                        None
                    }
                })
                .or_else(|| workflow.schema_evolution.map(|c| c.policy()))
                .unwrap_or_else(EvolutionPolicy::strict);
            nodes[from].downstream.push(BuiltEdge {
                node: to,
                branch: link.branch.clone(),
            });
            nodes[from].edge_evolutions.push(edge_evolution);
        }

        super::validation::validate_workflow_graph(&workflow.id, &nodes)?;

        // After the nodes: a store that cannot be opened refuses the
        // workflow the way a sink node does, and it is worth hearing about
        // the graph first.
        let dlq = self.build_dlq(workflow, registry, data_dir)?;

        // `run`, `flow` and `batch_resume` are the factory's to fill: they
        // come from the whole config, which this method never sees.
        Ok(BuiltWorkflow {
            workflow_id: workflow.id.clone(),
            workflow_name: workflow.name.clone(),
            nodes,
            registry: registry.clone(),
            inspector: self.inspector.clone(),
            dlq,
            run: RunMode::default(),
            flow: FlowPlan::default(),
            batch_resume: false,
            lineage: self.lineage.clone(),
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
            Some(shared) => shared,
            None => Arc::new(DlqShared::new(&workflow.id, &dlq.0)),
        };
        DeadLetterQueue::build(workflow, data_dir, registry, shared)
            .map(|q| Some(q.with_alerts(self.alerts.clone())))
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
        let connector = lookup_connector(build.registry, Role::Source, &spec.connector, &spec.id)?;
        let settings = resolve_heal(
            spec.heal.as_ref(),
            build.heal,
            connector.rebuild(Role::Source, &spec.config),
            &workflow.id,
            "source",
            &spec.id,
            &spec.connector,
        )?;
        let plain_config = config_with_schema(&spec.config, &spec.component, &workflow.schemas);
        #[cfg(feature = "crypto")]
        let (node_config, decrypt_ctx) = match &spec.decrypt {
            Some(cfg) => {
                let prepared = prepare_node_crypto(
                    &format!("workflow '{}' source '{}'.decrypt", workflow.id, spec.id),
                    &spec.id,
                    &spec.component,
                    plain_config,
                    cfg,
                    false,
                    self.key_provider.as_ref(),
                    build,
                )?;
                let wire_config = prepared.wire_config;
                (
                    wire_config,
                    Some((prepared.plain_schema, prepared.cipher, prepared.mode)),
                )
            }
            None => (plain_config, None),
        };
        #[cfg(not(feature = "crypto"))]
        let node_config = {
            let _ = build.encryption;
            if spec.decrypt.is_some() {
                return Err(super::factories::missing_crypto_error(&format!(
                    "workflow '{}' source '{}'.decrypt",
                    workflow.id, spec.id
                )));
            }
            plain_config
        };

        let on_corrupt = spec.on_corrupt.unwrap_or_default();
        let bound = spec.codec.as_deref().map(|tid| build.codecs[tid].clone());
        let rebuilder = Rebuilder::new(
            build.registry.clone(),
            &spec.connector,
            &spec.id,
            node_config,
            bound,
            self.channels.clone(),
            retry_sources.then(|| spec.retry.to_retry_mode()),
        )
        .with_identity(build.identity(workflow, &spec.id))
        .with_skip_corrupt(on_corrupt != super::config::CorruptRecordPolicy::Fail);
        let built = rebuilder.build_source()?;
        let evolution = spec
            .schema_evolution
            .or(workflow.schema_evolution)
            .map_or_else(EvolutionPolicy::strict, |c| c.policy());
        let mut source: Box<dyn Source> = match settings {
            Some(settings) => Box::new(
                HealingSource::new(built, rebuilder, settings, &workflow.id, &spec.id)
                    .with_evolution(evolution)
                    .with_alerts(self.alerts.clone()),
            ),
            None => built,
        };
        if on_corrupt != super::config::CorruptRecordPolicy::Fail {
            source.set_skip_corrupt(true);
        }
        #[cfg(feature = "crypto")]
        let source: Box<dyn Source> = match decrypt_ctx {
            Some((plain_schema, cipher, mode)) => {
                Box::new(DecryptingSource::new(source, plain_schema, cipher, mode)?)
            }
            None => source,
        };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            connector: spec.connector.clone(),
            component: Some(intern_component_name(&spec.component)),
            kind: BuiltNodeKind::Source(source),
            downstream: Vec::new(),
            artifact: None,
            window: None,
            heal_recovered: None,
            evolution,
            edge_evolutions: Vec::new(),
            on_corrupt,
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
        let connector = lookup_connector(build.registry, Role::Sink, &spec.connector, &spec.id)?;
        let settings = resolve_heal(
            spec.heal.as_ref(),
            build.heal,
            connector.rebuild(Role::Sink, &spec.config),
            &workflow.id,
            "sink",
            &spec.id,
            &spec.connector,
        )?;
        let plain_config = config_with_schema(&spec.config, &spec.component, &workflow.schemas);
        #[cfg(feature = "crypto")]
        let (node_config, encrypt_ctx) = match &spec.encrypt {
            Some(cfg) => {
                let prepared = prepare_node_crypto(
                    &format!("workflow '{}' sink '{}'.encrypt", workflow.id, spec.id),
                    &spec.id,
                    &spec.component,
                    plain_config,
                    cfg,
                    true,
                    self.key_provider.as_ref(),
                    build,
                )?;
                let wire_config = prepared.wire_config;
                (
                    wire_config,
                    Some((prepared.plain_schema, prepared.cipher, prepared.mode)),
                )
            }
            None => (plain_config, None),
        };
        #[cfg(not(feature = "crypto"))]
        let node_config = {
            let _ = build.encryption;
            if spec.encrypt.is_some() {
                return Err(super::factories::missing_crypto_error(&format!(
                    "workflow '{}' sink '{}'.encrypt",
                    workflow.id, spec.id
                )));
            }
            plain_config
        };

        let bound = spec.codec.as_deref().map(|tid| build.codecs[tid].clone());
        let rebuilder = Rebuilder::new(
            build.registry.clone(),
            &spec.connector,
            &spec.id,
            node_config,
            bound,
            self.channels.clone(),
            Some(spec.retry.to_retry_mode()),
        )
        .with_identity(build.identity(workflow, &spec.id));
        let built = rebuilder.build_sink()?;
        let evolution = spec
            .schema_evolution
            .or(workflow.schema_evolution)
            .map_or_else(EvolutionPolicy::strict, |c| c.policy());
        let (sink, heal_recovered): (Box<dyn Sink>, _) = match settings {
            Some(settings) => {
                let healing = HealingSink::new(built, rebuilder, settings, &workflow.id, &spec.id)
                    .with_evolution(evolution)
                    .with_alerts(self.alerts.clone());
                let recovered = healing.recovered_flag();
                (Box::new(healing), Some(recovered))
            }
            None => (built, None),
        };
        #[cfg(feature = "crypto")]
        let sink: Box<dyn Sink> = match encrypt_ctx {
            Some((plain_schema, cipher, mode)) => {
                Box::new(EncryptingSink::new(sink, plain_schema, cipher, mode)?)
            }
            None => sink,
        };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            connector: spec.connector.clone(),
            component: Some(intern_component_name(&spec.component)),
            kind: BuiltNodeKind::Sink(sink),
            downstream: Vec::new(),
            artifact: None,
            heal_recovered,
            window: None,
            evolution,
            edge_evolutions: Vec::new(),
            on_corrupt: CorruptRecordPolicy::default(),
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
    fn build_processor_node(
        &mut self,
        id: &str,
        workflow: &WorkflowSpec,
        built_nodes: &[BuiltNode],
    ) -> SaciResult<BuiltNode> {
        if let Some(spec) = workflow.operator.iter().find(|o| o.id == id) {
            return build_operator_node(spec, workflow, built_nodes);
        }
        #[cfg(feature = "wasm-host")]
        if let Some(spec) = workflow.wasm.iter().find(|w| w.id == id) {
            return self.build_wasm_node(spec, workflow);
        }
        #[cfg(not(feature = "wasm-host"))]
        if let Some(spec) = workflow.wasm.iter().find(|w| w.id == id) {
            return Err(super::factories::missing_wasm_host_error(
                &spec.id,
                &workflow.id,
            ));
        }
        #[cfg(feature = "plugin-host")]
        if let Some(spec) = workflow.plugin.iter().find(|p| p.id == id) {
            return self.build_plugin_node(spec, workflow);
        }
        #[cfg(not(feature = "plugin-host"))]
        if let Some(spec) = workflow.plugin.iter().find(|p| p.id == id) {
            return Err(super::factories::missing_plugin_host_error(
                &spec.id,
                &workflow.id,
            ));
        }
        Err(SaciError::configuration(format!(
            "workflow '{}': processor '{id}' names neither a wasm, plugin, nor operator node",
            workflow.id
        )))
    }

    #[cfg(feature = "wasm-host")]
    fn build_wasm_node(
        &mut self,
        spec: &super::config::WasmSpec,
        workflow: &WorkflowSpec,
    ) -> SaciResult<BuiltNode> {
        let workflow_id = &workflow.id;
        // The embedder-facing half of the windowing capability question, the
        // counterpart of the `#[cfg(not(feature = "wasm-host"))]` arm in
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
            match &spec.path {
                Some(module) => {
                    // Built at most once per builder and only for a node that
                    // names a `path`, so a config with no module-backed wasm
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
                                    "processor '{}': {}. \
                                     The pooling instance allocator reserves ~1 TiB of address \
                                     space per engine, so this is usually a process \
                                     address-space limit (RLIMIT_AS / `ulimit -v`, common in \
                                     containers and CI sandboxes) or too many live engines in \
                                     one process; share one with \
                                     `ServiceBuilder::wasm_engine`",
                                    spec.id,
                                    e.message()
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
                    let runtime = self.runtimes.get(&spec.id).cloned().ok_or_else(|| {
                        SaciError::configuration(format!(
                            "processor '{}': {RUNTIME_SOURCE_HELP}",
                            spec.id
                        ))
                    })?;
                    (Box::new(runtime), "native", None)
                }
            };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            connector: kind.to_string(),
            component: None,
            kind: BuiltNodeKind::Processor { runtime, kind },
            downstream: Vec::new(),
            artifact,
            window: spec.window.clone(),
            heal_recovered: None,
            evolution: workflow
                .schema_evolution
                .map_or_else(EvolutionPolicy::strict, |c| c.policy()),
            edge_evolutions: Vec::new(),
            on_corrupt: CorruptRecordPolicy::default(),
        })
    }

    #[cfg(feature = "plugin-host")]
    fn build_plugin_node(
        &mut self,
        spec: &super::config::PluginSpec,
        workflow: &WorkflowSpec,
    ) -> SaciResult<BuiltNode> {
        let workflow_id = &workflow.id;
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
            match &spec.path {
                Some(library) => {
                    let runtime = load_plugin_runtime(&spec, None)?
                        .with_identity(workflow_id.to_string(), spec.id.clone());
                    (Box::new(runtime), "plugin", Some(library.clone()))
                }
                None => {
                    let runtime = self.runtimes.get(&spec.id).cloned().ok_or_else(|| {
                        SaciError::configuration(format!(
                            "processor '{}': {RUNTIME_SOURCE_HELP}",
                            spec.id
                        ))
                    })?;
                    (Box::new(runtime), "native", None)
                }
            };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            connector: kind.to_string(),
            component: None,
            kind: BuiltNodeKind::Processor { runtime, kind },
            downstream: Vec::new(),
            artifact,
            window: spec.window.clone(),
            heal_recovered: None,
            evolution: workflow
                .schema_evolution
                .map_or_else(EvolutionPolicy::strict, |c| c.policy()),
            edge_evolutions: Vec::new(),
            on_corrupt: CorruptRecordPolicy::default(),
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
#[cfg(all(
    feature = "windows",
    any(feature = "wasm-host", feature = "plugin-host")
))]
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

#[cfg(all(
    not(feature = "windows"),
    any(feature = "wasm-host", feature = "plugin-host")
))]
fn config_with_window<T>(spec: T) -> T {
    spec
}

/// The two processor node specs, unified for [`config_with_window`].
#[cfg(all(
    feature = "windows",
    any(feature = "wasm-host", feature = "plugin-host")
))]
trait WindowConfigCarrier {
    fn window_config(&self) -> Option<&super::config::WindowConfig>;
    fn config_mut(&mut self) -> &mut HashMap<String, String>;
}

#[cfg(all(feature = "windows", feature = "wasm-host"))]
impl WindowConfigCarrier for super::config::WasmSpec {
    fn window_config(&self) -> Option<&super::config::WindowConfig> {
        self.window.as_ref()
    }
    fn config_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.config
    }
}

#[cfg(all(feature = "windows", feature = "plugin-host"))]
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

/// Resolve every declared `codec` node against `registry`, building one
/// instance per declaration.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a codec names an
/// unregistered format, or when its factory rejects its `config`. The
/// unregistered-format wording, feature hint included, belongs to
/// [`missing_codec_error`].
fn build_codecs(
    registry: &Registry,
    specs: &[CodecSpec],
    encryption: Option<&ServiceEncryptionConfig>,
    #[cfg(feature = "crypto")] injected_provider: Option<&Arc<dyn KeyProvider>>,
    #[cfg(feature = "crypto")] cached_provider: &mut Option<Arc<dyn KeyProvider>>,
) -> SaciResult<HashMap<String, Arc<dyn Codec>>> {
    let mut out = HashMap::with_capacity(specs.len());
    #[cfg(not(feature = "crypto"))]
    let _ = encryption;
    for spec in specs {
        let factory = registry.codecs().get(&spec.format).ok_or_else(|| {
            missing_codec_error(&spec.id, &spec.format, &registry.codecs().names())
        })?;
        let codec: Arc<dyn Codec> = factory.build(&spec.config)?;
        #[cfg(feature = "crypto")]
        let codec: Arc<dyn Codec> = if let Some(enc) = &spec.encryption
            && enc.enabled
        {
            let ctx = format!("codec '{}'.encryption", spec.id);
            enc.validate(&ctx)?;
            let cipher = resolve_cipher(
                &ctx,
                injected_provider,
                encryption,
                cached_provider,
                enc.algorithm.as_deref(),
                enc.active_key.as_deref(),
            )?;
            Arc::new(EncryptedCodec::new(codec, cipher))
        } else {
            codec
        };
        #[cfg(not(feature = "crypto"))]
        if let Some(enc) = &spec.encryption
            && enc.enabled
        {
            return Err(super::factories::missing_crypto_error(&format!(
                "codec '{}'.encryption",
                spec.id
            )));
        }
        out.insert(spec.id.clone(), codec);
    }
    Ok(out)
}

#[cfg(feature = "crypto")]
struct PreparedNodeCrypto {
    wire_config: ConfigValue,
    plain_schema: Arc<arrow_schema::Schema>,
    cipher: Arc<crate::crypto::Cipher>,
    mode: EncryptionMode,
}

#[cfg(feature = "crypto")]
#[allow(clippy::too_many_arguments)]
fn prepare_node_crypto(
    context: &str,
    node_id: &str,
    component: &str,
    plain_config: ConfigValue,
    crypto_cfg: &super::config::NodeCryptoConfig,
    allow_active_key: bool,
    injected_provider: Option<&Arc<dyn KeyProvider>>,
    build: &NodeBuild<'_>,
) -> SaciResult<PreparedNodeCrypto> {
    crypto_cfg.validate(context, allow_active_key)?;
    let mut cached = build.cached_provider.borrow_mut();
    let cipher = resolve_cipher(
        context,
        injected_provider,
        build.encryption,
        &mut cached,
        crypto_cfg.algorithm.as_deref(),
        crypto_cfg.active_key.as_deref(),
    )?;
    drop(cached);
    let mode = crypto_cfg.to_encryption_mode(context)?;
    let fields =
        crate::connector::parse_optional_fields(&plain_config, node_id)?.ok_or_else(|| {
            SaciError::configuration(format!(
                "{context}: source decrypt / sink encrypt requires declared 'field' entries \
                 or a workflow 'schema' matching component '{component}'"
            ))
        })?;
    let plain_schema = crate::connector::schema_of(&fields)?;
    let _ = wire_schema(&plain_schema, &mode)?;
    let wire_config = rewrite_config_fields_for_wire(plain_config, &fields, &mode)?;
    Ok(PreparedNodeCrypto {
        wire_config,
        plain_schema,
        cipher,
        mode,
    })
}

#[cfg(feature = "crypto")]
fn rewrite_config_fields_for_wire(
    mut config: ConfigValue,
    fields: &[crate::connector::FieldSpec],
    mode: &EncryptionMode,
) -> SaciResult<ConfigValue> {
    let wire_type = |enc: FieldEncoding| match enc {
        FieldEncoding::Binary => "binary",
        FieldEncoding::Base64Utf8 => "utf8",
    };
    let wire_entries: Vec<ConfigValue> = match mode {
        EncryptionMode::Payload { encoding } => vec![serde_json::json!({
            "id": PAYLOAD_COLUMN,
            "type": wire_type(*encoding),
            "nullable": false,
        })],
        EncryptionMode::Fields {
            fields: targets,
            encoding,
        } => fields
            .iter()
            .map(|f| {
                if targets.iter().any(|t| t == &f.name) {
                    Ok(serde_json::json!({
                        "id": f.name,
                        "type": wire_type(*encoding),
                        "nullable": f.nullable,
                    }))
                } else {
                    serde_json::to_value(f).map_err(|e| {
                        SaciError::configuration(format!(
                            "crypto: failed to serialize field '{}': {e}",
                            f.name
                        ))
                    })
                }
            })
            .collect::<SaciResult<Vec<_>>>()?,
    };
    if let ConfigValue::Object(table) = &mut config {
        table.insert("field".to_string(), ConfigValue::Array(wire_entries));
    }
    Ok(config)
}

/// `config` as the connector is built from it: with the workflow's schema for
/// `component` filled in as the `field` entries when the node declares none of
/// its own.
///
/// The clone is what the node's `Rebuilder` keeps, so a healed connector is
/// rebuilt from the same entries as the first build. A node that declares its
/// own `field` lines keeps them, and so does one whose `component` no
/// declared schema names.
fn config_with_schema(
    config: &ConfigValue,
    component: &str,
    schemas: &[SchemaSpec],
) -> ConfigValue {
    let mut config = config.clone();
    if let ConfigValue::Object(table) = &mut config
        && !table.contains_key("field")
        && let Some(schema) = schemas.iter().find(|schema| schema.id == component)
    {
        table.insert("field".to_string(), schema.field.clone());
    }
    config
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::connector::{ConfigMap, ConfigValue, ConnectorContext, REBUILD_UNDECLARED};
    use crate::dataset::Dataset;
    use crate::pipeline::Pipeline;
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, RetryConfig, ServiceMode, SinkSpec,
        SourceSpec, StandaloneConfig, WorkflowSpec,
    };
    use crate::system::{System, SystemMeta};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    #[cfg(feature = "connector-channel")]
    struct NoopConnector;
    #[cfg(feature = "connector-channel")]
    impl Connector for NoopConnector {
        fn name(&self) -> &'static str {
            "noop"
        }
        fn source(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            use crate::connector::channel::ChannelSource;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (src, _tx) = ChannelSource::new(schema, 1);
            Ok(Box::new(src))
        }
        fn sink(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, SaciError> {
            use crate::connector::channel::ChannelSink;
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
            encryption: None,
            alerts: Default::default(),
            lineage: Default::default(),
            variables: HashMap::new(),
        }
    }

    fn empty_workflow(id: &str) -> WorkflowSpec {
        WorkflowSpec {
            id: id.to_string(),
            name: None,
            codecs: Vec::new(),
            schemas: Vec::new(),
            sources: Vec::new(),
            wasm: Vec::new(),
            plugin: Vec::new(),
            operator: Vec::new(),
            sinks: Vec::new(),
            links: Vec::new(),
            dlq: None,
            run: None,
            schema_evolution: None,
        }
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_source_straight_to_sink_builds_with_no_processor() {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            heal: None,
            flow_control: None,
            id: "src1".to_string(),
            name: None,
            connector: "noop".to_string(),
            codec: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            decrypt: None,
            schema_evolution: None,
            on_corrupt: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            heal: None,
            id: "sink1".to_string(),
            name: None,
            connector: "noop".to_string(),
            codec: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            encrypt: None,
            schema_evolution: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
            schema_evolution: None,
        });
        let config = base_config(workflow);

        let service = ServiceBuilder::empty()
            .connector(NoopConnector)
            .build(&config)
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
            connector: "ghost".to_string(),
            codec: None,
            component: "comp".to_string(),
            retry: RetryConfig::default(),
            decrypt: None,
            schema_evolution: None,
            on_corrupt: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::empty().build(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("ghost"));
    }

    /// A declared `plugin` node in a binary with no plugin host is refused by
    /// name, through `build` rather than through the binary's gate.
    ///
    /// `validate_build_capabilities` is what an operator hits, and its own
    /// tests cover it. This covers the other caller: a library embedder
    /// driving `ServiceBuilder` directly, for whom
    /// `build_processor_node`'s `#[cfg(not(feature = "plugin-host"))]` arm is the
    /// only refusal there is. Without this test, that arm could fall through
    /// to the trailing "names neither a wasm nor a plugin node" error, the
    /// message a reader would be told after deleting the arm, and every
    /// existing test would still pass, because
    /// `every_host_refusal_names_the_feature_that_supplies_it` only calls
    /// the producer directly and never asks whether the builder reaches it.
    ///
    /// `plugin-host` is not in the `server` bundle, so this runs under a plain
    /// `cargo test -p saci --features server`.
    #[cfg(not(feature = "plugin-host"))]
    #[test]
    fn test_plugin_node_without_the_host_is_refused_by_the_builder() {
        let mut workflow = empty_workflow("w");
        workflow.plugin.push(super::super::config::PluginSpec {
            id: "audit".to_string(),
            name: None,
            path: Some("libaudit.so".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::empty().build(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("--features plugin-host"),
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
    /// Needs `wasm-host`, because a missing host is refused before the window
    /// is looked at. `cargo nextest run -p saci --no-default-features
    /// --features service,wasm-host,plugin-host,connector-channel --lib` is
    /// what exercises it.
    #[cfg(all(not(feature = "windows"), feature = "wasm-host"))]
    #[test]
    fn test_a_window_block_without_the_engine_is_refused_by_the_builder() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "aggregate".to_string(),
            name: None,
            path: Some("aggregate.wasm".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: Some(super::super::config::WindowConfig {
                spec: crate::window::WindowSpec::tumbling(30_000),
                time_field: "timestamp_ms".to_string(),
                key_fields: Vec::new(),
                allowed_lateness_ms: 0,
            }),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::empty().build(&config).unwrap_err();
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
    #[cfg(all(not(feature = "wasm-host"), not(feature = "windows")))]
    #[test]
    fn test_a_missing_host_is_named_before_a_missing_windowing_engine() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "aggregate".to_string(),
            name: None,
            path: Some("aggregate.wasm".to_string()),
            sha3_256: None,
            config: HashMap::new(),
            window: Some(super::super::config::WindowConfig {
                spec: crate::window::WindowSpec::tumbling(30_000),
                time_field: "timestamp_ms".to_string(),
                key_fields: Vec::new(),
                allowed_lateness_ms: 0,
            }),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::empty().build(&config).unwrap_err();
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
            connector: "ghost".to_string(),
            codec: None,
            component: "comp".to_string(),
            retry: RetryConfig::default(),
            encrypt: None,
            schema_evolution: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::empty().build(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("ghost"));
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_wasm_node_with_no_module_and_no_registered_runtime_is_an_error() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "p".to_string(),
            name: None,
            path: None,
            sha3_256: None,
            config: std::collections::HashMap::new(),
            window: None,
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::empty().build(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("processor 'p'"), "got: {err}");
        assert!(
            err.message().contains("ServiceBuilder::runtime"),
            "got: {err}"
        );
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_wasm_node_with_no_module_uses_the_registered_native_runtime() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "p".to_string(),
            name: None,
            path: None,
            sha3_256: None,
            config: std::collections::HashMap::new(),
            window: None,
        });
        let config = base_config(workflow);

        let service = ServiceBuilder::empty()
            .runtime("p", Pipeline::new("native-p"))
            .build(&config)
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

    #[cfg(all(feature = "wasm-host", feature = "connector-channel"))]
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

        struct StrictConnector;
        impl Connector for StrictConnector {
            fn name(&self) -> &'static str {
                "strict"
            }
            fn roles(&self) -> &'static [Role] {
                &[Role::Source]
            }
            fn source(
                &self,
                config: &ConfigValue,
                _ctx: &ConnectorContext,
            ) -> Result<Box<dyn Source>, SaciError> {
                serde_json::from_value::<StrictSourceConfig>(config.clone())
                    .map_err(|e| SaciError::configuration(format!("strict config: {e}")))?;
                use crate::connector::channel::ChannelSource;
                let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
                let (src, _tx) = ChannelSource::new(schema, 1);
                Ok(Box::new(src))
            }
        }

        // Source, processor and sink linked end to end, the same shape
        // `build` walks, but the wasm node names a module this test
        // never writes to disk, so `build` would fail trying to read it.
        fn workflow_naming_a_missing_module(source_config: ConfigValue) -> WorkflowSpec {
            let mut workflow = empty_workflow("w");
            workflow.sources.push(SourceSpec {
                heal: None,
                flow_control: None,
                id: "src1".to_string(),
                name: None,
                connector: "strict".to_string(),
                codec: None,
                component: "comp1".to_string(),
                retry: RetryConfig::default(),
                decrypt: None,
                schema_evolution: None,
                on_corrupt: None,
                config: source_config,
            });
            workflow.sinks.push(SinkSpec {
                heal: None,
                id: "sink1".to_string(),
                name: None,
                connector: "noop".to_string(),
                codec: None,
                component: "comp1".to_string(),
                retry: RetryConfig::default(),
                encrypt: None,
                schema_evolution: None,
                config: ConfigValue::Object(ConfigMap::new()),
            });
            workflow.wasm.push(super::super::config::WasmSpec {
                id: "p".to_string(),
                name: None,
                path: Some("pipelines/does-not-exist.wasm".to_string()),
                sha3_256: None,
                config: std::collections::HashMap::new(),
                window: None,
            });
            workflow.links.push(super::super::config::LinkSpec {
                from: "src1".to_string(),
                to: "p".to_string(),
                branch: None,
                schema_evolution: None,
            });
            workflow.links.push(super::super::config::LinkSpec {
                from: "p".to_string(),
                to: "sink1".to_string(),
                branch: None,
                schema_evolution: None,
            });
            workflow
        }

        // Half 1: a valid connector config plus a processor artifact that
        // does not exist on disk. This is the whole reason
        // `build_connectors_only` exists: it must succeed here, where
        // `build` would fail on the missing module before ever reaching
        // the sink.
        let mut valid = ConfigMap::new();
        valid.insert(
            "greeting".to_string(),
            ConfigValue::String("hi".to_string()),
        );
        let config = base_config(workflow_naming_a_missing_module(ConfigValue::Object(valid)));
        ServiceBuilder::empty()
            .connector(StrictConnector)
            .connector(NoopConnector)
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
        let err = ServiceBuilder::empty()
            .connector(StrictConnector)
            .connector(NoopConnector)
            .build_connectors_only(&config)
            .expect_err(
                "an unknown key in a connector's config must fail connectors-only validation",
            );
        assert!(
            err.message().contains("strict") && err.message().contains("no_such_key"),
            "error should name the connector and the offending key: {err}"
        );
    }

    /// `build_connectors_only` walks the same `build_source_node`/
    /// `build_sink_node` path `build` does, so a `saci` source and sink
    /// pair gets the node identity `ConnectorContext::identity` requires
    /// without ever reaching the "no identity" refusal.
    #[cfg(feature = "connector-peer")]
    #[test]
    fn build_connectors_only_binds_the_node_identity_for_a_saci_pair() {
        let raw = r#"
mode "standalone"

node id=1 name="svc-a" data_dir="/tmp/saci-test"

run "stream"

workflow "w" {
    source "in" connector="peer" component="Tick" {
        config {
            bind "127.0.0.1:0"
            field "v" type="int64" nullable=#false
        }
    }
    sink "out" connector="peer" component="Tick" {
        config {
            connect "127.0.0.1:9"
            field "v" type="int64" nullable=#false
        }
    }
    link from="in" to="out"
}
"#;
        let config = ServiceConfig::from_kdl(raw).expect("config loads");
        ServiceBuilder::new()
            .build_connectors_only(&config)
            .expect("the host binds the node identity for both saci halves");
    }

    /// The `schema` node of a workflow fills in the `field` entries of a node
    /// that declares none, leaves a node that declares its own alone, and
    /// leaves a node whose component no schema names alone too.
    #[test]
    fn config_with_schema_fills_in_only_a_node_that_declares_no_field() {
        let declared = crate::connector::from_kdl_str("field \"id\" type=\"int64\"\n")
            .expect("the field parses");
        let schemas = [SchemaSpec {
            id: "orders".to_string(),
            field: declared["field"].clone(),
        }];

        let bare = ConfigValue::Object(ConfigMap::new());
        assert_eq!(
            config_with_schema(&bare, "orders", &schemas)["field"],
            schemas[0].field
        );

        let own = crate::connector::from_kdl_str("field \"x\" type=\"utf8\"\n")
            .expect("the field parses");
        assert_eq!(config_with_schema(&own, "orders", &schemas), own);

        assert_eq!(config_with_schema(&bare, "other", &schemas), bare);
    }

    #[test]
    fn test_boxed_system_runs_on_runtime() {
        let mut pipeline = Pipeline::new("test");
        pipeline = pipeline.system_boxed(Box::new(NoopSystem));
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { pipeline.run().await }).unwrap();
    }

    #[test]
    fn test_build_returns_one_built_workflow_per_workflow() {
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
            encryption: None,
            alerts: Default::default(),
            lineage: Default::default(),
            variables: HashMap::new(),
        };

        let built = ServiceBuilder::empty()
            .build(&config)
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

    /// A connector for both roles: its source fails `failures` times and its
    /// sink fails `write_failures` writes, then each succeeds.
    struct FlakyConnector {
        failures: usize,
        calls: Arc<AtomicUsize>,
        write_failures: usize,
        write_calls: Arc<AtomicUsize>,
    }

    impl Connector for FlakyConnector {
        fn name(&self) -> &'static str {
            "flaky"
        }

        fn source(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            Ok(Box::new(FlakySource {
                failures_left: self.failures,
                calls: Arc::clone(&self.calls),
            }))
        }

        fn sink(
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

    /// A one-link workflow whose flaky source feeds a flaky sink, so
    /// `build` produces a valid graph with both nodes retry-wrapped.
    fn flaky_workflow(source_retry: RetryConfig, sink_retry: RetryConfig) -> WorkflowSpec {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            heal: None,
            flow_control: None,
            id: "src1".to_string(),
            name: None,
            connector: "flaky".to_string(),
            codec: None,
            component: "comp1".to_string(),
            retry: source_retry,
            decrypt: None,
            schema_evolution: None,
            on_corrupt: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            heal: None,
            id: "sink1".to_string(),
            name: None,
            connector: "flaky".to_string(),
            codec: None,
            component: "comp1".to_string(),
            retry: sink_retry,
            encrypt: None,
            schema_evolution: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
            schema_evolution: None,
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
        let mut service = ServiceBuilder::empty()
            .connector(FlakyConnector {
                failures: 2,
                calls: Arc::clone(&calls),
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build(&base_config(workflow))
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
        let mut service = ServiceBuilder::empty()
            .connector(FlakyConnector {
                failures: 2,
                calls: Arc::clone(&calls),
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build(&base_config(workflow))
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

    /// `run "stream"` hands the source over unwrapped: the stream runner
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
                run: RunMode::Stream,
            },
        };
        let mut service = ServiceBuilder::empty()
            .connector(FlakyConnector {
                failures: 2,
                calls: Arc::clone(&calls),
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build(&config)
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
                run: RunMode::Stream,
            },
        };
        let mut service = ServiceBuilder::empty()
            .connector(FlakyConnector {
                failures: 0,
                calls: Arc::new(AtomicUsize::new(0)),
                write_failures: 1,
                write_calls: Arc::clone(&write_calls),
            })
            .build(&config)
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
        let mut service = ServiceBuilder::empty()
            .connector(FlakyConnector {
                failures: 0,
                calls: Arc::new(AtomicUsize::new(0)),
                write_failures: 1,
                write_calls: Arc::clone(&write_calls),
            })
            .build(&base_config(workflow))
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

    /// A source-only connector that always fails, declares a rebuild sound and
    /// counts how many instances it has handed out.
    struct HealableConnector(Arc<AtomicUsize>);

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

    impl Connector for HealableConnector {
        fn name(&self) -> &'static str {
            "healable"
        }

        fn roles(&self) -> &'static [Role] {
            &[Role::Source]
        }

        fn source(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(AlwaysFailingSource))
        }

        fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
            Rebuild::Yes
        }
    }

    /// The `flaky` connector reused as a sink that refuses a rebuild, which is
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
            connector: "healable".to_string(),
            codec: None,
            component: "comp1".to_string(),
            retry: RetryConfig {
                max_attempts: 1,
                ..Default::default()
            },
            decrypt: None,
            schema_evolution: None,
            on_corrupt: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            heal: sink_heal,
            id: "sink1".to_string(),
            name: None,
            connector: "flaky".to_string(),
            codec: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            encrypt: None,
            schema_evolution: None,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
            schema_evolution: None,
        });
        workflow
    }

    fn build_heal_service(
        config: &ServiceConfig,
        builds: &Arc<AtomicUsize>,
    ) -> Result<BuiltWorkflow, SaciError> {
        Ok(ServiceBuilder::empty()
            .connector(HealableConnector(Arc::clone(builds)))
            .connector(FlakyConnector {
                failures: 0,
                calls: Arc::new(AtomicUsize::new(0)),
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build(config)?
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
            text.contains("heal is not available for connector 'flaky'"),
            "the refusal names the node and its connector: {text}"
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

    // ── BuiltWorkflow::builder ────────────────────────────────────────────────

    #[cfg(feature = "connector-channel")]
    fn int_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    /// A hand-assembled source-to-sink workflow runs through
    /// [`BuiltWorkflow::run`] and delivers every row.
    #[cfg(feature = "connector-channel")]
    #[tokio::test]
    async fn a_hand_built_workflow_moves_every_row() -> SaciResult<()> {
        use crate::connector::channel::{ChannelSink, ChannelSource};

        let (source, tx) = ChannelSource::new(int_schema(), 8);
        let (sink, mut rx) = ChannelSink::new(int_schema(), 8);
        let batch = RecordBatch::try_new(
            int_schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        tx.send(batch).await.unwrap();
        drop(tx);

        let workflow = BuiltWorkflow::builder("w")
            .source("a", "T", source)
            .sink("b", "T", sink)
            .link("a", "b")
            .build()?;
        assert_eq!(workflow.nodes[0].connector, "custom");
        assert_eq!(workflow.nodes[0].downstream.len(), 1);

        // The default run mode is continuous: it ends when the token fires,
        // which the stopper does once the sink has delivered the batch.
        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                let delivered = rx.recv().await.expect("the sink wrote").num_rows();
                cancel.cancel();
                delivered
            }
        };
        let (stats, delivered) = tokio::join!(workflow.run(cancel), stopper);
        assert_eq!(stats?.rows_processed, 4);
        assert_eq!(delivered, 4);
        Ok(())
    }

    /// A link whose target was declared before its origin is refused, as is
    /// every other shape the graph cannot carry.
    #[cfg(feature = "connector-channel")]
    #[test]
    fn the_workflow_builder_refuses_a_graph_it_cannot_order() {
        use crate::connector::channel::{ChannelSink, ChannelSource};

        let nodes = || {
            let (source, _tx) = ChannelSource::new(int_schema(), 1);
            let (sink, _rx) = ChannelSink::new(int_schema(), 1);
            BuiltWorkflow::builder("w")
                .source("a", "T", source)
                .sink("b", "T", sink)
        };

        let err = nodes().link("b", "a").build().unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message()
                .contains("declare nodes in flow order: 'a' comes before 'b'"),
            "{err}"
        );

        let err = nodes().link("a", "missing").build().unwrap_err();
        assert!(err.message().contains("undeclared node 'missing'"), "{err}");

        let err = nodes().link("a", "a").build().unwrap_err();
        assert!(err.message().contains("cannot link to itself"), "{err}");

        let (extra, _tx) = crate::connector::channel::ChannelSource::new(int_schema(), 1);
        let err = nodes().source("a", "T", extra).build().unwrap_err();
        assert!(err.message().contains("duplicate node id 'a'"), "{err}");
    }

    #[cfg(feature = "crypto")]
    #[tokio::test]
    async fn builder_key_provider_rewrites_wire_schema_and_encrypts_decrypts()
    -> Result<(), SaciError> {
        use crate::crypto::{KeyMaterial, StaticKeyProvider};
        use crate::service::config::ServiceConfig;
        use arrow_array::{Array, Int64Array, StringArray};

        struct MemPair {
            tx: Mutex<Option<tokio::sync::mpsc::Sender<RecordBatch>>>,
            rx: Mutex<Option<tokio::sync::mpsc::Receiver<RecordBatch>>>,
        }

        struct MemWireSource {
            schema: Arc<Schema>,
            rx: tokio::sync::mpsc::Receiver<RecordBatch>,
        }
        #[async_trait]
        impl Source for MemWireSource {
            fn schema(&self) -> Arc<Schema> {
                Arc::clone(&self.schema)
            }
            async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
                Ok(self.rx.recv().await)
            }
        }

        struct MemWireSink {
            schema: Arc<Schema>,
            tx: Option<tokio::sync::mpsc::Sender<RecordBatch>>,
            observed_wire_type: Arc<Mutex<Option<DataType>>>,
        }
        #[async_trait]
        impl Sink for MemWireSink {
            async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
                *self.observed_wire_type.lock().unwrap() =
                    Some(batch.schema().field(1).data_type().clone());
                let wire_vals = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("wire column is Utf8");
                assert_ne!(wire_vals.value(0), "alpha");
                assert_ne!(wire_vals.value(1), "beta");
                if let Some(tx) = &self.tx {
                    let _ = tx.send(batch.clone()).await;
                }
                Ok(())
            }
            async fn finish(&mut self) -> Result<(), SaciError> {
                self.tx.take();
                Ok(())
            }
            fn schema(&self) -> Arc<Schema> {
                Arc::clone(&self.schema)
            }
        }

        struct MemConnector {
            pair: Arc<MemPair>,
            initial: Mutex<Option<RecordBatch>>,
            collected: Arc<Mutex<Vec<RecordBatch>>>,
            observed_wire_type: Arc<Mutex<Option<DataType>>>,
        }
        impl Connector for MemConnector {
            fn name(&self) -> &'static str {
                "memwire"
            }
            fn source(
                &self,
                config: &ConfigValue,
                _ctx: &ConnectorContext,
            ) -> SaciResult<Box<dyn Source>> {
                let fields = crate::connector::parse_fields(config, "memwire")?;
                let schema = crate::connector::schema_of(&fields)?;
                if let Some(batch) = self.initial.lock().unwrap().take() {
                    let (tx, rx) = tokio::sync::mpsc::channel(4);
                    tokio::spawn(async move {
                        let _ = tx.send(batch).await;
                    });
                    Ok(Box::new(MemWireSource { schema, rx }))
                } else {
                    let rx = self.pair.rx.lock().unwrap().take().unwrap();
                    Ok(Box::new(MemWireSource { schema, rx }))
                }
            }
            fn sink(
                &self,
                config: &ConfigValue,
                _ctx: &ConnectorContext,
            ) -> SaciResult<Box<dyn Sink>> {
                let fields = crate::connector::parse_fields(config, "memwire")?;
                let schema = crate::connector::schema_of(&fields)?;
                let mut guard = self.pair.tx.lock().unwrap();
                if let Some(tx) = guard.take() {
                    Ok(Box::new(MemWireSink {
                        schema,
                        tx: Some(tx),
                        observed_wire_type: Arc::clone(&self.observed_wire_type),
                    }))
                } else {
                    struct CollectSink(Arc<Schema>, Arc<Mutex<Vec<RecordBatch>>>);
                    #[async_trait]
                    impl Sink for CollectSink {
                        async fn write_batch(
                            &mut self,
                            batch: &RecordBatch,
                        ) -> Result<(), SaciError> {
                            self.1.lock().unwrap().push(batch.clone());
                            Ok(())
                        }
                        async fn finish(&mut self) -> Result<(), SaciError> {
                            Ok(())
                        }
                        fn schema(&self) -> Arc<Schema> {
                            Arc::clone(&self.0)
                        }
                    }
                    Ok(Box::new(CollectSink(schema, Arc::clone(&self.collected))))
                }
            }
        }

        let plain_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("secret", DataType::Utf8, false),
        ]));
        let input = RecordBatch::try_new(
            plain_schema,
            vec![
                Arc::new(Int64Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let pair = Arc::new(MemPair {
            tx: Mutex::new(Some(tx)),
            rx: Mutex::new(Some(rx)),
        });
        let collected = Arc::new(Mutex::new(Vec::new()));
        let observed_wire_type = Arc::new(Mutex::new(None));

        let kdl = r#"
            mode "standalone"
            node id=1 data_dir="/tmp/saci-builder-crypto"
            run "once"
            workflow "producer" {
                schema "Rec" {
                    field "id" type="int64" nullable=#false
                    field "secret" type="utf8" nullable=#false
                }
                source "prod_in" connector="memwire" component="Rec"
                sink "prod_out" connector="memwire" component="Rec" {
                    encrypt fields="secret" encoding="base64"
                }
                link from="prod_in" to="prod_out"
            }
            workflow "consumer" {
                schema "Rec" {
                    field "id" type="int64" nullable=#false
                    field "secret" type="utf8" nullable=#false
                }
                source "cons_in" connector="memwire" component="Rec" {
                    decrypt fields="secret" encoding="base64"
                }
                sink "cons_out" connector="memwire" component="Rec"
                link from="cons_in" to="cons_out"
            }
        "#;
        let cfg = ServiceConfig::from_kdl(kdl)?;
        let provider = Arc::new(StaticKeyProvider::single(
            "k1",
            KeyMaterial::from_bytes([0x42; 32]),
        )?);
        let built = ServiceBuilder::empty()
            .connector(MemConnector {
                pair,
                initial: Mutex::new(Some(input)),
                collected: Arc::clone(&collected),
                observed_wire_type: Arc::clone(&observed_wire_type),
            })
            .key_provider(provider)
            .build(&cfg)?;
        for wf in built {
            wf.run(CancellationToken::new()).await?;
        }
        assert_eq!(*observed_wire_type.lock().unwrap(), Some(DataType::Utf8));
        let out = collected.lock().unwrap();
        assert_eq!(out.len(), 1);
        let secrets = out[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(secrets.value(0), "alpha");
        assert_eq!(secrets.value(1), "beta");
        Ok(())
    }
}
