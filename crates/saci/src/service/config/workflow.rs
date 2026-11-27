//! The `workflow` block: its nodes, links and load-time graph rules.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use super::default_config;
use super::encryption::{CodecEncryptionConfig, NodeCryptoConfig};
use super::mode::{RunMode, ServiceMode};
use super::processor::{OperatorSpec, PluginSpec, WasmSpec};
use super::retry::RetryConfig;
use super::window::WindowConfig;
use crate::config::{ConfigMap, ConfigValue, one_or_many};
use crate::connector::{parse_fields, schema_of};
use crate::error::{SaciError, SaciResult};
use crate::service::dlq::DlqConfig;
use crate::service::flow::FlowControlConfig;
use crate::service::heal::HealConfig;

/// A declared byte-format instance: a name a source or sink's `codec`
/// key can reference.
///
/// ```kdl
/// workflow "w" {
///     codec "orders-json" name="Orders NDJSON" format="ndjson" {
///         config infer_max=1000
///     }
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CodecSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Registered format name, resolved against the `CodecRegistry`.
    pub format: String,
    /// Optional whole-payload AEAD encryption wrapper around this codec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<CodecEncryptionConfig>,
    /// Handed to that format's factory. Empty table when absent.
    #[serde(
        default = "default_config",
        skip_serializing_if = "super::is_default_config"
    )]
    pub config: ConfigValue,
}

impl CodecSpec {
    /// A codec `id` for registered byte format `format`, with an empty
    /// `config`.
    pub fn new(id: impl Into<String>, format: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            format: format.into(),
            encryption: None,
            config: default_config(),
        }
    }
}

/// Declares an IO source that feeds rows into a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Name of the connector that builds this source, the `connector`
    /// property of the node.
    #[serde(rename = "connector")]
    pub connector: String,
    /// Id of the `codec` node that decodes this source's bytes. Absent
    /// for a connector that produces `RecordBatch`es directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Name of the component this source writes into. Checked against the
    /// runtime's `declared_components()` at load time.
    pub component: String,
    /// Retry policy for every source operation. Omitted block uses the
    /// defaults (4 attempts, exponential backoff).
    #[serde(default, skip_serializing_if = "super::is_default")]
    pub retry: RetryConfig,
    /// Self-healing override for this source, layered field by field over
    /// the top-level `heal` block. Absent means the top-level policy.
    ///
    /// Declaring the block on a connector that cannot be rebuilt is a
    /// load-time error rather than a block that quietly does nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heal: Option<HealConfig>,
    /// Flow-control override for this source, layered field by field over the
    /// top-level `flow_control` block. Absent means the top-level policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_control: Option<FlowControlConfig>,
    /// Optional field-level or whole-payload decryption on incoming batches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decrypt: Option<NodeCryptoConfig>,
    /// Opaque per-source configuration.
    #[serde(
        default = "default_config",
        skip_serializing_if = "super::is_default_config"
    )]
    pub config: ConfigValue,
}

impl SourceSpec {
    /// A source `id` that feeds `component` from `connector`.
    ///
    /// No codec, the default retry policy, no `heal` or `flow_control`
    /// override and an empty `config`; set the public fields for anything
    /// else.
    pub fn new(
        id: impl Into<String>,
        connector: impl Into<String>,
        component: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: None,
            connector: connector.into(),
            codec: None,
            component: component.into(),
            retry: RetryConfig::default(),
            heal: None,
            flow_control: None,
            decrypt: None,
            config: default_config(),
        }
    }
}

/// Declares an IO sink that drains rows from a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SinkSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Name of the connector that builds this sink, the `connector` property
    /// of the node.
    #[serde(rename = "connector")]
    pub connector: String,
    /// Id of the `codec` node that encodes this sink's bytes. Absent
    /// for a connector that consumes `RecordBatch`es directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Name of the component this sink reads from. Checked against the
    /// runtime's `declared_components()` at load time.
    pub component: String,
    /// Retry policy for every sink operation. Omitted block uses the
    /// defaults (4 attempts, exponential backoff).
    #[serde(default, skip_serializing_if = "super::is_default")]
    pub retry: RetryConfig,
    /// Self-healing override for this sink, layered field by field over the
    /// top-level `heal` block. Absent means the top-level policy.
    ///
    /// Declaring the block on a connector that cannot be rebuilt is a
    /// load-time error rather than a block that quietly does nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heal: Option<HealConfig>,
    /// Optional field-level or whole-payload encryption before writing to the sink.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypt: Option<NodeCryptoConfig>,
    /// Opaque per-sink configuration.
    #[serde(
        default = "default_config",
        skip_serializing_if = "super::is_default_config"
    )]
    pub config: ConfigValue,
}

impl SinkSpec {
    /// A sink `id` that drains `component` through `connector`.
    ///
    /// No codec, the default retry policy, no `heal` override and an empty
    /// `config`; set the public fields for anything else.
    pub fn new(
        id: impl Into<String>,
        connector: impl Into<String>,
        component: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: None,
            connector: connector.into(),
            codec: None,
            component: component.into(),
            retry: RetryConfig::default(),
            heal: None,
            encrypt: None,
            config: default_config(),
        }
    }
}

/// An explicit directed edge between two declared node ids.
///
/// `from` and `to` name a `source`, `wasm`, `plugin` or `sink` id, never a
/// `codec` id, which is not a graph node.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LinkSpec {
    /// The upstream node id.
    pub from: String,
    /// The downstream node id.
    pub to: String,
    /// Branch name this link carries. A processor's `run-result.routes`
    /// selects the links its output is delivered to by this name. Absent =
    /// unlabelled: never selected by a routing decision, delivered only under
    /// legacy multicast.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

impl LinkSpec {
    /// An unlabelled edge from node `from` to node `to`.
    ///
    /// Set [`branch`](Self::branch) to select the edge by a routing decision.
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            branch: None,
        }
    }
}

/// A schema declared once for the workflow: a list of `field` entries a
/// source or sink adopts when its `component` matches this schema's `id` and
/// its own `config` declares no `field` of its own.
///
/// ```kdl
/// schema "orders" {
///     field "id" type="int64" nullable=#false
///     field "total" type="decimal128" precision=12 scale=2
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaSpec {
    /// The component name this schema describes, from the node's leading
    /// argument. Unique within the workflow.
    pub id: String,
    /// The `field` entries, one entry or a list of them, in the shape a
    /// connector's `config` carries them (see
    /// [`parse_fields`](crate::connector::parse_fields)).
    pub field: ConfigValue,
}

impl SchemaSpec {
    /// A schema for component `id` with no `field` entries yet.
    ///
    /// Set [`field`](Self::field) to one entry or an array of them, in the
    /// shape [`parse_fields`](crate::connector::parse_fields) reads. A schema
    /// left empty writes no `field` node and is refused when read back.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            field: ConfigValue::Array(Vec::new()),
        }
    }
}

/// Which of the three graph roles a declared node plays.
///
/// Distinct from a node's KDL kind (`source`/`wasm`/`plugin`/`sink`): both
/// `wasm` and `plugin` nodes are [`NodeKind::Processor`], since a link treats
/// them identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Source,
    Processor,
    Sink,
}

/// One `workflow`: the whole DAG of sources, processors, sinks and
/// codecs, wired together by explicit [`LinkSpec`] declarations.
///
/// Every declared codec, source, `wasm`, `plugin` and sink id shares one
/// namespace and must be unique; see `WorkflowSpec::validate` for the full
/// set of load-time graph rules.
///
/// The `wasm` and `plugin` fields are declared unconditionally, like the
/// `mode "cluster"` half of [`ServiceMode`]: a build without those hosts
/// still parses a file that declares one, so
/// [`validate_build_capabilities`](crate::service::validation::validate_build_capabilities)
/// can name the `--features` flag that would run it instead of serde
/// reporting `wasm` as an unknown key.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSpec {
    /// Mandatory id, from the node's leading argument.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Declared byte formats, one `codec` node each.
    #[serde(
        rename = "codec",
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub codecs: Vec<CodecSpec>,
    /// Declared schemas, one `schema` node each, adopted by the sources and
    /// sinks whose `component` names one and that declare no `field` themselves.
    #[serde(
        rename = "schema",
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub schemas: Vec<SchemaSpec>,
    /// Data sources feeding the workflow. One `source` node each.
    #[serde(
        rename = "source",
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub sources: Vec<SourceSpec>,
    /// WASM processor nodes, one `wasm` node each. Needs `--features wasm-host`
    /// to build; see the type docs for why it parses without it.
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub wasm: Vec<WasmSpec>,
    /// Native plugin processor nodes, one `plugin` node each. Needs
    /// `--features plugin-host` to build.
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub plugin: Vec<PluginSpec>,
    /// Declarative relational, analytical, and stream-processing operator nodes,
    /// one `operator` node each. Runs natively on the engine with no external
    /// `.wasm` or `.so` artifact.
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub operator: Vec<OperatorSpec>,
    /// Data sinks draining the workflow. One `sink` node each.
    #[serde(
        rename = "sink",
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub sinks: Vec<SinkSpec>,
    /// Explicit edges between declared nodes. One `link` node each.
    #[serde(
        rename = "link",
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub links: Vec<LinkSpec>,
    /// This workflow's dead letter queue. Absent means a batch a sink
    /// refuses is logged and dropped, which is the behaviour with no `dlq`
    /// block at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq: Option<DlqConfig>,
}

impl WorkflowSpec {
    /// An empty workflow `id`: no name, codecs, schemas, nodes, links or
    /// `dlq`.
    ///
    /// Push the declarations onto the public fields, in the order they are
    /// written in the file.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
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
        }
    }
}

/// `^[A-Za-z0-9][A-Za-z0-9_-]*$`, at most 64 bytes.
///
/// Ids are used verbatim as OpenTelemetry attribute values and as topology
/// node ids, so the charset is closed rather than escaped.
fn is_valid_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 64 {
        return false;
    }
    let mut chars = id.chars();
    let first_is_alphanumeric = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
    first_is_alphanumeric
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl WorkflowSpec {
    /// Every declared node's id and kind, in declaration order: sources, then
    /// wasm processors, then plugin processors, then sinks.
    pub(crate) fn nodes(&self) -> Vec<(&str, NodeKind)> {
        let mut out = Vec::new();
        for s in &self.sources {
            out.push((s.id.as_str(), NodeKind::Source));
        }
        for w in &self.wasm {
            out.push((w.id.as_str(), NodeKind::Processor));
        }
        for p in &self.plugin {
            out.push((p.id.as_str(), NodeKind::Processor));
        }
        for o in &self.operator {
            out.push((o.id.as_str(), NodeKind::Processor));
        }
        for s in &self.sinks {
            out.push((s.id.as_str(), NodeKind::Sink));
        }
        out
    }

    /// Every declared id outside `nodes()` too (codecs), each paired
    /// with the KDL node kind that declared it, for id-validation messages.
    pub(super) fn declared_ids(&self) -> Vec<(&str, &'static str)> {
        let mut out = Vec::new();
        for t in &self.codecs {
            out.push((t.id.as_str(), "codec"));
        }
        for (id, kind) in self.nodes() {
            let label = match kind {
                NodeKind::Source => "source",
                NodeKind::Processor => "processor",
                NodeKind::Sink => "sink",
            };
            out.push((id, label));
        }
        out
    }

    /// Node indices in topological order, so a node always follows every node
    /// that links into it.
    ///
    /// # Errors
    ///
    /// `SaciError::Configuration` when the links contain a cycle.
    pub(crate) fn topological_order(&self) -> SaciResult<Vec<usize>> {
        let nodes = self.nodes();
        let n = nodes.len();
        let id_to_idx: HashMap<&str, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, (id, _))| (*id, i))
            .collect();

        // adjacency[i] = list of node indices that depend on i (i.e. i → dependent).
        let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut in_degree: Vec<usize> = vec![0; n];
        for link in &self.links {
            let (Some(&from), Some(&to)) = (
                id_to_idx.get(link.from.as_str()),
                id_to_idx.get(link.to.as_str()),
            ) else {
                // Unknown ids are rejected by `validate`'s rule 5; this
                // function is also called from within `validate` itself
                // (rule 8), before rule 5 in a hand-built (non-KDL) spec, so
                // skip rather than panic.
                continue;
            };
            adjacency[from].push(to);
            in_degree[to] += 1;
        }

        // Kahn's algorithm, draining the whole frontier each round so the
        // result is flattened frontier-by-frontier, keeping declaration order
        // inside each depth.
        let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut order: Vec<usize> = Vec::with_capacity(n);
        let mut visited = 0usize;

        while !queue.is_empty() {
            let frontier: Vec<usize> = queue.drain(..).collect();
            visited += frontier.len();

            let mut next_queue: VecDeque<usize> = VecDeque::new();
            for &node in &frontier {
                order.push(node);
                for &dep in &adjacency[node] {
                    in_degree[dep] -= 1;
                    if in_degree[dep] == 0 {
                        next_queue.push_back(dep);
                    }
                }
            }
            queue = next_queue;
        }

        if visited != n {
            return Err(SaciError::configuration(format!(
                "workflow '{}': links contain a cycle",
                self.id
            )));
        }

        Ok(order)
    }

    /// Validate every load-time graph rule.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] describing the first violation
    /// found, in the order documented on the type.
    pub(crate) fn validate(&self, mode: &ServiceMode) -> SaciResult<()> {
        let wf = &self.id;

        // 1. The workflow declares at least one node.
        let nodes = self.nodes();
        if nodes.is_empty() {
            return Err(SaciError::configuration(format!(
                "workflow '{wf}': declares no source, processor or sink node"
            )));
        }

        // 2. Every id matches the closed charset and length bound.
        if !is_valid_id(wf) {
            return Err(SaciError::configuration(format!(
                "workflow: id '{wf}' is invalid; ids must match \
                 ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes"
            )));
        }
        let declared = self.declared_ids();
        for (id, kind) in &declared {
            if !is_valid_id(id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': {kind} id '{id}' is invalid; ids must match \
                     ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes"
                )));
            }
        }

        // 3. No id is declared twice anywhere in the workflow.
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for (id, kind) in &declared {
            if let Some(&prev_kind) = seen.get(id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': id '{id}' is declared twice, as {prev_kind} and as {kind}"
                )));
            }
            seen.insert(id, kind);
        }

        // 4. Every source `codec` / sink `codec` names a declared codec id.
        let codec_ids: HashSet<&str> = self.codecs.iter().map(|t| t.id.as_str()).collect();
        for s in &self.sources {
            if let Some(t) = &s.codec
                && !codec_ids.contains(t.as_str())
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': source '{}' names codec '{t}', \
                     which is not declared",
                    s.id
                )));
            }
        }
        for s in &self.sinks {
            if let Some(t) = &s.codec
                && !codec_ids.contains(t.as_str())
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': sink '{}' names codec '{t}', which is not declared",
                    s.id
                )));
            }
        }

        // 5. Every link.from / link.to names a declared source, wasm, plugin
        //    or sink id, and from != to. A codec id is not a node, so
        //    it is rejected here too.
        let node_kind: HashMap<&str, NodeKind> =
            nodes.iter().map(|&(id, kind)| (id, kind)).collect();
        for link in &self.links {
            if link.from == link.to {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' links a node to itself",
                    link.from, link.to
                )));
            }
            if !node_kind.contains_key(link.from.as_str()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link names undeclared node '{}'",
                    link.from
                )));
            }
            if !node_kind.contains_key(link.to.as_str()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link names undeclared node '{}'",
                    link.to
                )));
            }
        }

        // 6. No (from, to) pair is declared twice.
        let mut seen_edges: HashSet<(&str, &str)> = HashSet::new();
        for link in &self.links {
            if !seen_edges.insert((link.from.as_str(), link.to.as_str())) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' is declared twice",
                    link.from, link.to
                )));
            }
        }

        // 7. The edge-kind matrix: a source has no input, a sink has no
        //    output.
        for link in &self.links {
            if node_kind[link.to.as_str()] == NodeKind::Source {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' targets source '{}'; \
                     a source has no input",
                    link.from, link.to, link.to
                )));
            }
            if node_kind[link.from.as_str()] == NodeKind::Sink {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' starts at sink '{}'; \
                     a sink has no output",
                    link.from, link.to, link.from
                )));
            }
        }

        // 8. The graph is acyclic.
        self.topological_order()?;

        // 9. Every source has at least one outbound link, and every sink at
        //    least one inbound link. A processor needs neither.
        for s in &self.sources {
            if !self.links.iter().any(|l| l.from == s.id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': source '{}' has no outbound link",
                    s.id
                )));
            }
        }
        for s in &self.sinks {
            if !self.links.iter().any(|l| l.to == s.id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': sink '{}' has no inbound link",
                    s.id
                )));
            }
        }

        // 10. Cluster mode: exactly one processor node, zero sources, zero
        //     sinks and zero links.
        if matches!(mode, ServiceMode::Cluster { .. }) {
            let processor_count = nodes
                .iter()
                .filter(|&&(_, kind)| kind == NodeKind::Processor)
                .count();
            if !self.operator.is_empty()
                || processor_count != 1
                || !self.sources.is_empty()
                || !self.sinks.is_empty()
                || !self.links.is_empty()
            {
                return Err(SaciError::configuration(format!(
                    "cluster mode runs exactly one 'wasm' or 'plugin' node with no source, \
                     sink or link ({} node(s), {} link(s) declared)",
                    nodes.len(),
                    self.links.len()
                )));
            }
            // The service-level window block tracks a watermark from the
            // node's inbound links, and cluster mode has none: the distributed
            // runner drives the one processor straight from partition claims.
            // Reject the block rather than silently not tracking anything.
            if let Some(w) = self.wasm.iter().find(|w| w.window.is_some()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': cluster mode cannot honour wasm node '{}' window \
                     block; cluster-mode windowing lives in the processor's own pipeline \
                     (WindowedSystem + WindowAccumulator)",
                    w.id
                )));
            }
            if let Some(p) = self.plugin.iter().find(|p| p.window.is_some()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': cluster mode cannot honour plugin node '{}' window \
                     block; cluster-mode windowing lives in the processor's own pipeline \
                     (WindowedSystem + WindowAccumulator)",
                    p.id
                )));
            }
        }

        // 11. `run "stream"`: at least one source in the whole
        //     workflow.
        let stream_mode = matches!(
            mode,
            ServiceMode::Standalone { config } if config.run == RunMode::Stream
        );
        if stream_mode && self.sources.is_empty() {
            return Err(SaciError::configuration(format!(
                "stream run mode requires at least one 'source' node ({}) declared",
                self.sources.len()
            )));
        }

        // 13. Outside stream mode, no source is live. Asked of the connectors
        //     this crate ships: a config is validated before any builder
        //     registers a connector of its own, and the builder asks its own
        //     registry again when it builds the workflow.
        if !stream_mode
            && let Some(live) = self.sources.iter().find(|s| {
                crate::service::factories::builtin_registry()
                    .connector(&s.connector)
                    .is_some_and(|connector| connector.live(&s.config))
            })
        {
            return Err(SaciError::configuration(format!(
                "source connector '{}' never reaches EOF; it requires standalone mode \
                 with run \"stream\"",
                live.connector
            )));
        }

        // Every declared source and sink carries a valid retry block.
        for spec in &self.sources {
            spec.retry
                .validate(&format!("workflow '{wf}' source '{}'", spec.id))?;
        }
        for spec in &self.sinks {
            spec.retry
                .validate(&format!("workflow '{wf}' sink '{}'", spec.id))?;
        }

        // 14. Every present link branch is a valid id.
        for link in &self.links {
            if let Some(branch) = &link.branch
                && !is_valid_id(branch)
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' branch '{branch}' is invalid; \
                     branches must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes",
                    link.from, link.to
                )));
            }
        }

        // 15. A labelled link must start at a processor.
        for link in &self.links {
            if let Some(branch) = &link.branch
                && node_kind[link.from.as_str()] != NodeKind::Processor
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' carries branch '{branch}' but '{}' is \
                     a source; only a processor can route",
                    link.from, link.to, link.from
                )));
            }
        }

        // 16. Per node, either every outbound link carries a branch or none
        //     do.
        for &(id, _) in &nodes {
            let mut labelled = false;
            let mut unlabelled = false;
            for link in &self.links {
                if link.from != id {
                    continue;
                }
                if link.branch.is_some() {
                    labelled = true;
                } else {
                    unlabelled = true;
                }
            }
            if labelled && unlabelled {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': node '{id}' mixes labelled and unlabelled outbound \
                     links; label every link or none"
                )));
            }
        }

        // 17. A declared `window` block must be geometrically sane. Checked
        //     whether or not this build carries the node's host or the
        //     windowing engine: a bad geometry is a defect in the file either
        //     way, and the capability refusal follows in
        //     `validate_build_capabilities`.
        for w in &self.wasm {
            validate_window_block(wf, "wasm", &w.id, w.window.as_ref())?;
        }
        for p in &self.plugin {
            validate_window_block(wf, "plugin", &p.id, p.window.as_ref())?;
        }
        for o in &self.operator {
            o.validate(wf)?;
            validate_window_block(wf, "operator", &o.id, o.window.as_ref())?;
        }

        // 18. A declared `dlq` block must name a store this crate knows. The
        //     store's own keys are the connector's to reject, by name, when
        //     the workflow is built.
        if let Some(dlq) = &self.dlq
            && crate::service::dlq::store_kind(&dlq.0.store).is_none()
        {
            return Err(SaciError::configuration(format!(
                "workflow '{wf}': dlq store '{}' is not one of {}",
                dlq.0.store,
                crate::service::dlq::store_names()
            )));
        }

        // 19. Schema ids are unique within the workflow, and each declares at
        //     least one `field` that parses.
        let mut schema_ids: HashSet<&str> = HashSet::new();
        for schema in &self.schemas {
            if !schema_ids.insert(schema.id.as_str()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': schema '{}' is declared twice",
                    schema.id
                )));
            }
            let mut table = ConfigMap::new();
            table.insert("field".to_string(), schema.field.clone());
            let table = ConfigValue::Object(table);
            let fields = parse_fields(&table, &format!("workflow '{wf}': schema '{}'", schema.id))?;
            if fields.is_empty() {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': schema '{}' declares no field",
                    schema.id
                )));
            }
            schema_of(&fields).map_err(|e| {
                SaciError::configuration(format!(
                    "workflow '{wf}': schema '{}': {}",
                    schema.id,
                    e.message()
                ))
            })?;
        }

        // 20. Validate codec/source/sink encryption blocks.
        for codec in &self.codecs {
            if let Some(enc) = &codec.encryption {
                enc.validate(&format!("workflow '{wf}' codec '{}'", codec.id))?;
            }
        }
        for source in &self.sources {
            if let Some(dec) = &source.decrypt {
                dec.validate(
                    &format!("workflow '{wf}' source '{}'.decrypt", source.id),
                    false,
                )?;
            }
        }
        for sink in &self.sinks {
            if let Some(enc) = &sink.encrypt {
                enc.validate(&format!("workflow '{wf}' sink '{}'.encrypt", sink.id), true)?;
            }
        }

        Ok(())
    }
}

/// Run a declared [`WindowConfig`]'s sanity checks, naming the workflow, node
/// kind and id in the error.
///
/// Runs in every build. The geometry is config shape, which no feature
/// changes, so `size_ms=0` is refused by name wherever it is read; whether
/// this binary can *serve* the block is
/// [`validate_build_capabilities`](crate::service::validation::validate_build_capabilities)'s
/// question.
fn validate_window_block(
    workflow_id: &str,
    kind: &str,
    id: &str,
    window: Option<&WindowConfig>,
) -> SaciResult<()> {
    let Some(window) = window else {
        return Ok(());
    };
    window.validate().map_err(|e| {
        SaciError::configuration(format!(
            "workflow '{workflow_id}': {kind} node '{id}' window is invalid: {}",
            e.message()
        ))
    })
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    #[cfg(all(feature = "connector-file", feature = "codec-ndjson"))]
    use crate::service::config::ServiceConfig;
    use crate::service::config::test_support::{parse, workflow_err, workflow_kdl};

    #[test]
    fn test_codec_node_and_codec_key_use_the_codec_vocabulary() {
        let raw = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
workflow "w" {
    codec "json" format="ndjson" {
        config infer_max=10
    }
    source "in" connector="noop" component="X" codec="json"
    sink "out" connector="noop" component="X"
    link from="in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        let workflow = &cfg.workflows[0];
        assert_eq!(workflow.codecs.len(), 1);
        assert_eq!(workflow.codecs[0].id, "json");
        assert_eq!(workflow.codecs[0].format, "ndjson");
        assert_eq!(workflow.codecs[0].config["infer_max"], 10);
        assert_eq!(workflow.sources[0].codec.as_deref(), Some("json"));
        assert_eq!(workflow.sinks[0].codec, None);

        for (old, key) in [
            (r#"transformer "json" format="ndjson""#, "transformer"),
            (
                r#"codec "json" format="ndjson" { options infer_max=10 }"#,
                "options",
            ),
        ] {
            let doc = format!(
                "mode \"standalone\"\nnode id=1 data_dir=\"/tmp/saci\"\nworkflow \"w\" {{\n{old}\n}}\n"
            );
            let err = parse(&doc).expect_err(old);
            assert!(err.contains(key), "{old}: {err}");
        }
    }

    #[test]
    fn test_cluster_mode_with_a_source_rejected_at_validate() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

cluster {
    peer id=1 addr="127.0.0.1:9000"
    peer id=2 addr="127.0.0.2:9000"
}

workflow "w" {
    source "kafka_in" connector="mongo" component="orders"
    sink "out" connector="noop" component="orders"
    link from="kafka_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert_eq!(
            err.category(),
            "configuration",
            "expected configuration error: {err}"
        );
        assert!(
            err.to_string()
                .contains("cluster mode runs exactly one 'wasm' or 'plugin' node"),
            "error should name the cluster rule: {err}"
        );
    }

    fn stream_kdl(sources: &str, sinks: &str, links: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

run "stream"

workflow "w" {{
{sources}
{sinks}
{links}
}}
"#
        )
    }

    #[test]
    fn test_stream_mode_with_one_source_validates() {
        let raw = stream_kdl(
            r#"source "ticks" connector="tcp" component="Tick""#,
            r#"sink "out" connector="noop" component="Tick""#,
            r#"link from="ticks" to="out""#,
        );
        let cfg = parse(&raw).expect("parse");
        match &cfg.mode {
            ServiceMode::Standalone { config } => assert_eq!(config.run, RunMode::Stream),
            _ => panic!("expected standalone"),
        }
        cfg.validate().expect("one source + stream mode is valid");
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_stream_mode_requires_at_least_one_source() {
        // A processor-only workflow passes every earlier rule, so rule 11 is
        // what fires: stream mode needs at least one source to pull from.
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

run "stream"

workflow "w" {
    wasm "p" path="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string()
                .contains("stream run mode requires at least one 'source' node"),
            "got: {err}"
        );
    }

    #[test]
    fn test_stream_mode_with_two_sources_is_valid() {
        // Two live sources feeding the same sink: the stream runner pulls
        // them round-robin, one batch per item.
        let raw = stream_kdl(
            r#"
source "a" connector="tcp" component="Tick"
source "b" connector="tcp" component="Tick"
"#,
            r#"sink "out" connector="noop" component="Tick""#,
            r#"
link from="a" to="out"
link from="b" to="out"
"#,
        );
        let cfg = parse(&raw).expect("parse");
        cfg.validate().expect("two sources + stream mode is valid");
    }

    #[test]
    #[cfg(all(
        feature = "connector-tcp",
        feature = "connector-udp",
        feature = "connector-peer"
    ))]
    fn test_live_sources_rejected_outside_stream_mode() {
        for type_name in ["tcp", "udp", "peer"] {
            let raw = format!(
                r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {{
    source "ticks" connector="{type_name}" component="Tick"
    sink "out" connector="noop" component="Tick"
    link from="ticks" to="out"
}}
"#
            );
            let cfg = parse(&raw).expect("parse");
            let err = cfg.validate().unwrap_err();
            assert_eq!(err.category(), "configuration", "got: {err}");
            assert!(err.to_string().contains("never reaches EOF"), "got: {err}");
        }
    }

    #[test]
    #[cfg(feature = "connector-kafka")]
    fn test_kafka_source_rejected_outside_stream_mode_unless_stop_at_end() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "orders_in" connector="kafka" component="Order"
    sink "out" connector="noop" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("never reaches EOF"), "got: {err}");

        let raw_stop_at_end = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "orders_in" connector="kafka" component="Order" {
        config stop_at_end=#true
    }
    sink "out" connector="noop" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw_stop_at_end).expect("parse");
        cfg.validate()
            .expect("stop_at_end=#true makes a KafkaSource usable outside stream mode");
    }

    #[test]
    #[cfg(feature = "connector-kafka")]
    fn test_kafka_source_compacted_is_accepted_outside_stream_mode_without_stop_at_end() {
        // A compacted KafkaSource is one-shot regardless of stop_at_end: it
        // always reaches EOF once its snapshot is read, so it needs no
        // stream mode either.
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "orders_in" connector="kafka" component="Order" {
        config {
            compacted #true
            key_field "id"
        }
    }
    sink "out" connector="noop" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        cfg.validate()
            .expect("compacted=#true makes a KafkaSource usable outside stream mode");
    }

    #[test]
    #[cfg(feature = "connector-http")]
    fn test_http_source_streaming_mode_rejected_outside_stream_mode() {
        // `mode kind="events"` reads a `text/event-stream` body that has no
        // end, so the source never reaches EOF and only the stream runner may
        // drive it.
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

run "once"

workflow "w" {
    codec "json" format="ndjson"
    source "feed" connector="http" component="Order" codec="json" {
        config {
            url "http://127.0.0.1:9/events"
            mode kind="events"
            field "id" type="int64" nullable=#false
        }
    }
    sink "out" connector="noop" component="Order"
    link from="feed" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("never reaches EOF"), "got: {err}");
    }

    #[test]
    #[cfg(feature = "connector-http")]
    fn test_http_source_document_mode_is_accepted_outside_stream_mode() {
        // The default read mode fetches one document and reaches EOF, so it
        // runs under any run mode, whether `mode kind="document"` is spelled
        // out or left absent.
        for mode in ["mode kind=\"document\"", ""] {
            let raw = format!(
                r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

run "once"

workflow "w" {{
    codec "json" format="ndjson"
    source "feed" connector="http" component="Order" codec="json" {{
        config {{
            url "http://127.0.0.1:9/orders"
            {mode}
        }}
    }}
    sink "out" connector="noop" component="Order"
    link from="feed" to="out"
}}
"#
            );
            let cfg = parse(&raw).expect("parse");
            cfg.validate()
                .expect("a document-mode HttpSource reaches EOF, so no stream mode is needed");
        }
    }

    #[test]
    #[cfg(feature = "connector-http")]
    fn test_http_push_source_rejected_outside_stream_mode() {
        // The push source listens for requests forever, like `tcp` and `udp`.
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

run "once"

workflow "w" {
    codec "json" format="ndjson"
    source "hook" connector="http-listen" component="Order" codec="json" {
        config {
            bind "127.0.0.1:0"
            field "id" type="int64" nullable=#false
        }
    }
    sink "out" connector="noop" component="Order"
    link from="hook" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("never reaches EOF"), "got: {err}");
    }

    #[test]
    fn test_workflow_systems_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    systems "proc" type="Proc"
}
"#;
        let err = parse(raw).expect_err("a systems node must be rejected, not silently dropped");
        assert!(
            err.contains("systems"),
            "error should name the offending key: {err}"
        );
    }

    #[test]
    fn test_workflow_components_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    components "orders" type="GenericComponent"
}
"#;
        let err = parse(raw).expect_err("a components node must be rejected, not silently dropped");
        assert!(err.contains("components"), "{err}");
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_two_wasm_nodes_are_independent_processors_linked_explicitly() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "validate" path="validate.wasm" {
        config min_amount="0.50"
    }
    wasm "settle" path="settle.wasm" {
        config fee_bps="290"
    }
    link from="validate" to="settle"
}
"#;
        let cfg = parse(raw).expect("parse");
        assert_eq!(cfg.workflows[0].wasm.len(), 2);
        assert_eq!(cfg.workflows[0].wasm[0].id, "validate");
        assert_eq!(
            cfg.workflows[0].wasm[0].path.as_deref(),
            Some("validate.wasm")
        );
        assert_eq!(
            cfg.workflows[0].wasm[0]
                .config
                .get("min_amount")
                .map(String::as_str),
            Some("0.50")
        );
        assert_eq!(cfg.workflows[0].wasm[1].id, "settle");
        assert_eq!(
            cfg.workflows[0].wasm[1]
                .config
                .get("fee_bps")
                .map(String::as_str),
            Some("290")
        );
        assert_eq!(
            cfg.workflows[0].links,
            vec![LinkSpec {
                from: "validate".to_string(),
                to: "settle".to_string(),
                branch: None,
            }]
        );
        cfg.validate().expect("two linked processors are valid");
    }

    /// A misspelled node kind is still a parse error naming the key.
    ///
    /// This is the half of [`WorkflowSpec`]'s `deny_unknown_fields` that the
    /// unconditional `wasm`/`plugin` fields put at risk. Declaring a host this
    /// build does not carry is deliberately not an unknown key, so
    /// that `validate_build_capabilities` can name the feature
    /// (`test_plugin_node_without_the_host_parses_and_names_the_feature`); a
    /// genuine typo must still be refused by name, which is what
    /// `docs/content/service/operate/troubleshooting.md`'s "A key it does not
    /// know" promises. Ungated: both properties hold in every build.
    ///
    /// Asserted on the offending key alone. Serde's `expected one of ...`
    /// tail is derive-generated, so its ordering and punctuation are
    /// implementation details no config file depends on.
    #[test]
    fn test_unknown_workflow_key_is_rejected_and_names_it() {
        let raw = workflow_kdl(r#"    wasmm "transform" path="transform.wasm""#);
        let err = parse(&raw).expect_err("a misspelled node kind must not parse");
        assert!(
            err.contains("wasmm"),
            "the error must name the offending key: {err}"
        );
    }

    /// The same inside a `source`, which has its own, different valid set.
    ///
    /// A workflow-level check cannot stand in for this one: the two structs
    /// carry `deny_unknown_fields` independently, and a node body is where a
    /// key is most easily mistaken for one a connector would read. Only a
    /// key inside the nested `config` block belongs to the connector.
    #[test]
    fn test_unknown_source_key_is_rejected_and_names_it() {
        let raw =
            workflow_kdl(r#"    source "in" connector="file" component="orders" truncat=#true"#);
        let err = parse(&raw).expect_err("a key no source can honour must not parse");
        assert!(
            err.contains("truncat"),
            "the error must name the offending key: {err}"
        );
    }

    #[test]
    fn test_standalone_mode_with_a_linked_source_and_sink_is_valid() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "kafka_in" connector="mongo" component="orders"
    sink "out" connector="noop" component="orders"
    link from="kafka_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        cfg.validate()
            .expect("standalone mode with a linked source and sink should be valid");
    }

    // ── WorkflowSpec::validate: graph rules ─────────────────────────────────

    #[test]
    fn rule_id_charset_is_enforced() {
        let err = workflow_err(
            r#"
source "bad/id" connector="noop" component="X"
sink "out" connector="noop" component="X"
link from="bad/id" to="out"
"#,
        );
        assert!(err.contains("is invalid"), "got: {err}");
        assert!(err.contains("bad/id"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_id_across_kinds_is_rejected() {
        let err = workflow_err(
            r#"
source "shared" connector="noop" component="X"
sink "shared" connector="noop" component="X"
link from="shared" to="shared"
"#,
        );
        assert!(err.contains("declared twice"), "got: {err}");
    }

    #[test]
    fn rule_source_codec_must_be_declared() {
        let err = workflow_err(
            r#"
source "in" connector="noop" codec="missing" component="X"
sink "out" connector="noop" component="X"
link from="in" to="out"
"#,
        );
        assert!(err.contains("names codec 'missing'"), "got: {err}");
    }

    #[test]
    fn rule_link_to_undeclared_node_is_rejected() {
        let err = workflow_err(
            r#"
source "in" connector="noop" component="X"
sink "out" connector="noop" component="X"
link from="in" to="ghost"
"#,
        );
        assert!(err.contains("undeclared node 'ghost'"), "got: {err}");
    }

    #[test]
    fn rule_self_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" connector="noop" component="X"
link from="in" to="in"
"#,
        );
        assert!(err.contains("links a node to itself"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" connector="noop" component="X"
sink "out" connector="noop" component="X"
link from="in" to="out"
link from="in" to="out"
"#,
        );
        assert!(err.contains("is declared twice"), "got: {err}");
    }

    #[test]
    fn rule_link_into_a_source_is_rejected() {
        let err = workflow_err(
            r#"
source "a" connector="noop" component="X"
source "b" connector="noop" component="X"
link from="a" to="b"
"#,
        );
        assert!(err.contains("a source has no input"), "got: {err}");
    }

    #[test]
    fn rule_link_out_of_a_sink_is_rejected() {
        let err = workflow_err(
            r#"
source "in" connector="noop" component="X"
sink "a" connector="noop" component="X"
sink "b" connector="noop" component="X"
link from="in" to="a"
link from="a" to="b"
"#,
        );
        assert!(err.contains("a sink has no output"), "got: {err}");
    }

    #[test]
    fn rule_two_link_cycle_is_rejected() {
        #[cfg(feature = "wasm-host")]
        {
            let err = workflow_err(
                r#"
wasm "p1" path="p1.wasm"
wasm "p2" path="p2.wasm"
link from="p1" to="p2"
link from="p2" to="p1"
"#,
            );
            assert!(err.contains("links contain a cycle"), "got: {err}");
        }
    }

    #[test]
    fn rule_source_with_no_outbound_link_is_rejected() {
        let err = workflow_err(r#"source "in" connector="noop" component="X""#);
        assert!(
            err.contains("source 'in' has no outbound link"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_sink_with_no_inbound_link_is_rejected() {
        let err = workflow_err(r#"sink "out" connector="noop" component="X""#);
        assert!(err.contains("sink 'out' has no inbound link"), "got: {err}");
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn rule_branch_charset_is_enforced() {
        let err = workflow_err(
            r#"
wasm "p" path="p.wasm"
sink "a" connector="noop" component="X"
sink "b" connector="noop" component="X"
link from="p" to="a" branch="bad/branch"
link from="p" to="b" branch="high"
"#,
        );
        assert!(err.contains("is invalid"), "got: {err}");
        assert!(err.contains("bad/branch"), "got: {err}");
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn rule_branch_on_a_source_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" connector="noop" component="X"
sink "out" connector="noop" component="X"
link from="in" to="out" branch="high"
"#,
        );
        assert!(err.contains("only a processor can route"), "got: {err}");
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn rule_node_mixes_labelled_and_unlabelled_links_is_rejected() {
        let err = workflow_err(
            r#"
wasm "p" path="p.wasm"
sink "a" connector="noop" component="X"
sink "b" connector="noop" component="X"
link from="p" to="a" branch="high"
link from="p" to="b"
"#,
        );
        assert!(err.contains("mixes labelled and unlabelled"), "got: {err}");
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn rule_two_labelled_links_validate() {
        let cfg = parse(&workflow_kdl(
            r#"
wasm "p" path="p.wasm"
sink "a" connector="noop" component="X"
sink "b" connector="noop" component="X"
link from="p" to="a" branch="high"
link from="p" to="b" branch="low"
"#,
        ))
        .expect("parse should succeed");
        cfg.validate().expect("two labelled links are valid");
    }

    // ── Workflow schemas: `schema "id" { field .. }` ─────────────────────────

    /// Two nodes wired source to sink on the `orders` component, for the
    /// tests that only care about the `schema` nodes beside them.
    const ORDERS_LINK: &str = r#"
source "in" connector="noop" component="orders"
sink "out" connector="noop" component="orders"
link from="in" to="out"
"#;

    #[test]
    fn test_schema_nodes_parse_one_field_and_many() {
        let cfg = parse(&workflow_kdl(&format!(
            r#"
schema "orders" {{
    field "id" type="int64"
}}
schema "totals" {{
    field "id" type="int64"
    field "total" type="decimal128" precision=12 scale=2
}}
{ORDERS_LINK}"#
        )))
        .expect("schema nodes parse");
        cfg.validate().expect("two distinct schemas are valid");

        let ids: Vec<&str> = cfg.workflows[0]
            .schemas
            .iter()
            .map(|schema| schema.id.as_str())
            .collect();
        assert_eq!(ids, ["orders", "totals"]);
    }

    #[test]
    fn rule_a_schema_id_declared_twice_is_rejected() {
        let err = workflow_err(&format!(
            r#"
schema "orders" {{
    field "id" type="int64"
}}
schema "orders" {{
    field "id" type="utf8"
}}
{ORDERS_LINK}"#
        ));
        assert!(
            err.contains("schema 'orders' is declared twice"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_a_schema_field_with_a_mixed_case_type_is_rejected_naming_it() {
        let err = workflow_err(&format!(
            r#"
schema "orders" {{
    field "x" type="Int64"
}}
{ORDERS_LINK}"#
        ));
        assert!(err.contains("Int64"), "got: {err}");
        assert!(err.contains("schema 'orders'"), "got: {err}");
    }

    #[test]
    fn rule_a_schema_field_that_is_no_arrow_type_is_rejected() {
        let err = workflow_err(&format!(
            r#"
schema "orders" {{
    field "total" type="decimal128"
}}
{ORDERS_LINK}"#
        ));
        assert!(err.contains("schema 'orders'"), "got: {err}");
        assert!(err.contains("'precision'"), "got: {err}");
    }

    /// A `file` to `file` workflow on the `orders` component: its
    /// `schema "orders"` holds `schema_lines`, and the `config` of the source
    /// and of the sink hold `source_lines` and `sink_lines`.
    #[cfg(all(feature = "connector-file", feature = "codec-ndjson"))]
    fn file_workflow_kdl(
        dir: &std::path::Path,
        schema_lines: &str,
        source_lines: &str,
        sink_lines: &str,
    ) -> String {
        let path = |name: &str| dir.join(name).to_string_lossy().replace('\\', "/");
        format!(
            r#"
mode "standalone"

node id=1 name="svc" data_dir="{data}"

run "once"

workflow "w" {{
    codec "rows" format="ndjson"
    schema "orders" {{
        {schema_lines}
    }}
    source "in" connector="file" codec="rows" component="orders" {{
        config path="{input}" {{
            {source_lines}
        }}
    }}
    sink "out" connector="file" codec="rows" component="orders" {{
        config path="{output}" {{
            {sink_lines}
        }}
    }}
    link from="in" to="out"
}}
"#,
            data = path("data"),
            input = path("in.ndjson"),
            output = path("out.ndjson"),
        )
    }

    /// A source and a sink that declare no `field` of their own read the
    /// workflow's `schema` for their component.
    #[cfg(all(feature = "connector-file", feature = "codec-ndjson"))]
    #[test]
    fn test_a_workflow_schema_is_the_schema_of_a_source_and_sink_that_declare_none() {
        use crate::io::{Sink, Source};
        use crate::service::{BuiltNodeKind, ServiceBuilder};

        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("in.ndjson"), "{\"id\":1}\n").expect("fixture");
        let kdl = file_workflow_kdl(dir.path(), r#"field "id" type="int64""#, "", "");
        let config = ServiceConfig::from_kdl(&kdl).expect("config loads");

        let workflow = ServiceBuilder::new()
            .build(&config)
            .expect("workflow builds")
            .remove(0);

        let BuiltNodeKind::Source(source) = &workflow.nodes[0].kind else {
            panic!("the source is declared first");
        };
        let BuiltNodeKind::Sink(sink) = &workflow.nodes[1].kind else {
            panic!("the sink follows it");
        };
        for schema in [source.schema(), sink.schema()] {
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
        }
    }

    /// A node that declares its own `field` lines keeps them: the workflow
    /// schema says `utf8` for `id`, the nodes say a non-null `int64` and a
    /// second column, and the built schemas are the nodes' own.
    #[cfg(all(feature = "connector-file", feature = "codec-ndjson"))]
    #[test]
    fn test_a_node_that_declares_its_own_fields_keeps_them() {
        use crate::io::{Sink, Source};
        use crate::service::{BuiltNodeKind, ServiceBuilder};

        let own = "field \"id\" type=\"int64\" nullable=#false\nfield \"note\" type=\"utf8\"";
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("in.ndjson"), "{\"id\":1,\"note\":\"a\"}\n")
            .expect("fixture");
        let kdl = file_workflow_kdl(dir.path(), r#"field "id" type="utf8""#, own, own);
        let config = ServiceConfig::from_kdl(&kdl).expect("config loads");

        let workflow = ServiceBuilder::new()
            .build(&config)
            .expect("workflow builds")
            .remove(0);

        let BuiltNodeKind::Source(source) = &workflow.nodes[0].kind else {
            panic!("the source is declared first");
        };
        let BuiltNodeKind::Sink(sink) = &workflow.nodes[1].kind else {
            panic!("the sink follows it");
        };
        for schema in [source.schema(), sink.schema()] {
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
            assert!(!schema.field(0).is_nullable());
            assert_eq!(schema.field(1).name(), "note");
        }
    }

    /// The old rule 10 rejected a processor fed by both a source and another
    /// processor. Fan-in merging is exactly what a windowed processor is for:
    /// one node receives rows from several streams and merges them, so the
    /// rule is gone and this shape must validate.
    #[cfg(feature = "wasm-host")]
    #[test]
    fn mixed_source_and_processor_fan_in_is_valid() {
        let cfg = parse(&workflow_kdl(
            r#"
source "s" connector="noop" component="X"
wasm "up" path="up.wasm"
wasm "down" path="down.wasm"
link from="s" to="down"
link from="up" to="down"
"#,
        ))
        .expect("parse should succeed");
        cfg.validate()
            .expect("a processor may be fed by sources and processors at once");
    }
}
