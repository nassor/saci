//! [`build_topology`]: what the dashboard draws, derived from config plus
//! every built node's self-description.
//!
//! ## One node per declared workflow node
//!
//! One [`TopoNode`] per [`BuiltNode`], in the topological order
//! `ServiceBuilder::build` produced, and one [`TopoEdge`] per entry of that
//! node's `downstream` list, which mirrors the config's `link` declarations. Cross-workflow
//! channel bridges are reported separately as [`BridgeEdge`]s, because their
//! two endpoints live in different workflows and belong to no one workflow's
//! edge list.
//!
//! There is one [`WorkflowTopology`] per declared workflow, carrying that
//! workflow's own `id`/`name`.
//!
//! A processor's `detail` carries `version`/`stateful` from its
//! `descriptor_info()` plus the artifact file name; nothing host-side can see
//! *inside* a processor, because the WIT `pipeline-descriptor` carries a
//! component list, not a system list, and a guest's `pipeline.stage` /
//! `system.execute` spans never cross the component boundary. Per-system
//! latency for a *native* runtime does exist, in
//! [`Snapshot::span_stats`](crate::inspector_wire::Snapshot::span_stats), which is
//! fed by the retained spans rather than the topology.
//!
//! ## Redaction is mandatory
//!
//! [`SourceSpec::config`](crate::service::config::SourceSpec::config) and
//! [`SinkSpec::config`](crate::service::config::SinkSpec::config) are opaque
//! config tables holding `connection.dsn`, `connection.password` and
//! credential file paths. [`TopoNode::detail`] therefore copies values through
//! the dotted keys each connector lists in
//! [`Connector::detail_keys`](crate::connector::Connector::detail_keys), per
//! role. A key outside the list is dropped, never masked, so nothing in the
//! response hints at a secret's shape or length, and a connector the
//! registry does not hold shows no detail at all: an unknown connector is
//! precisely the case where guessing which keys are safe is wrong.

use std::collections::HashMap;

use crate::config::ConfigValue;
use crate::connector::Role;
#[cfg(feature = "windows")]
use crate::inspector_wire::WindowInfo;
use crate::inspector_wire::{
    BridgeEdge, RuntimeInfo, TopoEdge, TopoNode, Topology, WorkflowTopology,
};
#[cfg(feature = "windows")]
use crate::window::WindowSpec;

#[cfg(all(test, feature = "connector-channel"))]
use super::builder::BuiltEdge;
use super::builder::{BuiltNode, BuiltNodeKind};
use super::config::{NodeKind, ServiceConfig, ServiceMode, WorkflowSpec};
use super::registry::Registry;

/// Read a dotted key out of a config table, rendering only scalars.
///
/// A table or array value is skipped rather than debug-printed: a nested table
/// under an allowlisted key could hold anything, including the credentials the
/// allowlist exists to keep out.
fn scalar_at(config: &ConfigValue, dotted: &str) -> Option<String> {
    let mut current = config;
    for segment in dotted.split('.') {
        current = current.get(segment)?;
    }
    match current {
        ConfigValue::String(s) => Some(s.clone()),
        ConfigValue::Number(n) => Some(n.to_string()),
        ConfigValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Every allowlisted `(key, value)` pair for one connector instance: the keys
/// the registered connector `connector` lists for `role`. A connector the
/// registry does not hold gets no detail at all.
fn detail_for(
    registry: &Registry,
    connector: &str,
    role: Role,
    config: &ConfigValue,
) -> Vec<(String, String)> {
    let Some(connector) = registry.connector(connector) else {
        return Vec::new();
    };
    connector
        .detail_keys(role)
        .iter()
        .filter_map(|key| scalar_at(config, key).map(|value| ((*key).to_string(), value)))
        .collect()
}

/// One [`TopoNode`] plus its `runtime` self-description, if any.
fn node_view(config: &ServiceConfig, registry: &Registry, node: &BuiltNode) -> TopoNode {
    #[cfg_attr(not(feature = "windows"), allow(unused_variables))]
    let (kind, detail, runtime, window) = match &node.kind {
        BuiltNodeKind::Source(_) => {
            let detail = config
                .workflows
                .iter()
                .flat_map(|w| w.sources.iter())
                .find(|s| s.id == node.id)
                .map_or_else(Vec::new, |spec| {
                    detail_for(registry, &node.connector, Role::Source, &spec.config)
                });
            ("source", detail, None, None)
        }
        BuiltNodeKind::Sink(_) => {
            let detail = config
                .workflows
                .iter()
                .flat_map(|w| w.sinks.iter())
                .find(|s| s.id == node.id)
                .map_or_else(Vec::new, |spec| {
                    detail_for(registry, &node.connector, Role::Sink, &spec.config)
                });
            ("sink", detail, None, None)
        }
        BuiltNodeKind::Processor { runtime, kind } => {
            let info = runtime.descriptor_info();
            #[cfg(feature = "windows")]
            let window = node.window.as_ref().map(window_info);
            #[cfg(not(feature = "windows"))]
            let window: Option<crate::inspector_wire::WindowInfo> = None;

            let mut detail = Vec::new();
            if !info.version.is_empty() {
                detail.push(("version".to_string(), info.version.clone()));
            }
            detail.push(("stateful".to_string(), info.stateful.to_string()));
            if let Some(path) = &node.artifact {
                let file_name = std::path::Path::new(path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(path);
                detail.push(("artifact".to_string(), file_name.to_string()));
            }
            #[cfg(feature = "windows")]
            if let Some(window) = node.window.as_ref() {
                detail.push(("window".to_string(), format_window_spec(&window.spec)));
                detail.push(("window.time_field".to_string(), window.time_field.clone()));
                if !window.key_fields.is_empty() {
                    detail.push((
                        "window.key_fields".to_string(),
                        window.key_fields.join(", "),
                    ));
                }
                detail.push((
                    "window.allowed_lateness_ms".to_string(),
                    window.allowed_lateness_ms.to_string(),
                ));
            }

            // The processor's or plugin's own name identifies which artifact is
            // loaded; `PipelineRuntime::name()` is whatever the host passed at
            // construction, which differs per kind: the declared node id for a
            // config-loaded `wasm` node, the manifest name for a `plugin`, the
            // pipeline's own name for a native runtime. Prefer the
            // self-described name; the fallback fires only for a native
            // runtime, which declares none.
            let declared_name = if info.name.is_empty() {
                runtime.name().to_string()
            } else {
                info.name.clone()
            };

            let runtime_info = RuntimeInfo {
                kind: (*kind).to_string(),
                name: declared_name,
                version: info.version,
                stateful: info.stateful,
                schema_fingerprint: info.schema_fingerprint,
                declared_components: runtime
                    .declared_components()
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            };
            ("processor", detail, Some(runtime_info), window)
        }
    };

    TopoNode {
        id: node.id.clone(),
        kind: kind.to_string(),
        name: node.name.clone(),
        type_name: node.connector.clone(),
        component: node.component.map(str::to_string),
        runtime,
        #[cfg(feature = "windows")]
        window,
        #[cfg(not(feature = "windows"))]
        window: None,
        detail,
    }
}

/// The human-readable geometry of a spec, for the topology detail pairs.
#[cfg(feature = "windows")]
fn format_window_spec(spec: &WindowSpec) -> String {
    match spec {
        WindowSpec::Tumbling { size_ms, .. } => format!("tumbling {size_ms}ms"),
        WindowSpec::Sliding {
            size_ms, slide_ms, ..
        } => format!("sliding {size_ms}ms/{slide_ms}ms"),
        WindowSpec::Session { gap_ms } => format!("session gap {gap_ms}ms"),
    }
}

/// Map a host [`WindowConfig`](crate::service::config::WindowConfig) to the
/// wire [`WindowInfo`] the dashboard renders.
#[cfg(feature = "windows")]
fn window_info(config: &crate::service::config::WindowConfig) -> WindowInfo {
    use crate::inspector_wire::WindowInfo as Wire;
    let (kind, size_ms, slide_ms, offset_ms, gap_ms) = match &config.spec {
        WindowSpec::Tumbling { size_ms, offset_ms } => {
            ("tumbling", Some(*size_ms), None, Some(*offset_ms), None)
        }
        WindowSpec::Sliding {
            size_ms,
            slide_ms,
            offset_ms,
        } => (
            "sliding",
            Some(*size_ms),
            Some(*slide_ms),
            Some(*offset_ms),
            None,
        ),
        WindowSpec::Session { gap_ms } => ("session", None, None, None, Some(*gap_ms)),
    };
    Wire {
        kind: kind.to_string(),
        size_ms,
        slide_ms,
        offset_ms,
        gap_ms,
        time_field: config.time_field.clone(),
        key_fields: config.key_fields.clone(),
        allowed_lateness_ms: config.allowed_lateness_ms,
    }
}

/// One workflow's `(TopoNode, TopoEdge)` contribution: `nodes` must be in the
/// topological order `ServiceBuilder::build` produced for that workflow,
/// so a `BuiltEdge::node` index resolves within this same slice.
fn workflow_topo_parts(
    config: &ServiceConfig,
    registry: &Registry,
    nodes: &[BuiltNode],
) -> (Vec<TopoNode>, Vec<TopoEdge>) {
    let topo_nodes: Vec<TopoNode> = nodes
        .iter()
        .map(|node| node_view(config, registry, node))
        .collect();

    let mut edges = Vec::new();
    for node in nodes {
        for edge in &node.downstream {
            let d = edge.node;
            edges.push(TopoEdge {
                from: node.id.clone(),
                to: nodes[d].id.clone(),
                branch: edge.branch.clone(),
            });
        }
    }
    (topo_nodes, edges)
}

/// One [`BridgeEdge`] per channel name with both a `channel` sink and a
/// `channel` source declared, so the bridge `ServiceBuilder::build` wires
/// through the channel registry is visible to the dashboard without belonging
/// to either workflow's edge list.
///
/// The result is sorted by channel name: it is built by scanning two
/// `HashMap`s, whose iteration order is not deterministic between builds, and
/// both the tests and the dashboard list expect a stable order.
fn channel_bridge_edges(config: &ServiceConfig) -> Vec<BridgeEdge> {
    let mut sinks: HashMap<&str, &str> = HashMap::new();
    let mut sources: HashMap<&str, &str> = HashMap::new();
    for wf in &config.workflows {
        for s in &wf.sinks {
            if s.connector == "channel"
                && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
            {
                sinks.insert(name, s.id.as_str());
            }
        }
        for s in &wf.sources {
            if s.connector == "channel"
                && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
            {
                sources.insert(name, s.id.as_str());
            }
        }
    }
    let mut out: Vec<BridgeEdge> = sinks
        .into_iter()
        .filter_map(|(name, sink_id)| {
            sources.get(name).map(|&source_id| BridgeEdge {
                channel: name.to_string(),
                from: sink_id.to_string(),
                to: source_id.to_string(),
            })
        })
        .collect();
    out.sort_by(|a, b| a.channel.cmp(&b.channel));
    out
}

/// Build the topology for one workflow from its [`WorkflowSpec`] and [`BuiltNode`]s.
pub(crate) fn workflow_topology(
    config: &ServiceConfig,
    registry: &Registry,
    spec: &WorkflowSpec,
    nodes: &[BuiltNode],
) -> WorkflowTopology {
    let (nodes, edges) = workflow_topo_parts(config, registry, nodes);
    WorkflowTopology {
        id: spec.id.clone(),
        name: spec.name.clone(),
        nodes,
        edges,
    }
}

/// Build the topology for every workflow this process runs.
///
/// `registry` is the one the workflows were built from: each node's
/// connector names the config keys its detail may show
/// ([`Connector::detail_keys`](crate::connector::Connector::detail_keys)). `workflow_nodes` holds one entry per declared
/// workflow, each the [`BuiltNode`]s `ServiceBuilder::build` produced for it,
/// in topological order, the order the dashboard's depth layout expects
/// within each workflow. The result carries one [`WorkflowTopology`] per
/// declared workflow, zipped from `config.workflows` in declaration order, so
/// each keeps its own `id`/`name` and its own edge list. Cross-workflow
/// channel bridges are reported separately in `Topology::bridges`, because
/// their endpoints span workflows.
pub fn build_topology(
    config: &ServiceConfig,
    registry: &Registry,
    workflow_nodes: &[&[BuiltNode]],
    version: u64,
) -> Topology {
    let workflows = config
        .workflows
        .iter()
        .zip(workflow_nodes)
        .map(|(spec, nodes)| workflow_topology(config, registry, spec, nodes))
        .collect();

    Topology {
        version,
        node_id: config.node.id.to_string(),
        mode: match config.mode {
            ServiceMode::Standalone { .. } => "standalone".to_string(),
            ServiceMode::Cluster { .. } => "cluster".to_string(),
        },
        workflows,
        bridges: channel_bridge_edges(config),
    }
}

/// Rebuild [`Topology`] after a live configuration reload, bumping `version`
/// and preserving existing [`RuntimeInfo`] for processor nodes not supplied in
/// `built_updates`.
pub(crate) fn update_topology_from_config(
    config: &ServiceConfig,
    registry: &Registry,
    existing: &Topology,
    built_updates: &HashMap<String, WorkflowTopology>,
) -> Topology {
    let existing_by_wf: HashMap<&str, &WorkflowTopology> = existing
        .workflows
        .iter()
        .map(|w| (w.id.as_str(), w))
        .collect();

    let workflows = config
        .workflows
        .iter()
        .map(|spec| {
            if let Some(fresh) = built_updates.get(&spec.id) {
                return fresh.clone();
            }
            let prev_nodes: HashMap<&str, &TopoNode> = existing_by_wf
                .get(spec.id.as_str())
                .map(|w| w.nodes.iter().map(|n| (n.id.as_str(), n)).collect())
                .unwrap_or_default();
            workflow_topology_from_spec(registry, spec, &prev_nodes)
        })
        .collect();

    Topology {
        version: existing.version.saturating_add(1),
        node_id: config.node.id.to_string(),
        mode: match config.mode {
            ServiceMode::Standalone { .. } => "standalone".to_string(),
            ServiceMode::Cluster { .. } => "cluster".to_string(),
        },
        workflows,
        bridges: channel_bridge_edges(config),
    }
}

fn workflow_topology_from_spec(
    registry: &Registry,
    spec: &WorkflowSpec,
    prev_nodes: &HashMap<&str, &TopoNode>,
) -> WorkflowTopology {
    let meta = spec.nodes();
    let order = spec
        .topological_order()
        .unwrap_or_else(|_| (0..meta.len()).collect());

    let mut topo_nodes = Vec::with_capacity(order.len());
    for nat_idx in order {
        let Some(&(id, kind)) = meta.get(nat_idx) else {
            continue;
        };
        let prev = prev_nodes.get(id).copied();
        let node = match kind {
            NodeKind::Source => {
                let source = spec.sources.iter().find(|s| s.id == id);
                let (name, connector, component, detail) = match source {
                    Some(s) => (
                        s.name.clone(),
                        s.connector.clone(),
                        Some(s.component.clone()),
                        detail_for(registry, &s.connector, Role::Source, &s.config),
                    ),
                    None => (None, String::new(), None, Vec::new()),
                };
                TopoNode {
                    id: id.to_string(),
                    kind: "source".to_string(),
                    name,
                    type_name: connector,
                    component,
                    runtime: None,
                    window: None,
                    detail,
                }
            }
            NodeKind::Sink => {
                let sink = spec.sinks.iter().find(|s| s.id == id);
                let (name, connector, component, detail) = match sink {
                    Some(s) => (
                        s.name.clone(),
                        s.connector.clone(),
                        Some(s.component.clone()),
                        detail_for(registry, &s.connector, Role::Sink, &s.config),
                    ),
                    None => (None, String::new(), None, Vec::new()),
                };
                TopoNode {
                    id: id.to_string(),
                    kind: "sink".to_string(),
                    name,
                    type_name: connector,
                    component,
                    runtime: None,
                    window: None,
                    detail,
                }
            }
            NodeKind::Processor => processor_topo_node_from_spec(spec, id, prev),
        };
        topo_nodes.push(node);
    }

    let edges = spec
        .links
        .iter()
        .map(|l| TopoEdge {
            from: l.from.clone(),
            to: l.to.clone(),
            branch: l.branch.clone(),
        })
        .collect();

    WorkflowTopology {
        id: spec.id.clone(),
        name: spec.name.clone(),
        nodes: topo_nodes,
        edges,
    }
}

fn processor_topo_node_from_spec(
    spec: &WorkflowSpec,
    id: &str,
    prev: Option<&TopoNode>,
) -> TopoNode {
    #[cfg(feature = "windows")]
    let mut win_cfg: Option<&crate::service::config::WindowConfig> = None;

    let (name, type_name, component, artifact): (
        Option<String>,
        String,
        Option<String>,
        Option<&str>,
    ) = if let Some(w) = spec.wasm.iter().find(|w| w.id == id) {
        #[cfg(feature = "windows")]
        {
            win_cfg = w.window.as_ref();
        }
        (w.name.clone(), "wasm".to_string(), None, w.path.as_deref())
    } else if let Some(p) = spec.plugin.iter().find(|p| p.id == id) {
        #[cfg(feature = "windows")]
        {
            win_cfg = p.window.as_ref();
        }
        (
            p.name.clone(),
            "plugin".to_string(),
            None,
            p.path.as_deref(),
        )
    } else if let Some(o) = spec.operator.iter().find(|o| o.id == id) {
        #[cfg(feature = "windows")]
        {
            win_cfg = o.window.as_ref();
        }
        (o.name.clone(), format!("operator:{}", o.kind), None, None)
    } else {
        (None, "processor".to_string(), None, None)
    };

    let runtime = prev.and_then(|n| n.runtime.clone());
    let mut detail = Vec::new();
    if let Some(info) = &runtime {
        if !info.version.is_empty() {
            detail.push(("version".to_string(), info.version.clone()));
        }
        detail.push(("stateful".to_string(), info.stateful.to_string()));
    }
    if let Some(path) = artifact {
        let file_name = std::path::Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(path);
        detail.push(("artifact".to_string(), file_name.to_string()));
    }
    #[cfg(feature = "windows")]
    let window = if let Some(win) = win_cfg {
        detail.push(("window".to_string(), format_window_spec(&win.spec)));
        detail.push(("window.time_field".to_string(), win.time_field.clone()));
        if !win.key_fields.is_empty() {
            detail.push(("window.key_fields".to_string(), win.key_fields.join(", ")));
        }
        detail.push((
            "window.allowed_lateness_ms".to_string(),
            win.allowed_lateness_ms.to_string(),
        ));
        Some(window_info(win))
    } else {
        None
    };
    #[cfg(not(feature = "windows"))]
    let window = None;

    TopoNode {
        id: id.to_string(),
        kind: "processor".to_string(),
        name,
        type_name,
        component,
        runtime,
        window,
        detail,
    }
}

#[cfg(all(test, feature = "service", feature = "connector-channel"))]
mod tests {
    use super::*;
    use crate::connector::Connector;
    use crate::pipeline::Pipeline;
    use crate::service::builder::{BuiltWorkflow, BuiltWorkflowBuilder};
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, RetryConfig, SinkSpec, SourceSpec,
        StandaloneConfig, WorkflowSpec,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── `detail_for` / `scalar_at`, tested directly ─────────────────────────

    fn cfg(raw: &str) -> ConfigValue {
        crate::config::from_kdl_str(raw).expect("parse test config")
    }

    /// A connector that declares only its detail keys, so these tests need no
    /// real connector compiled in. The keys mirror what the real connectors
    /// of the same name declare.
    struct DetailConnector {
        name: &'static str,
        source: &'static [&'static str],
        sink: &'static [&'static str],
    }

    impl Connector for DetailConnector {
        fn name(&self) -> &'static str {
            self.name
        }

        fn detail_keys(&self, role: Role) -> &'static [&'static str] {
            match role {
                Role::Source => self.source,
                Role::Sink => self.sink,
            }
        }
    }

    /// The registry the detail tests ask: `nats`, `postgresql`, `udp`, `file`
    /// and `channel`, each with the keys the real connector shows.
    fn detail_registry() -> Registry {
        const NATS: &[&str] = &["mode.kind", "mode.stream", "mode.subject"];
        const UDP: &[&str] = &[
            "bind",
            "connect",
            "multicast_group",
            "broadcast",
            "probe.kind",
            "probe.interval_ms",
            "probe.timeout_ms",
            "probe.on_failure",
        ];
        let mut registry = Registry::new();
        registry
            .register(DetailConnector {
                name: "nats",
                source: NATS,
                sink: NATS,
            })
            .register(DetailConnector {
                name: "postgresql",
                source: &["mode.kind", "mode.table"],
                sink: &["table", "write_mode"],
            })
            .register(DetailConnector {
                name: "udp",
                source: UDP,
                sink: UDP,
            })
            .register(DetailConnector {
                name: "file",
                source: &["path"],
                sink: &["path"],
            })
            .register(DetailConnector {
                name: "channel",
                source: &["name"],
                sink: &["name"],
            });
        registry
    }

    #[test]
    fn detail_for_keeps_only_allowlisted_keys() {
        let config = cfg(r#"
mode kind="core" subject="authorizations.raw"
connection url="nats://localhost:4222" password="hunter2"
"#);
        let detail = detail_for(&detail_registry(), "nats", Role::Source, &config);
        assert!(detail.contains(&("mode.kind".to_string(), "core".to_string())));
        assert!(detail.contains(&("mode.subject".to_string(), "authorizations.raw".to_string())));
        assert!(
            !detail.iter().any(|(k, _)| k.starts_with("connection")),
            "connection.password is not allowlisted for nats: {detail:?}"
        );
    }

    #[test]
    fn detail_for_an_unknown_connector_is_empty() {
        let config = cfg("secret_token \"abc\"\n");
        assert!(
            detail_for(
                &detail_registry(),
                "some-user-source",
                Role::Source,
                &config
            )
            .is_empty()
        );
    }

    /// A connector's keys are per role: the `postgresql` source shows a mode
    /// table, never the sink's `write_mode`.
    #[test]
    fn detail_keys_follow_the_node_role() {
        let config = cfg("table \"t\"\nwrite_mode \"upsert\"\n");
        let registry = detail_registry();
        assert!(detail_for(&registry, "postgresql", Role::Source, &config).is_empty());
        assert_eq!(
            detail_for(&registry, "postgresql", Role::Sink, &config).len(),
            2
        );
    }

    // ── `build_topology`, against a hand-assembled node list ────────────────
    //
    // A real `ServiceBuilder::build` needs every connector to construct
    // successfully (real broker DSNs, a real wasm artifact, ...), which is
    // exactly what `build_topology` itself must not depend on: it only reads
    // `BuiltNode` identity/kind plus each node's *declared* config for detail.
    // These tests assemble `BuiltNode`s directly so the topology shape is
    // tested in isolation, matching `crates/saci/src/service/validation.rs`'s
    // convention for the same reason.

    fn workflow_with_io(source_config: ConfigValue, sink_config: ConfigValue) -> WorkflowSpec {
        WorkflowSpec {
            id: "orders".to_string(),
            name: None,
            codecs: Vec::new(),
            schemas: Vec::new(),
            sources: vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "orders-in".to_string(),
                name: None,
                connector: "nats".to_string(),
                codec: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                decrypt: None,
                config: source_config,
            }],
            wasm: Vec::new(),
            plugin: Vec::new(),
            operator: Vec::new(),
            sinks: vec![SinkSpec {
                heal: None,
                id: "settlements".to_string(),
                name: None,
                connector: "postgresql".to_string(),
                codec: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                encrypt: None,
                config: sink_config,
            }],
            links: vec![super::super::config::LinkSpec {
                from: "orders-in".to_string(),
                to: "settlements".to_string(),
                branch: None,
            }],
            dlq: None,
            run: None,
        }
    }

    fn service_config(workflow: WorkflowSpec) -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 7,
                name: None,
                data_dir: std::path::PathBuf::from("/tmp/saci-topology-test"),
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
            variables: HashMap::new(),
        }
    }
    fn service_config_many(workflows: Vec<WorkflowSpec>) -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 7,
                name: None,
                data_dir: std::path::PathBuf::from("/tmp/saci-topology-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows,
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            encryption: None,
            alerts: Default::default(),
            variables: HashMap::new(),
        }
    }

    fn order_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// A `BuiltNode` backed by a cheap `ChannelSource`/`ChannelSink`/native
    /// `Pipeline`, but carrying whatever `id`/`connector` the caller declares:
    /// `node_view` selects the detail keys purely off `connector`, so the
    /// concrete Rust type underneath never needs to be the real connector.
    fn built_source(id: &str, connector: &str) -> BuiltNode {
        use crate::connector::channel::ChannelSource;
        let (src, _tx) = ChannelSource::new(order_schema(), 1);
        let mut node = lone_node(BuiltWorkflow::builder("w").source(id, "Order", src));
        node.connector = connector.to_string();
        node
    }

    fn built_sink(id: &str, connector: &str) -> BuiltNode {
        use crate::connector::channel::ChannelSink;
        let (sink, _rx) = ChannelSink::new(order_schema(), 1);
        let mut node = lone_node(BuiltWorkflow::builder("w").sink(id, "Order", sink));
        node.connector = connector.to_string();
        node
    }

    fn built_processor(id: &str, artifact: Option<&str>) -> BuiltNode {
        let mut node = lone_node(BuiltWorkflow::builder("w").processor(id, Pipeline::new(id)));
        node.name = Some("Validator".to_string());
        node.connector = "wasm".to_string();
        if let BuiltNodeKind::Processor { kind, .. } = &mut node.kind {
            *kind = "wasm";
        }
        node.artifact = artifact.map(str::to_string);
        node
    }

    /// The one node of a single-node workflow, so a test can edit its fields
    /// and wire its `downstream` by hand.
    fn lone_node(builder: BuiltWorkflowBuilder) -> BuiltNode {
        builder
            .build()
            .expect("a one-node workflow assembles")
            .nodes
            .remove(0)
    }

    #[test]
    fn nodes_and_edges_match_the_declared_io() {
        let workflow = workflow_with_io(default_config_value(), default_config_value());
        let config = service_config(workflow);
        let mut source = built_source("orders-in", "nats");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = built_sink("settlements", "postgresql");
        let nodes = [source, sink];
        let topology = build_topology(&config, &detail_registry(), &[nodes.as_slice()], 1);

        assert_eq!(topology.node_id, "7");
        assert_eq!(topology.mode, "standalone");
        assert_eq!(topology.workflows[0].id, "orders");

        let ids: Vec<&str> = topology.workflows[0]
            .nodes
            .iter()
            .map(|n| n.id.as_str())
            .collect();
        assert_eq!(ids, vec!["orders-in", "settlements"]);
        assert_eq!(topology.workflows[0].nodes[0].kind, "source");
        assert_eq!(topology.workflows[0].nodes[1].kind, "sink");

        assert_eq!(topology.workflows[0].edges.len(), 1);
        assert_eq!(topology.workflows[0].edges[0].from, "orders-in");
        assert_eq!(topology.workflows[0].edges[0].to, "settlements");
    }

    #[test]
    fn branch_labels_flow_into_topology_edges() {
        let workflow = workflow_with_io(default_config_value(), default_config_value());
        let config = service_config(workflow);
        let mut processor = built_processor("validate", Some("build/validate.wasm"));
        processor.downstream = vec![
            BuiltEdge {
                node: 1,
                branch: Some("high".to_string()),
            },
            BuiltEdge {
                node: 2,
                branch: Some("low".to_string()),
            },
        ];
        let nodes = [
            processor,
            built_sink("out_high", "postgresql"),
            built_sink("out_low", "postgresql"),
        ];
        let topology = build_topology(&config, &detail_registry(), &[nodes.as_slice()], 1);

        let branches: Vec<Option<&str>> = topology.workflows[0]
            .edges
            .iter()
            .map(|edge| edge.branch.as_deref())
            .collect();
        assert_eq!(branches, vec![Some("high"), Some("low")]);
    }

    fn default_config_value() -> ConfigValue {
        ConfigValue::Object(crate::config::ConfigMap::new())
    }

    #[test]
    fn no_detail_value_carries_a_credential() {
        let source_config = cfg(r#"
mode kind="core" subject="authorizations.raw"
connection url="nats://localhost:4222" password="hunter2"
"#);
        let sink_config = cfg(r#"
table "public.settlements"
write_mode "upsert"
connection dsn="postgres://postgres:s3cret@127.0.0.1:5432/saci"
"#);
        let workflow = workflow_with_io(source_config, sink_config);
        let config = service_config(workflow);
        let mut source = built_source("orders-in", "nats");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = built_sink("settlements", "postgresql");
        let nodes = [source, sink];
        let topology = build_topology(&config, &detail_registry(), &[nodes.as_slice()], 1);
        let rendered = serde_json::to_string(&topology).expect("serialize");

        assert!(!rendered.contains("hunter2"), "source password leaked");
        assert!(!rendered.contains("s3cret"), "sink DSN password leaked");
        assert!(!rendered.contains("dsn"), "DSN key leaked");

        let sink_node = &topology.workflows[0].nodes[1];
        assert!(
            sink_node
                .detail
                .contains(&("table".to_string(), "public.settlements".to_string()))
        );
        assert!(
            sink_node
                .detail
                .contains(&("write_mode".to_string(), "upsert".to_string()))
        );
    }

    /// The `udp` sink's own allowlist: `connect`, `broadcast` and every
    /// `probe.*` key render; `field`, present in the raw config but
    /// never in the allowlist, does not.
    #[test]
    fn a_udp_sink_shows_its_own_detail_keys_and_not_field() {
        let sink_config = cfg(r#"
connect "127.0.0.1:9701"
broadcast #true
probe kind="echo" interval_ms=100 on_failure="warn"
field "v" type="int64" nullable=#false
"#);
        let workflow = workflow_with_io(default_config_value(), sink_config);
        let config = service_config(workflow);
        let mut source = built_source("orders-in", "nats");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = built_sink("settlements", "udp");
        let nodes = [source, sink];
        let topology = build_topology(&config, &detail_registry(), &[nodes.as_slice()], 1);

        let sink_node = &topology.workflows[0].nodes[1];
        assert!(
            sink_node
                .detail
                .contains(&("connect".to_string(), "127.0.0.1:9701".to_string()))
        );
        assert!(
            sink_node
                .detail
                .contains(&("broadcast".to_string(), "true".to_string()))
        );
        assert!(
            sink_node
                .detail
                .contains(&("probe.kind".to_string(), "echo".to_string()))
        );
        assert!(
            sink_node
                .detail
                .contains(&("probe.interval_ms".to_string(), "100".to_string()))
        );
        assert!(
            sink_node
                .detail
                .contains(&("probe.on_failure".to_string(), "warn".to_string()))
        );
        assert!(
            sink_node.detail.iter().all(|(k, _)| k != "field"),
            "got: {:?}",
            sink_node.detail
        );
    }

    #[test]
    fn processor_node_carries_a_populated_runtime_and_source_sink_do_not() {
        let workflow = workflow_with_io(default_config_value(), default_config_value());
        let config = service_config(workflow);
        let mut source = built_source("orders-in", "nats");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let mut processor = built_processor("validate", Some("build/validate.wasm"));
        processor.downstream.push(BuiltEdge {
            node: 2,
            branch: None,
        });
        let sink = built_sink("settlements", "postgresql");
        let nodes = [source, processor, sink];
        let topology = build_topology(&config, &detail_registry(), &[nodes.as_slice()], 1);

        assert!(topology.workflows[0].nodes[0].runtime.is_none());
        assert!(topology.workflows[0].nodes[2].runtime.is_none());

        let proc_node = &topology.workflows[0].nodes[1];
        assert_eq!(proc_node.kind, "processor");
        assert_eq!(proc_node.name.as_deref(), Some("Validator"));
        let info = proc_node.runtime.as_ref().expect("processor runtime info");
        assert_eq!(info.kind, "wasm");
        assert!(
            proc_node
                .detail
                .contains(&("artifact".to_string(), "validate.wasm".to_string()))
        );
        assert!(
            proc_node.detail.iter().any(|(k, _)| k == "stateful"),
            "got: {:?}",
            proc_node.detail
        );
    }

    #[cfg(feature = "windows")]
    #[test]
    fn windowed_processor_carries_window_info_and_detail() {
        use crate::service::config::WindowConfig;
        use crate::window::WindowSpec;

        let workflow = workflow_with_io(default_config_value(), default_config_value());
        let config = service_config(workflow);
        let mut processor = built_processor("windowed", Some("build/windowed.wasm"));
        processor.window = Some(WindowConfig {
            spec: WindowSpec::tumbling(30_000),
            time_field: "timestamp_ms".to_string(),
            key_fields: vec!["category".to_string()],
            allowed_lateness_ms: 5_000,
        });
        let nodes = [processor];
        let topology = build_topology(&config, &detail_registry(), &[nodes.as_slice()], 1);

        let node = &topology.workflows[0].nodes[0];
        let info = node.window.as_ref().expect("window info");
        assert_eq!(info.kind, "tumbling");
        assert_eq!(info.size_ms, Some(30_000));
        assert_eq!(info.time_field, "timestamp_ms");
        assert_eq!(info.key_fields, vec!["category"]);
        assert_eq!(info.allowed_lateness_ms, 5_000);
        assert!(
            node.detail
                .iter()
                .any(|(k, v)| k == "window" && v == "tumbling 30000ms"),
            "got: {:?}",
            node.detail
        );
        assert!(
            node.detail
                .iter()
                .any(|(k, v)| k == "window.time_field" && v == "timestamp_ms"),
            "got: {:?}",
            node.detail
        );
    }

    #[test]
    fn two_workflows_keep_their_own_identity_and_the_bridge_is_reported_separately() {
        let producer = WorkflowSpec {
            id: "producer".to_string(),
            name: None,
            codecs: Vec::new(),
            schemas: Vec::new(),
            sources: vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "orders_in".to_string(),
                name: None,
                connector: "file".to_string(),
                codec: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                decrypt: None,
                config: default_config_value(),
            }],
            wasm: Vec::new(),
            plugin: Vec::new(),
            operator: Vec::new(),
            sinks: vec![SinkSpec {
                heal: None,
                id: "bridge_out".to_string(),
                name: None,
                connector: "channel".to_string(),
                codec: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                encrypt: None,
                config: cfg("name \"bridge\""),
            }],
            links: vec![super::super::config::LinkSpec {
                from: "orders_in".to_string(),
                to: "bridge_out".to_string(),
                branch: None,
            }],
            dlq: None,
            run: None,
        };
        let consumer = WorkflowSpec {
            id: "consumer".to_string(),
            name: None,
            codecs: Vec::new(),
            schemas: Vec::new(),
            sources: vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "bridge_in".to_string(),
                name: None,
                connector: "channel".to_string(),
                codec: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                decrypt: None,
                config: cfg("name \"bridge\""),
            }],
            wasm: Vec::new(),
            plugin: Vec::new(),
            operator: Vec::new(),
            sinks: vec![SinkSpec {
                heal: None,
                id: "orders_out".to_string(),
                name: None,
                connector: "file".to_string(),
                codec: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                encrypt: None,
                config: default_config_value(),
            }],
            links: vec![super::super::config::LinkSpec {
                from: "bridge_in".to_string(),
                to: "orders_out".to_string(),
                branch: None,
            }],
            dlq: None,
            run: None,
        };
        let config = service_config_many(vec![producer, consumer]);

        let mut source = built_source("orders_in", "file");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let bridge_out = built_sink("bridge_out", "channel");
        let producer_nodes = [source, bridge_out];

        let mut bridge_in = built_source("bridge_in", "channel");
        bridge_in.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let orders_out = built_sink("orders_out", "file");
        let consumer_nodes = [bridge_in, orders_out];

        let topology = build_topology(
            &config,
            &detail_registry(),
            &[producer_nodes.as_slice(), consumer_nodes.as_slice()],
            1,
        );

        assert_eq!(topology.workflows.len(), 2);
        let ids: Vec<&str> = topology.workflows.iter().map(|w| w.id.as_str()).collect();
        assert_eq!(ids, vec!["producer", "consumer"]);

        let producer_ids: Vec<&str> = topology.workflows[0]
            .nodes
            .iter()
            .map(|n| n.id.as_str())
            .collect();
        assert_eq!(producer_ids, vec!["orders_in", "bridge_out"]);
        assert_eq!(topology.workflows[0].edges.len(), 1);
        assert_eq!(topology.workflows[0].edges[0].from, "orders_in");
        assert_eq!(topology.workflows[0].edges[0].to, "bridge_out");

        let consumer_ids: Vec<&str> = topology.workflows[1]
            .nodes
            .iter()
            .map(|n| n.id.as_str())
            .collect();
        assert_eq!(consumer_ids, vec!["bridge_in", "orders_out"]);
        assert_eq!(topology.workflows[1].edges.len(), 1);
        assert_eq!(topology.workflows[1].edges[0].from, "bridge_in");
        assert_eq!(topology.workflows[1].edges[0].to, "orders_out");

        assert_eq!(
            topology.bridges,
            vec![BridgeEdge {
                channel: "bridge".to_string(),
                from: "bridge_out".to_string(),
                to: "bridge_in".to_string(),
            }]
        );
    }
}
