//! [`build_topology`]: what the dashboard draws, derived from config plus
//! every built node's self-description.
//!
//! ## One node per declared workflow node
//!
//! One [`TopoNode`] per [`BuiltNode`], in the topological order
//! `ServiceBuilder::build_all` produced, and one [`TopoEdge`] per entry of that
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
//! [`Snapshot::span_stats`](saci_inspector_wire::Snapshot::span_stats), which is
//! fed by the retained spans rather than the topology.
//!
//! ## Redaction is mandatory
//!
//! [`SourceSpec::config`](crate::service::config::SourceSpec::config) and
//! [`SinkSpec::config`](crate::service::config::SinkSpec::config) are opaque
//! config tables holding `connection.dsn`, `connection.password` and
//! credential file paths. [`TopoNode::detail`] therefore copies values through a
//! per-`type` allowlist; a key outside it is dropped, never masked, so nothing
//! in the response hints at a secret's shape or length.

use std::collections::HashMap;

use saci_config::ConfigValue;
#[cfg(feature = "windows")]
use saci_core::windows::WindowSpec;
#[cfg(feature = "windows")]
use saci_inspector_wire::WindowInfo;
use saci_inspector_wire::{
    BridgeEdge, RuntimeInfo, TopoEdge, TopoNode, Topology, WorkflowTopology,
};

#[cfg(test)]
use super::builder::BuiltEdge;
use super::builder::{BuiltNode, BuiltNodeKind};
use super::config::ServiceConfig;
use super::config::ServiceMode;

/// Which config keys may be shown, per connector `type`.
///
/// The keys are the strings `SourceFactory::type_name` / `SinkFactory::type_name`
/// return, which is what a config's `type =` names, not the Rust type names.
/// Both TCP halves register as `"tcp"`, so that one entry covers the source's
/// `bind` and the sink's `connect`.
///
/// Dotted keys walk nested tables (`mode.subject` reads `[sources.config.mode]`
/// then `subject`). A `type` absent from this table gets no detail at all: an
/// unknown connector is precisely the case where guessing which keys are safe
/// is wrong.
const DETAIL_ALLOWLIST: &[(&str, &[&str])] = &[
    ("NatsSource", &["mode.kind", "mode.stream", "mode.subject"]),
    ("NatsSink", &["mode.kind", "mode.stream", "mode.subject"]),
    ("PostgresSource", &["mode.kind", "mode.table"]),
    ("PostgresSink", &["table", "write_mode"]),
    ("TursoSource", &["mode.kind", "mode.table"]),
    ("TursoSink", &["table", "write_mode"]),
    ("S3Source", &["connection.bucket", "prefix"]),
    ("S3Sink", &["connection.bucket", "prefix"]),
    ("FileSource", &["path"]),
    ("FileSink", &["path"]),
    (
        "RedbSource",
        &["directory", "file", "table", "key_prefix", "key_suffix"],
    ),
    (
        "RedbSink",
        &[
            "directory",
            "file",
            "table",
            "key_prefix",
            "key_suffix",
            "durability",
            "compact",
        ],
    ),
    ("KafkaSource", &["topic"]),
    ("KafkaSink", &["topic"]),
    ("tcp", &["bind", "connect"]),
    ("saci", &["bind", "connect"]),
    ("ChannelSource", &["name"]),
    ("ChannelSink", &["name"]),
];

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

/// Every allowlisted `(key, value)` pair for one connector instance.
fn detail_for(type_name: &str, config: &ConfigValue) -> Vec<(String, String)> {
    let Some((_, keys)) = DETAIL_ALLOWLIST.iter().find(|(name, _)| *name == type_name) else {
        return Vec::new();
    };
    keys.iter()
        .filter_map(|key| scalar_at(config, key).map(|value| ((*key).to_string(), value)))
        .collect()
}

/// One [`TopoNode`] plus its `runtime` self-description, if any.
fn node_view(config: &ServiceConfig, node: &BuiltNode) -> TopoNode {
    #[cfg_attr(not(feature = "windows"), allow(unused_variables))]
    let (kind, detail, runtime, window) = match &node.kind {
        BuiltNodeKind::Source(_) => {
            let detail = config
                .workflows
                .iter()
                .flat_map(|w| w.sources.iter())
                .find(|s| s.id == node.id)
                .map_or_else(Vec::new, |spec| detail_for(&node.type_name, &spec.config));
            ("source", detail, None, None)
        }
        BuiltNodeKind::Sink(_) => {
            let detail = config
                .workflows
                .iter()
                .flat_map(|w| w.sinks.iter())
                .find(|s| s.id == node.id)
                .map_or_else(Vec::new, |spec| detail_for(&node.type_name, &spec.config));
            ("sink", detail, None, None)
        }
        BuiltNodeKind::Processor { runtime, kind } => {
            let info = runtime.descriptor_info();
            #[cfg(feature = "windows")]
            let window = node.window.as_ref().map(window_info);
            #[cfg(not(feature = "windows"))]
            let window: Option<saci_inspector_wire::WindowInfo> = None;

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
        type_name: node.type_name.clone(),
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
    use saci_inspector_wire::WindowInfo as Wire;
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
/// topological order `ServiceBuilder::build_all` produced for that workflow,
/// so a `BuiltEdge::node` index resolves within this same slice.
fn workflow_topo_parts(
    config: &ServiceConfig,
    nodes: &[BuiltNode],
) -> (Vec<TopoNode>, Vec<TopoEdge>) {
    let topo_nodes: Vec<TopoNode> = nodes.iter().map(|node| node_view(config, node)).collect();

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

/// One [`BridgeEdge`] per channel name with both a `ChannelSink` and a
/// `ChannelSource` declared, so the bridge `ServiceBuilder::build_all` wires
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
            if s.type_name == "ChannelSink"
                && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
            {
                sinks.insert(name, s.id.as_str());
            }
        }
        for s in &wf.sources {
            if s.type_name == "ChannelSource"
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

/// Build the topology for every workflow this process runs.
///
/// `workflow_nodes` holds one entry per declared workflow, each the
/// [`BuiltNode`]s `ServiceBuilder::build_all` produced for it, in topological
/// order, the order the dashboard's depth layout expects within each
/// workflow. The result carries one [`WorkflowTopology`] per declared
/// workflow, zipped from `config.workflows` in declaration order, so each
/// keeps its own `id`/`name` and its own edge list. Cross-workflow channel
/// bridges are reported separately in `Topology::bridges`, because their
/// endpoints span workflows.
pub fn build_topology(
    config: &ServiceConfig,
    workflow_nodes: &[&[BuiltNode]],
    version: u64,
) -> Topology {
    let workflows = config
        .workflows
        .iter()
        .zip(workflow_nodes)
        .map(|(spec, nodes)| {
            let (nodes, edges) = workflow_topo_parts(config, nodes);
            WorkflowTopology {
                id: spec.id.clone(),
                name: spec.name.clone(),
                nodes,
                edges,
            }
        })
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

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::pipeline::Pipeline;
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, RetryConfig, SinkSpec, SourceSpec,
        StandaloneConfig, WorkflowSpec,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── `detail_for` / `scalar_at`, tested directly ─────────────────────────

    fn cfg(raw: &str) -> ConfigValue {
        saci_config::from_kdl_str(raw).expect("parse test config")
    }

    #[test]
    fn detail_for_keeps_only_allowlisted_keys() {
        let config = cfg(r#"
mode kind="core" subject="authorizations.raw"
connection url="nats://localhost:4222" password="hunter2"
"#);
        let detail = detail_for("NatsSource", &config);
        assert!(detail.contains(&("mode.kind".to_string(), "core".to_string())));
        assert!(detail.contains(&("mode.subject".to_string(), "authorizations.raw".to_string())));
        assert!(
            !detail.iter().any(|(k, _)| k.starts_with("connection")),
            "connection.password is not allowlisted for NatsSource: {detail:?}"
        );
    }

    #[test]
    fn detail_for_an_unknown_type_is_empty() {
        let config = cfg("secret_token \"abc\"\n");
        assert!(detail_for("SomeUserDefinedSource", &config).is_empty());
    }

    /// Every allowlist entry must name a connector type this crate ships.
    ///
    /// A key that matches no connector is dead: the connector it was meant for
    /// silently renders no detail, which reads as "this connector has nothing
    /// to show" rather than as the typo it is. `TcpIngestSource` registers as
    /// `"tcp"`, which is exactly the mistake this pins down.
    ///
    /// Pinned against
    /// [`BUILTIN_CONNECTOR_FEATURES`](crate::service::factories::BUILTIN_CONNECTOR_FEATURES)
    /// rather than against the live registry. The registry holds only the
    /// connectors *this* build compiled in, so a registry check states the
    /// invariant as "every allowlist key is registered here", which is false
    /// in every build short of `all`, including the default bundle, where the
    /// five opt-in connectors are absent. Such a check would have to be
    /// switched off exactly where a dead allowlist key is hardest to notice.
    /// The table is declared unconditionally, and
    /// `factories::tests::builtin_connector_table_matches_the_registry` pins
    /// it against the registry in both directions, so the two checks chained
    /// still say "no allowlist key is a name no connector ever registers",
    /// and this half says it in every build.
    ///
    /// The relation is subset, not equality: the converse is deliberately not
    /// asserted, because `HttpSource` and `HttpSink` are tabled connectors
    /// with no allowlisted key at all, a URL carrying credentials. A duplicate
    /// key is checked too: `detail_for` resolves a `type` with `find`, so a
    /// second entry for the same connector never renders and its keys are
    /// dead.
    #[test]
    fn every_allowlist_key_is_a_connector_type_this_crate_ships() {
        use crate::service::factories::BUILTIN_CONNECTOR_FEATURES;

        for (type_name, _) in DETAIL_ALLOWLIST {
            assert!(
                BUILTIN_CONNECTOR_FEATURES
                    .iter()
                    .any(|(tabled, _)| tabled == type_name),
                "allowlist key '{type_name}' is not a connector type this crate \
                 ships: the key must be what SourceFactory/SinkFactory::type_name \
                 returns, as listed in BUILTIN_CONNECTOR_FEATURES"
            );
        }

        let mut seen: Vec<&str> = DETAIL_ALLOWLIST.iter().map(|(name, _)| *name).collect();
        let before = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(
            seen.len(),
            before,
            "DETAIL_ALLOWLIST has a duplicate type: detail_for finds the first \
             entry only, so the second one's keys never render; merge them"
        );
    }

    // ── `build_topology`, against a hand-assembled node list ────────────────
    //
    // A real `ServiceBuilder::build_all` needs every connector to construct
    // successfully (real broker DSNs, a real wasm artifact, ...), which is
    // exactly what `build_topology` itself must not depend on: it only reads
    // `BuiltNode` identity/kind plus each node's *declared* config for detail.
    // These tests assemble `BuiltNode`s directly so the topology shape is
    // tested in isolation, matching `crates/saci-service/src/service/validation.rs`'s
    // convention for the same reason.

    fn workflow_with_io(source_config: ConfigValue, sink_config: ConfigValue) -> WorkflowSpec {
        WorkflowSpec {
            id: "orders".to_string(),
            name: None,
            transformers: Vec::new(),
            sources: vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "orders-in".to_string(),
                name: None,
                type_name: "NatsSource".to_string(),
                transformer: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                config: source_config,
            }],
            wasm: Vec::new(),
            plugin: Vec::new(),
            sinks: vec![SinkSpec {
                heal: None,
                id: "settlements".to_string(),
                name: None,
                type_name: "PostgresSink".to_string(),
                transformer: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                config: sink_config,
            }],
            links: vec![super::super::config::LinkSpec {
                from: "orders-in".to_string(),
                to: "settlements".to_string(),
                branch: None,
            }],
            dlq: None,
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
            variables: HashMap::new(),
        }
    }

    fn order_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// A `BuiltNode` backed by a cheap `ChannelSource`/`ChannelSink`/native
    /// `Pipeline`, but carrying whatever `id`/`type_name` the caller declares:
    /// `node_view` selects `detail_for`'s allowlist purely off `type_name`, so
    /// the concrete Rust type underneath never needs to be the real connector.
    fn built_source(id: &str, type_name: &str) -> BuiltNode {
        use saci_connector_channel::ChannelSource;
        let (_tx, src) = ChannelSource::new(order_schema(), 1);
        BuiltNode {
            id: id.to_string(),
            name: None,
            type_name: type_name.to_string(),
            component: Some("Order"),
            kind: BuiltNodeKind::Source(Box::new(src)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        }
    }

    fn built_sink(id: &str, type_name: &str) -> BuiltNode {
        use saci_connector_channel::ChannelSink;
        let (sink, _rx) = ChannelSink::new(order_schema(), 1);
        BuiltNode {
            id: id.to_string(),
            name: None,
            type_name: type_name.to_string(),
            component: Some("Order"),
            kind: BuiltNodeKind::Sink(Box::new(sink)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        }
    }

    fn built_processor(id: &str, artifact: Option<&str>) -> BuiltNode {
        BuiltNode {
            id: id.to_string(),
            name: Some("Validator".to_string()),
            type_name: "wasm".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(Pipeline::new(id)),
                kind: "wasm",
            },
            downstream: Vec::new(),
            artifact: artifact.map(str::to_string),
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        }
    }

    #[test]
    fn nodes_and_edges_match_the_declared_io() {
        let workflow = workflow_with_io(default_config_value(), default_config_value());
        let config = service_config(workflow);
        let mut source = built_source("orders-in", "NatsSource");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = built_sink("settlements", "PostgresSink");
        let nodes = [source, sink];
        let topology = build_topology(&config, &[nodes.as_slice()], 1);

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
            built_sink("out_high", "PostgresSink"),
            built_sink("out_low", "PostgresSink"),
        ];
        let topology = build_topology(&config, &[nodes.as_slice()], 1);

        let branches: Vec<Option<&str>> = topology.workflows[0]
            .edges
            .iter()
            .map(|edge| edge.branch.as_deref())
            .collect();
        assert_eq!(branches, vec![Some("high"), Some("low")]);
    }

    fn default_config_value() -> ConfigValue {
        ConfigValue::Object(saci_config::ConfigMap::new())
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
        let mut source = built_source("orders-in", "NatsSource");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = built_sink("settlements", "PostgresSink");
        let nodes = [source, sink];
        let topology = build_topology(&config, &[nodes.as_slice()], 1);
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

    #[test]
    fn processor_node_carries_a_populated_runtime_and_source_sink_do_not() {
        let workflow = workflow_with_io(default_config_value(), default_config_value());
        let config = service_config(workflow);
        let mut source = built_source("orders-in", "NatsSource");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let mut processor = built_processor("validate", Some("build/validate.wasm"));
        processor.downstream.push(BuiltEdge {
            node: 2,
            branch: None,
        });
        let sink = built_sink("settlements", "PostgresSink");
        let nodes = [source, processor, sink];
        let topology = build_topology(&config, &[nodes.as_slice()], 1);

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
        use saci_core::windows::WindowSpec;

        let workflow = workflow_with_io(default_config_value(), default_config_value());
        let config = service_config(workflow);
        let mut processor = built_processor("windowed", Some("build/windowed.wasm"));
        processor.window = Some(WindowConfig {
            spec: WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 0,
            },
            time_field: "timestamp_ms".to_string(),
            key_fields: vec!["category".to_string()],
            allowed_lateness_ms: 5_000,
        });
        let nodes = [processor];
        let topology = build_topology(&config, &[nodes.as_slice()], 1);

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
            transformers: Vec::new(),
            sources: vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "orders_in".to_string(),
                name: None,
                type_name: "FileSource".to_string(),
                transformer: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                config: default_config_value(),
            }],
            wasm: Vec::new(),
            plugin: Vec::new(),
            sinks: vec![SinkSpec {
                heal: None,
                id: "bridge_out".to_string(),
                name: None,
                type_name: "ChannelSink".to_string(),
                transformer: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                config: cfg("name \"bridge\""),
            }],
            links: vec![super::super::config::LinkSpec {
                from: "orders_in".to_string(),
                to: "bridge_out".to_string(),
                branch: None,
            }],
            dlq: None,
        };
        let consumer = WorkflowSpec {
            id: "consumer".to_string(),
            name: None,
            transformers: Vec::new(),
            sources: vec![SourceSpec {
                heal: None,
                flow_control: None,
                id: "bridge_in".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                transformer: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                config: cfg("name \"bridge\""),
            }],
            wasm: Vec::new(),
            plugin: Vec::new(),
            sinks: vec![SinkSpec {
                heal: None,
                id: "orders_out".to_string(),
                name: None,
                type_name: "FileSink".to_string(),
                transformer: None,
                component: "Order".to_string(),
                retry: RetryConfig::default(),
                config: default_config_value(),
            }],
            links: vec![super::super::config::LinkSpec {
                from: "bridge_in".to_string(),
                to: "orders_out".to_string(),
                branch: None,
            }],
            dlq: None,
        };
        let config = service_config_many(vec![producer, consumer]);

        let mut source = built_source("orders_in", "FileSource");
        source.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let bridge_out = built_sink("bridge_out", "ChannelSink");
        let producer_nodes = [source, bridge_out];

        let mut bridge_in = built_source("bridge_in", "ChannelSource");
        bridge_in.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let orders_out = built_sink("orders_out", "FileSink");
        let consumer_nodes = [bridge_in, orders_out];

        let topology = build_topology(
            &config,
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
