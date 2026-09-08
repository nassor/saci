//! Load-time semantic validation for assembled services.
//!
//! [`validate_workflow_graph`] runs once every node is built, catching a
//! config/runtime mismatch before the first pipeline iteration:
//! [`validate_schema_fingerprint`] runs separately, in cluster mode only,
//! catching a runtime/persisted-state mismatch.
//!
//! [`validate_build_capabilities`] runs before either, on the config alone.
//! The config schema is the same in every build, so a file may name a host
//! this binary does not carry: a `wasm` or `plugin` node, or `mode "cluster"`.
//! This is the gate that answers "will this binary run this config" with the
//! `--features` flag that would, instead of leaving the refusal to whichever
//! later site happens to reach for the missing host first.

use std::collections::HashSet;

use arrow_schema::Fields;
use saci_core::SaciResult;
use saci_core::error::SaciError;

use crate::dataset::Dataset;

#[cfg(test)]
use super::builder::BuiltEdge;
use super::builder::{BuiltNode, BuiltNodeKind};
use super::config::{ServiceConfig, ServiceMode};
use super::factories::{
    missing_cluster_host_error, missing_plugin_host_error, missing_wasm_host_error,
    missing_windows_engine_error,
};

/// Verify every link can actually carry rows: matching components and
/// field-for-field identical Arrow schemas at both ends.
///
/// Per edge, by the two node kinds it connects:
///
/// - **source → processor**: the source's component must be one the
///   processor declares, and the two schemas for it must have equal
///   `fields()`.
/// - **source → sink**: the source's and the sink's schemas must have equal
///   `fields()`: no processor sits between them, so the bytes pass straight
///   through.
/// - **processor → processor**: the upstream's declared components must be a
///   superset of the downstream's, and every shared component's schema must
///   have equal `fields()` on both sides.
/// - **processor → sink**: the sink's component must be one the upstream
///   declares, and the two schemas for it must have equal `fields()`.
///
/// An empty `declared_components()` on either side of an edge opts that edge
/// out of every check above: a runtime that describes its components lazily,
/// or a test pipeline with none, does not participate in the comparison.
///
/// Finally, every processor with at least one inbound edge must have every
/// declared component delivered **or produced**. A processor inbound edge
/// delivers the whole set (the superset rule above guarantees it); when every
/// inbound edge is a source edge instead, each declared component must be
/// delivered by some source or consumed by some outbound edge: a sink's
/// component or a downstream processor's declared set. A component that is
/// neither delivered nor consumed by any outbound edge is a dead declaration
/// and an error. The produced case is what lets a windowing processor declare
/// the result component its outbound sinks read, which no inbound stream
/// carries. A processor with no inbound edge is an entry point and is exempt:
/// it starts from an empty dataset with every component at zero rows, which is
/// self-consistent.
///
/// Two sources feeding the same component of one processor is allowed: only
/// one component grows, so the dataset stays consistent. Two sources feeding
/// *different* components of one processor is allowed by the coverage rule
/// but only stays consistent while the two produce equal row counts per
/// iteration; an unequal iteration surfaces as the guest's existing
/// `Dataset::read_ipc` row-count error naming the component. That case is not
/// statically decidable and is not checked here.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] naming the workflow, the link and the
/// offending component on the first violation.
pub fn validate_workflow_graph(workflow_id: &str, nodes: &[BuiltNode]) -> SaciResult<()> {
    // Precomputed once per processor node, so an edge touching a fan-out or
    // fan-in node several times does not repeat the runtime call.
    let declared: Vec<Option<Vec<String>>> = nodes
        .iter()
        .map(|node| match &node.kind {
            BuiltNodeKind::Processor { runtime, .. } => Some(
                runtime
                    .declared_components()
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    let templates: Vec<Option<Dataset>> = nodes
        .iter()
        .map(|node| match &node.kind {
            BuiltNodeKind::Processor { runtime, .. } => Some(runtime.template_dataset()),
            _ => None,
        })
        .collect();

    let mut inbound: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for (i, node) in nodes.iter().enumerate() {
        for edge in &node.downstream {
            inbound[edge.node].push(i);
        }
    }

    for (i, node) in nodes.iter().enumerate() {
        for edge in &node.downstream {
            let d = edge.node;
            validate_edge(workflow_id, nodes, &declared, &templates, i, d)?;
        }
    }

    for (i, node) in nodes.iter().enumerate() {
        if matches!(node.kind, BuiltNodeKind::Processor { .. }) {
            validate_processor_coverage(workflow_id, nodes, &declared, &inbound, i)?;
        }
    }

    #[cfg(feature = "windows")]
    for (i, node) in nodes.iter().enumerate() {
        if let Some(window) = &node.window {
            validate_window(
                workflow_id,
                nodes,
                &declared,
                &templates,
                &inbound,
                i,
                window,
            )?;
        }
    }

    Ok(())
}

/// Verify a windowed processor's declaration against its inbound streams: every
/// component delivered to the node must carry the window's `time_field`, and
/// that field must be a type the host watermark tracker can read (Int64
/// milliseconds or an Arrow timestamp type).
///
/// The window block is a promise about the merged stream: the host advances
/// the node's watermark from the time column of all inbound data, so a stream
/// that lacks the field would silently never advance it. A processor with no
/// inbound edge (an entry point) skips the check: its data comes from its own
/// systems, and the host only tracks watermarks for nodes with inbound data.
///
/// A runtime that declares no components opts the node out of the check, the
/// same way an empty `declared_components()` opts its edges out of the schema
/// checks.
#[cfg(feature = "windows")]
fn validate_window(
    workflow_id: &str,
    nodes: &[BuiltNode],
    declared: &[Option<Vec<String>>],
    templates: &[Option<Dataset>],
    inbound: &[Vec<usize>],
    idx: usize,
    window: &super::config::WindowConfig,
) -> SaciResult<()> {
    use arrow_schema::DataType;

    let ins = &inbound[idx];
    if ins.is_empty() {
        return Ok(());
    }
    if declared[idx].as_ref().is_none_or(Vec::is_empty) {
        return Ok(());
    }

    // Every component the inbound links deliver, paired with the schema the
    // delivering side declared. A processor inbound delivers the whole
    // downstream declared set (the superset rule); a source inbound delivers
    // its one component.
    let mut delivered: Vec<(String, arrow_schema::Fields)> = Vec::new();
    for &i in ins {
        match &nodes[i].kind {
            BuiltNodeKind::Source(source) => {
                let component = nodes[i]
                    .component
                    .expect("a source node always declares its component")
                    .to_string();
                delivered.push((component, source.schema().fields().clone()));
            }
            BuiltNodeKind::Processor { .. } => {
                let up_declared = declared[i].as_ref().expect("processor node");
                let up_template = templates[i].as_ref().expect("processor node");
                for component in up_declared {
                    let Some(fields) = fields_of(up_template, component) else {
                        continue;
                    };
                    delivered.push((component.clone(), fields));
                }
            }
            BuiltNodeKind::Sink(_) => unreachable!("a sink is never a link source"),
        }
    }

    for (component, fields) in &delivered {
        let Some(field) = fields
            .iter()
            .find(|f| f.name() == window.time_field.as_str())
        else {
            return Err(SaciError::configuration(format!(
                "workflow '{workflow_id}': processor '{}' declares window time_field '{}' \
                 but component '{component}' delivered by an inbound link has no such field",
                nodes[idx].id, window.time_field
            )));
        };
        match field.data_type() {
            DataType::Int64 | DataType::Timestamp(_, _) => {}
            other => {
                return Err(SaciError::configuration(format!(
                    "workflow '{workflow_id}': processor '{}' declares window time_field \
                     '{}' on component '{component}' as {other:?}, but the host watermark \
                     tracker reads Int64 milliseconds or an Arrow timestamp type",
                    nodes[idx].id, window.time_field
                )));
            }
        }
    }

    Ok(())
}

/// The Arrow fields registered for `component` in `dataset`, if any.
///
/// Compares `fields()`, not whole `Schema`s: that is exactly what
/// `Dataset::append_record_batch` compares at run time, and schema metadata
/// legitimately differs between a source's and a processor's idea of the same
/// component.
fn fields_of(dataset: &Dataset, component: &str) -> Option<Fields> {
    dataset.schemas().get(component).map(|s| s.fields().clone())
}

fn validate_edge(
    workflow_id: &str,
    nodes: &[BuiltNode],
    declared: &[Option<Vec<String>>],
    templates: &[Option<Dataset>],
    from: usize,
    to: usize,
) -> SaciResult<()> {
    let from_node = &nodes[from];
    let to_node = &nodes[to];

    match (&from_node.kind, &to_node.kind) {
        (BuiltNodeKind::Source(source), BuiltNodeKind::Processor { .. }) => {
            let component = from_node
                .component
                .expect("a source node always declares its component");
            let proc_declared = declared[to].as_ref().expect("processor node");
            if proc_declared.is_empty() {
                return Ok(());
            }
            if !proc_declared.iter().any(|c| c == component) {
                return Err(SaciError::configuration(format!(
                    "workflow '{workflow_id}': link '{}' -> '{}': processor '{}' does not \
                     declare component '{component}', which source '{}' produces",
                    from_node.id, to_node.id, to_node.id, from_node.id
                )));
            }
            let proc_template = templates[to].as_ref().expect("processor node");
            if let Some(proc_fields) = fields_of(proc_template, component)
                && source.schema().fields() != &proc_fields
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{workflow_id}': link '{}' -> '{}': component '{component}' \
                     schema differs between source '{}' and processor '{}'",
                    from_node.id, to_node.id, from_node.id, to_node.id
                )));
            }
        }
        (BuiltNodeKind::Source(source), BuiltNodeKind::Sink(sink)) => {
            if source.schema().fields() != sink.schema().fields() {
                return Err(SaciError::configuration(format!(
                    "workflow '{workflow_id}': link '{}' -> '{}': source and sink schemas \
                     differ; a source-to-sink link has no processor to reconcile them",
                    from_node.id, to_node.id
                )));
            }
        }
        (BuiltNodeKind::Processor { .. }, BuiltNodeKind::Processor { .. }) => {
            let up = declared[from].as_ref().expect("processor node");
            let down = declared[to].as_ref().expect("processor node");
            if up.is_empty() || down.is_empty() {
                return Ok(());
            }
            let up_template = templates[from].as_ref().expect("processor node");
            let down_template = templates[to].as_ref().expect("processor node");
            for component in down {
                if !up.iter().any(|c| c == component) {
                    return Err(SaciError::configuration(format!(
                        "workflow '{workflow_id}': link '{}' -> '{}': processor '{}' declares \
                         component '{component}', which upstream processor '{}' does not; a \
                         processor-to-processor link must deliver every component the \
                         downstream processor declares",
                        from_node.id, to_node.id, to_node.id, from_node.id
                    )));
                }
                let up_fields = fields_of(up_template, component);
                let down_fields = fields_of(down_template, component);
                if up_fields != down_fields {
                    return Err(SaciError::configuration(format!(
                        "workflow '{workflow_id}': link '{}' -> '{}': component '{component}' \
                         schema differs between processor '{}' and processor '{}'",
                        from_node.id, to_node.id, from_node.id, to_node.id
                    )));
                }
            }
        }
        (BuiltNodeKind::Processor { .. }, BuiltNodeKind::Sink(sink)) => {
            let component = to_node
                .component
                .expect("a sink node always declares its component");
            let up = declared[from].as_ref().expect("processor node");
            if up.is_empty() {
                return Ok(());
            }
            if !up.iter().any(|c| c == component) {
                return Err(SaciError::configuration(format!(
                    "workflow '{workflow_id}': link '{}' -> '{}': processor '{}' does not \
                     declare component '{component}', which sink '{}' reads",
                    from_node.id, to_node.id, from_node.id, to_node.id
                )));
            }
            let up_template = templates[from].as_ref().expect("processor node");
            if let Some(up_fields) = fields_of(up_template, component)
                && sink.schema().fields() != &up_fields
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{workflow_id}': link '{}' -> '{}': component '{component}' \
                     schema differs between processor '{}' and sink '{}'",
                    from_node.id, to_node.id, from_node.id, to_node.id
                )));
            }
        }
        // Every other combination (a source or sink as a link target/source
        // respectively) is rejected by `WorkflowSpec::validate`'s edge-kind
        // matrix before a `BuiltNode` graph is ever constructed; reaching one
        // here means the config was built without validating it first.
        _ => {
            return Err(SaciError::configuration(format!(
                "workflow '{workflow_id}': link '{}' -> '{}' connects node kinds a validated \
                 workflow cannot produce; validate the config before building it",
                from_node.id, to_node.id
            )));
        }
    }

    Ok(())
}

fn validate_processor_coverage(
    workflow_id: &str,
    nodes: &[BuiltNode],
    declared: &[Option<Vec<String>>],
    inbound: &[Vec<usize>],
    idx: usize,
) -> SaciResult<()> {
    let ins = &inbound[idx];
    if ins.is_empty() {
        // No inbound edge: an entry point, starting from an empty dataset.
        return Ok(());
    }
    let declared_components = declared[idx].as_ref().expect("processor node");
    if declared_components.is_empty() {
        return Ok(());
    }

    // A processor inbound edge already delivers the whole declared set (the
    // superset rule in `validate_edge` guarantees it), so only an
    // all-sources fan-in needs its union checked against the declared set.
    let has_processor_inbound = ins
        .iter()
        .any(|&i| matches!(nodes[i].kind, BuiltNodeKind::Processor { .. }));
    if has_processor_inbound {
        return Ok(());
    }

    let delivered: HashSet<&str> = ins.iter().filter_map(|&i| nodes[i].component).collect();
    // A declared component is covered when a source delivers it *or* when the
    // processor itself produces it: a windowing processor declares the result
    // component its outbound sinks read (`WindowTotal`), which no inbound
    // stream carries. "Produced" means an outbound edge names it: a sink
    // node's component, or a downstream processor's declared set. A component
    // that is neither delivered nor consumed by any outbound edge is still a
    // dead declaration and still an error.
    let mut produced: HashSet<&str> = HashSet::new();
    for edge in &nodes[idx].downstream {
        match &nodes[edge.node].kind {
            BuiltNodeKind::Sink(_) => {
                if let Some(component) = nodes[edge.node].component {
                    produced.insert(component);
                }
            }
            BuiltNodeKind::Processor { .. } => {
                if let Some(names) = declared[edge.node].as_ref() {
                    produced.extend(names.iter().map(String::as_str));
                }
            }
            BuiltNodeKind::Source(_) => {}
        }
    }
    for component in declared_components {
        if !delivered.contains(component.as_str()) && !produced.contains(component.as_str()) {
            return Err(SaciError::configuration(format!(
                "workflow '{workflow_id}': processor '{}' declares component '{component}' but \
                 no inbound link delivers it and no outbound edge consumes it",
                nodes[idx].id
            )));
        }
    }
    Ok(())
}

/// Verify that the runtime's Arrow schema fingerprint matches the one recorded
/// by this node's persisted checkpoints.
///
/// `runtime` is `runtime.template_dataset().schemas().fingerprint()`, the same
/// `u32` a WASM processor reports as `pipeline-descriptor.schema-fingerprint` (the
/// processor formats it as 8-char hex; the value is identical). `persisted` is
/// [`CheckpointStore::persisted_schema_id`](crate::distributed::checkpoint::CheckpointStore::persisted_schema_id),
/// which is `None` on a node with no state yet.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when both are present and differ: the
/// persisted checkpoints describe a different schema shape than the pipeline
/// about to resume from them, so resuming would silently mix layouts.
pub fn validate_schema_fingerprint(runtime: u32, persisted: Option<u32>) -> SaciResult<()> {
    match persisted {
        None => Ok(()),
        Some(stored) if stored == runtime => Ok(()),
        Some(stored) => Err(SaciError::configuration(format!(
            "schema fingerprint mismatch: the pipeline declares {runtime:08x} but this \
             node's persisted checkpoints were written with {stored:08x}. The deployed \
             pipeline's component schemas changed. Either restore the previous pipeline \
             or clear node.data_dir before starting with the new schema."
        ))),
    }
}

/// Refuse a config declaring a capability this binary was not built with,
/// naming the feature that supplies it.
///
/// Four declarations name a build capability rather than a registry entry: a
/// `wasm` node needs `--features wasm`, a `plugin` node `--features plugin`,
/// a `window` block on either `--features windows`, and `mode "cluster"`
/// `--features service-cluster`. All four parse in every build,
/// deliberately, so the refusal can say which flag is missing instead of
/// reporting an unknown key or a mode that quietly does nothing.
///
/// The checks read `cfg!` rather than carrying `#[cfg]` arms, so every
/// producer stays compiled and reachable in every build and the whole
/// function is one constant-folded branch per capability in a build that has
/// them all.
///
/// Called by the binary's `validate` and `serve` on the loaded config, before
/// anything is built. `ServiceConfig::load` deliberately does not call it: a
/// library embedder parsing a config to inspect it is not asking whether
/// this binary can run it.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] naming the first declaration whose
/// host is absent, the host it needs, and the `--features` flag that carries
/// it.
pub fn validate_build_capabilities(config: &ServiceConfig) -> SaciResult<()> {
    for workflow in &config.workflows {
        if !cfg!(feature = "wasm")
            && let Some(spec) = workflow.wasm.first()
        {
            return Err(missing_wasm_host_error(&spec.id, &workflow.id));
        }
        if !cfg!(feature = "plugin")
            && let Some(spec) = workflow.plugin.first()
        {
            return Err(missing_plugin_host_error(&spec.id, &workflow.id));
        }
        if !cfg!(feature = "windows") {
            if let Some(spec) = workflow.wasm.iter().find(|s| s.window.is_some()) {
                return Err(missing_windows_engine_error("wasm", &spec.id, &workflow.id));
            }
            if let Some(spec) = workflow.plugin.iter().find(|s| s.window.is_some()) {
                return Err(missing_windows_engine_error(
                    "plugin",
                    &spec.id,
                    &workflow.id,
                ));
            }
        }
    }

    if !cfg!(feature = "service-cluster") && matches!(config.mode, ServiceMode::Cluster { .. }) {
        return Err(missing_cluster_host_error());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Pipeline;
    use arrow_schema::{DataType, Field, Schema};
    use saci_core::component::Component;
    use std::sync::Arc;

    struct Order;
    impl Component for Order {
        fn name() -> &'static str {
            "Order"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        }
    }

    struct OrderAlt;
    impl Component for OrderAlt {
        fn name() -> &'static str {
            "Order"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]))
        }
    }

    fn order_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn source_node(id: &str, component: &'static str, schema: Arc<Schema>) -> BuiltNode {
        use saci_connector_channel::ChannelSource;
        let (_tx, src) = ChannelSource::new(schema, 1);
        BuiltNode {
            id: id.to_string(),
            name: None,
            type_name: "ChannelSource".to_string(),
            component: Some(component),
            kind: BuiltNodeKind::Source(Box::new(src)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        }
    }

    fn sink_node(id: &str, component: &'static str, schema: Arc<Schema>) -> BuiltNode {
        use saci_connector_channel::ChannelSink;
        let (sink, _rx) = ChannelSink::new(schema, 1);
        BuiltNode {
            id: id.to_string(),
            name: None,
            type_name: "ChannelSink".to_string(),
            component: Some(component),
            kind: BuiltNodeKind::Sink(Box::new(sink)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        }
    }

    fn processor_node(id: &str, pipeline: Pipeline) -> BuiltNode {
        BuiltNode {
            id: id.to_string(),
            name: None,
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(pipeline),
                kind: "native",
            },
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        }
    }

    #[test]
    fn source_straight_to_sink_with_matching_schemas_is_valid() {
        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = sink_node("out", "Order", order_schema());
        validate_workflow_graph("w", &[src, sink]).expect("matching schemas");
    }

    #[test]
    fn source_straight_to_sink_with_differing_schemas_is_rejected() {
        let other_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = sink_node("out", "Order", other_schema);
        let err = validate_workflow_graph("w", &[src, sink]).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schemas"), "{err}");
    }

    #[test]
    fn source_to_processor_missing_declared_component_is_rejected() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        // The source claims to feed "Nonexistent", which the processor never declares.
        let mut src = source_node("in", "Nonexistent", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let proc = processor_node("p", pipeline);
        let err = validate_workflow_graph("w", &[src, proc]).unwrap_err();
        assert!(err.message().contains("Nonexistent"), "{err}");
    }

    #[test]
    fn source_to_processor_with_matching_component_and_schema_is_valid() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let proc = processor_node("p", pipeline);
        validate_workflow_graph("w", &[src, proc]).expect("matching component and schema");
    }

    #[test]
    fn source_to_processor_with_schema_mismatch_is_rejected() {
        let mut pipeline = Pipeline::new("p");
        pipeline
            .data_mut()
            .register_component::<OrderAlt>()
            .unwrap();
        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let proc = processor_node("p", pipeline);
        let err = validate_workflow_graph("w", &[src, proc]).unwrap_err();
        assert!(err.message().contains("schema"), "{err}");
    }

    #[test]
    fn processor_to_processor_superset_is_valid() {
        let mut upstream = Pipeline::new("up");
        upstream.data_mut().register_component::<Order>().unwrap();
        let mut downstream = Pipeline::new("down");
        downstream.data_mut().register_component::<Order>().unwrap();

        let mut up_node = processor_node("up", upstream);
        up_node.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let down_node = processor_node("down", downstream);
        validate_workflow_graph("w", &[up_node, down_node]).expect("identical single component");
    }

    #[test]
    fn processor_to_processor_missing_component_is_rejected() {
        let mut upstream = Pipeline::new("up");
        // Non-empty but not "Order": an empty `declared_components()` opts
        // the edge out of every check, so upstream must declare something
        // else to exercise the superset rule rather than the empty-set one.
        upstream.data_mut().register_raw_component(
            "Invoice",
            Arc::new(Schema::new(vec![Field::new(
                "total",
                DataType::Float64,
                false,
            )])),
        );
        let mut downstream = Pipeline::new("down");
        downstream.data_mut().register_component::<Order>().unwrap();

        let mut up_node = processor_node("up", upstream);
        up_node.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let down_node = processor_node("down", downstream);
        let err = validate_workflow_graph("w", &[up_node, down_node]).unwrap_err();
        assert!(err.message().contains("Order"), "{err}");
    }

    #[test]
    fn processor_to_sink_with_matching_component_is_valid() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        let mut proc = processor_node("p", pipeline);
        proc.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = sink_node("out", "Order", order_schema());
        validate_workflow_graph("w", &[proc, sink]).expect("matching component and schema");
    }

    #[test]
    fn processor_to_sink_missing_component_is_rejected() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        let mut proc = processor_node("p", pipeline);
        proc.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let sink = sink_node("out", "Invoice", order_schema());
        let err = validate_workflow_graph("w", &[proc, sink]).unwrap_err();
        assert!(err.message().contains("Invoice"), "{err}");
    }

    #[test]
    fn entry_point_processor_with_no_inbound_edge_needs_no_coverage() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        let proc = processor_node("p", pipeline);
        validate_workflow_graph("w", &[proc]).expect("entry point is exempt from coverage");
    }

    #[test]
    fn processor_fed_by_a_source_missing_a_declared_component_is_rejected() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        pipeline.data_mut().register_raw_component(
            "Invoice",
            Arc::new(Schema::new(vec![Field::new(
                "total",
                DataType::Float64,
                false,
            )])),
        );

        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let proc = processor_node("p", pipeline);
        let err = validate_workflow_graph("w", &[src, proc]).unwrap_err();
        assert!(
            err.message().contains("Invoice") && err.message().contains("no inbound link"),
            "{err}"
        );
    }

    #[test]
    fn empty_declared_components_opts_the_edge_out_of_every_check() {
        let pipeline = Pipeline::new("p");
        let other_schema = Arc::new(Schema::new(vec![Field::new(
            "whatever",
            DataType::Boolean,
            false,
        )]));
        let mut src = source_node("in", "Anything", other_schema);
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let proc = processor_node("p", pipeline);
        validate_workflow_graph("w", &[src, proc])
            .expect("a runtime with no declared components opts out");
    }

    #[test]
    fn test_fingerprint_passes_on_a_fresh_node() {
        validate_schema_fingerprint(0xdead_beef, None)
            .expect("a node with no persisted state has nothing to conflict with");
    }

    #[test]
    fn test_fingerprint_passes_when_it_matches() {
        validate_schema_fingerprint(0xdead_beef, Some(0xdead_beef)).expect("same shape resumes");
    }

    #[test]
    fn test_fingerprint_mismatch_is_rejected() {
        let err = validate_schema_fingerprint(0x0000_0001, Some(0x0000_0002))
            .expect_err("a redeployed pipeline must not resume against foreign state");
        assert_eq!(err.category(), "configuration");
        let msg = err.to_string();
        assert!(
            msg.contains("00000001"),
            "must name the pipeline's own: {msg}"
        );
        assert!(
            msg.contains("00000002"),
            "must name the persisted one: {msg}"
        );
        assert!(
            msg.contains("data_dir"),
            "must tell the operator what to do: {msg}"
        );
    }

    /// The result-component schema the coverage tests reuse.
    fn window_total_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "window_id",
            DataType::Int64,
            false,
        )]))
    }

    /// A source-fed processor may declare a component that no source delivers
    /// when an outbound sink consumes it: the windowing case, where N input
    /// rows reduce to M result rows in a separate component.
    #[test]
    fn output_component_consumed_by_a_sink_is_covered() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        pipeline
            .data_mut()
            .register_raw_component("WindowTotal", window_total_schema());

        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let mut proc = processor_node("p", pipeline);
        proc.downstream.push(BuiltEdge {
            node: 2,
            branch: None,
        });
        let sink = sink_node("out", "WindowTotal", window_total_schema());
        validate_workflow_graph("w", &[src, proc, sink])
            .expect("an outbound sink covers the produced component");
    }

    /// A declared component that is neither delivered nor consumed by any
    /// outbound edge is still a dead declaration and still an error.
    #[test]
    fn component_neither_delivered_nor_consumed_is_rejected() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        pipeline.data_mut().register_raw_component(
            "Ghost",
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        );

        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let proc = processor_node("p", pipeline);
        let err = validate_workflow_graph("w", &[src, proc]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Ghost"), "got: {msg}");
        assert!(msg.contains("no inbound link delivers it"), "got: {msg}");
    }

    // ── Windowing validation ────────────────────────────────────────────────

    #[cfg(feature = "windows")]
    fn timestamp_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    #[cfg(feature = "windows")]
    fn window() -> super::super::config::WindowConfig {
        use saci_core::windows::WindowSpec;
        super::super::config::WindowConfig {
            spec: WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 0,
            },
            time_field: "timestamp_ms".to_string(),
            key_fields: Vec::new(),
            allowed_lateness_ms: 0,
        }
    }

    /// A processor whose window time field is missing from a delivered
    /// component must be rejected at load time: the host would never be able
    /// to advance the node's watermark from that stream.
    #[cfg(feature = "windows")]
    #[test]
    fn window_time_field_missing_from_a_source_component_is_rejected() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        let mut src = source_node("in", "Order", order_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let mut proc = processor_node("p", pipeline);
        proc.window = Some(window());
        let err = validate_workflow_graph("w", &[src, proc]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("timestamp_ms"), "got: {msg}");
        assert!(msg.contains("no such field"), "got: {msg}");
    }

    /// Every delivered component must carry the window's time field; when all
    /// of them do, the node validates.
    #[cfg(feature = "windows")]
    #[test]
    fn window_time_field_present_on_all_delivered_components_is_valid() {
        let mut pipeline = Pipeline::new("p");
        pipeline
            .data_mut()
            .register_raw_component("Order", timestamp_schema());
        let mut src = source_node("in", "Order", timestamp_schema());
        src.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let mut proc = processor_node("p", pipeline);
        proc.window = Some(window());
        validate_workflow_graph("w", &[src, proc]).expect("time field present everywhere");
    }

    /// A processor-to-processor fan-in: the window's time field is checked
    /// against the upstream's declared components, not just against sources.
    #[cfg(feature = "windows")]
    #[test]
    fn window_time_field_is_checked_on_processor_inbound_components() {
        let mut upstream = Pipeline::new("up");
        upstream
            .data_mut()
            .register_raw_component("Order", timestamp_schema());
        let mut down = Pipeline::new("down");
        down.data_mut()
            .register_raw_component("Order", timestamp_schema());

        let mut up_node = processor_node("up", upstream);
        up_node.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let mut down_node = processor_node("down", down);
        down_node.window = Some(window());
        validate_workflow_graph("w", &[up_node, down_node])
            .expect("processor-delivered component carries the time field");

        // The same shape without the time field is rejected: both sides
        // agree on Order's schema (so the schema check passes), but neither
        // carries the window's time field.
        let mut upstream = Pipeline::new("up");
        upstream.data_mut().register_component::<Order>().unwrap();
        let mut down = Pipeline::new("down");
        down.data_mut().register_component::<Order>().unwrap();

        let mut up_node = processor_node("up", upstream);
        up_node.downstream.push(BuiltEdge {
            node: 1,
            branch: None,
        });
        let mut down_node = processor_node("down", down);
        down_node.window = Some(window());
        let err = validate_workflow_graph("w", &[up_node, down_node]).unwrap_err();
        assert!(err.to_string().contains("timestamp_ms"), "got: {err}");
    }

    /// An entry-point processor (no inbound links) skips the time-field
    /// check: its data comes from its own systems.
    #[cfg(feature = "windows")]
    #[test]
    fn window_on_an_entry_point_processor_needs_no_time_field() {
        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register_component::<Order>().unwrap();
        let mut proc = processor_node("p", pipeline);
        proc.window = Some(window());
        validate_workflow_graph("w", &[proc]).expect("no inbound stream, no check");
    }
}
