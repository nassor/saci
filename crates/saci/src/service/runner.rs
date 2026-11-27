//! What the batch and stream runners share.
//!
//! [`standalone`](super::standalone) and [`stream`](super::stream) pace and
//! pull differently, and each keeps its own loop, its own source pull and its
//! own controller observation. Everything the two do identically lives here:
//!
//! - [`Nodes`], the parallel per-node vectors a workflow is split into, and
//!   [`whole_arrivals`], which marks the sources that keep every arrival whole.
//! - The fan-out of a source chunk ([`fan_out_source`]), the watermark advance
//!   of a windowed processor ([`advance_watermark`]) and the delivery of a
//!   processor's output ([`deliver_outbound`]).
//! - The sink side: [`write_staged`], [`finish_sink`], [`finish_all_sinks`],
//!   [`flush_and_finish_all`] and [`report_sink_backlog`]; then
//!   [`finish_all_sources`].
//! - The flow-control log: [`FlowEpisode`], [`record_flow_decision`],
//!   [`log_flow_start`] and [`log_flow_finish`].
//!
//! Every helper takes the [`Nodes`] fields it needs as separate borrows. A
//! runner destructures its `Nodes` into locals once, so the borrow checker
//! sees disjoint fields (a processor step borrows `runtimes` and `datasets`
//! side by side, and splits `datasets` into the processor's own and a
//! downstream one) and no helper clones a node's vectors.
//!
//! Where the runners word an event differently they say so through a
//! parameter ([`PassLog`], the `stream` argument of the sink helpers) rather
//! than through a second copy of the code.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow_array::RecordBatch;

use crate::dataset::Dataset;
use crate::inspector::Inspector;
use crate::inspector_wire::{FlowDecision, FlowDecisionKind};
use crate::io::{Sink, Source};
use crate::metrics::{ProcessorMetrics, SinkMetrics, SourceMetrics};
use crate::runtime::PipelineRuntime;

use super::builder::{BuiltEdge, BuiltNode, BuiltNodeKind};
use super::dlq::DeadLetterQueue;
use super::flow::{FlowAdjustment, FlowCause, FlowController, FlowSettings};
use super::sampling::FLOW_CONTROL_TARGET;
use super::standalone::{NodeStats, WorkflowStats};
#[cfg(feature = "windows")]
use super::windowing::WindowTracker;

/// Which of the three roles a node plays while it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeRunKind {
    Source,
    Processor,
    Sink,
}

impl NodeRunKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            NodeRunKind::Source => "source",
            NodeRunKind::Processor => "processor",
            NodeRunKind::Sink => "sink",
        }
    }
}

/// A workflow's nodes as parallel per-node vectors, in declaration
/// (topological) order.
///
/// Parallel vectors, not one `Vec` of trait objects, so the borrow checker
/// sees disjoint fields rather than one `Vec` indexed twice: `runtimes` and
/// `datasets` are different fields, so a processor step borrows them
/// disjointly, and `datasets.split_at_mut(i + 1)` gives a processor's own
/// dataset and a downstream one as two non-overlapping slices. A runner
/// destructures this once into locals of the same names.
pub(super) struct Nodes {
    pub(super) ids: Vec<String>,
    pub(super) components: Vec<Option<&'static str>>,
    pub(super) downstream: Vec<Vec<BuiltEdge>>,
    pub(super) kinds: Vec<NodeRunKind>,
    pub(super) sources: Vec<Option<Box<dyn Source>>>,
    pub(super) runtimes: Vec<Option<Box<dyn PipelineRuntime>>>,
    pub(super) datasets: Vec<Option<Dataset>>,
    pub(super) sinks: Vec<Option<Box<dyn Sink>>>,
    pub(super) node_stats: Vec<NodeStats>,
    /// One bound metrics handle per node, of the node's own kind (`None`
    /// elsewhere), so a write builds no attribute.
    pub(super) source_metrics: Vec<Option<SourceMetrics>>,
    pub(super) processor_metrics: Vec<Option<ProcessorMetrics>>,
    pub(super) sink_metrics: Vec<Option<SinkMetrics>>,
    /// One flag per healed sink node, `None` elsewhere. A sink that just came
    /// back is when a dead letter replay is most likely to land.
    pub(super) recovered: Vec<Option<Arc<AtomicBool>>>,
    /// One watermark tracker per windowed processor node; `None` everywhere
    /// else. The tracker survives across iterations, so the watermark is
    /// monotonic over the whole run, exactly like the guest-side state a
    /// windowed processor keeps in its checkpoint blob.
    #[cfg(feature = "windows")]
    pub(super) trackers: Vec<Option<WindowTracker>>,
    /// The batches staged for each sink node since it last wrote.
    pub(super) staged: Vec<Vec<RecordBatch>>,
}

impl Nodes {
    /// Split a workflow's nodes into their per-node vectors. A processor gets
    /// its runtime's template dataset; every node binds one metrics handle of
    /// its own kind.
    pub(super) fn from_built(nodes: Vec<BuiltNode>) -> Self {
        let n = nodes.len();
        let mut ids: Vec<String> = Vec::with_capacity(n);
        let mut components: Vec<Option<&'static str>> = Vec::with_capacity(n);
        let mut downstream: Vec<Vec<BuiltEdge>> = Vec::with_capacity(n);
        let mut kinds: Vec<NodeRunKind> = Vec::with_capacity(n);
        let mut sources: Vec<Option<Box<dyn Source>>> = Vec::with_capacity(n);
        let mut runtimes: Vec<Option<Box<dyn PipelineRuntime>>> = Vec::with_capacity(n);
        let mut datasets: Vec<Option<Dataset>> = Vec::with_capacity(n);
        let mut sinks: Vec<Option<Box<dyn Sink>>> = Vec::with_capacity(n);
        let mut node_stats: Vec<NodeStats> = Vec::with_capacity(n);
        let mut source_metrics: Vec<Option<SourceMetrics>> = Vec::with_capacity(n);
        let mut processor_metrics: Vec<Option<ProcessorMetrics>> = Vec::with_capacity(n);
        let mut sink_metrics: Vec<Option<SinkMetrics>> = Vec::with_capacity(n);
        let mut recovered: Vec<Option<Arc<AtomicBool>>> = Vec::with_capacity(n);
        #[cfg(feature = "windows")]
        let mut trackers: Vec<Option<WindowTracker>> = Vec::with_capacity(n);

        for node in nodes {
            ids.push(node.id);
            components.push(node.component);
            downstream.push(node.downstream);
            recovered.push(node.heal_recovered);
            #[cfg(feature = "windows")]
            trackers.push(node.window.map(WindowTracker::new));

            let (kind, source, runtime, dataset, sink) = match node.kind {
                BuiltNodeKind::Source(source) => {
                    (NodeRunKind::Source, Some(source), None, None, None)
                }
                BuiltNodeKind::Processor { runtime, .. } => {
                    let dataset = runtime.template_dataset();
                    (
                        NodeRunKind::Processor,
                        None,
                        Some(runtime),
                        Some(dataset),
                        None,
                    )
                }
                BuiltNodeKind::Sink(sink) => (NodeRunKind::Sink, None, None, None, Some(sink)),
            };
            kinds.push(kind);
            {
                let id = ids.last().expect("just pushed");
                let instruments = crate::metrics::instruments();
                source_metrics.push((kind == NodeRunKind::Source).then(|| instruments.source(id)));
                processor_metrics
                    .push((kind == NodeRunKind::Processor).then(|| instruments.processor(id)));
                sink_metrics.push((kind == NodeRunKind::Sink).then(|| instruments.sink(id)));
            }
            sources.push(source);
            runtimes.push(runtime);
            datasets.push(dataset);
            sinks.push(sink);
            node_stats.push(NodeStats {
                id: Arc::from(ids.last().expect("just pushed").as_str()),
                kind: kind.as_str(),
                rows: 0,
                batches: 0,
                errors: 0,
            });
        }

        Self {
            ids,
            components,
            downstream,
            kinds,
            sources,
            runtimes,
            datasets,
            sinks,
            node_stats,
            source_metrics,
            processor_metrics,
            sink_metrics,
            recovered,
            #[cfg(feature = "windows")]
            trackers,
            staged: vec![Vec::new(); n],
        }
    }
}

/// Per node, whether a source on a path to a windowed node must keep every
/// arrival whole.
///
/// The credit still stops the drain pulling the *next* arrival, but the one in
/// hand is never sliced, because a windowed node observes event time at every
/// pass boundary and a credit-derived boundary would put a throughput
/// measurement in its output. See [`windowing`](super::windowing) for the rule
/// and its memory cost. All `false` in a build with no windowing engine.
pub(super) fn whole_arrivals(nodes: &Nodes) -> Vec<bool> {
    #[cfg(feature = "windows")]
    {
        super::windowing::reaches_windowed_node(&nodes.trackers, &nodes.downstream)
    }
    #[cfg(not(feature = "windows"))]
    {
        vec![false; nodes.ids.len()]
    }
}

/// What a runner stamps on the events the shared helpers log for one pass (or,
/// in the stream runner, one item): the workflow, the number the event names
/// as its `iteration`, and the span it hangs from.
///
/// The two runners agree on every event's text and fields; they differ in the
/// number. The batch runner names the pass being run, `stats.iterations + 1`;
/// the stream runner names `stats.iterations`, which it only advances once the
/// item is done.
#[derive(Clone, Copy)]
pub(super) struct PassLog<'a> {
    pub(super) workflow_id: &'a str,
    pub(super) iteration: u64,
    /// The span the events are children of: the pass's `workflow.batch` span,
    /// or, for a stream sink write, the item's `sink.write` span.
    #[cfg(feature = "tracing")]
    pub(super) span: &'a tracing::Span,
}

/// Append one admitted source chunk to every node `downstream[from]` names: a
/// processor's dataset, or a sink's staging buffer.
///
/// A processor that refuses the append costs one `iteration_errors` and is
/// logged naming both node ids; the chunk still reaches the other edges.
#[expect(
    clippy::too_many_arguments,
    reason = "one fan-out over one source's edges; every argument is a distinct borrow the runner owns"
)]
pub(super) fn fan_out_source(
    from: usize,
    downstream: &[Vec<BuiltEdge>],
    component: &str,
    batch: &RecordBatch,
    kinds: &[NodeRunKind],
    datasets: &mut [Option<Dataset>],
    staged: &mut [Vec<RecordBatch>],
    ids: &[String],
    stats: &mut WorkflowStats,
    log: PassLog<'_>,
) {
    #[cfg(not(feature = "tracing"))]
    let _ = (ids, log);
    for edge in &downstream[from] {
        let d = edge.node;
        match kinds[d] {
            NodeRunKind::Processor => {
                if let Err(_e) = datasets[d]
                    .as_mut()
                    .expect("processor node keeps its dataset")
                    .append_record_batch(component, batch.clone())
                {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        parent: log.span,
                        workflow = %log.workflow_id,
                        iteration = log.iteration,
                        from = %ids[from], to = %ids[d], error = %_e,
                        "fan-out append error (continuing)"
                    );
                    stats.iteration_errors += 1;
                }
            }
            NodeRunKind::Sink => staged[d].push(batch.clone()),
            NodeRunKind::Source => {
                unreachable!("a source is never a link target")
            }
        }
    }
}

/// Advance a windowed processor's watermark from everything this pass
/// delivered into its dataset, before the runtime sees the batch.
///
/// The fan-in merge is complete once every upstream node has run: sources
/// appended their batches directly and upstream processors forwarded their
/// datasets. Does nothing for a processor that declared no window.
#[cfg(feature = "windows")]
pub(super) fn advance_watermark(
    i: usize,
    trackers: &mut [Option<WindowTracker>],
    datasets: &mut [Option<Dataset>],
    ids: &[String],
    metrics: &ProcessorMetrics,
    stats: &mut WorkflowStats,
    log: PassLog<'_>,
) {
    let Some(tracker) = trackers[i].as_mut() else {
        return;
    };
    match tracker.advance_from(
        datasets[i]
            .as_ref()
            .expect("processor node keeps its dataset"),
    ) {
        Ok(advance) => {
            let dataset = datasets[i]
                .as_mut()
                .expect("processor node keeps its dataset");
            dataset.insert_resource(crate::windows::WindowWatermark(tracker.watermark_ms()));
            if tracker.has_watermark() {
                metrics.window_watermark(tracker.watermark_seconds());
            }
            super::windowing::report_watermark_advance(
                log.workflow_id,
                &ids[i],
                metrics,
                tracker,
                advance,
            );
        }
        Err(_e) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                parent: log.span,
                workflow = %log.workflow_id,
                iteration = log.iteration,
                processor = %ids[i],
                error = %_e,
                "window watermark advance error (continuing without it)"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = _e;
            stats.iteration_errors += 1;
        }
    }
}

/// Deliver processor `i`'s output along the links its routing decision
/// selects: forwarded into a downstream processor's dataset, or staged for a
/// sink.
///
/// A routing decision that names a branch no link carries is logged and
/// otherwise ignored. `rows_out` is what the processor's dataset held after
/// the run, counted against each selected labelled branch.
#[expect(
    clippy::too_many_arguments,
    reason = "one delivery over one processor's links; every argument is a distinct borrow the runner owns"
)]
pub(super) fn deliver_outbound(
    i: usize,
    routes: &Option<Vec<String>>,
    branch_batches: Option<&crate::operators::BranchPayloads>,
    rows_out: u64,
    metrics: &ProcessorMetrics,
    downstream: &[Vec<BuiltEdge>],
    kinds: &[NodeRunKind],
    components: &[Option<&'static str>],
    datasets: &mut [Option<Dataset>],
    staged: &mut [Vec<RecordBatch>],
    ids: &[String],
    stats: &mut WorkflowStats,
    log: PassLog<'_>,
) {
    #[cfg(not(feature = "tracing"))]
    let _ = (ids, log);
    for name in routes.iter().flatten() {
        if !downstream[i]
            .iter()
            .any(|e| e.branch.as_deref() == Some(name.as_str()))
        {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                parent: log.span,
                workflow = %log.workflow_id,
                iteration = log.iteration,
                processor = %ids[i],
                branch = %name,
                "routing decision names a branch no link carries (continuing)"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = name;
        }
    }

    for edge in &downstream[i] {
        let d = edge.node;
        if !super::builder::edge_selected(routes, &edge.branch) {
            continue;
        }
        let branch_batch = edge.branch.as_deref().and_then(|branch| {
            branch_batches.and_then(|bp| bp.batches.get(branch).map(|rb| (bp.component, rb)))
        });
        if let Some(branch) = &edge.branch {
            let branch_rows = branch_batch.map_or(rows_out, |(_, rb)| rb.num_rows() as u64);
            metrics.branch_rows(branch, branch_rows);
        }
        match kinds[d] {
            NodeRunKind::Processor => {
                let (left, right) = datasets.split_at_mut(i + 1);
                let src = left[i].as_ref().expect("processor dataset");
                let dst = right[d - i - 1]
                    .as_mut()
                    .expect("downstream processor dataset");
                let res = if let Some((comp, rb)) = branch_batch {
                    forward_with_branch_override(src, dst, comp, rb)
                } else {
                    src.forward_into(dst)
                };
                if let Err(_e) = res {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        parent: log.span,
                        workflow = %log.workflow_id,
                        iteration = log.iteration,
                        from = %ids[i], to = %ids[d], error = %_e,
                        "fan-out forward error (continuing)"
                    );
                    stats.iteration_errors += 1;
                }
            }
            NodeRunKind::Sink => {
                let component = components[d].expect("a sink node always declares a component");
                if let Some((comp, rb)) = branch_batch
                    && comp == component
                {
                    if rb.num_rows() > 0 {
                        staged[d].push(rb.clone());
                    }
                } else if let Ok(batch) = datasets[i]
                    .as_ref()
                    .expect("processor dataset")
                    .batch_named(component)
                    .cloned()
                    && batch.num_rows() > 0
                {
                    staged[d].push(batch);
                }
            }
            NodeRunKind::Source => {
                unreachable!("a source is never a link target")
            }
        }
    }
}

fn forward_with_branch_override(
    src: &Dataset,
    dst: &mut Dataset,
    override_component: &'static str,
    override_batch: &RecordBatch,
) -> Result<usize, crate::error::SaciError> {
    let names: Vec<&'static str> = dst.schemas().iter().map(|(name, _)| *name).collect();
    let mut appended = 0usize;
    for name in names {
        if name == override_component {
            appended += override_batch.num_rows();
            dst.append_record_batch(name, override_batch.clone())?;
        } else if src.schemas().contains(name) {
            let batch = src
                .batch_named(name)
                .expect("component present in schemas() must have a batch")
                .clone();
            appended += batch.num_rows();
            dst.append_record_batch(name, batch)?;
        }
    }
    Ok(appended)
}

/// Write every batch staged for one sink, without finalising it.
///
/// A batch the sink refuses goes to `dlq` when the workflow declared one,
/// and is dropped when it did not. Either way the pass continues: one dead
/// sink does not end the run.
///
/// `stream` is `Some` for the stream runner, which words the failure as
/// `stream sink write error (continuing)`, names the item's iteration and
/// hangs the event from the item's `sink.write` span; the batch runner passes
/// `None`, instruments the call itself and logs
/// `sink write error (continuing)`.
#[expect(
    clippy::too_many_arguments,
    reason = "one loop over one sink; every argument is a distinct borrow the runner owns"
)]
pub(super) async fn write_staged(
    sink: &mut dyn Sink,
    staged: &mut Vec<RecordBatch>,
    id: &str,
    metrics: &SinkMetrics,
    component: &str,
    workflow_id: &str,
    node_stat: &mut NodeStats,
    stats: &mut WorkflowStats,
    mut dlq: Option<&mut DeadLetterQueue>,
    stream: Option<PassLog<'_>>,
) {
    for batch in staged.drain(..) {
        let rows = batch.num_rows() as u64;
        match sink.write_batch(&batch).await {
            Ok(()) => {
                node_stat.rows += rows;
                node_stat.batches += 1;
                stats.sink_batches_written += 1;
                metrics.write(rows);
            }
            Err(e) => {
                #[cfg(feature = "tracing")]
                {
                    match stream {
                        None => tracing::error!(
                            workflow = %workflow_id,
                            sink = id,
                            error = %e,
                            "sink write error (continuing)"
                        ),
                        Some(item) => tracing::error!(
                            parent: item.span,
                            workflow = %workflow_id,
                            iteration = item.iteration,
                            sink = %id,
                            error = %e,
                            "stream sink write error (continuing)"
                        ),
                    }
                }
                stats.iteration_errors += 1;
                node_stat.errors += 1;
                if let Some(dlq) = dlq.as_deref_mut() {
                    dlq.record(&batch, id, component, &e, stats).await;
                }
                #[cfg(not(feature = "tracing"))]
                let _ = (workflow_id, &e, stream);
            }
        }
    }
}

/// Finalise one sink. `stream` selects the stream runner's wording of the
/// failure, `stream sink finish error`, over the batch runner's
/// `sink finish error`.
pub(super) async fn finish_sink(
    sink: &mut dyn Sink,
    id: &str,
    workflow_id: &str,
    node_stat: &mut NodeStats,
    stats: &mut WorkflowStats,
    stream: bool,
) {
    if let Err(_e) = sink.finish().await {
        #[cfg(feature = "tracing")]
        {
            if stream {
                tracing::error!(
                    workflow = %workflow_id,
                    sink = %id,
                    error = %_e,
                    "stream sink finish error"
                );
            } else {
                tracing::error!(
                    workflow = %workflow_id,
                    sink = id,
                    error = %_e,
                    "sink finish error"
                );
            }
        }
        #[cfg(not(feature = "tracing"))]
        let _ = (id, workflow_id, stream);
        stats.iteration_errors += 1;
        node_stat.errors += 1;
    }
}

/// Finalise every sink, in node order. Writes nothing: the runner has already
/// written what it staged. `stream` is [`finish_sink`]'s.
pub(super) async fn finish_all_sinks(
    sinks: &mut [Option<Box<dyn Sink>>],
    ids: &[String],
    workflow_id: &str,
    node_stats: &mut [NodeStats],
    stats: &mut WorkflowStats,
    stream: bool,
) {
    for i in 0..sinks.len() {
        if let Some(sink) = sinks[i].as_mut() {
            finish_sink(
                sink.as_mut(),
                &ids[i],
                workflow_id,
                &mut node_stats[i],
                stats,
                stream,
            )
            .await;
        }
    }
}

/// Write and finalise every sink's staged output. Used both for a normal
/// final iteration and for a cancellation that lands mid-pass, before the
/// main loop reaches every sink node on its own. Batch runner only: the stream
/// runner writes each item as it goes and drops what a mid-item cancellation
/// left staged.
#[expect(
    clippy::too_many_arguments,
    reason = "forwards write_staged's own argument list plus the sink slice"
)]
pub(super) async fn flush_and_finish_all(
    sinks: &mut [Option<Box<dyn Sink>>],
    staged: &mut [Vec<RecordBatch>],
    ids: &[String],
    sink_metrics: &[Option<SinkMetrics>],
    components: &[Option<&'static str>],
    workflow_id: &str,
    node_stats: &mut [NodeStats],
    stats: &mut WorkflowStats,
    mut dlq: Option<&mut DeadLetterQueue>,
) {
    for i in 0..sinks.len() {
        let (Some(sink), Some(metrics)) = (sinks[i].as_mut(), sink_metrics[i].as_ref()) else {
            continue;
        };
        write_staged(
            sink.as_mut(),
            &mut staged[i],
            &ids[i],
            metrics,
            components[i].unwrap_or_default(),
            workflow_id,
            &mut node_stats[i],
            stats,
            dlq.as_deref_mut(),
            None,
        )
        .await;
        finish_sink(
            sink.as_mut(),
            &ids[i],
            workflow_id,
            &mut node_stats[i],
            stats,
            false,
        )
        .await;
    }
}

/// Publish each sink's own backlog under its own id, and return the largest.
///
/// The maximum is what governs admission, because the whole consumer chain is
/// one shared resource, but a backlog is measured against one sink's flush
/// policy and only the per-sink value means anything to a reader. `None` when
/// no sink reports one.
pub(super) fn report_sink_backlog(
    sinks: &[Option<Box<dyn Sink>>],
    sink_metrics: &[Option<SinkMetrics>],
) -> Option<u64> {
    let mut largest: Option<u64> = None;
    for (sink, metrics) in sinks.iter().zip(sink_metrics) {
        let (Some(sink), Some(metrics)) = (sink.as_ref(), metrics.as_ref()) else {
            continue;
        };
        let Some(pending) = sink.pending_rows() else {
            continue;
        };
        let pending = pending as u64;
        metrics.pending_rows(pending);
        largest = Some(largest.map_or(pending, |seen| seen.max(pending)));
    }
    largest
}

/// Finalise every source, once, after the last drain.
///
/// A source whose delivery is a commitment (one that deletes what it yielded,
/// or commits an offset it held back) makes it durable here. Called after the
/// sinks are finished, so a source consuming what it handed over does it only
/// once the downstream write has landed.
pub(super) async fn finish_all_sources(
    sources: &mut [Option<Box<dyn Source>>],
    ids: &[String],
    workflow_id: &str,
    node_stats: &mut [NodeStats],
    stats: &mut WorkflowStats,
) {
    for i in 0..sources.len() {
        let Some(source) = sources[i].as_mut() else {
            continue;
        };
        if let Err(_e) = source.finish().await {
            #[cfg(feature = "tracing")]
            tracing::error!(
                workflow = %workflow_id,
                source = %ids[i],
                error = %_e,
                "source finish error"
            );
            stats.iteration_errors += 1;
            node_stats[i].errors += 1;
        }
    }
    #[cfg(not(feature = "tracing"))]
    let _ = (ids, workflow_id);
}

/// What the last recorded decision for one source was, plus the two totals the
/// finish line reports.
///
/// `last` is one `Option` of three `Copy` scalars, so a runner holds one per
/// source at no meaningful cost. The cause is compared by
/// [`Discriminant`](std::mem::Discriminant) rather than by value: two backlog
/// trips are the same episode whether the backlog read 8 rows or 16.
/// `adjustments` counts the episodes opened and `backoffs` every guard trip,
/// deduplicated or not.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(super) struct FlowEpisode {
    last: Option<(FlowDecisionKind, usize, std::mem::Discriminant<FlowCause>)>,
    adjustments: u64,
    backoffs: u64,
}

/// Log one adaptive-flow-control decision, and retain it when an inspector is
/// attached, on each pass that opened a new episode.
///
/// The mapping lives with the runners rather than with
/// [`FlowController`](super::flow::FlowController), which reads no clock and
/// knows no node ids: the runner stamps the time, names the source, and
/// phrases the cause. Both runners record the same record, so the mapping is
/// written once here and both of them call it.
///
/// **The buffer carries episodes, not trips.** Safety is unpaced, so a guard
/// under sustained pressure trips on pass after pass: a sink whose backlog
/// keeps growing divides the target to the same size for the same reason
/// hundreds of times a second, and at `min_rows` it divides nothing and sets
/// no cooldown either, so it reports on *every* pass. Recorded verbatim that
/// is a picket fence which fills the ring buffer in seconds and evicts every
/// epoch win and every other source's decisions, exactly when an operator
/// needs to see them. So `episode` remembers this source's last recorded
/// `(kind, to_rows, cause)` and a decision matching it is the same episode
/// and is dropped. A new record opens when the target lands somewhere else,
/// when the cause changes, or when another kind of adjustment intervenes.
/// [`FlowAdjustment::Held`] is not an adjustment and ends nothing: the
/// backlog trend rule zeroes its streak on any non-rise pass, so a plateauing
/// backlog alternates `Held` with a trip, and ending the episode there would
/// record one marker per rise. `saci_flow_backoff_total` counts every trip, so
/// the trip count is never lost: the counter carries trips, this buffer
/// carries episodes.
///
/// The `reason` is formatted once per episode and shared by the log line and
/// the record, and neither exists for a pass that held or for a decision
/// repeating the episode.
pub(super) fn record_flow_decision(
    inspector: Option<&Inspector>,
    workflow: &str,
    source: &str,
    adjustment: FlowAdjustment,
    episode: &mut FlowEpisode,
) {
    let (kind, moved) = match adjustment {
        FlowAdjustment::Held => return,
        FlowAdjustment::Grew(moved) => (FlowDecisionKind::Grew, moved),
        FlowAdjustment::Shrank(moved) => (FlowDecisionKind::Shrank, moved),
        FlowAdjustment::BackedOff(moved) => (FlowDecisionKind::BackedOff, moved),
        FlowAdjustment::HeldAtFloor(moved) => (FlowDecisionKind::HeldAtFloor, moved),
    };
    if matches!(
        kind,
        FlowDecisionKind::BackedOff | FlowDecisionKind::HeldAtFloor
    ) {
        episode.backoffs += 1;
    }
    let key = (kind, moved.to_rows, std::mem::discriminant(&moved.cause));
    if episode.last == Some(key) {
        return;
    }
    episode.last = Some(key);
    episode.adjustments += 1;
    let reason = flow_reason(moved.cause);
    #[cfg(feature = "tracing")]
    tracing::info!(
        target: FLOW_CONTROL_TARGET,
        workflow = %workflow,
        source = %source,
        kind = kind_label(kind),
        from_rows = moved.from_rows,
        to_rows = moved.to_rows,
        reason = %reason,
        "flow control decision"
    );
    let Some(inspector) = inspector else {
        return;
    };
    inspector.record_flow_decision(FlowDecision {
        at_unix_ms: crate::inspector::record::now_unix_ms(),
        workflow: workflow.to_string(),
        source: source.to_string(),
        kind,
        from_rows: moved.from_rows as u64,
        to_rows: moved.to_rows as u64,
        reason,
    });
}

/// One decision kind as the flow-control log line names it, matching the
/// vocabulary `docs/content/service/operate/flow-control.md` uses.
#[cfg(feature = "tracing")]
fn kind_label(kind: FlowDecisionKind) -> &'static str {
    match kind {
        FlowDecisionKind::Grew => "grew",
        FlowDecisionKind::Shrank => "shrank",
        FlowDecisionKind::BackedOff => "backed_off",
        FlowDecisionKind::HeldAtFloor => "held_at_floor",
    }
}

/// Announce one source's controller and the bounds it will search within.
///
/// Emitted for every controller a runner builds, pinned and disabled ones
/// included, whose fields say so. On [`FLOW_CONTROL_TARGET`], so neither
/// `log_level` nor a sampling ratio can silence it: how much a source is
/// admitting is the one thing an operator needs at the error-only default.
pub(super) fn log_flow_start(
    workflow: &str,
    source: &str,
    settings: &FlowSettings,
    controller: &FlowController,
) {
    #[cfg(feature = "tracing")]
    tracing::info!(
        target: FLOW_CONTROL_TARGET,
        workflow = %workflow,
        source = %source,
        enabled = settings.enabled,
        fixed_rows = ?settings.fixed_rows,
        start_rows = controller.target_rows(),
        min_rows = settings.min_rows,
        max_rows = settings.max_rows,
        max_chunk_bytes = settings.max_chunk_bytes,
        target_latency_ms = settings.target_latency_ms,
        adjust_interval_ms = settings.adjust_interval_ms,
        "flow control starting"
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (workflow, source, settings, controller);
}

/// Report where each governed source's search ended, once per runner exit.
pub(super) fn log_flow_finish(
    workflow: &str,
    ids: &[String],
    controllers: &[Option<FlowController>],
    episodes: &[FlowEpisode],
) {
    for (i, controller) in controllers.iter().enumerate() {
        let Some(controller) = controller else {
            continue;
        };
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: FLOW_CONTROL_TARGET,
            workflow = %workflow,
            source = %ids[i],
            final_rows = controller.incumbent_rows(),
            adjustments = episodes[i].adjustments,
            backoffs = episodes[i].backoffs,
            "flow control finished"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = (workflow, &ids[i], controller, &episodes[i]);
    }
}

/// One decision's cause as the dashboard shows it, naming the number that
/// decided.
fn flow_reason(cause: FlowCause) -> String {
    match cause {
        FlowCause::Experiment {
            winner_rows_per_second,
            loser_rows_per_second,
        } => format!(
            "experiment won: {} against {}",
            rate_phrase(winner_rows_per_second),
            rate_phrase(loser_rows_per_second)
        ),
        FlowCause::LatencyObjective {
            mean_pass_ms,
            objective_ms,
        } => format!(
            "latency objective breached: {mean_pass_ms} ms mean pass against {objective_ms} ms"
        ),
        FlowCause::PassError => "pass error".to_string(),
        FlowCause::ChunkBytes {
            projected_bytes,
            max_chunk_bytes,
        } => format!(
            "chunk over max_chunk_bytes: {} projected against {}",
            size_phrase(projected_bytes),
            size_phrase(max_chunk_bytes)
        ),
        FlowCause::SinkBacklog { pending_rows } => {
            format!("sink backlog growing: {pending_rows} rows pending")
        }
    }
}

/// A throughput as `"12.4k rows/s"`.
fn rate_phrase(rows_per_second: f64) -> String {
    match rows_per_second {
        rate if rate >= 1_000_000.0 => format!("{:.1}M rows/s", rate / 1_000_000.0),
        rate if rate >= 1_000.0 => format!("{:.1}k rows/s", rate / 1_000.0),
        rate => format!("{rate:.0} rows/s"),
    }
}

/// An Arrow weight as `"8.0 MiB"`.
fn size_phrase(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    let scaled = bytes as f64;
    if scaled >= MIB {
        format!("{:.1} MiB", scaled / MIB)
    } else if scaled >= KIB {
        format!("{:.1} KiB", scaled / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Every decision the inspector retained for `source`.
#[cfg(all(test, feature = "metrics"))]
pub(super) fn decisions_for(inspector: &Inspector, source: &str) -> Vec<FlowDecision> {
    inspector
        .snapshot(std::time::Duration::from_secs(300), true)
        .flow_decisions
        .into_iter()
        .filter(|decision| decision.source == source)
        .collect()
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use std::time::{Duration, Instant};

    use super::super::flow::{FlowOutcome, FlowSample};
    use super::*;

    /// An epoch that closes on a winner is the other half of what the
    /// dashboard draws, and its reason is the two rates that decided it.
    ///
    /// Driven with synthetic samples rather than a live runner: the win rule
    /// is a 5% margin on real measured rows per second, so a loaded machine
    /// can legitimately close every epoch undecided and a live run would
    /// flake. The back-off case in `standalone`'s tests stays end to end, because `ProbeSink`
    /// grows its backlog deterministically.
    #[test]
    fn an_epoch_win_is_recorded_with_the_rates_that_decided_it() {
        let inspector = Inspector::new(&crate::inspector::InspectorConfig::default());
        let settings = crate::service::flow::FlowSettings {
            min_rows: 100,
            max_rows: 100_000,
            start_rows: 1_000,
            max_chunk_bytes: 0,
            target_latency_ms: 0,
            // Long enough that each epoch collects the default four samples
            // per arm: a pass here costs 11 to 21 ms of synthetic time, and
            // an epoch that closes short of the minimum decides nothing.
            adjust_interval_ms: 300,
            ..Default::default()
        };
        let mut controller = FlowController::new(settings);
        let base = Instant::now();
        let mut episode = FlowEpisode::default();

        // One pass costs 10 ms of overhead plus 1 µs per row, so the larger
        // arm is unambiguously faster and the epoch has one right answer.
        let mut offset = Duration::ZERO;
        for _ in 0..40 {
            let rows = controller.target_rows() as u64;
            let elapsed = Duration::from_secs_f64(0.010 + rows as f64 * 0.000_001);
            offset += elapsed + Duration::from_millis(5);
            let adjustment = controller.observe(FlowSample {
                rows,
                elapsed,
                bytes: rows * 8,
                sink_pending: None,
                outcome: FlowOutcome::Ok,
                at: base + offset,
            });
            record_flow_decision(Some(&inspector), "w", "epoch-src", adjustment, &mut episode);
        }

        let decisions = decisions_for(&inspector, "epoch-src");
        let grew = decisions
            .iter()
            .find(|decision| decision.kind == FlowDecisionKind::Grew)
            .unwrap_or_else(|| panic!("no epoch closed on a larger winner: {decisions:?}"));
        assert_eq!(grew.workflow, "w");
        assert_eq!(grew.from_rows, 1_000, "the first epoch defends start_rows");
        assert_eq!(
            grew.to_rows, 2_000,
            "the winning candidate is one growth_factor step up"
        );
        assert!(
            grew.reason.starts_with("experiment won:")
                && grew.reason.matches("rows/s").count() == 2,
            "the reason must name both arms' rates: {:?}",
            grew.reason
        );
    }

    /// A guard that keeps tripping at `min_rows` divides nothing and sets no
    /// cooldown, so the controller reports it on every pass. One marker per
    /// episode is what a reader can use, and it is what keeps a pinned source
    /// from evicting every other decision in the buffer.
    #[test]
    fn a_source_pinned_at_the_floor_records_one_marker_per_episode() {
        let inspector = Inspector::new(&crate::inspector::InspectorConfig::default());
        let moved = crate::service::flow::FlowMove {
            from_rows: 64,
            to_rows: 64,
            cause: FlowCause::SinkBacklog { pending_rows: 9 },
        };
        // A plateauing backlog alternates `Held` with `HeldAtFloor`, because
        // the trend rule zeroes its streak on any non-rise pass. That is the
        // same pinned source, not a new episode.
        let mut episode = FlowEpisode::default();
        for pass in 0..500 {
            let adjustment = if pass % 3 == 0 {
                FlowAdjustment::Held
            } else {
                FlowAdjustment::HeldAtFloor(moved)
            };
            record_flow_decision(
                Some(&inspector),
                "w",
                "pinned-src",
                adjustment,
                &mut episode,
            );
        }
        assert_eq!(
            decisions_for(&inspector, "pinned-src").len(),
            1,
            "a source that never left the floor is one marker"
        );

        // The target moving ends the episode, so the next trip is a new
        // marker.
        record_flow_decision(
            Some(&inspector),
            "w",
            "pinned-src",
            FlowAdjustment::Grew(crate::service::flow::FlowMove {
                from_rows: 64,
                to_rows: 128,
                cause: FlowCause::Experiment {
                    winner_rows_per_second: 2_000.0,
                    loser_rows_per_second: 1_000.0,
                },
            }),
            &mut episode,
        );
        record_flow_decision(
            Some(&inspector),
            "w",
            "pinned-src",
            FlowAdjustment::HeldAtFloor(moved),
            &mut episode,
        );
        assert_eq!(decisions_for(&inspector, "pinned-src").len(), 3);
    }

    /// Safety is unpaced, so a sink losing ground divides the target to the
    /// same size for the same reason on pass after pass. Recorded verbatim
    /// that is a picket fence that fills the ring buffer in seconds and
    /// evicts every epoch win; the buffer carries one record per episode, and
    /// `saci_flow_backoff_total` carries the trip count.
    #[test]
    fn a_sustained_back_off_to_one_size_records_one_marker_per_episode() {
        let inspector = Inspector::new(&crate::inspector::InspectorConfig::default());
        let mut episode = FlowEpisode::default();
        let backlog_to = |to_rows: usize, pending_rows: u64| {
            FlowAdjustment::BackedOff(crate::service::flow::FlowMove {
                from_rows: to_rows * 2,
                to_rows,
                cause: FlowCause::SinkBacklog { pending_rows },
            })
        };

        // The backlog reading moves every pass; the decision does not.
        for pass in 0..400 {
            record_flow_decision(
                Some(&inspector),
                "w",
                "pressured-src",
                backlog_to(256, pass),
                &mut episode,
            );
        }
        assert_eq!(
            decisions_for(&inspector, "pressured-src").len(),
            1,
            "the same division for the same cause is one episode, whatever the backlog reads"
        );

        // A different landing size is a different episode.
        record_flow_decision(
            Some(&inspector),
            "w",
            "pressured-src",
            backlog_to(128, 40),
            &mut episode,
        );
        assert_eq!(decisions_for(&inspector, "pressured-src").len(), 2);

        // So is the same landing size reached for a different cause.
        record_flow_decision(
            Some(&inspector),
            "w",
            "pressured-src",
            FlowAdjustment::BackedOff(crate::service::flow::FlowMove {
                from_rows: 256,
                to_rows: 128,
                cause: FlowCause::PassError,
            }),
            &mut episode,
        );
        let recorded = decisions_for(&inspector, "pressured-src");
        assert_eq!(recorded.len(), 3);
        assert_eq!(recorded[2].reason, "pass error");
        assert_eq!(
            recorded[1].reason, "sink backlog growing: 40 rows pending",
            "each episode keeps the numbers of the pass that opened it"
        );
    }
}
