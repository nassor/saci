//! Stream runner for [`BuiltService`].
//!
//! [`run_stream`] drives a [`BuiltService`] one admitted chunk at a time: each
//! chunk of an arriving [`RecordBatch`] is appended
//! to that source node's dataset slot, fanned out, and every remaining node
//! runs in topological order before the next item is pulled. There is no
//! inter-item sleep: latency is bounded by the workflow itself, not by a
//! pacing timer.
//!
//! Selected by `run_mode` `kind = "stream"` in standalone mode;
//! [`run_standalone`](super::standalone::run_standalone) dispatches here.
//!
//! ## Semantics
//!
//! - **At least one source, pulled round-robin.** Config files are checked by
//!   [`ServiceConfig::validate`](super::config::ServiceConfig::validate);
//!   hand-built services are checked here. Each item is one chunk from one
//!   source, in a stable rotation across the declared sources: a windowing
//!   processor fed by several streams accumulates their rows into its open
//!   windows across items, one stream's chunk at a time. A source that
//!   reports EOF is dropped from the rotation while the live ones keep
//!   feeding items.
//! - **One pass per chunk.** Each arriving batch is split into chunks of the
//!   source controller's current target and each chunk is one workflow pass,
//!   with the tail admitted before any source is polled again. `slice` is
//!   zero-copy, so no row is copied, duplicated or reordered. A trailing
//!   chunk below the target is processed immediately: holding it back to fill
//!   a chunk would trade away the latency this mode exists for. `stats.rows_*`
//!   count chunks, the arrival counters and the persisted cursor count
//!   batches. The tail carries its arrival's Arrow weight with it (the
//!   `flow::Carry` buffer), because zero-copy is precisely why the tail can no
//!   longer be weighed on its own.
//!   A source on a path to a windowed node runs with no controller at all:
//!   one arrival is one item there, because a windowed node observes event
//!   time at every pass boundary, so neither the slice nor the
//!   `request_batch_rows` hint is available to a credit. Nothing would read
//!   its target, so it takes no samples and publishes no `saci_flow_*` series;
//!   its arrival size is the connector's declared batch size. See
//!   [`windowing`](super::windowing).
//!   Each chunk of every other source is one observation for that source's
//!   [`FlowController`], measured from the moment
//!   the batch is in hand so the wait for input is never counted; the
//!   controller adjusts at the close of its adjustment epoch, not per chunk.
//! - **State carry.** The blob returned by a processor's `run_on_with_state` is
//!   fed back as `prior` on the next item for that same processor, so
//!   processor state survives across items even though the WASM store does
//!   not. One blob per processor node; the blobs live in loop memory and, with
//!   no store configured, are never checkpointed, so they are lost on
//!   restart. With a `store "redb" { … }` block, priors and source cursors
//!   persist between runs and a restarted service resumes from its last save
//!   point.
//! - **At-most-once.** An item whose processor call fails drops that
//!   processor's fan-out for the item (logged and counted); `prior` is left
//!   untouched so the next item resumes from the last good state.
//! - **Sink finalisation.** Sinks are written per item but `finish()` is
//!   called once, at exit (source EOF or cancellation).
//! - **One trace per item.** A `workflow.batch` root span opens when the item
//!   arrives, holding one `runtime.run` per processor and one `sink.write`
//!   per sink. There is no `source.drain` span: the wait for input precedes
//!   the item and would otherwise dominate its latency.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use tokio::sync::RwLock;
#[cfg(feature = "tracing")]
use tracing::Instrument as _;

use crate::dataset::Dataset;
use crate::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_core::runtime::PipelineRuntime;

use super::builder::{BuiltNodeKind, BuiltService};
use super::dlq::ReplayCtx;
use super::flow::{Carry, FlowAdjustment, FlowController, FlowOutcome, FlowPlan, FlowSample};
use super::lifecycle::RunControl;
use super::redb_state::{RedbStateClient, SourceCursorMeta};
use super::sampling::FLOW_CONTROL_TARGET;
use super::standalone::{
    FlowEpisode, NodeRunStats, StandaloneStats, finish_all_sources, log_flow_finish,
    log_flow_start, record_flow_decision,
};
#[cfg(feature = "windows")]
use super::windowing::WindowTracker;

/// Which of the three roles a node plays while it runs. Mirrors
/// `standalone::NodeRunKind`; kept as a separate (private) type because the
/// two runners never share more than the enum shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeRunKind {
    Source,
    Processor,
    Sink,
}

/// Minimum spacing between `live_stats` publishes.
///
/// A per-item `RwLock` write would sit on the item path, and how long an item
/// takes is the controller's choice, not a constant: it searches for the
/// largest chunk that still pays, up to `target_latency_ms` (250 ms in this
/// mode). So the shared snapshot is refreshed on a fixed cadence rather than
/// per item (plus once at exit).
const PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

/// Backoff after a source error, so a permanently failing source cannot spin
/// the loop at full speed.
const SOURCE_ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// How long a source's first poll may wait before the runner rotates to the
/// next source.
///
/// Long enough for a connector to open its subscriptions (connect + subscribe
/// on `NatsSource`); short enough that a live pipeline does not linger on an
/// idle first source while its fan-in peers wait for their first poll.
const SOURCE_PRIME_TIMEOUT: Duration = Duration::from_secs(1);

/// Unix milliseconds now (wall clock), for persisted cursor timestamps.
fn wall_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Drive a [`BuiltService`] in stream mode: one workflow pass per admitted
/// chunk of an arriving source batch.
///
/// Returns [`StandaloneStats`] on success. Cancellation and source EOF are both
/// clean exits. Returns `Err` only for configuration violations detected at
/// entry.
///
/// `stats.iterations` counts items, each one chunk, `stats.total_busy_micros`
/// sums per-item processing time, and `stats.max_item_micros` records the
/// slowest item. `stats.source_batches_drained` and a *source* node's
/// `batches` count whole batches pulled from a source instead; a processor's
/// and a sink's `batches` count passes, like `iterations`.
///
/// ## Error policy
///
/// Mirrors [`run_standalone`](super::standalone::run_standalone): log, count in
/// `iteration_errors`, continue with the next item. A source error
/// additionally backs off for 10 ms (cancellable) to avoid a hot error loop.
///
/// The per-item span tree (`workflow.batch`, `runtime.run`, `sink.write`) is
/// `debug`, so neither the default `log_level="error"` nor `"info"` records
/// any of it. Every error and warning here therefore names its own
/// `workflow`, `iteration` and node rather than relying on a parent span's
/// fields.
///
/// `flow` carries the resolved flow-control policy per source node.
/// [`FlowPlan::stream_default`] is this mode's on-by-default policy and is
/// what a direct call should pass; [`FlowPlan::default`] is the batch policy
/// and carries no latency objective.
///
/// `control` is the runner's cancellation token plus its pause gate; a bare
/// [`CancellationToken`](tokio_util::sync::CancellationToken) converts into
/// one whose gate never parks.
pub async fn run_stream(
    built: BuiltService,
    control: impl Into<RunControl>,
    live_stats: Option<Arc<RwLock<StandaloneStats>>>,
    state: Option<Arc<RedbStateClient>>,
    flow: &FlowPlan,
) -> Result<StandaloneStats, SaciError> {
    let RunControl { cancel, pause } = control.into();
    let source_count = built
        .nodes
        .iter()
        .filter(|n| matches!(n.kind, BuiltNodeKind::Source(_)))
        .count();
    if source_count == 0 {
        return Err(SaciError::configuration(
            "stream mode requires at least one source (0 configured)",
        ));
    }

    let BuiltService {
        workflow_id,
        nodes,
        inspector,
        dlq,
        ..
    } = built;
    let mut dlq = dlq;
    let inspector = inspector.as_ref();
    let n = nodes.len();

    let mut ids: Vec<String> = Vec::with_capacity(n);
    let mut components: Vec<Option<&'static str>> = Vec::with_capacity(n);
    let mut downstream: Vec<Vec<crate::service::builder::BuiltEdge>> = Vec::with_capacity(n);
    let mut kinds: Vec<NodeRunKind> = Vec::with_capacity(n);
    let mut sources: Vec<Option<Box<dyn Source>>> = Vec::with_capacity(n);
    let mut runtimes: Vec<Option<Box<dyn PipelineRuntime>>> = Vec::with_capacity(n);
    let mut datasets: Vec<Option<Dataset>> = Vec::with_capacity(n);
    let mut sinks: Vec<Option<Box<dyn Sink>>> = Vec::with_capacity(n);
    let mut node_stats: Vec<NodeRunStats> = Vec::with_capacity(n);
    // One flag per healed sink node, `None` elsewhere. See the standalone
    // runner's own binding.
    let mut recovered: Vec<Option<Arc<std::sync::atomic::AtomicBool>>> = Vec::with_capacity(n);
    // One WIT checkpoint blob per processor node, fed back as `prior` on the
    // next item for that same node: every processor keeps its own.
    let mut prior: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    // One watermark tracker per windowed processor node, monotonic across
    // items, mirroring the standalone runner.
    #[cfg(feature = "windows")]
    let mut trackers: Vec<Option<WindowTracker>> = Vec::with_capacity(n);

    for node in nodes {
        ids.push(node.id);
        components.push(node.component);
        downstream.push(node.downstream);
        recovered.push(node.heal_recovered);
        prior.push(None);
        #[cfg(feature = "windows")]
        trackers.push(node.window.map(WindowTracker::new));

        let (kind, source, runtime, dataset, sink) = match node.kind {
            BuiltNodeKind::Source(source) => (NodeRunKind::Source, Some(source), None, None, None),
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
        sources.push(source);
        runtimes.push(runtime);
        datasets.push(dataset);
        sinks.push(sink);
        node_stats.push(NodeRunStats {
            id: ids.last().expect("just pushed").clone(),
            kind: match kind {
                NodeRunKind::Source => "source",
                NodeRunKind::Processor => "processor",
                NodeRunKind::Sink => "sink",
            }
            .to_string(),
            rows: 0,
            batches: 0,
            errors: 0,
        });
    }

    let mut staged: Vec<Vec<RecordBatch>> = vec![Vec::new(); n];

    let mut stats = StandaloneStats::default();
    let start = Instant::now();
    let mut last_publish = Instant::now();

    // Sources rotate round-robin: each item is one batch from exactly one
    // source. `exhausted` tracks sources that reported EOF, so a finished
    // source stops being polled while the live ones keep feeding items.
    let source_indices: Vec<usize> = (0..n)
        .filter(|&i| matches!(kinds[i], NodeRunKind::Source))
        .collect();
    let mut exhausted: Vec<bool> = vec![false; n];
    let mut remaining_sources = source_indices.len();
    let mut source_cursor = 0usize;
    // Items delivered per source, tracked so a persisted cursor can name the
    // exact count at the last save point.
    let mut items_per_source: Vec<u64> = vec![0; n];
    // A source on a path to a windowed node keeps every arrival whole: one
    // arrival is one item, because a windowed node observes event time at
    // every pass boundary and a credit-derived boundary would put a
    // throughput measurement in its output. See
    // [`windowing`](super::windowing) for the rule and its memory cost.
    #[cfg(feature = "windows")]
    let whole_arrivals: Vec<bool> = super::windowing::reaches_windowed_node(&trackers, &downstream);
    #[cfg(not(feature = "windows"))]
    let whole_arrivals: Vec<bool> = vec![false; n];

    // One controller per source node, plus the tail of the batch each source
    // is mid-way through. A pending tail is admitted before that source is
    // polled again, which is what keeps chunking order-preserving.
    //
    // A `whole_arrivals` source gets none. Neither of the two things a credit
    // can do here is available to it: the arrival is never sliced, and the
    // connector is never sent a size hint, so nothing in this loop would read
    // the target. A controller kept anyway would still take samples, run its
    // epoch experiment and publish `saci_flow_target_rows` and
    // `saci_flow_throughput_rows_per_second` for a source it does not govern,
    // which is the one question those series exist to answer. Its arrivals
    // are the connector's declared `batch_size`/`batch_rows` instead.
    let mut controllers: Vec<Option<FlowController>> = (0..n)
        .map(|i| {
            (matches!(kinds[i], NodeRunKind::Source) && !whole_arrivals[i]).then(|| {
                let settings = flow.for_source(&ids[i]);
                let controller = FlowController::new(settings);
                log_flow_start(&workflow_id, &ids[i], &settings, &controller);
                controller
            })
        })
        .collect();
    // Per source, the last decision recorded for it, so a guard tripping to
    // the same size for the same cause pass after pass records one marker
    // rather than one per trip. See `record_flow_decision`.
    let mut flow_episodes: Vec<FlowEpisode> = vec![FlowEpisode::default(); n];
    let mut pending: Vec<Option<Carry>> = (0..n).map(|_| None).collect();

    // Resume persisted state when a store is configured: processor priors
    // and source cursors come back from the redb file so a restart continues
    // from the last save point. Errors abort startup, because a store that
    // cannot be read must not silently restart from zero.
    if let Some(client) = &state {
        for i in 0..n {
            if matches!(kinds[i], NodeRunKind::Processor) {
                prior[i] = client.load_prior(&workflow_id, &ids[i]).await?;
            }
        }
        for &si in &source_indices {
            if let Some(meta) = client.load_source_cursor(&workflow_id, &ids[si]).await? {
                items_per_source[si] = meta.items_processed;
                #[cfg(feature = "tracing")]
                tracing::info!(
                    source = %ids[si],
                    items = meta.items_processed,
                    "resuming source from persisted cursor"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = meta;
            }
        }
    }

    #[cfg(feature = "tracing")]
    tracing::info!(
        workflow = %workflow_id,
        sources = source_indices.len(),
        "stream runner starting"
    );

    #[cfg(feature = "tracing")]
    for &si in &source_indices {
        if whole_arrivals[si] {
            tracing::info!(
                target: FLOW_CONTROL_TARGET,
                workflow = %workflow_id,
                source = %ids[si],
                "no flow control: source feeds a windowed node, so its arrivals stay whole and the connector's declared batch size governs them"
            );
        }
    }

    // Prime every source's first poll before the rotation blocks on any one
    // of them. A connector that subscribes lazily (`NatsSource` opens its
    // core subscriptions on the first `next_batch`) parks the loop for data
    // right after subscribing, so an unbounded first poll on the first source
    // would leave the fan-in sources behind it unsubscribed while messages
    // land on their subjects. Core NATS is at-most-once: a message published
    // with no subscriber is dropped. The prime runs the first polls
    // concurrently under `SOURCE_PRIME_TIMEOUT`; a timeout means the source
    // opened its subscriptions and is idle, and a batch that arrived during
    // the prime is handed to the rotation instead of being dropped.
    let mut prefetched: Vec<Option<RecordBatch>> = vec![None; n];
    let mut priming = Vec::with_capacity(source_indices.len());
    for &si in &source_indices {
        let mut source = sources[si]
            .take()
            .expect("the stream source keeps its source");
        // A source with a controller is one this mode can size: it has no
        // windowed node downstream, so a credit-sized fetch is a legitimate
        // pass boundary.
        if let Some(controller) = controllers[si]
            .as_ref()
            .filter(|controller| controller.enabled())
        {
            source.request_batch_rows(controller.target_rows());
        }
        priming.push(async move {
            let outcome = tokio::time::timeout(SOURCE_PRIME_TIMEOUT, source.next_batch()).await;
            (si, source, outcome)
        });
    }
    for (si, source, outcome) in futures::future::join_all(priming).await {
        sources[si] = Some(source);
        match outcome {
            Ok(Ok(Some(batch))) => prefetched[si] = Some(batch),
            Ok(Ok(None)) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    source = %ids[si],
                    "stream source reached EOF during prime"
                );
                exhausted[si] = true;
                remaining_sources -= 1;
            }
            Ok(Err(_e)) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    workflow = %workflow_id,
                    source = %ids[si],
                    error = %_e,
                    "stream source error during prime (continuing)"
                );
                stats.iteration_errors += 1;
                node_stats[si].errors += 1;
            }
            // The first poll spent its whole budget without a message: the
            // source opened whatever it needs and is idle, exactly the state
            // the loop wants.
            Err(_elapsed) => {}
        }
    }

    loop {
        // A pending chunk keeps its source in the rotation even after that
        // source reported EOF, so no admitted row is dropped at shutdown.
        if remaining_sources == 0 && pending.iter().all(Option::is_none) {
            break;
        }
        // The pause point, between items. A cancelled gate returns at once and
        // the `tokio::select!` on `next_batch` below still decides the exit, so
        // mid-stream cancellation drains pending carries exactly as it does
        // with no pause gate at all.
        //
        // A pause therefore settles on the next item, not immediately: a
        // runner already awaiting `next_batch` on an idle source stays there,
        // and the workflow reports `Pausing` until one arrives. The pause is
        // deliberately not raced against `next_batch`, because the `Source`
        // contract makes no cancel-safety promise and a dropped poll could
        // lose the message it had already taken. Cancellation races it anyway,
        // but that path is a shutdown, where the loss is the process ending.
        pause.park_while_paused(&cancel).await;
        // The `replay "before_sources"` point. In this mode the head of the
        // loop is reached only once an item has arrived, because the runner
        // blocks on `next_batch` below and that poll is not cancel-safe, the
        // same limit a pause has: on a silent stream neither a replay nor a
        // pause settles until something comes in.
        if let Some(dlq) = dlq.as_mut() {
            dlq.at_head(ReplayCtx {
                sinks: &mut sinks,
                ids: &ids,
                recovered: &recovered,
                node_stats: &mut node_stats,
                stats: &mut stats,
                cancel: &cancel,
            })
            .await;
        }
        // A source holding a tail is served before any source is polled: its
        // rows are already admitted, and blocking on another source's
        // `next_batch` would strand them for as long as that source is idle.
        // Otherwise advance to the next source that has not exhausted. The
        // cursor wraps through `source_indices`, so sources are visited in a
        // stable rotation and each item is one chunk from one source.
        let pending_position = (0..source_indices.len())
            .map(|step| (source_cursor + step) % source_indices.len())
            .find(|&position| pending[source_indices[position]].is_some());
        let source_index = match pending_position {
            Some(position) => {
                source_cursor = position + 1;
                source_indices[position]
            }
            None => loop {
                let idx = source_indices[source_cursor % source_indices.len()];
                source_cursor += 1;
                if !exhausted[idx] {
                    break idx;
                }
            },
        };

        // Three ways to reach a batch, in priority order: the tail of the
        // batch this source is mid-way through, the batch its primed first
        // poll already returned, or a fresh poll. Only a fresh poll counts as
        // an arriving batch for the cursor and the drain counter.
        //
        // No controller, or `enabled() == false`, means no admission credit
        // at all: the arriving batch is one pass, unsliced, and the connector
        // gets no hint. A source feeding a windowed node is in the first case.
        let credit = controllers[source_index]
            .as_ref()
            .is_some_and(FlowController::enabled);
        let target = if credit {
            controllers[source_index]
                .as_ref()
                .map_or(usize::MAX, FlowController::target_rows)
        } else {
            usize::MAX
        };
        let mut fresh_batch = true;
        // A tail carries the weight of the arrival it was cut from, because
        // that weight cannot be recovered from the tail itself.
        let mut carried_weight = None;
        let next = if let Some(carried) = pending[source_index].take() {
            fresh_batch = false;
            carried_weight = Some(carried.bytes_per_row);
            Ok(Some(carried.batch))
        } else {
            let source = sources[source_index]
                .as_mut()
                .expect("the stream source keeps its source");
            if let Some(batch) = prefetched[source_index].take() {
                Ok(Some(batch))
            } else {
                if credit {
                    source.request_batch_rows(target);
                }
                tokio::select! {
                    r = source.next_batch() => r,
                    _ = cancel.cancelled() => {
                        #[cfg(feature = "tracing")]
                        tracing::info!("stream runner cancelled while waiting for input");
                        break;
                    }
                }
            }
        };

        let batch = match next {
            Ok(None) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    source = %ids[source_index],
                    "stream source reached EOF"
                );
                exhausted[source_index] = true;
                remaining_sources -= 1;
                continue;
            }
            Ok(Some(batch)) => batch,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    workflow = %workflow_id,
                    iteration = stats.iterations + 1,
                    source = %ids[source_index],
                    error = %_e,
                    "stream source error (continuing)"
                );
                stats.iteration_errors += 1;
                node_stats[source_index].errors += 1;
                tokio::select! {
                    _ = tokio::time::sleep(SOURCE_ERROR_BACKOFF) => {}
                    _ = cancel.cancelled() => break,
                }
                continue;
            }
        };

        // One pass per chunk of at most `target` rows. `slice` is zero-copy,
        // so the chunk and the tail share the arriving batch's buffers and no
        // row is copied, duplicated or reordered. A trailing chunk below the
        // target is processed immediately: holding it back to fill a chunk
        // would trade the latency this mode exists for.
        //
        // A source feeding a windowed node holds no controller, so `target`
        // is `usize::MAX` here: its arrival is one item however large, and it
        // received no size hint either, so the item is the connector's own
        // batch.
        let bytes_per_row = carried_weight.unwrap_or_else(|| Carry::weigh(&batch));
        let batch = if batch.num_rows() > target {
            pending[source_index] = Some(Carry {
                batch: batch.slice(target, batch.num_rows() - target),
                bytes_per_row,
            });
            batch.slice(0, target)
        } else {
            batch
        };

        let item_start = Instant::now();
        let rows = batch.num_rows() as u64;

        // The root span opens once the item is in hand, so its duration is the
        // item's latency and not the wait for input before it.
        //
        // `debug`, not `info`: one tree of these opens per item, and the
        // default `log_level="error"` materialises no span at all.
        // `log_level="debug"` brings the per-item traces back; `"info"` gives
        // the `pipeline.run`-rooted ones only. Every error event below
        // therefore names its own workflow, iteration and node rather than
        // leaning on these fields.
        //
        // Children are created inside `batch_span.in_scope(...)`, so the batch
        // span is their contextual parent, which is the only form the
        // subscriber's sampler can follow.
        #[cfg(feature = "tracing")]
        let batch_span = tracing::debug_span!(
            "workflow.batch",
            workflow = %workflow_id,
            iteration = stats.iterations + 1,
            rows = rows
        );

        stats.rows_processed += rows;
        node_stats[source_index].rows += rows;
        crate::metrics::instruments().rows(&ids[source_index], rows);
        let errors_before = stats.iteration_errors;
        // Rows are counted per chunk so the totals match what passed through
        // the workflow; the arrival counters and the persisted cursor count
        // batches, so a batch split into chunks is one arrival, not several.
        if fresh_batch {
            stats.source_batches_drained += 1;
            node_stats[source_index].batches += 1;
            crate::metrics::instruments().source_batch(&ids[source_index]);
            // Persist the source cursor so a restart resumes from here. Best
            // effort: memory stays authoritative and an at-least-once source
            // covers a missed write with one replay.
            items_per_source[source_index] += 1;
            if let Some(client) = &state {
                let meta = SourceCursorMeta {
                    items_processed: items_per_source[source_index],
                    last_batch_at_ms: wall_clock_ms(),
                };
                if let Err(_e) = client
                    .save_source_cursor(&workflow_id, &ids[source_index], meta)
                    .await
                {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        workflow = %workflow_id,
                        source = %ids[source_index],
                        error = %_e,
                        "persisting source cursor failed (continuing; at-least-once replay covers it)"
                    );
                    #[cfg(not(feature = "tracing"))]
                    let _ = _e;
                }
            }
        }

        // Fan out the source item exactly like a batch-mode source drain.
        let component = components[source_index].expect("the stream source declares a component");
        for edge in &downstream[source_index] {
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
                            parent: &batch_span,
                            workflow = %workflow_id,
                            iteration = stats.iterations,
                            from = %ids[source_index],
                            to = %ids[d],
                            error = %_e,
                            "fan-out append error (continuing)"
                        );
                        stats.iteration_errors += 1;
                    }
                }
                NodeRunKind::Sink => staged[d].push(batch.clone()),
                NodeRunKind::Source => unreachable!("a source is never a link target"),
            }
        }

        let mut cancelled_mid_item = false;

        for i in 0..n {
            if i == source_index {
                continue;
            }
            match kinds[i] {
                // Another source: it contributes its own item, not this one.
                NodeRunKind::Source => continue,
                NodeRunKind::Processor => {
                    // Same fan-in watermark advance as the standalone runner:
                    // by the time this processor runs, every upstream node has
                    // delivered into its dataset for this item.
                    #[cfg(feature = "windows")]
                    if let Some(tracker) = trackers[i].as_mut() {
                        match tracker.advance_from(
                            datasets[i]
                                .as_ref()
                                .expect("processor node keeps its dataset"),
                        ) {
                            Ok(advance) => {
                                let dataset = datasets[i]
                                    .as_mut()
                                    .expect("processor node keeps its dataset");
                                dataset.insert_resource(saci_core::windows::WindowWatermark(
                                    tracker.watermark_ms(),
                                ));
                                if tracker.has_watermark() {
                                    crate::metrics::instruments()
                                        .window_watermark(&ids[i], tracker.watermark_seconds());
                                }
                                super::windowing::report_watermark_advance(
                                    &workflow_id,
                                    &ids[i],
                                    tracker,
                                    advance,
                                );
                            }
                            Err(_e) => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    parent: &batch_span,
                                    workflow = %workflow_id,
                                    iteration = stats.iterations,
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

                    let rows_in = datasets[i]
                        .as_ref()
                        .expect("processor node keeps its dataset")
                        .rows() as u64;
                    #[cfg(feature = "tracing")]
                    let run_span = batch_span.in_scope(|| {
                        tracing::debug_span!(
                            "runtime.run",
                            workflow = %workflow_id,
                            processor = %ids[i],
                            rows_in,
                            rows_out = tracing::field::Empty
                        )
                    });
                    let runtime = runtimes[i]
                        .as_ref()
                        .expect("processor node keeps its runtime");
                    let dataset = datasets[i]
                        .as_mut()
                        .expect("processor node keeps its dataset");
                    let run = runtime.run_on_with_state_and_routes(dataset, prior[i].as_deref());
                    #[cfg(feature = "tracing")]
                    let run = run.instrument(run_span.clone());
                    let run_result = tokio::select! {
                        r = run => Some(r),
                        _ = cancel.cancelled() => None,
                    };

                    let Some(run_result) = run_result else {
                        #[cfg(feature = "tracing")]
                        tracing::info!(parent: &batch_span, "stream runner cancelled during runtime run");
                        cancelled_mid_item = true;
                        break;
                    };

                    match run_result {
                        Ok(out) => {
                            // The checkpoint is persisted verbatim: `None`
                            // means the processor carries no state, so it
                            // must clear `prior` too.
                            prior[i] = out.state;
                            if let Some(client) = &state {
                                let result = match &prior[i] {
                                    Some(blob) => {
                                        client.save_prior(&workflow_id, &ids[i], blob).await
                                    }
                                    None => client.delete_prior(&workflow_id, &ids[i]).await,
                                };
                                if let Err(_e) = result {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(
                                        workflow = %workflow_id,
                                        processor = %ids[i],
                                        error = %_e,
                                        "persisting processor state failed (continuing)"
                                    );
                                    #[cfg(not(feature = "tracing"))]
                                    let _ = _e;
                                }
                            }
                            let rows_out = datasets[i]
                                .as_ref()
                                .expect("processor node keeps its dataset")
                                .rows() as u64;
                            #[cfg(feature = "tracing")]
                            run_span.record("rows_out", rows_out);
                            #[cfg(not(feature = "tracing"))]
                            let _ = rows_out;
                            node_stats[i].rows += rows_out;
                            node_stats[i].batches += 1;

                            let routes = &out.routes;
                            for name in routes.iter().flatten() {
                                if !downstream[i]
                                    .iter()
                                    .any(|e| e.branch.as_deref() == Some(name.as_str()))
                                {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(
                                        parent: &batch_span,
                                        workflow = %workflow_id,
                                        iteration = stats.iterations,
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
                                if !crate::service::builder::edge_selected(routes, &edge.branch) {
                                    continue;
                                }
                                if let Some(branch) = &edge.branch {
                                    crate::metrics::instruments()
                                        .processor_branch_rows(&ids[i], branch, rows_out);
                                }
                                match kinds[d] {
                                    NodeRunKind::Processor => {
                                        let (left, right) = datasets.split_at_mut(i + 1);
                                        let src = left[i].as_ref().expect("processor dataset");
                                        let dst = right[d - i - 1]
                                            .as_mut()
                                            .expect("downstream processor dataset");
                                        if let Err(_e) = src.forward_into(dst) {
                                            #[cfg(feature = "tracing")]
                                            tracing::warn!(
                                                parent: &batch_span,
                                                workflow = %workflow_id,
                                                iteration = stats.iterations,
                                                from = %ids[i],
                                                to = %ids[d],
                                                error = %_e,
                                                "fan-out forward error (continuing)"
                                            );
                                            stats.iteration_errors += 1;
                                        }
                                    }
                                    NodeRunKind::Sink => {
                                        let component = components[d]
                                            .expect("a sink node always declares a component");
                                        if let Some(fwd) = datasets[i]
                                            .as_ref()
                                            .expect("processor dataset")
                                            .batch_for(component)
                                            .cloned()
                                            && fwd.num_rows() > 0
                                        {
                                            staged[d].push(fwd);
                                        }
                                    }
                                    NodeRunKind::Source => {
                                        unreachable!("a source is never a link target")
                                    }
                                }
                            }
                        }
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(
                                parent: &run_span,
                                workflow = %workflow_id,
                                iteration = stats.iterations,
                                processor = %ids[i],
                                error = %_e,
                                "stream processor error (dropping item for this node)"
                            );
                            stats.iteration_errors += 1;
                            node_stats[i].errors += 1;
                            crate::metrics::instruments().workflow_error(&workflow_id);
                        }
                    }

                    datasets[i]
                        .as_mut()
                        .expect("processor node keeps its dataset")
                        .clear();
                }
                NodeRunKind::Sink => {
                    let component = components[i].expect("a sink node always declares a component");
                    #[cfg(feature = "tracing")]
                    let write_span = batch_span.in_scope(|| {
                        tracing::debug_span!(
                            "sink.write",
                            workflow = %workflow_id,
                            sink = %ids[i],
                            component,
                            rows = tracing::field::Empty
                        )
                    });
                    let rows_before: u64 = staged[i].iter().map(|b| b.num_rows() as u64).sum();
                    for b in staged[i].drain(..) {
                        let sink = sinks[i].as_mut().expect("sink node keeps its sink");
                        match sink.write_batch(&b).await {
                            Ok(()) => {
                                let rows = b.num_rows() as u64;
                                node_stats[i].rows += rows;
                                node_stats[i].batches += 1;
                                stats.sink_batches_written += 1;
                                crate::metrics::instruments().sink_write(&ids[i], rows);
                            }
                            Err(e) => {
                                #[cfg(feature = "tracing")]
                                tracing::error!(
                                    parent: &write_span,
                                    workflow = %workflow_id,
                                    iteration = stats.iterations,
                                    sink = %ids[i],
                                    error = %e,
                                    "stream sink write error (continuing)"
                                );
                                stats.iteration_errors += 1;
                                node_stats[i].errors += 1;
                                if let Some(dlq) = dlq.as_mut() {
                                    dlq.record(&b, &ids[i], component, &e, &mut stats).await;
                                }
                            }
                        }
                    }
                    #[cfg(feature = "tracing")]
                    write_span.record("rows", rows_before);
                    #[cfg(not(feature = "tracing"))]
                    let _ = rows_before;
                }
            }
        }

        if cancelled_mid_item {
            finish_all_sinks(&mut sinks, &ids, &workflow_id, &mut node_stats, &mut stats).await;
            finish_all_sources(
                &mut sources,
                &ids,
                &workflow_id,
                &mut node_stats,
                &mut stats,
            )
            .await;
            if let Some(dlq) = dlq.as_mut() {
                dlq.finish().await;
            }
            stats.total_duration_ms = start.elapsed().as_millis() as u64;
            stats.nodes = node_stats.clone();
            publish(&live_stats, &stats).await;
            log_flow_finish(&workflow_id, &ids, &controllers, &flow_episodes);
            return Ok(stats);
        }

        // The `replay "after_sources"` point: the item has been written, and
        // whatever is left of the pass goes to the letters waiting. Past the
        // cancellation exit above, so a shutdown never opens the store.
        if let Some(dlq) = dlq.as_mut() {
            dlq.at_tail(ReplayCtx {
                sinks: &mut sinks,
                ids: &ids,
                recovered: &recovered,
                node_stats: &mut node_stats,
                stats: &mut stats,
                cancel: &cancel,
            })
            .await;
        }

        let item_micros = item_start.elapsed().as_micros() as u64;
        stats.iterations += 1;
        crate::metrics::instruments().workflow_run(&workflow_id);
        stats.total_busy_micros += item_micros;

        // Each sink's own backlog is published under its own id; the maximum
        // is what governs admission, because the whole consumer chain is one
        // shared resource, but a backlog is measured against one sink's flush
        // policy and only the per-sink value means anything to a reader.
        let mut sink_pending: Option<u64> = None;
        for i in 0..n {
            let Some(pending) = sinks[i].as_ref().and_then(|sink| sink.pending_rows()) else {
                continue;
            };
            let pending = pending as u64;
            crate::metrics::instruments().sink_pending_rows(&ids[i], pending);
            sink_pending = Some(sink_pending.map_or(pending, |seen| seen.max(pending)));
        }
        // Feed the chunk back to this source's controller. `item_start` opens
        // once the batch is in hand, so the sample excludes the `next_batch`
        // await and the prime phase. It covers what the item then did: the
        // fan-out, the processor runs, the sink writes, and on a fresh
        // arrival the source-cursor write, charged to consumer time like
        // every other store write inside a pass.
        if let Some(controller) = controllers[source_index].as_mut() {
            let outcome = if stats.iteration_errors > errors_before {
                FlowOutcome::Error
            } else {
                FlowOutcome::Ok
            };
            let adjustment = controller.observe(FlowSample {
                rows,
                elapsed: Duration::from_micros(item_micros),
                bytes: (rows as f64 * bytes_per_row) as u64,
                sink_pending,
                outcome,
                at: Instant::now(),
            });
            // A guard that tripped at `min_rows` divided nothing, but it is
            // the same adverse evidence and the one an operator most needs to
            // see: a source pinned against a wall it cannot back away from.
            if matches!(
                adjustment,
                FlowAdjustment::BackedOff(_) | FlowAdjustment::HeldAtFloor(_)
            ) {
                crate::metrics::instruments().flow_backoff(&ids[source_index]);
            }
            record_flow_decision(
                inspector,
                &workflow_id,
                &ids[source_index],
                adjustment,
                &mut flow_episodes[source_index],
            );
            if let Some(_abandoned) = controller.take_latency_notice() {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    workflow = %workflow_id,
                    source = %ids[source_index],
                    abandoned = _abandoned,
                    "flow control latency objective unreachable at min_rows"
                );
            }
            // A disabled controller governs nothing: reporting its target as
            // the flow-control target would be indistinguishable from flow
            // control being on at that size.
            if controller.enabled() {
                crate::metrics::instruments().flow_state(
                    &ids[source_index],
                    controller.target_rows(),
                    controller.throughput(),
                );
            }
        }
        stats.max_item_micros = stats.max_item_micros.max(item_micros);

        #[cfg(feature = "tracing")]
        drop(batch_span);

        stats.nodes = node_stats.clone();
        if live_stats.is_some() && last_publish.elapsed() >= PUBLISH_INTERVAL {
            publish(&live_stats, &stats).await;
            last_publish = Instant::now();
        }
    }

    finish_all_sinks(&mut sinks, &ids, &workflow_id, &mut node_stats, &mut stats).await;
    finish_all_sources(
        &mut sources,
        &ids,
        &workflow_id,
        &mut node_stats,
        &mut stats,
    )
    .await;
    if let Some(dlq) = dlq.as_mut() {
        dlq.finish().await;
    }
    stats.total_duration_ms = start.elapsed().as_millis() as u64;
    stats.nodes = node_stats.clone();
    publish(&live_stats, &stats).await;

    #[cfg(feature = "tracing")]
    tracing::info!(
        passes = stats.iterations,
        source_batches = stats.source_batches_drained,
        rows_processed = stats.rows_processed,
        iteration_errors = stats.iteration_errors,
        total_busy_micros = stats.total_busy_micros,
        max_item_micros = stats.max_item_micros,
        total_duration_ms = stats.total_duration_ms,
        "stream runner clean shutdown"
    );

    log_flow_finish(&workflow_id, &ids, &controllers, &flow_episodes);

    Ok(stats)
}

/// Finalise every sink. Each item already wrote its own batches, so only
/// `finish` remains.
async fn finish_all_sinks(
    sinks: &mut [Option<Box<dyn Sink>>],
    ids: &[String],
    workflow_id: &str,
    node_stats: &mut [NodeRunStats],
    stats: &mut StandaloneStats,
) {
    for i in 0..sinks.len() {
        let Some(sink) = sinks[i].as_mut() else {
            continue;
        };
        if let Err(_e) = sink.finish().await {
            #[cfg(feature = "tracing")]
            tracing::error!(
                workflow = %workflow_id,
                sink = %ids[i],
                error = %_e,
                "stream sink finish error"
            );
            stats.iteration_errors += 1;
            node_stats[i].errors += 1;
        }
    }
}

async fn publish(shared: &Option<Arc<RwLock<StandaloneStats>>>, stats: &StandaloneStats) {
    if let Some(shared) = shared {
        *shared.write().await = stats.clone();
    }
}

#[cfg(all(test, feature = "service", feature = "metrics"))]
mod tests {
    use super::*;
    use crate::service::builder::{BuiltEdge, BuiltNode};
    use crate::service::flow::FlowSettings;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use saci_connector_channel::ChannelSource;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio_util::sync::CancellationToken;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// A sink that keeps no buffer, and so reports no backlog.
    ///
    /// The byte ceiling and sink pressure are two guards with one effect, so a
    /// test about either needs the other held still: a buffering sink whose
    /// consumer is a task rather than a person grows its backlog for as long
    /// as the writer outpaces it, which is sink pressure and would back the
    /// controller off for reasons this test is not about.
    struct CountingSink {
        schema: Arc<Schema>,
        rows: Arc<AtomicU64>,
    }

    #[async_trait]
    impl Sink for CountingSink {
        async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
            self.rows
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }
    }

    fn node(id: &str, kind: BuiltNodeKind, downstream: Vec<usize>) -> BuiltNode {
        BuiltNode {
            id: id.to_string(),
            name: None,
            type_name: "test".to_string(),
            component: Some("V"),
            kind,
            downstream: downstream
                .into_iter()
                .map(|node| BuiltEdge { node, branch: None })
                .collect(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        }
    }

    /// One arrival many times the admitted target, whose *chunks* sit well
    /// inside `max_chunk_bytes` even though the whole arrival does not.
    ///
    /// Every chunk after the first is cut from a tail, and a tail is a
    /// zero-copy slice that still reports its parent's buffers. Weighing one
    /// would charge a 1 024-row chunk with up to the whole 64 KiB arrival, so
    /// the byte guard would trip on the trailing chunks of exactly the
    /// arrivals this mode exists to cut up.
    #[tokio::test]
    async fn chunking_one_large_arrival_does_not_trip_the_byte_ceiling() {
        let rows = 8_192;
        let (tx, source) = ChannelSource::new(schema(), 8);
        let delivered = Arc::new(AtomicU64::new(0));
        let sink = CountingSink {
            schema: schema(),
            rows: Arc::clone(&delivered),
        };
        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(
                (0..rows as i64).collect::<Vec<_>>(),
            ))],
        )
        .expect("the arrival is well formed");
        // 8 bytes a row: one 1 024-row chunk weighs about 8 KiB, a third of
        // the ceiling below, while the whole arrival weighs about 64 KiB.
        tx.send(batch).await.expect("the source accepts it");
        drop(tx);

        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes: vec![
                node(
                    "stream-chunked-src",
                    BuiltNodeKind::Source(Box::new(source)),
                    vec![1],
                ),
                node(
                    "stream-out",
                    BuiltNodeKind::Sink(Box::new(sink)),
                    Vec::new(),
                ),
            ],
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
            dlq: None,
        };
        // A range whose *every* admissible size is far inside the ceiling:
        // 1 024 rows weigh about 8 KiB against a 16 KiB bound, and the arms
        // can only ever be 1 024 or 512, so no scheduling accident can bring a
        // legitimate chunk near the guard. What the buggy weighing produces is
        // not near it either: it charges a trailing chunk with the whole
        // 64 KiB arrival.
        let plan = FlowPlan::uniform(FlowSettings {
            min_rows: 512,
            max_rows: 1_024,
            start_rows: 1_024,
            max_chunk_bytes: 16_384,
            target_latency_ms: 0,
            ..FlowSettings::default()
        });

        let stats = tokio::time::timeout(
            Duration::from_secs(30),
            run_stream(built, CancellationToken::new(), None, None, &plan),
        )
        .await
        .expect("the run terminates at EOF rather than parking")
        .expect("the run reaches EOF cleanly");
        assert_eq!(stats.rows_processed, rows as u64, "every row must flow");
        assert!(
            stats.iterations > 1,
            "the arrival must actually have been chunked, got {} pass(es)",
            stats.iterations
        );
        assert_eq!(
            delivered.load(Ordering::Relaxed),
            rows as u64,
            "chunking must deliver the whole arrival, once"
        );

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        // The one unambiguous reading. A back-off is the only thing that moves
        // the target inside an epoch, and this run is far shorter than one, so
        // "never backed off" is also "never shrank". The gauge itself cannot
        // say so on its own: it publishes the arm under test, which alternates
        // between the incumbent and a smaller candidate by design.
        assert!(
            !text
                .lines()
                .filter(|line| line.starts_with("saci_flow_backoff_total"))
                .any(|line| line.contains(r#"source="stream-chunked-src""#)),
            "no chunk came near the byte ceiling, so nothing may back off:\n{text}"
        );
    }

    /// The condition an operator sees as a dead sink: an inbound stream whose
    /// event time restarted behind where it stopped, feeding a windowed node.
    ///
    /// The watermark is monotonic and does not rewind with the stream, so
    /// every arrival after the restart is beyond the node's lateness budget
    /// and its windowing logic drops all of it. The runner cannot fix that,
    /// because it is what event time means, so it counts each such arrival
    /// under the node's own id, which is the only number that separates this
    /// from a pipeline with nothing to do.
    #[cfg(all(feature = "windows", feature = "metrics"))]
    #[tokio::test]
    async fn a_rewound_stream_counts_every_arrival_a_windowed_node_drops() {
        use saci_core::runtime::PipelineRuntime;

        const PROCESSOR: &str = "stream-rewound-win";

        fn sale_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "timestamp_ms",
                DataType::Int64,
                false,
            )]))
        }

        struct Noop;
        #[async_trait(?Send)]
        impl PipelineRuntime for Noop {
            fn name(&self) -> &str {
                "noop"
            }
            async fn run_on(&self, _data: &mut Dataset) -> Result<(), SaciError> {
                Ok(())
            }
            fn template_dataset(&self) -> Dataset {
                let mut dataset = Dataset::new();
                dataset.register_raw_component("Sale", sale_schema());
                dataset
            }
        }

        let (tx, source) = ChannelSource::new(sale_schema(), 16);
        let arrival = |ts: i64| {
            RecordBatch::try_new(sale_schema(), vec![Arc::new(Int64Array::from(vec![ts]))])
                .expect("the arrival is well formed")
        };
        // Two arrivals of a healthy stream, then three of the same stream
        // restarted from its base: 1 000 and 2 000 both sit more than the
        // 5 000 ms budget below the 120 000 ms watermark the first phase left.
        for ts in [100_000, 120_000, 1_000, 2_000, 3_000] {
            tx.send(arrival(ts)).await.expect("the source accepts it");
        }
        drop(tx);

        let mut source_node = node(
            "stream-rewound-src",
            BuiltNodeKind::Source(Box::new(source)),
            vec![1],
        );
        source_node.component = Some("Sale");
        let mut processor_node = node(
            PROCESSOR,
            BuiltNodeKind::Processor {
                runtime: Box::new(Noop),
                kind: "native",
            },
            Vec::new(),
        );
        processor_node.component = None;
        processor_node.window = Some(crate::service::config::WindowConfig {
            spec: saci_core::windows::WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 0,
            },
            time_field: "timestamp_ms".to_string(),
            key_fields: Vec::new(),
            allowed_lateness_ms: 5_000,
        });

        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes: vec![source_node, processor_node],
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
            dlq: None,
        };

        tokio::time::timeout(
            Duration::from_secs(30),
            run_stream(
                built,
                CancellationToken::new(),
                None,
                None,
                &FlowPlan::stream_default(),
            ),
        )
        .await
        .expect("the run terminates at EOF rather than parking")
        .expect("the run reaches EOF cleanly");

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        let counted: Vec<&str> = text
            .lines()
            .filter(|line| {
                line.starts_with("saci_window_late_arrivals_total")
                    && line.contains(&format!(r#"processor="{PROCESSOR}""#))
            })
            .collect();
        assert_eq!(
            counted.len(),
            1,
            "exactly one attributed series is expected, got:\n{text}"
        );
        assert!(
            counted[0].ends_with(" 3"),
            "the three rewound arrivals must be counted and the two healthy ones must not: {}",
            counted[0]
        );
    }

    /// Where a [`LoggingSource`] or [`LoggingSink`] records a `finish()`
    /// call, so a test can assert the runner's promise that every sink
    /// finishes before any source does.
    type FinishLog = Arc<std::sync::Mutex<Vec<&'static str>>>;

    /// A source over one queued arrival (or none) that counts its `finish()`
    /// calls and records each one in a shared [`FinishLog`].
    struct LoggingSource {
        inner: ChannelSource,
        finish_calls: Arc<AtomicU64>,
        log: FinishLog,
    }

    impl LoggingSource {
        fn new(inner: ChannelSource, log: FinishLog) -> (Self, Arc<AtomicU64>) {
            let finish_calls = Arc::new(AtomicU64::new(0));
            (
                Self {
                    inner,
                    finish_calls: Arc::clone(&finish_calls),
                    log,
                },
                finish_calls,
            )
        }
    }

    #[async_trait]
    impl Source for LoggingSource {
        fn schema(&self) -> Arc<Schema> {
            self.inner.schema()
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            self.inner.next_batch().await
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            self.finish_calls.fetch_add(1, Ordering::SeqCst);
            self.log.lock().unwrap().push("source.finish");
            Ok(())
        }
    }

    /// A sink that accepts every write and records each `finish()` call in a
    /// shared [`FinishLog`].
    struct LoggingSink {
        schema: Arc<Schema>,
        log: FinishLog,
        finish_calls: Arc<AtomicU64>,
    }

    impl LoggingSink {
        fn new(log: FinishLog) -> (Self, Arc<AtomicU64>) {
            let finish_calls = Arc::new(AtomicU64::new(0));
            (
                Self {
                    schema: schema(),
                    log,
                    finish_calls: Arc::clone(&finish_calls),
                },
                finish_calls,
            )
        }
    }

    #[async_trait]
    impl Sink for LoggingSink {
        async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<(), SaciError> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            self.finish_calls.fetch_add(1, Ordering::SeqCst);
            self.log.lock().unwrap().push("sink.finish");
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }
    }

    /// A processor whose `run_on` never returns on its own, so a test can
    /// cancel the run while an item is in flight through it.
    struct SlowRuntime;

    #[async_trait(?Send)]
    impl PipelineRuntime for SlowRuntime {
        fn name(&self) -> &str {
            "slow"
        }

        async fn run_on(&self, _data: &mut Dataset) -> Result<(), SaciError> {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        }

        fn template_dataset(&self) -> Dataset {
            let mut dataset = Dataset::new();
            dataset.register_raw_component("V", schema());
            dataset
        }
    }

    /// A stream run that reaches source EOF finishes its sink, and only then
    /// its source: `finish_all_sources` runs after `finish_all_sinks` on the
    /// normal end.
    #[tokio::test]
    async fn a_stream_run_at_eof_finishes_its_source_once_and_after_its_sink() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (tx, chan_source) = ChannelSource::new(schema(), 4);
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .expect("the batch is well formed");
        tx.send(batch).await.expect("the source accepts it");
        drop(tx);
        let (source, source_finishes) = LoggingSource::new(chan_source, Arc::clone(&log));
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes: vec![
                node("src", BuiltNodeKind::Source(Box::new(source)), vec![1]),
                node("out", BuiltNodeKind::Sink(Box::new(sink)), Vec::new()),
            ],
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
            dlq: None,
        };

        let stats = tokio::time::timeout(
            Duration::from_secs(10),
            run_stream(
                built,
                CancellationToken::new(),
                None,
                None,
                &FlowPlan::stream_default(),
            ),
        )
        .await
        .expect("the run reaches EOF rather than parking")
        .expect("a healthy stream run exits cleanly");

        assert_eq!(stats.rows_processed, 3, "the one arrival flowed");
        assert_eq!(
            source_finishes.load(Ordering::SeqCst),
            1,
            "finish runs exactly once"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["sink.finish", "source.finish"],
            "the sink is finished before the source that fed it"
        );
    }

    /// A cancel landing mid-item, inside a processor's `run_on`, still
    /// finishes the sink before the source through the shared exit path.
    #[tokio::test]
    async fn a_cancel_mid_item_finishes_the_source_once_and_after_the_sink() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (tx, chan_source) = ChannelSource::new(schema(), 4);
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1]))])
            .expect("the batch is well formed");
        tx.send(batch).await.expect("the source accepts it");
        let (source, source_finishes) = LoggingSource::new(chan_source, Arc::clone(&log));
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let source_node = node("src", BuiltNodeKind::Source(Box::new(source)), vec![1]);
        let mut processor_node = node(
            "slow",
            BuiltNodeKind::Processor {
                runtime: Box::new(SlowRuntime),
                kind: "native",
            },
            vec![2],
        );
        processor_node.component = None;
        let sink_node = node("out", BuiltNodeKind::Sink(Box::new(sink)), Vec::new());

        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes: vec![source_node, processor_node, sink_node],
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
            dlq: None,
        };

        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                cancel.cancel();
            }
        };
        let plan = FlowPlan::stream_default();
        let (stats, ()) = tokio::join!(run_stream(built, cancel, None, None, &plan), stopper);
        let stats = stats.expect("a cancel mid item still exits cleanly");

        assert_eq!(
            stats.iterations, 0,
            "the item never finished, so it is never counted"
        );
        assert_eq!(
            source_finishes.load(Ordering::SeqCst),
            1,
            "finish runs exactly once"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["sink.finish", "source.finish"],
            "the sink is finished before the source that fed it, even mid-item"
        );
    }

    /// A cancel that lands while the runner is blocked on `next_batch` for
    /// an idle source, after priming already gave up on it, is the third
    /// distinct exit from the item loop (a bare `break` alongside the EOF
    /// one, both falling through to the same unconditional shared finish
    /// block): still sink before source, exactly once each.
    #[tokio::test]
    async fn a_cancel_while_waiting_for_input_finishes_source_after_sink() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        // No batch ever sent, and `tx` kept alive: `next_batch` blocks
        // forever rather than returning EOF, so the only way out is the
        // `tokio::select!` cancel arm.
        let (_tx, chan_source) = ChannelSource::new(schema(), 4);
        let (source, source_finishes) = LoggingSource::new(chan_source, Arc::clone(&log));
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes: vec![
                node("src", BuiltNodeKind::Source(Box::new(source)), vec![1]),
                node("out", BuiltNodeKind::Sink(Box::new(sink)), Vec::new()),
            ],
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
            dlq: None,
        };

        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                // Past `SOURCE_PRIME_TIMEOUT` (1 s), so the cancel lands in
                // the main loop's own `next_batch` wait rather than racing
                // the prime's timeout.
                tokio::time::sleep(Duration::from_millis(1_100)).await;
                cancel.cancel();
            }
        };
        let plan = FlowPlan::stream_default();
        let (stats, ()) = tokio::join!(run_stream(built, cancel, None, None, &plan), stopper);
        let stats = stats.expect("a cancel while idle still exits cleanly");

        assert_eq!(stats.iterations, 0, "no item ever arrived");
        assert_eq!(
            source_finishes.load(Ordering::SeqCst),
            1,
            "finish runs exactly once"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["sink.finish", "source.finish"],
            "the sink is finished before the source that fed it"
        );
    }
}
