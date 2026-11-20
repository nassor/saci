//! Standalone runner for [`BuiltWorkflow`].
//!
//! [`BuiltWorkflow::run`] drives a [`BuiltWorkflow`] through repeated processing
//! iterations in a single process with no distributed coordination, walking
//! every declared node in topological order each pass. It handles
//! cancellation, transient per-node errors, and run-mode pacing (one-shot,
//! continuous, or interval-based).
//!
//! ## Store-per-call semantics (WASM runtimes)
//!
//! A `WasmPipelineRuntime` creates a fresh wasmtime `Store` for every `run_batch` call and
//! resets processor linear memory, unless its embedder opted in with `with_warm_instances`;
//! the service builder never does. In-processor state that must
//! survive iterations (accumulators, window buffers, caches) has to travel
//! through the host as the `prior`/`run-result.checkpoint` blob
//! `run_batch` threads, or it is silently lost.
//!
//! Interval/one-shot state carry across iterations is opt-in: with
//! `store "redb" { batch_resume #true }` the runner threads the processor
//! state blob as `prior` on every iteration and persists it to the local
//! redb file, so a restarted service resumes from its last save point.
//! Without the flag (the default) every iteration passes `None` as prior and
//! discards the output state, exactly as a run with no `store` block.
//!
//! A processor node declaring a `window` block is the exception: its blob is
//! threaded from one pass to the next whatever `store` says. A `window`
//! block declares that the node accumulates event-time state over more than
//! one pass, and a backlog reaches it over several passes; a dropped blob
//! would leave every pass aggregating from an empty accumulator and a
//! watermark of `i64::MIN`, so no window would ever close. Threading is in
//! memory only. *Persisting* the blob is still `batch_resume`'s decision, so
//! a windowed node with no store keeps its accumulator for the life of the
//! process and no longer. [`windowing`](super::windowing) states what a pass
//! boundary means to such a node.
//!
//! ## Adaptive flow control, and what pacing means
//!
//! `RunMode::Continuous` and `RunMode::Interval` admit a bounded number of
//! rows per source per iteration: whatever that source's
//! [`FlowController`] currently targets. The
//! unconsumed tail of the last `RecordBatch` stays in a per-source carry-over
//! buffer and is admitted before anything new on the next iteration, so
//! re-chunking never loses, duplicates or reorders a row.
//!
//! A source on a path to a windowed node is bounded but never sliced: the
//! credit stops the drain pulling the next arrival, and the arrival in hand
//! passes through whole, overshooting the credit by its tail. No boundary is
//! ever drawn inside an arrival, which is where a windowed node observes
//! event time; how many whole arrivals share one pass is still the credit's
//! choice here. `request_batch_rows` is withheld from such a source for the
//! same reason: it would let the credit choose the arrival's size, and so
//! the pass boundary, at the connector instead. The connector keeps its
//! declared fetch size, which is also what bounds the memory one pass holds;
//! see [`windowing`](super::windowing).
//!
//! Rows are counted per chunk, so `stats.rows_processed` matches what passed
//! through the workflow; `source_batches_drained` counts arrivals, so a
//! source batch re-chunked over several iterations is one drained batch
//! rather than one per chunk.
//!
//! Each iteration is one observation per source, never a decision: the
//! controller adjusts at the close of its adjustment epoch
//! (`adjust_interval_ms`), and only a guard acts on the pass that saw it. An
//! epoch that rests on a settled size decides nothing and admits at that
//! size throughout, so a converged source runs whole stretches of epochs
//! without an adjustment. An observation carries the rows admitted, the
//! consumer chain's own time, the Arrow memory those rows weigh, the largest
//! sink backlog, and whether the pass raised an error. Each source's *wait
//! for input* is timed separately and subtracted, so a connector's blocking
//! poll never counts as consumer time; the fan-out that follows a pull is
//! consumer work and stays in, being the part of the cost that scales with
//! the credit. What remains is split across the sources in proportion to the
//! rows each admitted, because the processors and sinks they share have one
//! throughput.
//!
//! An error is evidence about whatever produced it. A processor, a sink or a
//! fan-out append fails for the whole shared chain, so every source feeding it
//! sees the failure; a source's own drain error is that source's alone, and
//! backing its peers off for it would walk a whole workflow down to `min_rows`
//! because one connector is flapping.
//!
//! Pacing applies to a drained iteration only. An iteration that spent its
//! credit while the source was still live, or that left a carry-over slice, is
//! backlogged and re-enters immediately. `every_ms` is therefore the poll
//! cadence of an idle or drained source, never a throughput cap: a backlog
//! drains at full speed in credit-sized chunks, and only a pass that reached
//! EOF on every source (or admitted nothing) waits out `Continuous`'s 100 ms
//! or `Interval`'s `every_ms`.
//!
//! `RunMode::Once` engages no controller. A single pass must drain
//! everything by definition, so admitting a credit-sized prefix and exiting
//! would silently drop the rest of the source. `flow_control { enabled
//! #false }` restores drain-to-EOF iterations and unconditional pacing in
//! every mode.
//!
//! ## One trace per iteration, at `debug`
//!
//! Each iteration opens a `workflow.batch` root span holding one `source.drain`
//! per source, one `runtime.run` per processor, and one `sink.write` per sink,
//! in topological order. The span closes before run-mode pacing, so its
//! duration is the iteration and not the wait after it. `runtime.run` is the
//! contextual parent of whatever the runtime opens: `pipeline.run` for a
//! native [`Pipeline`](crate::Pipeline), `processor.batch` for a WASM
//! processor or a native plugin. That tree is the only host-side view of a
//! pipeline whose systems run inside a guest.
//!
//! The whole tree is `debug`: one opens per iteration, and materialising every
//! one of them costs more per item than the item. The default
//! `observability log_level="error"` records no span at all, and `"info"`
//! records none of this tree; set `log_level="debug"` to get the
//! per-iteration waterfall back. Error and warning events do not depend on
//! it: each names its own `workflow`, `iteration` and node, so a failure is
//! diagnosable with no span in sight.
//!
//! ## Example
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use tokio_util::sync::CancellationToken;
//! use saci::service::BuiltWorkflow;
//! // Build a BuiltWorkflow (via ServiceBuilder::build) then:
//! // let stats = built.run(cancel).await?;
//! # }
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
#[cfg(feature = "tracing")]
use tracing::Instrument as _;

use crate::error::SaciError;

use super::builder::{BuiltWorkflow, RunOptions};
use super::config::RunMode;
use super::dlq::ReplayCtx;
use super::flow::{Carry, FlowAdjustment, FlowController, FlowOutcome, FlowSample};
use super::lifecycle::RunControl;
#[cfg(feature = "windows")]
use super::runner::advance_watermark;
use super::runner::{
    self, FlowEpisode, NodeRunKind, Nodes, PassLog, deliver_outbound, fan_out_source,
    finish_all_sinks, finish_all_sources, flush_and_finish_all, log_flow_finish, log_flow_start,
    record_flow_decision, report_sink_backlog, write_staged,
};

/// Serialize a shared node id as a plain string; serde's `Arc` support sits
/// behind its `rc` feature, which this crate does not otherwise need.
fn serialize_arc_str<S: serde::Serializer>(
    id: &Arc<str>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(id)
}

/// Cumulative counters for one workflow node, across every iteration.
#[derive(Debug, Default, Clone, Serialize)]
pub struct NodeStats {
    /// The node's declared id.
    #[serde(serialize_with = "serialize_arc_str")]
    pub id: Arc<str>,
    /// `"source"`, `"processor"` or `"sink"`.
    pub kind: &'static str,
    /// Rows this node produced (source, processor) or wrote (sink).
    pub rows: u64,
    /// Batches this node accounted for, in the unit its kind counts in: a
    /// source counts arrivals, so a batch re-chunked over several passes is
    /// one; a processor and a sink count non-empty passes, so that same batch
    /// is one apiece per chunk.
    pub batches: u64,
    /// Errors this node raised.
    pub errors: u64,
}

/// Diagnostic counters accumulated over a [`BuiltWorkflow::run`] call.
///
/// Fields are public so the HTTP control plane can expose them via `/metrics`
/// by reading from a shared `Arc<RwLock<WorkflowStats>>`.
#[derive(Debug, Default, Clone)]
pub struct WorkflowStats {
    /// Total number of completed workflow passes.
    ///
    /// Under an admission credit a pass is one credit-sized admission rather
    /// than one drain to EOF, so this counter and `saci_workflow_passes_total`
    /// are per pass while `source_batches_drained` and a source node's
    /// `batches` are per arrival. A pass is one whole drain again where no
    /// credit applies: `flow_control { enabled #false }` and
    /// `RunMode::Once`, which builds no controller at all.
    /// The `stream` runner fills the same field once
    /// per chunk, of the single source that chunk came from.
    pub iterations: u64,
    /// Total number of source drain calls that returned at least one row.
    pub source_batches_drained: u64,
    /// Total rows loaded from sources across all iterations.
    pub rows_processed: u64,
    /// Total number of sink drain calls that wrote at least one row.
    pub sink_batches_written: u64,
    /// Count of non-fatal errors (source, processor, or sink failures).
    pub iteration_errors: u64,
    /// Wall-clock time from the first iteration to the last, in milliseconds.
    pub total_duration_ms: u64,
    /// Sum of per-item processing time in microseconds. Stream mode only; the
    /// batch loop leaves this at 0.
    pub total_busy_micros: u64,
    /// Slowest single item, in microseconds. Stream mode only; the batch loop
    /// leaves this at 0.
    pub max_item_micros: u64,
    /// Batches a sink refused that the dead letter queue stored.
    ///
    /// Zero for a workflow with no `dlq` block, where a refused batch is
    /// logged and dropped.
    pub dead_letters_recorded: u64,
    /// Dead letters a replay delivered to their sink.
    pub dead_letters_replayed: u64,
    /// Per-node breakdown of the flat counters above, one entry per declared
    /// workflow node, in topological order.
    pub nodes: Vec<NodeStats>,
}

/// Drive a [`BuiltWorkflow`] through repeated processing iterations.
///
/// Returns [`WorkflowStats`] on success, including on cancellation, which is
/// a clean exit. `Err` is reserved for unrecoverable conditions such as an
/// internal invariant violation.
///
/// Each iteration checks cancellation, then walks every declared node in
/// topological order: a source drains into every downstream node, a processor
/// runs and forwards its output, and a sink writes whatever was staged for it.
/// Live stats publish to `options.live_stats` when it is `Some` so `GET /status`
/// sees current progress, then the run paces or exits according to the
/// workflow's [`RunMode`], which is never [`RunMode::Stream`] here:
/// [`BuiltWorkflow::run`] sends that to the stream runner.
///
/// ## Error policy
///
/// - Source errors: log WARN, increment `iteration_errors`, stop draining that
///   source this iteration, continue to the next node.
/// - Processor errors: log ERROR, increment `iteration_errors`, record
///   `saci_workflow_pass_errors_total`, skip its fan-out so a failed processor feeds nothing
///   downstream, still clear its dataset, continue.
/// - Sink errors: log ERROR, increment `iteration_errors`, continue.
/// - A `forward_into` or `append_record_batch` fan-out error: log WARN naming
///   both node ids, increment `iteration_errors`, continue.
///
/// `options.control` is the runner's cancellation token plus its pause gate.
pub(crate) async fn run(
    built: BuiltWorkflow,
    options: RunOptions,
) -> Result<WorkflowStats, SaciError> {
    let RunOptions {
        control: RunControl { cancel, pause },
        live_stats,
        state,
    } = options;
    let BuiltWorkflow {
        workflow_id,
        nodes,
        inspector,
        dlq,
        run: run_mode,
        flow: flow_plan,
        batch_resume,
        ..
    } = built;
    let mut dlq = dlq;
    let inspector = inspector.as_ref();
    let n = nodes.len();

    // One bound metrics handle for the workflow, so a write builds no
    // attribute; each node binds its own in `Nodes::from_built`.
    let workflow_metrics = crate::metrics::instruments().workflow(&workflow_id);

    // The nodes as parallel per-node vectors, so the borrow checker sees
    // disjoint fields: a processor step borrows `runtimes` and `datasets`
    // side by side, and `datasets.split_at_mut(i + 1)` gives a processor's own
    // dataset and a downstream one as two non-overlapping slices. Destructured
    // into locals of the same names.
    let nodes = Nodes::from_built(nodes);
    // A source on a path to a windowed node keeps every arrival whole; see
    // `runner::whole_arrivals` for the rule.
    let whole_arrivals: Vec<bool> = runner::whole_arrivals(&nodes);
    let Nodes {
        ids,
        components,
        downstream,
        kinds,
        mut sources,
        runtimes,
        mut datasets,
        mut sinks,
        mut node_stats,
        source_metrics,
        processor_metrics,
        sink_metrics,
        recovered,
        #[cfg(feature = "windows")]
        mut trackers,
        mut staged,
    } = nodes;

    let mut stats = WorkflowStats::default();
    let start = Instant::now();

    #[cfg(feature = "tracing")]
    tracing::info!(workflow = %workflow_id, mode = ?run_mode, "standalone runner starting");
    #[cfg(not(feature = "tracing"))]
    let _ = &workflow_id;
    // Interval/one-shot processor state carry, opt-in per store: with
    // `store "redb" { batch_resume #true }` the runner threads the processor
    // state blob as `prior` and persists it, so a restarted service resumes
    // from its last save point. Default (no store or no flag): per-call
    // fresh-Store behaviour. The workflow carries the decision.
    let mut prior: Vec<Option<Vec<u8>>> = vec![None; n];
    if batch_resume && let Some(client) = &state {
        for i in 0..n {
            if matches!(kinds[i], NodeRunKind::Processor) {
                prior[i] = client.load_prior(&workflow_id, &ids[i]).await?;
            }
        }
    }
    // A windowed processor node carries its state across passes whatever the
    // store says. A `window` block is the declaration that this node
    // accumulates event-time state over more than one pass, and a backlog
    // reaches it over several passes: dropping the blob between them
    // would leave every pass aggregating from an empty accumulator and a
    // watermark of `i64::MIN`, so no window would ever close. Threading it is
    // in-memory and free; *persisting* it is still `batch_resume`'s decision,
    // so a windowed node with no store keeps its accumulator for the life of
    // the process and no longer.
    #[cfg(feature = "windows")]
    let state_carry: Vec<bool> = (0..n)
        .map(|i| batch_resume || trackers[i].is_some())
        .collect();
    #[cfg(not(feature = "windows"))]
    let state_carry: Vec<bool> = vec![batch_resume; n];

    // One controller per source node, resolved from the config's top-level
    // `flow_control` block and each source's own override.
    //
    // `RunMode::Once` engages none: a single pass must drain every source
    // by definition, so admitting a credit-sized prefix and exiting would
    // silently drop the rest of the source. Every other batch mode gets one.
    // The workflow carries the resolved policy; `flow_plan` was bound above.
    let mut controllers: Vec<Option<FlowController>> = (0..n)
        .map(|i| {
            (run_mode != RunMode::Once && kinds[i] == NodeRunKind::Source).then(|| {
                let settings = flow_plan.for_source(&ids[i]);
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
    // The unconsumed tail of the last batch each source produced, admitted
    // first on the next iteration.
    let mut carry: Vec<Option<Carry>> = (0..n).map(|_| None).collect();
    let mut admitted_rows: Vec<u64> = vec![0; n];
    let mut admitted_bytes: Vec<u64> = vec![0; n];
    // Time each source spent *waiting* for input, so a connector's blocking
    // poll never counts as consumer time. The fan-out that follows a pull is
    // consumer work and stays in the measurement.
    let mut drain_elapsed: Vec<Duration> = vec![Duration::ZERO; n];
    // Whether each source's own drain raised an error this iteration. A
    // source's failure is evidence about that source, not about the peers
    // sharing the workflow with it.
    let mut source_failed: Vec<bool> = vec![false; n];

    // Set the moment any path finishes every sink, so the shared shutdown
    // below never finishes them twice and never skips them: `RunMode`'s two
    // pacing arms can cancel between passes, after the per-pass
    // `is_oneshot_final || cancelled_before_finish` finish check already ran
    // and found nothing due, so the shutdown below is the only place left
    // that can still finish them for that exit.
    let mut sinks_finished = false;

    // Labelled for the interval pacing arm, which sits inside a second loop
    // and has to leave this one to reach the shutdown bookkeeping below.
    'passes: loop {
        // The pause point, ahead of the span below: the inspector times a
        // `workflow.batch` from `on_new_span` to `on_close`, so parking inside
        // one would report the whole paused wall clock as a single iteration
        // in the traces tab. Between passes, so staged batches, carry-over
        // slices, flow controllers and window trackers are all intact while
        // the runner is parked, and a cancel arriving meanwhile releases the
        // gate straight into the drain below.
        pause.park_while_paused(&cancel).await;

        // One root span per iteration, which is the trace the dashboard draws.
        // Children are created inside `batch_span.in_scope(...)`, so the batch
        // span is their contextual parent, which is the only form the
        // subscriber's sampler can follow. `in_scope` is synchronous and holds
        // no guard across an await: an entered guard held across one would
        // adopt every span the runtime opens on this thread meanwhile.
        //
        // `debug`, not `info`: one tree of these opens per iteration, and the
        // default `log_level="error"` materialises no span at all.
        // `log_level="debug"` brings the per-iteration traces back; `"info"`
        // gives the `pipeline.run`-rooted ones only. Every error event below
        // therefore names its own workflow, iteration and node rather than
        // leaning on these fields.
        #[cfg(feature = "tracing")]
        let batch_span = tracing::debug_span!(
            "workflow.batch",
            workflow = %workflow_id,
            iteration = stats.iterations + 1,
            rows = tracing::field::Empty
        );

        let log = PassLog {
            workflow_id: &workflow_id,
            iteration: stats.iterations + 1,
            #[cfg(feature = "tracing")]
            span: &batch_span,
        };

        if cancel.is_cancelled() {
            #[cfg(feature = "tracing")]
            tracing::info!(parent: &batch_span, "standalone runner cancelled, draining in-flight work");
            let flush = flush_and_finish_all(
                &mut sinks,
                &mut staged,
                &ids,
                &sink_metrics,
                &components,
                &workflow_id,
                &mut node_stats,
                &mut stats,
                dlq.as_mut(),
            );
            #[cfg(feature = "tracing")]
            flush.instrument(batch_span.clone()).await;
            #[cfg(not(feature = "tracing"))]
            flush.await;
            sinks_finished = true;
            break;
        }

        // The replay point a `replay "before_sources"` block asks for: after
        // the pause gate and the cancellation drain, before the first source
        // is pulled, so a replayed letter reaches its sink ahead of anything
        // new.
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

        let iter_start = Instant::now();
        // Per-iteration progress, so `debug` alongside the span tree it
        // annotates; the runner's start and shutdown lines stay at `info`.
        #[cfg(feature = "tracing")]
        tracing::debug!(parent: &batch_span, workflow = %workflow_id, iteration = stats.iterations + 1, mode = ?run_mode, "iteration starting");

        let mut total_rows_in: u64 = 0;
        let mut cancelled_mid_pass = false;
        let errors_before = stats.iteration_errors;
        // Counted alongside `stats.iteration_errors` so the shared consumer
        // chain's own errors can be told apart from a source's drain error
        // without depending on where the drains sit in topological order.
        let mut drain_errors: u64 = 0;
        let mut backlogged = false;
        admitted_rows.fill(0);
        admitted_bytes.fill(0);
        drain_elapsed.fill(Duration::ZERO);
        source_failed.fill(false);

        for i in 0..n {
            match kinds[i] {
                NodeRunKind::Source => {
                    let component =
                        components[i].expect("a source node always declares a component");
                    let metrics = source_metrics[i]
                        .as_ref()
                        .expect("a source node binds its metrics");
                    #[cfg(feature = "tracing")]
                    let drain_span = batch_span.in_scope(|| {
                        tracing::debug_span!(
                            "source.drain",
                            workflow = %workflow_id,
                            source = %ids[i],
                            component,
                            rows = tracing::field::Empty
                        )
                    });
                    let mut source_rows: u64 = 0;
                    // `None` outside the credit modes; `enabled() == false`
                    // keeps the drain-to-EOF shape below.
                    let credit = controllers[i].as_ref().is_some_and(FlowController::enabled);
                    let target = controllers[i]
                        .as_ref()
                        .map_or(usize::MAX, FlowController::target_rows);

                    loop {
                        let remaining = if credit {
                            target.saturating_sub(source_rows as usize)
                        } else {
                            usize::MAX
                        };
                        if remaining == 0 {
                            // The credit is spent while the source has not
                            // reported EOF, so work may still be waiting:
                            // this iteration is backlogged and skips pacing.
                            backlogged = true;
                            break;
                        }

                        // A tail left by the previous iteration is admitted
                        // before anything new is pulled, which is what keeps
                        // re-chunking order-preserving. Only a fresh pull is
                        // an arrival; a carried tail continues one already
                        // counted.
                        let mut fresh_batch = true;
                        let (batch, bytes_per_row) = match carry[i].take() {
                            Some(carried) => {
                                fresh_batch = false;
                                (carried.batch, carried.bytes_per_row)
                            }
                            None => {
                                let source =
                                    sources[i].as_mut().expect("source node keeps its source");
                                // Withheld on a path to a windowed node: the
                                // hint would let the credit size the arrival
                                // at the connector, which is the pass
                                // boundary that node observes event time at.
                                if credit && !whole_arrivals[i] {
                                    source.request_batch_rows(remaining);
                                }
                                // Only the wait for input is subtracted from
                                // the pass: the fan-out below is consumer work
                                // whose cost scales with the credit, and
                                // hiding it would bias the experiment toward
                                // larger chunks.
                                let wait_start = Instant::now();
                                let next = tokio::select! {
                                    r = source.next_batch() => Some(r),
                                    _ = cancel.cancelled() => None,
                                };
                                drain_elapsed[i] += wait_start.elapsed();
                                let Some(result) = next else {
                                    #[cfg(feature = "tracing")]
                                    tracing::info!(parent: &batch_span, "standalone runner cancelled during source drain");
                                    cancelled_mid_pass = true;
                                    break;
                                };
                                match result {
                                    Ok(None) => break,
                                    Ok(Some(batch)) => {
                                        // Weighed only for a source a controller
                                        // governs: the weight feeds its byte
                                        // accounting and nothing else.
                                        let weight = if controllers[i].is_some() {
                                            Carry::weigh(&batch)
                                        } else {
                                            0.0
                                        };
                                        (batch, weight)
                                    }
                                    Err(_e) => {
                                        #[cfg(feature = "tracing")]
                                        tracing::warn!(
                                            parent: &batch_span,
                                            workflow = %workflow_id,
                                            iteration = stats.iterations + 1,
                                            source = %ids[i],
                                            error = %_e,
                                            "source drain error (continuing)"
                                        );
                                        stats.iteration_errors += 1;
                                        drain_errors += 1;
                                        source_failed[i] = true;
                                        node_stats[i].errors += 1;
                                        break;
                                    }
                                }
                            }
                        };

                        // `slice` is zero-copy: the chunk and the tail share
                        // the batch's buffers, so nothing is copied and no row
                        // is duplicated or reordered. A source feeding a
                        // windowed node is never sliced: it overshoots its
                        // credit by the arrival's tail instead, and the
                        // `remaining == 0` check above ends the drain on the
                        // next turn of the loop.
                        let batch = if !whole_arrivals[i] && batch.num_rows() > remaining {
                            let tail = batch.slice(remaining, batch.num_rows() - remaining);
                            carry[i] = Some(Carry {
                                batch: tail,
                                bytes_per_row,
                            });
                            backlogged = true;
                            batch.slice(0, remaining)
                        } else {
                            batch
                        };

                        let rows = batch.num_rows() as u64;
                        source_rows += rows;
                        total_rows_in += rows;
                        admitted_rows[i] += rows;
                        admitted_bytes[i] += (rows as f64 * bytes_per_row) as u64;
                        stats.rows_processed += rows;
                        node_stats[i].rows += rows;
                        metrics.rows(rows);
                        // Rows are counted per chunk so the totals match what
                        // passed through the workflow; the arrival counters
                        // count source batches, so a batch split into chunks
                        // is one arrival, not several.
                        if fresh_batch {
                            stats.source_batches_drained += 1;
                            node_stats[i].batches += 1;
                            metrics.batch();
                        }

                        fan_out_source(
                            i,
                            &downstream,
                            component,
                            &batch,
                            &kinds,
                            &mut datasets,
                            &mut staged,
                            &ids,
                            &mut stats,
                            log,
                        );
                    }

                    #[cfg(feature = "tracing")]
                    drain_span.record("rows", source_rows);
                    #[cfg(not(feature = "tracing"))]
                    let _ = source_rows;

                    if cancelled_mid_pass {
                        break;
                    }
                }

                NodeRunKind::Processor => {
                    let metrics = processor_metrics[i]
                        .as_ref()
                        .expect("a processor node binds its metrics");
                    // The fan-in merge is complete once every upstream node has
                    // run: sources appended their batches directly and upstream
                    // processors forwarded their datasets. A windowed node's
                    // watermark therefore advances from everything this
                    // iteration delivered, before the runtime sees the batch.
                    #[cfg(feature = "windows")]
                    advance_watermark(
                        i,
                        &mut trackers,
                        &mut datasets,
                        &ids,
                        metrics,
                        &mut stats,
                        log,
                    );

                    let rows_in = datasets[i]
                        .as_ref()
                        .expect("processor node keeps its dataset")
                        .rows() as u64;
                    // `runtime.run` is the seam the out-of-process runtimes hang
                    // from: it is the contextual parent of a native pipeline's
                    // `pipeline.run` and of a processor's host-side
                    // `processor.batch`.
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
                    let prior_blob = if state_carry[i] {
                        prior[i].as_deref()
                    } else {
                        None
                    };
                    let run = runtime.run_batch(dataset, prior_blob);
                    #[cfg(feature = "tracing")]
                    let run = run.instrument(run_span.clone());
                    let run_result = tokio::select! {
                        r = run => Some(r),
                        _ = cancel.cancelled() => None,
                    };

                    let Some(run_result) = run_result else {
                        #[cfg(feature = "tracing")]
                        tracing::info!(parent: &batch_span, "standalone runner cancelled during runtime run");
                        cancelled_mid_pass = true;
                        break;
                    };

                    match run_result {
                        Ok(out) => {
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
                            // `out.state` is threaded whenever this node
                            // carries state across passes: a windowed node
                            // always, any other only under
                            // `store "redb" { batch_resume true }`, which is
                            // also the only thing that persists it. Otherwise
                            // it is discarded, keeping today's per-call
                            // fresh-Store behaviour.
                            if state_carry[i] {
                                prior[i] = out.state;
                                if batch_resume && let Some(client) = &state {
                                    let result = match &prior[i] {
                                        Some(blob) => {
                                            client.save_prior(&workflow_id, &ids[i], blob).await
                                        }
                                        None => client.delete_prior(&workflow_id, &ids[i]).await,
                                    };
                                    if let Err(_e) = result {
                                        #[cfg(feature = "tracing")]
                                        tracing::warn!(
                                            parent: &batch_span,
                                            workflow = %workflow_id,
                                            iteration = stats.iterations + 1,
                                            processor = %ids[i],
                                            error = %_e,
                                            "persisting processor state failed (continuing)"
                                        );
                                        #[cfg(not(feature = "tracing"))]
                                        let _ = _e;
                                    }
                                }
                            } else {
                                let _ = out.state;
                            }

                            deliver_outbound(
                                i,
                                &out.routes,
                                rows_out,
                                metrics,
                                &downstream,
                                &kinds,
                                &components,
                                &mut datasets,
                                &mut staged,
                                &ids,
                                &mut stats,
                                log,
                            );
                        }
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(
                                parent: &run_span,
                                workflow = %workflow_id,
                                iteration = stats.iterations + 1,
                                processor = %ids[i],
                                error = %_e,
                                "processor error (continuing, skipping fan-out)"
                            );
                            stats.iteration_errors += 1;
                            node_stats[i].errors += 1;
                            workflow_metrics.pass_error();
                            // Fall through to clear() without fanning out.
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
                    let sink = sinks[i].as_mut().expect("sink node keeps its sink");
                    let metrics = sink_metrics[i]
                        .as_ref()
                        .expect("a sink node binds its metrics");
                    let write = write_staged(
                        sink.as_mut(),
                        &mut staged[i],
                        &ids[i],
                        metrics,
                        component,
                        &workflow_id,
                        &mut node_stats[i],
                        &mut stats,
                        dlq.as_mut(),
                        None,
                    );
                    #[cfg(feature = "tracing")]
                    write.instrument(write_span.clone()).await;
                    #[cfg(not(feature = "tracing"))]
                    write.await;
                    #[cfg(feature = "tracing")]
                    write_span.record("rows", rows_before);
                    #[cfg(not(feature = "tracing"))]
                    let _ = rows_before;
                }
            }
        }

        if cancelled_mid_pass {
            let flush = flush_and_finish_all(
                &mut sinks,
                &mut staged,
                &ids,
                &sink_metrics,
                &components,
                &workflow_id,
                &mut node_stats,
                &mut stats,
                dlq.as_mut(),
            );
            #[cfg(feature = "tracing")]
            flush.instrument(batch_span.clone()).await;
            #[cfg(not(feature = "tracing"))]
            flush.await;
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
            log_flow_finish(&workflow_id, &ids, &controllers, &flow_episodes);
            return Ok(stats);
        }

        // The replay point a `replay "after_sources"` block asks for: after
        // the last node wrote, before pacing, so new arrivals go first and
        // the replay uses what is left of the pass. Past the cancellation
        // exit above, so a shutdown never opens the store.
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

        #[cfg(feature = "tracing")]
        batch_span.record("rows", total_rows_in);
        #[cfg(not(feature = "tracing"))]
        let _ = total_rows_in;

        // Feed the pass back to every source controller.
        //
        // The measurement is the consumer chain's time, not the iteration's:
        // waiting for input is not consumption, and a source with a
        // one-second poll timeout would otherwise report its poll timeout as
        // its throughput and breach any latency objective on every pass. Each
        // source's wait for input is timed separately and subtracted.
        //
        // What remains is attributed to each source in proportion to the rows
        // it admitted. The processors and sinks are one shared resource, so
        // their throughput is one number: proportional attribution is what
        // makes every controller see that same rows per second whatever the
        // split of the admission was.
        let pass_elapsed = iter_start.elapsed();
        let total_wait: Duration = drain_elapsed.iter().sum();
        let consume_elapsed = pass_elapsed.saturating_sub(total_wait);
        let total_admitted: u64 = admitted_rows.iter().sum();
        // An error in the shared chain (a processor, a sink, a fan-out append)
        // is evidence about every source feeding it. A source's own drain
        // error is evidence about that source alone: backing its healthy peers
        // off for it would walk a whole workflow down to `min_rows` because
        // one connector is flapping, and would report the back-offs against
        // sources that never failed.
        let chain_errors = stats.iteration_errors - errors_before > drain_errors;
        // The backlog belongs to the shared consumer chain, so every
        // controller sees the same number. Each sink's own figure is published
        // under its own id: the maximum is what governs admission, but a
        // backlog is measured against one sink's flush policy, so only the
        // per-sink value means anything to a reader.
        let sink_pending = report_sink_backlog(&sinks, &sink_metrics);
        for i in 0..n {
            let Some(controller) = controllers[i].as_mut() else {
                continue;
            };
            let metrics = source_metrics[i]
                .as_ref()
                .expect("a source node binds its metrics");
            let rows = admitted_rows[i];
            let elapsed = if rows == 0 || total_admitted == 0 {
                Duration::ZERO
            } else {
                consume_elapsed.mul_f64(rows as f64 / total_admitted as f64)
            };
            let outcome = if chain_errors || source_failed[i] {
                FlowOutcome::Error
            } else {
                FlowOutcome::Ok
            };
            let adjustment = controller.observe(FlowSample {
                rows,
                elapsed,
                bytes: admitted_bytes[i],
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
                metrics.flow_backoff();
            }
            record_flow_decision(
                inspector,
                &workflow_id,
                &ids[i],
                adjustment,
                &mut flow_episodes[i],
            );
            if let Some(_abandoned) = controller.take_latency_notice() {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    workflow = %workflow_id,
                    source = %ids[i],
                    abandoned = _abandoned,
                    "flow control latency objective unreachable at min_rows"
                );
            }
            // A disabled controller governs nothing: reporting its target as
            // the flow-control target would be indistinguishable from flow
            // control being on at that size.
            if controller.enabled() {
                metrics.flow_state(controller.target_rows(), controller.throughput());
            }
        }

        let is_oneshot_final = run_mode == RunMode::Once;
        let cancelled_before_finish = cancel.is_cancelled();
        if is_oneshot_final || cancelled_before_finish {
            let finish = finish_all_sinks(
                &mut sinks,
                &ids,
                &workflow_id,
                &mut node_stats,
                &mut stats,
                false,
            );
            #[cfg(feature = "tracing")]
            finish.instrument(batch_span.clone()).await;
            #[cfg(not(feature = "tracing"))]
            finish.await;
            sinks_finished = true;
        }

        stats.iterations += 1;
        workflow_metrics.pass();
        let iter_ms = iter_start.elapsed().as_millis() as u64;

        if let Some(shared) = &live_stats {
            stats.nodes = node_stats.clone();
            *shared.write().await = stats.clone();
        }

        // Every processor dataset was already cleared per-node above; a
        // source or sink node has none to clear.

        // Per-iteration progress, so `debug` alongside the span tree it
        // annotates; the runner's shutdown summary stays at `info`.
        #[cfg(feature = "tracing")]
        tracing::debug!(
            parent: &batch_span,
            workflow = %workflow_id,
            iteration = stats.iterations,
            rows_processed = stats.rows_processed,
            duration_ms = iter_ms,
            "iteration complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = iter_ms;

        // Close the trace here: run-mode pacing is the gap between iterations,
        // not part of one.
        #[cfg(feature = "tracing")]
        drop(batch_span);

        if cancelled_before_finish {
            #[cfg(feature = "tracing")]
            tracing::info!("standalone runner cancelled after runtime, clean exit");
            break;
        }

        // Pacing is the idle poll cadence, not a throughput cap: an iteration
        // that spent its credit with the source still live, or that left a
        // carry-over slice, re-enters at once instead of waiting out
        // `Continuous`'s 100 ms or `Interval`'s `every_ms`. The loop head
        // re-checks cancellation, so skipping the wait costs no
        // responsiveness. It skips a *wait*, never the one-shot exit: a single
        // pass must leave this loop whatever it left behind.
        match &run_mode {
            RunMode::Once => {
                #[cfg(feature = "tracing")]
                tracing::info!("one-shot mode: exiting after first iteration");
                break;
            }

            // A pending pause skips pacing for the same reason a backlog
            // does: the wait is idle cadence, and the loop head is where the
            // runner parks. Without this an `every_ms` of a minute would
            // leave a `pause` request reported as `pausing` for a minute.
            RunMode::Continuous | RunMode::Interval { .. } if backlogged || pause.is_paused() => {}

            RunMode::Continuous => {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {}
                    _ = cancel.cancelled() => {
                        #[cfg(feature = "tracing")]
                        tracing::info!("standalone runner cancelled during continuous pause");
                        break;
                    }
                }
            }

            RunMode::Interval { every_ms } => {
                let interval = tokio::time::Duration::from_millis(*every_ms);
                let deadline = tokio::time::Instant::now() + interval;

                loop {
                    // A pause requested mid-sleep settles within one slice.
                    if pause.is_paused() {
                        break;
                    }
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let slice = remaining.min(tokio::time::Duration::from_millis(100));
                    tokio::select! {
                        _ = tokio::time::sleep(slice) => {}
                        _ = cancel.cancelled() => {
                            #[cfg(feature = "tracing")]
                            tracing::info!("standalone runner cancelled during interval sleep");
                            break 'passes;
                        }
                    }
                }
            }

            // Dispatched to the stream runner by `BuiltWorkflow::run`.
            RunMode::Stream => unreachable!("stream mode never reaches the batch loop"),
        }
    }

    // Every exit from the loop above finishes the sinks exactly once, then
    // the sources, then the queue. Three paths already finished the sinks on
    // their way here and set `sinks_finished`: the loop-head cancellation
    // drain, the one-shot exit, and a cancel observed right after the
    // runtime ran. A cancel during either pacing arm (`Continuous`'s
    // `tokio::select!` or `Interval`'s inner sleep loop) leaves this pass's
    // `is_oneshot_final || cancelled_before_finish` check unset, because the
    // cancel lands *after* that check already found nothing to finish, so
    // this is the one place left that still owes the sinks their `finish`
    // before a source that consumed what it handed over is told to forget
    // it. The one path that `return`s from inside the loop, a cancel landing
    // mid-pass, calls both directly and never reaches here.
    if !sinks_finished {
        flush_and_finish_all(
            &mut sinks,
            &mut staged,
            &ids,
            &sink_metrics,
            &components,
            &workflow_id,
            &mut node_stats,
            &mut stats,
            dlq.as_mut(),
        )
        .await;
    }

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

    #[cfg(feature = "tracing")]
    tracing::info!(
        iterations = stats.iterations,
        rows_processed = stats.rows_processed,
        iteration_errors = stats.iteration_errors,
        total_duration_ms = stats.total_duration_ms,
        "standalone runner clean shutdown"
    );

    log_flow_finish(&workflow_id, &ids, &controllers, &flow_episodes);

    Ok(stats)
}

#[cfg(all(test, feature = "service", feature = "connector-channel"))]
mod tests {
    use super::*;
    use crate::SaciResult;
    use crate::connector::channel::{ChannelSink, ChannelSource};
    use crate::dataset::Dataset;
    use crate::inspector::Inspector;
    use crate::inspector_wire::FlowDecisionKind;
    use crate::io::{Sink, Source};
    use crate::pipeline::Pipeline;
    use crate::runtime::PipelineRuntime;
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, ServiceConfig, ServiceMode, StandaloneConfig,
    };
    use crate::service::flow::FlowPlan;
    #[cfg(feature = "metrics")]
    use crate::service::runner::decisions_for;
    use crate::service::sampling::FLOW_CONTROL_TARGET;
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn config(run_mode: RunMode) -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/saci-standalone-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig { run: run_mode },
            },
            workflows: vec![crate::service::config::WorkflowSpec {
                id: "w".to_string(),
                name: None,
                codecs: Vec::new(),
                schemas: Vec::new(),
                sources: Vec::new(),
                wasm: Vec::new(),
                plugin: Vec::new(),
                sinks: Vec::new(),
                links: Vec::new(),
                dlq: None,
            }],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn source_straight_to_sink_forwards_every_row() {
        let (source, tx) = ChannelSource::new(schema(), 8);
        let (sink, mut rx) = ChannelSink::new(schema(), 8);

        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        tx.send(batch.clone()).await.unwrap();
        drop(tx);

        let built = BuiltWorkflow::builder("w")
            .source("in", "V", source)
            .sink("out", "V", sink)
            .link("in", "out")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        let stats = built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");

        assert_eq!(stats.rows_processed, 3);
        assert_eq!(stats.sink_batches_written, 1);
        assert_eq!(stats.nodes.len(), 2);
        assert_eq!(&*stats.nodes[0].id, "in");
        assert_eq!(stats.nodes[0].rows, 3);
        assert_eq!(&*stats.nodes[1].id, "out");
        assert_eq!(stats.nodes[1].rows, 3);

        let received = rx.recv().await.expect("sink forwarded the batch");
        assert_eq!(received.num_rows(), 3);
    }

    #[tokio::test]
    async fn processor_entry_point_runs_with_an_empty_dataset() {
        let (sink, mut rx) = ChannelSink::new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            8,
        );

        struct OrderComponent;
        impl crate::component::Component for OrderComponent {
            fn name() -> &'static str {
                "Order"
            }
            fn schema() -> Arc<Schema> {
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
            }
        }

        let mut pipeline = Pipeline::new("p");
        pipeline.data_mut().register::<OrderComponent>().unwrap();

        let built = BuiltWorkflow::builder("w")
            .processor("p", pipeline)
            .sink("out", "Order", sink)
            .link("p", "out")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        let stats = built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds even though the processor starts from an empty dataset");

        assert_eq!(stats.iterations, 1);
        assert_eq!(stats.iteration_errors, 0);
        assert!(
            rx.try_recv().is_err(),
            "an identity run over zero rows writes nothing"
        );
    }

    /// A runtime that appends three rows to the batch dataset and reports a
    /// fixed routing decision, so the runner's delivery is what a test asserts.
    struct RoutingRuntime {
        routes: Option<Vec<String>>,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct V {
        v: i32,
    }

    impl crate::component::Component for V {
        fn name() -> &'static str {
            "V"
        }
        fn schema() -> Arc<Schema> {
            schema()
        }
    }

    #[async_trait(?Send)]
    impl PipelineRuntime for RoutingRuntime {
        fn name(&self) -> &str {
            "routing"
        }

        async fn run_batch(
            &self,
            data: &mut Dataset,
            _prior: Option<&[u8]>,
        ) -> SaciResult<crate::runtime::RuntimeOutput> {
            data.append::<V>(&[V { v: 1 }, V { v: 2 }, V { v: 3 }])?;
            Ok(crate::runtime::RuntimeOutput {
                state: None,
                routes: self.routes.clone(),
            })
        }

        fn template_dataset(&self) -> Dataset {
            let mut dataset = Dataset::new();
            dataset.register::<V>().expect("register V");
            dataset
        }
    }

    /// A one-processor workflow with two labelled sink edges, `a` and `b`.
    fn routing_built(
        routes: Option<Vec<String>>,
    ) -> (
        BuiltWorkflow,
        tokio::sync::mpsc::Receiver<RecordBatch>,
        tokio::sync::mpsc::Receiver<RecordBatch>,
    ) {
        let (sink_a, rx_a) = ChannelSink::new(schema(), 8);
        let (sink_b, rx_b) = ChannelSink::new(schema(), 8);
        let built = BuiltWorkflow::builder("w")
            .processor("p", RoutingRuntime { routes })
            .sink("out_a", "V", sink_a)
            .sink("out_b", "V", sink_b)
            .link_branch("p", "out_a", "a")
            .link_branch("p", "out_b", "b")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();
        (built, rx_a, rx_b)
    }

    #[tokio::test]
    async fn routing_processor_delivers_only_to_the_selected_branch() {
        let (built, mut rx_a, mut rx_b) = routing_built(Some(vec!["a".to_string()]));
        built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");

        let received = rx_a.recv().await.expect("sink a received the batch");
        assert_eq!(received.num_rows(), 3);
        assert!(
            rx_b.try_recv().is_err(),
            "sink b must not receive a batch the routing decision did not select"
        );
    }

    #[tokio::test]
    async fn routing_processor_can_route_to_nowhere() {
        let (built, mut rx_a, mut rx_b) = routing_built(Some(Vec::new()));
        built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");

        assert!(
            rx_a.try_recv().is_err(),
            "an empty routing decision delivers nowhere"
        );
        assert!(rx_b.try_recv().is_err());
    }

    #[tokio::test]
    async fn routing_processor_without_routes_multicasts_to_every_edge() {
        let (built, mut rx_a, mut rx_b) = routing_built(None);
        built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");

        assert_eq!(rx_a.recv().await.expect("sink a").num_rows(), 3);
        assert_eq!(rx_b.recv().await.expect("sink b").num_rows(), 3);
    }

    /// A runtime that counts the rows its dataset held when run began, so a
    /// test can assert how much fan-in merged before the call.
    struct RowCounter {
        rows_seen: Arc<std::sync::Mutex<u64>>,
    }

    #[async_trait(?Send)]
    impl PipelineRuntime for RowCounter {
        fn name(&self) -> &str {
            "row-counter"
        }

        async fn run_batch(
            &self,
            data: &mut Dataset,
            _prior: Option<&[u8]>,
        ) -> SaciResult<crate::runtime::RuntimeOutput> {
            *self.rows_seen.lock().unwrap() += data.rows() as u64;
            Ok(crate::runtime::RuntimeOutput::default())
        }

        fn template_dataset(&self) -> Dataset {
            let mut dataset = Dataset::new();
            dataset.register::<V>().expect("register V");
            dataset
        }
    }

    /// Two sources feeding one processor must merge into a single dataset
    /// before the processor runs: the windowing contract is that a processor
    /// receives the rows of every one of its inbound nodes in one batch.
    #[tokio::test]
    async fn two_sources_merge_into_one_processor() {
        let (source_a, tx_a) = ChannelSource::new(schema(), 8);
        let (source_b, tx_b) = ChannelSource::new(schema(), 8);
        let rows_seen = Arc::new(std::sync::Mutex::new(0u64));

        let batch_a = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let batch_b = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![3, 4, 5]))],
        )
        .unwrap();
        tx_a.send(batch_a).await.unwrap();
        tx_b.send(batch_b).await.unwrap();
        drop(tx_a);
        drop(tx_b);

        let built = BuiltWorkflow::builder("w")
            .source("a", "V", source_a)
            .source("b", "V", source_b)
            .processor(
                "p",
                RowCounter {
                    rows_seen: Arc::clone(&rows_seen),
                },
            )
            .link("a", "p")
            .link("b", "p")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        let stats = built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");

        assert_eq!(stats.rows_processed, 5);
        assert_eq!(
            *rows_seen.lock().unwrap(),
            5,
            "the processor must receive both sources' rows merged into one dataset"
        );
    }

    /// A mixed fan-in (one source and one upstream processor feeding the same
    /// downstream processor) must merge just like the all-sources case.
    #[tokio::test]
    async fn source_and_processor_fan_in_merge_into_one_processor() {
        let (source, tx) = ChannelSource::new(schema(), 8);
        let rows_seen = Arc::new(std::sync::Mutex::new(0u64));

        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![7, 8]))],
        )
        .unwrap();
        tx.send(batch).await.unwrap();
        drop(tx);

        // Upstream: appends three rows of its own on every run.
        struct Producer;
        #[async_trait(?Send)]
        impl PipelineRuntime for Producer {
            fn name(&self) -> &str {
                "producer"
            }
            async fn run_batch(
                &self,
                data: &mut Dataset,
                _prior: Option<&[u8]>,
            ) -> SaciResult<crate::runtime::RuntimeOutput> {
                data.append::<V>(&[V { v: 9 }, V { v: 10 }, V { v: 11 }])?;
                Ok(crate::runtime::RuntimeOutput::default())
            }
            fn template_dataset(&self) -> Dataset {
                let mut dataset = Dataset::new();
                dataset.register::<V>().expect("register V");
                dataset
            }
        }

        let built = BuiltWorkflow::builder("w")
            .source("s", "V", source)
            .processor("up", Producer)
            .processor(
                "down",
                RowCounter {
                    rows_seen: Arc::clone(&rows_seen),
                },
            )
            .link("s", "down")
            .link("up", "down")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        let stats = built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");

        assert_eq!(stats.rows_processed, 2);
        assert_eq!(
            *rows_seen.lock().unwrap(),
            5,
            "the downstream processor must see the source's 2 rows and the upstream's 3"
        );
    }

    /// A windowed processor node: the runner advances the node's watermark
    /// from the merged inbound timestamps, inserts the `WindowWatermark`
    /// resource for an in-process runtime to read, and records the
    /// `saci_window_watermark_seconds` series attributed to the node.
    #[cfg(feature = "windows")]
    #[tokio::test]
    async fn windowed_processor_tracks_watermark_from_merged_input() {
        use crate::window::WindowSpec;
        use crate::windows::WindowWatermark;

        fn trade_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("timestamp_ms", DataType::Int64, false),
                Field::new("price", DataType::Float64, false),
            ]))
        }

        let (source_a, tx_a) = ChannelSource::new(trade_schema(), 8);
        let (source_b, tx_b) = ChannelSource::new(trade_schema(), 8);
        let watermark_seen = Arc::new(std::sync::Mutex::new(i64::MIN));

        struct WatermarkReader {
            seen: Arc<std::sync::Mutex<i64>>,
        }
        #[async_trait(?Send)]
        impl PipelineRuntime for WatermarkReader {
            fn name(&self) -> &str {
                "watermark-reader"
            }
            async fn run_batch(
                &self,
                data: &mut Dataset,
                _prior: Option<&[u8]>,
            ) -> SaciResult<crate::runtime::RuntimeOutput> {
                if let Some(watermark) = data.get_resource::<WindowWatermark>() {
                    *self.seen.lock().unwrap() = watermark.as_ms();
                }
                Ok(crate::runtime::RuntimeOutput::default())
            }
            fn template_dataset(&self) -> Dataset {
                let mut dataset = Dataset::new();
                dataset.register_schema("Trade", trade_schema()).unwrap();
                dataset
            }
        }

        let batch_at = |ts: i64| {
            RecordBatch::try_new(
                trade_schema(),
                vec![
                    Arc::new(arrow_array::Int64Array::from(vec![ts]))
                        as Arc<dyn arrow_array::Array>,
                    Arc::new(arrow_array::Float64Array::from(vec![1.0]))
                        as Arc<dyn arrow_array::Array>,
                ],
            )
            .unwrap()
        };
        tx_a.send(batch_at(1_000)).await.unwrap();
        tx_a.send(batch_at(2_000)).await.unwrap();
        tx_b.send(batch_at(3_000)).await.unwrap();
        drop(tx_a);
        drop(tx_b);

        let built = BuiltWorkflow::builder("w")
            .source("a", "Trade", source_a)
            .source("b", "Trade", source_b)
            .processor(
                "p",
                WatermarkReader {
                    seen: Arc::clone(&watermark_seen),
                },
            )
            .window(
                "p",
                crate::service::config::WindowConfig {
                    spec: WindowSpec::tumbling(30_000),
                    time_field: "timestamp_ms".to_string(),
                    key_fields: Vec::new(),
                    allowed_lateness_ms: 0,
                },
            )
            .link("a", "p")
            .link("b", "p")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        built
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");

        assert_eq!(
            *watermark_seen.lock().unwrap(),
            3_000,
            "the watermark resource must carry the max merged timestamp"
        );

        // The series must carry the node's id, so the dashboard can attribute
        // the number to exactly this processor box.
        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            text.contains("saci_window_watermark_seconds") && text.contains("processor=\"p\""),
            "window watermark series missing from:\n{text}"
        );
    }

    /// A disabled controller governs nothing, so it must publish no
    /// `saci_flow_target_rows`. A gauge reading 4096 for a source the runner
    /// is draining to EOF is indistinguishable from flow control being on at
    /// 4096, which is the one question the series exists to answer.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_disabled_controller_publishes_no_flow_target() {
        async fn run_briefly(source_id: &str, enabled: bool) {
            let (source, tx) = ChannelSource::new(schema(), 8);
            let (sink, _rx) = ChannelSink::new(schema(), 8);
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            tx.send(batch).await.unwrap();
            drop(tx);

            let mut cfg = config(RunMode::Continuous);
            cfg.flow_control = crate::service::config::FlowControlConfig {
                enabled: Some(enabled),
                ..Default::default()
            };
            let built = BuiltWorkflow::builder("w")
                .source(source_id, "V", source)
                .sink("out", "V", sink)
                .link(source_id, "out")
                .run_mode(RunMode::Continuous)
                .flow(FlowPlan::from_config(&cfg))
                .build()
                .unwrap();

            // `Continuous` never exits on its own: one drained iteration, then
            // cancel out of the pacing sleep.
            let cancel = CancellationToken::new();
            let stopper = {
                let cancel = cancel.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    cancel.cancel();
                }
            };
            let (stats, ()) = tokio::join!(built.run(cancel.clone()), stopper);
            let stats = stats.expect("run succeeds");
            assert_eq!(stats.rows_processed, 3, "every row must still flow");
        }

        run_briefly("flow-on-src", true).await;
        run_briefly("flow-off-src", false).await;

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        let targets: Vec<&str> = text
            .lines()
            .filter(|line| line.starts_with("saci_flow_target_rows"))
            .collect();
        assert!(
            targets
                .iter()
                .any(|line| line.contains(r#"source="flow-on-src""#)),
            "an enabled controller must publish its target:\n{text}"
        );
        assert!(
            !targets
                .iter()
                .any(|line| line.contains(r#"source="flow-off-src""#)),
            "a disabled controller governs nothing and must publish no target:\n{text}"
        );
    }

    /// The flow-control lines are the one thing `log_level` cannot silence: a
    /// service running at `off` still says which sources it governs and where
    /// their search ended, and says nothing else.
    #[tokio::test]
    async fn flow_control_lines_are_emitted_at_log_level_off() {
        use crate::inspector::buffer::TimeBoundedBuffer;
        use crate::inspector::layer::InspectorLayer;
        use crate::inspector::record::{LogRecord, SpanRecord};
        use crate::service::logging::env_filter_for;
        use tracing_subscriber::prelude::*;

        // Keep a second dispatcher alive for the length of this test.
        //
        // `tracing` caches each callsite's interest process-wide, computed on
        // that callsite's first hit anywhere in the process. While at most one
        // dispatcher has ever been registered, `tracing_core`'s
        // `Dispatchers::rebuilder` takes its `JustOne` shortcut and asks
        // `get_default`, meaning the *thread* that got there first: a sibling
        // test driving a runner with no subscriber of its own latches
        // `log_flow_finish`'s line off for the rest of the binary, which one
        // process per test hides under nextest and a parallel `cargo test`
        // walks straight into. A second live dispatcher takes that shortcut
        // away, so a foreign first hit folds over the registered list, sees
        // this test's subscriber, and yields `sometimes` rather than `never`;
        // the per-event filter still decides what each thread captures.
        // Warming the callsites here instead would not work: `MAX_LEVEL`
        // starts at `OFF` and is only raised when a dispatcher registers, so
        // an `info!` reached before the guard below emits nothing at all.
        let _keepalive = tracing::Dispatch::new(tracing_subscriber::registry());

        let spans = TimeBoundedBuffer::<SpanRecord>::new(Duration::from_secs(60), 1024);
        let logs = TimeBoundedBuffer::<LogRecord>::new(Duration::from_secs(60), 1024);
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(env_filter_for("off", None))
                .with(InspectorLayer::new(spans, logs.clone())),
        );

        let (source, tx) = ChannelSource::new(schema(), 8);
        let (sink, _rx) = ChannelSink::new(schema(), 8);
        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        tx.send(batch).await.unwrap();
        drop(tx);

        let built = BuiltWorkflow::builder("w")
            .source("quiet-src", "V", source)
            .sink("out", "V", sink)
            .link("quiet-src", "out")
            .run_mode(RunMode::Continuous)
            .build()
            .unwrap();

        // `Continuous` never exits on its own: one drained iteration, then
        // cancel out of the pacing sleep.
        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                cancel.cancel();
            }
        };
        let (stats, ()) = tokio::join!(built.run(cancel.clone()), stopper);
        stats.expect("run succeeds");

        let captured = logs.read_recent();
        let flow: Vec<&LogRecord> = captured
            .iter()
            .filter(|record| record.target == FLOW_CONTROL_TARGET)
            .collect();
        assert_eq!(
            flow.len(),
            captured.len(),
            "the filter is off, so nothing but the flow lines may be captured: {captured:?}"
        );

        let starting: Vec<&&LogRecord> = flow
            .iter()
            .filter(|record| record.message == "flow control starting")
            .collect();
        let finished: Vec<&&LogRecord> = flow
            .iter()
            .filter(|record| record.message == "flow control finished")
            .collect();
        assert_eq!(starting.len(), 1, "one line per governed source: {flow:?}");
        assert_eq!(finished.len(), 1, "one line per governed source: {flow:?}");
        for record in starting.iter().chain(finished.iter()) {
            assert!(
                record
                    .fields
                    .iter()
                    .any(|(key, value)| key == "source" && value == "quiet-src"),
                "each line names its source: {:?}",
                record.fields
            );
        }
    }

    /// A source on a path to a windowed node, and a pass-through runtime for
    /// the windowed node itself.
    ///
    /// The runner reaches the source through
    /// [`reaches_windowed_node`](crate::service::windowing::reaches_windowed_node),
    /// which is what both flow-target tests below turn on.
    #[cfg(all(feature = "metrics", feature = "windows"))]
    fn windowed_service(
        source_id: &str,
        source: ChannelSource,
        run_mode: RunMode,
    ) -> BuiltWorkflow {
        use crate::window::WindowSpec;

        struct PassThrough;
        #[async_trait(?Send)]
        impl PipelineRuntime for PassThrough {
            fn name(&self) -> &str {
                "pass-through"
            }
            async fn run_batch(
                &self,
                _data: &mut Dataset,
                _prior: Option<&[u8]>,
            ) -> SaciResult<crate::runtime::RuntimeOutput> {
                Ok(crate::runtime::RuntimeOutput::default())
            }
            fn template_dataset(&self) -> Dataset {
                let mut dataset = Dataset::new();
                dataset.register_schema("Trade", windowed_schema()).unwrap();
                dataset
            }
        }

        BuiltWorkflow::builder("w")
            .source(source_id, "Trade", source)
            .processor("w-proc", PassThrough)
            .window(
                "w-proc",
                crate::service::config::WindowConfig {
                    spec: WindowSpec::tumbling(30_000),
                    time_field: "timestamp_ms".to_string(),
                    key_fields: Vec::new(),
                    allowed_lateness_ms: 0,
                },
            )
            .link(source_id, "w-proc")
            .run_mode(run_mode)
            .build()
            .unwrap()
    }

    #[cfg(all(feature = "metrics", feature = "windows"))]
    fn windowed_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]))
    }

    #[cfg(all(feature = "metrics", feature = "windows"))]
    fn windowed_batch(ts: i64) -> RecordBatch {
        RecordBatch::try_new(
            windowed_schema(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![ts])) as Arc<dyn arrow_array::Array>,
                Arc::new(arrow_array::Float64Array::from(vec![1.0])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap()
    }

    /// Stream mode governs a windowed-path source with nothing: the credit
    /// picks neither the slice (one arrival is one item) nor the fetch size
    /// (no `request_batch_rows`), so the source carries no controller and must
    /// publish no `saci_flow_target_rows`. A gauge there would name a size no
    /// code applies.
    #[cfg(all(feature = "metrics", feature = "windows"))]
    #[tokio::test]
    async fn a_windowed_path_source_publishes_no_flow_target_in_stream_mode() {
        let (source, tx) = ChannelSource::new(windowed_schema(), 8);
        tx.send(windowed_batch(1_000)).await.unwrap();
        drop(tx);

        // EOF on the second poll ends the rotation, so the run needs no
        // cancellation.
        let stats = windowed_service("stream-windowed-src", source, RunMode::Stream)
            .run(CancellationToken::new())
            .await
            .expect("run succeeds");
        assert_eq!(stats.rows_processed, 1, "the row must still flow");

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            !text
                .lines()
                .filter(|line| line.starts_with("saci_flow_target_rows"))
                .any(|line| line.contains(r#"source="stream-windowed-src""#)),
            "a windowed-path stream source has no controller and must publish no target:\n{text}"
        );
    }

    /// The batch runners keep the credit for a windowed-path source. It never
    /// slices the arrival, but a spent credit still ends the drain, so it
    /// decides how many whole arrivals one pass admits: the target is a real
    /// number there and the series must carry it.
    #[cfg(all(feature = "metrics", feature = "windows"))]
    #[tokio::test]
    async fn a_windowed_path_source_publishes_a_flow_target_in_continuous_mode() {
        let (source, tx) = ChannelSource::new(windowed_schema(), 8);
        tx.send(windowed_batch(1_000)).await.unwrap();
        drop(tx);

        // `Continuous` never exits on its own: one drained iteration, then
        // cancel out of the pacing sleep.
        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                cancel.cancel();
            }
        };
        let (stats, ()) = tokio::join!(
            windowed_service("continuous-windowed-src", source, RunMode::Continuous)
                .run(cancel.clone()),
            stopper
        );
        let stats = stats.expect("run succeeds");
        assert_eq!(stats.rows_processed, 1, "the row must still flow");

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            text.lines()
                .filter(|line| line.starts_with("saci_flow_target_rows"))
                .any(|line| line.contains(r#"source="continuous-windowed-src""#)),
            "the credit still bounds a batch pass, so its target must be published:\n{text}"
        );
    }

    /// A source whose every poll fails. Never reaches EOF, so a run ends by
    /// cancellation rather than by draining it.
    #[cfg(feature = "metrics")]
    struct AlwaysFailingSource {
        schema: Arc<Schema>,
    }

    #[cfg(feature = "metrics")]
    #[async_trait]
    impl Source for AlwaysFailingSource {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            Err(SaciError::generic("the source is down"))
        }
    }

    #[cfg(feature = "metrics")]
    async fn run_until_cancelled(built: BuiltWorkflow, millis: u64) {
        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(millis)).await;
                cancel.cancel();
            }
        };
        let (stats, ()) = tokio::join!(built.run(cancel), stopper);
        stats.expect("the run exits cleanly even though a source is failing");
    }

    #[cfg(feature = "metrics")]
    fn backed_off(metrics: &str, source: &str) -> bool {
        metrics
            .lines()
            .filter(|line| line.starts_with("saci_flow_backoff_total"))
            .any(|line| line.contains(&format!(r#"source="{source}""#)))
    }

    /// Whether the source published an adaptive target at all.
    #[cfg(feature = "metrics")]
    fn published_target(metrics: &str, source: &str) -> bool {
        metrics
            .lines()
            .filter(|line| line.starts_with("saci_flow_target_rows"))
            .any(|line| line.contains(&format!(r#"source="{source}""#)))
    }

    /// What a [`ProbeSink`] does to the pass that writes to it.
    #[cfg(feature = "metrics")]
    enum SinkBehaviour {
        /// Accepts everything and reports no backlog, so a test using it can
        /// only trip the guard it is actually about.
        Quiet,
        /// Accepts everything and reports a backlog one larger on every write,
        /// which is the trend `PRESSURE_TREND_PASSES` looks for.
        GrowingBacklog,
        /// Fails every write, an error in the chain every source shares.
        Failing,
    }

    /// A sink whose backpressure and failure behaviour a test picks.
    ///
    /// A fabricated `pending_rows` is the honest double here: the sink's
    /// number is the *input* to the pressure rule, not the thing under test,
    /// and no real sink grows its backlog on a schedule a test can predict.
    #[cfg(feature = "metrics")]
    struct ProbeSink {
        schema: Arc<Schema>,
        behaviour: SinkBehaviour,
        rows: Arc<std::sync::atomic::AtomicU64>,
        writes: std::sync::atomic::AtomicU64,
    }

    #[cfg(feature = "metrics")]
    impl ProbeSink {
        fn new(behaviour: SinkBehaviour) -> (Self, Arc<std::sync::atomic::AtomicU64>) {
            let rows = Arc::new(std::sync::atomic::AtomicU64::new(0));
            (
                Self {
                    schema: schema(),
                    behaviour,
                    rows: Arc::clone(&rows),
                    writes: std::sync::atomic::AtomicU64::new(0),
                },
                rows,
            )
        }
    }

    #[cfg(feature = "metrics")]
    #[async_trait]
    impl Sink for ProbeSink {
        async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
            if matches!(self.behaviour, SinkBehaviour::Failing) {
                return Err(SaciError::generic("the sink is down"));
            }
            self.rows.fetch_add(
                batch.num_rows() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        fn pending_rows(&self) -> Option<usize> {
            match self.behaviour {
                SinkBehaviour::GrowingBacklog => {
                    Some(self.writes.load(std::sync::atomic::Ordering::Relaxed) as usize)
                }
                _ => None,
            }
        }
    }

    /// A source holding one arrival of `rows` consecutive values, then EOF.
    #[cfg(feature = "metrics")]
    async fn one_big_arrival(rows: i32) -> ChannelSource {
        let (source, tx) = ChannelSource::new(schema(), 4);
        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(
                (0..rows).collect::<Vec<i32>>(),
            ))],
        )
        .expect("the arrival is well formed");
        tx.send(batch).await.expect("the source accepts it");
        drop(tx);
        source
    }

    /// A failing connector is evidence about its own source, not about the
    /// peers that happen to share a workflow with it. Backing every controller
    /// off for one flapping source walks a whole workflow down to `min_rows`
    /// and reports the back-offs against sources that never failed.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn one_sources_drain_error_does_not_back_off_its_healthy_peers() {
        let (healthy, tx) = ChannelSource::new(schema(), 8);
        let (sink, _rx) = ChannelSink::new(schema(), 8);
        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        tx.send(batch).await.unwrap();
        drop(tx);

        let built = BuiltWorkflow::builder("w")
            .source(
                "peers-failing-src",
                "V",
                AlwaysFailingSource { schema: schema() },
            )
            .source("peers-healthy-src", "V", healthy)
            .sink("peers-out", "V", sink)
            .link("peers-failing-src", "peers-out")
            .link("peers-healthy-src", "peers-out")
            .run_mode(RunMode::Continuous)
            .build()
            .unwrap();

        run_until_cancelled(built, 250).await;

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            backed_off(&text, "peers-failing-src"),
            "the source that failed must be the one charged for it:\n{text}"
        );
        assert!(
            !backed_off(&text, "peers-healthy-src"),
            "a healthy source must not be backed off for its peer's failure:\n{text}"
        );
    }

    /// A guard tripping at `min_rows` divides nothing, which is the right
    /// policy and the wrong silence: a source stuck against a wall at the
    /// floor is exactly what the counter exists to surface.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_guard_trip_at_min_rows_is_still_counted_as_a_back_off() {
        let (sink, _rx) = ChannelSink::new(schema(), 8);
        // One admissible size, which is therefore also the floor: every guard
        // trip has nowhere left to divide to.
        let mut cfg = config(RunMode::Continuous);
        cfg.flow_control = crate::service::config::FlowControlConfig {
            min_rows: Some(4_096),
            max_rows: Some(4_096),
            ..Default::default()
        };

        let built = BuiltWorkflow::builder("w")
            .source(
                "floor-guard-src",
                "V",
                AlwaysFailingSource { schema: schema() },
            )
            .sink("floor-out", "V", sink)
            .link("floor-guard-src", "floor-out")
            .run_mode(RunMode::Continuous)
            .flow(FlowPlan::from_config(&cfg))
            .build()
            .unwrap();

        run_until_cancelled(built, 250).await;

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            backed_off(&text, "floor-guard-src"),
            "a guard trip at the floor is still a back-off an operator must see:\n{text}"
        );
    }

    /// `RunMode::Once` engages no controller, and the reason is
    /// load-bearing rather than an optimisation: a single pass must drain
    /// every source by definition, so a credit-sized prefix would exit having
    /// silently dropped the rest of the arrival. The default credit is 4 096
    /// rows, so a 5 000-row arrival would lose 904 of them.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_one_shot_run_admits_a_whole_arrival_and_governs_nothing() {
        const ROWS: i32 = 5_000;

        let (sink, delivered) = ProbeSink::new(SinkBehaviour::Quiet);
        let built = BuiltWorkflow::builder("w")
            .source("one-shot-src", "V", one_big_arrival(ROWS).await)
            .sink("one-shot-out", "V", sink)
            .link("one-shot-src", "one-shot-out")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        let stats = built
            .run(CancellationToken::new())
            .await
            .expect("the one-shot run succeeds");

        assert_eq!(
            stats.rows_processed, ROWS as u64,
            "a one-shot pass must drain the arrival, not admit a credit's worth of it"
        );
        assert_eq!(
            delivered.load(std::sync::atomic::Ordering::Relaxed),
            ROWS as u64,
            "and every one of those rows must reach the sink"
        );
        assert_eq!(stats.iterations, 1, "one-shot is one pass");

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            !published_target(&text, "one-shot-src"),
            "a source nothing governs must publish no target:\n{text}"
        );
    }

    /// The other half of the attribution rule. A processor or sink failure is
    /// the shared chain failing, and every source feeding it is implicated,
    /// however cleanly its own connector behaved.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_shared_chain_failure_backs_off_every_feeding_source() {
        let (left, tx_left) = ChannelSource::new(schema(), 8);
        let (right, tx_right) = ChannelSource::new(schema(), 8);
        for tx in [&tx_left, &tx_right] {
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("the batch is well formed");
            tx.send(batch).await.expect("the source accepts it");
        }
        drop(tx_left);
        drop(tx_right);

        let (sink, _delivered) = ProbeSink::new(SinkBehaviour::Failing);
        let built = BuiltWorkflow::builder("w")
            .source("chain-left-src", "V", left)
            .source("chain-right-src", "V", right)
            .sink("chain-out", "V", sink)
            .link("chain-left-src", "chain-out")
            .link("chain-right-src", "chain-out")
            .run_mode(RunMode::Continuous)
            .build()
            .unwrap();

        run_until_cancelled(built, 250).await;

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        for source in ["chain-left-src", "chain-right-src"] {
            assert!(
                backed_off(&text, source),
                "the sink they share failed, so {source} must back off too:\n{text}"
            );
        }
    }

    /// `Sink::pending_rows` reaching the controller as `FlowSample::sink_pending`
    /// is wiring no unit test can see: the rule is a trend across passes, so it
    /// needs a runner driving real passes against a sink whose backlog really
    /// grows.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_growing_sink_backlog_backs_the_source_off() {
        let (sink, _delivered) = ProbeSink::new(SinkBehaviour::GrowingBacklog);
        // A credit well below the arrival, so one arrival is many passes and
        // the backlog has consecutive passes to grow on. The source never
        // errors and no chunk comes near the byte ceiling, so a back-off here
        // can only be the backlog trend.
        let mut cfg = config(RunMode::Continuous);
        cfg.flow_control = crate::service::config::FlowControlConfig {
            min_rows: Some(64),
            max_rows: Some(8_192),
            start_rows: Some(512),
            ..Default::default()
        };

        let built = BuiltWorkflow::builder("w")
            .source("backlog-src", "V", one_big_arrival(4_096).await)
            .sink("backlog-out", "V", sink)
            .link("backlog-src", "backlog-out")
            .run_mode(RunMode::Continuous)
            .flow(FlowPlan::from_config(&cfg))
            .build()
            .unwrap();

        run_until_cancelled(built, 250).await;

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            backed_off(&text, "backlog-src"),
            "a backlog growing on consecutive passes is congestion:\n{text}"
        );
    }

    /// The back-off an operator sees on the dashboard has to name the sink
    /// backlog that caused it and the rows either side of the division, not
    /// merely that something moved.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_backlog_back_off_is_recorded_with_its_rows_and_its_cause() {
        let inspector = Inspector::new(&crate::inspector::InspectorConfig::default());
        let (sink, _delivered) = ProbeSink::new(SinkBehaviour::GrowingBacklog);
        let mut cfg = config(RunMode::Continuous);
        cfg.flow_control = crate::service::config::FlowControlConfig {
            min_rows: Some(64),
            max_rows: Some(8_192),
            start_rows: Some(512),
            ..Default::default()
        };

        let built = BuiltWorkflow::builder("w")
            .source("recorded-backlog-src", "V", one_big_arrival(4_096).await)
            .sink("recorded-backlog-out", "V", sink)
            .link("recorded-backlog-src", "recorded-backlog-out")
            .run_mode(RunMode::Continuous)
            .flow(FlowPlan::from_config(&cfg))
            .inspector(inspector.clone())
            .build()
            .unwrap();

        run_until_cancelled(built, 250).await;

        let decisions = decisions_for(&inspector, "recorded-backlog-src");
        let backed_off = decisions
            .iter()
            .find(|decision| decision.kind == FlowDecisionKind::BackedOff)
            .unwrap_or_else(|| panic!("no back-off was recorded: {decisions:?}"));
        assert_eq!(backed_off.workflow, "w");
        assert_eq!(backed_off.from_rows, 512, "the guard tripped at start_rows");
        assert_eq!(
            backed_off.to_rows, 256,
            "the default backoff_factor of 2 halves it"
        );
        assert!(
            backed_off.reason.starts_with("sink backlog growing:")
                && backed_off.reason.contains("rows pending"),
            "the reason must name the backlog, not restate the kind: {:?}",
            backed_off.reason
        );
    }

    /// The converse of the stream runner's byte-ceiling case: a chunk whose
    /// *honest* weight really does project past `max_chunk_bytes` must divide
    /// the target.
    ///
    /// 512 four-byte rows are 2 KiB of buffer against a 1 KiB ceiling, so the
    /// margin is the row arithmetic and not Arrow's per-array bookkeeping: a
    /// ceiling the chunk cleared only by that ~100-byte overhead would flip
    /// the day arrow changed how it accounts for it. The halved 256-row chunk
    /// is still over, so the cooldown extends rather than the search
    /// resuming, which is what the guard is supposed to do against a wall
    /// this hard.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_chunk_that_really_exceeds_the_byte_ceiling_backs_the_source_off() {
        let (sink, _delivered) = ProbeSink::new(SinkBehaviour::Quiet);
        let mut cfg = config(RunMode::Continuous);
        cfg.flow_control = crate::service::config::FlowControlConfig {
            min_rows: Some(64),
            max_rows: Some(8_192),
            start_rows: Some(512),
            max_chunk_bytes: Some(1_024),
            ..Default::default()
        };

        let built = BuiltWorkflow::builder("w")
            .source("wide-chunk-src", "V", one_big_arrival(4_096).await)
            .sink("wide-chunk-out", "V", sink)
            .link("wide-chunk-src", "wide-chunk-out")
            .run_mode(RunMode::Continuous)
            .flow(FlowPlan::from_config(&cfg))
            .build()
            .unwrap();

        run_until_cancelled(built, 250).await;

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            backed_off(&text, "wide-chunk-src"),
            "512 four-byte rows is 2 KiB, twice the 1 KiB ceiling:\n{text}"
        );
    }

    /// The whole loop, end to end, against a real redb store: a refused batch
    /// is recorded, the next run replays it into a healthy sink, and the run
    /// after that finds the store empty because the replay consumed it.
    ///
    /// Nothing smaller proves this. `record` runs inside `write_staged`, the
    /// startup replay runs at the head of a pass, and the `consume` delete
    /// lands in `Source::finish`, so the three of them only meet in a runner.
    #[cfg(all(feature = "metrics", feature = "connector-redb"))]
    #[tokio::test]
    async fn a_refused_batch_is_recorded_replayed_and_then_gone() {
        use crate::service::dlq::{DeadLetterQueue, DlqShared};
        use crate::service::registry::Registry;

        let dir = tempfile::tempdir().expect("tempdir");
        let spec = crate::service::config::WorkflowSpec {
            id: "dlqwf".to_string(),
            name: None,
            codecs: Vec::new(),
            schemas: Vec::new(),
            sources: Vec::new(),
            wasm: Vec::new(),
            plugin: Vec::new(),
            sinks: Vec::new(),
            links: Vec::new(),
            dlq: Some(crate::service::config::DlqConfig(
                crate::service::config::DlqBlock::default(),
            )),
        };
        let mut registry = Registry::new();
        registry.register(crate::connector::redb::RedbConnector);
        let registry = Arc::new(registry);
        let shared = Arc::new(DlqShared::new(
            &spec.id,
            &crate::service::config::DlqBlock::default(),
        ));

        /// One pass over a workflow whose only sink behaves as asked.
        async fn pass(
            behaviour: SinkBehaviour,
            queue: DeadLetterQueue,
        ) -> (WorkflowStats, Arc<std::sync::atomic::AtomicU64>) {
            let (source, tx) = ChannelSource::new(schema(), 4);
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(arrow_array::Int32Array::from(vec![7, 8, 9]))],
            )
            .expect("the batch is well formed");
            tx.send(batch).await.expect("the source accepts it");
            drop(tx);

            let (sink, delivered) = ProbeSink::new(behaviour);
            let built = BuiltWorkflow::builder("dlqwf")
                .source("dlq-src", "V", source)
                .sink("dlq-out", "V", sink)
                .link("dlq-src", "dlq-out")
                .run_mode(RunMode::Once)
                .dlq(queue)
                .build()
                .unwrap();
            let stats = built
                .run(CancellationToken::new())
                .await
                .expect("the run exits cleanly even with a failing sink");
            (stats, delivered)
        }

        let build_queue = || {
            DeadLetterQueue::build(&spec, dir.path(), &registry, Arc::clone(&shared))
                .expect("the redb store opens")
        };

        // Run 1: the sink refuses, so the batch lands in the store.
        let (stats, delivered) = pass(SinkBehaviour::Failing, build_queue()).await;
        assert_eq!(stats.dead_letters_recorded, 1);
        assert_eq!(stats.dead_letters_replayed, 0);
        assert_eq!(delivered.load(std::sync::atomic::Ordering::Relaxed), 0);
        let summary = shared.summary();
        assert_eq!(summary.letters, 1);
        assert_eq!(summary.rows, 3);
        assert_eq!(summary.groups.len(), 1);
        assert_eq!(summary.groups[0].sink, "dlq-out");
        assert!(
            summary.groups[0].reason.contains("the sink is down"),
            "the group key is the sink's own error: {:?}",
            summary.groups[0].reason
        );

        // Run 2: the sink is healthy, so the startup replay delivers it.
        let (stats, delivered) = pass(SinkBehaviour::Quiet, build_queue()).await;
        assert_eq!(stats.dead_letters_replayed, 1);
        assert_eq!(stats.dead_letters_recorded, 0);
        assert_eq!(
            delivered.load(std::sync::atomic::Ordering::Relaxed),
            6,
            "three replayed rows plus the three this pass drained"
        );
        let summary = shared.summary();
        assert_eq!(summary.letters, 0);
        assert!(summary.known, "a clean drain read the store through");
        assert!(summary.groups.is_empty());

        // Run 3: `consume` deleted the entry, so there is nothing left.
        let (stats, delivered) = pass(SinkBehaviour::Quiet, build_queue()).await;
        assert_eq!(stats.dead_letters_replayed, 0);
        assert_eq!(stats.dead_letters_recorded, 0);
        assert_eq!(
            delivered.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "this pass delivered its own rows and nothing else"
        );
    }

    /// Where a [`LoggingSource`] or [`LoggingSink`] records a `finish()`
    /// call, so a test can assert the runner's promise that every sink
    /// finishes before any source does.
    #[cfg(feature = "metrics")]
    type FinishLog = Arc<std::sync::Mutex<Vec<&'static str>>>;

    /// A source over one queued arrival (or none) that counts its `finish()`
    /// calls and records each one in a shared [`FinishLog`].
    #[cfg(feature = "metrics")]
    struct LoggingSource {
        inner: ChannelSource,
        finish_calls: Arc<std::sync::atomic::AtomicU64>,
        log: FinishLog,
        fail_finish: bool,
    }

    #[cfg(feature = "metrics")]
    impl LoggingSource {
        fn new(inner: ChannelSource, log: FinishLog) -> (Self, Arc<std::sync::atomic::AtomicU64>) {
            let finish_calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
            (
                Self {
                    inner,
                    finish_calls: Arc::clone(&finish_calls),
                    log,
                    fail_finish: false,
                },
                finish_calls,
            )
        }

        /// Like [`Self::new`], but every `finish()` call fails.
        fn failing(
            inner: ChannelSource,
            log: FinishLog,
        ) -> (Self, Arc<std::sync::atomic::AtomicU64>) {
            let finish_calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
            (
                Self {
                    inner,
                    finish_calls: Arc::clone(&finish_calls),
                    log,
                    fail_finish: true,
                },
                finish_calls,
            )
        }
    }

    #[cfg(feature = "metrics")]
    #[async_trait]
    impl Source for LoggingSource {
        fn schema(&self) -> Arc<Schema> {
            self.inner.schema()
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            self.inner.next_batch().await
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            self.finish_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.log.lock().unwrap().push("source.finish");
            if self.fail_finish {
                Err(SaciError::generic("the source refused to finish"))
            } else {
                Ok(())
            }
        }
    }

    /// A sink that accepts every write and records each `finish()` call in a
    /// shared [`FinishLog`].
    #[cfg(feature = "metrics")]
    struct LoggingSink {
        schema: Arc<Schema>,
        log: FinishLog,
        finish_calls: Arc<std::sync::atomic::AtomicU64>,
    }

    #[cfg(feature = "metrics")]
    impl LoggingSink {
        fn new(log: FinishLog) -> (Self, Arc<std::sync::atomic::AtomicU64>) {
            let finish_calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
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

    #[cfg(feature = "metrics")]
    #[async_trait]
    impl Sink for LoggingSink {
        async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<(), SaciError> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            self.finish_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.log.lock().unwrap().push("sink.finish");
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }
    }

    /// One item queued for a [`LoggingSource`], EOF right after.
    #[cfg(feature = "metrics")]
    async fn one_item_source(log: FinishLog) -> (LoggingSource, Arc<std::sync::atomic::AtomicU64>) {
        let (source, tx) = ChannelSource::new(schema(), 4);
        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("the batch is well formed");
        tx.send(batch).await.expect("the source accepts it");
        drop(tx);
        LoggingSource::new(source, log)
    }

    /// A one-shot run drains its source, finishes its sink, and only then
    /// finishes its source: `finish_all_sources` runs after
    /// `flush_and_finish_all` on every exit path.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_one_shot_run_finishes_its_source_once_and_after_its_sink() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (source, source_finishes) = one_item_source(Arc::clone(&log)).await;
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let built = BuiltWorkflow::builder("w")
            .source("src", "V", source)
            .sink("out", "V", sink)
            .link("src", "out")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        let stats = built
            .run(CancellationToken::new())
            .await
            .expect("a healthy one-shot run exits cleanly");

        assert_eq!(stats.rows_processed, 3, "the one arrival drained");
        assert_eq!(
            source_finishes.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "finish runs exactly once"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["sink.finish", "source.finish"],
            "the sink is finished before the source that fed it"
        );
    }

    /// A cancel observed at the very top of the loop, before any pass ran,
    /// still drains the sink and then the source through the shared exit
    /// path.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_loop_head_cancel_before_any_pass_still_finishes_source_after_sink() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (source, source_finishes) = one_item_source(Arc::clone(&log)).await;
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let built = BuiltWorkflow::builder("w")
            .source("src", "V", source)
            .sink("out", "V", sink)
            .link("src", "out")
            .run_mode(RunMode::Continuous)
            .build()
            .unwrap();

        let cancel = CancellationToken::new();
        cancel.cancel();
        let stats = built
            .run(cancel)
            .await
            .expect("a pre-cancelled run exits cleanly");

        assert_eq!(stats.iterations, 0, "the cancel landed before any pass ran");
        assert_eq!(source_finishes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            *log.lock().unwrap(),
            vec!["sink.finish", "source.finish"],
            "the loop-head drain finishes the sink before the shared exit path finishes the source"
        );
    }

    /// A cancel that lands during `RunMode::Interval`'s pacing sleep, after
    /// a pass already completed, still finishes every sink before the
    /// source that fed it, exactly once each, through the shared exit path.
    ///
    /// `cancelled_before_finish` is read once, at the top of the
    /// per-iteration finish check, strictly before pacing runs, so a cancel
    /// that lands *during* the sleep never routes through
    /// `finish_sink`/`flush_and_finish_all` for that pass on its own; the
    /// shared post-loop fallback (`if !sinks_finished { .. }`) is what still
    /// closes it out.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_cancel_during_interval_pacing_still_finishes_the_source_once() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (source, source_finishes) = one_item_source(Arc::clone(&log)).await;
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let built = BuiltWorkflow::builder("w")
            .source("src", "V", source)
            .sink("out", "V", sink)
            .link("src", "out")
            .run_mode(RunMode::Interval { every_ms: 5_000 })
            .build()
            .unwrap();

        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                // Long enough that the first pass, and its
                // `cancelled_before_finish` check, completes before this
                // fires; short enough to land inside the interval sleep that
                // follows.
                tokio::time::sleep(Duration::from_millis(100)).await;
                cancel.cancel();
            }
        };
        let (stats, ()) = tokio::join!(built.run(cancel), stopper);
        let stats = stats.expect("cancelling mid-interval still exits cleanly");

        assert_eq!(
            stats.iterations, 1,
            "exactly the one pass before the cancel"
        );
        assert_eq!(
            source_finishes.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the shared exit path still finishes the source exactly once"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["sink.finish", "source.finish"],
            "the sink is finished before the source, even on this exit path"
        );
    }

    /// The same gap as `RunMode::Interval`'s pacing sleep, in
    /// `RunMode::Continuous`'s 100 ms `tokio::select!` arm: a cancel that
    /// lands there, after a pass already completed, still finishes every
    /// sink before the source that fed it, exactly once each.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_cancel_during_continuous_pacing_still_finishes_source_after_sink() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (source, source_finishes) = one_item_source(Arc::clone(&log)).await;
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let built = BuiltWorkflow::builder("w")
            .source("src", "V", source)
            .sink("out", "V", sink)
            .link("src", "out")
            .run_mode(RunMode::Continuous)
            .build()
            .unwrap();

        let cancel = CancellationToken::new();
        let stopper = {
            let cancel = cancel.clone();
            async move {
                // Long enough that the first pass, and its
                // `cancelled_before_finish` check, completes before this
                // fires; short enough to land inside the 100 ms continuous
                // pause that follows.
                tokio::time::sleep(Duration::from_millis(20)).await;
                cancel.cancel();
            }
        };
        let (stats, ()) = tokio::join!(built.run(cancel), stopper);
        let stats = stats.expect("cancelling mid-pause still exits cleanly");

        assert_eq!(
            stats.iterations, 1,
            "exactly the one pass before the cancel"
        );
        assert_eq!(
            source_finishes.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the shared exit path still finishes the source exactly once"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["sink.finish", "source.finish"],
            "the sink is finished before the source, even on this exit path"
        );
    }

    /// A source's `finish` error is counted like any other node error and
    /// does not fail the run.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn a_source_finish_error_is_counted_without_failing_the_run() {
        let log: FinishLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (chan_source, tx) = ChannelSource::new(schema(), 4);
        drop(tx);
        let (source, source_finishes) = LoggingSource::failing(chan_source, Arc::clone(&log));
        let (sink, _sink_finishes) = LoggingSink::new(Arc::clone(&log));

        let built = BuiltWorkflow::builder("w")
            .source("src", "V", source)
            .sink("out", "V", sink)
            .link("src", "out")
            .run_mode(RunMode::Once)
            .build()
            .unwrap();

        let stats = built
            .run(CancellationToken::new())
            .await
            .expect("a source finish error does not fail the run");

        assert_eq!(source_finishes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(stats.iteration_errors, 1);
        let src_stats = stats
            .nodes
            .iter()
            .find(|n| &*n.id == "src")
            .expect("the source node has stats");
        assert_eq!(src_stats.errors, 1);
    }
}
