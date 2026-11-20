//! In-process telemetry: ring buffers, a `tracing` layer, and a metric
//! exporter, read back through the control plane's JSON API.
//!
//! Nothing leaves the process. There is no collector to run, no scraper to
//! point at the service, and no storage to provision: spans, log events,
//! metric samples and flow-control decisions land in four
//! [`TimeBoundedBuffer`]s whose retention and capacity come from
//! [`InspectorConfig`], and `/api/*` reads them back.
//!
//! ## Wiring
//!
//! | Piece | Installed by |
//! |---|---|
//! | [`InspectorLayer`] | [`init_logging`](crate::service::init_logging), as one more layer on the existing registry |
//! | [`InMemoryMetricExporter`] | `saci-service serve`, as a second `PeriodicReader` on the shared `SdkMeterProvider` |
//! | [`Topology`] | [`ServiceBuilder::build`](crate::service::ServiceBuilder::build), once, through [`Inspector::set_topology`] |
//! | [`FlowDecision`] | the standalone and stream runners, through [`Inspector::record_flow_decision`], on each pass a source's admission target moved |
//!
//! Unlike the `crate::metrics::Instruments` set, this type is **not** process-global:
//! the router needs a handle and tests need isolated instances, so it is cloned
//! (every field is an `Arc`) and threaded explicitly.
//!
//! ## The `EnvFilter` and the sampler apply here too
//!
//! `init_logging` installs one subscriber-wide `EnvFilter`, and puts this
//! layer in the same sampled group as the format and OTLP layers. The default
//! `log_level="error"` materialises no span, so the traces tab is empty until
//! the level is raised, and a `RUST_LOG` that suppresses `crate` spans
//! empties the span buffer at any level; suppressing `crate` additionally
//! empties [`Snapshot::span_stats`]. That is the same caveat
//! `saci_stage_duration_seconds` already carries. A `sample_ratio` below 1.0
//! drops whole traces from this buffer exactly as it drops them from stdout.
//!
//! ## Which series drive which part of the dashboard
//!
//! Every name below already exists in [`crate::metrics`]; the inspector reads
//! them and introduces none.
//!
//! | `saci_workflow_passes_total` | left-rail run counter, summed over `workflow`, and, under `workflow="<id>"`, each workflow card's run badge. One per workflow *pass*, which under flow control is one credit-sized admission per source rather than one drain to EOF, so this number and the batch counters below have different denominators |
//! | `saci_workflow_pass_errors_total` | left-rail error badge, summed over `workflow`, and, under `workflow="<id>"`, each workflow card's error badge |
//! | `saci_stage_duration_seconds` | per-system latency, native runtime only |
//! | `saci_source_rows_total` | source node throughput, source-to-processor/sink edge rate, and a sink node's throughput when a source feeds it directly |
//! | `saci_source_batches_total` | source node detail |
//! | `saci_sink_batches_total` | sink node detail only. A batch is whatever row count the upstream handed over, so it rates nothing |
//! | `saci_sink_rows_total` | sink node throughput, and the processor-to-sink edge rate: those are the rows the runner handed to that sink, where the processor's rows-out counts its whole output dataset |
//! | `saci_sink_pending_rows` | sink node backlog, under `sink="<id>"` only |
//! | `saci_watchdog_ticks_total` | omitted: a watchdog-internal heartbeat is not a user-facing number |
//! | `saci_ready` | left-rail ready badge |
//! | `saci_uptime_seconds` | left-rail uptime |
//! | `saci_raft_commit_index`, `saci_raft_term`, `saci_raft_leader_id` | cluster facts row, shown only in cluster mode |
//! | `saci_processor_batch_duration_seconds` | processor node latency |
//! | `saci_processor_rows_in_total` | processor node rows in |
//! | `saci_processor_rows_out_total` | processor node rows out, the processor-to-processor edge rate, the labelled-edge rate under `branch`, and the processor-to-sink edge rate only before that sink has written |
//! | `saci_processor_systems_run_total` | processor node detail |
//! | `saci_processor_retries_total` | processor node retry badge |
//! | `saci_processor_custom` | processor-defined metric list in the node's detail sheet |
//! | `saci_window_watermark_seconds` | the live watermark in a windowed processor node's detail sheet, beside the window geometry, under `processor="<id>"` only |
//! | `saci_window_late_arrivals_total` | the dropped-arrival count in the same sheet section, beside the watermark, under `processor="<id>"` only. Absent while the count is zero, so a healthy node carries no row for it |
//! | `saci_flow_target_rows`, `saci_flow_throughput_rows_per_second`, `saci_flow_backoff_total` | source node detail: its admission target, its smoothed rate, and how often a guard has tripped |
//!
//! The three `saci_flow_*` series report where a source's admission control
//! stands. [`Snapshot::flow_decisions`] reports how it got there: one
//! [`FlowDecision`] per episode a source went through, carrying the rows
//! either side of the move and the evidence behind it, so the dashboard draws
//! each one as an event marker on the same time axis as the series history
//! and shows its `reason` verbatim. An episode, not a trip: a guard under
//! sustained pressure divides the target to the same size for the same reason
//! pass after pass, and only the pass that opened the run is recorded, which
//! is what keeps the sequence readable and the buffer small.
//! `saci_flow_backoff_total` counts the trips.
//!
//! Every series above that belongs to a node or a workflow is recorded once,
//! under that id alone: a sample names the node or workflow that produced it,
//! and a process-wide figure is the sum over the attribute. A workflow can
//! declare several nodes of one kind, and a process several workflows.
//! [`Snapshot::edges`] rates every edge from its upstream node's own series,
//! source or processor; an edge whose upstream node has not sampled yet is
//! omitted rather than reported as zero.

pub mod buffer;
mod config;
pub mod layer;
pub mod metric_exporter;
pub mod record;
mod series;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

pub use crate::inspector_wire as wire;
pub use buffer::TimeBoundedBuffer;
pub use config::InspectorConfig;
pub use layer::InspectorLayer;
pub use metric_exporter::InMemoryMetricExporter;

use crate::inspector_wire::{
    BufferStats, FlowDecision, LogRecord, MetricSample, Snapshot, SpanRecord, SpanStat, Topology,
    TraceDetail, TraceSummary,
};
use record::{level_rank, now_unix_ms};
use series::{edge_rates, summarize_series};

/// Span names [`Snapshot::span_stats`] groups.
const STAGE_SPAN: &str = "pipeline.stage";
const SYSTEM_SPAN: &str = "system.execute";

/// Handle on the process's in-memory telemetry.
///
/// Cheap to clone: every buffer is `Arc`-backed and clones share storage.
#[derive(Debug, Clone)]
pub struct Inspector {
    spans: TimeBoundedBuffer<SpanRecord>,
    logs: TimeBoundedBuffer<LogRecord>,
    samples: TimeBoundedBuffer<MetricSample>,
    flow_decisions: TimeBoundedBuffer<FlowDecision>,
    topology: Arc<OnceLock<Topology>>,
    started_at: Instant,
    ui_enabled: bool,
}

impl Inspector {
    /// Build the four buffers from `cfg`.
    pub fn new(cfg: &InspectorConfig) -> Self {
        let ttl = cfg.retention();
        Self {
            spans: TimeBoundedBuffer::new(ttl, cfg.max_spans),
            logs: TimeBoundedBuffer::new(ttl, cfg.max_logs),
            samples: TimeBoundedBuffer::new(ttl, cfg.max_samples),
            flow_decisions: TimeBoundedBuffer::new(ttl, cfg.max_flow_decisions),
            topology: Arc::new(OnceLock::new()),
            started_at: Instant::now(),
            ui_enabled: cfg.ui,
        }
    }

    /// Retain one adaptive flow-control decision.
    ///
    /// Called by a runner on the pass that decided, so the record's
    /// `at_unix_ms` is when the target moved rather than when the dashboard
    /// asked.
    pub fn record_flow_decision(&self, decision: FlowDecision) {
        self.flow_decisions.push(decision);
    }

    /// The `tracing` layer that fills the span and log buffers.
    pub fn layer(&self) -> InspectorLayer {
        InspectorLayer::new(self.spans.clone(), self.logs.clone())
    }

    /// The metric exporter that fills the sample buffer.
    pub fn metric_exporter(&self) -> InMemoryMetricExporter {
        InMemoryMetricExporter::new(self.samples.clone())
    }

    /// Whether `/ui` should be served.
    pub fn ui_enabled(&self) -> bool {
        self.ui_enabled
    }

    /// Publish the topology. Only the first call takes effect.
    ///
    /// One process runs one pipeline, so the topology is fixed once the service
    /// is built; a second call would mean two builds raced, which is a bug in
    /// the caller rather than something to merge.
    pub fn set_topology(&self, topology: Topology) {
        if self.topology.set(topology).is_err() {
            tracing::warn!("inspector topology already set; ignoring the second publication");
        }
    }

    /// The published topology, or an empty one before `set_topology` ran.
    pub fn topology(&self) -> Topology {
        self.topology.get().cloned().unwrap_or_default()
    }

    /// Everything the dashboard needs for one frame.
    ///
    /// `window` bounds how far back the series history reaches; `ready` is the
    /// service's own readiness flag, which the inspector does not own.
    pub fn snapshot(&self, window: Duration, ready: bool) -> Snapshot {
        let now_ms = now_unix_ms();
        let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        let cutoff = now_ms.saturating_sub(window_ms);

        let samples: Vec<MetricSample> = self
            .samples
            .read_recent()
            .into_iter()
            .filter(|s| s.at_unix_ms >= cutoff)
            .collect();

        let series = summarize_series(&samples);
        let topology = self.topology();
        let edges = edge_rates(&topology, &series);
        let span_stats = self.span_stats(cutoff);
        // The same cutoff the series histories use, so a marker and the point
        // it annotates are on one time axis.
        let decisions: Vec<FlowDecision> = self
            .flow_decisions
            .read_recent()
            .into_iter()
            .filter(|decision| decision.at_unix_ms >= cutoff)
            .collect();

        Snapshot {
            topology_version: topology.version,
            sampled_at_unix_ms: now_ms,
            uptime_secs: self.started_at.elapsed().as_secs(),
            ready,
            series,
            edges,
            span_stats,
            flow_decisions: decisions,
            buffers: BufferStats {
                spans: self.spans.len(),
                logs: self.logs.len(),
                samples: self.samples.len(),
                flow_decisions: self.flow_decisions.len(),
                // One total across every buffer, as the field documents: a
                // capacity eviction anywhere means the window the dashboard
                // reports is shorter than the one configured, and which
                // buffer lost the record does not change that answer.
                dropped: self.spans.dropped()
                    + self.logs.dropped()
                    + self.samples.dropped()
                    + self.flow_decisions.dropped(),
            },
        }
    }

    /// Newest-first trace list, at most `limit` entries.
    pub fn traces(&self, limit: usize) -> Vec<TraceSummary> {
        let spans = self.spans.read_recent();
        let mut errored: Vec<u64> = self
            .logs
            .read_recent()
            .into_iter()
            .filter(|log| log.level == "ERROR")
            .filter_map(|log| log.trace_id)
            .collect();
        errored.sort_unstable();
        errored.dedup();

        let mut roots: Vec<&SpanRecord> = Vec::new();
        let mut counts: HashMap<u64, usize> = HashMap::new();
        for span in &spans {
            *counts.entry(span.trace_id).or_default() += 1;
            if span.parent_id.is_none() {
                roots.push(span);
            }
        }

        let mut summaries: Vec<TraceSummary> = roots
            .into_iter()
            .map(|root| TraceSummary {
                trace_id: root.trace_id,
                name: root.name.clone(),
                started_unix_ms: root.started_unix_ms,
                duration_us: root.duration_us,
                span_count: counts.get(&root.trace_id).copied().unwrap_or(1),
                error: errored.binary_search(&root.trace_id).is_ok(),
            })
            .collect();

        summaries.sort_unstable_by_key(|summary| std::cmp::Reverse(summary.started_unix_ms));
        summaries.truncate(limit);
        summaries
    }

    /// Every retained span and log line belonging to one trace.
    ///
    /// Returns `None` when the trace has aged out, so the handler can answer
    /// 404 instead of an empty document that looks like a live but silent trace.
    pub fn trace(&self, trace_id: u64) -> Option<TraceDetail> {
        let mut spans: Vec<SpanRecord> = self
            .spans
            .read_recent()
            .into_iter()
            .filter(|span| span.trace_id == trace_id)
            .collect();
        if spans.is_empty() {
            return None;
        }
        spans.sort_unstable_by_key(|span| span.started_unix_ms);

        let mut logs: Vec<LogRecord> = self
            .logs
            .read_recent()
            .into_iter()
            .filter(|log| log.trace_id == Some(trace_id))
            .collect();
        logs.sort_unstable_by_key(|log| log.at_unix_ms);

        Some(TraceDetail { spans, logs })
    }

    /// Newest-first log lines, filtered at or above `min_level` when given.
    pub fn logs(&self, limit: usize, min_level: Option<&str>) -> Vec<LogRecord> {
        match min_level {
            None => self.logs.read_last(limit),
            Some(level) => {
                let max_rank = level_rank(level);
                self.logs
                    .read_last(usize::MAX)
                    .into_iter()
                    .filter(|log| level_rank(&log.level) <= max_rank)
                    .take(limit)
                    .collect()
            }
        }
    }

    /// Per-stage and per-system latency from the retained span records.
    fn span_stats(&self, cutoff_unix_ms: u64) -> Vec<SpanStat> {
        let mut grouped: HashMap<(&'static str, String), Vec<u64>> = HashMap::new();
        for span in self.spans.read_recent() {
            if span.started_unix_ms < cutoff_unix_ms {
                continue;
            }
            let (name, field) = match span.name.as_ref() {
                STAGE_SPAN => (STAGE_SPAN, "stage"),
                SYSTEM_SPAN => (SYSTEM_SPAN, "system"),
                _ => continue,
            };
            let Some((_, key)) = span.fields.iter().find(|(k, _)| k == field) else {
                continue;
            };
            grouped
                .entry((name, key.clone()))
                .or_default()
                .push(span.duration_us);
        }

        let mut stats: Vec<SpanStat> = grouped
            .into_iter()
            .map(|((span, key), mut durations)| {
                durations.sort_unstable();
                SpanStat {
                    span: std::borrow::Cow::Borrowed(span),
                    key,
                    count: durations.len(),
                    p50_us: percentile(&durations, 0.50),
                    p95_us: percentile(&durations, 0.95),
                    max_us: durations.last().copied().unwrap_or(0),
                }
            })
            .collect();
        stats.sort_unstable_by(|a, b| a.span.cmp(&b.span).then_with(|| a.key.cmp(&b.key)));
        stats
    }
}

/// Nearest-rank percentile of a sorted, non-empty slice.
fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let last = sorted.len() - 1;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "index is clamped into the slice below"
    )]
    let idx = ((last as f64) * q).round() as usize;
    sorted[idx.min(last)]
}

#[cfg(test)]
mod tests {
    use super::series::fixtures::topology_with;
    use super::*;
    use crate::inspector_wire::FlowDecisionKind;
    use std::borrow::Cow;

    fn config() -> InspectorConfig {
        InspectorConfig {
            retention_ms: 60_000,
            ..InspectorConfig::default()
        }
    }

    #[test]
    fn snapshot_reports_buffer_occupancy_and_topology_version() {
        let inspector = Inspector::new(&config());
        inspector.set_topology(topology_with(&["orders-in"], &["settlements"]));
        inspector.spans.push(SpanRecord {
            trace_id: 1,
            span_id: 1,
            parent_id: None,
            name: Cow::Borrowed("pipeline.run"),
            target: Cow::Borrowed("saci"),
            started_unix_ms: now_unix_ms(),
            duration_us: 10,
            fields: Vec::new(),
        });

        let snapshot = inspector.snapshot(Duration::from_secs(60), true);
        assert_eq!(snapshot.topology_version, 1);
        assert_eq!(snapshot.buffers.spans, 1);
        assert!(snapshot.ready);
        assert_eq!(
            snapshot.edges.len(),
            0,
            "no metric sample exists yet, so every edge is omitted rather than reported as zero"
        );
    }

    /// A decision marker is only meaningful on the axis the series histories
    /// draw, so the snapshot has to cut them at the same window the caller
    /// asked for rather than returning everything retained.
    #[test]
    fn snapshot_carries_only_the_decisions_inside_the_requested_window() {
        let inspector = Inspector::new(&config());
        let now = now_unix_ms();
        for (source, at) in [
            ("aged-out", now - 40_000),
            ("just-inside", now - 20_000),
            ("newest", now - 1_000),
        ] {
            inspector.record_flow_decision(FlowDecision {
                at_unix_ms: at,
                workflow: "w".to_string(),
                source: source.to_string(),
                kind: FlowDecisionKind::Grew,
                from_rows: 512,
                to_rows: 1_024,
                reason: "experiment won: 12.4k rows/s against 9.1k rows/s".to_string(),
            });
        }

        let inside = inspector.snapshot(Duration::from_secs(30), true);
        assert_eq!(
            inside
                .flow_decisions
                .iter()
                .map(|decision| decision.source.as_str())
                .collect::<Vec<_>>(),
            ["just-inside", "newest"],
            "oldest first, and the 40-second-old decision is outside a 30-second window"
        );
        assert_eq!(
            inside.buffers.flow_decisions, 3,
            "occupancy is what the buffer holds, not what one window returned"
        );

        let wider = inspector.snapshot(Duration::from_secs(60), true);
        assert_eq!(wider.flow_decisions.len(), 3);
    }

    #[test]
    fn span_stats_group_stage_records_by_stage_field() {
        let inspector = Inspector::new(&config());
        for (stage, duration) in [("0", 100u64), ("0", 300), ("1", 900)] {
            inspector.spans.push(SpanRecord {
                trace_id: 1,
                span_id: 2,
                parent_id: Some(1),
                name: Cow::Borrowed(STAGE_SPAN),
                target: Cow::Borrowed("saci::pipeline::execution"),
                started_unix_ms: now_unix_ms(),
                duration_us: duration,
                fields: vec![("stage".to_string(), stage.to_string())],
            });
        }

        let stats = inspector.span_stats(0);
        assert_eq!(stats.len(), 2, "one entry per stage index: {stats:?}");
        let first = stats.iter().find(|s| s.key == "0").expect("stage 0");
        assert_eq!(first.count, 2);
        assert_eq!(first.max_us, 300);
        let second = stats.iter().find(|s| s.key == "1").expect("stage 1");
        assert_eq!(second.p50_us, 900);
    }

    #[test]
    fn traces_are_newest_first_and_flag_errors() {
        let inspector = Inspector::new(&config());
        let base = now_unix_ms();
        for (trace, started) in [(10u64, base), (11, base + 50)] {
            inspector.spans.push(SpanRecord {
                trace_id: trace,
                span_id: trace,
                parent_id: None,
                name: Cow::Borrowed("pipeline.run"),
                target: Cow::Borrowed("saci"),
                started_unix_ms: started,
                duration_us: 5,
                fields: Vec::new(),
            });
        }
        inspector.logs.push(LogRecord {
            level: Cow::Borrowed("ERROR"),
            target: Cow::Borrowed("saci"),
            message: "boom".to_string(),
            at_unix_ms: base,
            span_id: Some(10),
            trace_id: Some(10),
            fields: Vec::new(),
        });

        let traces = inspector.traces(10);
        assert_eq!(traces.len(), 2);
        assert_eq!(traces[0].trace_id, 11, "newest first");
        assert!(!traces[0].error);
        assert!(traces[1].error, "trace 10 has an ERROR log");

        let detail = inspector.trace(10).expect("trace 10 retained");
        assert_eq!(detail.spans.len(), 1);
        assert_eq!(detail.logs.len(), 1);
        assert!(inspector.trace(999).is_none());
    }

    #[test]
    fn logs_filter_at_or_above_the_requested_level() {
        let inspector = Inspector::new(&config());
        for level in ["ERROR", "WARN", "INFO", "DEBUG"] {
            inspector.logs.push(LogRecord {
                level: Cow::Borrowed(level),
                target: Cow::Borrowed("test"),
                message: level.to_string(),
                at_unix_ms: now_unix_ms(),
                span_id: None,
                trace_id: None,
                fields: Vec::new(),
            });
        }

        let warn_and_above = inspector.logs(100, Some("warn"));
        assert_eq!(warn_and_above.len(), 2);
        assert_eq!(inspector.logs(100, None).len(), 4);
        assert_eq!(inspector.logs(1, None).len(), 1);
    }
}
