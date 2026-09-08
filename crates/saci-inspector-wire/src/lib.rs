//! The JSON contract between the `saci-service` inspector and its dashboard.
//!
//! Both sides of `/api/*` compile these types: the host serializes them from
//! `saci_service::inspector`, and the `saci-service-ui` WASM bundle deserializes
//! them in the browser. One definition, so the wire shape cannot drift.
//!
//! The crate carries `serde` and nothing else. `saci-service` itself cannot
//! compile for `wasm32-unknown-unknown` (its non-optional `saci-core` dependency
//! pulls tokio, and through it mio), so the shared types cannot live there
//! behind a feature: they need a crate the browser target can build.
//!
//! ## Conventions
//!
//! - Field names are `snake_case`, matching the Rust identifiers.
//! - Attribute and detail maps are arrays of two-element arrays, not JSON
//!   objects, so key order is part of the response and a client renders the
//!   same order the server produced.
//! - Every string that comes from `tracing` metadata is a
//!   [`Cow<'static, str>`](std::borrow::Cow): the host borrows the `&'static
//!   str` the macro already allocated, and the browser owns a decoded `String`.
//! - Timestamps are Unix milliseconds (`u64`), durations microseconds (`u64`)
//!   or fractional seconds (`f64`), never a locale-dependent string.

#![forbid(unsafe_code)]

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// A `(key, value)` pair as it appears on the wire: a two-element array.
pub type Pair = (String, String);

// ── Topology ─────────────────────────────────────────────────────────────────

/// The shape of the running service, as drawn by the dashboard.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// Bumped whenever the topology is replaced. A client compares it against
    /// [`Snapshot::topology_version`] to know its layout is stale.
    pub version: u64,
    /// `node.id` from config, as a string so a client never loses precision.
    pub node_id: String,
    /// `"standalone"` or `"cluster"`.
    pub mode: String,
    /// Every workflow this process runs, in declaration order.
    pub workflows: Vec<WorkflowTopology>,
    /// Cross-workflow channel bridges. Not part of any one workflow's `edges`:
    /// the two endpoints live in different workflows.
    #[serde(default)]
    pub bridges: Vec<BridgeEdge>,
}

/// The workflow: its declared identity, its nodes, and the edges between them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTopology {
    /// The workflow's declared id.
    pub id: String,
    /// Declared name, absent when the config named none.
    pub name: Option<String>,
    /// Every declared node, in topological order: a node always follows every
    /// node that links into it, within this workflow.
    pub nodes: Vec<TopoNode>,
    /// Every declared `link`.
    pub edges: Vec<TopoEdge>,
}

/// What executes a processor node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInfo {
    /// `"wasm"`, `"plugin"` or `"native"`.
    pub kind: String,
    /// The identity the runtime declares for itself: a processor's or plugin's
    /// own name, falling back to the host's pipeline name for a native runtime,
    /// which declares none.
    pub name: String,
    /// Version the runtime reports about itself; empty when it reports none.
    pub version: String,
    /// Whether the runtime carries state across batches.
    pub stateful: bool,
    /// Fingerprint of the runtime's component schemas; empty when it reports
    /// none.
    pub schema_fingerprint: String,
    /// Component names the runtime declares.
    pub declared_components: Vec<String>,
}

/// The windowing declaration of a processor node, as the dashboard reads it.
///
/// Mirrors the host's `WindowConfig` (which itself wraps the saci-core
/// `WindowSpec`) field for field, minus saci-core, because this crate is the
/// one type both the host and the browser can compile. The geometry fields are
/// `Option` because which ones apply depends on `kind`: tumbling and sliding
/// carry `size_ms`, sliding adds `slide_ms`, session carries `gap_ms`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WindowInfo {
    /// `"tumbling"` | `"sliding"` | `"session"`.
    pub kind: String,
    /// Fixed window size in milliseconds (tumbling, sliding).
    pub size_ms: Option<i64>,
    /// Slide interval in milliseconds (sliding).
    pub slide_ms: Option<i64>,
    /// Alignment offset in milliseconds (tumbling, sliding).
    pub offset_ms: Option<i64>,
    /// Session inactivity gap in milliseconds (session).
    pub gap_ms: Option<i64>,
    /// The event-time column every inbound component must carry.
    pub time_field: String,
    /// Grouping key columns, empty for a global window.
    pub key_fields: Vec<String>,
    /// How many milliseconds past the watermark a late row is still accepted.
    pub allowed_lateness_ms: i64,
}

/// One box in the dashboard graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopoNode {
    /// The declared id. Unique workflow-wide, so it is also the graph node id.
    pub id: String,
    /// `"source"` | `"processor"` | `"sink"`.
    pub kind: String,
    /// Declared name, absent when the config named none.
    pub name: Option<String>,
    /// Connector `type` for a source or sink, runtime kind for a processor.
    pub type_name: String,
    /// The component this node reads or writes. `None` for a processor.
    pub component: Option<String>,
    /// A processor's self-description. `None` for a source or sink.
    pub runtime: Option<RuntimeInfo>,
    /// The processor node's windowing declaration, when its config declares a
    /// `window` block. `None` for a non-windowed processor and for every
    /// source or sink. Defaults to `None` so a snapshot from a host without
    /// window support still decodes.
    #[serde(default)]
    pub window: Option<WindowInfo>,
    /// Allowlisted connector options for a source or sink, or a processor's
    /// version/stateful/artifact pairs. Never a blanket copy of the config
    /// table: a source's `config` holds DSNs and credentials.
    pub detail: Vec<Pair>,
}

/// A directed edge between two [`TopoNode`] ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopoEdge {
    /// Source node id.
    pub from: String,
    /// Destination node id.
    pub to: String,
    /// Branch name the edge carries, absent for an unlabelled link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// A named in-process channel joining a `ChannelSink` in one workflow to a
/// `ChannelSource` in another.
///
/// Distinct from [`TopoEdge`] because it is not a declared `link`: no config
/// names both ends, they meet on the shared channel `name` alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeEdge {
    /// The channel `name` both halves declare.
    pub channel: String,
    /// The `ChannelSink` node id, in the producing workflow.
    pub from: String,
    /// The `ChannelSource` node id, in the consuming workflow.
    pub to: String,
}

// ── Metrics ──────────────────────────────────────────────────────────────────

/// The attribute key carrying a source node's id on its metric series.
pub const SOURCE_ATTR: &str = "source";
/// The attribute key carrying a processor node's id on its metric series.
///
/// Attribution is additive: every processor metric is recorded once with no
/// attributes, which is the process-wide total a `/metrics` consumer has
/// always seen, and once more under `processor="<id>"`.
pub const PROCESSOR_ATTR: &str = "processor";
/// The attribute key carrying a sink node's id on its metric series.
pub const SINK_ATTR: &str = "sink";
/// The attribute key carrying a branch name on a processor's per-edge series.
pub const BRANCH_ATTR: &str = "branch";
/// The attribute key carrying a workflow's declared id on its metric series.
///
/// Attribution is additive, as for the node keys: `saci_workflow_runs_total`
/// and `saci_workflow_errors_total` are each recorded once unattributed — the
/// process-wide total every `/metrics` consumer has always read — and once
/// more under `workflow="<id>"`. Summing both forms double counts.
pub const WORKFLOW_ATTR: &str = "workflow";

/// What kind of instrument produced a series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesKind {
    /// Monotonic total. [`SeriesSummary::rate_per_sec`] is its derivative
    /// over a trailing window.
    Counter,
    /// Instantaneous value. `rate_per_sec` repeats the value itself.
    Gauge,
    /// Bucketed distribution. `value` is the sum, `count` the observation
    /// count, so a client can divide for the mean.
    Histogram,
}

/// One instrument's value at one export interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesPoint {
    /// Instrument name, e.g. `saci_rows_processed_total`.
    pub name: String,
    /// Which instrument kind it came from.
    pub kind: SeriesKind,
    /// The data point's attributes, sorted by key.
    pub attrs: Vec<Pair>,
    /// Cumulative value for a counter or histogram sum, current value for a
    /// gauge.
    pub value: f64,
    /// Histogram observation count; `0` for a counter or gauge.
    pub count: u64,
}

/// Everything one metric export interval produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    /// When the export ran, Unix milliseconds.
    pub at_unix_ms: u64,
    /// Every data point in that export, flattened across scopes.
    pub series: Vec<SeriesPoint>,
}

/// One `(timestamp, value)` sample for a sparkline.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PointAt {
    /// Unix milliseconds.
    pub t: u64,
    /// The series value at `t`.
    pub v: f64,
}

/// One series as the dashboard reads it: latest value, rate, and history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesSummary {
    /// Instrument name.
    pub name: String,
    /// Which instrument kind it came from.
    pub kind: SeriesKind,
    /// The data point's attributes, sorted by key.
    pub attrs: Vec<Pair>,
    /// Newest value in the window.
    pub value: f64,
    /// Histogram observation count; `0` for a counter or gauge.
    pub count: u64,
    /// For a counter, the derivative over a trailing window several export
    /// intervals wide, in units per second: a writer that moves its rows in
    /// one burst per pass and one that moves them evenly report the same
    /// number for the same total over the same span, and a writer whose pass
    /// is longer than that window still reads bursty. For a gauge, the value
    /// itself. For a histogram, the rate of its sum. A series that went
    /// backwards inside the window reports `0.0`.
    pub rate_per_sec: f64,
    /// History over the requested window, oldest first, decimated to at most
    /// [`MAX_POINTS`] entries.
    pub points: Vec<PointAt>,
}

/// How many history points a [`SeriesSummary`] carries at most.
///
/// A one-hour window at a one-second export interval is 3600 samples; sending
/// all of them per series per poll would dominate the response.
pub const MAX_POINTS: usize = 120;

/// A live throughput number for one [`TopoEdge`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeRate {
    /// Source node id.
    pub from: String,
    /// Destination node id.
    pub to: String,
    /// Items per second flowing along the edge.
    pub rate_per_sec: f64,
    /// What `rate_per_sec` counts: `"rows"` or `"batches"`. The last
    /// processor's edge to a sink falls back to batches when no processor row
    /// count exists, and says so rather than presenting batches as rows.
    pub unit: String,
}

/// Latency of one repeated span, grouped by one of its field values.
///
/// `saci_stage_duration_seconds` is recorded with no attributes, so for a native
/// pipeline per-stage and per-system latency exists nowhere in the metric
/// series. These numbers come from the retained span records instead, grouped
/// on the field that identifies the unit of work. A wasm processor's or a
/// native plugin's own per-batch latency does exist as a series — the
/// [`PROCESSOR_ATTR`]-attributed `saci_processor_batch_duration_seconds` —
/// because their inner spans never reach the host and leave this empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanStat {
    /// Span name the records came from, e.g. `pipeline.stage`.
    pub span: Cow<'static, str>,
    /// Value of the grouping field: the stage index, or the system name.
    pub key: String,
    /// How many retained records went into the numbers below.
    pub count: usize,
    /// Median duration, microseconds.
    pub p50_us: u64,
    /// 95th percentile duration, microseconds.
    pub p95_us: u64,
    /// Slowest retained occurrence, microseconds.
    pub max_us: u64,
}

/// What one adaptive-flow-control decision did to a source's admission
/// target.
///
/// One variant per outcome the host's flow controller can reach: an
/// adjustment epoch that closed on a winner, and a safety guard that tripped
/// with room to divide or with the target already on its floor. A pass that
/// moved nothing produces no decision and so has no variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowDecisionKind {
    /// An adjustment epoch closed and committed a larger target.
    Grew,
    /// An adjustment epoch closed and committed a smaller target.
    Shrank,
    /// A safety guard tripped and divided the target on the pass that saw it.
    BackedOff,
    /// A safety guard tripped with the target already at its floor, so there
    /// was nothing left to divide and the target held.
    HeldAtFloor,
}

/// One adaptive-flow-control *episode* on one source node.
///
/// One record per episode, not one per guard trip. A source's admission
/// control adapts on a paced epoch but reacts to trouble immediately, so a
/// sink losing ground divides the target to the same size for the same reason
/// on pass after pass, hundreds of times a second. The host records the pass
/// that opened each such run and drops the repeats: a new record appears when
/// the target lands somewhere else, when the cause changes, or when a
/// different [`FlowDecisionKind`] intervenes. Read `saci_flow_backoff_total`
/// for the number of trips; this is the sequence of distinct states, which is
/// what a chart can draw.
///
/// `from_rows` and `to_rows` are the target either side of the decision, so a
/// marker can be drawn without reading the `saci_flow_target_rows` series, and
/// they are the numbers of the pass that opened the episode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowDecision {
    /// When the decision landed, Unix milliseconds.
    pub at_unix_ms: u64,
    /// Declared id of the workflow the source belongs to.
    pub workflow: String,
    /// Declared id of the source node whose target the decision moved.
    pub source: String,
    /// What the decision did.
    pub kind: FlowDecisionKind,
    /// Admission target before the decision, in rows.
    pub from_rows: u64,
    /// Admission target after the decision, in rows. Equal to `from_rows`
    /// for [`FlowDecisionKind::HeldAtFloor`].
    pub to_rows: u64,
    /// The evidence behind the decision, in one phrase, naming the number
    /// that decided: `"experiment won: 12.4k rows/s against 9.1k rows/s"`,
    /// `"latency objective breached: 420 ms mean pass against 250 ms"`,
    /// `"sink backlog growing: 8192 rows pending"`, `"chunk over
    /// max_chunk_bytes: 12.6 MiB projected against 8.0 MiB"`, `"pass error"`.
    /// A client shows it verbatim, so it is never a restatement of
    /// [`kind`](Self::kind).
    pub reason: String,
}

/// How full the inspector's ring buffers are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferStats {
    /// Retained span records.
    pub spans: usize,
    /// Retained log records.
    pub logs: usize,
    /// Retained metric samples.
    pub samples: usize,
    /// Retained flow-control decisions.
    ///
    /// Defaulted for the same reason [`Snapshot::flow_decisions`] is: an older
    /// host's snapshot omits it, and a missing field in a nested struct fails
    /// the whole document otherwise.
    #[serde(default)]
    pub flow_decisions: usize,
    /// Records dropped because a buffer hit its capacity bound, across every
    /// buffer. Non-zero means the window is shorter than configured.
    pub dropped: u64,
}

/// The one document the dashboard polls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// [`Topology::version`] this snapshot was computed against.
    pub topology_version: u64,
    /// When the snapshot was taken, Unix milliseconds.
    pub sampled_at_unix_ms: u64,
    /// Seconds since the inspector was built.
    pub uptime_secs: u64,
    /// Whether the service reports itself ready.
    pub ready: bool,
    /// Every series with at least one sample in the window.
    pub series: Vec<SeriesSummary>,
    /// One entry per rated [`TopoEdge`], keyed by `(from, to)`. A
    /// processor-to-processor boundary is rated from the upstream processor's
    /// [`PROCESSOR_ATTR`]-attributed `saci_processor_rows_out_total`. An edge
    /// whose series has no sample yet is omitted rather than reported as
    /// zero, so a lookup by `(from, to)` is the contract and position is not.
    pub edges: Vec<EdgeRate>,
    /// Per-stage and per-system latency, derived from the retained span
    /// records. Empty for a WASM-hosted pipeline: those spans open inside the
    /// guest and never reach the host.
    pub span_stats: Vec<SpanStat>,
    /// Adaptive flow-control episodes that landed inside the same window the
    /// [`SeriesSummary::points`] histories cover, oldest first, so a client
    /// can draw them as event markers on the same time axis as any series.
    /// One entry per [`FlowDecision`] episode, not per guard trip.
    #[serde(default)]
    pub flow_decisions: Vec<FlowDecision>,
    /// Ring-buffer occupancy.
    pub buffers: BufferStats,
}

// ── Spans and logs ───────────────────────────────────────────────────────────

/// One closed `tracing` span.
///
/// `trace_id` is the root span's `tracing` id, not a W3C trace id: this
/// telemetry never leaves the process, and minting 128-bit ids would need a
/// second id map for no local benefit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanRecord {
    /// Root span id of the tree this span belongs to.
    pub trace_id: u64,
    /// This span's id.
    pub span_id: u64,
    /// Parent span id, absent for a root span.
    pub parent_id: Option<u64>,
    /// Span name, e.g. `pipeline.stage`.
    pub name: Cow<'static, str>,
    /// Emitting module path.
    pub target: Cow<'static, str>,
    /// When the span opened, Unix milliseconds.
    pub started_unix_ms: u64,
    /// How long it stayed open, microseconds.
    pub duration_us: u64,
    /// Recorded fields, in the order the subscriber saw them.
    pub fields: Vec<Pair>,
}

/// One `tracing` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    /// `"ERROR"`, `"WARN"`, `"INFO"`, `"DEBUG"` or `"TRACE"`.
    pub level: Cow<'static, str>,
    /// Emitting module path.
    pub target: Cow<'static, str>,
    /// The event's `message` field, empty when it has none.
    pub message: String,
    /// When the event fired, Unix milliseconds.
    pub at_unix_ms: u64,
    /// Innermost open span, when the event fired inside one.
    pub span_id: Option<u64>,
    /// That span's trace id.
    pub trace_id: Option<u64>,
    /// Every field except `message`.
    pub fields: Vec<Pair>,
}

/// One row in the traces list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceSummary {
    /// Root span id of the tree.
    pub trace_id: u64,
    /// The root span's name.
    pub name: Cow<'static, str>,
    /// When the root span opened, Unix milliseconds.
    pub started_unix_ms: u64,
    /// Root span duration, microseconds.
    pub duration_us: u64,
    /// How many retained spans belong to the tree.
    pub span_count: usize,
    /// Whether any retained log line in the tree is at `ERROR`.
    pub error: bool,
}

/// Everything retained about one trace, for the waterfall view.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TraceDetail {
    /// Every span in the tree, oldest first.
    pub spans: Vec<SpanRecord>,
    /// Every log line emitted inside the tree, oldest first.
    pub logs: Vec<LogRecord>,
}

// ── Workflow lifecycle ───────────────────────────────────────────────────────

/// Where one workflow's runner is in its lifecycle.
///
/// `Starting`, `Pausing` and `Stopping` are transient: a control request that
/// returns one of them was accepted but had not settled when the response was
/// written. `Completed` is a runner that reached the end of its work on its
/// own (a `one_shot` pass, or every source at EOF); `Stopped` is one an
/// operator stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRunState {
    /// A runner is being built and started.
    Starting,
    /// A runner is processing.
    Running,
    /// A pause was requested; the runner has not parked yet.
    Pausing,
    /// The runner is parked between passes and admits nothing.
    Paused,
    /// A stop was requested; the runner is draining.
    Stopping,
    /// No runner: an operator stopped it, and it can be started again.
    Stopped,
    /// The runner finished its work on its own.
    Completed,
    /// The runner returned an error. See [`WorkflowStatus::error`].
    Failed,
}

impl WorkflowRunState {
    /// The wire name, matching the `snake_case` serde representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// One workflow's lifecycle, as `/api/workflows` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStatus {
    /// The declared workflow id.
    pub id: String,
    /// The declared workflow name, when it has one.
    pub name: Option<String>,
    /// Where the runner is.
    pub state: WorkflowRunState,
    /// When the workflow entered `state`, Unix milliseconds.
    pub since_unix_ms: u64,
    /// How many times a runner has been started for this workflow, including
    /// the first.
    pub runs: u64,
    /// Whether `start`, `stop` and `restart` are available.
    pub restartable: bool,
    /// The error a `Failed` state carries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Why `restartable` is false, for the tooltip on a disabled button.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_blocked_reason: Option<String>,
}

/// One workflow a service-wide verb could not be applied to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRefusal {
    /// The declared workflow id.
    pub id: String,
    /// Why the verb was refused, in the same words the per-workflow endpoint
    /// would have used.
    pub error: String,
}

/// What one verb did across every controllable workflow.
///
/// A service-wide verb is applied per workflow, so a mixed answer is normal:
/// pausing a service where one workflow is already paused leaves that one
/// alone and pauses the rest. `refused` names the ones the verb could not
/// reach, and is empty on a clean sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceLifecycleReport {
    /// Every workflow the verb reached, with its resulting status, in
    /// declaration order.
    pub applied: Vec<WorkflowStatus>,
    /// Every workflow the verb could not be applied to.
    pub refused: Vec<WorkflowRefusal>,
    /// Whether every transition the verb started had settled when this was
    /// written.
    pub settled: bool,
}

// ── Dead letter queue ────────────────────────────────────────────────────────

/// Every letter waiting under one `(sink, reason)` pair.
///
/// The reason is the failure as the sink reported it, so the grouping is by
/// what went wrong rather than by when: a sink that has been refusing writes
/// for an hour is one row, however many letters it holds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqGroup {
    /// The declared id of the sink that refused the batch.
    pub sink: String,
    /// The failure, rendered from the sink's own error.
    pub reason: String,
    /// Letters in this group.
    pub letters: u64,
    /// Rows across those letters.
    pub rows: u64,
    /// When the oldest of them was first recorded, Unix milliseconds.
    pub first_failed_at_unix_ms: u64,
    /// When the newest of them was recorded or last re-recorded.
    pub last_failed_at_unix_ms: u64,
    /// The highest replay count any letter in the group carries.
    pub max_replays: u32,
}

/// What started a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DlqTrigger {
    /// The workflow's first pass of this process.
    Startup,
    /// A sink reported itself rebuilt.
    Heal,
    /// The automatic backoff schedule came due.
    Schedule,
    /// `POST /api/workflows/{id}/dlq/replay`.
    Manual,
    /// `POST /api/workflows/{id}/dlq/purge`: every letter is discarded
    /// instead of written.
    Purge,
}

impl DlqTrigger {
    /// The trigger as it appears on the wire and in a log line.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Heal => "heal",
            Self::Schedule => "schedule",
            Self::Manual => "manual",
            Self::Purge => "purge",
        }
    }
}

/// What one replay did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqReplayReport {
    /// What started it.
    pub trigger: DlqTrigger,
    /// When it started, Unix milliseconds.
    pub started_at_unix_ms: u64,
    /// How long it took.
    pub duration_ms: u64,
    /// Letters whose sink accepted them this time.
    pub delivered: u64,
    /// Letters written back to the store, either because the sink refused
    /// them again or because a filter excluded them.
    pub retained: u64,
    /// Letters discarded, which only a `purge` produces.
    pub purged: u64,
    /// Letters the store itself would not take back. These are gone.
    pub lost: u64,
    /// The error that ended the drain early, when one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One workflow's dead letter queue, as `GET /api/dlq` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqSummary {
    /// The declared workflow id.
    pub workflow: String,
    /// The store backing the queue: `"redb"`, `"kafka"` or `"nats"`.
    pub store: String,
    /// The configured replay point, `"before_sources"` or `"after_sources"`.
    pub replay: String,
    /// Whether a replay of this process has read the store through. Until
    /// one has, the counters below cover what this process recorded and not
    /// what an earlier one left behind.
    pub known: bool,
    /// Letters waiting.
    pub letters: u64,
    /// Rows across them.
    pub rows: u64,
    /// Those letters grouped by `(sink, reason)`.
    pub groups: Vec<DlqGroup>,
    /// What the last replay did, absent until one has run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_replay: Option<DlqReplayReport>,
    /// When the automatic schedule next comes due, Unix milliseconds.
    /// Absent when nothing is waiting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_auto_replay_unix_ms: Option<u64>,
    /// Whether a requested replay is waiting for the runner to reach its
    /// replay point.
    pub replay_pending: bool,
}

/// The body of `POST /api/workflows/{id}/dlq/replay`.
///
/// Both fields absent replays every letter; either one narrows the replay to
/// the letters matching it exactly, and the rest are written back untouched.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqReplayRequest {
    /// Replay only the letters this sink refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    /// Replay only the letters carrying this reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_serialize_as_two_element_arrays() {
        let node = TopoNode {
            id: "orders-in".to_string(),
            kind: "source".to_string(),
            name: Some("Authorizations".to_string()),
            type_name: "NatsSource".to_string(),
            component: Some("Order".to_string()),
            runtime: None,
            window: None,
            detail: vec![("mode.kind".to_string(), "core".to_string())],
        };
        let json = serde_json::to_string(&node).expect("serialize");
        assert!(
            json.contains(r#""detail":[["mode.kind","core"]]"#),
            "got: {json}"
        );
    }

    #[test]
    fn series_kind_is_lowercase_on_the_wire() {
        let point = SeriesPoint {
            name: "saci_rows_processed_total".to_string(),
            kind: SeriesKind::Counter,
            attrs: Vec::new(),
            value: 12.0,
            count: 0,
        };
        let json = serde_json::to_string(&point).expect("serialize");
        assert!(json.contains(r#""kind":"counter""#), "got: {json}");
    }

    #[test]
    fn flow_decision_kind_is_snake_case_on_the_wire() {
        let decision = FlowDecision {
            at_unix_ms: 1_750_000_000_000,
            workflow: "orders".to_string(),
            source: "orders-in".to_string(),
            kind: FlowDecisionKind::HeldAtFloor,
            from_rows: 1_024,
            to_rows: 1_024,
            reason: "sink backlog growing: 8192 rows pending".to_string(),
        };
        let json = serde_json::to_string(&decision).expect("serialize");
        assert!(json.contains(r#""kind":"held_at_floor""#), "got: {json}");
        let back: FlowDecision = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, decision);
    }

    #[test]
    fn span_record_round_trips_into_owned_strings() {
        let record = SpanRecord {
            trace_id: 1,
            span_id: 2,
            parent_id: Some(1),
            name: Cow::Borrowed("pipeline.stage"),
            target: Cow::Borrowed("saci_core::pipeline::execution"),
            started_unix_ms: 1_750_000_000_000,
            duration_us: 1234,
            fields: vec![("stage".to_string(), "1".to_string())],
        };
        let json = serde_json::to_string(&record).expect("serialize");
        let back: SpanRecord = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, record);
        assert!(matches!(back.name, Cow::Owned(_)));
    }

    #[test]
    fn a_summary_with_no_last_replay_or_schedule_deserialises_with_both_none() {
        let json = r#"{"workflow":"orders","store":"redb","replay":"before_sources","known":false,"letters":0,"rows":0,"groups":[],"replay_pending":false}"#;
        let summary: DlqSummary = serde_json::from_str(json).expect("deserialize");
        assert!(summary.last_replay.is_none());
        assert!(summary.next_auto_replay_unix_ms.is_none());
    }

    #[test]
    fn a_summary_carrying_last_replay_and_a_schedule_serialises_them_present() {
        let summary = DlqSummary {
            workflow: "orders".to_string(),
            store: "redb".to_string(),
            replay: "before_sources".to_string(),
            known: true,
            letters: 1,
            rows: 3,
            groups: Vec::new(),
            last_replay: Some(DlqReplayReport {
                trigger: DlqTrigger::Manual,
                started_at_unix_ms: 1,
                duration_ms: 2,
                delivered: 3,
                retained: 0,
                purged: 0,
                lost: 0,
                error: None,
            }),
            next_auto_replay_unix_ms: Some(5_000),
            replay_pending: false,
        };
        let json = serde_json::to_string(&summary).expect("serialize");
        assert!(
            json.contains(r#""last_replay":{"#),
            "a present last_replay must not be skipped: {json}"
        );
        assert!(
            json.contains(r#""next_auto_replay_unix_ms":5000"#),
            "a present schedule must not be skipped: {json}"
        );
        let back: DlqSummary = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, summary);
    }

    #[test]
    fn an_empty_replay_request_body_is_default() {
        let request: DlqReplayRequest = serde_json::from_str("{}").expect("deserialize");
        assert_eq!(request, DlqReplayRequest::default());
    }

    #[test]
    fn dlq_trigger_round_trips_its_snake_case_names_and_agrees_with_as_str() {
        for trigger in [
            DlqTrigger::Startup,
            DlqTrigger::Heal,
            DlqTrigger::Schedule,
            DlqTrigger::Manual,
            DlqTrigger::Purge,
        ] {
            let json = serde_json::to_string(&trigger).expect("serialize");
            assert_eq!(
                json,
                format!("\"{}\"", trigger.as_str()),
                "as_str must agree with the serialised wire form"
            );
            let back: DlqTrigger = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, trigger);
        }
    }
}
