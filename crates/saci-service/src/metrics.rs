//! OpenTelemetry instruments for the service's own metric series.
//!
//! [`Instruments`] has the same method surface whether or not the `metrics`
//! feature is on, so no call site carries a `#[cfg]`. With the feature off
//! every method is an empty inline body and the type is zero-sized.
//!
//! Unlike the connector crates, these instruments are process-global: a
//! service process can run several workflows, but
//! [`ServiceConfig::validate`](crate::service::config::ServiceConfig::validate)
//! enforces node ids unique across every declared workflow, so the OTel
//! attribution keys (`source=`/`processor=`/`sink=`, bare node ids) never
//! collide. There is therefore no per-workflow handle to thread through the
//! processor runtime, the host state or the distributed runner.
//!
//! Names follow the repository's `saci_<subsystem>_<thing>_total` / `_seconds`
//! convention.
//!
//! ## Writers
//!
//! | Series | Written by |
//! |---|---|
//! | `saci_workflow_runs_total` | standalone, stream and cluster runners |
//! | `saci_workflow_errors_total` | the same three runners, on a failed processor run |
//! | `saci_stage_duration_seconds` | [`SpanMetricsLayer`](crate::service::SpanMetricsLayer), from the `pipeline.stage` span a native `saci_core::Pipeline` opens |
//! | `saci_source_batches_drained_total` | standalone, stream and cluster runners |
//! | `saci_sink_batches_written_total`, `saci_sink_rows_written_total` | standalone and stream runners |
//! | `saci_sink_pending_rows` | the same two runners, per sink that answers its backlog probe |
//! | `saci_rows_processed_total` | standalone, stream and cluster runners |
//! | `saci_liveness_counter`, `saci_ready`, `saci_uptime_seconds` | the HTTP watchdog task |
//! | `saci_raft_commit_index`, `saci_raft_term`, `saci_raft_leader_id` | the cluster runner's Raft gauge task |
//! | `saci_processor_*` | the wasm host, from the WIT `run-metrics` record and `host-io::metric` |
//! | `saci_window_watermark_seconds` | the standalone and stream runners, per windowed processor node |
//! | `saci_window_late_arrivals_total` | the same two runners, per arrival such a node dropped whole |
//! | `saci_flow_target_rows`, `saci_flow_throughput_rows_per_second`, `saci_flow_backoff_total` | the standalone and stream runners, per source node, from its [`FlowController`](crate::service::flow::FlowController) |
//! | `saci_connector_heals_total`, `saci_connector_heal_failures_total` | [`HealingSource`](crate::service::heal::HealingSource) and [`HealingSink`](crate::service::heal::HealingSink), per rebuild of a failing connector |
//! | `saci_dlq_letters_recorded_total`, `saci_dlq_rows_recorded_total`, `saci_dlq_letters_replayed_total`, `saci_dlq_letters_lost_total` | [`DeadLetterQueue`](crate::service::dlq::DeadLetterQueue), per batch a sink refused and per letter a replay delivered or the store dropped |
//! | `saci_dlq_letters` | the same type, per workflow, after every record and every replay |
//!
//! ## Attribution by node and workflow id
//!
//! `source_batch`/`rows` carry [`saci_inspector_wire::SOURCE_ATTR`],
//! `sink_write` and `sink_pending_rows` carry
//! [`saci_inspector_wire::SINK_ATTR`], the six `saci_processor_*`
//! series carry [`saci_inspector_wire::PROCESSOR_ATTR`], and the two workflow
//! counters carry [`saci_inspector_wire::WORKFLOW_ATTR`]. Every node and
//! every workflow has a declared id, so every one of its metrics is
//! attributed by it.
//!
//! A workflow id is a dimension on the two `saci_workflow_*` counters: the
//! unattributed form stays the process-wide total, and the attributed form
//! names which workflow ran or failed.
//!
//! Attribution is **additive**, not a replacement: each value is recorded once
//! with no attributes, which is the process-wide total `/metrics` has always
//! exposed, and once more under the node's own id. Existing queries and alerts
//! on the unattributed names keep working untouched, at the cost that summing
//! the attributed and unattributed forms double counts.
//!
//! The flow-control writers add three series, the sink writers two, the
//! self-healing writers two and the dead letter queue five, for thirty-two in
//! total. Nineteen of the thirty-two are recorded twice, and every one of
//! those second writes adds
//! `source_batch`, `rows`, `flow_backoff` and the two heal counters written
//! for a source under `SOURCE_ATTR`, `sink_write`'s two counters, the two
//! heal counters written for a sink and the four `saci_dlq_letters_*`/
//! `saci_dlq_rows_*` counters under `SINK_ATTR`, the six
//! `saci_processor_*` series under `PROCESSOR_ATTR`, and
//! `workflow_run`/`workflow_error` under `WORKFLOW_ATTR`. Eighteen of the
//! nineteen pair an unattributed form with an attributed one;
//! `saci_processor_metric` is the exception, because both of its writes carry
//! `metric=` and only the second adds `processor=`, so it has no
//! attribute-free form. Five kinds of series are recorded only under a node
//! or workflow id, with no process-wide form:
//! `saci_window_watermark_seconds` and
//! `saci_window_late_arrivals_total`, because a watermark and the arrivals it
//! rejects belong to one node's merged inbound stream, the two
//! `saci_flow_*` gauges, because an admission target and its throughput belong
//! to one source, `saci_sink_pending_rows`, because a backlog is measured
//! against one sink's own flush policy, and `saci_dlq_letters`, because a
//! queue belongs to one workflow. Summing any of them across nodes
//! would be meaningless.

/// Upper bound on distinct `metric` attribute values accepted from processor
/// code.
///
/// Processor-chosen names are unbounded input, and every new attribute value
/// adds a permanent entry to the SDK's aggregation store.
// The instrument set is unconditional: `/metrics` exposes all thirty-two series
// with help text before any value is recorded, whichever writers the build
// compiles in. A `service`-only build has no Raft or processor writer, so those
// members are legitimately unread there.
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
pub(crate) const MAX_PROCESSOR_METRIC_NAMES: usize = 256;

/// Every service-owned counter, histogram and gauge, built up front.
#[cfg(feature = "metrics")]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
pub struct Instruments {
    workflow_runs: opentelemetry::metrics::Counter<u64>,
    workflow_errors: opentelemetry::metrics::Counter<u64>,
    stage_duration: opentelemetry::metrics::Histogram<f64>,
    source_batches: opentelemetry::metrics::Counter<u64>,
    sink_batches: opentelemetry::metrics::Counter<u64>,
    sink_rows: opentelemetry::metrics::Counter<u64>,
    sink_pending: opentelemetry::metrics::Gauge<f64>,
    rows_processed: opentelemetry::metrics::Counter<u64>,
    liveness: opentelemetry::metrics::Gauge<f64>,
    ready: opentelemetry::metrics::Gauge<f64>,
    uptime: opentelemetry::metrics::Gauge<f64>,
    raft_commit_index: opentelemetry::metrics::Gauge<f64>,
    raft_term: opentelemetry::metrics::Gauge<f64>,
    raft_leader_id: opentelemetry::metrics::Gauge<f64>,
    processor_batch_duration: opentelemetry::metrics::Histogram<f64>,
    processor_rows_in: opentelemetry::metrics::Counter<u64>,
    processor_rows_out: opentelemetry::metrics::Counter<u64>,
    processor_systems_run: opentelemetry::metrics::Counter<u64>,
    processor_retries: opentelemetry::metrics::Counter<u64>,
    processor_metric: opentelemetry::metrics::Histogram<f64>,
    window_watermark: opentelemetry::metrics::Gauge<f64>,
    window_late_arrivals: opentelemetry::metrics::Counter<u64>,
    flow_target_rows: opentelemetry::metrics::Gauge<f64>,
    flow_throughput: opentelemetry::metrics::Gauge<f64>,
    flow_backoffs: opentelemetry::metrics::Counter<u64>,
    connector_heals: opentelemetry::metrics::Counter<u64>,
    connector_heal_failures: opentelemetry::metrics::Counter<u64>,
    dlq_letters_recorded: opentelemetry::metrics::Counter<u64>,
    dlq_rows_recorded: opentelemetry::metrics::Counter<u64>,
    dlq_letters_replayed: opentelemetry::metrics::Counter<u64>,
    dlq_letters_lost: opentelemetry::metrics::Counter<u64>,
    dlq_letters: opentelemetry::metrics::Gauge<f64>,
    /// Metric names already admitted, bounded by [`MAX_PROCESSOR_METRIC_NAMES`].
    processor_metric_names: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Set once the cap is first hit, so the warning is logged one time.
    processor_metric_cap_warned: std::sync::atomic::AtomicBool,
}

/// Zero-sized stand-in used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
pub struct Instruments;

/// A Prometheus registry the whole lib test binary shares.
///
/// Installing a meter provider is a process-global one-shot, and the lib test
/// binary has many tests that write metrics, so whichever one runs first would
/// otherwise bind [`INSTRUMENTS`] to the no-op default provider. Forcing this
/// from the `INSTRUMENTS` initializer makes the order deterministic: the
/// instruments always record into this registry.
#[cfg(all(test, feature = "metrics"))]
static TEST_REGISTRY: std::sync::LazyLock<prometheus::Registry> = std::sync::LazyLock::new(|| {
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .without_counter_suffixes()
        .with_registry(registry.clone())
        .build()
        .expect("build prometheus exporter");
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(exporter)
        .build();
    opentelemetry::global::set_meter_provider(provider);
    registry
});

/// The registry [`instruments`] records into during lib tests.
#[cfg(all(test, feature = "metrics"))]
pub(crate) fn test_registry() -> &'static prometheus::Registry {
    &TEST_REGISTRY
}

static INSTRUMENTS: std::sync::LazyLock<Instruments> = std::sync::LazyLock::new(|| {
    #[cfg(all(test, feature = "metrics"))]
    std::sync::LazyLock::force(&TEST_REGISTRY);
    Instruments::new()
});

/// Build every instrument and bind it to the installed meter provider.
///
/// Instruments bind to whichever provider is installed when they are built, so
/// call this at startup right after `opentelemetry::global::set_meter_provider`
/// and before the first value is recorded. Idempotent.
pub fn init() {
    std::sync::LazyLock::force(&INSTRUMENTS);
}

/// The process-wide instruments.
///
/// Builds them on first use, which is what a library embedder or a test that
/// never calls [`init`] gets.
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
pub(crate) fn instruments() -> &'static Instruments {
    &INSTRUMENTS
}

#[cfg(feature = "metrics")]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl Instruments {
    /// Build every instrument against `opentelemetry::global::meter("saci")`.
    ///
    /// Gauges are synchronous rather than observable: an observable gauge needs
    /// its callback at build time, and the state it would read (the HTTP
    /// service state, the Raft driver handle) does not exist yet at startup.
    fn new() -> Self {
        let meter = opentelemetry::global::meter("saci");
        Self {
            workflow_runs: meter
                .u64_counter("saci_workflow_runs_total")
                .with_description(
                    "Total number of workflow passes; a pass is one credit-sized \
                     admission per source, not one drain to EOF",
                )
                .build(),
            workflow_errors: meter
                .u64_counter("saci_workflow_errors_total")
                .with_description("Total number of workflow run errors")
                .build(),
            stage_duration: meter
                .f64_histogram("saci_stage_duration_seconds")
                .with_description("Stage execution duration in seconds")
                .build(),
            source_batches: meter
                .u64_counter("saci_source_batches_drained_total")
                .with_description("Total source batches drained")
                .build(),
            sink_batches: meter
                .u64_counter("saci_sink_batches_written_total")
                .with_description("Total sink batches written")
                .build(),
            sink_rows: meter
                .u64_counter("saci_sink_rows_written_total")
                .with_description("Total rows written to sinks")
                .build(),
            sink_pending: meter
                .f64_gauge("saci_sink_pending_rows")
                .with_description("Rows buffered in one sink and not yet consumed downstream")
                .build(),
            rows_processed: meter
                .u64_counter("saci_rows_processed_total")
                .with_description("Total rows processed through the pipeline")
                .build(),
            liveness: meter
                .f64_gauge("saci_liveness_counter")
                .with_description("Watchdog liveness counter")
                .build(),
            ready: meter
                .f64_gauge("saci_ready")
                .with_description("Service ready state (1=ready)")
                .build(),
            uptime: meter
                .f64_gauge("saci_uptime_seconds")
                .with_description("Service uptime in seconds")
                .build(),
            raft_commit_index: meter
                .f64_gauge("saci_raft_commit_index")
                .with_description("Raft commit index")
                .build(),
            raft_term: meter
                .f64_gauge("saci_raft_term")
                .with_description("Current Raft term")
                .build(),
            raft_leader_id: meter
                .f64_gauge("saci_raft_leader_id")
                .with_description("Current Raft leader node ID")
                .build(),
            processor_batch_duration: meter
                .f64_histogram("saci_processor_batch_duration_seconds")
                .with_description("Processor run-batch wall time in seconds")
                .build(),
            processor_rows_in: meter
                .u64_counter("saci_processor_rows_in_total")
                .with_description("Total rows handed to the processor")
                .build(),
            processor_rows_out: meter
                .u64_counter("saci_processor_rows_out_total")
                .with_description("Total rows returned by the processor")
                .build(),
            processor_systems_run: meter
                .u64_counter("saci_processor_systems_run_total")
                .with_description("Total systems the processor executed")
                .build(),
            processor_retries: meter
                .u64_counter("saci_processor_retries_total")
                .with_description("Total system retries inside the processor")
                .build(),
            processor_metric: meter
                .f64_histogram("saci_processor_metric")
                .with_description("Values reported by processor code through host-io::metric")
                .build(),
            window_watermark: meter
                .f64_gauge("saci_window_watermark_seconds")
                .with_description(
                    "Event-time watermark of a windowed processor node, in epoch seconds",
                )
                .build(),
            window_late_arrivals: meter
                .u64_counter("saci_window_late_arrivals_total")
                .with_description(
                    "Arrivals a windowed processor node dropped whole, every row beyond its allowed lateness",
                )
                .build(),
            flow_target_rows: meter
                .f64_gauge("saci_flow_target_rows")
                .with_description("Adaptive flow-control target, in rows, for one source node")
                .build(),
            flow_throughput: meter
                .f64_gauge("saci_flow_throughput_rows_per_second")
                .with_description("Smoothed rows per second observed for one source node")
                .build(),
            flow_backoffs: meter
                .u64_counter("saci_flow_backoff_total")
                .with_description("Total adaptive flow-control back-off events")
                .build(),
            connector_heals: meter
                .u64_counter("saci_connector_heals_total")
                .with_description("Connector rebuilds that produced a working instance")
                .build(),
            connector_heal_failures: meter
                .u64_counter("saci_connector_heal_failures_total")
                .with_description(
                    "Connector rebuild attempts that did not produce a working instance",
                )
                .build(),
            dlq_letters_recorded: meter
                .u64_counter("saci_dlq_letters_recorded_total")
                .with_description("Batches a sink refused and the dead letter queue stored")
                .build(),
            dlq_rows_recorded: meter
                .u64_counter("saci_dlq_rows_recorded_total")
                .with_description("Rows across the batches the dead letter queue stored")
                .build(),
            dlq_letters_replayed: meter
                .u64_counter("saci_dlq_letters_replayed_total")
                .with_description("Dead letters a replay delivered to their sink")
                .build(),
            dlq_letters_lost: meter
                .u64_counter("saci_dlq_letters_lost_total")
                .with_description("Dead letters the store itself would not take")
                .build(),
            dlq_letters: meter
                .f64_gauge("saci_dlq_letters")
                .with_description("Dead letters waiting in one workflow's queue")
                .build(),
            processor_metric_names: std::sync::Mutex::new(std::collections::HashSet::new()),
            processor_metric_cap_warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// One completed workflow pass in workflow `workflow`.
    ///
    /// A pass, not a source batch: under flow control a source batch larger
    /// than the admission credit is carried over several passes, so this
    /// counter and `saci_source_batches_drained_total` have different
    /// denominators. The latter counts arrivals.
    pub(crate) fn workflow_run(&self, workflow: &str) {
        self.workflow_runs.add(1, &[]);
        self.workflow_runs.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::WORKFLOW_ATTR,
                workflow.to_string(),
            )],
        );
    }

    /// One workflow run that returned an error, in workflow `workflow`.
    ///
    /// Source and sink drain failures are counted separately in the runner
    /// stats and deliberately do not land here: conflating three failure kinds
    /// into one series makes it unalertable.
    pub(crate) fn workflow_error(&self, workflow: &str) {
        self.workflow_errors.add(1, &[]);
        self.workflow_errors.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::WORKFLOW_ATTR,
                workflow.to_string(),
            )],
        );
    }

    /// One stage that took `seconds`.
    ///
    /// Measures the `pipeline.stage` span a native `saci_core::Pipeline` opens;
    /// there is nothing to attribute this by beyond the process-wide total,
    /// because `saci-core` carries no metrics dependency of its own.
    pub(crate) fn stage_duration(&self, seconds: f64) {
        self.stage_duration.record(seconds, &[]);
    }

    /// One non-empty drain from source `source`.
    pub(crate) fn source_batch(&self, source: &str) {
        self.source_batches.add(1, &[]);
        self.source_batches.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SOURCE_ATTR,
                source.to_string(),
            )],
        );
    }

    /// One non-empty write to sink `sink`, of `rows` rows.
    ///
    /// The batch count alone cannot answer "how much is this sink moving": a
    /// batch is whatever row count the upstream handed over, so the same
    /// throughput reads as one batch or a hundred depending on the admission
    /// target. The row count is the comparable number.
    pub(crate) fn sink_write(&self, sink: &str, rows: u64) {
        self.sink_batches.add(1, &[]);
        self.sink_rows.add(rows, &[]);
        let attrs = [opentelemetry::KeyValue::new(
            saci_inspector_wire::SINK_ATTR,
            sink.to_string(),
        )];
        self.sink_batches.add(1, &attrs);
        self.sink_rows.add(rows, &attrs);
    }

    /// The backlog one sink reports through its `Sink::pending_rows` probe.
    ///
    /// Attributed by sink only, like
    /// [`window_watermark`](Self::window_watermark): a backlog is measured
    /// against one sink's own flush policy, so summing across sinks compares
    /// numbers that mean different things. A sink that reports no backlog
    /// records nothing rather than zero, which would be indistinguishable
    /// from a drained buffer.
    pub(crate) fn sink_pending_rows(&self, sink: &str, rows: u64) {
        self.sink_pending.record(
            rows as f64,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SINK_ATTR,
                sink.to_string(),
            )],
        );
    }

    /// `n` rows drained from source `source`.
    pub(crate) fn rows(&self, source: &str, n: u64) {
        self.rows_processed.add(n, &[]);
        self.rows_processed.add(
            n,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SOURCE_ATTR,
                source.to_string(),
            )],
        );
    }

    /// Latest watchdog liveness counter, readiness flag and uptime.
    pub(crate) fn service_gauges(&self, liveness: u64, ready: bool, uptime_seconds: f64) {
        self.liveness.record(liveness as f64, &[]);
        self.ready.record(if ready { 1.0 } else { 0.0 }, &[]);
        self.uptime.record(uptime_seconds, &[]);
    }

    /// Latest Raft commit index, term and leader.
    ///
    /// A missing leader records `-1.0` rather than skipping the write: a gauge
    /// keeps its last value, so a skip would leave a stale leader id visible
    /// after an election loss.
    pub(crate) fn raft(&self, commit_index: u64, term: u64, leader_id: Option<u64>) {
        self.raft_commit_index.record(commit_index as f64, &[]);
        self.raft_term.record(term as f64, &[]);
        self.raft_leader_id
            .record(leader_id.map_or(-1.0, |id| id as f64), &[]);
    }

    /// One processor `run-batch` call, from the WIT `run-metrics` record.
    ///
    /// `processor` is the declared id of the workflow node this runtime backs.
    /// Attribution is additive: every value lands in the unattributed series
    /// regardless, because that is the process-wide total a `/metrics`
    /// consumer already reads, and this processor records the same value a
    /// second time under `processor="<id>"`. Summing both forms therefore
    /// double counts.
    pub(crate) fn processor_batch(
        &self,
        processor: &str,
        wall_ns: u64,
        rows_in: u64,
        rows_out: u64,
        systems_run: u32,
        retries: u32,
    ) {
        let seconds = wall_ns as f64 / 1e9;
        let systems_run = u64::from(systems_run);
        let retries = u64::from(retries);

        self.processor_batch_duration.record(seconds, &[]);
        self.processor_rows_in.add(rows_in, &[]);
        self.processor_rows_out.add(rows_out, &[]);
        self.processor_systems_run.add(systems_run, &[]);
        self.processor_retries.add(retries, &[]);

        // One array for all five writes. The key borrows a `&'static str`.
        let attrs = [opentelemetry::KeyValue::new(
            saci_inspector_wire::PROCESSOR_ATTR,
            processor.to_string(),
        )];
        self.processor_batch_duration.record(seconds, &attrs);
        self.processor_rows_in.add(rows_in, &attrs);
        self.processor_rows_out.add(rows_out, &attrs);
        self.processor_systems_run.add(systems_run, &attrs);
        self.processor_retries.add(retries, &attrs);
    }

    /// `n` rows a processor delivered along one labelled outbound edge.
    ///
    /// A third attributed form of `saci_processor_rows_out_total`, keyed
    /// `processor="<id>", branch="<name>"`, so the dashboard can rate each
    /// branch edge separately. Same instrument, no new series.
    pub(crate) fn processor_branch_rows(&self, processor: &str, branch: &str, n: u64) {
        self.processor_rows_out.add(
            n,
            &[
                opentelemetry::KeyValue::new(
                    saci_inspector_wire::PROCESSOR_ATTR,
                    processor.to_string(),
                ),
                opentelemetry::KeyValue::new(saci_inspector_wire::BRANCH_ATTR, branch.to_string()),
            ],
        );
    }

    /// The event-time watermark of one windowed processor node, in epoch
    /// seconds.
    ///
    /// Unlike the other attributed series this one has no unattributed form:
    /// a watermark belongs to a node's merged inbound stream, and there is no
    /// meaningful process-wide watermark to sum over. Recorded only when the
    /// node's config declares a `window` block and the tracker has seen at
    /// least one timestamp.
    pub(crate) fn window_watermark(&self, processor: &str, seconds: f64) {
        self.window_watermark.record(
            seconds,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::PROCESSOR_ATTR,
                processor.to_string(),
            )],
        );
    }

    /// One arrival a windowed processor node dropped whole: every row in it
    /// was older than `watermark - allowed_lateness_ms`, so the node opened
    /// no window, closed none, and passed nothing downstream.
    ///
    /// Attributed by processor only, like
    /// [`window_watermark`](Self::window_watermark): the count is a property
    /// of one node's merged inbound stream against one node's watermark.
    ///
    /// A rising count with live sources is the signature of an inbound stream
    /// whose event time has fallen behind the node's watermark: a producer
    /// restarted against a still-running service, a replay rewound, or one row
    /// with a far-future timestamp having dragged the watermark past
    /// everything real that follows. The sinks behind such a node stay empty
    /// while it climbs.
    pub(crate) fn window_late_arrival(&self, processor: &str) {
        self.window_late_arrivals.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::PROCESSOR_ATTR,
                processor.to_string(),
            )],
        );
    }

    /// The current adaptive target and smoothed throughput of one source node.
    ///
    /// Attributed by source only, for the same reason
    /// [`window_watermark`](Self::window_watermark) is: both values describe
    /// one source's admission, and a process-wide gauge over them would report
    /// whichever source wrote last.
    pub(crate) fn flow_state(&self, source: &str, target_rows: usize, rows_per_second: f64) {
        let attrs = [opentelemetry::KeyValue::new(
            saci_inspector_wire::SOURCE_ATTR,
            source.to_string(),
        )];
        self.flow_target_rows.record(target_rows as f64, &attrs);
        self.flow_throughput.record(rows_per_second, &attrs);
    }

    /// One flow-control back-off on source `source`.
    pub(crate) fn flow_backoff(&self, source: &str) {
        self.flow_backoffs.add(1, &[]);
        self.flow_backoffs.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SOURCE_ATTR,
                source.to_string(),
            )],
        );
    }

    /// One source node rebuilt into a working instance.
    pub(crate) fn source_heal(&self, source: &str) {
        self.connector_heals.add(1, &[]);
        self.connector_heals.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SOURCE_ATTR,
                source.to_string(),
            )],
        );
    }

    /// One rebuild attempt on a source node that produced nothing usable.
    pub(crate) fn source_heal_failure(&self, source: &str) {
        self.connector_heal_failures.add(1, &[]);
        self.connector_heal_failures.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SOURCE_ATTR,
                source.to_string(),
            )],
        );
    }

    /// One sink node rebuilt into a working instance.
    pub(crate) fn sink_heal(&self, sink: &str) {
        self.connector_heals.add(1, &[]);
        self.connector_heals.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SINK_ATTR,
                sink.to_string(),
            )],
        );
    }

    /// One rebuild attempt on a sink node that produced nothing usable.
    pub(crate) fn sink_heal_failure(&self, sink: &str) {
        self.connector_heal_failures.add(1, &[]);
        self.connector_heal_failures.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SINK_ATTR,
                sink.to_string(),
            )],
        );
    }

    /// One batch of `rows` rows that sink `sink` refused and the queue stored.
    pub(crate) fn dlq_recorded(&self, sink: &str, rows: u64) {
        self.dlq_letters_recorded.add(1, &[]);
        self.dlq_rows_recorded.add(rows, &[]);
        let attrs = [opentelemetry::KeyValue::new(
            saci_inspector_wire::SINK_ATTR,
            sink.to_string(),
        )];
        self.dlq_letters_recorded.add(1, &attrs);
        self.dlq_rows_recorded.add(rows, &attrs);
    }

    /// One dead letter a replay delivered to sink `sink`.
    pub(crate) fn dlq_replayed(&self, sink: &str) {
        self.dlq_letters_replayed.add(1, &[]);
        self.dlq_letters_replayed.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SINK_ATTR,
                sink.to_string(),
            )],
        );
    }

    /// One dead letter the store would not take, bound for sink `sink`.
    ///
    /// The batch is gone: a letter is never buffered in memory waiting for
    /// the store to come back.
    pub(crate) fn dlq_lost(&self, sink: &str) {
        self.dlq_letters_lost.add(1, &[]);
        self.dlq_letters_lost.add(
            1,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::SINK_ATTR,
                sink.to_string(),
            )],
        );
    }

    /// How many letters workflow `workflow` currently holds.
    ///
    /// Attributed by workflow only, like
    /// [`sink_pending_rows`](Self::sink_pending_rows) is by sink: a queue
    /// belongs to one workflow, and a queue holding nothing is a state worth
    /// reporting, so this records zero as readily as any other value.
    pub(crate) fn dlq_pending(&self, workflow: &str, letters: u64) {
        self.dlq_letters.record(
            letters as f64,
            &[opentelemetry::KeyValue::new(
                saci_inspector_wire::WORKFLOW_ATTR,
                workflow.to_string(),
            )],
        );
    }

    /// One value reported by processor code through `host-io::metric`.
    ///
    /// A histogram covers all three intents the WIT contract allows: a counter
    /// would reject a negative value and a gauge would lose every value but the
    /// last. Names are processor-chosen, so distinct names are capped at
    /// [`MAX_PROCESSOR_METRIC_NAMES`] and anything past that is dropped after
    /// one warning.
    ///
    /// `processor` attributes the value the same additive way
    /// [`processor_batch`](Self::processor_batch) does. The cap counts
    /// distinct *names*, not name-and-processor pairs: two processors
    /// reporting one name spend one entry.
    pub(crate) fn processor_metric(&self, processor: &str, name: &str, value: f64) {
        let admitted = {
            let mut names = self
                .processor_metric_names
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if names.contains(name) {
                true
            } else if names.len() < MAX_PROCESSOR_METRIC_NAMES {
                names.insert(name.to_string());
                true
            } else {
                false
            }
        };

        if !admitted {
            if !self
                .processor_metric_cap_warned
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    metric = %name,
                    cap = MAX_PROCESSOR_METRIC_NAMES,
                    "processor metric name cap reached; further new names are dropped"
                );
            }
            return;
        }

        // The name is allocated once and moved into the attributed write, so
        // adding the second series costs one extra allocation for `processor`.
        // Keys are sorted ("metric" < "processor"), matching the documented
        // wire contract for this series' attribute order.
        let metric = opentelemetry::KeyValue::new("metric", name.to_string());
        self.processor_metric
            .record(value, std::slice::from_ref(&metric));
        self.processor_metric.record(
            value,
            &[
                metric,
                opentelemetry::KeyValue::new(
                    saci_inspector_wire::PROCESSOR_ATTR,
                    processor.to_string(),
                ),
            ],
        );
    }
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl Instruments {
    #[inline]
    fn new() -> Self {
        Self
    }
    #[inline]
    pub(crate) fn workflow_run(&self, _workflow: &str) {}
    #[inline]
    pub(crate) fn workflow_error(&self, _workflow: &str) {}
    #[inline]
    pub(crate) fn stage_duration(&self, _seconds: f64) {}
    #[inline]
    pub(crate) fn source_batch(&self, _source: &str) {}
    #[inline]
    pub(crate) fn sink_write(&self, _sink: &str, _rows: u64) {}
    #[inline]
    pub(crate) fn sink_pending_rows(&self, _sink: &str, _rows: u64) {}
    #[inline]
    pub(crate) fn rows(&self, _source: &str, _n: u64) {}
    #[inline]
    pub(crate) fn service_gauges(&self, _liveness: u64, _ready: bool, _uptime_seconds: f64) {}
    #[inline]
    pub(crate) fn raft(&self, _commit_index: u64, _term: u64, _leader_id: Option<u64>) {}
    #[inline]
    pub(crate) fn processor_batch(
        &self,
        _processor: &str,
        _wall_ns: u64,
        _rows_in: u64,
        _rows_out: u64,
        _systems_run: u32,
        _retries: u32,
    ) {
    }

    #[inline]
    pub(crate) fn processor_branch_rows(&self, _processor: &str, _branch: &str, _n: u64) {}
    #[inline]
    pub(crate) fn processor_metric(&self, _processor: &str, _name: &str, _value: f64) {}
    #[inline]
    pub(crate) fn window_watermark(&self, _processor: &str, _seconds: f64) {}
    #[inline]
    pub(crate) fn window_late_arrival(&self, _processor: &str) {}
    #[inline]
    pub(crate) fn flow_state(&self, _source: &str, _target_rows: usize, _rows_per_second: f64) {}
    #[inline]
    pub(crate) fn flow_backoff(&self, _source: &str) {}

    #[inline]
    pub(crate) fn source_heal(&self, _source: &str) {}

    #[inline]
    pub(crate) fn source_heal_failure(&self, _source: &str) {}

    #[inline]
    pub(crate) fn sink_heal(&self, _sink: &str) {}

    #[inline]
    pub(crate) fn sink_heal_failure(&self, _sink: &str) {}

    #[inline]
    pub(crate) fn dlq_recorded(&self, _sink: &str, _rows: u64) {}

    #[inline]
    pub(crate) fn dlq_replayed(&self, _sink: &str) {}

    #[inline]
    pub(crate) fn dlq_lost(&self, _sink: &str) {}

    #[inline]
    pub(crate) fn dlq_pending(&self, _workflow: &str, _letters: u64) {}
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use prometheus::TextEncoder;

    /// Every line of the scrape naming `series`.
    fn lines_for(text: &str, series: &str) -> Vec<String> {
        text.lines()
            .filter(|line| line.starts_with(&format!("{series}{{")))
            .map(str::to_string)
            .collect()
    }

    /// A processor must land in both the process-wide series and its own, so
    /// an existing `/metrics` query keeps its meaning while the dashboard gains
    /// a per-processor number.
    ///
    /// The processor id is deliberately an unlikely one: the lib test binary
    /// shares one process-global registry, so this asserts on which label sets
    /// exist rather than on their values, which other tests also move.
    #[test]
    fn a_processor_writes_both_the_unattributed_and_the_attributed_series() {
        let registry = super::test_registry();
        super::instruments().processor_batch("proc-7", 1_500_000, 16, 16, 1, 0);

        let text = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .expect("encode prometheus text");

        for series in [
            "saci_processor_rows_in_total",
            "saci_processor_rows_out_total",
            "saci_processor_systems_run_total",
            "saci_processor_retries_total",
        ] {
            let lines = lines_for(&text, series);
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains(r#"processor="proc-7""#)),
                "{series} must carry the processor attribute:\n{text}"
            );
            assert!(
                lines.iter().any(|line| !line.contains("processor=")),
                "{series} must keep its unattributed form:\n{text}"
            );
        }

        let counts = lines_for(&text, "saci_processor_batch_duration_seconds_count");
        assert!(
            counts
                .iter()
                .any(|line| line.contains(r#"processor="proc-7""#)),
            "the batch histogram must carry the processor attribute:\n{text}"
        );
        assert!(
            counts.iter().any(|line| !line.contains("processor=")),
            "the batch histogram must keep its unattributed form:\n{text}"
        );
    }

    /// A workflow must land in both the process-wide counters and its own, so
    /// an existing `/metrics` query keeps its meaning while the dashboard
    /// gains a per-workflow badge.
    ///
    /// The workflow id is deliberately an unlikely one, for the same reason
    /// as the processor test above.
    #[test]
    fn a_workflow_writes_both_the_unattributed_and_the_attributed_counters() {
        let registry = super::test_registry();
        super::instruments().workflow_run("alpha");
        super::instruments().workflow_error("alpha");

        let text = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .expect("encode prometheus text");

        for series in ["saci_workflow_runs_total", "saci_workflow_errors_total"] {
            let lines = lines_for(&text, series);
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains(r#"workflow="alpha""#)),
                "{series} must carry the workflow attribute:\n{text}"
            );
            assert!(
                lines.iter().any(|line| !line.contains("workflow=")),
                "{series} must keep its unattributed form:\n{text}"
            );
        }
    }
}
