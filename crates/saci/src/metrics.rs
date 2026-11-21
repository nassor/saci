//! OpenTelemetry instruments for the service's own metric series.
//!
//! `Instruments` and the four handles it binds (`WorkflowMetrics`,
//! `SourceMetrics`, `ProcessorMetrics`, `SinkMetrics`) have the same method
//! surface whether or not the `metrics` feature is on, so no call site carries
//! a `#[cfg]`. With the feature off every method is an empty inline body and
//! every type is zero-sized.
//!
//! Unlike the connector crates, these instruments are process-global: a
//! service process can run several workflows, but
//! [`ServiceConfig::validate`](crate::service::ServiceConfig::validate)
//! enforces node ids unique across every declared workflow, so the OTel
//! attribution keys (`source=`/`processor=`/`sink=`, bare node ids) never
//! collide. A runner binds one handle per node and one per workflow when it
//! starts. The handle owns that id's attribute, so a write builds none.
//!
//! Names follow the repository's `saci_<subsystem>_<thing>_total` / `_seconds`
//! convention.
//!
//! ## Writers
//!
//! | Series | Written by |
//! |---|---|
//! | `saci_workflow_passes_total` | standalone, stream and cluster runners |
//! | `saci_workflow_pass_errors_total` | the same three runners, on a failed processor run |
//! | `saci_stage_duration_seconds` | [`SpanMetricsLayer`](crate::service::SpanMetricsLayer), from the `pipeline.stage` span a native `crate::Pipeline` opens |
//! | `saci_source_batches_total` | standalone, stream and cluster runners |
//! | `saci_sink_batches_total`, `saci_sink_rows_total` | standalone and stream runners |
//! | `saci_sink_pending_rows` | the same two runners, per sink that answers its backlog probe |
//! | `saci_source_rows_total` | standalone, stream and cluster runners |
//! | `saci_watchdog_ticks_total`, `saci_ready`, `saci_uptime_seconds` | the HTTP watchdog task |
//! | `saci_raft_commit_index`, `saci_raft_term`, `saci_raft_leader_id` | the cluster runner's Raft gauge task |
//! | `saci_processor_*` | the wasm host, from the WIT `run-metrics` record and `host-io::metric` |
//! | `saci_window_watermark_seconds` | the standalone and stream runners, per windowed processor node |
//! | `saci_window_late_arrivals_total` | the same two runners, per arrival such a node dropped whole |
//! | `saci_flow_target_rows`, `saci_flow_throughput_rows_per_second`, `saci_flow_backoff_total` | the standalone and stream runners, per source node, from its [`FlowController`](crate::service::FlowController) |
//! | `saci_connector_heals_total`, `saci_connector_heal_failures_total` | [`HealingSource`](crate::service::HealingSource) and [`HealingSink`](crate::service::HealingSink), per rebuild of a failing connector |
//! | `saci_dlq_letters_recorded_total`, `saci_dlq_rows_recorded_total`, `saci_dlq_letters_replayed_total`, `saci_dlq_letters_lost_total` | [`DeadLetterQueue`](crate::service::DeadLetterQueue), per batch a sink refused and per letter a replay delivered or the store dropped |
//! | `saci_dlq_letters_pending` | the same type, per workflow, after every record and every replay |
//! | `saci_http_requests_total`, `saci_http_request_errors_total`, `saci_http_request_duration_seconds`, `saci_http_bytes_total` | the `http` connector's `HttpSource`, `HttpSink` and `HttpListenSource`, on the process-global meter with no registration here |
//!
//! The four `saci_http_*` series are the one group in that table this module
//! does not build: the connector crate holds its own instruments and reaches
//! the same meter, so they appear on `/metrics` as soon as a build carries
//! `connector-http`. Each carries `role` = `source`, `sink` or `push`, plus
//! `workflow` and `node` once the host bound a `NodeIdentity`.
//! `saci_http_requests_total` counts exchanges that produced a response and
//! adds `method` and the decimal `status`. `saci_http_request_errors_total`
//! counts failures under `kind` = `transport`, `status`, `decode`, `size` or
//! `auth`. `saci_http_request_duration_seconds` is a histogram in seconds over
//! one exchange, labelled `method`. `saci_http_bytes_total` counts body bytes
//! under `direction` = `in` or `out`.
//!
//! ## Attribution by node and workflow id
//!
//! Every series that belongs to a node or a workflow is recorded once, under
//! that id ([`crate::inspector_wire::SOURCE_ATTR`],
//! [`crate::inspector_wire::SINK_ATTR`],
//! [`crate::inspector_wire::PROCESSOR_ATTR`] or
//! [`crate::inspector_wire::WORKFLOW_ATTR`]), and a process-wide total is
//! `sum()` over the attribute. `saci_processor_rows_out_total` also carries a
//! `processor` plus `branch` form per labelled edge, so its total selects
//! `branch=""`.

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
pub(crate) struct Instruments {
    workflow_passes: opentelemetry::metrics::Counter<u64>,
    workflow_pass_errors: opentelemetry::metrics::Counter<u64>,
    stage_duration: opentelemetry::metrics::Histogram<f64>,
    source_batches: opentelemetry::metrics::Counter<u64>,
    sink_batches: opentelemetry::metrics::Counter<u64>,
    sink_rows: opentelemetry::metrics::Counter<u64>,
    sink_pending: opentelemetry::metrics::Gauge<f64>,
    source_rows: opentelemetry::metrics::Counter<u64>,
    watchdog_ticks: opentelemetry::metrics::Gauge<f64>,
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
    processor_custom: opentelemetry::metrics::Histogram<f64>,
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
    dlq_letters_pending: opentelemetry::metrics::Gauge<f64>,
    /// Metric names already admitted, bounded by [`MAX_PROCESSOR_METRIC_NAMES`].
    processor_metric_names: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Set once the cap is first hit, so the warning is logged one time.
    processor_metric_cap_warned: std::sync::atomic::AtomicBool,
}

/// Zero-sized stand-in used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
pub(crate) struct Instruments;

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
            workflow_passes: meter
                .u64_counter("saci_workflow_passes_total")
                .with_description(
                    "Total number of workflow passes; a pass is one credit-sized \
                     admission per source, not one drain to EOF",
                )
                .build(),
            workflow_pass_errors: meter
                .u64_counter("saci_workflow_pass_errors_total")
                .with_description("Total number of workflow passes that returned an error")
                .build(),
            stage_duration: meter
                .f64_histogram("saci_stage_duration_seconds")
                .with_description("Stage execution duration in seconds")
                .build(),
            source_batches: meter
                .u64_counter("saci_source_batches_total")
                .with_description("Total source batches drained")
                .build(),
            sink_batches: meter
                .u64_counter("saci_sink_batches_total")
                .with_description("Total sink batches written")
                .build(),
            sink_rows: meter
                .u64_counter("saci_sink_rows_total")
                .with_description("Total rows written to sinks")
                .build(),
            sink_pending: meter
                .f64_gauge("saci_sink_pending_rows")
                .with_description("Rows buffered in one sink and not yet consumed downstream")
                .build(),
            source_rows: meter
                .u64_counter("saci_source_rows_total")
                .with_description("Total rows drained from sources")
                .build(),
            watchdog_ticks: meter
                .f64_gauge("saci_watchdog_ticks_total")
                .with_description("Watchdog ticks completed since the service started")
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
            processor_custom: meter
                .f64_histogram("saci_processor_custom")
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
            dlq_letters_pending: meter
                .f64_gauge("saci_dlq_letters_pending")
                .with_description("Dead letters waiting in one workflow's queue")
                .build(),
            processor_metric_names: std::sync::Mutex::new(std::collections::HashSet::new()),
            processor_metric_cap_warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The writers of workflow `id`, bound to its `workflow=` attribute.
    pub(crate) fn workflow(&'static self, id: &str) -> WorkflowMetrics {
        WorkflowMetrics {
            instruments: self,
            attrs: bound(crate::inspector_wire::WORKFLOW_ATTR, id),
        }
    }

    /// The writers of source node `id`, bound to its `source=` attribute.
    pub(crate) fn source(&'static self, id: &str) -> SourceMetrics {
        SourceMetrics {
            instruments: self,
            attrs: bound(crate::inspector_wire::SOURCE_ATTR, id),
        }
    }

    /// The writers of processor node `id`, bound to its `processor=`
    /// attribute.
    pub(crate) fn processor(&'static self, id: &str) -> ProcessorMetrics {
        ProcessorMetrics {
            instruments: self,
            attrs: bound(crate::inspector_wire::PROCESSOR_ATTR, id),
        }
    }

    /// The writers of sink node `id`, bound to its `sink=` attribute.
    pub(crate) fn sink(&'static self, id: &str) -> SinkMetrics {
        SinkMetrics {
            instruments: self,
            attrs: bound(crate::inspector_wire::SINK_ATTR, id),
        }
    }

    /// One stage that took `seconds`.
    ///
    /// Measures the `pipeline.stage` span a native `crate::Pipeline` opens;
    /// there is nothing to attribute this by, because the engine carries no
    /// metrics dependency of its own.
    pub(crate) fn stage_duration(&self, seconds: f64) {
        self.stage_duration.record(seconds, &[]);
    }

    /// Latest watchdog tick count, readiness flag and uptime.
    pub(crate) fn service_gauges(&self, ticks: u64, ready: bool, uptime_seconds: f64) {
        self.watchdog_ticks.record(ticks as f64, &[]);
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

    /// Whether processor-chosen metric `name` may be recorded.
    ///
    /// Names are processor-chosen, so distinct names are capped at
    /// [`MAX_PROCESSOR_METRIC_NAMES`] and anything past that is dropped after
    /// one warning. The cap counts distinct *names*, not name-and-processor
    /// pairs: two processors reporting one name spend one entry.
    fn admit_processor_metric(&self, name: &str) -> bool {
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

        if !admitted
            && !self
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
        admitted
    }
}

/// The attribute array a handle owns: `key` = `id`.
///
/// The value is a reference-counted string, so cloning the attribute for a
/// write that adds a second key costs a counter bump, not an allocation.
#[cfg(feature = "metrics")]
fn bound(key: &'static str, id: &str) -> [opentelemetry::KeyValue; 1] {
    [opentelemetry::KeyValue::new(
        key,
        std::sync::Arc::<str>::from(id),
    )]
}

/// The writers of one workflow, bound to its `workflow=` attribute.
#[cfg(feature = "metrics")]
#[derive(Clone)]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
pub(crate) struct WorkflowMetrics {
    instruments: &'static Instruments,
    attrs: [opentelemetry::KeyValue; 1],
}

#[cfg(feature = "metrics")]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl WorkflowMetrics {
    /// One completed workflow pass.
    ///
    /// A pass, not a source batch: under flow control a source batch larger
    /// than the admission credit is carried over several passes, so this
    /// counter and `saci_source_batches_total` have different denominators.
    /// The latter counts arrivals.
    pub(crate) fn pass(&self) {
        self.instruments.workflow_passes.add(1, &self.attrs);
    }

    /// One workflow pass that returned an error.
    ///
    /// Source and sink drain failures are counted separately in the runner
    /// stats and deliberately do not land here: conflating three failure kinds
    /// into one series makes it unalertable.
    pub(crate) fn pass_error(&self) {
        self.instruments.workflow_pass_errors.add(1, &self.attrs);
    }

    /// How many letters the workflow's dead letter queue currently holds.
    ///
    /// A queue holding nothing is a state worth reporting, so this records
    /// zero as readily as any other value.
    pub(crate) fn dlq_pending(&self, letters: u64) {
        self.instruments
            .dlq_letters_pending
            .record(letters as f64, &self.attrs);
    }
}

/// The writers of one source node, bound to its `source=` attribute.
#[cfg(feature = "metrics")]
#[derive(Clone)]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
pub(crate) struct SourceMetrics {
    instruments: &'static Instruments,
    attrs: [opentelemetry::KeyValue; 1],
}

#[cfg(feature = "metrics")]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl SourceMetrics {
    /// One non-empty drain.
    pub(crate) fn batch(&self) {
        self.instruments.source_batches.add(1, &self.attrs);
    }

    /// `n` rows drained.
    pub(crate) fn rows(&self, n: u64) {
        self.instruments.source_rows.add(n, &self.attrs);
    }

    /// The current adaptive target and smoothed throughput.
    ///
    /// Both values describe one source's admission, so a process-wide gauge
    /// over them would report whichever source wrote last.
    pub(crate) fn flow_state(&self, target_rows: usize, rows_per_sec: f64) {
        self.instruments
            .flow_target_rows
            .record(target_rows as f64, &self.attrs);
        self.instruments
            .flow_throughput
            .record(rows_per_sec, &self.attrs);
    }

    /// One flow-control back-off.
    pub(crate) fn flow_backoff(&self) {
        self.instruments.flow_backoffs.add(1, &self.attrs);
    }

    /// One rebuild into a working instance.
    pub(crate) fn heal(&self) {
        self.instruments.connector_heals.add(1, &self.attrs);
    }

    /// One rebuild attempt that produced nothing usable.
    pub(crate) fn heal_failure(&self) {
        self.instruments.connector_heal_failures.add(1, &self.attrs);
    }
}

/// The writers of one processor node, bound to its `processor=` attribute.
#[cfg(feature = "metrics")]
#[derive(Clone)]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
pub(crate) struct ProcessorMetrics {
    instruments: &'static Instruments,
    attrs: [opentelemetry::KeyValue; 1],
}

#[cfg(feature = "metrics")]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl ProcessorMetrics {
    /// One processor `run-batch` call, from the WIT `run-metrics` record.
    pub(crate) fn batch(
        &self,
        wall_ns: u64,
        rows_in: u64,
        rows_out: u64,
        systems_run: u32,
        retries: u32,
    ) {
        let i = self.instruments;
        i.processor_batch_duration
            .record(wall_ns as f64 / 1e9, &self.attrs);
        i.processor_rows_in.add(rows_in, &self.attrs);
        i.processor_rows_out.add(rows_out, &self.attrs);
        i.processor_systems_run
            .add(u64::from(systems_run), &self.attrs);
        i.processor_retries.add(u64::from(retries), &self.attrs);
    }

    /// `n` rows delivered along one labelled outbound edge.
    ///
    /// A second attributed form of `saci_processor_rows_out_total`, keyed
    /// `processor="<id>", branch="<name>"`, so the dashboard can rate each
    /// branch edge separately. Same instrument, no new series.
    pub(crate) fn branch_rows(&self, branch: &str, n: u64) {
        self.instruments.processor_rows_out.add(
            n,
            &[
                self.attrs[0].clone(),
                opentelemetry::KeyValue::new(
                    crate::inspector_wire::BRANCH_ATTR,
                    branch.to_string(),
                ),
            ],
        );
    }

    /// The event-time watermark of a windowed node, in epoch seconds.
    ///
    /// A watermark belongs to a node's merged inbound stream, so there is no
    /// meaningful process-wide watermark to sum over. Recorded only when the
    /// node's config declares a `window` block and the tracker has seen at
    /// least one timestamp.
    pub(crate) fn window_watermark(&self, seconds: f64) {
        self.instruments
            .window_watermark
            .record(seconds, &self.attrs);
    }

    /// One arrival a windowed node dropped whole: every row in it was older
    /// than `watermark - allowed_lateness_ms`, so the node opened no window,
    /// closed none, and passed nothing downstream.
    ///
    /// A rising count with live sources is the signature of an inbound stream
    /// whose event time has fallen behind the node's watermark: a producer
    /// restarted against a still-running service, a replay rewound, or one row
    /// with a far-future timestamp having dragged the watermark past
    /// everything real that follows. The sinks behind such a node stay empty
    /// while it climbs.
    pub(crate) fn window_late_arrival(&self) {
        self.instruments.window_late_arrivals.add(1, &self.attrs);
    }

    /// One value reported by processor code through `host-io::metric`.
    ///
    /// A histogram covers all three intents the WIT contract allows: a counter
    /// would reject a negative value and a gauge would lose every value but
    /// the last. Distinct names are capped at [`MAX_PROCESSOR_METRIC_NAMES`].
    pub(crate) fn custom(&self, name: &str, value: f64) {
        if !self.instruments.admit_processor_metric(name) {
            return;
        }
        // Keys are sorted ("metric" < "processor"), matching the documented
        // wire contract for this series' attribute order.
        self.instruments.processor_custom.record(
            value,
            &[
                opentelemetry::KeyValue::new("metric", name.to_string()),
                self.attrs[0].clone(),
            ],
        );
    }
}

/// The writers of one sink node, bound to its `sink=` attribute.
#[cfg(feature = "metrics")]
#[derive(Clone)]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
pub(crate) struct SinkMetrics {
    instruments: &'static Instruments,
    attrs: [opentelemetry::KeyValue; 1],
}

#[cfg(feature = "metrics")]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl SinkMetrics {
    /// One non-empty write of `rows` rows.
    ///
    /// The batch count alone cannot answer "how much is this sink moving": a
    /// batch is whatever row count the upstream handed over, so the same
    /// throughput reads as one batch or a hundred depending on the admission
    /// target. The row count is the comparable number.
    pub(crate) fn write(&self, rows: u64) {
        self.instruments.sink_batches.add(1, &self.attrs);
        self.instruments.sink_rows.add(rows, &self.attrs);
    }

    /// The backlog the sink reports through its `Sink::pending_rows` probe.
    ///
    /// A backlog is measured against one sink's own flush policy, so summing
    /// across sinks compares numbers that mean different things. A sink that
    /// reports no backlog records nothing rather than zero, which would be
    /// indistinguishable from a drained buffer.
    pub(crate) fn pending_rows(&self, rows: u64) {
        self.instruments
            .sink_pending
            .record(rows as f64, &self.attrs);
    }

    /// One rebuild into a working instance.
    pub(crate) fn heal(&self) {
        self.instruments.connector_heals.add(1, &self.attrs);
    }

    /// One rebuild attempt that produced nothing usable.
    pub(crate) fn heal_failure(&self) {
        self.instruments.connector_heal_failures.add(1, &self.attrs);
    }

    /// One batch of `rows` rows the sink refused and the queue stored.
    pub(crate) fn dlq_recorded(&self, rows: u64) {
        self.instruments.dlq_letters_recorded.add(1, &self.attrs);
        self.instruments.dlq_rows_recorded.add(rows, &self.attrs);
    }

    /// One dead letter a replay delivered to the sink.
    pub(crate) fn dlq_replayed(&self) {
        self.instruments.dlq_letters_replayed.add(1, &self.attrs);
    }

    /// One dead letter the store would not take, bound for the sink.
    ///
    /// The batch is gone: a letter is never buffered in memory waiting for
    /// the store to come back.
    pub(crate) fn dlq_lost(&self) {
        self.instruments.dlq_letters_lost.add(1, &self.attrs);
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
    pub(crate) fn workflow(&'static self, _id: &str) -> WorkflowMetrics {
        WorkflowMetrics
    }
    #[inline]
    pub(crate) fn source(&'static self, _id: &str) -> SourceMetrics {
        SourceMetrics
    }
    #[inline]
    pub(crate) fn processor(&'static self, _id: &str) -> ProcessorMetrics {
        ProcessorMetrics
    }
    #[inline]
    pub(crate) fn sink(&'static self, _id: &str) -> SinkMetrics {
        SinkMetrics
    }
    #[inline]
    pub(crate) fn stage_duration(&self, _seconds: f64) {}
    #[inline]
    pub(crate) fn service_gauges(&self, _ticks: u64, _ready: bool, _uptime_seconds: f64) {}
    #[inline]
    pub(crate) fn raft(&self, _commit_index: u64, _term: u64, _leader_id: Option<u64>) {}
}

/// Zero-sized stand-in for the workflow handle when `metrics` is off.
#[cfg(not(feature = "metrics"))]
#[derive(Clone)]
pub(crate) struct WorkflowMetrics;

#[cfg(not(feature = "metrics"))]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl WorkflowMetrics {
    #[inline]
    pub(crate) fn pass(&self) {}
    #[inline]
    pub(crate) fn pass_error(&self) {}
    #[inline]
    pub(crate) fn dlq_pending(&self, _letters: u64) {}
}

/// Zero-sized stand-in for the source handle when `metrics` is off.
#[cfg(not(feature = "metrics"))]
#[derive(Clone)]
pub(crate) struct SourceMetrics;

#[cfg(not(feature = "metrics"))]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl SourceMetrics {
    #[inline]
    pub(crate) fn batch(&self) {}
    #[inline]
    pub(crate) fn rows(&self, _n: u64) {}
    #[inline]
    pub(crate) fn flow_state(&self, _target_rows: usize, _rows_per_sec: f64) {}
    #[inline]
    pub(crate) fn flow_backoff(&self) {}
    #[inline]
    pub(crate) fn heal(&self) {}
    #[inline]
    pub(crate) fn heal_failure(&self) {}
}

/// Zero-sized stand-in for the processor handle when `metrics` is off.
#[cfg(not(feature = "metrics"))]
#[derive(Clone)]
pub(crate) struct ProcessorMetrics;

#[cfg(not(feature = "metrics"))]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl ProcessorMetrics {
    #[inline]
    pub(crate) fn batch(
        &self,
        _wall_ns: u64,
        _rows_in: u64,
        _rows_out: u64,
        _systems_run: u32,
        _retries: u32,
    ) {
    }
    #[inline]
    pub(crate) fn branch_rows(&self, _branch: &str, _n: u64) {}
    #[inline]
    pub(crate) fn window_watermark(&self, _seconds: f64) {}
    #[inline]
    pub(crate) fn window_late_arrival(&self) {}
    #[inline]
    pub(crate) fn custom(&self, _name: &str, _value: f64) {}
}

/// Zero-sized stand-in for the sink handle when `metrics` is off.
#[cfg(not(feature = "metrics"))]
#[derive(Clone)]
pub(crate) struct SinkMetrics;

#[cfg(not(feature = "metrics"))]
#[allow(dead_code, reason = "which writers exist depends on the feature set")]
impl SinkMetrics {
    #[inline]
    pub(crate) fn write(&self, _rows: u64) {}
    #[inline]
    pub(crate) fn pending_rows(&self, _rows: u64) {}
    #[inline]
    pub(crate) fn heal(&self) {}
    #[inline]
    pub(crate) fn heal_failure(&self) {}
    #[inline]
    pub(crate) fn dlq_recorded(&self, _rows: u64) {}
    #[inline]
    pub(crate) fn dlq_replayed(&self) {}
    #[inline]
    pub(crate) fn dlq_lost(&self) {}
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

    /// The sample value of the one line of `series` that carries `label`.
    fn value_of(text: &str, series: &str, label: &str) -> f64 {
        let lines: Vec<String> = lines_for(text, series)
            .into_iter()
            .filter(|line| line.contains(label))
            .collect();
        assert_eq!(lines.len(), 1, "{series} with {label}:\n{text}");
        lines[0]
            .rsplit(' ')
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("no sample value in {}", lines[0]))
    }

    fn scrape() -> String {
        TextEncoder::new()
            .encode_to_string(&super::test_registry().gather())
            .expect("encode prometheus text")
    }

    /// A processor writes one sample per event, under its own id, so every
    /// line of every `saci_processor_*` series carries `processor=`.
    ///
    /// The processor id is deliberately an unlikely one: the lib test binary
    /// shares one process-global registry, so this asserts on which label sets
    /// exist rather than on values other tests also move.
    #[test]
    fn a_processor_writes_only_its_attributed_series() {
        super::instruments()
            .processor("proc-7")
            .batch(1_500_000, 16, 16, 1, 0);
        let text = scrape();

        for series in [
            "saci_processor_rows_in_total",
            "saci_processor_rows_out_total",
            "saci_processor_systems_run_total",
            "saci_processor_retries_total",
            "saci_processor_batch_duration_seconds_count",
        ] {
            let lines = lines_for(&text, series);
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains(r#"processor="proc-7""#)),
                "{series} must carry the processor attribute:\n{text}"
            );
            assert!(
                lines.iter().all(|line| line.contains("processor=")),
                "{series} must have no unattributed sample:\n{text}"
            );
        }
    }

    /// A workflow writes one sample per event, under its own id.
    ///
    /// The workflow id is deliberately an unlikely one, for the same reason
    /// as the processor test above.
    #[test]
    fn a_workflow_writes_only_its_attributed_counters() {
        let workflow = super::instruments().workflow("wf-unit-alpha");
        workflow.pass();
        workflow.pass();
        workflow.pass_error();
        let text = scrape();

        for series in [
            "saci_workflow_passes_total",
            "saci_workflow_pass_errors_total",
        ] {
            let lines = lines_for(&text, series);
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains(r#"workflow="wf-unit-alpha""#)),
                "{series} must carry the workflow attribute:\n{text}"
            );
            assert!(
                lines.iter().all(|line| line.contains("workflow=")),
                "{series} must have no unattributed sample:\n{text}"
            );
        }
        assert_eq!(
            value_of(
                &text,
                "saci_workflow_passes_total",
                r#"workflow="wf-unit-alpha""#
            ),
            2.0
        );
    }

    /// A sink write moves its batch and row counters once each, under the
    /// sink's own id.
    ///
    /// A processor's custom metric is covered where the name cap is, in
    /// `wasm_host::host_impl`: the cap is process-wide state, so a second
    /// test spending names from it would make both order dependent.
    #[test]
    fn a_sink_write_records_one_sample() {
        super::instruments().sink("sink-z").write(5);
        let text = scrape();

        assert_eq!(
            value_of(&text, "saci_sink_rows_total", r#"sink="sink-z""#),
            5.0
        );
        assert_eq!(
            value_of(&text, "saci_sink_batches_total", r#"sink="sink-z""#),
            1.0
        );
    }
}
