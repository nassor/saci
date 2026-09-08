//! Adaptive flow control, end to end through the real runners.
//!
//! The invariant every case shares: re-chunking a source batch into
//! admission-sized chunks never loses, duplicates or reorders a row. Each case
//! also pins one behaviour of the controller that only the runner can show:
//! that the target adapts upward, that a backlogged iteration does not wait
//! out its poll interval, that a re-chunked batch is still one arrival, that
//! disabling flow control restores drain-to-EOF with unconditional pacing, and
//! that a source's own blocking poll is not counted as consumer time.
//!
//! The `windowed` module at the bottom carries the composition property: a
//! windowed processor's closed-window results do not depend on a source's
//! admission credit, because the runner never splits an arrival on a path
//! that reaches such a node.

#![cfg(feature = "service")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use saci_connector_channel::{ChannelSink, ChannelSource};
use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_core::runtime::PipelineRuntime;
use saci_core::{Dataset, SaciResult};
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use saci_service::service::config::{
    FlowControlConfig, HttpConfig, NodeConfig, ObservabilityConfig, RunMode, ServiceConfig,
    ServiceMode, StandaloneConfig, WorkflowSpec,
};
use saci_service::service::flow::FlowPlan;
use saci_service::service::registry::Registry;
use saci_service::service::standalone::{StandaloneStats, run_standalone};
use saci_service::service::stream::run_stream;

const COMP: &str = "values";

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch_of(schema: &Arc<Schema>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(values))])
        .expect("build a batch")
}

/// A processor whose cost is per pass, not per row: the shape that makes a
/// larger chunk genuinely faster, so an experiment has something to find. The
/// dataset passes through untouched, so the sink sees exactly the admitted
/// rows.
struct FixedCostRuntime {
    schema: Arc<Schema>,
    per_pass: Duration,
}

#[async_trait(?Send)]
impl PipelineRuntime for FixedCostRuntime {
    fn name(&self) -> &str {
        "fixed-cost"
    }

    async fn run_on(&self, _data: &mut Dataset) -> SaciResult<()> {
        tokio::time::sleep(self.per_pass).await;
        Ok(())
    }

    async fn run_on_with_state(
        &self,
        data: &mut Dataset,
        _prior: Option<&[u8]>,
    ) -> SaciResult<Option<Vec<u8>>> {
        self.run_on(data).await.map(|()| None)
    }

    fn declared_components(&self) -> Vec<&str> {
        vec![COMP]
    }

    fn template_dataset(&self) -> Dataset {
        let mut dataset = Dataset::new();
        dataset.register_raw_component(COMP, Arc::clone(&self.schema));
        dataset
    }
}

/// A source that blocks before every batch, the shape of a connector with a
/// poll timeout (`KafkaSource` and `NatsSource` both default to one second).
/// Waiting here is not consumer time and must not be measured as such.
struct SlowPollSource {
    schema: Arc<Schema>,
    poll_delay: Duration,
    rows_per_batch: usize,
    batches: usize,
    next_value: i64,
}

#[async_trait]
impl Source for SlowPollSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if self.batches == 0 {
            return Ok(None);
        }
        tokio::time::sleep(self.poll_delay).await;
        self.batches -= 1;
        let start = self.next_value;
        self.next_value += self.rows_per_batch as i64;
        Ok(Some(batch_of(
            &self.schema,
            (start..self.next_value).collect(),
        )))
    }
}

/// Assemble `source` -> one [`FixedCostRuntime`] -> one [`ChannelSink`], the
/// topological order every runner requires.
fn service_with(
    source: Box<dyn Source>,
    schema: Arc<Schema>,
    per_pass: Duration,
) -> (BuiltService, mpsc::Receiver<RecordBatch>) {
    let (sink, rx) = ChannelSink::new(Arc::clone(&schema), 4_096);
    let nodes = vec![
        BuiltNode {
            id: "in".to_string(),
            name: None,
            type_name: "ChannelSource".to_string(),
            component: Some(COMP),
            kind: BuiltNodeKind::Source(source),
            downstream: vec![BuiltEdge {
                node: 1,
                branch: None,
            }],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
        BuiltNode {
            id: "work".to_string(),
            name: None,
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(FixedCostRuntime {
                    schema: Arc::clone(&schema),
                    per_pass,
                }),
                kind: "native",
            },
            downstream: vec![BuiltEdge {
                node: 2,
                branch: None,
            }],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
        BuiltNode {
            id: "out".to_string(),
            name: None,
            type_name: "ChannelSink".to_string(),
            component: Some(COMP),
            kind: BuiltNodeKind::Sink(Box::new(sink)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
    ];
    (
        BuiltService {
            workflow_id: "flow".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(Registry::new()),
            inspector: None,
            dlq: None,
        },
        rx,
    )
}

/// A channel source preloaded with `batches` batches of `rows_per_batch`
/// consecutive values, already at EOF.
fn preloaded_source(
    schema: &Arc<Schema>,
    batches: usize,
    rows_per_batch: usize,
) -> Box<dyn Source> {
    let (tx, source) = ChannelSource::new(Arc::clone(schema), batches + 1);
    for b in 0..batches {
        let start = (b * rows_per_batch) as i64;
        let end = start + rows_per_batch as i64;
        tx.try_send(batch_of(schema, (start..end).collect()))
            .expect("the channel is sized for every batch");
    }
    drop(tx);
    Box::new(source)
}

/// A config in `run_mode` whose top-level `flow_control` is `flow`. A
/// hand-built service declares no source in the config, so every source
/// resolves this block.
fn config_with(run_mode: RunMode, flow: FlowControlConfig) -> ServiceConfig {
    ServiceConfig {
        heal: Default::default(),
        flow_control: flow,
        node: NodeConfig {
            id: 1,
            name: None,
            data_dir: std::path::PathBuf::from("/tmp/saci-flow-test"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig { run_mode },
        },
        workflows: vec![WorkflowSpec {
            id: "flow".to_string(),
            name: None,
            transformers: Vec::new(),
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
        variables: std::collections::HashMap::new(),
    }
}

/// A fast-adapting policy: the shipped one-minute epoch and four-sample arms
/// cannot close inside a test, so the experiment is paced at 50 ms with two
/// samples per arm. Everything else is the shipped default.
fn quick_flow(start_rows: u64) -> FlowControlConfig {
    FlowControlConfig {
        min_rows: Some(64),
        max_rows: Some(8_192),
        start_rows: Some(start_rows),
        adjust_interval_ms: Some(50),
        min_samples_per_arm: Some(2),
        ..FlowControlConfig::default()
    }
}

/// Every chunk the sink received, in arrival order.
fn drain(rx: &mut mpsc::Receiver<RecordBatch>) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        out.push(batch);
    }
    out
}

/// Concatenated values of every chunk, in arrival order.
fn values(chunks: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for chunk in chunks {
        let column = chunk
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column");
        out.extend(column.values().iter().copied());
    }
    out
}

/// Drive `service` under `config` until `total` rows have reached the sink (or
/// a generous deadline passes), then cancel and join.
///
/// A `PipelineRuntime` future is `?Send`, so the runner runs on a `LocalSet`
/// exactly as the stream-mode tests drive it.
async fn run_until_rows(
    service: BuiltService,
    config: ServiceConfig,
    rx: &mut mpsc::Receiver<RecordBatch>,
    total: usize,
) -> (StandaloneStats, Vec<RecordBatch>) {
    let cancel = CancellationToken::new();
    let runner_cancel = cancel.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_standalone(service, &config, runner_cancel, None, None).await
    });

    let mut chunks = local
        .run_until(async {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut chunks = Vec::new();
            let mut rows = 0usize;
            while rows < total && Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                    Ok(Some(chunk)) => {
                        rows += chunk.num_rows();
                        chunks.push(chunk);
                    }
                    Ok(None) => break,
                    Err(_elapsed) => {}
                }
            }
            chunks
        })
        .await;

    cancel.cancel();
    let stats = local
        .run_until(handle)
        .await
        .expect("the runner task joins")
        .expect("the run succeeds");
    chunks.extend(drain(rx));
    (stats, chunks)
}

/// The whole point: a multi-thousand-row source re-chunked by the controller
/// arrives complete, once each, in order, and the target climbs above where it
/// started.
#[tokio::test]
async fn flow_control_delivers_every_row_exactly_once_in_order_while_adapting_upward() {
    const BATCHES: usize = 24;
    const ROWS_PER_BATCH: usize = 1_024;
    const TOTAL: usize = BATCHES * ROWS_PER_BATCH;

    let schema = test_schema();
    let source = preloaded_source(&schema, BATCHES, ROWS_PER_BATCH);
    let (service, mut rx) = service_with(source, Arc::clone(&schema), Duration::from_millis(3));
    let config = config_with(RunMode::Continuous, quick_flow(256));

    let (stats, chunks) = run_until_rows(service, config, &mut rx, TOTAL).await;

    let delivered = values(&chunks);
    assert_eq!(
        delivered.len(),
        TOTAL,
        "every admitted row must reach the sink exactly once"
    );
    assert!(
        delivered.iter().copied().eq(0..TOTAL as i64),
        "re-chunking must not reorder or duplicate a row"
    );
    assert_eq!(stats.rows_processed, TOTAL as u64);
    assert_eq!(stats.iteration_errors, 0);

    let largest = chunks
        .iter()
        .map(RecordBatch::num_rows)
        .max()
        .expect("at least one chunk");
    // Why a climbing target matters: pinned at start_rows, this source would
    // be re-chunked into TOTAL/256 = 96 credit-sized passes, each paying the
    // whole per-pass cost of the runtime. Climbing carries the same rows in
    // fewer, larger passes.
    assert!(
        largest > 256,
        "the target must climb above start_rows; largest chunk was {largest}"
    );
}

/// `interval_ms` is the idle poll cadence, not a throughput cap: an iteration
/// that left work pending re-enters at once.
#[tokio::test]
async fn a_backlogged_interval_iteration_does_not_wait_out_the_poll_interval() {
    const BATCHES: usize = 8;
    const ROWS_PER_BATCH: usize = 1_024;
    const TOTAL: usize = BATCHES * ROWS_PER_BATCH;

    let schema = test_schema();
    let source = preloaded_source(&schema, BATCHES, ROWS_PER_BATCH);
    let (service, mut rx) = service_with(source, Arc::clone(&schema), Duration::from_millis(1));
    // One minute between polls: only a drained iteration may pay it.
    let config = config_with(
        RunMode::Interval {
            interval_ms: 60_000,
        },
        quick_flow(256),
    );

    let (stats, chunks) = run_until_rows(service, config, &mut rx, TOTAL).await;

    assert_eq!(
        values(&chunks).len(),
        TOTAL,
        "the whole backlog must drain without waiting for the next tick"
    );
    assert!(
        stats.iterations > 1,
        "a credit-sized admission means several iterations, got {}",
        stats.iterations
    );
}

/// `source_batches_drained` counts arrivals, not chunks: one source batch
/// several times the admission credit is one drained batch however many passes
/// carry it, while the row counters still count every row. Counting a carried
/// tail as an arrival would report eight batches where one was pulled.
#[tokio::test]
async fn a_re_chunked_source_batch_is_one_arrival_carrying_all_its_rows() {
    const ROWS: usize = 2_048;
    const PIN: u64 = 256;

    let schema = test_schema();
    let source = preloaded_source(&schema, 1, ROWS);
    let (service, mut rx) = service_with(source, Arc::clone(&schema), Duration::from_millis(1));
    // Pinned, so the chunk count is arithmetic rather than whatever the
    // experiment happens to settle on. `ChannelSource` ignores the row hint,
    // so the runner receives all 2048 rows in one pull and re-chunks them
    // itself.
    let config = config_with(
        RunMode::Interval {
            interval_ms: 60_000,
        },
        FlowControlConfig {
            rows: Some(PIN),
            ..FlowControlConfig::default()
        },
    );

    let (stats, chunks) = run_until_rows(service, config, &mut rx, ROWS).await;

    let delivered = values(&chunks);
    assert!(
        delivered.iter().copied().eq(0..ROWS as i64),
        "the pulled batch must arrive complete, once each, in order"
    );
    assert_eq!(
        chunks.len(),
        ROWS / PIN as usize,
        "a pinned {PIN}-row credit carries {ROWS} rows in that many chunks, \
         or this case is not re-chunking anything"
    );
    assert_eq!(
        stats.source_batches_drained, 1,
        "one pull is one arrival, however many chunks carried it"
    );
    assert_eq!(stats.rows_processed, ROWS as u64);

    let source_node = stats
        .nodes
        .iter()
        .find(|node| node.id == "in")
        .expect("the source node appears in the per-node breakdown");
    assert_eq!(
        source_node.batches, 1,
        "the per-node arrival counter agrees with the flat one"
    );
    assert_eq!(source_node.rows, ROWS as u64);
}

/// With flow control off, an iteration drains every source to EOF and pacing
/// is unconditional: one iteration for a finite source, exactly as a runner
/// with no flow control at all.
#[tokio::test]
async fn a_disabled_flow_control_run_drains_a_finite_source_in_one_iteration() {
    const BATCHES: usize = 8;
    const ROWS_PER_BATCH: usize = 1_024;
    const TOTAL: usize = BATCHES * ROWS_PER_BATCH;

    let schema = test_schema();
    let source = preloaded_source(&schema, BATCHES, ROWS_PER_BATCH);
    let (service, mut rx) = service_with(source, Arc::clone(&schema), Duration::from_millis(1));
    let config = config_with(
        RunMode::Interval {
            interval_ms: 60_000,
        },
        FlowControlConfig {
            enabled: Some(false),
            ..FlowControlConfig::default()
        },
    );

    let (stats, chunks) = run_until_rows(service, config, &mut rx, TOTAL).await;

    assert_eq!(values(&chunks).len(), TOTAL);
    assert_eq!(
        stats.iterations, 1,
        "drain-to-EOF plus unconditional pacing is one iteration for a finite source"
    );
    assert_eq!(
        chunks.len(),
        1,
        "with no admission credit the whole source is one drained dataset and one sink write"
    );
}

/// A source that blocks for half a second before each batch must not have that
/// wait counted as consumer time. `total_busy_micros` is the per-item time the
/// runner measures and the controller judges an arm on, so charging the poll to
/// it would leave no room in the run's wall clock for the sleeping that
/// certainly happened. In stream mode, where the latency objective is on by
/// default at 250 ms, every candidate arm would then breach it.
#[tokio::test]
async fn a_slow_source_poll_is_not_consumer_time() {
    const BATCHES: usize = 4;
    const ROWS_PER_BATCH: usize = 8_192;
    const TOTAL: usize = BATCHES * ROWS_PER_BATCH;
    const START_ROWS: u64 = 1_024;
    const POLL_DELAY: Duration = Duration::from_millis(500);

    let schema = test_schema();
    let source = Box::new(SlowPollSource {
        schema: Arc::clone(&schema),
        poll_delay: POLL_DELAY,
        rows_per_batch: ROWS_PER_BATCH,
        batches: BATCHES,
        next_value: 0,
    });
    let (service, rx) = service_with(source, Arc::clone(&schema), Duration::from_millis(3));
    let config = config_with(RunMode::Stream, quick_flow(START_ROWS));
    let flow = FlowPlan::from_config(&config);
    assert_eq!(
        flow.for_source("in").target_latency_ms,
        250,
        "stream mode carries the latency objective this test relies on"
    );

    // The sink is drained concurrently, as a live consumer would: a backlog
    // nobody consumes is real pressure and would back the target off, which is
    // a different mechanism from the one under test.
    let collector = tokio::spawn(async move {
        let mut rx = rx;
        let mut chunks = Vec::new();
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }
        chunks
    });
    let started = Instant::now();
    let stats = run_stream(service, CancellationToken::new(), None, None, &flow)
        .await
        .expect("the stream run reaches EOF");
    let elapsed = started.elapsed();
    let chunks = collector.await.expect("the collector task joins");
    let delivered = values(&chunks);
    assert_eq!(delivered.len(), TOTAL, "every row must arrive once");
    assert!(
        delivered.iter().copied().eq(0..TOTAL as i64),
        "chunking a stream batch must not reorder or duplicate a row"
    );
    assert_eq!(stats.iteration_errors, 0);

    // The source sleeps once per batch, and the runner is serial, so the run's
    // wall clock is the waiting plus the per-item time it measured. Charging
    // the poll to the consumer would fold the waiting into
    // `total_busy_micros` and leave no room for it outside. Comparing the two
    // measurements from the same run rather than the per-item total against a
    // fixed bound is what makes this independent of how loaded the host is:
    // work stretches under load, sleeping does not shrink.
    let waited = POLL_DELAY * BATCHES as u32;
    let busy = Duration::from_micros(stats.total_busy_micros);
    assert!(
        elapsed.saturating_sub(busy) >= waited,
        "the poll wait must not be measured as consumer time: the run took \
         {elapsed:?} with {busy:?} of per-item time, leaving {:?} for {waited:?} \
         of sleeping",
        elapsed.saturating_sub(busy)
    );
}

/// Disabling flow control in stream mode restores the old shape exactly: the
/// arriving batch is one pass, unsliced, and the connector is given no hint.
#[tokio::test]
async fn a_disabled_stream_run_passes_each_arriving_batch_through_unsliced() {
    const BATCHES: usize = 3;
    const ROWS_PER_BATCH: usize = 4_096;
    const TOTAL: usize = BATCHES * ROWS_PER_BATCH;

    let schema = test_schema();
    let source = preloaded_source(&schema, BATCHES, ROWS_PER_BATCH);
    let (service, rx) = service_with(source, Arc::clone(&schema), Duration::from_millis(1));
    let config = config_with(
        RunMode::Stream,
        FlowControlConfig {
            enabled: Some(false),
            ..FlowControlConfig::default()
        },
    );
    let flow = FlowPlan::from_config(&config);

    let collector = tokio::spawn(async move {
        let mut rx = rx;
        let mut chunks = Vec::new();
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }
        chunks
    });
    let stats = run_stream(service, CancellationToken::new(), None, None, &flow)
        .await
        .expect("the stream run reaches EOF");
    let chunks = collector.await.expect("the collector task joins");

    assert_eq!(values(&chunks).len(), TOTAL);
    assert_eq!(
        chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        vec![ROWS_PER_BATCH; BATCHES],
        "a disabled controller must not slice an arriving batch"
    );
    assert_eq!(
        stats.iterations, BATCHES as u64,
        "one item per arriving batch, as before flow control"
    );
}

/// The stream counterpart of
/// `a_re_chunked_source_batch_is_one_arrival_carrying_all_its_rows`: chunking
/// changes what a *pass* is, and must change nothing about what an *arrival*
/// is. Counting a chunk as an arrival would report eight pulls where one
/// happened, and would advance the persisted source cursor eight times for
/// one batch.
#[tokio::test]
async fn a_re_chunked_stream_arrival_is_one_drained_batch() {
    const ROWS: usize = 2_048;
    const PIN: u64 = 256;

    let schema = test_schema();
    let source = preloaded_source(&schema, 1, ROWS);
    let (service, rx) = service_with(source, Arc::clone(&schema), Duration::from_millis(1));
    let config = config_with(
        RunMode::Stream,
        FlowControlConfig {
            rows: Some(PIN),
            ..FlowControlConfig::default()
        },
    );
    let flow = FlowPlan::from_config(&config);

    let collector = tokio::spawn(async move {
        let mut rx = rx;
        let mut chunks = Vec::new();
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }
        chunks
    });
    let stats = run_stream(service, CancellationToken::new(), None, None, &flow)
        .await
        .expect("the stream run reaches EOF");
    let chunks = collector.await.expect("the collector task joins");

    assert!(
        values(&chunks).iter().copied().eq(0..ROWS as i64),
        "the arrival must arrive complete, once each, in order"
    );
    assert_eq!(
        chunks.len(),
        ROWS / PIN as usize,
        "a pinned {PIN}-row credit carries {ROWS} rows in that many chunks, \
         or this case is not re-chunking anything"
    );
    assert_eq!(
        stats.iterations,
        (ROWS / PIN as usize) as u64,
        "one pass per chunk"
    );
    assert_eq!(
        stats.source_batches_drained, 1,
        "one pull is one arrival, however many passes carried it"
    );
    let source_node = stats
        .nodes
        .iter()
        .find(|node| node.id == "in")
        .expect("the source node appears in the per-node breakdown");
    assert_eq!(
        source_node.batches, 1,
        "the per-node arrival counter agrees with the flat one"
    );
    assert_eq!(source_node.rows, ROWS as u64);
}

/// A source part-way through an arrival is served before any source is
/// polled.
///
/// The rotation is otherwise round-robin, so without that priority a
/// half-delivered arrival would sit in the carry-over buffer for as long as a
/// sibling source stayed idle, its rows already pulled from the connector,
/// already counted as an arrival, and not yet anywhere. With it, an arrival
/// that spans several passes is finished before the rotation moves on.
#[tokio::test]
async fn a_stream_tail_is_finished_before_a_sibling_source_is_polled() {
    const BIG_ROWS: usize = 2_048;
    const PIN: u64 = 256;
    const SIBLING_BASE: i64 = 1_000_000;
    const SIBLING_BATCHES: i64 = 3;
    const SIBLING_ROWS: i64 = 8;

    let schema = test_schema();
    let big = preloaded_source(&schema, 1, BIG_ROWS);
    let (tx, sibling) = ChannelSource::new(Arc::clone(&schema), 8);
    for batch in 0..SIBLING_BATCHES {
        let start = SIBLING_BASE + batch * SIBLING_ROWS;
        tx.try_send(batch_of(&schema, (start..start + SIBLING_ROWS).collect()))
            .expect("the channel is sized for every batch");
    }
    drop(tx);

    let (sink, rx) = ChannelSink::new(Arc::clone(&schema), 4_096);
    let service = BuiltService {
        workflow_id: "flow".to_string(),
        workflow_name: None,
        nodes: vec![
            BuiltNode {
                id: "big".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some(COMP),
                kind: BuiltNodeKind::Source(big),
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
                heal_recovered: None,
            },
            BuiltNode {
                id: "sibling".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some(COMP),
                kind: BuiltNodeKind::Source(Box::new(sibling)),
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
                heal_recovered: None,
            },
            BuiltNode {
                id: "work".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(FixedCostRuntime {
                        schema: Arc::clone(&schema),
                        per_pass: Duration::from_millis(1),
                    }),
                    kind: "native",
                },
                downstream: vec![BuiltEdge {
                    node: 3,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
                heal_recovered: None,
            },
            BuiltNode {
                id: "out".to_string(),
                name: None,
                type_name: "ChannelSink".to_string(),
                component: Some(COMP),
                kind: BuiltNodeKind::Sink(Box::new(sink)),
                downstream: Vec::new(),
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
                heal_recovered: None,
            },
        ],
        registry: Arc::new(Registry::new()),
        inspector: None,
        dlq: None,
    };
    let config = config_with(
        RunMode::Stream,
        FlowControlConfig {
            rows: Some(PIN),
            ..FlowControlConfig::default()
        },
    );
    let flow = FlowPlan::from_config(&config);

    let collector = tokio::spawn(async move {
        let mut rx = rx;
        let mut chunks = Vec::new();
        while let Some(chunk) = rx.recv().await {
            chunks.push(chunk);
        }
        chunks
    });
    let stats = run_stream(service, CancellationToken::new(), None, None, &flow)
        .await
        .expect("the stream run reaches EOF");
    let delivered = values(&collector.await.expect("the collector task joins"));

    let sibling_total = (SIBLING_BATCHES * SIBLING_ROWS) as usize;
    assert_eq!(
        delivered.len(),
        BIG_ROWS + sibling_total,
        "every row arrives"
    );
    assert!(
        delivered[..BIG_ROWS].iter().copied().eq(0..BIG_ROWS as i64),
        "the eight chunks of the big arrival must be delivered in order and \
         without the sibling's rows cutting in: a tail is served before any \
         source is polled, got {:?}",
        &delivered[..BIG_ROWS.min(delivered.len())]
    );
    assert!(
        delivered[BIG_ROWS..].iter().all(|v| *v >= SIBLING_BASE),
        "and the sibling's arrivals follow once the tail is spent"
    );
    assert_eq!(
        stats.source_batches_drained,
        1 + SIBLING_BATCHES as u64,
        "four pulls across the two sources, however many passes carried them"
    );
}

// ---------------------------------------------------------------------------
// Windowing x flow control
// ---------------------------------------------------------------------------

/// Credit independence of a windowed workflow's closed-window results.
///
/// A windowed processor's output is a function of how rows are grouped into
/// passes: each pass advances a watermark, classifies rows against it, and
/// fires whatever windows that watermark closed. So the runner does not let a
/// source's admission credit choose a boundary a windowed node can see. No
/// boundary is ever drawn inside an arrival on a path that reaches such a
/// node: the credit only stops the runner pulling the *next* arrival, and it
/// never reaches the connector as a `request_batch_rows` hint.
///
/// These cases pin the two halves of that. `flow_control { enabled #false }`
/// (one pass per drain) and `flow_control { rows 64 }` (a credit spent on the
/// first arrival, so one pass per arrival) must emit the same set of window
/// rows: with disorder deep inside a single arrival, well past the lateness
/// budget, which only a whole arrival can carry; and with disorder inside the
/// budget across the arrival boundary, which is where the budget does the
/// work.
///
/// What the budget still has to cover is disorder *across* a source's
/// arrivals and event-time skew between fan-in peers.
/// [`WINDOW_LATENESS_MS`] encodes that here: the credit does still decide how
/// many arrivals share a pass, and fan-in skew is a property of the streams.
/// See `crates/saci-service/src/service/windowing.rs` for the rule as
/// documentation.
#[cfg(feature = "windows")]
mod windowed {
    use super::*;

    use arrow_array::{Array, Float64Array, StringArray};
    use saci_core::windows::WindowSpec;
    use saci_core::windows::watermark::WatermarkState;
    use saci_service::service::config::WindowConfig;

    /// Inbound component: one sale at an instant in event time.
    const SALE: &str = "Sale";
    /// Outbound component: one closed `(window_id, symbol)` aggregate.
    const TOTAL: &str = "WindowTotal";
    /// Grouping keys, cycled row by row and shared by every source, so a
    /// merged window group can only be right if every source's rows for it
    /// landed before it fired.
    const SYMBOLS: [&str; 2] = ["a", "b"];
    /// Tumbling window size.
    const WINDOW_SIZE_MS: i64 = 500;
    /// Allowed lateness. It covers what the budget is responsible for here:
    /// the event-time span of one source batch (150 rows x 20 ms, or 200 rows
    /// x 10 ms = 2 s), which is the largest fan-in skew either runner shape
    /// can produce, since stream mode with flow control off admits a whole
    /// batch from one source before its peer's first row; and the 1.49 s of
    /// across-arrival disorder the late-row fixture carries.
    const WINDOW_LATENESS_MS: i64 = 3_000;

    fn sale_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("sym", DataType::Utf8, false),
            Field::new("amt", DataType::Float64, false),
        ]))
    }

    fn total_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("window_id", DataType::Int64, false),
            Field::new("sym", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("sum", DataType::Float64, false),
        ]))
    }

    /// One open window group, carried across passes in the state blob.
    #[derive(serde::Serialize, serde::Deserialize)]
    struct OpenGroup {
        window_id: i64,
        sym: String,
        count: i64,
        sum: f64,
    }

    /// The processor's cross-pass state: the watermark it left behind plus
    /// every window group still open. Exactly the shape
    /// `examples/windowing/tumbling/plugin` keeps in its checkpoint.
    #[derive(serde::Serialize, serde::Deserialize, Default)]
    struct WindowState {
        /// `None` until the first timestamp is observed.
        watermark_ms: Option<i64>,
        open: Vec<OpenGroup>,
    }

    /// A windowed processor in the shape the engine documents: accumulate into
    /// open windows across passes, fire a window once its watermark has passed
    /// `end + allowed_lateness`, and classify lateness against the watermark
    /// the *previous* pass left, `WatermarkState`'s own rule, so a row is
    /// never late relative to the pass it arrived in.
    ///
    /// Amounts are integer-valued so a group's `sum` is exact in `f64` however
    /// the runner ordered the additions: the two shapes accumulate a fan-in
    /// group in different orders, and float addition is not associative.
    struct WindowingRuntime {
        in_schema: Arc<Schema>,
        out_schema: Arc<Schema>,
    }

    #[async_trait(?Send)]
    impl PipelineRuntime for WindowingRuntime {
        fn name(&self) -> &str {
            "windowing"
        }

        async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
            self.run_on_with_state(data, None).await.map(|_| ())
        }

        async fn run_on_with_state(
            &self,
            data: &mut Dataset,
            prior: Option<&[u8]>,
        ) -> SaciResult<Option<Vec<u8>>> {
            let mut state: WindowState = match prior {
                Some(blob) => serde_json::from_slice(blob)
                    .map_err(|e| SaciError::generic(format!("window state decode: {e}")))?,
                None => WindowState::default(),
            };

            let batch = data
                .batch_for(SALE)
                .cloned()
                .ok_or_else(|| SaciError::generic("Sale component missing"))?;

            let mut watermark = WatermarkState::new(WINDOW_LATENESS_MS);
            if let Some(previous) = state.watermark_ms {
                watermark.advance(previous);
            }

            if batch.num_rows() > 0 {
                let ts = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| SaciError::generic("ts is not Int64"))?;
                let sym = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| SaciError::generic("sym is not Utf8"))?;
                let amt = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| SaciError::generic("amt is not Float64"))?;

                // Classify first, advance second: a row is late only against
                // what earlier passes established, never against its own.
                let accepted: Vec<usize> = (0..batch.num_rows())
                    .filter(|&i| !watermark.is_beyond_lateness(ts.value(i)))
                    .collect();
                for i in 0..batch.num_rows() {
                    watermark.advance(ts.value(i));
                }

                for i in accepted {
                    let window_id = WindowSpec::assign_tumbling(ts.value(i), WINDOW_SIZE_MS, 0);
                    let symbol = sym.value(i);
                    match state
                        .open
                        .iter_mut()
                        .find(|g| g.window_id == window_id && g.sym == symbol)
                    {
                        Some(group) => {
                            group.count += 1;
                            group.sum += amt.value(i);
                        }
                        None => state.open.push(OpenGroup {
                            window_id,
                            sym: symbol.to_string(),
                            count: 1,
                            sum: amt.value(i),
                        }),
                    }
                }
            }

            let now = watermark.current_watermark();
            state.watermark_ms = (now != i64::MIN).then_some(now);

            // Fire on `end + allowed_lateness`, the garbage-collection time:
            // a window stays open for its whole lateness budget, so a row
            // inside that budget still joins the group it belongs to instead
            // of re-firing a window that already closed.
            let mut fired: Vec<OpenGroup> = Vec::new();
            let mut still_open: Vec<OpenGroup> = Vec::new();
            for group in std::mem::take(&mut state.open) {
                let end = (group.window_id + 1) * WINDOW_SIZE_MS;
                if state
                    .watermark_ms
                    .is_some_and(|wm| end + WINDOW_LATENESS_MS <= wm)
                {
                    fired.push(group);
                } else {
                    still_open.push(group);
                }
            }
            state.open = still_open;

            if !fired.is_empty() {
                let rows = &mut fired;
                rows.sort_by(|l, r| (l.window_id, &l.sym).cmp(&(r.window_id, &r.sym)));
                let out = RecordBatch::try_new(
                    Arc::clone(&self.out_schema),
                    vec![
                        Arc::new(Int64Array::from(
                            rows.iter().map(|g| g.window_id).collect::<Vec<_>>(),
                        )),
                        Arc::new(StringArray::from(
                            rows.iter().map(|g| g.sym.as_str()).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            rows.iter().map(|g| g.count).collect::<Vec<_>>(),
                        )),
                        Arc::new(Float64Array::from(
                            rows.iter().map(|g| g.sum).collect::<Vec<_>>(),
                        )),
                    ],
                )
                .map_err(|e| SaciError::generic(format!("window total batch: {e}")))?;
                data.append_record_batch(TOTAL, out)?;
            }

            let blob = serde_json::to_vec(&state)
                .map_err(|e| SaciError::generic(format!("window state encode: {e}")))?;
            Ok(Some(blob))
        }

        fn declared_components(&self) -> Vec<&str> {
            vec![SALE, TOTAL]
        }

        fn template_dataset(&self) -> Dataset {
            let mut dataset = Dataset::new();
            dataset.register_raw_component(SALE, Arc::clone(&self.in_schema));
            dataset.register_raw_component(TOTAL, Arc::clone(&self.out_schema));
            dataset
        }
    }

    /// One source batch: `count` rows whose global indices start at `first`,
    /// timestamps `offset_ms + index * step_ms`, symbols cycling through
    /// [`SYMBOLS`], amounts `amount_base + index`, integral, so a group's sum
    /// is exact whatever order the runner accumulated it in.
    fn sale_batch(
        schema: &Arc<Schema>,
        first: usize,
        count: usize,
        step_ms: i64,
        offset_ms: i64,
        amount_base: i64,
    ) -> RecordBatch {
        let indices: Vec<usize> = (first..first + count).collect();
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(
                    indices
                        .iter()
                        .map(|&i| offset_ms + i as i64 * step_ms)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    indices
                        .iter()
                        .map(|&i| SYMBOLS[i % SYMBOLS.len()])
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    indices
                        .iter()
                        .map(|&i| (amount_base + i as i64) as f64)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("build a sale batch")
    }

    /// A channel source preloaded with `batches` ascending-event-time batches,
    /// already at EOF. `offset_ms` staggers a fan-in peer against it, which is
    /// the skew [`WINDOW_LATENESS_MS`] has to cover. The late-row fixture
    /// below carries the disorder cases.
    fn sale_source(
        schema: &Arc<Schema>,
        batches: usize,
        rows_per_batch: usize,
        step_ms: i64,
        offset_ms: i64,
        amount_base: i64,
    ) -> Box<dyn Source> {
        let (tx, source) = ChannelSource::new(Arc::clone(schema), batches + 1);
        for b in 0..batches {
            tx.try_send(sale_batch(
                schema,
                b * rows_per_batch,
                rows_per_batch,
                step_ms,
                offset_ms,
                amount_base,
            ))
            .expect("the channel is sized for every batch");
        }
        drop(tx);
        Box::new(source)
    }

    /// Event-time base of the late-row fixture, so a row moved backwards
    /// still lands at a non-negative timestamp.
    const LATE_BASE_MS: i64 = 10_000;

    /// One source batch of `count` rows whose global indices start at `first`,
    /// in ascending event time `LATE_BASE_MS + index * step_ms`, except the
    /// row at local index `late_offset`, which sits `back_ms` earlier than its
    /// own ascending timestamp. Symbol and amount still follow the row's
    /// global index, so the moved row keeps its identity and belongs to the
    /// window its new timestamp names.
    fn late_row_batch(
        schema: &Arc<Schema>,
        first: usize,
        count: usize,
        step_ms: i64,
        late_offset: usize,
        back_ms: i64,
    ) -> RecordBatch {
        let indices: Vec<usize> = (first..first + count).collect();
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(
                    indices
                        .iter()
                        .enumerate()
                        .map(|(local, &i)| {
                            let ascending = LATE_BASE_MS + i as i64 * step_ms;
                            if local == late_offset {
                                ascending - back_ms
                            } else {
                                ascending
                            }
                        })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    indices
                        .iter()
                        .map(|&i| SYMBOLS[i % SYMBOLS.len()])
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    indices.iter().map(|&i| i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("build a late-row sale batch")
    }

    /// A channel source preloaded with `batches` [`late_row_batch`] arrivals,
    /// already at EOF: every arrival carries one row moved `back_ms` back in
    /// event time, at local index `late_offset`.
    fn late_row_source(
        schema: &Arc<Schema>,
        batches: usize,
        rows_per_batch: usize,
        step_ms: i64,
        late_offset: usize,
        back_ms: i64,
    ) -> Box<dyn Source> {
        let (tx, source) = ChannelSource::new(Arc::clone(schema), batches + 1);
        for b in 0..batches {
            tx.try_send(late_row_batch(
                schema,
                b * rows_per_batch,
                rows_per_batch,
                step_ms,
                late_offset,
                back_ms,
            ))
            .expect("the channel is sized for every batch");
        }
        drop(tx);
        Box::new(source)
    }

    /// `sources -> one windowed processor -> one [`ChannelSink`]`, in
    /// topological order. The processor node declares the `window` block, which
    /// is what makes it a windowed node to the runner.
    fn windowed_service(
        sources: Vec<Box<dyn Source>>,
    ) -> (BuiltService, mpsc::Receiver<RecordBatch>) {
        let in_schema = sale_schema();
        let out_schema = total_schema();
        let (sink, rx) = ChannelSink::new(Arc::clone(&out_schema), 4_096);
        let processor = sources.len();
        let mut nodes: Vec<BuiltNode> = sources
            .into_iter()
            .enumerate()
            .map(|(i, source)| BuiltNode {
                id: format!("in{i}"),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some(SALE),
                kind: BuiltNodeKind::Source(source),
                downstream: vec![BuiltEdge {
                    node: processor,
                    branch: None,
                }],
                artifact: None,
                window: None,
                heal_recovered: None,
            })
            .collect();
        nodes.push(BuiltNode {
            id: "win".to_string(),
            name: None,
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(WindowingRuntime {
                    in_schema,
                    out_schema: Arc::clone(&out_schema),
                }),
                kind: "native",
            },
            downstream: vec![BuiltEdge {
                node: processor + 1,
                branch: None,
            }],
            artifact: None,
            window: Some(WindowConfig {
                spec: WindowSpec::Tumbling {
                    size_ms: WINDOW_SIZE_MS,
                    offset_ms: 0,
                },
                time_field: "ts".to_string(),
                key_fields: vec!["sym".to_string()],
                allowed_lateness_ms: WINDOW_LATENESS_MS,
            }),
            heal_recovered: None,
        });
        nodes.push(BuiltNode {
            id: "out".to_string(),
            name: None,
            type_name: "ChannelSink".to_string(),
            component: Some(TOTAL),
            kind: BuiltNodeKind::Sink(Box::new(sink)),
            downstream: Vec::new(),
            artifact: None,
            window: None,
            heal_recovered: None,
        });
        (
            BuiltService {
                workflow_id: "flow".to_string(),
                workflow_name: None,
                nodes,
                registry: Arc::new(Registry::new()),
                inspector: None,
                dlq: None,
            },
            rx,
        )
    }

    /// One emitted window row, comparable and orderable. `sum` is compared as
    /// its exact bit pattern: the assertion is set equality of what the sink
    /// received, not an approximation of it.
    type WindowRow = (i64, String, i64, u64);

    fn window_rows(chunks: &[RecordBatch]) -> Vec<WindowRow> {
        let mut rows = Vec::new();
        for chunk in chunks {
            let window_id = chunk
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("window_id column");
            let sym = chunk
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("sym column");
            let count = chunk
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count column");
            let sum = chunk
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("sum column");
            for i in 0..chunk.num_rows() {
                rows.push((
                    window_id.value(i),
                    sym.value(i).to_string(),
                    count.value(i),
                    sum.value(i).to_bits(),
                ));
            }
        }
        rows.sort();
        rows
    }

    /// Drive `service` under `config` until the sink has been quiet for a beat
    /// (or the runner exited on its own, which is what stream mode does at
    /// EOF), then cancel and join. Returns the emitted window rows, sorted.
    ///
    /// The last drain happens after the runner has returned, so a batch the
    /// cancellation path flushed still counts.
    async fn emitted_rows(
        service: BuiltService,
        config: ServiceConfig,
        mut rx: mpsc::Receiver<RecordBatch>,
    ) -> Vec<WindowRow> {
        let cancel = CancellationToken::new();
        let collector_cancel = cancel.clone();
        let runner = run_standalone(service, &config, cancel, None, None);
        let collect = async move {
            let mut chunks: Vec<RecordBatch> = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut quiet = 0u32;
            while Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(150), rx.recv()).await {
                    Ok(Some(chunk)) => {
                        chunks.push(chunk);
                        quiet = 0;
                    }
                    Ok(None) => break,
                    Err(_elapsed) => {
                        quiet += 1;
                        if quiet >= 4 && !chunks.is_empty() {
                            break;
                        }
                    }
                }
            }
            collector_cancel.cancel();
            (chunks, rx)
        };
        let (stats, (mut chunks, mut rx)) = tokio::join!(runner, collect);
        stats.expect("the windowed run succeeds");
        while let Ok(chunk) = rx.try_recv() {
            chunks.push(chunk);
        }
        window_rows(&chunks)
    }

    /// Drain-to-EOF, the shape before adaptive flow control.
    fn unchunked() -> FlowControlConfig {
        FlowControlConfig {
            enabled: Some(false),
            ..FlowControlConfig::default()
        }
    }

    /// A credit far below the size of every source batch here, so it is spent
    /// on the first arrival of every drain: `Continuous` gets one pass per
    /// arrival instead of one per drain. Pinned rather than adaptive so the
    /// grouping is reproducible; the adaptive controller would choose a
    /// different number on every machine, which is why none of these results
    /// may depend on it.
    fn chunked() -> FlowControlConfig {
        FlowControlConfig {
            rows: Some(64),
            ..FlowControlConfig::default()
        }
    }

    /// Assert the two shapes agree, and that they agreed on something worth
    /// agreeing about.
    fn assert_invariant(unchunked_rows: &[WindowRow], chunked_rows: &[WindowRow], what: &str) {
        let windows: std::collections::BTreeSet<i64> = unchunked_rows.iter().map(|r| r.0).collect();
        assert!(
            windows.len() >= 10,
            "{what}: the drain-to-EOF run must close enough windows for the \
             comparison to mean anything, closed {:?}",
            windows
        );
        let duplicated = {
            let mut keys: Vec<(i64, &str)> = chunked_rows
                .iter()
                .map(|(id, sym, _, _)| (*id, sym.as_str()))
                .collect();
            let before = keys.len();
            keys.sort_unstable();
            keys.dedup();
            before != keys.len()
        };
        assert!(
            !duplicated,
            "{what}: a chunked run must fire each window group exactly once, \
             got {chunked_rows:?}"
        );
        assert_eq!(
            unchunked_rows, chunked_rows,
            "{what}: closed-window results must not depend on the source's \
             admission credit"
        );
    }

    /// Fan-in: two sources, each with its own controller, interleaved 10 ms
    /// apart in event time and sharing both grouping keys, so every window
    /// group is a merge of both streams. One drain-to-EOF pass over both
    /// drains against four passes, one per arrival per source, which is where
    /// a per-source credit can skew the merged watermark and where the
    /// lateness budget keeps the skew invisible.
    ///
    /// `RunMode::Stream` has no counterpart: an item there is one arrival from
    /// one source with or without a credit, so the two shapes would be the
    /// same run. `stream_disorder_beyond_the_budget_inside_one_arrival_is_carried_whole`
    /// is what holds that in place.
    #[tokio::test]
    async fn fan_in_window_results_do_not_depend_on_chunk_size() {
        const BATCHES: usize = 4;
        const ROWS: usize = 150;

        let schema = sale_schema();
        let pair = || {
            vec![
                sale_source(&schema, BATCHES, ROWS, 20, 0, 0),
                sale_source(&schema, BATCHES, ROWS, 20, 10, 10_000),
            ]
        };

        let (whole, rx) = windowed_service(pair());
        let unchunked_rows =
            emitted_rows(whole, config_with(RunMode::Continuous, unchunked()), rx).await;

        let (split, rx) = windowed_service(pair());
        let chunked_rows =
            emitted_rows(split, config_with(RunMode::Continuous, chunked()), rx).await;

        assert_invariant(&unchunked_rows, &chunked_rows, "continuous fan-in");
    }

    /// Rows in the single arrival of the beyond-budget cases.
    const DEEP_ROWS: usize = 400;
    /// Event-time step between consecutive rows of that arrival.
    const DEEP_STEP_MS: i64 = 30;
    /// Local index of the row moved backwards, past the 64-row credit several
    /// times over: a run that sliced mid-arrival would meet it holding a
    /// watermark the arrival's own earlier rows advanced.
    const DEEP_INDEX: usize = 300;
    /// How far that row moves back: 8.98 s, three times
    /// [`WINDOW_LATENESS_MS`], landing it at `ts = 10_020`.
    const DEEP_BACK_MS: i64 = 8_980;

    /// A single arrival carrying event-time disorder further back than
    /// `allowed_lateness_ms`.
    ///
    /// Row 300 of one 400-row arrival sits 8.98 s behind its neighbours, so
    /// only the pass boundary decides its fate. Classified against the
    /// watermark the arrival *started* from it is on time and its window is
    /// still open; classified against a watermark that the arrival's own later
    /// rows already advanced it is beyond the budget and dropped.
    ///
    /// Nothing an operator can configure fixes that, because the boundary
    /// would come from a throughput measurement rather than from the data. So
    /// a source on a path to a windowed node is never sliced mid-arrival:
    /// the credit bounds how much is pulled, and one arrival stays one pass.
    /// This case fails the moment mid-arrival slicing comes back: the group
    /// `(window 0, "a")` loses a row and a count.
    #[tokio::test]
    async fn continuous_disorder_beyond_the_budget_inside_one_arrival_is_carried_whole() {
        let schema = sale_schema();
        let arrival = || {
            vec![late_row_source(
                &schema,
                1,
                DEEP_ROWS,
                DEEP_STEP_MS,
                DEEP_INDEX,
                DEEP_BACK_MS,
            )]
        };

        let (whole, rx) = windowed_service(arrival());
        let unchunked_rows =
            emitted_rows(whole, config_with(RunMode::Continuous, unchunked()), rx).await;

        let (split, rx) = windowed_service(arrival());
        let chunked_rows =
            emitted_rows(split, config_with(RunMode::Continuous, chunked()), rx).await;

        assert_invariant(
            &unchunked_rows,
            &chunked_rows,
            "continuous disorder beyond the budget",
        );
    }

    /// The stream runner's form of the same case: one arriving batch is one
    /// item there too, so the row moved back inside it survives.
    #[tokio::test]
    async fn stream_disorder_beyond_the_budget_inside_one_arrival_is_carried_whole() {
        let schema = sale_schema();
        let arrival = || {
            vec![late_row_source(
                &schema,
                1,
                DEEP_ROWS,
                DEEP_STEP_MS,
                DEEP_INDEX,
                DEEP_BACK_MS,
            )]
        };

        let (whole, rx) = windowed_service(arrival());
        let unchunked_rows =
            emitted_rows(whole, config_with(RunMode::Stream, unchunked()), rx).await;

        let (split, rx) = windowed_service(arrival());
        let chunked_rows = emitted_rows(split, config_with(RunMode::Stream, chunked()), rx).await;

        assert_invariant(
            &unchunked_rows,
            &chunked_rows,
            "stream disorder beyond the budget",
        );
    }

    /// Disorder *inside* the lateness budget, across the one boundary the
    /// credit still moves.
    ///
    /// Six 200-row arrivals, each carrying one row 2.5 s behind its own
    /// ascending position, which puts it 1.49 s behind the previous arrival's
    /// leading edge and inside the 3 s budget. The drain-to-EOF shape merges
    /// all six arrivals into one pass and never classifies that row as late.
    /// The chunked shape spends its 64-row credit on the first arrival and so
    /// gives each arrival its own pass, classifying every moved row against a
    /// watermark already past it. Agreeing is the lateness budget doing its
    /// job: at `WINDOW_LATENESS_MS = 1000` these two disagree, so the
    /// invariance here is exercised rather than assumed.
    ///
    /// `RunMode::Stream` has no counterpart, for the reason
    /// [`fan_in_window_results_do_not_depend_on_chunk_size`] gives.
    #[tokio::test]
    async fn continuous_window_results_survive_disorder_inside_the_lateness_budget() {
        const BATCHES: usize = 6;
        const ROWS: usize = 200;
        const STEP_MS: i64 = 10;
        const LATE_OFFSET: usize = 100;
        const BACK_MS: i64 = 2_500;

        let schema = sale_schema();
        let source = || {
            vec![late_row_source(
                &schema,
                BATCHES,
                ROWS,
                STEP_MS,
                LATE_OFFSET,
                BACK_MS,
            )]
        };

        let (whole, rx) = windowed_service(source());
        let unchunked_rows =
            emitted_rows(whole, config_with(RunMode::Continuous, unchunked()), rx).await;

        let (split, rx) = windowed_service(source());
        let chunked_rows =
            emitted_rows(split, config_with(RunMode::Continuous, chunked()), rx).await;

        assert_invariant(
            &unchunked_rows,
            &chunked_rows,
            "continuous disorder inside the budget",
        );
    }

    /// Across-arrival disorder that exceeds `allowed_lateness_ms`: the
    /// documented limit of the invariance rule, not a defect in it. The rule
    /// in `crates/saci-service/src/service/windowing.rs` only promises
    /// agreement "while event-time disorder across arrivals ... stay[s]
    /// inside `allowed_lateness_ms`"; push the disorder past that budget and
    /// the two shapes are *supposed* to disagree, because the credit's
    /// ordinary job of deciding how many arrivals share a pass is exactly
    /// what produces the difference. This case exists so that boundary
    /// cannot silently move: nothing here should ever fail by these two
    /// shapes agreeing.
    ///
    /// Six 200-row arrivals, ascending within each, except every arrival
    /// after the first carries one row at local index 100 pulled back
    /// `BACK_MS` = 6.01 s: `6_010 - 10 * 101 = 5_000` ms behind the
    /// *previous* arrival's own last (unmoved) row, 2 s past
    /// [`WINDOW_LATENESS_MS`] = 3 s, where the sibling case above lands the
    /// same arithmetic 1.49 s *inside* it.
    ///
    /// `unchunked()` drains all six arrivals into one pass, so every one of
    /// those rows is classified against the watermark that pass *started*
    /// from (unset, so nothing in it is ever late) and is kept. `chunked()`
    /// spends its 64-row credit on the first arrival, so each arrival gets
    /// its own pass, and the moved row of the second arrival (global index
    /// 300, `ts = 6_990`) is classified against the watermark the first
    /// arrival's own last row already left at `11_990`:
    /// `11_990 - 3_000 = 8_990 > 6_990`, so it is dropped before it ever
    /// joins a group. Window `13` (`[6_500, 7_000)`) sits below every
    /// arrival's ascending range, so that row is its only possible member:
    /// the group `(window_id: 13, sym: "a")` is `count: 1, sum: 300.0` in
    /// the unchunked output and does not exist at all in the chunked one.
    #[tokio::test]
    async fn continuous_disorder_beyond_the_budget_across_arrivals_diverges_under_chunking() {
        const BATCHES: usize = 6;
        const ROWS: usize = 200;
        const STEP_MS: i64 = 10;
        const LATE_OFFSET: usize = 100;
        const BACK_MS: i64 = 6_010;

        let schema = sale_schema();
        let source = || {
            vec![late_row_source(
                &schema,
                BATCHES,
                ROWS,
                STEP_MS,
                LATE_OFFSET,
                BACK_MS,
            )]
        };

        let (whole, rx) = windowed_service(source());
        let unchunked_rows =
            emitted_rows(whole, config_with(RunMode::Continuous, unchunked()), rx).await;

        let (split, rx) = windowed_service(source());
        let chunked_rows =
            emitted_rows(split, config_with(RunMode::Continuous, chunked()), rx).await;

        assert_ne!(
            unchunked_rows, chunked_rows,
            "disorder beyond the lateness budget is the documented limit of \
             the invariance rule: the two shapes are allowed to disagree here"
        );

        const LOST_WINDOW: i64 = 13;
        const LOST_SYM: &str = "a";
        match unchunked_rows
            .iter()
            .find(|(window_id, sym, _, _)| *window_id == LOST_WINDOW && sym.as_str() == LOST_SYM)
        {
            Some((_, _, count, sum_bits)) => assert_eq!(
                (*count, *sum_bits),
                (1, 300.0f64.to_bits()),
                "the unchunked run's only possible contributor to window \
                 {LOST_WINDOW} is the one row moved back across the arrival \
                 boundary: count and sum must be exactly that row's"
            ),
            None => panic!(
                "expected window {LOST_WINDOW} group {LOST_SYM:?} in the \
                 unchunked output, got {unchunked_rows:?}"
            ),
        }
        assert!(
            !chunked_rows
                .iter()
                .any(|(window_id, sym, _, _)| *window_id == LOST_WINDOW && sym.as_str() == LOST_SYM),
            "the chunked run must drop window {LOST_WINDOW} group {LOST_SYM:?} \
             entirely: the credit split its pass at the arrival boundary, so \
             the only row that could have populated it was classified \
             against a watermark the previous arrival had already advanced \
             past, got {chunked_rows:?}"
        );
    }
}
