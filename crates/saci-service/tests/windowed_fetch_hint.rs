//! The connector fetch-size hint on a path to a windowed node.
//!
//! `Source::request_batch_rows` carries the admission credit to the connector,
//! and `KafkaSource`, `NatsSource` and `PostgresSource` act on it by
//! overwriting their own `batch_size`/`batch_rows`. On a path to a windowed
//! node that would let a throughput measurement choose the arrival boundaries
//! themselves, which is where that node observes event time. So the runners
//! withhold the hint there, exactly as they withhold the slice, and the
//! connector keeps its declared fetch size.
//!
//! Both runners are covered: `run_standalone`'s drain loop, and `run_stream`'s
//! prime phase and rotation.

#![cfg(all(feature = "service", feature = "windows"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_core::runtime::PipelineRuntime;
use saci_core::windows::WindowSpec;
use saci_core::{Dataset, SaciResult};
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use saci_service::service::config::{
    FlowControlConfig, HttpConfig, NodeConfig, ObservabilityConfig, RunMode, ServiceConfig,
    ServiceMode, StandaloneConfig, WindowConfig, WorkflowSpec,
};
use saci_service::service::flow::FlowPlan;
use saci_service::service::registry::Registry;
use saci_service::service::standalone::run_standalone;
use saci_service::service::stream::run_stream;

/// The one component both branches carry, timestamped so the windowed node's
/// tracker has a `time_field` to advance from.
const SALE: &str = "Sale";
/// Rows in every arrival, far above the pinned credit, so a hinted connector
/// would visibly shrink its arrivals.
const ROWS_PER_BATCH: usize = 200;
/// Arrivals per source.
const BATCHES: usize = 3;
/// The pinned credit. Pinned rather than adaptive so the assertion names one
/// number instead of whatever the search happened to reach.
const CREDIT_ROWS: u64 = 64;

fn sale_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn sale_batch(schema: &Arc<Schema>, first: i64, count: usize) -> RecordBatch {
    let values: Vec<i64> = (first..first + count as i64).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(
                values.iter().map(|v| v * 10).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("build a sale batch")
}

/// What one source was told and asked, shared with the test after the runner
/// has taken ownership of the source.
#[derive(Debug, Default)]
struct SourceLog {
    /// Every `request_batch_rows` value, in call order.
    hints: Vec<usize>,
    /// How many times `next_batch` was called.
    polls: usize,
}

/// A source that answers with preloaded batches and then EOF, recording every
/// fetch-size hint it was handed.
struct RecordingSource {
    schema: Arc<Schema>,
    remaining: usize,
    next_first: i64,
    log: Arc<Mutex<SourceLog>>,
}

impl RecordingSource {
    fn new(schema: &Arc<Schema>) -> (Self, Arc<Mutex<SourceLog>>) {
        let log = Arc::new(Mutex::new(SourceLog::default()));
        let source = Self {
            schema: Arc::clone(schema),
            remaining: BATCHES,
            next_first: 0,
            log: Arc::clone(&log),
        };
        (source, log)
    }
}

#[async_trait]
impl Source for RecordingSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.log.lock().expect("log is not poisoned").polls += 1;
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let first = self.next_first;
        self.next_first += ROWS_PER_BATCH as i64;
        Ok(Some(sale_batch(&self.schema, first, ROWS_PER_BATCH)))
    }

    fn request_batch_rows(&mut self, rows: usize) {
        self.log
            .lock()
            .expect("log is not poisoned")
            .hints
            .push(rows);
    }
}

/// A processor that leaves the dataset alone: the run exists to exercise the
/// drain, not to compute anything.
struct PassThrough {
    schema: Arc<Schema>,
}

#[async_trait(?Send)]
impl PipelineRuntime for PassThrough {
    fn name(&self) -> &str {
        "pass-through"
    }

    async fn run_on(&self, _data: &mut Dataset) -> SaciResult<()> {
        Ok(())
    }

    async fn run_on_with_state(
        &self,
        _data: &mut Dataset,
        _prior: Option<&[u8]>,
    ) -> SaciResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn declared_components(&self) -> Vec<&str> {
        vec![SALE]
    }

    fn template_dataset(&self) -> Dataset {
        let mut dataset = Dataset::new();
        dataset.register_raw_component(SALE, Arc::clone(&self.schema));
        dataset
    }
}

fn window_config() -> WindowConfig {
    WindowConfig {
        spec: WindowSpec::Tumbling {
            size_ms: 1_000,
            offset_ms: 0,
        },
        time_field: "ts".to_string(),
        key_fields: Vec::new(),
        allowed_lateness_ms: 1_000,
    }
}

/// Two independent branches in one workflow, in topological order:
///
/// ```text
/// 0 win_in   -> 2 win   (declares a `window` block)
/// 1 plain_in -> 3 plain (declares none)
/// ```
///
/// One workflow rather than two runs, so both sources meet the same runner,
/// the same controller settings and the same schedule: the only difference
/// between them is what their branch ends in.
fn two_branch_service() -> (BuiltService, Arc<Mutex<SourceLog>>, Arc<Mutex<SourceLog>>) {
    let schema = sale_schema();
    let (windowed_source, windowed_log) = RecordingSource::new(&schema);
    let (plain_source, plain_log) = RecordingSource::new(&schema);

    let source_node = |id: &str, source: RecordingSource, target: usize| BuiltNode {
        id: id.to_string(),
        name: None,
        type_name: "RecordingSource".to_string(),
        component: Some(SALE),
        kind: BuiltNodeKind::Source(Box::new(source)),
        downstream: vec![BuiltEdge {
            node: target,
            branch: None,
        }],
        artifact: None,
        window: None,
        heal_recovered: None,
    };
    let processor_node = |id: &str, window: Option<WindowConfig>| BuiltNode {
        id: id.to_string(),
        name: None,
        type_name: "native".to_string(),
        component: None,
        kind: BuiltNodeKind::Processor {
            runtime: Box::new(PassThrough {
                schema: sale_schema(),
            }),
            kind: "native",
        },
        downstream: Vec::new(),
        artifact: None,
        window,
        heal_recovered: None,
    };

    let nodes = vec![
        source_node("win_in", windowed_source, 2),
        source_node("plain_in", plain_source, 3),
        processor_node("win", Some(window_config())),
        processor_node("plain", None),
    ];

    (
        BuiltService {
            workflow_id: "hint".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(Registry::new()),
            inspector: None,
            dlq: None,
        },
        windowed_log,
        plain_log,
    )
}

/// A config in `run_mode` whose every source resolves the top-level
/// `flow_control` block, since a hand-built service declares no source there.
fn pinned_config(run_mode: RunMode) -> ServiceConfig {
    ServiceConfig {
        heal: Default::default(),
        flow_control: FlowControlConfig {
            rows: Some(CREDIT_ROWS),
            ..FlowControlConfig::default()
        },
        node: NodeConfig {
            id: 1,
            name: None,
            data_dir: std::path::PathBuf::from("/tmp/saci-window-hint-test"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig { run_mode },
        },
        workflows: vec![WorkflowSpec {
            id: "hint".to_string(),
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

/// Assert the two halves of the rule against one run's recorded logs.
fn assert_only_the_plain_branch_is_hinted(
    windowed: &Arc<Mutex<SourceLog>>,
    plain: &Arc<Mutex<SourceLog>>,
    what: &str,
) {
    let windowed = windowed.lock().expect("log is not poisoned");
    let plain = plain.lock().expect("log is not poisoned");

    assert!(
        windowed.polls >= BATCHES,
        "{what}: the windowed branch's source must have been drained for the \
         hint assertion to mean anything, polled {} time(s)",
        windowed.polls
    );
    assert!(
        windowed.hints.is_empty(),
        "{what}: a source on a path to a windowed node must receive no \
         fetch-size hint, got {:?}",
        windowed.hints
    );
    assert!(
        !plain.hints.is_empty(),
        "{what}: a source on an ordinary path must still be hinted, so the \
         windowed branch's silence is the rule and not a dead code path"
    );
    assert!(
        plain
            .hints
            .iter()
            .all(|&rows| rows > 0 && rows <= CREDIT_ROWS as usize),
        "{what}: every hint must be the pinned credit or the remainder of it, \
         got {:?}",
        plain.hints
    );
}

/// `run_standalone`'s drain loop.
///
/// The windowed branch's source overshoots its 64-row credit by the tail of a
/// 200-row arrival and is left to choose that size itself; the plain branch's
/// source is asked for the credit before every fresh pull.
#[tokio::test]
async fn a_continuous_drain_hints_only_the_source_off_the_windowed_path() {
    let (service, windowed_log, plain_log) = two_branch_service();
    let config = pinned_config(RunMode::Continuous);
    let cancel = CancellationToken::new();

    // `run_standalone`'s future is `?Send`, so it is joined in place rather
    // than spawned. Every arrival is admitted in the first few backlogged
    // iterations, which skip pacing; the wait covers the drained iteration
    // that follows.
    let stop = {
        let cancel = cancel.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            cancel.cancel();
        }
    };
    let (stats, ()) = tokio::join!(
        run_standalone(service, &config, cancel.clone(), None, None),
        stop
    );
    stats.expect("the continuous run succeeds");

    assert_only_the_plain_branch_is_hinted(&windowed_log, &plain_log, "continuous");
}

/// `run_stream`'s prime phase and rotation, the other two call sites.
///
/// The prime hints every enabled source before the rotation starts, so a
/// windowed-path source that leaked would leak there even if the rotation
/// never polled it again.
#[tokio::test]
async fn a_stream_run_hints_only_the_source_off_the_windowed_path() {
    let (service, windowed_log, plain_log) = two_branch_service();
    let config = pinned_config(RunMode::Stream);
    let flow = FlowPlan::from_config(&config);

    tokio::time::timeout(
        Duration::from_secs(20),
        run_stream(service, CancellationToken::new(), None, None, &flow),
    )
    .await
    .expect("the stream run reaches EOF before the timeout")
    .expect("the stream run succeeds");

    assert_only_the_plain_branch_is_hinted(&windowed_log, &plain_log, "stream");
}
