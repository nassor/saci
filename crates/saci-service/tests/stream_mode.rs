//! Stream-mode runner integration tests.
//!
//! Covers the properties the batch loop cannot provide: per-item state carry,
//! clean mid-stream cancellation, live TCP ingest, and the `run_standalone`
//! dispatch that selects this runner from `run_mode kind = "stream"`.

#![cfg(feature = "service")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use saci_connector_channel::{ChannelSink, ChannelSource};
use saci_connector_tcp::TcpIngestSource;
use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_core::runtime::PipelineRuntime;
use saci_core::{Dataset, SaciResult};
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use saci_service::service::config::{
    HttpConfig, NodeConfig, ObservabilityConfig, RunMode, ServiceConfig, ServiceMode,
    StandaloneConfig, WorkflowSpec,
};
use saci_service::service::flow::FlowPlan;
use saci_service::service::registry::Registry;
use saci_service::service::standalone::run_standalone;
use saci_service::service::stream::run_stream;
use saci_transformer_arrow_ipc::ArrowIpcTransformer;

const COMP: &str = "values";

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn make_batch(schema: Arc<Schema>, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
}

type SeenPriors = Arc<Mutex<Vec<Option<Vec<u8>>>>>;

struct CounterRuntime {
    schema: Arc<Schema>,
    seen_priors: SeenPriors,
    calls: Arc<AtomicUsize>,
}

impl CounterRuntime {
    fn new(schema: Arc<Schema>) -> (Self, SeenPriors) {
        let seen: SeenPriors = Arc::new(Mutex::new(Vec::new()));
        let rt = Self {
            schema,
            seen_priors: Arc::clone(&seen),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        (rt, seen)
    }

    fn decode(blob: Option<&[u8]>) -> u64 {
        match blob {
            Some(b) if b.len() == 8 => u64::from_le_bytes(b.try_into().expect("8 bytes")),
            _ => 0,
        }
    }
}

#[async_trait(?Send)]
impl PipelineRuntime for CounterRuntime {
    fn name(&self) -> &str {
        "counter"
    }

    async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
        self.run_on_with_state(data, None).await.map(|_| ())
    }

    async fn run_on_with_state(
        &self,
        _data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<Option<Vec<u8>>> {
        self.seen_priors
            .lock()
            .unwrap()
            .push(prior.map(<[u8]>::to_vec));
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some((Self::decode(prior) + 1).to_le_bytes().to_vec()))
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

/// Assemble a [`BuiltService`] with `sources` first, one `CounterRuntime`
/// processor next, then `sinks`, matching the topological order every runner
/// requires. The processor links every source to itself and itself to every
/// sink.
fn built_service(
    sources: Vec<BuiltNode>,
    sinks: Vec<BuiltNode>,
    schema: Arc<Schema>,
) -> (BuiltService, SeenPriors) {
    let (runtime, seen) = CounterRuntime::new(schema);
    let source_count = sources.len();
    let mut nodes = sources;
    let processor_idx = nodes.len();
    nodes.push(BuiltNode {
        id: "counter".to_string(),
        name: None,
        type_name: "native".to_string(),
        component: None,
        kind: BuiltNodeKind::Processor {
            runtime: Box::new(runtime),
            kind: "native",
        },
        downstream: Vec::new(),
        artifact: None,
        #[cfg(feature = "windows")]
        window: None,
        heal_recovered: None,
    });
    for node in &mut nodes[..source_count] {
        node.downstream.push(BuiltEdge {
            node: processor_idx,
            branch: None,
        });
    }
    let sink_start = nodes.len();
    nodes.extend(sinks);
    nodes[processor_idx].downstream = (sink_start..nodes.len())
        .map(|node| BuiltEdge { node, branch: None })
        .collect();
    (
        BuiltService {
            workflow_id: "test".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(Registry::new()),
            inspector: None,
            dlq: None,
        },
        seen,
    )
}

/// Every call gets a distinct id: several tests build more than one source or
/// sink node in the same service.
fn next_id(prefix: &str) -> String {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    format!("{prefix}-{}", COUNTER.fetch_add(1, AtomicOrdering::SeqCst))
}

fn channel_source(schema: Arc<Schema>, buffer: usize) -> (mpsc::Sender<RecordBatch>, BuiltNode) {
    let (tx, src) = ChannelSource::new(schema, buffer);
    (
        tx,
        BuiltNode {
            id: next_id("test_source"),
            name: None,
            type_name: "ChannelSource".to_string(),
            component: Some(COMP),
            kind: BuiltNodeKind::Source(Box::new(src)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
    )
}

fn channel_sink(schema: Arc<Schema>, buffer: usize) -> (BuiltNode, mpsc::Receiver<RecordBatch>) {
    let (sink, rx) = ChannelSink::new(schema, buffer);
    (
        BuiltNode {
            id: next_id("test_sink"),
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
        rx,
    )
}

/// Stream mode chains `run_on_with_state` output into the next item's `prior`.
/// The batch loop calls `run_on` and drops processor state.
#[tokio::test]
async fn stream_carries_runtime_state_across_items() {
    let schema = test_schema();
    let (tx, source) = channel_source(Arc::clone(&schema), 8);
    let (sink, mut rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    for i in 0..5i64 {
        tx.send(make_batch(Arc::clone(&schema), &[i]))
            .await
            .unwrap();
    }
    drop(tx); // EOF

    let stats = run_stream(
        service,
        CancellationToken::new(),
        None,
        None,
        &FlowPlan::stream_default(),
    )
    .await
    .unwrap();

    assert_eq!(stats.iterations, 5, "one iteration per item");
    assert_eq!(stats.rows_processed, 5);
    assert_eq!(stats.iteration_errors, 0);
    assert_eq!(stats.sink_batches_written, 5);
    assert!(
        stats.max_item_micros > 0,
        "per-item timing must be recorded"
    );
    assert!(stats.total_busy_micros >= stats.max_item_micros);

    let priors = seen.lock().unwrap();
    let decoded: Vec<u64> = priors
        .iter()
        .map(|p| CounterRuntime::decode(p.as_deref()))
        .collect();
    assert_eq!(
        decoded,
        vec![0, 1, 2, 3, 4],
        "each item must see the previous item's checkpoint"
    );

    let mut sink_rows = 0usize;
    let mut sink_batches = 0usize;
    while let Ok(batch) = rx.try_recv() {
        sink_rows += batch.num_rows();
        sink_batches += 1;
    }
    assert_eq!(sink_batches, 5, "one sink write per item");
    assert_eq!(sink_rows, 5);
}

/// Cancelling while the source is idle exits cleanly, keeping the stats for the
/// items already processed.
#[tokio::test]
async fn stream_cancels_cleanly_mid_stream() {
    let schema = test_schema();
    let (tx, source) = channel_source(Arc::clone(&schema), 8);
    let (sink, _rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, _seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_stream(
            service,
            cancel_clone,
            None,
            None,
            &FlowPlan::stream_default(),
        )
        .await
    });

    let feed_schema = Arc::clone(&schema);
    let feeder = local.spawn_local(async move {
        for i in 0..3i64 {
            let _ = tx.send(make_batch(Arc::clone(&feed_schema), &[i])).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Hold the sender open so the source never signals EOF.
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    local
        .run_until(async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            cancel.cancel();
        })
        .await;

    let stats = local
        .run_until(async { handle.await.unwrap().unwrap() })
        .await;
    feeder.abort();

    assert_eq!(stats.iterations, 3, "all fed items processed before cancel");
    assert_eq!(stats.iteration_errors, 0);
}

#[tokio::test]
async fn stream_requires_at_least_one_source() {
    let schema = test_schema();
    let (service, _seen) = built_service(vec![], vec![], Arc::clone(&schema));

    let err = run_stream(
        service,
        CancellationToken::new(),
        None,
        None,
        &FlowPlan::stream_default(),
    )
    .await
    .expect_err("a source-less stream must be rejected");
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(
        err.to_string()
            .contains("stream mode requires at least one source"),
        "got: {err}"
    );
}

/// Two sources feeding one processor: each item is one batch from one source,
/// so the processor's rows accumulate across items in round-robin order.
#[tokio::test]
async fn stream_rotates_round_robin_across_two_sources() {
    let schema = test_schema();
    // Every batch is queued before `run_stream` starts draining, so each
    // channel has to hold that source's whole share: two batches for `a`, one
    // for `b`. A capacity that cannot hold them deadlocks the second `send`.
    let (tx_a, mut a) = channel_source(Arc::clone(&schema), 2);
    let (tx_b, mut b) = channel_source(Arc::clone(&schema), 1);
    a.downstream.push(BuiltEdge {
        node: 2,
        branch: None,
    });
    b.downstream.push(BuiltEdge {
        node: 2,
        branch: None,
    });

    // The processor records how many rows its dataset held on each call.
    let rows_per_item: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    struct RowTaker {
        seen: Arc<Mutex<Vec<u64>>>,
    }
    #[async_trait(?Send)]
    impl PipelineRuntime for RowTaker {
        fn name(&self) -> &str {
            "row-taker"
        }
        async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
            self.seen.lock().unwrap().push(data.rows() as u64);
            Ok(())
        }
        fn declared_components(&self) -> Vec<&str> {
            vec![COMP]
        }
        fn template_dataset(&self) -> Dataset {
            let mut dataset = Dataset::new();
            dataset.register_raw_component(COMP, test_schema());
            dataset
        }
    }

    let nodes = vec![
        a,
        b,
        BuiltNode {
            id: "p".to_string(),
            name: None,
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(RowTaker {
                    seen: Arc::clone(&rows_per_item),
                }),
                kind: "native",
            },
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
    ];
    let built = BuiltService {
        workflow_id: "test".to_string(),
        workflow_name: None,
        nodes,
        registry: Arc::new(Registry::new()),
        inspector: None,
        dlq: None,
    };

    tx_a.send(make_batch(Arc::clone(&schema), &[1, 2]))
        .await
        .unwrap();
    tx_b.send(make_batch(Arc::clone(&schema), &[3, 4, 5]))
        .await
        .unwrap();
    tx_a.send(make_batch(Arc::clone(&schema), &[6]))
        .await
        .unwrap();
    drop(tx_a);
    drop(tx_b);

    run_stream(
        built,
        CancellationToken::new(),
        None,
        None,
        &FlowPlan::stream_default(),
    )
    .await
    .expect("run succeeds");

    // Round-robin order: a's 2 rows, then b's 3 rows, then a's 1 row. Each
    // item is a fresh workflow pass, so the dataset holds only that item's
    // rows when the processor runs.
    assert_eq!(
        rows_per_item.lock().unwrap().as_slice(),
        &[2, 3, 1],
        "one batch per source per item, in rotation"
    );
}

/// One frame = u32 big-endian length + one Arrow IPC stream payload.
/// A live source that opens its "subscription" on its first poll, then blocks
/// for data: the `NatsSource` lifecycle the stream runner must prime. The
/// flag is what a publisher would observe before publishing: a core-NATS
/// message published while the flag is still down is dropped by the server.
struct SubscribeOnFirstPollSource {
    inner: ChannelSource,
    subscribed: Arc<AtomicBool>,
}

#[async_trait]
impl Source for SubscribeOnFirstPollSource {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.subscribed.store(true, Ordering::SeqCst);
        self.inner.next_batch().await
    }
}

/// The windowing e2e's startup race, at the runner level: two live sources
/// feeding one processor (the two core-NATS fan-in subjects), with the first
/// message published on the second subject before the first subject ever
/// receives one. Core NATS is at-most-once: a message published with no
/// subscriber is dropped, so the second source's subscription must open while
/// the runner is parked on the first source with no data. The runner primes
/// every source's first poll at start; assert the second source subscribes
/// while the first is idle, then that the message published to it is
/// delivered rather than lost.
#[tokio::test]
async fn stream_primes_every_source_before_blocking_on_any() {
    let schema = test_schema();
    let (tx_a, rx_a) = mpsc::channel(4);
    let (tx_b, rx_b) = mpsc::channel(4);
    let subscribed_a = Arc::new(AtomicBool::new(false));
    let subscribed_b = Arc::new(AtomicBool::new(false));

    let source_a = BuiltNode {
        id: next_id("fanin_a"),
        name: None,
        type_name: "SubscribeOnFirstPollSource".to_string(),
        component: Some(COMP),
        kind: BuiltNodeKind::Source(Box::new(SubscribeOnFirstPollSource {
            inner: ChannelSource::from_receiver(Arc::clone(&schema), rx_a),
            subscribed: Arc::clone(&subscribed_a),
        })),
        downstream: Vec::new(),
        artifact: None,
        #[cfg(feature = "windows")]
        window: None,
        heal_recovered: None,
    };
    let source_b = BuiltNode {
        id: next_id("fanin_b"),
        name: None,
        type_name: "SubscribeOnFirstPollSource".to_string(),
        component: Some(COMP),
        kind: BuiltNodeKind::Source(Box::new(SubscribeOnFirstPollSource {
            inner: ChannelSource::from_receiver(Arc::clone(&schema), rx_b),
            subscribed: Arc::clone(&subscribed_b),
        })),
        downstream: Vec::new(),
        artifact: None,
        #[cfg(feature = "windows")]
        window: None,
        heal_recovered: None,
    };
    let (sink, mut rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, _seen) = built_service(vec![source_a, source_b], vec![sink], Arc::clone(&schema));

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_stream(
            service,
            cancel_clone,
            None,
            None,
            &FlowPlan::stream_default(),
        )
        .await
    });

    let stats = local
        .run_until(async move {
            // The core of the race: B must open its subscription while A's
            // first poll is parked with no data on it. Without the prime the
            // runner never gets here: it blocks on A forever, so B stays
            // unsubscribed and core NATS drops B's first message.
            tokio::time::timeout(Duration::from_secs(5), async {
                while !subscribed_b.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect(
                "source B must open its subscription while source A is parked: \
                 the runner primes every source's first poll before blocking on any",
            );

            // B's subscription is open now: publish to B before A ever
            // receives a message, the ordering core NATS drops when no
            // subscriber is listening, and assert the message arrives.
            tx_b.send(make_batch(Arc::clone(&schema), &[100]))
                .await
                .unwrap();
            tx_a.send(make_batch(Arc::clone(&schema), &[1]))
                .await
                .unwrap();

            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let mut got_a = false;
            let mut got_b = false;
            while !(got_a && got_b) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "sink must receive both fan-in messages, got a={got_a} b={got_b}"
                );
                let batch = recv_timeout(&mut rx).await;
                for value in batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 column")
                {
                    match value.expect("int64 value") {
                        1 => got_a = true,
                        100 => got_b = true,
                        other => panic!("unexpected value {other}"),
                    }
                }
            }

            // EOF both sources so the runner exits cleanly with stats.
            drop(tx_a);
            drop(tx_b);
            handle.await.unwrap().unwrap()
        })
        .await;

    assert_eq!(stats.rows_processed, 2, "no fan-in message may be lost");
    assert_eq!(stats.iteration_errors, 0);
    assert!(
        subscribed_a.load(Ordering::SeqCst),
        "the runner must have polled source A too"
    );
}

fn frame(schema: &Arc<Schema>, values: &[i64]) -> Vec<u8> {
    let batch = make_batch(Arc::clone(schema), values);
    let mut payload: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let mut out = Vec::with_capacity(payload.len() + 4);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

async fn recv_timeout(rx: &mut mpsc::Receiver<RecordBatch>) -> RecordBatch {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("sink write timed out")
        .expect("sink channel closed")
}

/// Frames arriving over TCP drive the stream loop; an oversized frame kills
/// only its own connection, leaving the loop live for the next producer.
#[tokio::test]
async fn stream_ingests_tcp_frames_and_survives_a_bad_connection() {
    let schema = test_schema();
    let tcp = TcpIngestSource::new(
        "127.0.0.1:0",
        Arc::clone(&schema),
        8,
        4096,
        Arc::new(ArrowIpcTransformer::new()),
    )
    .unwrap();
    let addr = tcp.local_addr();

    let source = BuiltNode {
        id: "tcp_in".to_string(),
        name: None,
        type_name: "TcpIngestSource".to_string(),
        component: Some(COMP),
        kind: BuiltNodeKind::Source(Box::new(tcp)),
        downstream: Vec::new(),
        artifact: None,
        #[cfg(feature = "windows")]
        window: None,
        heal_recovered: None,
    };
    let (sink, mut rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, _seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_stream(
            service,
            cancel_clone,
            None,
            None,
            &FlowPlan::stream_default(),
        )
        .await
    });

    let stats = local
        .run_until(async move {
            // Three good frames on one connection.
            let mut conn = TcpStream::connect(addr).await.unwrap();
            for i in 0..3i64 {
                conn.write_all(&frame(&schema, &[i])).await.unwrap();
            }
            conn.flush().await.unwrap();

            for i in 0..3i64 {
                let batch = recv_timeout(&mut rx).await;
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(col.value(0), i, "frames must arrive in order");
            }
            drop(conn);

            // An oversized frame: the header alone exceeds max_frame_bytes, so
            // the reader closes that connection without consuming the payload.
            let mut bad = TcpStream::connect(addr).await.unwrap();
            bad.write_all(&99_999u32.to_be_bytes()).await.unwrap();
            bad.flush().await.unwrap();
            drop(bad);

            // The listener is still up: a fresh connection still delivers.
            let mut conn = TcpStream::connect(addr).await.unwrap();
            conn.write_all(&frame(&schema, &[42])).await.unwrap();
            conn.flush().await.unwrap();
            let batch = recv_timeout(&mut rx).await;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(col.value(0), 42, "loop survives a bad connection");
            drop(conn);

            cancel.cancel();
            handle.await.unwrap().unwrap()
        })
        .await;

    assert_eq!(stats.iterations, 4, "3 good frames + 1 after the bad one");
    assert_eq!(stats.iteration_errors, 0);
}

fn stream_config() -> ServiceConfig {
    ServiceConfig {
        heal: Default::default(),
        flow_control: Default::default(),
        node: NodeConfig {
            id: 1,
            name: None,
            data_dir: std::path::PathBuf::from("/tmp/saci-stream-test"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::Stream,
            },
        },
        workflows: vec![WorkflowSpec {
            id: "test".to_string(),
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

/// `run_mode kind = "stream"` must route `run_standalone` into the stream runner:
/// per-item invocations with state carry, not the batch loop.
#[tokio::test]
async fn run_standalone_dispatches_stream_mode() {
    let schema = test_schema();
    let (tx, source) = channel_source(Arc::clone(&schema), 8);
    let (sink, mut rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    for i in 0..4i64 {
        tx.send(make_batch(Arc::clone(&schema), &[i]))
            .await
            .unwrap();
    }
    drop(tx); // EOF

    let config = stream_config();
    let stats = run_standalone(service, &config, CancellationToken::new(), None, None)
        .await
        .unwrap();

    assert_eq!(stats.iterations, 4, "one iteration per item, not one batch");
    assert_eq!(stats.iteration_errors, 0);
    assert!(
        stats.max_item_micros > 0,
        "stream-only counters must be populated, proving the stream runner ran"
    );

    let decoded: Vec<u64> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|p| CounterRuntime::decode(p.as_deref()))
        .collect();
    assert_eq!(decoded, vec![0, 1, 2, 3], "state carried across items");

    let mut sink_batches = 0usize;
    while rx.try_recv().is_ok() {
        sink_batches += 1;
    }
    assert_eq!(sink_batches, 4);
}

/// A runtime that reports a fixed routing decision without touching the data.
struct BranchRuntime {
    routes: Option<Vec<String>>,
}

#[async_trait(?Send)]
impl PipelineRuntime for BranchRuntime {
    fn name(&self) -> &str {
        "branch"
    }

    async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
        self.run_on_with_state_and_routes(data, None)
            .await
            .map(|_| ())
    }

    async fn run_on_with_state_and_routes(
        &self,
        _data: &mut Dataset,
        _prior: Option<&[u8]>,
    ) -> SaciResult<saci_core::runtime::RuntimeOutput> {
        Ok(saci_core::runtime::RuntimeOutput {
            state: None,
            routes: self.routes.clone(),
        })
    }

    fn declared_components(&self) -> Vec<&str> {
        vec![COMP]
    }

    fn template_dataset(&self) -> Dataset {
        let mut dataset = Dataset::new();
        dataset.register_raw_component(COMP, test_schema());
        dataset
    }
}

#[tokio::test]
async fn stream_routing_processor_delivers_to_the_selected_branch() {
    let schema = test_schema();
    let (tx, mut source) = channel_source(Arc::clone(&schema), 8);
    source.downstream.push(BuiltEdge {
        node: 1,
        branch: None,
    });
    let (sink_a, mut rx_a) = channel_sink(Arc::clone(&schema), 16);
    let (sink_b, mut rx_b) = channel_sink(Arc::clone(&schema), 16);

    let nodes = vec![
        source,
        BuiltNode {
            id: "router".to_string(),
            name: None,
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(BranchRuntime {
                    routes: Some(vec!["a".to_string()]),
                }),
                kind: "native",
            },
            downstream: vec![
                BuiltEdge {
                    node: 2,
                    branch: Some("a".to_string()),
                },
                BuiltEdge {
                    node: 3,
                    branch: Some("b".to_string()),
                },
            ],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
        sink_a,
        sink_b,
    ];
    let built = BuiltService {
        workflow_id: "test".to_string(),
        workflow_name: None,
        nodes,
        registry: Arc::new(Registry::new()),
        inspector: None,
        dlq: None,
    };

    tx.send(make_batch(Arc::clone(&schema), &[1]))
        .await
        .unwrap();
    drop(tx); // EOF

    run_stream(
        built,
        CancellationToken::new(),
        None,
        None,
        &FlowPlan::stream_default(),
    )
    .await
    .expect("run succeeds");

    assert_eq!(
        rx_a.recv()
            .await
            .expect("sink a received the item")
            .num_rows(),
        1
    );
    assert!(
        rx_b.try_recv().is_err(),
        "sink b must not receive an item routed to branch a"
    );
}

/// A windowed processor in stream mode: each item advances the node's
/// watermark from that item's timestamps, monotonically across items.
#[cfg(feature = "windows")]
#[tokio::test]
async fn stream_windowed_processor_tracks_watermark_across_items() {
    use saci_core::windows::{WindowSpec, WindowWatermark};

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let (tx, mut source) = channel_source(Arc::clone(&schema), 8);
    source.downstream.push(BuiltEdge {
        node: 1,
        branch: None,
    });

    let watermarks: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

    struct StreamWatermarkReader {
        seen: Arc<Mutex<Vec<i64>>>,
        schema: Arc<Schema>,
    }
    #[async_trait(?Send)]
    impl PipelineRuntime for StreamWatermarkReader {
        fn name(&self) -> &str {
            "stream-watermark-reader"
        }
        async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
            if let Some(watermark) = data.get_resource::<WindowWatermark>() {
                self.seen.lock().unwrap().push(watermark.as_ms());
            }
            Ok(())
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

    let nodes = vec![
        source,
        BuiltNode {
            id: "p".to_string(),
            name: None,
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(StreamWatermarkReader {
                    seen: Arc::clone(&watermarks),
                    schema: Arc::clone(&schema),
                }),
                kind: "native",
            },
            downstream: Vec::new(),
            artifact: None,
            window: Some(saci_service::service::config::WindowConfig {
                spec: WindowSpec::Tumbling {
                    size_ms: 1_000,
                    offset_ms: 0,
                },
                time_field: "timestamp_ms".to_string(),
                key_fields: Vec::new(),
                allowed_lateness_ms: 0,
            }),
            heal_recovered: None,
        },
    ];
    let built = BuiltService {
        workflow_id: "test".to_string(),
        workflow_name: None,
        nodes,
        registry: Arc::new(Registry::new()),
        inspector: None,
        dlq: None,
    };

    let ts_batch = |ts: i64| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(Int64Array::from(vec![ts])),
            ],
        )
        .unwrap()
    };
    tx.send(ts_batch(1_000)).await.unwrap();
    tx.send(ts_batch(500)).await.unwrap(); // older: must not move the watermark
    tx.send(ts_batch(2_500)).await.unwrap();
    drop(tx); // EOF

    run_stream(
        built,
        CancellationToken::new(),
        None,
        None,
        &FlowPlan::stream_default(),
    )
    .await
    .expect("run succeeds");

    let seen = watermarks.lock().unwrap();
    assert_eq!(
        seen.as_slice(),
        &[1_000, 1_000, 2_500],
        "the watermark must advance monotonically per item"
    );
}
