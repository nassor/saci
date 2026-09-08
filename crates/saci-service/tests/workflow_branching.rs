//! Conditional fan-out routing end to end: a real `saci-processor-smoketest`
//! WebAssembly component returning a `RouteDecision` from its `route` config
//! key, driven through the real `run_standalone` / `run_stream` runners with
//! real `ChannelSink`s, asserting branch-selected delivery, multi-branch fan,
//! legacy multicast and drop.
//!
//! Its own test binary because installing a meter provider is a
//! process-global one-shot; the same reason `workflow_metrics.rs` lives alone.
//!
//! ```bash
//! cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
//! cargo test --test workflow_branching -p saci-service --features wasm,service
//! ```

#![cfg(all(feature = "wasm", feature = "service"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, UInt64Array};
use saci_connector_channel::{ChannelSink, ChannelSource};
use saci_core::component::Component as _;
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use saci_service::service::config::{
    FlowControlConfig, HttpConfig, NodeConfig, ObservabilityConfig, RunMode, ServiceConfig,
    ServiceMode, StandaloneConfig, WorkflowSpec,
};
use saci_service::service::flow::FlowPlan;
use saci_service::service::registry::Registry;
use saci_service::service::standalone::run_standalone;
use saci_service::service::stream::run_stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[path = "common/smoketest.rs"]
mod smoketest;

use smoketest::{Ping, load_runtime};

fn config(run_mode: RunMode) -> ServiceConfig {
    ServiceConfig {
        heal: Default::default(),
        flow_control: Default::default(),
        node: NodeConfig {
            id: 1,
            name: None,
            data_dir: PathBuf::from("/tmp/saci-branching-test"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig { run_mode },
        },
        workflows: vec![WorkflowSpec {
            id: "w".to_string(),
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

/// The branching workflow: source `in` → processor `router` (the real wasm
/// smoketest with `config` injected) → sinks `out_a` (branch `a`) and `out_b`
/// (branch `b`), in topological order.
fn branching_built(
    config_map: HashMap<String, String>,
) -> (
    BuiltService,
    mpsc::Sender<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
) {
    let (tx, source) = ChannelSource::new(Ping::schema(), 8);
    let (sink_a, rx_a) = ChannelSink::new(Ping::schema(), 8);
    let (sink_b, rx_b) = ChannelSink::new(Ping::schema(), 8);
    let nodes = vec![
        BuiltNode {
            id: "in".to_string(),
            name: None,
            type_name: "ChannelSource".to_string(),
            component: Some("Ping"),
            kind: BuiltNodeKind::Source(Box::new(source)),
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
            id: "router".to_string(),
            name: None,
            type_name: "wasm".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(load_runtime(config_map)),
                kind: "wasm",
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
        BuiltNode {
            id: "out_a".to_string(),
            name: None,
            type_name: "ChannelSink".to_string(),
            component: Some("Ping"),
            kind: BuiltNodeKind::Sink(Box::new(sink_a)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
        BuiltNode {
            id: "out_b".to_string(),
            name: None,
            type_name: "ChannelSink".to_string(),
            component: Some("Ping"),
            kind: BuiltNodeKind::Sink(Box::new(sink_b)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
    ];
    (
        BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(Registry::new()),
            inspector: None,
            dlq: None,
        },
        tx,
        rx_a,
        rx_b,
    )
}

fn three_row_batch() -> RecordBatch {
    RecordBatch::try_new(
        Ping::schema(),
        vec![Arc::new(UInt64Array::from(vec![1, 2, 3]))],
    )
    .expect("three-row batch")
}

#[tokio::test]
async fn routing_processor_delivers_only_to_the_selected_branch() {
    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a".to_string())]));
    tx.send(three_row_batch()).await.expect("send batch");
    drop(tx);

    run_standalone(
        built,
        &config(RunMode::OneShot),
        CancellationToken::new(),
        None,
        None,
    )
    .await
    .expect("run succeeds");

    assert_eq!(
        rx_a.recv()
            .await
            .expect("sink a received the batch")
            .num_rows(),
        3
    );
    assert!(
        rx_b.try_recv().is_err(),
        "sink b must not receive a batch routed to branch a"
    );
}

#[tokio::test]
async fn routing_processor_can_fan_to_several_branches() {
    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a,b".to_string())]));
    tx.send(three_row_batch()).await.expect("send batch");
    drop(tx);

    run_standalone(
        built,
        &config(RunMode::OneShot),
        CancellationToken::new(),
        None,
        None,
    )
    .await
    .expect("run succeeds");

    assert_eq!(rx_a.recv().await.expect("sink a").num_rows(), 3);
    assert_eq!(rx_b.recv().await.expect("sink b").num_rows(), 3);
}

#[tokio::test]
async fn legacy_processor_without_routes_multicasts_to_every_labelled_edge() {
    let (built, tx, mut rx_a, mut rx_b) = branching_built(HashMap::new());
    tx.send(three_row_batch()).await.expect("send batch");
    drop(tx);

    run_standalone(
        built,
        &config(RunMode::OneShot),
        CancellationToken::new(),
        None,
        None,
    )
    .await
    .expect("run succeeds");

    assert_eq!(rx_a.recv().await.expect("sink a").num_rows(), 3);
    assert_eq!(rx_b.recv().await.expect("sink b").num_rows(), 3);
}

#[tokio::test]
async fn stream_routing_processor_delivers_to_the_selected_branch() {
    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a".to_string())]));
    tx.send(three_row_batch()).await.expect("send item");
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
        3
    );
    assert!(
        rx_b.try_recv().is_err(),
        "sink b must not receive an item routed to branch a"
    );
}

/// A batch of `n` distinct, ordered rows: large enough that a small `rows`
/// pin splits it into several chunks.
fn many_row_batch(n: u64) -> RecordBatch {
    RecordBatch::try_new(
        Ping::schema(),
        vec![Arc::new(UInt64Array::from((1..=n).collect::<Vec<u64>>()))],
    )
    .expect("many-row batch")
}

/// A stream-mode [`FlowPlan`] resolving `flow_control` exactly as
/// `run_stream` would from a config declaring it, without going through
/// [`ServiceConfig::load`].
fn stream_flow_plan(flow_control: FlowControlConfig) -> FlowPlan {
    let mut cfg = config(RunMode::Stream);
    cfg.flow_control = flow_control;
    FlowPlan::from_config(&cfg)
}

/// Every batch still buffered on `rx`, in arrival order.
fn collect_batches(rx: &mut mpsc::Receiver<RecordBatch>) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        batches.push(batch);
    }
    batches
}

/// Every row across `batches`, concatenated in arrival order.
fn flatten_rows(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("seq column")
                .values()
                .iter()
                .copied()
        })
        .collect()
}

/// Branch delivery must be invariant to chunk size: the same input pushed
/// through the same branching workflow delivers the same rows, in the same
/// order, to branch `a` whether flow control is disabled entirely (one pass
/// drains the whole 37-row batch) or pinned to `rows 12` (splitting it into
/// four passes of 12, 12, 12 and 1 row, each producing its own
/// [`RouteDecision`]). The smoketest router's decision is a fixed branch list
/// from its `route` config key, not derived from the chunk's content, so it
/// names the same branch on every pass regardless of how the batch was cut.
#[tokio::test]
async fn branch_delivery_is_invariant_to_chunk_size() {
    let input = many_row_batch(37);

    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a".to_string())]));
    tx.send(input.clone()).await.expect("send batch");
    drop(tx);
    let unchunked = stream_flow_plan(FlowControlConfig {
        enabled: Some(false),
        ..Default::default()
    });
    run_stream(built, CancellationToken::new(), None, None, &unchunked)
        .await
        .expect("unchunked run succeeds");
    let unchunked_batches = collect_batches(&mut rx_a);
    assert_eq!(
        unchunked_batches.len(),
        1,
        "flow control disabled must drain the whole batch in one pass"
    );
    assert!(
        rx_b.try_recv().is_err(),
        "branch b must stay empty in the unchunked run"
    );

    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a".to_string())]));
    tx.send(input.clone()).await.expect("send batch");
    drop(tx);
    let chunked = stream_flow_plan(FlowControlConfig {
        rows: Some(12),
        ..Default::default()
    });
    run_stream(built, CancellationToken::new(), None, None, &chunked)
        .await
        .expect("chunked run succeeds");
    let chunked_batches = collect_batches(&mut rx_a);
    assert!(
        rx_b.try_recv().is_err(),
        "branch b must stay empty in the chunked run"
    );

    assert_eq!(
        chunked_batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![12, 12, 12, 1],
        "rows 12 must actually split the 37-row batch into four passes"
    );
    assert_eq!(
        flatten_rows(&unchunked_batches),
        flatten_rows(&chunked_batches),
        "branch a's delivered rows must be identical regardless of chunk size"
    );
}
