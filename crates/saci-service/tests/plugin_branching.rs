//! Conditional fan-out routing through a real native plugin: the
//! `saci-plugin-smoketest` shared library returning a `RouteDecision` from its
//! `smoketest.route` config key, driven through the real `run_standalone`
//! runner, asserting branch-selected delivery.
//!
//! ```bash
//! cargo build -p saci-plugin-smoketest
//! cargo test --test plugin_branching -p saci-service --features plugin,service
//! ```

#![cfg(all(feature = "plugin", feature = "service"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use saci_connector_channel::{ChannelSink, ChannelSource};
use saci_service::component::Component;
use saci_service::plugin::NativePluginRuntime;
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use saci_service::service::config::{
    HttpConfig, NodeConfig, ObservabilityConfig, RunMode, ServiceConfig, ServiceMode,
    StandaloneConfig, WorkflowSpec,
};
use saci_service::service::registry::Registry;
use saci_service::service::standalone::run_standalone;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Host-side mirror of the `Counter` component declared in
/// `crates/saci-plugin-smoketest/src/lib.rs`, matching
/// `plugin_roundtrip.rs`'s mirror.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Counter {
    id: i64,
    seen: i64,
}

impl Component for Counter {
    fn name() -> &'static str {
        "Counter"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("seen", DataType::Int64, false),
        ]))
    }
}

/// Locate the built cdylib, exactly as `plugin_roundtrip.rs` does.
fn smoketest_plugin_path() -> PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    let profile_dir = exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("profile directory above deps/");

    profile_dir.join(format!(
        "{}saci_plugin_smoketest{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

fn load_runtime(config: HashMap<String, String>) -> NativePluginRuntime {
    let path = smoketest_plugin_path();
    assert!(
        path.exists(),
        "smoketest plugin not found at {}; run `cargo build -p saci-plugin-smoketest` first",
        path.display()
    );

    NativePluginRuntime::open(&path, config).expect("NativePluginRuntime::open")
}

fn config() -> ServiceConfig {
    ServiceConfig {
        heal: Default::default(),
        flow_control: Default::default(),
        node: NodeConfig {
            id: 1,
            name: None,
            data_dir: PathBuf::from("/tmp/saci-plugin-branching-test"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::OneShot,
            },
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

/// The branching workflow: source `in` → plugin `router` (with `config`
/// injected) → sinks `out_a` (branch `a`) and `out_b` (branch `b`), in
/// topological order.
fn branching_built(
    config_map: HashMap<String, String>,
) -> (
    BuiltService,
    mpsc::Sender<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
) {
    let (tx, source) = ChannelSource::new(Counter::schema(), 8);
    let (sink_a, rx_a) = ChannelSink::new(Counter::schema(), 8);
    let (sink_b, rx_b) = ChannelSink::new(Counter::schema(), 8);
    let nodes = vec![
        BuiltNode {
            id: "in".to_string(),
            name: None,
            type_name: "ChannelSource".to_string(),
            component: Some("Counter"),
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
            type_name: "plugin".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(load_runtime(config_map)),
                kind: "plugin",
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
            component: Some("Counter"),
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
            component: Some("Counter"),
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

#[tokio::test(flavor = "current_thread")]
async fn plugin_routing_processor_delivers_to_the_selected_branch() {
    let (built, tx, mut rx_a, mut rx_b) = branching_built(HashMap::from([(
        "smoketest.route".to_string(),
        "a".to_string(),
    )]));
    let batch = RecordBatch::try_new(
        Counter::schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![0, 0, 0])),
        ],
    )
    .expect("three-row batch");
    tx.send(batch).await.expect("send batch");
    drop(tx);

    run_standalone(built, &config(), CancellationToken::new(), None, None)
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
