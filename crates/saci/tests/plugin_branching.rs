//! Conditional fan-out routing through a real native plugin: the
//! `saci-plugin-smoketest` shared library returning a `RouteDecision` from its
//! `smoketest.route` config key, driven through the real `BuiltWorkflow::run`
//! runner, asserting branch-selected delivery.
//!
//! ```bash
//! cargo build -p saci-plugin-smoketest
//! cargo test --test plugin_branching -p saci --features server,plugin-host,service
//! ```

#![cfg(all(
    feature = "connector-channel",
    feature = "plugin-host",
    feature = "service"
))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use saci::component::Component;
use saci::connector::channel::{ChannelSink, ChannelSource};
use saci::plugin_host::NativePluginRuntime;
use saci::service::BuiltWorkflow;
use saci::service::RunMode;
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
    let name = format!(
        "{}saci_plugin_smoketest{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    exe.ancestors()
        .map(|dir| dir.join(&name))
        .find(|path| path.exists())
        .unwrap_or_else(|| {
            let profile_dir = exe
                .parent()
                .and_then(|deps| deps.parent())
                .expect("profile directory above the test binary");
            profile_dir.join(&name)
        })
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

/// The branching workflow: source `in` → plugin `router` (with `config`
/// injected) → sinks `out_a` (branch `a`) and `out_b` (branch `b`), in
/// topological order.
fn branching_built(
    config_map: HashMap<String, String>,
) -> (
    BuiltWorkflow,
    mpsc::Sender<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
) {
    let (source, tx) = ChannelSource::new(Counter::schema(), 8);
    let (sink_a, rx_a) = ChannelSink::new(Counter::schema(), 8);
    let (sink_b, rx_b) = ChannelSink::new(Counter::schema(), 8);
    let mut built = BuiltWorkflow::builder("w")
        .source("in", "Counter", source)
        .processor("router", load_runtime(config_map))
        .sink("out_a", "Counter", sink_a)
        .sink("out_b", "Counter", sink_b)
        .link("in", "router")
        .link_branch("router", "out_a", "a")
        .link_branch("router", "out_b", "b")
        .run_mode(RunMode::Once)
        .build()
        .expect("build workflow");
    built.nodes[0].connector = "channel".to_string();
    built.nodes[1].connector = "plugin".to_string();
    built.nodes[2].connector = "channel".to_string();
    built.nodes[3].connector = "channel".to_string();
    (built, tx, rx_a, rx_b)
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

    built
        .run(CancellationToken::new())
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
