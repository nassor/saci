//! Per-item latency for stream mode, timed end-to-end from the producer.
//!
//! The native half drives a one-system `Pipeline` through
//! [`service::stream::run_stream`] over a `ChannelSource`/`ChannelSink` pair,
//! timing each single-row batch out and back. The WASM half (`--features
//! wasm`) times `WasmPipelineRuntime::run_on_with_state` on a one-row dataset,
//! which isolates the host/processor boundary: store creation, instantiation from
//! the pre-linked `InstancePre`, IPC in, IPC out.
//!
//! ```text
//! cargo run --release -p saci-service --features service,wasm --example stream_latency
//! ```
//!
//! The WASM half needs the smoketest component:
//!
//! ```text
//! cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use saci_connector_channel::{ChannelSink, ChannelSource};
use saci_core::system::{SystemMeta, WriteSet, system_fn};
use saci_core::{Pipeline, SaciError};
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use saci_service::service::flow::FlowPlan;
use saci_service::service::registry::Registry;
use saci_service::service::stream::run_stream;

const COMP: &str = "Tick";
const NATIVE_ITEMS: usize = 10_000;

fn tick_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn one_row(schema: Arc<Schema>, v: i64) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
}

/// `p`-th percentile of an already-sorted slice, `p` in 0.0..=1.0.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn report(label: &str, mut samples: Vec<u64>) {
    samples.sort_unstable();
    let total: u64 = samples.iter().sum();
    println!(
        "{label:<22} n={:<7} mean={:>7.1}µs  p50={:>6}µs  p99={:>7}µs  max={:>7}µs",
        samples.len(),
        total as f64 / samples.len() as f64,
        percentile(&samples, 0.50),
        percentile(&samples, 0.99),
        samples.last().copied().unwrap_or(0),
    );
}

/// One system: double the `v` column in place.
fn doubling_pipeline(schema: Arc<Schema>) -> Pipeline {
    let mut pipeline = Pipeline::new("stream_latency");
    pipeline.data_mut().register_raw_component(COMP, schema);
    pipeline.add_system(system_fn(
        SystemMeta::new("double").read(COMP, "v").write(COMP, "v"),
        |data| {
            let batch = data
                .batch_for(COMP)
                .ok_or_else(|| SaciError::generic("Tick component missing"))?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| SaciError::generic("Tick.v is not Int64"))?;
            let doubled: Int64Array = col.iter().map(|v| v.map(|x| x * 2)).collect();
            data.apply_write_set(WriteSet::new().put(COMP, "v", Arc::new(doubled)))
        },
    ));
    pipeline
}

async fn native_latency() -> Vec<u64> {
    let schema = tick_schema();

    // Buffer 1 on both ends keeps the producer from running ahead, so each
    // sample is one full source to sink traversal.
    let (tx, source) = ChannelSource::new(Arc::clone(&schema), 1);
    let (sink, mut rx) = ChannelSink::new(Arc::clone(&schema), 1);

    let service = BuiltService {
        workflow_id: "stream-latency".to_string(),
        workflow_name: None,
        nodes: vec![
            BuiltNode {
                id: "in".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some(COMP),
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
                id: "double".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(doubling_pipeline(Arc::clone(&schema))),
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
        ],
        registry: Arc::new(Registry::new()),
        inspector: None,
        dlq: None,
    };

    let cancel = CancellationToken::new();
    let cancel_runner = cancel.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_stream(
            service,
            cancel_runner,
            None,
            None,
            &FlowPlan::stream_default(),
        )
        .await
    });

    let samples = local
        .run_until(async move {
            // Warm-up: first item pays plan construction and channel setup.
            for i in 0..64i64 {
                tx.send(one_row(Arc::clone(&schema), i)).await.unwrap();
                rx.recv().await.expect("warm-up item");
            }

            let mut samples = Vec::with_capacity(NATIVE_ITEMS);
            for i in 0..NATIVE_ITEMS as i64 {
                let batch = one_row(Arc::clone(&schema), i);
                let started = Instant::now();
                tx.send(batch).await.unwrap();
                let out = rx.recv().await.expect("sink item");
                samples.push(started.elapsed().as_micros() as u64);

                debug_assert_eq!(
                    out.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    i * 2
                );
            }

            drop(tx); // EOF
            samples
        })
        .await;

    let stats = local
        .run_until(async { handle.await.unwrap().unwrap() })
        .await;
    println!(
        "native runner stats:    items={} errors={} busy={}µs max_item={}µs",
        stats.iterations, stats.iteration_errors, stats.total_busy_micros, stats.max_item_micros
    );

    samples
}

#[cfg(feature = "wasm")]
mod wasm_probe {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;

    use saci_core::runtime::PipelineRuntime;
    use saci_service::component::Component;
    use saci_service::wasm::{WasmEngine, WasmPipelineRuntime};
    use serde::{Deserialize, Serialize};

    const WASM_CALLS: usize = 1_000;

    /// Host-side mirror of the smoketest processor's `Ping` component.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    struct Ping {
        seq: u64,
    }

    impl Component for Ping {
        fn name() -> &'static str {
            "Ping"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "seq",
                DataType::UInt64,
                false,
            )]))
        }
    }

    fn smoketest_wasm_path() -> PathBuf {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root above crates/saci-service");
        workspace_root
            .join("target")
            .join("wasm32-wasip2")
            .join("release")
            .join("saci_processor_smoketest.wasm")
    }

    /// Returns `None` when the smoketest component has not been built.
    pub async fn processor_call_latency() -> Option<Vec<u64>> {
        let path = smoketest_wasm_path();
        if !path.exists() {
            println!(
                "wasm half skipped: {} not found — run \
                 `cargo build --release -p saci-processor-smoketest --target wasm32-wasip2`",
                path.display()
            );
            return None;
        }

        let bytes = std::fs::read(&path).expect("read smoketest wasm");
        let engine = WasmEngine::new().expect("WasmEngine init");
        let runtime =
            WasmPipelineRuntime::from_bytes(engine, "smoketest", &bytes, HashMap::new(), 60)
                .expect("load smoketest component");

        let mut dataset = runtime.template_dataset();
        dataset
            .append::<Ping>(&[Ping { seq: 0 }])
            .expect("seed row");

        // Warm-up: first calls pay JIT code-cache and page-fault costs.
        let mut prior = None;
        for _ in 0..32 {
            prior = runtime
                .run_on_with_state(&mut dataset, prior.as_deref())
                .await
                .expect("warm-up processor call");
        }

        let mut samples = Vec::with_capacity(WASM_CALLS);
        for _ in 0..WASM_CALLS {
            let started = Instant::now();
            prior = runtime
                .run_on_with_state(&mut dataset, prior.as_deref())
                .await
                .expect("processor call");
            samples.push(started.elapsed().as_micros() as u64);
        }

        Some(samples)
    }
}

#[tokio::main]
async fn main() {
    println!("stream mode per-item latency\n");

    let native = native_latency().await;
    report("native (source→sink)", native);

    #[cfg(feature = "wasm")]
    if let Some(samples) = wasm_probe::processor_call_latency().await {
        report("wasm run_on_with_state", samples);
    }

    #[cfg(not(feature = "wasm"))]
    println!("\n(build with --features service,wasm for the processor-boundary numbers)");

    // Keep the epoch ticker from being the last thing holding the runtime.
    tokio::time::sleep(Duration::from_millis(10)).await;
}
