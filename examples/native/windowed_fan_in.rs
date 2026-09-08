//! Windowed fan-in over two sources: Beam-style windowing in service mode.
//!
//! Two [`ChannelSource`]s feed the same component of one windowed processor
//! node. The runner merges both streams into a single dataset before the
//! call: a processor receives the rows of every one of its inbound nodes.
//! The processor holds the windowing logic: a native [`Pipeline`] whose
//! [`WindowedSystem`] aggregates the merged trades into 30-second tumbling
//! windows. The host tracks the node's event-time watermark from the
//! `window` block declared in the processor node's config, exposes it to the
//! pipeline as a `WindowWatermark` resource, and records it as the
//! `saci_window_watermark_seconds` series the dashboard reads.
//!
//! ```bash
//! cargo run -p saci-service --example windowed_fan_in --features service,windows,connector-channel
//! ```

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use saci_connector_channel::{ChannelSink, ChannelSource};
use tokio_util::sync::CancellationToken;

use saci_service::SaciError;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::pipeline::Pipeline;
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use saci_service::service::config::{
    HttpConfig, NodeConfig, ObservabilityConfig, RunMode, ServiceConfig, ServiceMode,
    StandaloneConfig, WorkflowSpec,
};
use saci_service::service::standalone::run_standalone;
use saci_service::system::{System, SystemMeta};
use saci_service::windows::{
    ReduceAggregate, WindowFunction, WindowResults, WindowSpec, WindowWatermark,
    WindowedSystemBuilder,
};

/// A trade event: one price at one instant, in milliseconds since the epoch.
#[derive(serde::Serialize, serde::Deserialize)]
struct Trade {
    timestamp_ms: i64,
    price: f64,
}

impl Component for Trade {
    fn name() -> &'static str {
        "Trade"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]))
    }
}

/// One aggregate row per (window, key) group, the processor node's output.
#[derive(serde::Serialize, serde::Deserialize)]
struct WindowTotal {
    window_id: i64,
    key_hash: i64,
    total: f64,
}

impl Component for WindowTotal {
    fn name() -> &'static str {
        "WindowTotal"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("window_id", DataType::Int64, false),
            Field::new("key_hash", DataType::Int64, false),
            Field::new("total", DataType::Float64, false),
        ]))
    }
}

/// Reads the [`WindowResults`] the [`WindowedSystem`] left on the batch
/// dataset, publishes them as `WindowTotal` rows for the sink, and prints the
/// host-tracked watermark.
struct ReportSystem;

#[async_trait]
impl System for ReportSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("report")
            .read_resource::<WindowResults>()
            .read_resource::<WindowWatermark>()
            .write_component("WindowTotal")
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let results = data
            .get_resource::<WindowResults>()
            .ok_or_else(|| SaciError::generic("WindowResults resource not found"))?;

        let mut rows = Vec::new();
        for batch in &results.batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let wid = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| SaciError::generic("window_id column missing"))?;
            let kh = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| SaciError::generic("key_hash column missing"))?;
            let total = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| SaciError::generic("total column missing"))?;
            for row in 0..batch.num_rows() {
                rows.push(WindowTotal {
                    window_id: wid.value(row),
                    key_hash: kh.value(row),
                    total: total.value(row),
                });
            }
        }
        data.append::<WindowTotal>(&rows)?;

        println!("[report]  {} window group(s) emitted", rows.len());
        for row in &rows {
            let start = row.window_id * 30;
            println!(
                "          window [{start:>3}s, {:>3}s): sum = {:.2}",
                start + 30,
                row.total
            );
        }

        if let Some(watermark) = data.get_resource::<WindowWatermark>() {
            println!(
                "[report]  host watermark: {} ms ({:.1} s)",
                watermark.as_ms(),
                watermark.as_seconds()
            );
        }
        Ok(())
    }
}

/// The windowed processor node's pipeline: aggregate merged trades into
/// 30-second tumbling windows and publish the totals.
fn build_windowed_pipeline() -> Pipeline {
    let windowed = WindowedSystemBuilder::new()
        .source("Trade", "timestamp_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 30_000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "price",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(5_000)
        .build()
        .expect("windowed system builds");

    let mut pipeline = Pipeline::new("windowed-fan-in");
    pipeline
        .data_mut()
        .register_component::<Trade>()
        .expect("register Trade");
    pipeline
        .data_mut()
        .register_component::<WindowTotal>()
        .expect("register WindowTotal");
    pipeline.add_system(windowed);
    pipeline.add_system(ReportSystem);
    pipeline
}

fn trade_batch(trades: &[(i64, f64)]) -> RecordBatch {
    let ts: ArrayRef = Arc::new(Int64Array::from(
        trades.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
    ));
    let price: ArrayRef = Arc::new(Float64Array::from(
        trades.iter().map(|(_, p)| *p).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(Trade::schema(), vec![ts, price]).expect("trade batch")
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
    let trade_schema = Trade::schema();

    // Two independent streams feeding the same component of one processor:
    // the fan-in merge is the whole point of the example.
    let (tx_a, source_a) = ChannelSource::new(trade_schema.clone(), 16);
    let (tx_b, source_b) = ChannelSource::new(trade_schema.clone(), 16);
    let (sink, mut rx) = ChannelSink::new(WindowTotal::schema(), 16);

    // Stream A: window 0 only. Stream B: window 0 and window 1.
    tx_a.send(trade_batch(&[(5_000, 10.0), (12_000, 20.0)]))
        .await
        .expect("send A");
    tx_b.send(trade_batch(&[(18_000, 30.0), (35_000, 40.0)]))
        .await
        .expect("send B");
    drop(tx_a);
    drop(tx_b);

    // The workflow graph, built by hand so the example can hold the channel
    // senders and the sink receiver. The shape is exactly what a config file
    // with two `source` nodes, one `wasm` node carrying a `window` block, and
    // one `sink` node would produce.
    let nodes = vec![
        BuiltNode {
            id: "trades_a".to_string(),
            name: Some("Trades A".to_string()),
            type_name: "ChannelSource".to_string(),
            component: Some("Trade"),
            kind: BuiltNodeKind::Source(Box::new(source_a)),
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
            id: "trades_b".to_string(),
            name: Some("Trades B".to_string()),
            type_name: "ChannelSource".to_string(),
            component: Some("Trade"),
            kind: BuiltNodeKind::Source(Box::new(source_b)),
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
            id: "windowed".to_string(),
            name: Some("Windowed aggregate".to_string()),
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(build_windowed_pipeline()),
                kind: "native",
            },
            downstream: vec![BuiltEdge {
                node: 3,
                branch: None,
            }],
            artifact: None,
            #[cfg(feature = "windows")]
            window: Some(saci_service::service::config::WindowConfig {
                spec: WindowSpec::Tumbling {
                    size_ms: 30_000,
                    offset_ms: 0,
                },
                time_field: "timestamp_ms".to_string(),
                key_fields: Vec::new(),
                allowed_lateness_ms: 5_000,
            }),
            heal_recovered: None,
        },
        BuiltNode {
            id: "totals".to_string(),
            name: Some("Window totals".to_string()),
            type_name: "ChannelSink".to_string(),
            component: Some("WindowTotal"),
            kind: BuiltNodeKind::Sink(Box::new(sink)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
    ];

    let config = ServiceConfig {
        heal: Default::default(),
        flow_control: Default::default(),
        node: NodeConfig {
            id: 1,
            name: Some("demo".to_string()),
            data_dir: std::path::PathBuf::from("/tmp/saci-windowed-fan-in"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::OneShot,
            },
        },
        workflows: vec![WorkflowSpec {
            id: "windowed-fan-in".to_string(),
            name: Some("Two streams, one windowed processor".to_string()),
            transformers: Vec::new(),
            sources: Vec::new(),
            plugin: Vec::new(),
            wasm: Vec::new(),
            sinks: Vec::new(),
            links: Vec::new(),
            dlq: None,
        }],
        store: None,
        http: HttpConfig::default(),
        observability: ObservabilityConfig::default(),
        variables: std::collections::HashMap::new(),
    };

    let built = BuiltService {
        workflow_id: "windowed-fan-in".to_string(),
        workflow_name: Some("Two streams, one windowed processor".to_string()),
        nodes,
        registry: std::sync::Arc::new(saci_service::service::registry::Registry::new()),
        inspector: None,
        dlq: None,
    };

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None).await?;
    println!();
    println!(
        "iterations: {} · rows in: {} · sink batches: {}",
        stats.iterations, stats.rows_processed, stats.sink_batches_written
    );

    // One row per closed window: [0s,30s) sums 10+20+30 = 60, [30s,60s) sums 40.
    let mut received = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        let wid = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("window_id");
        let total = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("total");
        for row in 0..batch.num_rows() {
            received.push((wid.value(row), total.value(row)));
        }
    }
    assert_eq!(received.len(), 2, "two windows, two totals: {received:?}");
    assert!(
        (received[0].1 - 60.0).abs() < 1e-9,
        "window 0 sum: {:?}",
        received
    );
    assert!(
        (received[1].1 - 40.0).abs() < 1e-9,
        "window 1 sum: {:?}",
        received
    );
    println!("assertions passed: two window totals from two merged streams");
    Ok(())
}
