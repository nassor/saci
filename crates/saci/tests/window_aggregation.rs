// Tumbling window aggregation over a keyed source. Requires the `windows` feature.

#![cfg(all(feature = "engine", feature = "windows"))]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use saci::SaciError;
use saci::component::Component;
use saci::dataset::Dataset;
use saci::pipeline::Pipeline;
use saci::system::{System, SystemMeta};
use saci::windows::{
    ReduceAggregate, WindowFunction, WindowResults, WindowSpec, WindowedSystemBuilder,
};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SalesEvent {
    timestamp_ms: i64,
    category: String,
    amount: f64,
}

impl Component for SalesEvent {
    fn name() -> &'static str {
        "SalesEvent"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }
}

struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").writes_all::<SalesEvent>()
    }
    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let events = vec![
            // window 0: 0 to 30s
            SalesEvent {
                timestamp_ms: 5_000,
                category: "Electronics".into(),
                amount: 299.99,
            },
            SalesEvent {
                timestamp_ms: 12_000,
                category: "Books".into(),
                amount: 24.95,
            },
            SalesEvent {
                timestamp_ms: 18_000,
                category: "Electronics".into(),
                amount: 149.50,
            },
            SalesEvent {
                timestamp_ms: 25_000,
                category: "Books".into(),
                amount: 39.99,
            },
            // window 1: 30 to 60s
            SalesEvent {
                timestamp_ms: 31_000,
                category: "Electronics".into(),
                amount: 599.00,
            },
            SalesEvent {
                timestamp_ms: 45_000,
                category: "Electronics".into(),
                amount: 89.99,
            },
            SalesEvent {
                timestamp_ms: 50_000,
                category: "Books".into(),
                amount: 14.99,
            },
            // window 2: 60 to 90s
            SalesEvent {
                timestamp_ms: 62_000,
                category: "Electronics".into(),
                amount: 199.00,
            },
            SalesEvent {
                timestamp_ms: 75_000,
                category: "Books".into(),
                amount: 49.99,
            },
        ];
        data.append::<SalesEvent>(&events)?;
        Ok(())
    }
}

#[tokio::test]
async fn test_window_aggregation_produces_results_for_all_windows() {
    let windowed = WindowedSystemBuilder::new()
        .source("SalesEvent", "timestamp_ms")
        .keyed_by(&["category"])
        .window(WindowSpec::Tumbling {
            size_ms: 30_000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "amount",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(5_000)
        .build()
        .unwrap();

    let mut pipeline = Pipeline::new("window_aggregation")
        .component::<SalesEvent>()
        .system(IngestSystem)
        .system(windowed);

    pipeline.run().await.unwrap();

    let results = pipeline
        .data()
        .get_resource::<WindowResults>()
        .expect("WindowResults resource not found after pipeline run");

    // 9 events over 3 windows and 2 categories, so at least 3 groups.
    assert!(
        results.total_rows() >= 3,
        "expected at least 3 window groups"
    );
    // Every timestamp sits inside its own window, so nothing is late.
    assert!(
        results.side_output.is_empty(),
        "no events should be dropped"
    );
}

#[tokio::test]
async fn test_window_aggregation_stage_layout_is_valid() {
    let windowed = WindowedSystemBuilder::new()
        .source("SalesEvent", "timestamp_ms")
        .keyed_by(&["category"])
        .window(WindowSpec::Tumbling {
            size_ms: 30_000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "amount",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    let mut pipeline = Pipeline::new("window_stages")
        .component::<SalesEvent>()
        .system(IngestSystem)
        .system(windowed);

    pipeline.run().await.unwrap();

    let stages = pipeline.plan().unwrap();
    assert!(stages.len() >= 2, "expected at least 2 stages");
}
