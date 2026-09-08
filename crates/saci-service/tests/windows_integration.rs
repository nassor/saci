//! Windowed aggregation (tumbling, keyed, sliding, session) and streaming semantics
//! (watermarks, late data, side-output, ProcessWindowFn).
//!
//! ```text
//! cargo test --test windows_integration --features windows
//! ```

#![cfg(feature = "windows")]

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::error::SaciError;
use saci_service::pipeline::Pipeline;
use saci_service::system::System;
use saci_service::windows::{
    WindowResults, WindowSpec,
    function::{ProcessWindowFn, ReduceAggregate, WindowContext, WindowFunction},
    system::WindowedSystemBuilder,
};

// ---------------------------------------------------------------------------
// Shared test components
// ---------------------------------------------------------------------------

/// Non-keyed source component with a timestamp and a value column.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct SourceComponent {
    ts_ms: i64,
    value: f64,
}

impl Component for SourceComponent {
    fn name() -> &'static str {
        "SourceComponent"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("ts_ms", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }
}

/// Keyed source component with timestamp, key (String), and value.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct KeyedSource {
    ts_ms: i64,
    key: String,
    value: f64,
}

impl Component for KeyedSource {
    fn name() -> &'static str {
        "KeyedSource"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("ts_ms", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }
}

fn sum_values(results: &WindowResults, col_idx: usize) -> f64 {
    results
        .batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
        })
        .sum()
}

fn all_window_ids(results: &WindowResults) -> Vec<i64> {
    results
        .batches
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// 10 rows with timestamps [1000, 1500, 2000, ..., 5500] ms and values [10, 20, 30, ...].
/// Tumbling window of 2000 ms → 3 windows:
///   window 0 (0..1999):   ts 1000, 1500           → values 10, 20         → sum = 30
///   window 1 (2000..3999): ts 2000, 2500, 3000, 3500 → values 30,40,50,60 → sum = 180
///   window 2 (4000..5999): ts 4000, 4500, 5000, 5500 → values 70,80,90,100 → sum = 340
#[tokio::test]
async fn test_tumbling_non_keyed_sum() {
    let rows: Vec<SourceComponent> = (0..10)
        .map(|i| SourceComponent {
            ts_ms: 1000 + i as i64 * 500,
            value: (i + 1) as f64 * 10.0,
        })
        .collect();

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();

    assert_eq!(
        results.batches.len(),
        3,
        "expected 3 window groups, got {}",
        results.batches.len()
    );

    let mut pairs: Vec<(i64, f64)> = results
        .batches
        .iter()
        .map(|b| {
            let win_id = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let sum_val = b
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            (win_id, sum_val)
        })
        .collect();
    pairs.sort_by_key(|&(w, _)| w);

    assert_eq!(pairs[0].0, 0, "first window ID should be 0");
    assert!(
        (pairs[0].1 - 30.0).abs() < 1e-9,
        "window 0 sum: expected 30.0, got {}",
        pairs[0].1
    );

    assert_eq!(pairs[1].0, 1, "second window ID should be 1");
    assert!(
        (pairs[1].1 - 180.0).abs() < 1e-9,
        "window 1 sum: expected 180.0, got {}",
        pairs[1].1
    );

    assert_eq!(pairs[2].0, 2, "third window ID should be 2");
    assert!(
        (pairs[2].1 - 340.0).abs() < 1e-9,
        "window 2 sum: expected 340.0, got {}",
        pairs[2].1
    );
}

/// 10 rows, 2 keys (A, B), timestamps interleaved, values 10..100.
/// Tumbling window of 2000 ms.
/// All timestamps [1000..5500] span windows 0, 1, 2.
/// Key A gets rows at ts 1000, 2000, 3000, 4000, 5000 → values 10,30,50,70,90
/// Key B gets rows at ts 1500, 2500, 3500, 4500, 5500 → values 20,40,60,80,100
///
/// Per window per key:
///   window 0 A: ts 1000         → value 10
///   window 0 B: ts 1500         → value 20
///   window 1 A: ts 2000, 3000   → values 30,50 → sum 80
///   window 1 B: ts 2500, 3500   → values 40,60 → sum 100
///   window 2 A: ts 4000, 5000   → values 70,90 → sum 160
///   window 2 B: ts 4500, 5500   → values 80,100 → sum 180
#[tokio::test]
async fn test_tumbling_keyed_sum() {
    let keys = ["A", "B"];
    let rows: Vec<KeyedSource> = (0..10)
        .map(|i| KeyedSource {
            ts_ms: 1000 + i as i64 * 500,
            key: keys[i % 2].to_string(),
            value: (i + 1) as f64 * 10.0,
        })
        .collect();

    let mut pipeline = Dataset::new();
    pipeline.register_component::<KeyedSource>().unwrap();
    pipeline.append::<KeyedSource>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("KeyedSource", "ts_ms")
        .keyed_by(&["key"])
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();

    // 3 windows × 2 keys = 6 groups (one batch per group).
    assert_eq!(
        results.batches.len(),
        6,
        "expected 6 keyed window groups, got {}",
        results.batches.len()
    );

    // Total sum must equal 10+20+...+100 = 550.
    let total: f64 = sum_values(results, 2);
    assert!(
        (total - 550.0).abs() < 1e-9,
        "total sum across all keyed groups: expected 550.0, got {total}"
    );

    // Each window should appear exactly twice (once per key).
    let mut window_counts: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for &wid in &all_window_ids(results) {
        *window_counts.entry(wid).or_default() += 1;
    }
    assert_eq!(window_counts[&0], 2, "window 0 should have 2 key groups");
    assert_eq!(window_counts[&1], 2, "window 1 should have 2 key groups");
    assert_eq!(window_counts[&2], 2, "window 2 should have 2 key groups");
}

/// Runs a windowed system inside a pipeline and checks the result schema, row
/// counts, and per-window sums.
#[tokio::test]
async fn test_windowed_system_in_pipeline() {
    let rows: Vec<SourceComponent> = vec![
        SourceComponent {
            ts_ms: 100,
            value: 10.0,
        },
        SourceComponent {
            ts_ms: 200,
            value: 20.0,
        },
        SourceComponent {
            ts_ms: 1200,
            value: 30.0,
        },
        SourceComponent {
            ts_ms: 1800,
            value: 40.0,
        },
        SourceComponent {
            ts_ms: 2100,
            value: 50.0,
        },
    ];

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 1000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    let mut p = Pipeline::new("test");
    p.data_mut()
        .register_component::<SourceComponent>()
        .unwrap();
    p.data_mut().append::<SourceComponent>(&rows).unwrap();
    p.add_system(sys);
    p.run().await.unwrap();

    let results = p
        .data()
        .get_resource::<WindowResults>()
        .expect("WindowResults should be in pipeline after run");

    // 3 windows: [0..999], [1000..1999], [2000..2999]
    assert_eq!(results.batches.len(), 3, "expected 3 window result batches");

    let first_batch = &results.batches[0];
    assert_eq!(first_batch.schema().fields().len(), 3);
    assert_eq!(first_batch.schema().field(0).name(), "window_id");
    assert_eq!(first_batch.schema().field(1).name(), "key_hash");

    for batch in &results.batches {
        assert_eq!(batch.num_rows(), 1, "each group batch should have 1 row");
    }

    // Total sum = 10+20+30+40+50 = 150.
    let total: f64 = sum_values(results, 2);
    assert!(
        (total - 150.0).abs() < 1e-9,
        "total sum: expected 150.0, got {total}"
    );
}

/// Zero-row pipeline runs without panic and produces empty WindowResults.
#[tokio::test]
async fn test_empty_world_no_panic() {
    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline
        .get_resource::<WindowResults>()
        .expect("WindowResults resource should be inserted even for empty pipeline");

    assert!(
        results.batches.is_empty(),
        "empty pipeline should produce no result batches"
    );
    assert_eq!(results.total_rows(), 0);
}

/// Values 10, 20, 30, 5, 15 in a single tumbling window → min = 5.
#[tokio::test]
async fn test_min_aggregation() {
    let rows = vec![
        SourceComponent {
            ts_ms: 100,
            value: 10.0,
        },
        SourceComponent {
            ts_ms: 200,
            value: 20.0,
        },
        SourceComponent {
            ts_ms: 300,
            value: 30.0,
        },
        SourceComponent {
            ts_ms: 400,
            value: 5.0,
        },
        SourceComponent {
            ts_ms: 500,
            value: 15.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Min,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();
    assert_eq!(results.batches.len(), 1);
    let min_val = results.batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!(
        (min_val - 5.0).abs() < 1e-9,
        "min aggregation: expected 5.0, got {min_val}"
    );
}

/// Values 10, 20, 30, 5, 15 in a single tumbling window → max = 30.
#[tokio::test]
async fn test_max_aggregation() {
    let rows = vec![
        SourceComponent {
            ts_ms: 100,
            value: 10.0,
        },
        SourceComponent {
            ts_ms: 200,
            value: 20.0,
        },
        SourceComponent {
            ts_ms: 300,
            value: 30.0,
        },
        SourceComponent {
            ts_ms: 400,
            value: 5.0,
        },
        SourceComponent {
            ts_ms: 500,
            value: 15.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Max,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();
    assert_eq!(results.batches.len(), 1);
    let max_val = results.batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!(
        (max_val - 30.0).abs() < 1e-9,
        "max aggregation: expected 30.0, got {max_val}"
    );
}

/// 5 rows in one window and 3 rows in another → counts are 5 and 3.
#[tokio::test]
async fn test_count_aggregation() {
    let rows = vec![
        SourceComponent {
            ts_ms: 100,
            value: 1.0,
        },
        SourceComponent {
            ts_ms: 200,
            value: 2.0,
        },
        SourceComponent {
            ts_ms: 300,
            value: 3.0,
        },
        SourceComponent {
            ts_ms: 400,
            value: 4.0,
        },
        SourceComponent {
            ts_ms: 500,
            value: 5.0,
        },
        SourceComponent {
            ts_ms: 2100,
            value: 6.0,
        },
        SourceComponent {
            ts_ms: 2200,
            value: 7.0,
        },
        SourceComponent {
            ts_ms: 2300,
            value: 8.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Count,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();
    assert_eq!(results.batches.len(), 2, "expected 2 window groups");

    let mut pairs: Vec<(i64, f64)> = results
        .batches
        .iter()
        .map(|b| {
            let win_id = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let count = b
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            (win_id, count)
        })
        .collect();
    pairs.sort_by_key(|&(w, _)| w);

    assert!(
        (pairs[0].1 - 5.0).abs() < 1e-9,
        "window 0 count: expected 5.0, got {}",
        pairs[0].1
    );
    assert!(
        (pairs[1].1 - 3.0).abs() < 1e-9,
        "window 1 count: expected 3.0, got {}",
        pairs[1].1
    );
}

/// Values 10, 20, 30 → mean = 20.
#[tokio::test]
async fn test_mean_aggregation() {
    let rows = vec![
        SourceComponent {
            ts_ms: 100,
            value: 10.0,
        },
        SourceComponent {
            ts_ms: 200,
            value: 20.0,
        },
        SourceComponent {
            ts_ms: 300,
            value: 30.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Mean,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();
    assert_eq!(results.batches.len(), 1);
    let mean_val = results.batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!(
        (mean_val - 20.0).abs() < 1e-9,
        "mean aggregation: expected 20.0, got {mean_val}"
    );
}

/// Mean across two windows with different group sizes.
#[tokio::test]
async fn test_mean_aggregation_two_windows() {
    // window 0: values 10, 20 → mean = 15
    // window 1: values 30, 60, 90 → mean = 60
    let rows = vec![
        SourceComponent {
            ts_ms: 100,
            value: 10.0,
        },
        SourceComponent {
            ts_ms: 200,
            value: 20.0,
        },
        SourceComponent {
            ts_ms: 2100,
            value: 30.0,
        },
        SourceComponent {
            ts_ms: 2200,
            value: 60.0,
        },
        SourceComponent {
            ts_ms: 2300,
            value: 90.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Mean,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();
    assert_eq!(results.batches.len(), 2, "expected 2 window groups");

    let mut pairs: Vec<(i64, f64)> = results
        .batches
        .iter()
        .map(|b| {
            let win_id = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let mean = b
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            (win_id, mean)
        })
        .collect();
    pairs.sort_by_key(|&(w, _)| w);

    assert!(
        (pairs[0].1 - 15.0).abs() < 1e-9,
        "window 0 mean: expected 15.0, got {}",
        pairs[0].1
    );
    assert!(
        (pairs[1].1 - 60.0).abs() < 1e-9,
        "window 1 mean: expected 60.0, got {}",
        pairs[1].1
    );
}

/// Tumbling window boundary alignment: offsets, exact boundaries, negative timestamps.
#[test]
fn test_window_assignment_correctness() {
    // 1-second windows (1000 ms), no offset
    assert_eq!(WindowSpec::assign_tumbling(0, 1000, 0), 0);
    assert_eq!(WindowSpec::assign_tumbling(999, 1000, 0), 0);
    assert_eq!(WindowSpec::assign_tumbling(1000, 1000, 0), 1);
    assert_eq!(WindowSpec::assign_tumbling(1999, 1000, 0), 1);
    assert_eq!(WindowSpec::assign_tumbling(2000, 1000, 0), 2);

    // 2-second windows (2000 ms), no offset
    assert_eq!(WindowSpec::assign_tumbling(0, 2000, 0), 0);
    assert_eq!(WindowSpec::assign_tumbling(1999, 2000, 0), 0);
    assert_eq!(WindowSpec::assign_tumbling(2000, 2000, 0), 1);
    assert_eq!(WindowSpec::assign_tumbling(3999, 2000, 0), 1);
    assert_eq!(WindowSpec::assign_tumbling(4000, 2000, 0), 2);

    // 500 ms offset, 1000 ms window: aligned boundaries 500, 1500, 2500
    assert_eq!(WindowSpec::assign_tumbling(400, 1000, 500), -1);
    assert_eq!(WindowSpec::assign_tumbling(500, 1000, 500), 0);
    assert_eq!(WindowSpec::assign_tumbling(1499, 1000, 500), 0);
    assert_eq!(WindowSpec::assign_tumbling(1500, 1000, 500), 1);

    // Negative timestamps (floor division)
    assert_eq!(WindowSpec::assign_tumbling(-1, 1000, 0), -1);
    assert_eq!(WindowSpec::assign_tumbling(-1000, 1000, 0), -1);
    assert_eq!(WindowSpec::assign_tumbling(-1001, 1000, 0), -2);
    assert_eq!(WindowSpec::assign_tumbling(-500, 1000, 0), -1);
}

// ---------------------------------------------------------------------------
// Session window integration tests
// ---------------------------------------------------------------------------

/// Single key, two clear sessions separated by a gap larger than gap_ms.
///
/// Data layout:
///   session 0: ts 100, 200, 300   values 10, 20, 30  → sum = 60
///   (gap: 5000 - 300 = 4700 ms > 1000 ms)
///   session 1: ts 5000, 5100      values 40, 50       → sum = 90
///
/// Expected: 2 result batches, correct sums and session_start_ts/session_end_ts.
#[tokio::test]
async fn test_session_single_key() {
    let rows = vec![
        SourceComponent {
            ts_ms: 100,
            value: 10.0,
        },
        SourceComponent {
            ts_ms: 200,
            value: 20.0,
        },
        SourceComponent {
            ts_ms: 300,
            value: 30.0,
        },
        SourceComponent {
            ts_ms: 5000,
            value: 40.0,
        },
        SourceComponent {
            ts_ms: 5100,
            value: 50.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Session { gap_ms: 1000 })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();
    assert_eq!(
        results.batches.len(),
        2,
        "expected 2 sessions, got {}",
        results.batches.len()
    );

    let mut sessions: Vec<(i64, f64, i64, i64)> = results
        .batches
        .iter()
        .map(|b| {
            let session_id = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let sum_val = b
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            let start_ts = b
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let end_ts = b
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            (session_id, sum_val, start_ts, end_ts)
        })
        .collect();
    sessions.sort_by_key(|&(sid, ..)| sid);

    let (_, sum0, start0, end0) = sessions[0];
    let (_, sum1, start1, end1) = sessions[1];

    assert!(
        (sum0 - 60.0).abs() < 1e-9,
        "session 0 sum: expected 60.0, got {sum0}"
    );
    assert_eq!(start0, 100, "session 0 start_ts");
    assert_eq!(end0, 300, "session 0 end_ts");

    assert!(
        (sum1 - 90.0).abs() < 1e-9,
        "session 1 sum: expected 90.0, got {sum1}"
    );
    assert_eq!(start1, 5000, "session 1 start_ts");
    assert_eq!(end1, 5100, "session 1 end_ts");
}

// ---------------------------------------------------------------------------
// Sliding window integration tests
// ---------------------------------------------------------------------------

/// Non-keyed sliding window: size=2000ms, slide=1000ms → k=2.
///
/// 4 rows at ts 500, 1200, 2100, 3500:
///
/// ts=500  → window ids [0, -1]
/// ts=1200 → window ids [1, 0]
/// ts=2100 → window ids [2, 1]
/// ts=3500 → window ids [3, 2]
///
/// Window -1: {500}   → sum 10
/// Window  0: {500, 1200} → sum 10+20 = 30
/// Window  1: {1200, 2100} → sum 20+30 = 50
/// Window  2: {2100, 3500} → sum 30+40 = 70
/// Window  3: {3500}       → sum 40
///
/// Total across all windows = 200: each value is counted twice, so 2 * 100.
#[tokio::test]
async fn test_sliding_non_keyed_sum() {
    let rows = vec![
        SourceComponent {
            ts_ms: 500,
            value: 10.0,
        },
        SourceComponent {
            ts_ms: 1200,
            value: 20.0,
        },
        SourceComponent {
            ts_ms: 2100,
            value: 30.0,
        },
        SourceComponent {
            ts_ms: 3500,
            value: 40.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Sliding {
            size_ms: 2000,
            slide_ms: 1000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();

    // 5 distinct window groups: -1, 0, 1, 2, 3
    assert_eq!(
        results.batches.len(),
        5,
        "expected 5 window groups, got {}",
        results.batches.len()
    );

    // Total across all groups: each value counted twice (k=2).
    let total: f64 = sum_values(results, 2);
    assert!(
        (total - 200.0).abs() < 1e-9,
        "total sum: expected 200.0 (each value counted twice), got {total}"
    );

    let mut pairs: Vec<(i64, f64)> = results
        .batches
        .iter()
        .map(|b| {
            let wid = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let s = b
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            (wid, s)
        })
        .collect();
    pairs.sort_by_key(|&(w, _)| w);

    let expected: Vec<(i64, f64)> = vec![(-1, 10.0), (0, 30.0), (1, 50.0), (2, 70.0), (3, 40.0)];
    for ((wid, sum), (exp_wid, exp_sum)) in pairs.iter().zip(expected.iter()) {
        assert_eq!(wid, exp_wid, "window_id mismatch");
        assert!(
            (sum - exp_sum).abs() < 1e-9,
            "window {wid} sum: expected {exp_sum}, got {sum}"
        );
    }
}

/// Sliding window driven through a full pipeline run.
#[tokio::test]
async fn test_sliding_in_pipeline() {
    let rows: Vec<SourceComponent> = (0..6)
        .map(|i| SourceComponent {
            ts_ms: i as i64 * 500, // 0, 500, 1000, 1500, 2000, 2500
            value: (i + 1) as f64, // 1, 2, 3, 4, 5, 6
        })
        .collect();

    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Sliding {
            size_ms: 2000,
            slide_ms: 1000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    let mut p = Pipeline::new("test");
    p.data_mut()
        .register_component::<SourceComponent>()
        .unwrap();
    p.data_mut().append::<SourceComponent>(&rows).unwrap();
    p.add_system(sys);
    p.run().await.unwrap();

    let results = p
        .data()
        .get_resource::<WindowResults>()
        .expect("WindowResults should be present after run");

    // k=2, 6 original rows → total expanded rows = 12 → 2 * (1+2+3+4+5+6) = 42
    let total: f64 = sum_values(results, 2);
    assert!(
        (total - 42.0).abs() < 1e-9,
        "scheduler sliding total: expected 42.0, got {total}"
    );

    for batch in &results.batches {
        assert_eq!(batch.num_rows(), 1, "each group batch should have 1 row");
    }
}

/// Amplification limit: k*N > 100_000_000 returns an error and does not panic.
#[tokio::test]
async fn test_sliding_amplification_limit_error() {
    let rows = vec![SourceComponent {
        ts_ms: 0,
        value: 1.0,
    }];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();

    // size=10^10, slide=1 → k=10^10, far exceeds 10^8 limit.
    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Sliding {
            size_ms: 10_000_000_000,
            slide_ms: 1,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    let result = sys.run(&mut pipeline).await;
    assert!(result.is_err(), "expected amplification error");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("amplification"),
        "error message should mention amplification, got: {msg}"
    );
}

/// Two keys with independent sessions.
///
/// Key A events: ts 0, 100 (gap 100 ≤ 500), ts 3000 (gap 2900 > 500) → 2 sessions for A
/// Key B events: ts 50, 150, 250 (gaps ≤ 500) → 1 session for B
///
/// Interleaved original order: A0, B0, A1, B1, B2, A2
/// Expected: 3 result batches (A-s0, A-s1, B-s0).
#[tokio::test]
async fn test_session_multi_key() {
    let rows = vec![
        KeyedSource {
            ts_ms: 0,
            key: "A".to_string(),
            value: 10.0,
        },
        KeyedSource {
            ts_ms: 50,
            key: "B".to_string(),
            value: 1.0,
        },
        KeyedSource {
            ts_ms: 100,
            key: "A".to_string(),
            value: 20.0,
        },
        KeyedSource {
            ts_ms: 150,
            key: "B".to_string(),
            value: 2.0,
        },
        KeyedSource {
            ts_ms: 250,
            key: "B".to_string(),
            value: 3.0,
        },
        KeyedSource {
            ts_ms: 3000,
            key: "A".to_string(),
            value: 30.0,
        },
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<KeyedSource>().unwrap();
    pipeline.append::<KeyedSource>(&rows).unwrap();

    let sys = WindowedSystemBuilder::new()
        .source("KeyedSource", "ts_ms")
        .keyed_by(&["key"])
        .window(WindowSpec::Session { gap_ms: 500 })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();
    assert_eq!(
        results.batches.len(),
        3,
        "expected 3 sessions (2 for A + 1 for B), got {}",
        results.batches.len()
    );

    // Total sum: A(10+20+30) + B(1+2+3) = 60 + 6 = 66.
    let total: f64 = sum_values(results, 2);
    assert!(
        (total - 66.0).abs() < 1e-9,
        "total sum across all sessions: expected 66.0, got {total}"
    );

    let mut sums: Vec<f64> = results
        .batches
        .iter()
        .map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        })
        .collect();
    sums.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Expected group sums: 6.0 (B), 30.0 (A-s0), 30.0 (A-s1)
    assert!(
        (sums[0] - 6.0).abs() < 1e-9,
        "B session sum: expected 6.0, got {}",
        sums[0]
    );
    assert!(
        (sums[1] - 30.0).abs() < 1e-9,
        "A session 0 sum: expected 30.0, got {}",
        sums[1]
    );
    assert!(
        (sums[2] - 30.0).abs() < 1e-9,
        "A session 1 sum: expected 30.0, got {}",
        sums[2]
    );
}

// ===========================================================================
// Streaming semantics: watermarks, late data, side-output
// ===========================================================================

/// Three runs simulating an out-of-order event stream.
///
/// Run 1: on-time data [ts=1000, 2000, 3000] → watermark advances to 3000.
/// Run 2: late-but-acceptable data [ts=2500] with allowed_lateness=1000.
///        watermark=3000, threshold=3000-1000=2000. ts=2500 >= 2000 → accepted.
/// Run 3: beyond-lateness data [ts=1500].
///        watermark=3000, threshold=2000. ts=1500 < 2000 → side-output.
#[tokio::test]
async fn test_watermark_progression_out_of_order() {
    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(1000)
        .build()
        .unwrap();

    // Run 1: on-time rows, watermark advances to 3000.
    {
        let rows = vec![
            SourceComponent {
                ts_ms: 1000,
                value: 10.0,
            },
            SourceComponent {
                ts_ms: 2000,
                value: 20.0,
            },
            SourceComponent {
                ts_ms: 3000,
                value: 30.0,
            },
        ];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.side_output.total_rows(), 0, "run 1: no drops");
        assert!(results.late_batches.is_empty(), "run 1: no late firings");
        assert!(!results.batches.is_empty());
    }

    // Run 2: late-but-acceptable row (ts=2500 with watermark=3000, threshold=2000).
    {
        let rows = vec![SourceComponent {
            ts_ms: 2500,
            value: 99.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.side_output.total_rows(), 0, "run 2: no drops");
        // Window [2000,4000) already fired, so ts=2500 re-fires it late.
        assert_eq!(results.late_batches.len(), 1, "run 2: one late re-firing");
    }

    // Run 3: beyond-lateness row (ts=1500 with watermark=3000, threshold=2000).
    {
        let rows = vec![SourceComponent {
            ts_ms: 1500,
            value: 77.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(
            results.side_output.total_rows(),
            1,
            "run 3: 1 row dropped to side-output"
        );
        assert!(results.batches.is_empty(), "run 3: no on-time firings");
    }
}

/// Allowed-lateness boundary: watermark 5000, allowed_lateness 1000, threshold 4000.
///
/// - ts=4000 sits exactly at the threshold and is accepted (comparison is strict `<`).
/// - ts=3999 is one below the threshold and goes to side-output.
#[tokio::test]
async fn test_allowed_lateness_boundary() {
    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(1000)
        .build()
        .unwrap();

    // First run: advance watermark to 5000.
    {
        let rows = vec![SourceComponent {
            ts_ms: 5000,
            value: 1.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();
    }

    // Second run: ts=4000 (exactly at threshold=5000-1000=4000) → accepted.
    {
        let rows = vec![SourceComponent {
            ts_ms: 4000,
            value: 55.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(
            results.side_output.total_rows(),
            0,
            "ts=4000 at threshold is NOT beyond lateness"
        );
        // Window [4000,6000) already fired, so ts=4000 re-fires it late.
        assert_eq!(results.late_batches.len(), 1);
    }

    // Third run: ts=3999 (one below threshold) → beyond lateness → side-output.
    {
        let rows = vec![SourceComponent {
            ts_ms: 3999,
            value: 42.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(
            results.side_output.total_rows(),
            1,
            "ts=3999 below threshold is beyond lateness → side-output"
        );
        assert!(results.batches.is_empty(), "no on-time batches");
        assert!(results.late_batches.is_empty(), "no late batches");
    }
}

/// Re-firings land in `late_batches`, first firings in `batches`.
///
/// Tumbling 2000 ms window, allowed_lateness 5000.
///
/// Run 1: ts=[1000, 1500], window 0 fires on time with sum 30.
/// Run 2: ts=[1200] is late but within lateness, so window 0 re-fires.
#[tokio::test]
async fn test_window_refiring_on_late_data() {
    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(5000)
        .build()
        .unwrap();

    // Run 1: first firing, on time.
    {
        let rows = vec![
            SourceComponent {
                ts_ms: 1000,
                value: 10.0,
            },
            SourceComponent {
                ts_ms: 1500,
                value: 20.0,
            },
        ];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 1, "one on-time window group");
        assert!(results.late_batches.is_empty(), "no late firings yet");
        assert_eq!(results.side_output.total_rows(), 0);

        let sum_val = results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((sum_val - 30.0).abs() < 1e-9, "first firing sum: {sum_val}");
    }

    // Run 2: window 0 already fired; ts=1200 arrives late and re-fires it.
    {
        let rows = vec![SourceComponent {
            ts_ms: 1200,
            value: 5.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert!(
            results.batches.is_empty(),
            "re-firing goes to late_batches, not batches"
        );
        assert_eq!(results.late_batches.len(), 1, "one late re-firing");
        assert_eq!(results.side_output.total_rows(), 0);

        // The late re-firing contains only the new late row.
        let late_sum = results.late_batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            (late_sum - 5.0).abs() < 1e-9,
            "late refiring sum: {late_sum}"
        );
    }
}

/// Rows past the allowed-lateness budget land in `results.side_output`, not in
/// `results.batches` or `results.late_batches`.
#[tokio::test]
async fn test_side_output_routing() {
    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 1000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(500) // 500ms of tolerance
        .build()
        .unwrap();

    // Advance watermark to 3000.
    {
        let rows = vec![SourceComponent {
            ts_ms: 3000,
            value: 1.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();
    }

    // Send 3 rows with varying lateness:
    //   ts=2600: late but acceptable (3000-500=2500 ≤ 2600 < 3000)
    //   ts=2499: beyond lateness (2499 < 2500) → side-output
    //   ts=2000: beyond lateness → side-output
    {
        let rows = vec![
            SourceComponent {
                ts_ms: 2600,
                value: 10.0,
            }, // late-acceptable
            SourceComponent {
                ts_ms: 2499,
                value: 20.0,
            }, // beyond lateness
            SourceComponent {
                ts_ms: 2000,
                value: 30.0,
            }, // beyond lateness
        ];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();

        assert_eq!(
            results.side_output.total_rows(),
            2,
            "two beyond-lateness rows → side-output"
        );

        // ts=2600 lands in window 2, [2000,3000), which has not fired yet, so it
        // is a first firing and goes to `batches`.
        assert_eq!(
            results.batches.len(),
            1,
            "ts=2600 triggers first-time firing of window 2"
        );
        assert!(results.late_batches.is_empty(), "no late re-firings");

        let side_batch = &results.side_output.batches[0];
        assert_eq!(side_batch.num_rows(), 2);
    }
}

/// A `ProcessWindowFn` that annotates each row with `WindowContext::is_late_firing`
/// and `WindowContext::watermark`.
struct AnnotatingFn;

impl ProcessWindowFn for AnnotatingFn {
    fn process(&self, ctx: &WindowContext, batch: &RecordBatch) -> Result<RecordBatch, SaciError> {
        use arrow_array::BooleanArray;
        use arrow_schema::{DataType, Field as ArrowField};

        // Output schema: original columns + is_late_firing (Boolean) + watermark (Int64).
        let mut fields: Vec<ArrowField> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| ArrowField::new(f.name(), f.data_type().clone(), f.is_nullable()))
            .collect();
        fields.push(ArrowField::new("is_late_firing", DataType::Boolean, false));
        fields.push(ArrowField::new("watermark", DataType::Int64, false));
        let out_schema = Arc::new(arrow_schema::Schema::new(fields));

        let n = batch.num_rows();
        let mut cols: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();
        cols.push(Arc::new(BooleanArray::from(vec![ctx.is_late_firing; n])));
        cols.push(Arc::new(Int64Array::from(vec![ctx.watermark; n])));

        RecordBatch::try_new(out_schema, cols)
            .map_err(|e| SaciError::generic(format!("AnnotatingFn: {e}")))
    }
}

#[tokio::test]
async fn test_process_window_fn_with_context() {
    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 2000,
            offset_ms: 0,
        })
        .function(WindowFunction::Process(Box::new(AnnotatingFn)))
        .allowed_lateness(5000)
        .build()
        .unwrap();

    // Run 1: first firing (on-time).
    {
        let rows = vec![
            SourceComponent {
                ts_ms: 500,
                value: 1.0,
            },
            SourceComponent {
                ts_ms: 800,
                value: 2.0,
            },
        ];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 1, "one on-time window group");
        assert!(results.late_batches.is_empty());

        // The output batch must have `is_late_firing = false`.
        let batch = &results.batches[0];
        let is_late_col = batch
            .column(batch.num_columns() - 2)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        for i in 0..is_late_col.len() {
            assert!(
                !is_late_col.value(i),
                "first firing: is_late_firing should be false"
            );
        }
    }

    // Run 2: ts=600 arrives after the watermark reached 800, so window 0 re-fires.
    {
        let rows = vec![SourceComponent {
            ts_ms: 600,
            value: 3.0,
        }];
        let mut pipeline = Dataset::new();
        pipeline.register_component::<SourceComponent>().unwrap();
        pipeline.append::<SourceComponent>(&rows).unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert!(results.batches.is_empty(), "re-firing goes to late_batches");
        assert_eq!(results.late_batches.len(), 1);

        // The output batch must have `is_late_firing = true`.
        let batch = &results.late_batches[0];
        let is_late_col = batch
            .column(batch.num_columns() - 2)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        for i in 0..is_late_col.len() {
            assert!(
                is_late_col.value(i),
                "late re-firing: is_late_firing should be true"
            );
        }

        // Watermark column should be set to the current watermark.
        let wm_col = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            wm_col.value(0) >= 800,
            "watermark in context should be >= 800"
        );
    }
}

/// When `.allowed_lateness()` is NOT called, watermark tracking is disabled.
/// All data passes through without any side-output or late classifications.
#[tokio::test]
async fn test_no_watermark_all_data_passes_through() {
    let sys = WindowedSystemBuilder::new()
        .source("SourceComponent", "ts_ms")
        .window(WindowSpec::Tumbling {
            size_ms: 1000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "value",
            aggregate: ReduceAggregate::Sum,
        })
        .build()
        .unwrap();

    // Mix of "old" and "new" timestamps in one batch.
    let rows = vec![
        SourceComponent {
            ts_ms: 5000,
            value: 100.0,
        }, // "current"
        SourceComponent {
            ts_ms: 100,
            value: 1.0,
        }, // "very old"
        SourceComponent {
            ts_ms: 200,
            value: 2.0,
        }, // "very old"
    ];

    let mut pipeline = Dataset::new();
    pipeline.register_component::<SourceComponent>().unwrap();
    pipeline.append::<SourceComponent>(&rows).unwrap();
    sys.run(&mut pipeline).await.unwrap();

    let results = pipeline.get_resource::<WindowResults>().unwrap();

    // All 3 rows processed normally: 2 window groups, [0,1000) and [5000,6000).
    assert_eq!(results.batches.len(), 2, "two window groups");
    assert!(
        results.late_batches.is_empty(),
        "no late batches without watermark"
    );
    assert_eq!(
        results.side_output.total_rows(),
        0,
        "no side-output without watermark"
    );
    assert_eq!(results.total_rows(), 2);
}
