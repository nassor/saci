// Windowed aggregation benchmarks
//
// Run through the harness:
//
//   cargo xtask bench windows
//
// One component of a timestamp, a key and nine f64 columns, 1M rows over 64
// keys, summed per key under each window shape. Each iteration times one
// `WindowedSystem::run` over the whole dataset.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use saci::System;
use saci::dataset::Dataset;
use saci::window::WindowSpec;
use saci::windows::{WindowFunction, WindowedSystem};

const ROWS: usize = 1_000_000;
const KEYS: usize = 64;
/// Milliseconds between consecutive rows: 1M rows span about 28 hours.
const STEP_MS: i64 = 100;

fn schema() -> Arc<Schema> {
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ];
    fields.extend((0..8).map(|i| Field::new(format!("pad{i}"), DataType::Float64, false)));
    Arc::new(Schema::new(fields))
}

fn dataset() -> Dataset {
    let schema = schema();
    let ts: Int64Array = (0..ROWS as i64).map(|i| i * STEP_MS).collect();
    let keys: Vec<String> = (0..KEYS).map(|k| format!("k{k:02}")).collect();
    let key: StringArray = (0..ROWS).map(|i| Some(keys[i % KEYS].as_str())).collect();
    let value: Float64Array = (0..ROWS).map(|i| i as f64).collect();
    let mut columns: Vec<arrow_array::ArrayRef> =
        vec![Arc::new(ts), Arc::new(key), Arc::new(value)];
    for p in 0..8 {
        columns.push(Arc::new(
            (0..ROWS).map(|i| (i + p) as f64).collect::<Float64Array>(),
        ));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();

    let mut data = Dataset::new();
    data.register_schema("Event", schema).unwrap();
    data.append_record_batch("Event", batch).unwrap();
    data
}

fn bench_shape(c: &mut Criterion, group: &str, spec: WindowSpec) {
    let mut data = dataset();
    let system = WindowedSystem::new_named("Event", "ts", spec, WindowFunction::sum("value"))
        .unwrap()
        .key_by(["key"]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut g = c.benchmark_group(group);
    g.sample_size(10);
    g.bench_function("run_1m_rows_64_keys", |b| {
        b.iter(|| runtime.block_on(system.run(&mut data)).unwrap());
    });
    g.finish();
}

fn bench_windows(c: &mut Criterion) {
    bench_shape(c, "tumbling_sum", WindowSpec::tumbling(60_000));
    bench_shape(c, "sliding_sum", WindowSpec::sliding(60_000, 15_000));
    bench_shape(c, "session_sum", WindowSpec::session(30_000));
}

criterion_group!(benches, bench_windows);
criterion_main!(benches);
