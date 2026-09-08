// Distributed benchmarks
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native" \
//     cargo bench --bench distributed --features distributed
//
// One benchmark: checkpoint serialization, the Arrow IPC bytes/sec a
// `CheckpointStore` write is bounded by for a wide schema.

#![cfg(feature = "distributed")]

use std::sync::Arc;

use arrow_array::Float64Array;
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use saci_core::component::Component;
use saci_core::dataset::Dataset;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct WideRow {
    f0: f64,
    f1: f64,
    f2: f64,
    f3: f64,
    f4: f64,
    f5: f64,
    f6: f64,
    f7: f64,
    f8: f64,
    f9: f64,
}

impl Component for WideRow {
    fn name() -> &'static str {
        "WideRow"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(
            (0..10)
                .map(|i| Field::new(format!("f{i}"), DataType::Float64, false))
                .collect::<Vec<_>>(),
        ))
    }
}

fn make_arrow_pipeline(n_rows: usize) -> Dataset {
    use arrow_array::RecordBatch;

    let mut pipeline = Dataset::new();
    pipeline.register_component::<WideRow>().unwrap();

    let col: Vec<f64> = (0..n_rows).map(|i| i as f64).collect();
    let arrays: Vec<Arc<dyn arrow_array::Array>> = (0..10)
        .map(|_| Arc::new(Float64Array::from(col.clone())) as Arc<dyn arrow_array::Array>)
        .collect();

    let schema = WideRow::schema();
    let batch = RecordBatch::try_new(schema, arrays).expect("build batch");
    pipeline
        .append_record_batch(WideRow::name(), batch)
        .expect("append");
    pipeline
}

fn bench_checkpoint_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_serialization");

    for &n_rows in &[1_000usize, 100_000, 500_000] {
        let label = format!("{}k_rows_10_cols", n_rows / 1_000);
        let pipeline = make_arrow_pipeline(n_rows);

        group.throughput(criterion::Throughput::Elements(n_rows as u64));
        group.bench_function(&label, |b| {
            b.iter(|| {
                let mut buf = Vec::new();
                pipeline
                    .write_ipc(&mut buf)
                    .expect("write_ipc failed in benchmark");
                std::hint::black_box(buf.len())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_checkpoint_serialize);

criterion_main!(benches);
