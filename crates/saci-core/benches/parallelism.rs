// Parallelism benchmarks
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native" cargo bench --bench parallelism
//
// Three benchmarks:
//   1. Cross-system parallelism: 4 ParallelSystems in one stage vs sequential.
//   2. Intra-system slice parallelism: 1 system over 10M rows with slices vs
//      single-threaded.
//   3. Threshold behaviour at 10k / 1M / 10M rows, covering the cost profile
//      ParallelSystem::run_slice keys off.

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use saci_core::SaciError;
use saci_core::component::Component;
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{
    ParallelSystem, SLICE_PARALLEL_THRESHOLD, SliceWriteSet, SystemMeta, WriteSet,
};
use serde::{Deserialize, Serialize};

// Four writable f64 fields, so four systems can write independently.
#[derive(Serialize, Deserialize, Clone)]
struct Record {
    a: f64,
    b: f64,
    c: f64,
    d: f64,
    base: f64, // read-only input
}

impl Component for Record {
    fn name() -> &'static str {
        "Record"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Float64, false),
            Field::new("base", DataType::Float64, false),
        ]))
    }
}

fn world_with_records(n: usize) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_component::<Record>().unwrap();
    let records: Vec<Record> = (0..n)
        .map(|i| Record {
            a: 0.0,
            b: 0.0,
            c: 0.0,
            d: 0.0,
            base: i as f64 + 1.0,
        })
        .collect();
    pipeline.append::<Record>(&records).unwrap();
    pipeline
}

// Stand-in for a non-trivial per-element computation.
fn compute_transform(base: &Float64Array) -> Vec<f64> {
    base.values().iter().map(|&v| (v * v).sqrt()).collect()
}

// Four ParallelSystems, each writing one field independently. The scheduler puts
// all four in one stage and runs them concurrently; the sequential group runs the
// same work one system at a time.

struct WriteFieldA;
struct WriteFieldB;
struct WriteFieldC;
struct WriteFieldD;

macro_rules! impl_field_writer {
    ($type:ty, $field:literal, $method:literal) => {
        #[async_trait]
        impl ParallelSystem for $type {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new($method)
                    .read("Record", "base")
                    .write("Record", $field)
            }
            async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
                let col = pipeline.column::<Record>("base").unwrap().clone();
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                let result: Vec<f64> = arr.values().iter().map(|&v| (v * v).sqrt()).collect();
                let new_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(result));
                Ok(WriteSet::new().put("Record", $field, new_arr))
            }
        }
    };
}

impl_field_writer!(WriteFieldA, "a", "write_a");
impl_field_writer!(WriteFieldB, "b", "write_b");
impl_field_writer!(WriteFieldC, "c", "write_c");
impl_field_writer!(WriteFieldD, "d", "write_d");

fn bench_cross_system_parallelism(c: &mut Criterion) {
    let rows: usize = 1_000_000;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let num_cpus = num_cpus::get();
    println!(
        "\n[bench_cross_system_parallelism] {} rows, {} CPUs",
        rows, num_cpus
    );

    let mut group = c.benchmark_group("cross_system_parallelism");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    group.bench_function("parallel_4_systems", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("bench");
            wl.data = world_with_records(rows);
            wl.add_parallel_system(WriteFieldA);
            wl.add_parallel_system(WriteFieldB);
            wl.add_parallel_system(WriteFieldC);
            wl.add_parallel_system(WriteFieldD);
            rt.block_on(wl.run()).unwrap();
        })
    });

    group.bench_function("sequential_4_systems", |b| {
        use saci_core::system::system_fn;
        b.iter(|| {
            let mut wl = Pipeline::new("bench");
            wl.data = world_with_records(rows);
            // Sequential path: each system takes &mut dataset.
            for field in ["a", "b", "c", "d"] {
                wl.add_system(system_fn(
                    SystemMeta::new(field)
                        .read("Record", "base")
                        .write("Record", field),
                    move |w| {
                        let col = w.column::<Record>("base").unwrap().clone();
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        let result: Vec<f64> =
                            arr.values().iter().map(|&v| (v * v).sqrt()).collect();
                        let new_arr: Arc<dyn arrow_array::Array> =
                            Arc::new(Float64Array::from(result));
                        let batch = w.columns::<Record>().unwrap().clone();
                        let schema = batch.schema();
                        let idx = schema.index_of(field).unwrap();
                        let mut columns: Vec<Arc<dyn arrow_array::Array>> =
                            batch.columns().to_vec();
                        columns[idx] = new_arr;
                        let new_batch = arrow_array::RecordBatch::try_new(schema, columns).unwrap();
                        w.replace_batch::<Record>(new_batch)?;
                        Ok(())
                    },
                ));
            }
            rt.block_on(wl.run()).unwrap();
        })
    });

    group.finish();
}

// One system over 10M rows: slices on (rayon) against slices off (single-threaded
// `run`).

struct SliceSystem;

#[async_trait]
impl ParallelSystem for SliceSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("slice_system")
            .read("Record", "base")
            .write("Record", "a")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        // Single-threaded fallback (used when below threshold or slice opt-out).
        let col = pipeline.column::<Record>("base").unwrap().clone();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        let result = compute_transform(arr);
        let new_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(result));
        Ok(WriteSet::new().put("Record", "a", new_arr))
    }

    fn run_slice(
        &self,
        pipeline: &Dataset,
        rows: std::ops::Range<u32>,
    ) -> Option<Result<SliceWriteSet, SaciError>> {
        let col = pipeline.column::<Record>("base")?;
        let arr = col.as_any().downcast_ref::<Float64Array>()?;
        let start = rows.start as usize;
        let len = (rows.end - rows.start) as usize;
        let slice = arr.slice(start, len);
        let slice_arr = slice.as_any().downcast_ref::<Float64Array>().unwrap();
        let result = compute_transform(slice_arr);
        let new_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(result));
        Some(Ok(SliceWriteSet::new(rows).put("Record", "a", new_arr)))
    }
}

/// Baseline that opts out of slice parallelism.
struct NoSliceSystem;

#[async_trait]
impl ParallelSystem for NoSliceSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("no_slice_system")
            .read("Record", "base")
            .write("Record", "a")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let col = pipeline.column::<Record>("base").unwrap().clone();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        let result = compute_transform(arr);
        let new_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(result));
        Ok(WriteSet::new().put("Record", "a", new_arr))
    }
    // No `run_slice` override, so the default `None` disables slice parallelism.
}

fn bench_slice_parallelism(c: &mut Criterion) {
    let rows: usize = 10_000_000;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let num_cpus = num_cpus::get();
    println!(
        "\n[bench_slice_parallelism] {} rows, {} CPUs",
        rows, num_cpus
    );

    let mut group = c.benchmark_group("slice_parallelism");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(15));

    group.bench_function("with_slices_10M", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("bench");
            wl.data = world_with_records(rows);
            wl.add_parallel_system(SliceSystem);
            rt.block_on(wl.run()).unwrap();
        })
    });

    group.bench_function("no_slices_10M", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("bench");
            wl.data = world_with_records(rows);
            wl.add_parallel_system(NoSliceSystem);
            rt.block_on(wl.run()).unwrap();
        })
    });

    group.finish();
}

// The same SliceSystem at 10k / 1M / 10M rows. 10k sits below the threshold and
// takes the single-threaded path; 1M and 10M take the rayon path.

fn bench_threshold_behaviour(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let num_cpus = num_cpus::get();
    println!(
        "\n[bench_threshold_behaviour] SLICE_PARALLEL_THRESHOLD={}, {} CPUs",
        SLICE_PARALLEL_THRESHOLD, num_cpus
    );

    let mut group = c.benchmark_group("threshold_behaviour");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(8));

    for rows in [10_000usize, 1_000_000, 10_000_000] {
        let label = format!("{}_rows", rows);
        group.bench_function(&label, |b| {
            b.iter(|| {
                let mut wl = Pipeline::new("bench");
                wl.data = world_with_records(rows);
                wl.add_parallel_system(SliceSystem);
                rt.block_on(wl.run()).unwrap();
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_cross_system_parallelism,
    bench_slice_parallelism,
    bench_threshold_behaviour,
);
criterion_main!(benches);
