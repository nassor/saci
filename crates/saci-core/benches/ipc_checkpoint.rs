// IPC checkpoint round-trip benchmark
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench --bench ipc_checkpoint -- --sample-size 10
//
// Measures the Pipeline Arrow-IPC encode/decode round-trip against a postcard
// row-oriented baseline. This is the checkpoint-recovery path: DistributedRunner
// uses `pipeline.write_ipc` / `Pipeline::read_ipc`.
//
// Data shape: 1M rows × 10 columns of mixed types
//   - 3 × i64
//   - 3 × f64
//   - 2 × String (100 distinct values, avg ~8 chars)
//   - 1 × bool
//   - 1 × Option<f64>   (stored as nullable f64)
//
// Postcard baseline: same data as Vec<MixedRow> (row struct), serialized with
// postcard::to_allocvec and deserialized with postcard::from_bytes.

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use saci_core::dataset::Dataset;
use serde::{Deserialize, Serialize};

// Installs mimalloc so the benchmark uses the same allocator as the shipped
// binary (`saci-service`'s `mimalloc` feature).
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

// The batch is built straight from arrays rather than through serde_arrow, so it
// matches the schema Pipeline registers exactly.

fn mixed_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("i0", DataType::Int64, false),
        Field::new("i1", DataType::Int64, false),
        Field::new("i2", DataType::Int64, false),
        Field::new("f0", DataType::Float64, false),
        Field::new("f1", DataType::Float64, false),
        Field::new("f2", DataType::Float64, false),
        Field::new("s0", DataType::Utf8, false),
        Field::new("s1", DataType::Utf8, false),
        Field::new("b0", DataType::Boolean, false),
        Field::new("fn0", DataType::Float64, true), // nullable
    ]))
}

#[derive(Serialize, Deserialize, Clone)]
struct MixedRow {
    i0: i64,
    i1: i64,
    i2: i64,
    f0: f64,
    f1: f64,
    f2: f64,
    s0: String,
    s1: String,
    b0: bool,
    fn0: Option<f64>,
}

// Distinct values standing in for categorical columns.
fn make_distinct_strings(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("category_{i:04}")).collect()
}

fn generate_mixed_rows(n: usize, seed: u64) -> (Vec<MixedRow>, RecordBatch) {
    use std::num::Wrapping;
    let mut state = Wrapping(seed);
    let lcg = |s: &mut Wrapping<u64>| -> u64 {
        *s = *s * Wrapping(6364136223846793005) + Wrapping(1442695040888963407);
        s.0
    };

    let distinct = make_distinct_strings(100);

    let mut rows = Vec::with_capacity(n);
    let mut i0v = Vec::with_capacity(n);
    let mut i1v = Vec::with_capacity(n);
    let mut i2v = Vec::with_capacity(n);
    let mut f0v = Vec::with_capacity(n);
    let mut f1v = Vec::with_capacity(n);
    let mut f2v = Vec::with_capacity(n);
    let mut s0v = Vec::with_capacity(n);
    let mut s1v = Vec::with_capacity(n);
    let mut b0v = Vec::with_capacity(n);
    let mut fn0v: Vec<Option<f64>> = Vec::with_capacity(n);

    for _ in 0..n {
        let r0 = lcg(&mut state);
        let r1 = lcg(&mut state);
        let r2 = lcg(&mut state);
        let r3 = lcg(&mut state);
        let r4 = lcg(&mut state);
        let r5 = lcg(&mut state);
        let r6 = lcg(&mut state);

        let i0 = r0 as i64;
        let i1 = r1 as i64;
        let i2 = r2 as i64;
        let f0 = (r3 as f64) / u64::MAX as f64 * 1e6;
        let f1 = (r4 as f64) / u64::MAX as f64 * 1e9;
        let f2 = (r5 as f64) / u64::MAX as f64;
        let s0 = distinct[(r0 % 100) as usize].clone();
        let s1 = distinct[(r1 % 100) as usize].clone();
        let b0 = r6 % 2 == 0;
        let fn0 = if r6 % 10 == 0 {
            None
        } else {
            Some((r6 as f64) / u64::MAX as f64 * 1e4)
        };

        i0v.push(i0);
        i1v.push(i1);
        i2v.push(i2);
        f0v.push(f0);
        f1v.push(f1);
        f2v.push(f2);
        s0v.push(s0.clone());
        s1v.push(s1.clone());
        b0v.push(b0);
        fn0v.push(fn0);

        rows.push(MixedRow {
            i0,
            i1,
            i2,
            f0,
            f1,
            f2,
            s0,
            s1,
            b0,
            fn0,
        });
    }

    let batch = RecordBatch::try_new(
        mixed_schema(),
        vec![
            Arc::new(Int64Array::from(i0v)),
            Arc::new(Int64Array::from(i1v)),
            Arc::new(Int64Array::from(i2v)),
            Arc::new(Float64Array::from(f0v)),
            Arc::new(Float64Array::from(f1v)),
            Arc::new(Float64Array::from(f2v)),
            Arc::new(StringArray::from(s0v)),
            Arc::new(StringArray::from(s1v)),
            Arc::new(BooleanArray::from(b0v)),
            Arc::new(Float64Array::from(fn0v)),
        ],
    )
    .expect("generate_mixed_rows: RecordBatch construction failed");

    (rows, batch)
}

fn build_pipeline(batch: RecordBatch) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_raw_component("Mixed", mixed_schema());
    pipeline.append_record_batch("Mixed", batch).unwrap();
    pipeline
}

fn bench_ipc_at_size(c: &mut Criterion, n: usize) {
    const SEED: u64 = 42;

    let (rows, batch) = generate_mixed_rows(n, SEED);
    let dataset = build_pipeline(batch);

    let mut ipc_bytes: Vec<u8> = Vec::with_capacity(64 * 1024 * 1024);
    dataset.write_ipc(&mut ipc_bytes).unwrap();

    let postcard_bytes = postcard::to_allocvec(&rows).unwrap();

    println!(
        "\n[ipc_checkpoint] {} rows, IPC bytes={:.3}MB, postcard bytes={:.3}MB",
        n,
        ipc_bytes.len() as f64 / 1e6,
        postcard_bytes.len() as f64 / 1e6
    );

    {
        let mut cursor = std::io::Cursor::new(&ipc_bytes);
        let restored = Dataset::read_ipc(&mut cursor).unwrap();
        assert_eq!(restored.rows(), n);
    }

    let group_name = format!("ipc_checkpoint_{n}rows");
    let mut group = c.benchmark_group(&group_name);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    group.bench_function("ipc_encode", |b| {
        b.iter(|| {
            let mut buf: Vec<u8> = Vec::with_capacity(ipc_bytes.len());
            dataset.write_ipc(&mut buf).unwrap();
            std::hint::black_box(buf.len())
        })
    });

    group.bench_function("ipc_decode", |b| {
        b.iter(|| {
            let mut cursor = std::io::Cursor::new(std::hint::black_box(&ipc_bytes));
            let w = Dataset::read_ipc(&mut cursor).unwrap();
            std::hint::black_box(w.rows())
        })
    });

    group.bench_function("postcard_encode", |b| {
        b.iter(|| {
            let bytes = postcard::to_allocvec(std::hint::black_box(&rows)).unwrap();
            std::hint::black_box(bytes.len())
        })
    });

    group.bench_function("postcard_decode", |b| {
        b.iter(|| {
            let decoded: Vec<MixedRow> =
                postcard::from_bytes(std::hint::black_box(&postcard_bytes)).unwrap();
            std::hint::black_box(decoded.len())
        })
    });

    group.finish();
}

/// Stream mode round-trips one checkpoint per item, so the single-row cost is
/// the per-item state overhead, not an amortised bulk figure.
fn bench_ipc_1(c: &mut Criterion) {
    bench_ipc_at_size(c, 1);
}

fn bench_ipc_1k(c: &mut Criterion) {
    bench_ipc_at_size(c, 1_000);
}

fn bench_ipc_10k(c: &mut Criterion) {
    bench_ipc_at_size(c, 10_000);
}

fn bench_ipc_100k(c: &mut Criterion) {
    bench_ipc_at_size(c, 100_000);
}

fn bench_ipc_1m(c: &mut Criterion) {
    bench_ipc_at_size(c, 1_000_000);
}

criterion_group!(
    benches,
    bench_ipc_1,
    bench_ipc_1k,
    bench_ipc_10k,
    bench_ipc_100k,
    bench_ipc_1m
);
criterion_main!(benches);
