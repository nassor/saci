// Batch mode versus Stream mode: what item size costs.
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench -p saci-core --bench batch_vs_stream
//
// The two processing modes differ in exactly one way: batch mode makes one
// pipeline invocation over N rows, stream mode makes N/k invocations over k
// rows each. The systems, the DAG and the stage plan are identical. This
// benchmark holds the total row count fixed and sweeps k, so the only variable
// is item size.
//
// Two groups:
//
//   1. `item_size`: 100 000 rows of Q6-shaped work (a filter-and-compute
//      `ParallelSystem` plus a sequential accumulator), processed at item sizes
//      from 1 row to the whole 100 000 in one shot. `k = 100_000` is batch mode;
//      `k = 1` is stream mode at its finest grain. Reported as total wall time
//      for the same work, so the ratio is the throughput cost of streaming.
//
//   2. `stage_dispatch`: a stage holding two independent `ParallelSystem`s,
//      swept across `STAGE_INLINE_THRESHOLD` (100 000 rows). Below the
//      threshold the executor runs them inline; at or above it dispatches one
//      `spawn_blocking` per system. The discontinuity at the boundary is what
//      dispatch costs.
//
// The accumulator lives in an `Arc<Mutex<f64>>` inside the system struct, not in
// a `Dataset` resource: stream mode clears the dataset between items and
// `clear()` drops resources. That is the cross-item state contract for
// `run_stream`. Asserting batch total == stream total checks the modes agree.

use std::sync::{Arc, Mutex};

use arrow_array::{Array, Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use saci_core::SaciError;
use saci_core::dataset::Dataset;
use saci_core::io::source::Source;
use saci_core::pipeline::Pipeline;
use saci_core::system::{
    ParallelSystem, STAGE_INLINE_THRESHOLD, System, SystemMeta, WriteSet, system_fn,
};

// Installs mimalloc so the benchmark uses the same allocator as the shipped
// binary (`saci-service`'s `mimalloc` feature).
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Q6 predicate constants, same as `tpch_q6`.
const SHIPDATE_GE: i32 = 8766; // 1994-01-01
const SHIPDATE_LT: i32 = 9131; // 1995-01-01
const DISCOUNT_LO: f64 = 0.05;
const DISCOUNT_HI: f64 = 0.07;
const QUANTITY_LT: f64 = 24.0;

const COMP: &str = "Tick";
const TOTAL_ROWS: usize = 100_000;
const SEED: u64 = 42;

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

/// Six columns: four predicate/compute inputs plus two output columns.
///
/// The outputs live on the *same* component as the inputs so the row count
/// always matches, in both modes. An output on a separate component would need
/// rows pre-appended, which stream mode's `clear()` removes.
fn tick_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("shipdate", DataType::Int32, false),
        Field::new("quantity", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
        Field::new("revenue", DataType::Float64, false),
        Field::new("taxed", DataType::Float64, false),
    ]))
}

fn generate_batch(n: usize, seed: u64) -> RecordBatch {
    use std::num::Wrapping;
    let mut state = Wrapping(seed);
    let mut lcg = || -> u64 {
        state = state * Wrapping(6364136223846793005) + Wrapping(1442695040888963407);
        state.0
    };

    let mut shipdate = Vec::with_capacity(n);
    let mut quantity = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut discount = Vec::with_capacity(n);

    for _ in 0..n {
        let qty = 1.0 + (lcg() % 50) as f64;
        let unit = 0.90 + (lcg() % 10_499_001) as f64 / 100.0;
        quantity.push(qty);
        price.push(qty * unit);
        discount.push((lcg() % 11) as f64 / 100.0);
        // 1992-01-02 (8036) .. 1998-12-01, a 2525-day range.
        shipdate.push(8036i32 + (lcg() % 2525) as i32);
    }

    let zeros = vec![0.0f64; n];
    RecordBatch::try_new(
        tick_schema(),
        vec![
            Arc::new(Int32Array::from(shipdate)),
            Arc::new(Float64Array::from(quantity)),
            Arc::new(Float64Array::from(price)),
            Arc::new(Float64Array::from(discount)),
            Arc::new(Float64Array::from(zeros.clone())),
            Arc::new(Float64Array::from(zeros)),
        ],
    )
    .expect("generate_batch failed")
}

fn f64_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array, SaciError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| SaciError::generic(format!("{name} missing or not Float64")))
}

// ---------------------------------------------------------------------------
// Systems
// ---------------------------------------------------------------------------

/// Filter and compute in one parallel pass: `revenue = price * discount` for
/// rows passing the Q6 predicates, 0.0 otherwise.
struct RevenueStage;

#[async_trait]
impl ParallelSystem for RevenueStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("revenue")
            .read(COMP, "shipdate")
            .read(COMP, "quantity")
            .read(COMP, "price")
            .read(COMP, "discount")
            .write(COMP, "revenue")
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = data
            .batch_for(COMP)
            .ok_or_else(|| SaciError::generic("Tick component not found"))?;
        let sd = batch
            .column_by_name("shipdate")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| SaciError::generic("shipdate missing or not Int32"))?;
        let qty = f64_col(batch, "quantity")?;
        let price = f64_col(batch, "price")?;
        let disc = f64_col(batch, "discount")?;

        let out: Float64Array = (0..batch.num_rows())
            .map(|i| {
                let d = disc.value(i);
                let pass = (SHIPDATE_GE..SHIPDATE_LT).contains(&sd.value(i))
                    && (DISCOUNT_LO..=DISCOUNT_HI).contains(&d)
                    && qty.value(i) < QUANTITY_LT;
                if pass { price.value(i) * d } else { 0.0 }
            })
            .collect();

        Ok(WriteSet::new().put(COMP, "revenue", Arc::new(out)))
    }
}

/// A second, independent parallel system writing a disjoint column, so it
/// shares a stage with [`RevenueStage`]. Only used by the `stage_dispatch`
/// group.
struct TaxStage;

#[async_trait]
impl ParallelSystem for TaxStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("tax")
            .read(COMP, "price")
            .read(COMP, "discount")
            .write(COMP, "taxed")
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = data
            .batch_for(COMP)
            .ok_or_else(|| SaciError::generic("Tick component not found"))?;
        let price = f64_col(batch, "price")?;
        let disc = f64_col(batch, "discount")?;

        let out: Float64Array = (0..batch.num_rows())
            .map(|i| price.value(i) * (1.0 - disc.value(i)))
            .collect();

        Ok(WriteSet::new().put(COMP, "taxed", Arc::new(out)))
    }
}

/// Sequential accumulator. State lives in the struct, not in a `Dataset`
/// resource, so it survives the per-item `clear()` that stream mode performs.
struct SumStage {
    total: Arc<Mutex<f64>>,
}

#[async_trait]
impl System for SumStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("sum").read(COMP, "revenue")
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let batch = data
            .batch_for(COMP)
            .ok_or_else(|| SaciError::generic("Tick component not found"))?;
        let rev = f64_col(batch, "revenue")?;
        let mut sum = 0.0f64;
        for i in 0..rev.len() {
            sum += rev.value(i);
        }
        *self.total.lock().unwrap() += sum;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Source that replays pre-sliced batches
// ---------------------------------------------------------------------------

/// Yields pre-built batches in order, then EOF.
///
/// The batches are zero-copy `RecordBatch::slice` views of one parent batch and
/// are cloned per call (a handful of `Arc` bumps), so the source contributes
/// almost nothing to the measurement. A channel would add wakeup latency to
/// every item.
struct ReplaySource {
    batches: Arc<Vec<RecordBatch>>,
    pos: usize,
    schema: Arc<Schema>,
}

impl ReplaySource {
    fn new(batches: Arc<Vec<RecordBatch>>, schema: Arc<Schema>) -> Self {
        Self {
            batches,
            pos: 0,
            schema,
        }
    }
}

#[async_trait]
impl Source for ReplaySource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        let out = self.batches.get(self.pos).cloned();
        if out.is_some() {
            self.pos += 1;
        }
        Ok(out)
    }
}

/// Split `parent` into `ceil(rows / item_size)` zero-copy slices.
fn slice_into_items(parent: &RecordBatch, item_size: usize) -> Arc<Vec<RecordBatch>> {
    let total = parent.num_rows();
    let mut out = Vec::with_capacity(total.div_ceil(item_size));
    let mut offset = 0usize;
    while offset < total {
        let len = item_size.min(total - offset);
        out.push(parent.slice(offset, len));
        offset += len;
    }
    Arc::new(out)
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

/// Batch mode: one invocation over the whole batch, via `Pipeline::run`.
fn run_batch_mode(rt: &tokio::runtime::Runtime, parent: &RecordBatch) -> f64 {
    let total = Arc::new(Mutex::new(0.0f64));
    let mut p = Pipeline::new("batch");
    p.data.register_raw_component(COMP, tick_schema());
    p.data
        .append_record_batch(COMP, parent.slice(0, parent.num_rows()))
        .expect("append");
    p.add_parallel_system(RevenueStage);
    p.add_system(SumStage {
        total: Arc::clone(&total),
    });
    rt.block_on(p.run()).expect("batch run");
    *total.lock().unwrap()
}

/// Stream mode: one invocation per item, via `Pipeline::run_stream`.
fn run_stream_mode(
    rt: &tokio::runtime::Runtime,
    items: Arc<Vec<RecordBatch>>,
    parallel_stage: bool,
) -> f64 {
    let total = Arc::new(Mutex::new(0.0f64));
    let mut p = Pipeline::new("stream");
    p.data.register_raw_component(COMP, tick_schema());
    p.add_parallel_system(RevenueStage);
    if parallel_stage {
        p.add_parallel_system(TaxStage);
    }
    p.add_system(SumStage {
        total: Arc::clone(&total),
    });
    p.add_source(COMP, ReplaySource::new(items, tick_schema()));
    rt.block_on(p.run_stream()).expect("stream run");
    *total.lock().unwrap()
}

/// One invocation over `rows`, with two independent parallel systems sharing a
/// stage. Used to probe the inline/dispatch boundary.
fn run_two_parallel_stage(rt: &tokio::runtime::Runtime, parent: &RecordBatch) -> f64 {
    let total = Arc::new(Mutex::new(0.0f64));
    let mut p = Pipeline::new("two_parallel");
    p.data.register_raw_component(COMP, tick_schema());
    p.data
        .append_record_batch(COMP, parent.clone())
        .expect("append");
    p.add_parallel_system(RevenueStage);
    p.add_parallel_system(TaxStage);
    p.add_system(SumStage {
        total: Arc::clone(&total),
    });
    rt.block_on(p.run()).expect("two-parallel run");
    *total.lock().unwrap()
}

/// Scalar row-oriented baseline, for scale.
fn scalar_revenue(batch: &RecordBatch) -> f64 {
    let sd = batch
        .column_by_name("shipdate")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let qty = f64_col(batch, "quantity").unwrap();
    let price = f64_col(batch, "price").unwrap();
    let disc = f64_col(batch, "discount").unwrap();

    let mut revenue = 0.0f64;
    for i in 0..batch.num_rows() {
        let d = disc.value(i);
        if (SHIPDATE_GE..SHIPDATE_LT).contains(&sd.value(i))
            && (DISCOUNT_LO..=DISCOUNT_HI).contains(&d)
            && qty.value(i) < QUANTITY_LT
        {
            revenue += price.value(i) * d;
        }
    }
    revenue
}

fn assert_close(label: &str, expected: f64, got: f64) {
    let tol = expected.abs() * 1e-9 + 1e-6;
    assert!(
        (expected - got).abs() <= tol,
        "{label}: expected {expected:.6}, got {got:.6}"
    );
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_batch_vs_stream(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let parent = generate_batch(TOTAL_ROWS, SEED);
    let expected = scalar_revenue(&parent);

    println!(
        "\n[batch_vs_stream] {} rows, {} columns, {} CPUs, STAGE_INLINE_THRESHOLD={}",
        TOTAL_ROWS,
        parent.num_columns(),
        num_cpus::get(),
        STAGE_INLINE_THRESHOLD
    );

    // Correctness: every mode and item size must agree with the scalar total.
    {
        assert_close("batch", expected, run_batch_mode(&rt, &parent));
        assert_close(
            "two_parallel",
            expected,
            run_two_parallel_stage(&rt, &parent),
        );
        for k in [1usize, 100, 10_000, TOTAL_ROWS] {
            let got = run_stream_mode(&rt, slice_into_items(&parent, k), false);
            assert_close(&format!("stream_k{k}"), expected, got);
        }
        println!("[batch_vs_stream] correctness check passed — revenue={expected:.4}");
    }

    // ── Group 1: item size sweep, same total work ────────────────────────────
    let mut group = c.benchmark_group("item_size");
    // Rebuilding a 100k-row dataset per iteration is allocator-sensitive, so the
    // sample count is raised to keep the confidence interval tight.
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(20));

    group.bench_function("scalar_baseline", |b| {
        b.iter(|| std::hint::black_box(scalar_revenue(std::hint::black_box(&parent))))
    });

    group.bench_function("batch_run_no_io", |b| {
        b.iter(|| std::hint::black_box(run_batch_mode(&rt, std::hint::black_box(&parent))))
    });

    for k in [1usize, 10, 100, 1_000, 10_000, TOTAL_ROWS] {
        // Slicing happens once, outside the timed region.
        let items = slice_into_items(&parent, k);
        let name = if k == TOTAL_ROWS {
            "stream_whole_batch".to_string()
        } else {
            format!("stream_items_of_{k}")
        };
        group.bench_function(&name, |b| {
            b.iter(|| {
                std::hint::black_box(run_stream_mode(
                    &rt,
                    Arc::clone(std::hint::black_box(&items)),
                    false,
                ))
            })
        });
    }

    group.finish();

    // ── Group 2: multi-parallel stage, inline versus dispatched ──────────────
    // Swept well past `STAGE_INLINE_THRESHOLD` to find the row count where one
    // `spawn_blocking` per system starts paying for itself. Neither system
    // implements `run_slice`, so this isolates stage-level dispatch.
    let stage_parent = generate_batch(1_048_576, SEED);
    let mut group = c.benchmark_group("stage_dispatch");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    for rows in [
        256usize, 512, 1_024, 4_096, 16_384, 65_536, 131_072, 262_144, 1_048_576,
    ] {
        let slice = stage_parent.slice(0, rows);
        let label = if (rows as u32) < STAGE_INLINE_THRESHOLD {
            format!("{rows}rows_inline")
        } else {
            format!("{rows}rows_dispatched")
        };
        group.bench_function(&label, |b| {
            b.iter(|| {
                std::hint::black_box(run_two_parallel_stage(&rt, std::hint::black_box(&slice)))
            })
        });
    }

    group.finish();

    // ── Group 3: per-item floor, an empty-DAG pipeline ───────────────────────
    // Isolates what stream mode costs before any user code runs: clear, append,
    // stage walk, sink drain. One system that does nothing measurable.
    let mut group = c.benchmark_group("per_item_floor");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    let one_row_items = slice_into_items(&parent.slice(0, 10_000), 1);
    group.bench_function("noop_system_10k_items", |b| {
        b.iter(|| {
            let mut p = Pipeline::new("floor");
            p.data.register_raw_component(COMP, tick_schema());
            p.add_system(system_fn(
                SystemMeta::new("noop").read(COMP, "price"),
                |_| Ok(()),
            ));
            p.add_source(
                COMP,
                ReplaySource::new(Arc::clone(&one_row_items), tick_schema()),
            );
            let stats = rt.block_on(p.run_stream()).expect("floor run");
            std::hint::black_box(stats.systems_run)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_batch_vs_stream);
criterion_main!(benches);
