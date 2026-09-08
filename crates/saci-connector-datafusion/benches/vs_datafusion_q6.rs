// DataFusion vs SACI: TPC-H Q6 comparison
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench -p saci-connector-datafusion --bench vs_datafusion_q6 -- --sample-size 10
//
// This benchmark compares SACI's Scheduler against DataFusion for TPC-H Q6.
//
// DataFusion is an OLAP query engine with a vectorized executor, expression
// compilation and a cost-based optimizer. SACI is a distributed batch processing
// engine for imperative pipeline workloads, so it is expected to trail
// DataFusion on single-query SQL by 2-10x. This benchmark records that gap.
//
// Data: 1M rows, same synthetic lineitem generator as tpch_q6.rs (seed=42).
// Q6 SQL:
//   SELECT SUM(l_extendedprice * l_discount) AS revenue
//   FROM lineitem
//   WHERE l_shipdate >= 8766   -- 1994-01-01 as days since epoch
//     AND l_shipdate < 9131    -- 1995-01-01
//     AND l_discount BETWEEN 0.05 AND 0.07
//     AND l_quantity < 24;
//
// Dates are integers (days since epoch) throughout, matching the synthetic data.
// DataFusion operates on the same Int32 column.

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, UInt8Array,
};
use arrow_buffer::{BooleanBuffer, MutableBuffer};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use saci_core::SaciError;
use saci_core::component::Component;
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{ParallelSystem, ResourceUpdate, System, SystemMeta, WriteSet};
use serde::{Deserialize, Serialize};

// Installs mimalloc so the benchmark uses the same allocator as the shipped
// binary (`saci-service`'s `mimalloc` feature).
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ---------------------------------------------------------------------------
// Date constants
// ---------------------------------------------------------------------------
const SHIPDATE_GE: i32 = 8766;
const SHIPDATE_LT: i32 = 9131;
const DISCOUNT_LO: f64 = 0.05;
const DISCOUNT_HI: f64 = 0.07;
const QUANTITY_LT: f64 = 24.0;
// 9131 - 8766 = 365. The window is two orders of magnitude short of `i32::MAX`,
// so `SHIPDATE_LT - SHIPDATE_GE` cannot overflow and the unsigned-wrap trick
// below (`(sd - GE) as u32 < SPAN`) is exact for every `i32` input: the only
// residues below 365 come from `sd` in `[GE, LT)`, since the aliasing
// alternative `sd = k - 2^32 + GE` lies outside `i32`.
const SHIPDATE_SPAN: u32 = (SHIPDATE_LT - SHIPDATE_GE) as u32;

// ---------------------------------------------------------------------------
// Lineitem component (12-column schema)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct Lineitem {
    l_orderkey: i64,
    l_partkey: i64,
    l_suppkey: i64,
    l_linenumber: i32,
    l_quantity: f64,
    l_extendedprice: f64,
    l_discount: f64,
    l_tax: f64,
    l_returnflag: u8,
    l_linestatus: u8,
    l_shipdate: i32,
    l_commitdate: i32,
}

impl Component for Lineitem {
    fn name() -> &'static str {
        "Lineitem"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::UInt8, false),
            Field::new("l_linestatus", DataType::UInt8, false),
            Field::new("l_shipdate", DataType::Int32, false),
            Field::new("l_commitdate", DataType::Int32, false),
        ]))
    }
}

// Revenue placeholder component
#[derive(Serialize, Deserialize, Clone)]
struct Revenue {
    piece: f64,
}

impl Component for Revenue {
    fn name() -> &'static str {
        "Revenue"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "piece",
            DataType::Float64,
            false,
        )]))
    }
}

// Resources
struct FilterMask(Arc<BooleanArray>);
struct Q6Revenue(f64);

// ---------------------------------------------------------------------------
// Synthetic data generator (same LCG as tpch_q6.rs)
// ---------------------------------------------------------------------------

fn generate_lineitem_batch(n: usize, seed: u64) -> RecordBatch {
    use std::num::Wrapping;
    let mut state = Wrapping(seed);
    let lcg = |s: &mut Wrapping<u64>| -> u64 {
        *s = *s * Wrapping(6364136223846793005) + Wrapping(1442695040888963407);
        s.0
    };

    let mut l_orderkey = Vec::with_capacity(n);
    let mut l_partkey = Vec::with_capacity(n);
    let mut l_suppkey = Vec::with_capacity(n);
    let mut l_linenumber = Vec::with_capacity(n);
    let mut l_quantity = Vec::with_capacity(n);
    let mut l_extendedprice = Vec::with_capacity(n);
    let mut l_discount = Vec::with_capacity(n);
    let mut l_tax = Vec::with_capacity(n);
    let mut l_returnflag = Vec::with_capacity(n);
    let mut l_linestatus = Vec::with_capacity(n);
    let mut l_shipdate = Vec::with_capacity(n);
    let mut l_commitdate = Vec::with_capacity(n);

    for i in 0..n {
        let r0 = lcg(&mut state);
        let r1 = lcg(&mut state);
        let r2 = lcg(&mut state);
        let r3 = lcg(&mut state);
        let r4 = lcg(&mut state);
        let r5 = lcg(&mut state);
        let r6 = lcg(&mut state);
        let r7 = lcg(&mut state);

        l_orderkey.push(i as i64 / 6 + 1);
        l_partkey.push((r0 % 200_000) as i64 + 1);
        l_suppkey.push((r1 % 10_000) as i64 + 1);
        l_linenumber.push((i % 7 + 1) as i32);
        let qty = 1.0 + (r2 % 50) as f64;
        l_quantity.push(qty);
        let unit_price = 0.90 + (r3 % 10499001) as f64 / 100.0;
        l_extendedprice.push(qty * unit_price);
        l_discount.push((r4 % 11) as f64 / 100.0);
        l_tax.push((r5 % 9) as f64 / 100.0);
        l_returnflag.push((r6 % 3) as u8);
        l_linestatus.push((r6 % 2) as u8);
        let sd_base = 8036i32;
        let sd_range = 2525u64;
        l_shipdate.push(sd_base + (r7 % sd_range) as i32);
        l_commitdate.push(sd_base + (lcg(&mut state) % sd_range) as i32 + 30);
    }

    RecordBatch::try_new(
        Lineitem::schema(),
        vec![
            Arc::new(Int64Array::from(l_orderkey)),
            Arc::new(Int64Array::from(l_partkey)),
            Arc::new(Int64Array::from(l_suppkey)),
            Arc::new(Int32Array::from(l_linenumber)),
            Arc::new(Float64Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Float64Array::from(l_tax)),
            Arc::new(UInt8Array::from(l_returnflag)),
            Arc::new(UInt8Array::from(l_linestatus)),
            Arc::new(Int32Array::from(l_shipdate)),
            Arc::new(Int32Array::from(l_commitdate)),
        ],
    )
    .expect("generate_lineitem_batch failed")
}

// ---------------------------------------------------------------------------
// SACI Scheduler stages
// ---------------------------------------------------------------------------

struct SaciFilterStage;

#[async_trait]
impl ParallelSystem for SaciFilterStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("saci_q6_filter")
            .read("Lineitem", "l_shipdate")
            .read("Lineitem", "l_discount")
            .read("Lineitem", "l_quantity")
            .write_resource::<FilterMask>()
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| SaciError::generic("Lineitem not found"))?;
        let mask = compute_filter_mask(batch)?;
        let update = ResourceUpdate::new(FilterMask(Arc::new(mask)));
        Ok(WriteSet::new().with_resource(update))
    }
}

/// The Q6 composite predicate for one row.
///
/// Two shipdate comparisons collapse into one unsigned compare; see
/// `SHIPDATE_SPAN`. The remaining three are side-effect-free `f64` compares, so
/// `&&` lowers to a branchless `and` and the whole thing vectorises.
#[inline(always)]
fn q6_row_matches(sd: i32, disc: f64, qty: f64) -> bool {
    (sd as u32).wrapping_sub(SHIPDATE_GE as u32) < SHIPDATE_SPAN
        && (DISCOUNT_LO..=DISCOUNT_HI).contains(&disc)
        && qty < QUANTITY_LT
}

/// Build the composite filter mask in a single bit-packing pass.
///
/// Same body as `saci-core/benches/tpch_q6.rs`'s `compute_filter_mask`: the two
/// benchmarks run the identical stage over identical data, so they keep the
/// identical shape.
///
/// Collecting a `Vec<bool>` and handing it to `BooleanArray::from` costs two
/// passes over an 8x oversized intermediate. This reads the three `.values()`
/// slices once and writes 125 KB of bitmap. The inner loop is the
/// 64-lanes-into-a-`u64` idiom of `MutableBuffer::collect_bool`, but over
/// fixed-size array references, so every index is provably in bounds and no
/// panic edge splits the loop body.
///
/// All three columns are non-nullable (`Lineitem::schema()` above) and are built
/// from plain `Vec`s, so they carry no null buffer and the mask gets `None`.
fn compute_filter_mask(batch: &RecordBatch) -> Result<BooleanArray, SaciError> {
    let sd_arr = batch
        .column_by_name("l_shipdate")
        .ok_or_else(|| SaciError::generic("l_shipdate not found"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| SaciError::generic("l_shipdate wrong type"))?;
    let disc_arr = batch
        .column_by_name("l_discount")
        .ok_or_else(|| SaciError::generic("l_discount not found"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| SaciError::generic("l_discount wrong type"))?;
    let qty_arr = batch
        .column_by_name("l_quantity")
        .ok_or_else(|| SaciError::generic("l_quantity not found"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| SaciError::generic("l_quantity wrong type"))?;

    let sd = sd_arr.values();
    let disc = disc_arr.values();
    let qty = qty_arr.values();
    let n = sd.len();
    if disc.len() != n || qty.len() != n {
        return Err(SaciError::generic("filter columns differ in length"));
    }

    const LANES: usize = 64;
    let full = n / LANES;
    let mut words: Vec<u64> = Vec::with_capacity(n.div_ceil(LANES));

    words.extend((0..full).map(|w| {
        let base = w * LANES;
        let sd_c: &[i32; LANES] = sd[base..base + LANES].try_into().unwrap();
        let disc_c: &[f64; LANES] = disc[base..base + LANES].try_into().unwrap();
        let qty_c: &[f64; LANES] = qty[base..base + LANES].try_into().unwrap();
        let mut packed = 0u64;
        for b in 0..LANES {
            packed |= (q6_row_matches(sd_c[b], disc_c[b], qty_c[b]) as u64) << b;
        }
        packed
    }));

    let tail = full * LANES;
    if tail < n {
        let mut packed = 0u64;
        for (b, i) in (tail..n).enumerate() {
            packed |= (q6_row_matches(sd[i], disc[i], qty[i]) as u64) << b;
        }
        words.push(packed);
    }

    let mut buffer: MutableBuffer = words.into();
    buffer.truncate(n.div_ceil(8));
    Ok(BooleanArray::new(
        BooleanBuffer::new(buffer.into(), 0, n),
        None,
    ))
}

struct SaciComputeStage;

#[async_trait]
impl ParallelSystem for SaciComputeStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("saci_q6_compute")
            .read("Lineitem", "l_extendedprice")
            .read("Lineitem", "l_discount")
            .read_resource::<FilterMask>()
            .write("Revenue", "piece")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| SaciError::generic("Lineitem not found"))?;
        let price_arr = batch
            .column_by_name("l_extendedprice")
            .ok_or_else(|| SaciError::generic("l_extendedprice not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("l_extendedprice wrong type"))?;
        let disc_arr = batch
            .column_by_name("l_discount")
            .ok_or_else(|| SaciError::generic("l_discount not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("l_discount wrong type"))?;

        // `(0..n).map(|i| price.value(i) * disc.value(i))` does not vectorise:
        // LLVM cannot prove either index in bounds, so the two `assert!`s stay in
        // the loop. Zipping the `.values()` slices is in-bounds by construction.
        let pieces: Vec<f64> = price_arr
            .values()
            .iter()
            .zip(disc_arr.values().iter())
            .map(|(p, d)| p * d)
            .collect();

        Ok(WriteSet::new().put("Revenue", "piece", Arc::new(Float64Array::from(pieces))))
    }
}

// ---------------------------------------------------------------------------
// Masked sum
// ---------------------------------------------------------------------------

/// Sum `piece` over the rows `mask` selects, by walking the mask's set bits a
/// word at a time.
///
/// Same body as `saci-core/benches/tpch_q6.rs`'s `masked_sum_set_bits`. At this
/// selectivity the ~98% of `u64` mask words that are entirely zero cost one
/// load, one test and one not-taken branch, and only passing rows are ever
/// loaded from `piece`. Reading `mask.value(i)` per row instead would pay a bit
/// extract through the concrete, non-generic, non-`#[inline]`
/// `BooleanArray::value`.
///
/// `as_chunks::<64>` yields `&[f64; 64]`, so `trailing_zeros() < 64` makes the
/// index provably in bounds and the inner loop carries no panic edge. Summation
/// runs left-to-right over the row index, so the total is reproducible and can
/// be asserted against DataFusion's own.
fn masked_sum_set_bits(piece: &[f64], mask: &BooleanBuffer) -> f64 {
    let chunks = mask.bit_chunks();
    let mut acc = 0.0f64;
    for (c, w) in piece.as_chunks::<64>().0.iter().zip(chunks.iter()) {
        let mut w = w;
        while w != 0 {
            acc += c[w.trailing_zeros() as usize];
            w &= w - 1;
        }
    }
    let base = chunks.chunk_len() * 64;
    let mut w = chunks.remainder_bits();
    while w != 0 {
        acc += piece[base + w.trailing_zeros() as usize];
        w &= w - 1;
    }
    acc
}

struct SaciAggregateStage;

#[async_trait]
impl System for SaciAggregateStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("saci_q6_aggregate")
            .read("Revenue", "piece")
            .read_resource::<FilterMask>()
            .write_resource::<Q6Revenue>()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let mask_arr = pipeline
            .get_resource::<FilterMask>()
            .ok_or_else(|| SaciError::generic("FilterMask not found"))?
            .0
            .clone();

        let rev_batch = pipeline
            .batch_for("Revenue")
            .ok_or_else(|| SaciError::generic("Revenue not found"))?;
        let piece_arr = rev_batch
            .column_by_name("piece")
            .ok_or_else(|| SaciError::generic("piece not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("piece wrong type"))?;

        if mask_arr.null_count() != 0 {
            return Err(SaciError::generic("filter mask must not contain nulls"));
        }
        if mask_arr.len() != piece_arr.len() {
            return Err(SaciError::generic("mask and piece differ in length"));
        }
        let revenue = masked_sum_set_bits(piece_arr.values(), mask_arr.values());

        pipeline.insert_resource(Q6Revenue(revenue));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SACI pipeline builder
// ---------------------------------------------------------------------------

fn build_saci_pipeline(batch: &RecordBatch) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_component::<Lineitem>().unwrap();
    pipeline.register_component::<Revenue>().unwrap();
    pipeline
        .append_record_batch("Lineitem", batch.clone())
        .unwrap();
    let n = batch.num_rows();
    let rev = RecordBatch::try_new(
        Revenue::schema(),
        vec![Arc::new(Float64Array::from(vec![0.0f64; n]))],
    )
    .unwrap();
    pipeline.append_record_batch("Revenue", rev).unwrap();
    pipeline
}

// ---------------------------------------------------------------------------
// DataFusion Q6 runner
// ---------------------------------------------------------------------------

/// The Q6 SQL text.
///
/// One definition, shared by every DataFusion variant below, so a decomposed
/// benchmark cannot drift from the whole-closure benchmark it decomposes.
fn q6_sql() -> String {
    format!(
        "SELECT SUM(l_extendedprice * l_discount) AS revenue \
         FROM lineitem \
         WHERE l_shipdate >= {SHIPDATE_GE} \
           AND l_shipdate < {SHIPDATE_LT} \
           AND l_discount >= {DISCOUNT_LO} \
           AND l_discount <= {DISCOUNT_HI} \
           AND l_quantity < {QUANTITY_LT}"
    )
}

/// A fresh `SessionContext` with `batch` registered as the `lineitem` table.
///
/// This is DataFusion's *deployment* setup: `SessionContext::new` builds a
/// session state and registers every built-in scalar, aggregate and window
/// function, and `register_table` publishes the `MemTable` in the default
/// catalog. A server does this once at start-up, not once per query.
fn q6_context(batch: RecordBatch) -> SessionContext {
    let ctx = SessionContext::new();

    let schema = batch.schema();
    let provider = MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable creation failed");
    ctx.register_table("lineitem", Arc::new(provider))
        .expect("register_table failed");
    ctx
}

/// Extract the single `SUM` cell from a Q6 result set.
fn q6_revenue(results: &[RecordBatch]) -> f64 {
    if results.is_empty() || results[0].num_rows() == 0 {
        return 0.0;
    }
    let col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("DataFusion revenue column is not Float64");
    col.value(0)
}

async fn datafusion_q6(batch: RecordBatch) -> f64 {
    let ctx = q6_context(batch);
    let df = ctx.sql(&q6_sql()).await.expect("sql parse failed");
    let results = df.collect().await.expect("datafusion execute failed");
    q6_revenue(&results)
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn bench_vs_datafusion_q6(c: &mut Criterion) {
    const N: usize = 1_000_000;
    const SEED: u64 = 42;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let batch = generate_lineitem_batch(N, SEED);

    println!("\n[vs_datafusion_q6] {} rows, {} CPUs", N, num_cpus::get());

    // Correctness: SACI and DataFusion must agree on the revenue total.
    {
        let saci_revenue = {
            let mut wl = Pipeline::new("q6_check");
            wl.data = build_saci_pipeline(&batch);
            wl.add_parallel_system(SaciFilterStage);
            wl.add_parallel_system(SaciComputeStage);
            wl.add_system(SaciAggregateStage);
            rt.block_on(wl.run()).unwrap();
            wl.data
                .get_resource::<Q6Revenue>()
                .map(|r| r.0)
                .unwrap_or(0.0)
        };

        let df_revenue = rt.block_on(datafusion_q6(batch.clone()));

        let eps = saci_revenue.abs() * 1e-9 + 1.0;
        assert!(
            (saci_revenue - df_revenue).abs() < eps,
            "SACI vs DataFusion Q6 revenue mismatch: saci={saci_revenue:.4} df={df_revenue:.4}"
        );
        println!("[vs_datafusion_q6] correctness check passed — revenue={saci_revenue:.4}");

        // The decomposed variants below must answer the same query. `q6_context`
        // + `sql` + `create_physical_plan` + `collect` is `DataFrame::collect`
        // unrolled, so the plan is identical, but the answer is not
        // bit-reproducible: the plan ends in `CoalescePartitionsExec` over 32
        // partial sums merged in completion order, so DataFusion's total moves by
        // a few ULP between executions. Assert to the precision this comparison
        // publishes.
        let split_revenue = {
            let ctx = q6_context(batch.clone());
            rt.block_on(async {
                let df = ctx.sql(&q6_sql()).await.expect("sql parse failed");
                let plan = df
                    .create_physical_plan()
                    .await
                    .expect("physical planning failed");
                println!(
                    "[vs_datafusion_q6] DataFusion physical plan:\n{}",
                    displayable(plan.as_ref()).indent(false)
                );
                let task_ctx = Arc::new(df.task_ctx());
                let results = collect(plan, task_ctx).await.expect("execute failed");
                q6_revenue(&results)
            })
        };
        assert_eq!(
            format!("{split_revenue:.4}"),
            format!("{df_revenue:.4}"),
            "decomposed variants must answer the same query: split={split_revenue:.4} whole={df_revenue:.4}"
        );
        assert_eq!(
            format!("{split_revenue:.4}"),
            "689469343.9966",
            "decomposed revenue moved off the published total: {split_revenue:.4}"
        );
    }

    let mut group = c.benchmark_group("vs_datafusion_q6");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));

    group.bench_function("saci_pipeline", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("q6");
            wl.data = build_saci_pipeline(std::hint::black_box(&batch));
            wl.add_parallel_system(SaciFilterStage);
            wl.add_parallel_system(SaciComputeStage);
            wl.add_system(SaciAggregateStage);
            rt.block_on(wl.run()).unwrap();
            let r = wl
                .data
                .get_resource::<Q6Revenue>()
                .map(|r| r.0)
                .unwrap_or(0.0);
            std::hint::black_box(r)
        })
    });

    // SACI's own per-iteration fixed cost, so both sides of the comparison are
    // decomposed: `Dataset::new`, two component registrations, the `Lineitem`
    // append, a fresh 8 MB zeroed `Revenue` column (`build_saci_pipeline` above),
    // the three system registrations and the drop of all of it.
    //
    // It does not include the scheduler's stage-plan build. That happens inside
    // `Pipeline::run` via `ensure_plan`, which is `pub(super)` and unreachable
    // from a bench, and it is paid per iteration as well because every iteration
    // builds a fresh `Pipeline` with fresh `OnceLock`s. So this is a lower bound
    // on the part of `saci_pipeline` a deployment would pay once.
    group.bench_function("saci_pipeline_setup_only", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("q6");
            wl.data = build_saci_pipeline(std::hint::black_box(&batch));
            wl.add_parallel_system(SaciFilterStage);
            wl.add_parallel_system(SaciComputeStage);
            wl.add_system(SaciAggregateStage);
            std::hint::black_box(wl.data.rows())
        })
    });

    group.bench_function("datafusion_sql", |b| {
        b.iter(|| {
            let r = rt.block_on(datafusion_q6(std::hint::black_box(batch.clone())));
            std::hint::black_box(r)
        })
    });

    // -- DataFusion, decomposed --------------------------------------------
    //
    // `datafusion_sql` above times one whole `datafusion_q6` call: session
    // construction, table registration, parse, logical planning, logical
    // optimisation, physical planning and execution. A deployment pays the first
    // six once and only the last per query. The three benchmarks below split it
    // so the parts add back up to the whole.
    //
    // The split point is not the obvious one. `SessionContext::sql` runs
    // sqlparser and `SqlToRel` and stops at an unoptimised `LogicalPlan`;
    // `SessionState::create_physical_plan` runs the whole logical optimiser plus
    // physical planning and the physical optimiser rules. So planning is `sql()`
    // and `create_physical_plan()`, and execution begins at
    // `physical_plan::collect`. Splitting at the `sql()`/`collect()` seam would
    // charge the logical optimiser and the physical planner to execution.

    // Once-per-deployment: session state (every built-in UDF) + `MemTable` +
    // catalog registration.
    group.bench_function("datafusion_sql_setup_only", |b| {
        b.iter(|| q6_context(std::hint::black_box(batch.clone())))
    });

    // Once-per-query-shape: parse, logical plan, logical optimisation and
    // physical planning, against a context that already exists and already has
    // the table. The SQL string is formatted once outside, so this is planning
    // and nothing else.
    let plan_ctx = q6_context(batch.clone());
    let plan_sql = q6_sql();
    group.bench_function("datafusion_sql_plan_only", |b| {
        b.iter(|| {
            let plan = rt.block_on(async {
                plan_ctx
                    .sql(std::hint::black_box(&plan_sql))
                    .await
                    .expect("sql parse failed")
                    .create_physical_plan()
                    .await
                    .expect("physical planning failed")
            });
            std::hint::black_box(plan)
        })
    });

    // Per-query: one execution of an already-planned query. This is the
    // DataFusion number comparable to `saci_pipeline`.
    //
    // The physical plan is rebuilt every iteration, outside the timed window,
    // rather than built once and reused: a DataFusion `ExecutionPlan` is not free
    // to re-execute without limit, because every `execute()` registers a fresh
    // metric set into the node's `ExecutionPlanMetricsSet`, an append-only `Vec`
    // behind a mutex. Over the iterations criterion runs at this sample size that
    // grows without bound and would be billed to execution. `iter_custom` times
    // the `collect` alone; `Instant::now()` costs ~20 ns against a ~500 µs body.
    //
    // Re-executing a memory source is supported: the generator is reset so each
    // `execute()` call produces an independent stream from the beginning.
    let exec_ctx = q6_context(batch.clone());
    let exec_df = rt
        .block_on(exec_ctx.sql(&q6_sql()))
        .expect("sql parse failed");
    group.bench_function("datafusion_sql_execute_only", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = std::time::Duration::ZERO;
            for _ in 0..iters {
                let plan = rt
                    .block_on(exec_df.create_physical_plan())
                    .expect("physical planning failed");
                // `DataFrame::collect`'s own first line, kept inside the timed
                // window: a fresh `TaskContext` per execution is what
                // DataFusion itself does.
                let task_ctx = Arc::new(exec_df.task_ctx());
                let start = std::time::Instant::now();
                let results = rt
                    .block_on(collect(plan, task_ctx))
                    .expect("datafusion execute failed");
                elapsed += start.elapsed();
                std::hint::black_box(q6_revenue(&results));
            }
            elapsed
        })
    });

    group.finish();

    println!("[vs_datafusion_q6] Read the decomposition, not just the headline:");
    println!("  datafusion_sql times session construction, table registration,");
    println!("  parse, logical optimisation, physical planning AND execution in one");
    println!("  closure. A deployment pays all but the last once, so compare");
    println!("  saci_pipeline against datafusion_sql_execute_only for execution, and");
    println!("  note that saci_pipeline still carries its own per-iteration setup");
    println!("  (saci_pipeline_setup_only) which DataFusion's number does not.");
}

criterion_group!(benches, bench_vs_datafusion_q6);
criterion_main!(benches);
