// TPC-H Query 1 benchmark
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench --bench tpch_q1 -- --sample-size 10
//
// TPC-H Q1:
//   SELECT l_returnflag, l_linestatus,
//          SUM(l_quantity), SUM(l_extendedprice),
//          SUM(l_extendedprice * (1 - l_discount)),
//          SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//          AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
//          COUNT(*)
//   FROM lineitem
//   WHERE l_shipdate <= '1998-12-01' - INTERVAL '90' DAY
//   GROUP BY l_returnflag, l_linestatus
//   ORDER BY l_returnflag, l_linestatus;
//
// Synthetic data: ~1 000 000 rows, seed=42.
// Threshold date: 1998-09-02 (days since epoch 1970-01-01 = 10471).
//
// Architecture:
//   Stage 1 (ParallelSystem): FilterStage computes a boolean mask for
//     l_shipdate <= threshold and stores it as a BooleanArray resource.
//   Stage 2 (ParallelSystem): ComputeStage reads the mask plus the price,
//     discount and tax columns, writes disc_price and charge columns.
//   Stage 3 (System): AggregateStage groups on (returnflag, linestatus)
//     and stores a Vec<Q1GroupResult> resource.
//
// Scalar baseline: single-pass Vec<LineItem> loop with the same filter and
//   aggregation logic, used as a lower bound for the Arrow pipeline.

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, UInt8Array,
};
use arrow_buffer::{BooleanBuffer, MutableBuffer};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use saci_core::SaciError;
use saci_core::component::Component;
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{
    ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta, WriteSet,
};
use serde::{Deserialize, Serialize};

mod support;
use support::tpch::Lineitem;

// Installs mimalloc so the benchmark uses the same allocator as the shipped
// binary (`saci-service`'s `mimalloc` feature).
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ---------------------------------------------------------------------------
// Lineitem schema (TPC-H subset used by Q1)
// ---------------------------------------------------------------------------
// Days since Unix epoch for 1998-09-02 (= 1998-12-01 minus 90 days)
const SHIPDATE_THRESHOLD: i32 = 10471;

// Distinct returnflag values (A=0, N=1, R=2)
const RETURNFLAG_VALUES: &[u8] = &[0, 1, 2];
// Distinct linestatus values (F=0, O=1)
const LINESTATUS_VALUES: &[u8] = &[0, 1];

// Group slots. Both key domains are dense and zero-based, so a value *is* its
// own index and a (returnflag, linestatus) pair addresses a fixed array at
// `returnflag * LINESTATUS_VALUES.len() + linestatus`. That ordering is already
// lexicographic by (returnflag, linestatus), which is the order Q1's ORDER BY
// wants, so nothing has to be sorted afterwards.
const Q1_GROUPS: usize = RETURNFLAG_VALUES.len() * LINESTATUS_VALUES.len();

// Derived columns component: disc_price and charge.
#[derive(Serialize, Deserialize, Clone)]
struct LineitemDerived {
    disc_price: f64,
    charge: f64,
}

impl Component for LineitemDerived {
    fn name() -> &'static str {
        "LineitemDerived"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("disc_price", DataType::Float64, false),
            Field::new("charge", DataType::Float64, false),
        ]))
    }
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// Boolean mask from the filter stage: row i passes iff mask[i] = true.
struct FilterMask(Arc<BooleanArray>);

/// Aggregation result record.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Q1GroupResult {
    returnflag: u8,
    linestatus: u8,
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    avg_qty: f64,
    avg_price: f64,
    avg_disc: f64,
    count_order: u64,
}

/// The final aggregation resource: sorted by (returnflag, linestatus).
struct Q1Result(Vec<Q1GroupResult>);

// ---------------------------------------------------------------------------
// Data generator
// ---------------------------------------------------------------------------

/// Generate a RecordBatch of `n` rows with seed-based deterministic data.
fn generate_lineitem_batch(n: usize, seed: u64) -> RecordBatch {
    use std::num::Wrapping;

    let mut state = Wrapping(seed);
    let lcg_next = |s: &mut Wrapping<u64>| -> u64 {
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
        let r0 = lcg_next(&mut state);
        let r1 = lcg_next(&mut state);
        let r2 = lcg_next(&mut state);
        let r3 = lcg_next(&mut state);

        l_orderkey.push(i as i64 / 6 + 1);
        l_partkey.push((r0 % 200_000) as i64 + 1);
        l_suppkey.push((r1 % 10_000) as i64 + 1);
        l_linenumber.push((i % 7 + 1) as i32);
        l_quantity.push(1.0 + (r2 % 50) as f64);
        // extendedprice: quantity * unit price; unit price in [0.90, 104999.00]
        let unit_price = 0.90 + (r3 % 10499001) as f64 / 100.0;
        let qty = l_quantity[i];
        l_extendedprice.push(qty * unit_price);
        // discount: 0.00..0.10 in steps of 0.01
        let disc_r = lcg_next(&mut state);
        l_discount.push((disc_r % 11) as f64 / 100.0);
        // tax: 0.00..0.08 in steps of 0.01
        let tax_r = lcg_next(&mut state);
        l_tax.push((tax_r % 9) as f64 / 100.0);
        // returnflag: A/N/R  (roughly: F rows get A or R, O rows get N)
        let rf_r = lcg_next(&mut state);
        l_returnflag.push(RETURNFLAG_VALUES[(rf_r % 3) as usize]);
        // linestatus: F or O
        let ls_r = lcg_next(&mut state);
        l_linestatus.push(LINESTATUS_VALUES[(ls_r % 2) as usize]);
        // shipdate: 1992-01-02 (8036) .. 1998-12-01 (10471+90=10561)
        // ~80% of rows should pass the filter (shipdate <= 10471)
        let sd_r = lcg_next(&mut state);
        let shipdate_base = 8036i32;
        let shipdate_range = 2560i32; // covers ~7 years; 80% is <= threshold
        l_shipdate.push(shipdate_base + (sd_r % shipdate_range as u64) as i32);
        let cd_r = lcg_next(&mut state);
        l_commitdate.push(shipdate_base + (cd_r % 2560) as i32 + 30);
    }

    let schema = Lineitem::schema();
    RecordBatch::try_new(
        schema,
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
    .expect("generate_lineitem_batch: RecordBatch construction failed")
}

// ---------------------------------------------------------------------------
// Scalar baseline structs
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LineItemRow {
    l_quantity: f64,
    l_extendedprice: f64,
    l_discount: f64,
    l_tax: f64,
    l_returnflag: u8,
    l_linestatus: u8,
    l_shipdate: i32,
}

fn extract_scalar_rows(batch: &RecordBatch) -> Vec<LineItemRow> {
    let qty = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let price = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let disc = batch
        .column(6)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let tax = batch
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let rf = batch
        .column(8)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let ls = batch
        .column(9)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let sd = batch
        .column(10)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    (0..batch.num_rows())
        .map(|i| LineItemRow {
            l_quantity: qty.value(i),
            l_extendedprice: price.value(i),
            l_discount: disc.value(i),
            l_tax: tax.value(i),
            l_returnflag: rf.value(i),
            l_linestatus: ls.value(i),
            l_shipdate: sd.value(i),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Scalar Q1 baseline
// ---------------------------------------------------------------------------

fn scalar_q1(rows: &[LineItemRow]) -> Vec<Q1GroupResult> {
    #[derive(Default, Clone, Copy)]
    struct Acc {
        sum_qty: f64,
        sum_price: f64,
        sum_disc_price: f64,
        sum_charge: f64,
        count: u64,
        sum_disc: f64, // for avg_disc
    }

    // The row loop below is row-oriented on purpose: that is the shape this
    // baseline represents, and it is not vectorised. The group lookup is a
    // six-slot array index, the same lookup the SACI aggregate uses, so the
    // reference is not weakened by a hashing cost the pipeline never pays.
    let mut groups = [Acc::default(); Q1_GROUPS];

    for row in rows {
        if row.l_shipdate > SHIPDATE_THRESHOLD {
            continue;
        }
        let slot = row.l_returnflag as usize * LINESTATUS_VALUES.len() + row.l_linestatus as usize;
        let acc = &mut groups[slot];
        let disc_price = row.l_extendedprice * (1.0 - row.l_discount);
        let charge = disc_price * (1.0 + row.l_tax);
        acc.sum_qty += row.l_quantity;
        acc.sum_price += row.l_extendedprice;
        acc.sum_disc_price += disc_price;
        acc.sum_charge += charge;
        acc.sum_disc += row.l_discount;
        acc.count += 1;
    }

    // Slot order is already (returnflag, linestatus) ascending. Empty slots are
    // dropped, so the group count is the number of populated groups.
    let mut result: Vec<Q1GroupResult> = Vec::with_capacity(Q1_GROUPS);
    for (slot, acc) in groups.iter().enumerate() {
        if acc.count == 0 {
            continue;
        }
        let count = acc.count as f64;
        result.push(Q1GroupResult {
            returnflag: (slot / LINESTATUS_VALUES.len()) as u8,
            linestatus: (slot % LINESTATUS_VALUES.len()) as u8,
            sum_qty: acc.sum_qty,
            sum_base_price: acc.sum_price,
            sum_disc_price: acc.sum_disc_price,
            sum_charge: acc.sum_charge,
            avg_qty: acc.sum_qty / count,
            avg_price: acc.sum_price / count,
            avg_disc: acc.sum_disc / count,
            count_order: acc.count,
        });
    }
    result
}

// ---------------------------------------------------------------------------
// Scheduler stages
// ---------------------------------------------------------------------------

/// Build the shipdate filter mask in a single bit-packing pass.
///
/// Collecting a `Vec<bool>` and handing it to `BooleanArray::from` costs two
/// passes over an 8x oversized intermediate. This writes the 125 KB bitmap
/// directly.
///
/// The inner loop is the 64-lanes-into-a-`u64` idiom of
/// `MutableBuffer::collect_bool`, but over `&[i32; 64]` chunks, so every access
/// is provably in bounds and no panic edge splits the loop body.
///
/// `l_shipdate` is non-nullable (`benches/support/tpch.rs`) and carries no null
/// buffer, so the mask is built with `None` for nulls.
fn shipdate_mask(sd: &[i32]) -> BooleanArray {
    const LANES: usize = 64;
    let n = sd.len();
    let (chunks, tail) = sd.as_chunks::<LANES>();

    let mut words: Vec<u64> = Vec::with_capacity(n.div_ceil(LANES));
    words.extend(chunks.iter().map(|c| {
        let mut packed = 0u64;
        for (b, &v) in c.iter().enumerate() {
            packed |= u64::from(v <= SHIPDATE_THRESHOLD) << b;
        }
        packed
    }));
    if !tail.is_empty() {
        let mut packed = 0u64;
        for (b, &v) in tail.iter().enumerate() {
            packed |= u64::from(v <= SHIPDATE_THRESHOLD) << b;
        }
        words.push(packed);
    }

    let mut buffer: MutableBuffer = words.into();
    buffer.truncate(n.div_ceil(8));
    BooleanArray::new(BooleanBuffer::new(buffer.into(), 0, n), None)
}

// Stage 1: FilterStage builds the boolean mask resource.
struct FilterStage;

#[async_trait]
impl ParallelSystem for FilterStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q1_filter")
            .read("Lineitem", "l_shipdate")
            .write_resource::<FilterMask>()
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| SaciError::generic("Lineitem not found"))?;
        let sd_col = batch
            .column_by_name("l_shipdate")
            .ok_or_else(|| SaciError::generic("l_shipdate not found"))?;
        let sd_arr = sd_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| SaciError::generic("l_shipdate wrong type"))?;

        let bool_array = Arc::new(shipdate_mask(sd_arr.values()));
        let update = ResourceUpdate::new(FilterMask(bool_array));

        Ok(WriteSet::new().with_resource(update))
    }

    fn run_slice(
        &self,
        pipeline: &Dataset,
        rows: std::ops::Range<u32>,
    ) -> Option<Result<SliceWriteSet, SaciError>> {
        let batch = pipeline.batch_for("Lineitem")?;
        let sd_col = batch.column_by_name("l_shipdate")?;
        let sd_arr = sd_col.as_any().downcast_ref::<Int32Array>()?;
        let start = rows.start as usize;
        let len = (rows.end - rows.start) as usize;
        let slice = sd_arr.slice(start, len);
        let slice_arr = slice.as_any().downcast_ref::<Int32Array>().unwrap();
        let bool_array: Arc<dyn arrow_array::Array> = Arc::new(shipdate_mask(slice_arr.values()));
        Some(Ok(SliceWriteSet::new(rows).put(
            "_filter_mask",
            "mask",
            bool_array,
        )))
    }

    fn merge_slices(&self, slices: Vec<SliceWriteSet>) -> Result<WriteSet, SaciError> {
        use arrow_select::concat::concat;
        let arrays: Vec<&dyn arrow_array::Array> = slices
            .iter()
            .filter_map(|s| s.fields.get(&("_filter_mask", "mask")))
            .map(|a| a.as_ref())
            .collect();
        if arrays.is_empty() {
            let empty: Arc<BooleanArray> = Arc::new(BooleanArray::from(vec![false; 0]));
            return Ok(WriteSet::new().with_resource(ResourceUpdate::new(FilterMask(empty))));
        }
        let merged = concat(&arrays)
            .map_err(|e| SaciError::generic(format!("FilterStage merge error: {e}")))?;
        let bool_arr = merged
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| SaciError::generic("FilterStage: merged array is not BooleanArray"))?;
        let update = ResourceUpdate::new(FilterMask(Arc::new(bool_arr.clone())));
        Ok(WriteSet::new().with_resource(update))
    }
}

/// Compute `disc_price` and `charge` for every row in one pass.
///
/// The cost of an indexed `for i in 0..n` loop over `PrimitiveArray::value(i)`
/// is loop shape, not the accessor's bounds `assert!`, which is inlined: LLVM
/// will not prove range-derived indices in bounds, so it will not vectorise.
/// Slices whose lengths it has already related are provably in bounds.
///
/// Two outputs from one input pass rules out a plain `collect`, so this walks
/// `&[f64; 64]` chunks and `extend_from_slice`s two 512-byte stack buffers that
/// never leave L1. Fusing the two outputs moves 40 MB where two separate
/// vectorised passes would move 48 MB.
fn compute_derived(
    price: &[f64],
    disc: &[f64],
    tax: &[f64],
) -> Result<(Vec<f64>, Vec<f64>), SaciError> {
    const LANES: usize = 64;
    let n = price.len();
    if disc.len() != n || tax.len() != n {
        return Err(SaciError::generic("compute columns differ in length"));
    }

    let mut disc_price: Vec<f64> = Vec::with_capacity(n);
    let mut charge: Vec<f64> = Vec::with_capacity(n);

    let (p_chunks, p_tail) = price.as_chunks::<LANES>();
    let (d_chunks, d_tail) = disc.as_chunks::<LANES>();
    let (t_chunks, t_tail) = tax.as_chunks::<LANES>();

    for ((p, d), t) in p_chunks.iter().zip(d_chunks).zip(t_chunks) {
        let mut dp = [0.0f64; LANES];
        let mut ch = [0.0f64; LANES];
        for b in 0..LANES {
            dp[b] = p[b] * (1.0 - d[b]);
            ch[b] = dp[b] * (1.0 + t[b]);
        }
        disc_price.extend_from_slice(&dp);
        charge.extend_from_slice(&ch);
    }
    for ((&p, &d), &t) in p_tail.iter().zip(d_tail).zip(t_tail) {
        let dp = p * (1.0 - d);
        disc_price.push(dp);
        charge.push(dp * (1.0 + t));
    }

    Ok((disc_price, charge))
}

// Stage 2: ComputeStage writes the disc_price and charge columns.
struct ComputeStage;

#[async_trait]
impl ParallelSystem for ComputeStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q1_compute")
            .read("Lineitem", "l_extendedprice")
            .read("Lineitem", "l_discount")
            .read("Lineitem", "l_tax")
            .read_resource::<FilterMask>()
            .write("LineitemDerived", "disc_price")
            .write("LineitemDerived", "charge")
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
        let tax_arr = batch
            .column_by_name("l_tax")
            .ok_or_else(|| SaciError::generic("l_tax not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("l_tax wrong type"))?;

        let (disc_price, charge) =
            compute_derived(price_arr.values(), disc_arr.values(), tax_arr.values())?;

        Ok(WriteSet::new()
            .put(
                "LineitemDerived",
                "disc_price",
                Arc::new(Float64Array::from(disc_price)),
            )
            .put(
                "LineitemDerived",
                "charge",
                Arc::new(Float64Array::from(charge)),
            ))
    }
}

// Stage 3: AggregateStage groups on (returnflag, linestatus) sequentially.
struct AggregateStage;

#[async_trait]
impl System for AggregateStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q1_aggregate")
            .read("Lineitem", "l_quantity")
            .read("Lineitem", "l_extendedprice")
            .read("Lineitem", "l_discount")
            .read("Lineitem", "l_returnflag")
            .read("Lineitem", "l_linestatus")
            .read("LineitemDerived", "disc_price")
            .read("LineitemDerived", "charge")
            .read_resource::<FilterMask>()
            .write_resource::<Q1Result>()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let mask = pipeline
            .get_resource::<FilterMask>()
            .ok_or_else(|| SaciError::generic("FilterMask resource not found"))?;
        let mask_arr = mask.0.clone();

        let li_batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| SaciError::generic("Lineitem not found"))?;
        let ld_batch = pipeline
            .batch_for("LineitemDerived")
            .ok_or_else(|| SaciError::generic("LineitemDerived not found"))?;

        let qty_arr = li_batch
            .column_by_name("l_quantity")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let price_arr = li_batch
            .column_by_name("l_extendedprice")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let disc_arr = li_batch
            .column_by_name("l_discount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let rf_arr = li_batch
            .column_by_name("l_returnflag")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let ls_arr = li_batch
            .column_by_name("l_linestatus")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let dp_arr = ld_batch
            .column_by_name("disc_price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ch_arr = ld_batch
            .column_by_name("charge")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        #[derive(Default, Clone, Copy)]
        struct Acc {
            sum_qty: f64,
            sum_price: f64,
            sum_disc_price: f64,
            sum_charge: f64,
            sum_disc: f64,
            count: u64,
        }

        let n = qty_arr.len();
        let qty_c = qty_arr.values();
        let price_c = price_arr.values();
        let disc_c = disc_arr.values();
        let rf_c = rf_arr.values();
        let ls_c = ls_arr.values();
        let dp_c = dp_arr.values();
        let ch_c = ch_arr.values();

        if mask_arr.null_count() != 0 {
            return Err(SaciError::generic("filter mask must not contain nulls"));
        }
        if mask_arr.len() != n
            || price_c.len() != n
            || disc_c.len() != n
            || rf_c.len() != n
            || ls_c.len() != n
            || dp_c.len() != n
            || ch_c.len() != n
        {
            return Err(SaciError::generic("aggregate columns differ in length"));
        }

        // Six accumulators in a fixed array, indexed arithmetically. The columns
        // are read through zipped `.values()` slices rather than `value(i)`: the
        // scatter into `groups` is data-dependent so this loop cannot vectorise,
        // but the seven column walks stay sequential and lose their per-row
        // accessor calls, including `BooleanArray::value`, the one Arrow accessor
        // that is concrete, non-generic and carries no inline hint.
        //
        // Accumulation runs in row-major order within each group, so the sums are
        // reproducible.
        let mut groups = [Acc::default(); Q1_GROUPS];
        for ((((&rf, &ls), ((&qty, &price), &disc)), (&dp, &ch)), pass) in rf_c
            .iter()
            .zip(ls_c)
            .zip(qty_c.iter().zip(price_c).zip(disc_c))
            .zip(dp_c.iter().zip(ch_c))
            .zip(mask_arr.values().iter())
        {
            if !pass {
                continue;
            }
            let acc = &mut groups[rf as usize * LINESTATUS_VALUES.len() + ls as usize];
            acc.sum_qty += qty;
            acc.sum_price += price;
            acc.sum_disc_price += dp;
            acc.sum_charge += ch;
            acc.sum_disc += disc;
            acc.count += 1;
        }

        // Slot order is already (returnflag, linestatus) ascending, so no sort is
        // needed.
        let mut result: Vec<Q1GroupResult> = Vec::with_capacity(Q1_GROUPS);
        for (slot, acc) in groups.iter().enumerate() {
            if acc.count == 0 {
                continue;
            }
            let count = acc.count as f64;
            result.push(Q1GroupResult {
                returnflag: (slot / LINESTATUS_VALUES.len()) as u8,
                linestatus: (slot % LINESTATUS_VALUES.len()) as u8,
                sum_qty: acc.sum_qty,
                sum_base_price: acc.sum_price,
                sum_disc_price: acc.sum_disc_price,
                sum_charge: acc.sum_charge,
                avg_qty: acc.sum_qty / count,
                avg_price: acc.sum_price / count,
                avg_disc: acc.sum_disc / count,
                count_order: acc.count,
            });
        }

        pipeline.insert_resource(Q1Result(result));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pipeline builder
// ---------------------------------------------------------------------------

fn build_pipeline(batch: &RecordBatch) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_component::<Lineitem>().unwrap();
    pipeline.register_component::<LineitemDerived>().unwrap();

    pipeline
        .append_record_batch("Lineitem", batch.clone())
        .unwrap();

    // Placeholder LineitemDerived: zeros, same row count.
    let n = batch.num_rows();
    let derived_batch = RecordBatch::try_new(
        LineitemDerived::schema(),
        vec![
            Arc::new(Float64Array::from(vec![0.0f64; n])),
            Arc::new(Float64Array::from(vec![0.0f64; n])),
        ],
    )
    .unwrap();
    pipeline
        .append_record_batch("LineitemDerived", derived_batch)
        .unwrap();

    pipeline
}

// ---------------------------------------------------------------------------
// Correctness check
// ---------------------------------------------------------------------------

/// Cross-check the SACI pipeline's grouped output against the scalar reference.
///
/// Group identity and `count_order` must match exactly. Every floating-point
/// aggregate is checked against a relative tolerance and separately counted for
/// bit-equality, so a change that reassociates a summation surfaces instead of
/// hiding inside the tolerance.
fn assert_results_match(scalar: &[Q1GroupResult], arrow: &[Q1GroupResult]) {
    assert_eq!(
        scalar.len(),
        arrow.len(),
        "Q1 result group count mismatch: scalar={} arrow={}",
        scalar.len(),
        arrow.len()
    );
    let mut checked = 0usize;
    let mut bit_identical = 0usize;
    for (s, a) in scalar.iter().zip(arrow.iter()) {
        assert_eq!(
            s.returnflag, a.returnflag,
            "returnflag mismatch: {} vs {}",
            s.returnflag, a.returnflag
        );
        assert_eq!(
            s.linestatus, a.linestatus,
            "linestatus mismatch: {} vs {}",
            s.linestatus, a.linestatus
        );
        assert_eq!(
            s.count_order, a.count_order,
            "count_order mismatch for group ({},{}): scalar={} arrow={}",
            s.returnflag, s.linestatus, s.count_order, a.count_order
        );
        for (name, sv, av) in [
            ("sum_qty", s.sum_qty, a.sum_qty),
            ("sum_base_price", s.sum_base_price, a.sum_base_price),
            ("sum_disc_price", s.sum_disc_price, a.sum_disc_price),
            ("sum_charge", s.sum_charge, a.sum_charge),
            ("avg_qty", s.avg_qty, a.avg_qty),
            ("avg_price", s.avg_price, a.avg_price),
            ("avg_disc", s.avg_disc, a.avg_disc),
        ] {
            let eps = sv.abs() * 1e-9 + 1e-6;
            assert!(
                (sv - av).abs() < eps,
                "{name} mismatch for group ({},{}): scalar={sv:.6} arrow={av:.6}",
                s.returnflag,
                s.linestatus
            );
            checked += 1;
            bit_identical += usize::from(sv.to_bits() == av.to_bits());
        }
    }
    println!(
        "[tpch_q1] cross-check: {} groups, {}/{} f64 aggregates bit-identical \
         to the scalar reference",
        scalar.len(),
        bit_identical,
        checked
    );
    // Print the aggregates with their bit patterns, so a change that reassociates
    // a summation can be diffed against a recorded value.
    for r in scalar {
        println!(
            "[tpch_q1] group ({},{}) count={} sum_charge={:.4} bits={:016x}",
            r.returnflag,
            r.linestatus,
            r.count_order,
            r.sum_charge,
            r.sum_charge.to_bits()
        );
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_q1(c: &mut Criterion) {
    const N: usize = 1_000_000;
    const SEED: u64 = 42;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let batch = generate_lineitem_batch(N, SEED);
    let scalar_rows = extract_scalar_rows(&batch);

    println!("\n[tpch_q1] {} rows, {} CPUs", N, num_cpus::get());

    {
        let scalar_result = scalar_q1(&scalar_rows);
        let mut wl = Pipeline::new("q1_check");
        wl.data = build_pipeline(&batch);
        wl.add_parallel_system(FilterStage);
        wl.add_parallel_system(ComputeStage);
        wl.add_system(AggregateStage);
        rt.block_on(wl.run()).unwrap();
        let arrow_result = wl.data.get_resource::<Q1Result>().unwrap();
        assert_results_match(&scalar_result, &arrow_result.0);
        println!(
            "[tpch_q1] correctness check passed ({} groups)",
            scalar_result.len()
        );
    }

    let mut group = c.benchmark_group("tpch_q1");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(15));

    group.bench_function("scalar_baseline", |b| {
        b.iter(|| {
            let result = scalar_q1(std::hint::black_box(&scalar_rows));
            std::hint::black_box(result)
        })
    });

    // Setup-only control: builds exactly the dataset `saci_pipeline` builds and
    // runs no stage body, so it isolates setup cost from the stage bodies.
    group.bench_function("q1_setup_only", |b| {
        b.iter(|| {
            let ds = build_pipeline(std::hint::black_box(&batch));
            std::hint::black_box(ds.rows())
        })
    });

    // Per-stage decomposition, mirroring `tpch_q6`'s `narrow_stage_*` benches.
    // Datasets are built once, outside the timed region, so these measure the
    // stage bodies alone: no Pipeline, no stage plan, no spawn_blocking and no
    // per-iteration allocation of the inputs.
    {
        let ds_clean = build_pipeline(&batch);

        group.bench_function("q1_stage_filter", |b| {
            b.iter(|| {
                let ws = rt
                    .block_on(FilterStage.run(std::hint::black_box(&ds_clean)))
                    .unwrap();
                std::hint::black_box(ws.resource_updates.len())
            })
        });

        // Filter applied, so compute and aggregate see the same FilterMask
        // resource the real pipeline hands them.
        let mut ds_full = build_pipeline(&batch);
        let ws = rt.block_on(FilterStage.run(&ds_full)).unwrap();
        ds_full.apply_write_set(ws).unwrap();

        group.bench_function("q1_stage_compute", |b| {
            b.iter(|| {
                let ws = rt
                    .block_on(ComputeStage.run(std::hint::black_box(&ds_full)))
                    .unwrap();
                std::hint::black_box(ws.fields.len())
            })
        });

        let ws = rt.block_on(ComputeStage.run(&ds_full)).unwrap();
        ds_full.apply_write_set(ws).unwrap();

        group.bench_function("q1_stage_aggregate", |b| {
            b.iter(|| {
                rt.block_on(AggregateStage.run(std::hint::black_box(&mut ds_full)))
                    .unwrap();
                std::hint::black_box(ds_full.get_resource::<Q1Result>().map(|r| r.0.len()))
            })
        });
    }

    group.bench_function("saci_pipeline", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("q1");
            wl.data = build_pipeline(std::hint::black_box(&batch));
            wl.add_parallel_system(FilterStage);
            wl.add_parallel_system(ComputeStage);
            wl.add_system(AggregateStage);
            rt.block_on(wl.run()).unwrap();
            let result = wl
                .data
                .get_resource::<Q1Result>()
                .map(|r| r.0.len())
                .unwrap_or(0);
            std::hint::black_box(result)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_q1);
criterion_main!(benches);
