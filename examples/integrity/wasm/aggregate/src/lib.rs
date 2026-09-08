//! Per-region windowed aggregation as a WebAssembly processor.
//!
//! The second stage of `examples/integrity/`: the `settle` workflow feeds this
//! component two streams that arrive one batch at a time, `ClassifiedOrder`
//! rows bridged in-process from the `ingest` workflow's channel and `Payment`
//! rows off a NATS JetStream consumer. Both carry `event_ms`, so the host can
//! advance one watermark over the merged stream.
//!
//! Rows accumulate into tumbling windows keyed by `(window_id, region)`, and a
//! group is emitted as a `RegionTotal` once the watermark passes the window's
//! end. The geometry comes from the `window.*` config keys the host injects
//! from the KDL `window` block, the same way
//! `examples/windowing/tumbling/wasm` reads them, so the two examples agree
//! on what `window_id` means.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p integrity-aggregate-wasm --target wasm32-wasip2
//! ```
//!
//! The output component lands at
//! `target/wasm32-wasip2/release/integrity_aggregate_wasm.wasm`.

#![deny(missing_docs)]

// The bindings are generated in place from `crates/saci-processor/wit`. The
// module and the `export_pipeline!` invocation below are gated on
// `target_arch = "wasm32"`: the expansion emits canonical ABI intrinsics and the
// `component-type` custom section, neither of which the host target can link.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../../../crates/saci-processor/wit",
        world: "saci-pipeline",
        generate_all,
    });
}

use std::sync::Arc;

use saci_processor::ProcessorState;
use saci_processor::arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use saci_processor::arrow_schema::{DataType, Field, FieldRef, Schema};
use saci_processor::prelude::*;
use saci_processor::windows::WindowSpec;

/// Window size the KDL block declares, used when the host injects no
/// `window.size_ms`.
pub const DEFAULT_SIZE_MS: i64 = 10_000;
/// Lateness budget the KDL block declares, used when the host injects no
/// `window.allowed_lateness_ms`.
pub const DEFAULT_LATENESS_MS: i64 = 2_000;

/// 64-bit FNV-1a over `bytes`, reinterpreted as `i64`.
///
/// Duplicated in each of this example's three processor crates on purpose: a
/// wasm processor component is self-contained, and a shared crate would be a
/// fourth workspace member for twelve lines.
pub fn fnv1a64(bytes: &[u8]) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// Round to two decimals the way the verifier does, so a money total survives
/// the round trip through its own text form bit for bit.
fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// One classified order, as `integrity-classify-wasm` emits it and the channel
/// bridge delivers it. Field for field identical to that crate's own
/// declaration, because the host compares the two schemas at load time.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ClassifiedOrder {
    /// Caller-assigned identity.
    pub order_id: i64,
    /// Catalog key.
    pub sku: String,
    /// Grouping key for the window.
    pub region: String,
    /// `"express"` or `"standard"`.
    pub priority: String,
    /// Units ordered.
    pub qty: i32,
    /// Price of one unit, before tax.
    pub unit_price: f64,
    /// Taxed line value, rounded to two decimals.
    pub line_total: f64,
    /// The catalog's tax flag for this sku.
    pub taxable: bool,
    /// The branch the producing batch routed to.
    pub branch: String,
    /// FNV-1a over the row's canonical text form.
    pub checksum: i64,
    /// Event time in milliseconds since the epoch.
    pub event_ms: i64,
}

impl Component for ClassifiedOrder {
    fn name() -> &'static str {
        "ClassifiedOrder"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("sku", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("priority", DataType::Utf8, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("unit_price", DataType::Float64, false),
            Field::new("line_total", DataType::Float64, false),
            Field::new("taxable", DataType::Boolean, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("checksum", DataType::Int64, false),
            Field::new("event_ms", DataType::Int64, false),
        ]))
    }
}

/// One settlement, as the JetStream consumer decodes it.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Payment {
    /// Caller-assigned identity.
    pub payment_id: i64,
    /// The order this settles.
    pub order_id: i64,
    /// Settled value.
    pub amount: f64,
    /// ISO currency code.
    pub currency: String,
    /// Whether the settlement completed.
    pub settled: bool,
    /// Grouping key for the window; the same vocabulary orders use.
    pub region: String,
    /// Event time in milliseconds since the epoch.
    pub event_ms: i64,
}

impl Component for Payment {
    fn name() -> &'static str {
        "Payment"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("payment_id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("settled", DataType::Boolean, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("event_ms", DataType::Int64, false),
        ]))
    }
}

/// One closed window group: both streams' contribution to one region over one
/// window.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RegionTotal {
    /// Tumbling window id, `event_ms / size_ms`.
    pub window_id: i64,
    /// The grouping key.
    pub region: String,
    /// `ClassifiedOrder` rows merged into the group.
    pub order_count: i64,
    /// `Payment` rows merged into the group.
    pub payment_count: i64,
    /// Sum of `line_total`, rounded to two decimals.
    pub order_amount: f64,
    /// Sum of `amount`, rounded to two decimals.
    pub payment_amount: f64,
    /// FNV-1a over the row's canonical text form.
    pub checksum: i64,
}

impl Component for RegionTotal {
    fn name() -> &'static str {
        "RegionTotal"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("window_id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("order_count", DataType::Int64, false),
            Field::new("payment_count", DataType::Int64, false),
            Field::new("order_amount", DataType::Float64, false),
            Field::new("payment_amount", DataType::Float64, false),
            Field::new("checksum", DataType::Int64, false),
        ]))
    }
}

/// One open window group, carried across batches in the checkpoint.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OpenWindow {
    /// Tumbling window id.
    pub window_id: i64,
    /// The grouping key.
    pub region: String,
    /// Orders merged so far.
    pub order_count: i64,
    /// Payments merged so far.
    pub payment_count: i64,
    /// Running order total.
    pub order_amount: f64,
    /// Running payment total.
    pub payment_amount: f64,
}

/// The processor's cross-batch state: the watermark plus every open group.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AggregateState {
    /// Highest event timestamp observed so far, milliseconds since the epoch.
    /// Starts at `i64::MIN`, meaning "nothing observed yet".
    pub watermark_ms: i64,
    /// Open (not yet closed) window groups.
    pub open: Vec<OpenWindow>,
}

impl Default for AggregateState {
    fn default() -> Self {
        Self {
            watermark_ms: i64::MIN,
            open: Vec::new(),
        }
    }
}

impl Component for AggregateState {
    fn name() -> &'static str {
        "AggregateState"
    }
    fn schema() -> Arc<Schema> {
        // Traced from a sample so the nested `open: Vec<OpenWindow>` column
        // matches serde_arrow's encoding exactly; a hand-written nested schema
        // would drift from it and fail the state checkpoint round trip.
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = AggregateState {
            watermark_ms: 0,
            open: vec![OpenWindow {
                window_id: 0,
                region: "sample".to_string(),
                order_count: 0,
                payment_count: 0,
                order_amount: 0.0,
                payment_amount: 0.0,
            }],
        };
        let fields = Vec::<FieldRef>::from_samples(&[sample], TracingOptions::default())
            .expect("AggregateState schema traces from a sample");
        Arc::new(Schema::new(fields))
    }
}

/// The window geometry the host injected from the KDL `window` block.
pub struct WindowGeometry {
    /// Window size in milliseconds.
    pub size_ms: i64,
    /// Alignment offset in milliseconds.
    pub offset_ms: i64,
    /// Milliseconds past the watermark a late row is still accepted.
    pub allowed_lateness_ms: i64,
}

/// Read the geometry from the injected `window.*` config keys, falling back to
/// what `examples/integrity/integrity.kdl` declares.
#[cfg(target_arch = "wasm32")]
fn geometry() -> WindowGeometry {
    WindowGeometry {
        size_ms: saci_config_parse::<i64>("window.size_ms")
            .and_then(Result::ok)
            .unwrap_or(DEFAULT_SIZE_MS),
        offset_ms: saci_config_parse::<i64>("window.offset_ms")
            .and_then(Result::ok)
            .unwrap_or(0),
        allowed_lateness_ms: saci_config_parse::<i64>("window.allowed_lateness_ms")
            .and_then(Result::ok)
            .unwrap_or(DEFAULT_LATENESS_MS),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn geometry() -> WindowGeometry {
    WindowGeometry {
        size_ms: DEFAULT_SIZE_MS,
        offset_ms: 0,
        allowed_lateness_ms: DEFAULT_LATENESS_MS,
    }
}

/// Report a metric through `host-io::metric`; a no-op outside wasm, where the
/// host bindings do not exist.
#[cfg(target_arch = "wasm32")]
fn report_metric(name: &str, value: f64) {
    crate::bindings::saci::pipeline::host_io::metric(name, value);
}

#[cfg(not(target_arch = "wasm32"))]
fn report_metric(_name: &str, _value: f64) {}

/// The canonical text a [`RegionTotal`]'s checksum is taken over.
///
/// The verifier rebuilds this string from the row it received and hashes it
/// again, so every format specifier here is part of the example's contract.
pub fn region_checksum_input(row: &RegionTotal) -> String {
    format!(
        "{}|{}|{}|{}|{:.2}|{:.2}",
        row.window_id,
        row.region,
        row.order_count,
        row.payment_count,
        row.order_amount,
        row.payment_amount
    )
}

/// Downcast the named column, naming the column on failure.
fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, SaciError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| SaciError::generic(format!("aggregate: column '{name}' missing: {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| SaciError::generic(format!("aggregate: column '{name}' has the wrong type")))
}

/// The open group for `(window_id, region)`, created empty when absent.
fn group_for<'a>(
    open: &'a mut Vec<OpenWindow>,
    window_id: i64,
    region: &str,
) -> &'a mut OpenWindow {
    if let Some(position) = open
        .iter()
        .position(|w| w.window_id == window_id && w.region == region)
    {
        return &mut open[position];
    }
    open.push(OpenWindow {
        window_id,
        region: region.to_string(),
        order_count: 0,
        payment_count: 0,
        order_amount: 0.0,
        payment_amount: 0.0,
    });
    open.last_mut().expect("just pushed")
}

/// Merge both components of the batch into the open windows and emit every
/// group whose window has closed.
pub fn aggregate_impl(data: &mut Dataset, geo: &WindowGeometry) -> Result<(), SaciError> {
    let orders = data
        .columns::<ClassifiedOrder>()
        .ok_or_else(|| SaciError::generic("aggregate: ClassifiedOrder component missing"))?
        .clone();
    let payments = data
        .columns::<Payment>()
        .ok_or_else(|| SaciError::generic("aggregate: Payment component missing"))?
        .clone();

    let order_time = column::<Int64Array>(&orders, "event_ms")?;
    let order_region = column::<StringArray>(&orders, "region")?;
    let order_total = column::<Float64Array>(&orders, "line_total")?;
    let payment_time = column::<Int64Array>(&payments, "event_ms")?;
    let payment_region = column::<StringArray>(&payments, "region")?;
    let payment_amount = column::<Float64Array>(&payments, "amount")?;

    let state = data
        .get_resource_mut::<ProcessorState<AggregateState>>()
        .ok_or_else(|| {
            SaciError::generic("aggregate: ProcessorState<AggregateState> resource missing")
        })?;
    // The SDK serialises the resource as rows of the state component; exactly
    // one row carries the whole state.
    if state.rows.is_empty() {
        state.rows.push(AggregateState::default());
    }
    let window_state = &mut state.rows[0];

    // Advance the watermark from the whole batch, both components, before
    // classifying any row: rows sharing the maximum are on time.
    for &ts in order_time.values().iter().chain(payment_time.values()) {
        if ts > window_state.watermark_ms {
            window_state.watermark_ms = ts;
        }
    }

    // Mirror WatermarkState's lateness rule: with no watermark yet, or a
    // lateness budget at least as large as the watermark, nothing is late.
    let threshold = if window_state.watermark_ms == i64::MIN
        || geo.allowed_lateness_ms >= window_state.watermark_ms
    {
        i64::MIN
    } else {
        window_state.watermark_ms - geo.allowed_lateness_ms
    };

    let mut late_rows = 0u64;
    for i in 0..orders.num_rows() {
        let ts = order_time.value(i);
        if ts < threshold {
            late_rows += 1;
            continue;
        }
        let wid = WindowSpec::assign_tumbling(ts, geo.size_ms, geo.offset_ms);
        let group = group_for(&mut window_state.open, wid, order_region.value(i));
        group.order_count += 1;
        group.order_amount += order_total.value(i);
    }
    for i in 0..payments.num_rows() {
        let ts = payment_time.value(i);
        if ts < threshold {
            late_rows += 1;
            continue;
        }
        let wid = WindowSpec::assign_tumbling(ts, geo.size_ms, geo.offset_ms);
        let group = group_for(&mut window_state.open, wid, payment_region.value(i));
        group.payment_count += 1;
        group.payment_amount += payment_amount.value(i);
    }

    // Emit every group whose window end has passed, Beam's default trigger:
    // `start = wid * size + offset`, `end = start + size`.
    let watermark = window_state.watermark_ms;
    let mut emitted: Vec<RegionTotal> = Vec::new();
    window_state.open.retain(|w| {
        let end = w.window_id * geo.size_ms + geo.offset_ms + geo.size_ms;
        if end > watermark {
            return true;
        }
        let mut row = RegionTotal {
            window_id: w.window_id,
            region: w.region.clone(),
            order_count: w.order_count,
            payment_count: w.payment_count,
            order_amount: round2(w.order_amount),
            payment_amount: round2(w.payment_amount),
            checksum: 0,
        };
        row.checksum = fnv1a64(region_checksum_input(&row).as_bytes());
        emitted.push(row);
        false
    });
    let open_count = window_state.open.len();
    let closed = emitted.len();
    // The borrow through `state` ends here (NLL), before `data.append` below.

    data.append::<RegionTotal>(&emitted)?;
    report_metric("aggregate.open", open_count as f64);
    report_metric("aggregate.closed", closed as f64);
    report_metric("aggregate.late_rows", late_rows as f64);
    Ok(())
}

/// Build the aggregation pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export, and constructed exactly once per component instance.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("integrity-aggregate");
    pipeline
        .data
        .register_component::<ClassifiedOrder>()
        .expect("register ClassifiedOrder");
    pipeline
        .data
        .register_component::<Payment>()
        .expect("register Payment");
    pipeline
        .data
        .register_component::<RegionTotal>()
        .expect("register RegionTotal");
    pipeline.add_system(system_fn(
        SystemMeta::new("aggregate")
            .read_component("ClassifiedOrder")
            .read_component("Payment")
            .write_component("RegionTotal"),
        |data| {
            let geo = geometry();
            aggregate_impl(data, &geo)
        },
    ));
    pipeline
}

#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build, state = AggregateState);

#[cfg(test)]
mod tests {
    use super::*;
    use saci_processor::__rt::{ProcessorStateSpec, Stateful};

    fn geo() -> WindowGeometry {
        WindowGeometry {
            size_ms: DEFAULT_SIZE_MS,
            offset_ms: 0,
            allowed_lateness_ms: DEFAULT_LATENESS_MS,
        }
    }

    fn dataset_with_state() -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<ClassifiedOrder>().unwrap();
        data.register_component::<Payment>().unwrap();
        data.register_component::<RegionTotal>().unwrap();
        <Stateful<AggregateState> as ProcessorStateSpec>::restore(&mut data, None).unwrap();
        data
    }

    fn order(event_ms: i64, region: &str, line_total: f64) -> ClassifiedOrder {
        ClassifiedOrder {
            order_id: event_ms,
            sku: "sku-1".to_string(),
            region: region.to_string(),
            priority: "standard".to_string(),
            qty: 1,
            unit_price: line_total,
            line_total,
            taxable: false,
            branch: "standard".to_string(),
            checksum: 0,
            event_ms,
        }
    }

    fn payment(event_ms: i64, region: &str, amount: f64) -> Payment {
        Payment {
            payment_id: event_ms,
            order_id: event_ms,
            amount,
            currency: "EUR".to_string(),
            settled: true,
            region: region.to_string(),
            event_ms,
        }
    }

    /// One emitted `RegionTotal`: window, region, counts and totals.
    type Total = (i64, String, i64, i64, f64, f64, i64);

    fn totals(data: &Dataset) -> Vec<Total> {
        let batch = data.batch_for("RegionTotal").expect("RegionTotal batch");
        let window_id = column::<Int64Array>(batch, "window_id").unwrap();
        let region = column::<StringArray>(batch, "region").unwrap();
        let order_count = column::<Int64Array>(batch, "order_count").unwrap();
        let payment_count = column::<Int64Array>(batch, "payment_count").unwrap();
        let order_amount = column::<Float64Array>(batch, "order_amount").unwrap();
        let payment_amount = column::<Float64Array>(batch, "payment_amount").unwrap();
        let checksum = column::<Int64Array>(batch, "checksum").unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    window_id.value(i),
                    region.value(i).to_string(),
                    order_count.value(i),
                    payment_count.value(i),
                    order_amount.value(i),
                    payment_amount.value(i),
                    checksum.value(i),
                )
            })
            .collect()
    }

    /// One workflow item, exactly as the stream runner drives it: a fresh
    /// dataset, the previous item's checkpoint restored, one batch from one
    /// source, then the next checkpoint.
    fn run_batch(
        prior: Option<&[u8]>,
        orders: &[ClassifiedOrder],
        payments: &[Payment],
    ) -> (Dataset, Vec<Total>, Option<Vec<u8>>) {
        let mut data = dataset_with_state();
        <Stateful<AggregateState> as ProcessorStateSpec>::restore(&mut data, prior).unwrap();
        data.append::<ClassifiedOrder>(orders).unwrap();
        data.append::<Payment>(payments).unwrap();
        aggregate_impl(&mut data, &geo()).unwrap();
        let emitted = totals(&data);
        let next = <Stateful<AggregateState> as ProcessorStateSpec>::capture(&data).unwrap();
        (data, emitted, next)
    }

    #[test]
    fn no_emission_until_the_watermark_passes_the_window_end() {
        let (_, emitted, _) = run_batch(None, &[order(1_000, "eu", 10.0)], &[]);
        assert!(emitted.is_empty(), "window 0 ends at 10 000 ms");
    }

    #[test]
    fn the_two_streams_merge_into_one_group_across_items() {
        // Item 1: the channel bridge delivers orders alone.
        let (_, emitted, blob) = run_batch(None, &[order(1_000, "eu", 10.0)], &[]);
        assert!(emitted.is_empty());

        // Item 2: JetStream delivers payments alone, into the same window.
        let (_, emitted, blob) = run_batch(blob.as_deref(), &[], &[payment(2_000, "eu", 4.0)]);
        assert!(emitted.is_empty());

        // Item 3: a later order closes window 0 with both contributions.
        let (_, emitted, _) = run_batch(blob.as_deref(), &[order(11_000, "eu", 1.0)], &[]);
        assert_eq!(
            emitted,
            vec![(
                0,
                "eu".to_string(),
                1,
                1,
                10.0,
                4.0,
                fnv1a64(b"0|eu|1|1|10.00|4.00")
            )],
            "one group per (window, region), fed by both streams"
        );
    }

    #[test]
    fn groups_are_keyed_by_region() {
        let (_, emitted, blob) = run_batch(
            None,
            &[order(1_000, "eu", 10.0), order(2_000, "us", 20.0)],
            &[],
        );
        assert!(emitted.is_empty());

        let (_, emitted, _) = run_batch(blob.as_deref(), &[], &[payment(11_000, "eu", 1.0)]);
        let regions: Vec<String> = emitted.iter().map(|t| t.1.clone()).collect();
        assert_eq!(regions, vec!["eu".to_string(), "us".to_string()]);
        assert_eq!(emitted[0].4, 10.0);
        assert_eq!(emitted[1].4, 20.0);
    }

    #[test]
    fn state_survives_the_checkpoint() {
        let (_, _, blob) = run_batch(None, &[order(1_000, "eu", 10.0)], &[]);
        let (data, emitted, _) = run_batch(blob.as_deref(), &[], &[payment(3_000, "eu", 5.0)]);
        assert!(emitted.is_empty());
        let state = data
            .get_resource::<ProcessorState<AggregateState>>()
            .expect("state resource");
        assert_eq!(state.rows[0].open.len(), 1);
        assert_eq!(state.rows[0].open[0].order_count, 1);
        assert_eq!(state.rows[0].open[0].payment_count, 1);
    }

    #[test]
    fn a_row_beyond_the_lateness_budget_is_dropped() {
        let (_, _, blob) = run_batch(None, &[order(100_000, "eu", 10.0)], &[]);
        // 90 000 < 100 000 - 2 000: beyond the budget.
        let (data, emitted, _) = run_batch(blob.as_deref(), &[order(90_000, "eu", 99.0)], &[]);
        assert!(emitted.is_empty());
        let state = data
            .get_resource::<ProcessorState<AggregateState>>()
            .expect("state resource");
        assert_eq!(state.rows[0].open.len(), 1, "window 10 stays open");
        assert_eq!(state.rows[0].open[0].order_count, 1);
    }

    #[test]
    fn a_row_inside_the_lateness_budget_refires_its_window() {
        let (_, _, blob) = run_batch(None, &[order(9_500, "eu", 10.0)], &[]);
        let (_, emitted, blob) = run_batch(blob.as_deref(), &[order(11_000, "eu", 1.0)], &[]);
        assert_eq!(emitted.len(), 1, "window 0 closes");

        // 9 500 >= 11 000 - 2 000: still acceptable, so the group reopens.
        let (_, emitted, _) = run_batch(blob.as_deref(), &[], &[payment(9_500, "eu", 2.0)]);
        assert_eq!(
            emitted,
            vec![(
                0,
                "eu".to_string(),
                0,
                1,
                0.0,
                2.0,
                fnv1a64(b"0|eu|0|1|0.00|2.00")
            )],
            "the reopened group carries only what arrived after the first firing"
        );
    }

    #[test]
    fn a_total_is_rounded_before_it_is_emitted_and_checksummed() {
        // 0.1 + 0.2 is 0.30000000000000004 in binary floating point; the
        // emitted total is the rounded value, so a verifier summing the same
        // rows and rounding the same way lands on the identical bits.
        let (_, emitted, _) = run_batch(
            None,
            &[
                order(9_500, "eu", 0.1),
                order(9_600, "eu", 0.2),
                order(11_000, "eu", 1.0),
            ],
            &[],
        );
        assert_eq!(emitted.len(), 1, "window 0 closes, window 1 stays open");
        assert_eq!(emitted[0].4, 0.3);
        assert_ne!(0.1_f64 + 0.2_f64, 0.3_f64, "the raw sum would not match");
        assert_eq!(emitted[0].6, fnv1a64(b"0|eu|2|0|0.30|0.00"));
    }

    #[test]
    fn fnv1a64_matches_the_reference_vector() {
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8cu64 as i64);
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325u64 as i64);
    }
}
