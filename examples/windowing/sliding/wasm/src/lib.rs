//! Sliding windowing demo as a WebAssembly processor.
//!
//! The processor of `examples/windowing/sliding/`: receives the merged rows of
//! both NATS sources (one batch per source per stream item), keeps every open
//! window in its checkpoint state, and emits one `SlidingTotal` row per closed
//! `(window, symbol)` group. That is Beam's default trigger: a window closes
//! when the event-time watermark passes its end. A sliding window overlaps its
//! neighbours, so one row is counted in `ceil(size_ms / slide_ms)` windows at
//! once and each window's total is a `size_ms`-long moving aggregate advancing
//! every `slide_ms`. The geometry comes from the `window.*` config keys the
//! host injects from the KDL `window` block.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p windowing-sliding-wasm --target wasm32-wasip2
//! ```
//!
//! The output component lands at
//! `target/wasm32-wasip2/release/windowing_sliding_wasm.wasm`.

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
use saci_processor::arrow_array::{Float64Array, Int64Array, StringArray};
use saci_processor::arrow_schema::{DataType, Field, FieldRef, Schema};
use saci_processor::prelude::*;
use saci_processor::windows::WindowSpec;

/// One row of the demo workload: a sale at an instant in time.
///
/// The same shape the tumbling and session demos declare, so all three read
/// the same NDJSON source configuration.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Sale {
    /// Unix timestamp in milliseconds; the window's event time.
    pub timestamp_ms: i64,
    /// Grouping key, e.g. a stock ticker.
    pub symbol: String,
    /// The value summed per (window, symbol).
    pub amount: f64,
}

impl Component for Sale {
    fn name() -> &'static str {
        "Sale"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }
}

/// One aggregate row the processor emits when a sliding window closes.
///
/// The window is reported by its bounds rather than its id: consecutive rows
/// of one symbol are `slide_ms` apart in `window_start_ms` and always
/// `size_ms` long, which is what makes the overlap readable in the sink.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SlidingTotal {
    /// Inclusive start of the window, milliseconds since the epoch.
    pub window_start_ms: i64,
    /// Exclusive end of the window: `window_start_ms + size_ms`.
    pub window_end_ms: i64,
    /// The grouping key.
    pub symbol: String,
    /// Rows merged into the window group.
    pub count: i64,
    /// Sum of `amount` over the merged rows.
    pub sum: f64,
}

impl Component for SlidingTotal {
    fn name() -> &'static str {
        "SlidingTotal"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("window_start_ms", DataType::Int64, false),
            Field::new("window_end_ms", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("sum", DataType::Float64, false),
        ]))
    }
}

/// One open window group, carried across batches in the checkpoint.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OpenWindow {
    /// Sliding window id: the tumbling id of `slide_ms` steps whose window
    /// starts at `window_id * slide_ms + offset_ms`.
    pub window_id: i64,
    /// The grouping key.
    pub symbol: String,
    /// Rows merged so far.
    pub count: i64,
    /// Running sum.
    pub sum: f64,
}

/// The processor's cross-batch state: the watermark plus every open group.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SlidingState {
    /// Highest event timestamp observed so far, milliseconds since the epoch.
    /// Starts at `i64::MIN`, meaning nothing observed yet.
    pub watermark_ms: i64,
    /// Open (not yet closed) window groups. One row lands in `ceil(size_ms /
    /// slide_ms)` of them, so this holds that many groups per active key.
    pub open: Vec<OpenWindow>,
}

impl Default for SlidingState {
    fn default() -> Self {
        Self {
            watermark_ms: i64::MIN,
            open: Vec::new(),
        }
    }
}

impl Component for SlidingState {
    fn name() -> &'static str {
        "SlidingState"
    }
    fn schema() -> Arc<Schema> {
        // Traced from a sample so the nested `open: Vec<OpenWindow>` column
        // matches serde_arrow's encoding exactly; a hand-written nested schema
        // would drift from it and fail the state checkpoint round trip.
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = SlidingState {
            watermark_ms: 0,
            open: vec![OpenWindow {
                window_id: 0,
                symbol: "sample".to_string(),
                count: 0,
                sum: 0.0,
            }],
        };
        let fields = Vec::<FieldRef>::from_samples(&[sample], TracingOptions::default())
            .expect("SlidingState schema traces from a sample");
        Arc::new(Schema::new(fields))
    }
}

/// The window geometry the host injected from the KDL `window` block.
pub struct WindowGeometry {
    /// Window length in milliseconds.
    pub size_ms: i64,
    /// Milliseconds between the starts of two consecutive windows.
    pub slide_ms: i64,
    /// Alignment offset in milliseconds.
    pub offset_ms: i64,
    /// Milliseconds past the watermark a late row is still accepted.
    pub allowed_lateness_ms: i64,
}

/// Read the geometry from the injected `window.*` config keys, falling back
/// to the same defaults the KDL block's serde defaults use.
#[cfg(target_arch = "wasm32")]
fn geometry() -> WindowGeometry {
    WindowGeometry {
        size_ms: saci_config_parse::<i64>("window.size_ms")
            .and_then(Result::ok)
            .unwrap_or(60_000),
        slide_ms: saci_config_parse::<i64>("window.slide_ms")
            .and_then(Result::ok)
            .unwrap_or(15_000),
        offset_ms: saci_config_parse::<i64>("window.offset_ms")
            .and_then(Result::ok)
            .unwrap_or(0),
        allowed_lateness_ms: saci_config_parse::<i64>("window.allowed_lateness_ms")
            .and_then(Result::ok)
            .unwrap_or(0),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn geometry() -> WindowGeometry {
    WindowGeometry {
        size_ms: 60_000,
        slide_ms: 15_000,
        offset_ms: 0,
        allowed_lateness_ms: 0,
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

/// Merge the batch into every window that contains each row and emit the
/// groups whose windows have closed, the core of the demo's sliding logic.
pub fn accumulate_impl(data: &mut Dataset, geo: &WindowGeometry) -> Result<(), SaciError> {
    let batch = data
        .columns::<Sale>()
        .ok_or_else(|| SaciError::generic("windowing: Sale component missing"))?
        .clone();
    let n = batch.num_rows();

    // An empty batch still runs the systems: emit nothing, change nothing.
    if n == 0 {
        return Ok(());
    }

    let schema = batch.schema();
    let ts_idx = schema
        .index_of("timestamp_ms")
        .map_err(|e| SaciError::generic(format!("windowing: timestamp_ms missing: {e}")))?;
    let sym_idx = schema
        .index_of("symbol")
        .map_err(|e| SaciError::generic(format!("windowing: symbol missing: {e}")))?;
    let amt_idx = schema
        .index_of("amount")
        .map_err(|e| SaciError::generic(format!("windowing: amount missing: {e}")))?;
    let ts_col = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| SaciError::generic("windowing: timestamp_ms is not Int64"))?;
    let sym_col = batch
        .column(sym_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SaciError::generic("windowing: symbol is not a string"))?;
    let amt_col = batch
        .column(amt_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| SaciError::generic("windowing: amount is not Float64"))?;

    let state = data
        .get_resource_mut::<ProcessorState<SlidingState>>()
        .ok_or_else(|| {
            SaciError::generic("windowing: ProcessorState<SlidingState> resource missing")
        })?;
    // The SDK serialises the resource as rows of the state component; exactly
    // one row carries the whole SlidingState.
    if state.rows.is_empty() {
        state.rows.push(SlidingState::default());
    }
    let window_state = &mut state.rows[0];

    // Advance the watermark from the whole batch before classifying any row,
    // exactly like the host does: rows in the same batch as the maximum are
    // on time.
    for &ts in ts_col.values() {
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
    for i in 0..n {
        let ts = ts_col.value(i);
        if ts < threshold {
            late_rows += 1;
            continue;
        }
        // `assign_sliding` returns `ceil(size_ms / slide_ms)` ids, with
        // duplicates when `size_ms` is not a multiple of `slide_ms`. The
        // dedup is load bearing: a duplicate id would merge the row twice.
        let mut ids = WindowSpec::assign_sliding(ts, geo.size_ms, geo.slide_ms, geo.offset_ms);
        ids.sort_unstable();
        ids.dedup();

        let symbol = sym_col.value(i);
        let amount = amt_col.value(i);
        for wid in ids {
            if let Some(open) = window_state
                .open
                .iter_mut()
                .find(|w| w.window_id == wid && w.symbol == symbol)
            {
                open.count += 1;
                open.sum += amount;
            } else {
                window_state.open.push(OpenWindow {
                    window_id: wid,
                    symbol: symbol.to_string(),
                    count: 1,
                    sum: amount,
                });
            }
        }
    }

    // Emit every group whose window end has passed, Beam's default trigger:
    // `start = wid * slide + offset`, `end = start + size`. Windows overlap,
    // so several of one key's groups can close on the same batch, oldest
    // first.
    let watermark = window_state.watermark_ms;
    let mut emitted: Vec<SlidingTotal> = Vec::new();
    window_state.open.retain(|w| {
        let start = w.window_id * geo.slide_ms + geo.offset_ms;
        let end = start + geo.size_ms;
        if end <= watermark {
            emitted.push(SlidingTotal {
                window_start_ms: start,
                window_end_ms: end,
                symbol: w.symbol.clone(),
                count: w.count,
                sum: w.sum,
            });
            false
        } else {
            true
        }
    });
    let open_count = window_state.open.len();
    let closed = emitted.len();
    // The borrow through `state` ends here (NLL), before `data.append` below.

    data.append::<SlidingTotal>(&emitted)?;
    report_metric("window.open", open_count as f64);
    report_metric("window.closed", closed as f64);
    report_metric("window.late_rows", late_rows as f64);
    Ok(())
}

/// Build the sliding windowing demo pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export, and constructed exactly once per component instance.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("windowing_sliding");
    pipeline
        .data
        .register_component::<Sale>()
        .expect("register Sale");
    pipeline
        .data
        .register_component::<SlidingTotal>()
        .expect("register SlidingTotal");
    pipeline.add_system(system_fn(
        SystemMeta::new("accumulate")
            .read_component("Sale")
            .write_component("SlidingTotal"),
        |data| {
            let geo = geometry();
            accumulate_impl(data, &geo)
        },
    ));
    pipeline
}

#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build, state = SlidingState);

#[cfg(test)]
mod tests {
    use super::*;
    use saci_processor::__rt::{ProcessorStateSpec, Stateful};

    fn geo() -> WindowGeometry {
        WindowGeometry {
            size_ms: 60_000,
            slide_ms: 15_000,
            offset_ms: 0,
            allowed_lateness_ms: 5_000,
        }
    }

    fn dataset_with_state() -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<Sale>().unwrap();
        data.register_component::<SlidingTotal>().unwrap();
        <Stateful<SlidingState> as ProcessorStateSpec>::restore(&mut data, None).unwrap();
        data
    }

    fn sale(ts: i64, symbol: &str, amount: f64) -> Sale {
        Sale {
            timestamp_ms: ts,
            symbol: symbol.to_string(),
            amount,
        }
    }

    /// One emitted `SlidingTotal` row: bounds, symbol, count, sum.
    type Total = (i64, i64, String, i64, f64);

    fn totals(data: &Dataset) -> Vec<Total> {
        let batch = data.batch_for("SlidingTotal").expect("SlidingTotal batch");
        let start = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let end = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sym = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let count = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sum = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    start.value(i),
                    end.value(i),
                    sym.value(i).to_string(),
                    count.value(i),
                    sum.value(i),
                )
            })
            .collect()
    }

    /// The open groups the state carries, as `(window_id, symbol, count)`.
    fn open_groups(data: &Dataset) -> Vec<(i64, String, i64)> {
        let state = data
            .get_resource::<ProcessorState<SlidingState>>()
            .expect("state resource");
        let mut groups: Vec<(i64, String, i64)> = state.rows[0]
            .open
            .iter()
            .map(|w| (w.window_id, w.symbol.clone(), w.count))
            .collect();
        groups.sort();
        groups
    }

    /// One workflow item, exactly as the stream runner drives it: a fresh
    /// dataset, the previous batch's checkpoint restored, one batch of rows,
    /// then the next checkpoint. Returns the dataset (for state assertions),
    /// the emitted totals, and the blob the next item resumes from.
    fn run_batch(prior: Option<&[u8]>, sales: &[Sale]) -> (Dataset, Vec<Total>, Option<Vec<u8>>) {
        let mut data = dataset_with_state();
        <Stateful<SlidingState> as ProcessorStateSpec>::restore(&mut data, prior).unwrap();
        data.append::<Sale>(sales).unwrap();
        accumulate_impl(&mut data, &geo()).unwrap();
        let emitted = totals(&data);
        let next = <Stateful<SlidingState> as ProcessorStateSpec>::capture(&data).unwrap();
        (data, emitted, next)
    }

    #[test]
    fn a_row_opens_every_window_that_contains_it() {
        let (data, emitted, _) = run_batch(None, &[sale(59_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty(), "the earliest window ends at 60 000 ms");
        assert_eq!(
            open_groups(&data),
            vec![
                (0, "AAPL".to_string(), 1),
                (1, "AAPL".to_string(), 1),
                (2, "AAPL".to_string(), 1),
                (3, "AAPL".to_string(), 1),
            ],
            "one row lands in ceil(60 000 / 15 000) = 4 overlapping windows"
        );
    }

    #[test]
    fn windows_close_oldest_first() {
        let (_, emitted, blob) = run_batch(None, &[sale(59_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty());

        // 61 000 advances the watermark past window [0, 60 000) only; it
        // belongs to windows 1 through 4, so window 1 now holds both rows.
        let (data, emitted, _) = run_batch(blob.as_deref(), &[sale(61_000, "AAPL", 7.0)]);
        assert_eq!(
            emitted,
            vec![(0, 60_000, "AAPL".to_string(), 1, 10.0)],
            "only the window whose end the watermark passed closes"
        );
        assert_eq!(
            open_groups(&data),
            vec![
                (1, "AAPL".to_string(), 2),
                (2, "AAPL".to_string(), 2),
                (3, "AAPL".to_string(), 2),
                (4, "AAPL".to_string(), 1),
            ],
            "the windows [15 000, 75 000) through [45 000, 105 000) carry both \
             rows: the overlap is what a sliding window is for"
        );
    }

    #[test]
    fn state_round_trips_through_the_checkpoint() {
        let (_, emitted, blob) = run_batch(None, &[sale(1_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty());

        // A fresh dataset resuming from the blob: the open windows from the
        // first batch survive, and the oldest closes once the next batch
        // advances the watermark past its end.
        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(61_000, "GOOG", 5.0)]);
        assert_eq!(
            emitted,
            vec![
                (-45_000, 15_000, "AAPL".to_string(), 1, 10.0),
                (-30_000, 30_000, "AAPL".to_string(), 1, 10.0),
                (-15_000, 45_000, "AAPL".to_string(), 1, 10.0),
                (0, 60_000, "AAPL".to_string(), 1, 10.0),
            ],
            "every window holding the first batch's row closes, and the row \
             survived the checkpoint to be counted in each"
        );
    }

    #[test]
    fn late_rows_beyond_lateness_are_dropped() {
        let (_, emitted, blob) = run_batch(None, &[sale(200_000, "AAPL", 10.0)]);
        assert!(
            emitted.is_empty(),
            "window [150 000, 210 000) is still open"
        );

        // 100 000 < 200 000 - 5 000: beyond the lateness budget, dropped.
        let (data, emitted, _) = run_batch(blob.as_deref(), &[sale(100_000, "AAPL", 99.0)]);
        assert!(emitted.is_empty());
        assert_eq!(
            open_groups(&data),
            vec![
                (10, "AAPL".to_string(), 1),
                (11, "AAPL".to_string(), 1),
                (12, "AAPL".to_string(), 1),
                (13, "AAPL".to_string(), 1),
            ],
            "the late row joined no window"
        );
    }

    #[test]
    fn windows_are_keyed_by_symbol() {
        let (_, emitted, blob) = run_batch(
            None,
            &[sale(59_000, "AAPL", 10.0), sale(59_000, "MSFT", 20.0)],
        );
        assert!(emitted.is_empty());

        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(61_000, "GOOG", 5.0)]);
        assert_eq!(
            emitted,
            vec![
                (0, 60_000, "AAPL".to_string(), 1, 10.0),
                (0, 60_000, "MSFT".to_string(), 1, 20.0),
            ],
            "one group per (window, symbol)"
        );
    }
}
