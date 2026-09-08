//! Tumbling windowing demo as a native plugin.
//!
//! The native counterpart of `examples/windowing/tumbling/wasm`: declares the
//! same `Sale` input and `WindowTotal` output components, keeps open windows
//! in its checkpoint state, and emits one `WindowTotal` row per closed
//! `(window_id, symbol)` group when the event-time watermark passes the
//! window's end. The geometry comes from the `window.*` config keys the host
//! injects from the KDL `window` block, so the plugin and the wasm processor
//! run the identical logic with no duplicated configuration.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p windowing-tumbling-plugin
//! ```
//!
//! The artifact name is platform specific:
//! `libwindowing_tumbling_plugin.so` on Linux,
//! `libwindowing_tumbling_plugin.dylib` on macOS,
//! `windowing_tumbling_plugin.dll` on Windows.

#![deny(missing_docs)]

use std::sync::Arc;

use saci_plugin::ProcessorState;
use saci_plugin::arrow_array::{Float64Array, Int64Array, StringArray};
use saci_plugin::arrow_schema::{DataType, Field, FieldRef, Schema};
use saci_plugin::prelude::*;
use saci_plugin::windows::WindowSpec;

/// One row of the demo workload: a sale at an instant in time.
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

/// One aggregate row the plugin emits when a window closes.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct WindowTotal {
    /// Tumbling window id: `floor((ts - offset) / size)`.
    pub window_id: i64,
    /// The grouping key.
    pub symbol: String,
    /// Rows merged into the window group.
    pub count: i64,
    /// Sum of `amount` over the merged rows.
    pub sum: f64,
}

impl Component for WindowTotal {
    fn name() -> &'static str {
        "WindowTotal"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("window_id", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("sum", DataType::Float64, false),
        ]))
    }
}

/// One open window group, carried across batches in the checkpoint.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OpenWindow {
    /// Tumbling window id.
    pub window_id: i64,
    /// The grouping key.
    pub symbol: String,
    /// Rows merged so far.
    pub count: i64,
    /// Running sum.
    pub sum: f64,
}

/// The plugin's cross-batch state: the watermark plus every open group.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct WindowState {
    /// Highest event timestamp observed so far, milliseconds since the epoch.
    /// Starts at `i64::MIN` — "nothing observed yet".
    pub watermark_ms: i64,
    /// Open (not yet closed) window groups.
    pub open: Vec<OpenWindow>,
}

impl Default for WindowState {
    fn default() -> Self {
        Self {
            watermark_ms: i64::MIN,
            open: Vec::new(),
        }
    }
}

impl Component for WindowState {
    fn name() -> &'static str {
        "WindowState"
    }
    fn schema() -> Arc<Schema> {
        // Traced from a sample so the nested `open: Vec<OpenWindow>` column
        // matches serde_arrow's encoding exactly; a hand-written nested schema
        // would drift from it and fail the state checkpoint round trip.
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = WindowState {
            watermark_ms: 0,
            open: vec![OpenWindow {
                window_id: 0,
                symbol: "sample".to_string(),
                count: 0,
                sum: 0.0,
            }],
        };
        let fields = Vec::<FieldRef>::from_samples(&[sample], TracingOptions::default())
            .expect("WindowState schema traces from a sample");
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

/// Read the geometry from the injected `window.*` config keys, falling back
/// to the same defaults the KDL block's serde defaults use.
fn geometry() -> WindowGeometry {
    WindowGeometry {
        size_ms: saci_config_parse::<i64>("window.size_ms")
            .and_then(Result::ok)
            .unwrap_or(30_000),
        offset_ms: saci_config_parse::<i64>("window.offset_ms")
            .and_then(Result::ok)
            .unwrap_or(0),
        allowed_lateness_ms: saci_config_parse::<i64>("window.allowed_lateness_ms")
            .and_then(Result::ok)
            .unwrap_or(0),
    }
}

/// Merge the batch into the open windows and emit every group whose window
/// has closed, the core of the demo's windowing logic.
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
        .get_resource_mut::<ProcessorState<WindowState>>()
        .ok_or_else(|| {
            SaciError::generic("windowing: ProcessorState<WindowState> resource missing")
        })?;
    // The SDK serialises the resource as rows of the state component; exactly
    // one row carries the whole WindowState.
    if state.rows.is_empty() {
        state.rows.push(WindowState::default());
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
        let wid = WindowSpec::assign_tumbling(ts, geo.size_ms, geo.offset_ms);
        let symbol = sym_col.value(i).to_string();
        let amount = amt_col.value(i);
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
                symbol,
                count: 1,
                sum: amount,
            });
        }
    }

    // Emit every group whose window end has passed, Beam's default trigger:
    // `start = wid * size + offset`, `end = start + size`.
    let watermark = window_state.watermark_ms;
    let mut emitted: Vec<WindowTotal> = Vec::new();
    window_state.open.retain(|w| {
        let end = w.window_id * geo.size_ms + geo.offset_ms + geo.size_ms;
        if end <= watermark {
            emitted.push(WindowTotal {
                window_id: w.window_id,
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

    data.append::<WindowTotal>(&emitted)?;
    saci_plugin::host::metric("window.open", open_count as f64);
    saci_plugin::host::metric("window.closed", closed as f64);
    saci_plugin::host::metric("window.late_rows", late_rows as f64);
    Ok(())
}

/// Build the windowing demo pipeline.
///
/// Called lazily by the `export_plugin!` macro on the first `describe` or
/// `run-batch` call, and constructed exactly once per loaded library.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("windowing-tumbling-plugin");
    pipeline
        .data
        .register_component::<Sale>()
        .expect("register Sale");
    pipeline
        .data
        .register_component::<WindowTotal>()
        .expect("register WindowTotal");
    pipeline.add_system(system_fn(
        SystemMeta::new("accumulate")
            .read_component("Sale")
            .write_component("WindowTotal"),
        |data| {
            let geo = geometry();
            accumulate_impl(data, &geo)
        },
    ));
    pipeline
}

saci_plugin::export_plugin!(build, state = WindowState);

#[cfg(test)]
mod tests {
    use super::*;
    use saci_plugin::__rt::{ProcessorStateSpec, Stateful};

    fn geo() -> WindowGeometry {
        WindowGeometry {
            size_ms: 30_000,
            offset_ms: 0,
            allowed_lateness_ms: 5_000,
        }
    }

    fn dataset_with_state() -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<Sale>().unwrap();
        data.register_component::<WindowTotal>().unwrap();
        <Stateful<WindowState> as ProcessorStateSpec>::restore(&mut data, None).unwrap();
        data
    }

    fn sale(ts: i64, symbol: &str, amount: f64) -> Sale {
        Sale {
            timestamp_ms: ts,
            symbol: symbol.to_string(),
            amount,
        }
    }

    /// One emitted `WindowTotal` row: window id, symbol, count, sum.
    type Total = (i64, String, i64, f64);

    fn totals(data: &Dataset) -> Vec<Total> {
        let batch = data.batch_for("WindowTotal").expect("WindowTotal batch");
        let wid = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sym = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let count = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sum = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    wid.value(i),
                    sym.value(i).to_string(),
                    count.value(i),
                    sum.value(i),
                )
            })
            .collect()
    }

    /// One workflow item, exactly as the stream runner drives it: a fresh
    /// dataset, the previous batch's checkpoint restored, one batch of rows,
    /// then the next checkpoint. Returns the dataset (for state assertions),
    /// the emitted totals, and the blob the next item resumes from.
    fn run_batch(prior: Option<&[u8]>, sales: &[Sale]) -> (Dataset, Vec<Total>, Option<Vec<u8>>) {
        let mut data = dataset_with_state();
        <Stateful<WindowState> as ProcessorStateSpec>::restore(&mut data, prior).unwrap();
        data.append::<Sale>(sales).unwrap();
        accumulate_impl(&mut data, &geo()).unwrap();
        let emitted = totals(&data);
        let next = <Stateful<WindowState> as ProcessorStateSpec>::capture(&data).unwrap();
        (data, emitted, next)
    }

    #[test]
    fn no_emission_until_the_watermark_passes_the_window_end() {
        let (_, emitted, _) = run_batch(None, &[sale(1_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty(), "window 0 ends at 30 000 ms");
    }

    #[test]
    fn closed_windows_emit_and_open_ones_stay() {
        let (_, emitted, blob) = run_batch(
            None,
            &[sale(29_000, "AAPL", 10.0), sale(29_500, "AAPL", 7.0)],
        );
        assert!(emitted.is_empty(), "window 0 is still open");

        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(31_000, "AAPL", 5.0)]);
        assert_eq!(
            emitted,
            vec![(0, "AAPL".to_string(), 2, 17.0)],
            "window 0 closes with both rows merged; the window-1 row stays open"
        );
    }

    #[test]
    fn state_round_trips_through_the_checkpoint() {
        let (_, emitted, blob) = run_batch(None, &[sale(1_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty());

        // A fresh dataset resuming from the blob: the open window from the
        // first batch survives, and closes once the next batch advances the
        // watermark past its end.
        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(31_000, "GOOG", 5.0)]);
        assert_eq!(
            emitted,
            vec![(0, "AAPL".to_string(), 1, 10.0)],
            "the open window from the first batch survives the checkpoint"
        );
    }

    #[test]
    fn late_rows_beyond_lateness_are_dropped() {
        let (_, emitted, blob) = run_batch(None, &[sale(100_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty());

        // 90 000 < 100 000 - 5 000: beyond the lateness budget, dropped.
        let (data, emitted, _) = run_batch(blob.as_deref(), &[sale(90_000, "AAPL", 99.0)]);
        assert!(emitted.is_empty());
        let state = data
            .get_resource::<ProcessorState<WindowState>>()
            .expect("state resource");
        assert_eq!(state.rows[0].open.len(), 1, "window 3 stays open");
        assert_eq!(state.rows[0].open[0].count, 1, "the late row was dropped");
        assert_eq!(state.rows[0].open[0].sum, 10.0);
    }

    #[test]
    fn late_rows_within_lateness_reopen_and_refire() {
        let (_, emitted, blob) = run_batch(None, &[sale(29_500, "AAPL", 10.0)]);
        assert!(emitted.is_empty());

        // Window 0 closes once a window-1 row advances the watermark.
        let (_, emitted, blob) = run_batch(blob.as_deref(), &[sale(31_000, "AAPL", 5.0)]);
        assert_eq!(
            emitted,
            vec![(0, "AAPL".to_string(), 1, 10.0)],
            "window 0 closes on the second batch"
        );

        // 29 000 is inside the lateness budget (>= 31 000 - 5 000): the group
        // reopens and refires as a second row.
        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(29_000, "AAPL", 7.0)]);
        assert_eq!(
            emitted,
            vec![(0, "AAPL".to_string(), 1, 7.0)],
            "a late-but-acceptable row refires its window"
        );
    }

    #[test]
    fn windows_are_keyed_by_symbol() {
        let (_, emitted, blob) = run_batch(
            None,
            &[sale(29_500, "AAPL", 10.0), sale(29_500, "MSFT", 20.0)],
        );
        assert!(emitted.is_empty());

        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(31_000, "GOOG", 5.0)]);
        assert_eq!(
            emitted,
            vec![
                (0, "AAPL".to_string(), 1, 10.0),
                (0, "MSFT".to_string(), 1, 20.0),
            ],
            "one group per (window, symbol)"
        );
    }
}
