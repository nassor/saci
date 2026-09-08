//! Session windowing demo as a WebAssembly processor.
//!
//! The processor of `examples/windowing/session/`: receives the merged rows of
//! both NATS sources (one batch per source per stream item), keeps every open
//! session in its checkpoint state, and emits one `SessionTotal` row per
//! closed `(session, symbol)` group. A session is delimited by silence rather
//! than by a clock: consecutive events of one key belong to the same session
//! while they are no more than `gap_ms` apart, so a session's length is data,
//! not configuration, and it closes once the watermark passes its last event
//! plus `gap_ms`, the instant no further event can join it. The geometry comes
//! from the `window.*` config keys the host injects from the KDL `window`
//! block.
//!
//! `saci_core::windows::assign_sessions` is deliberately not used: it
//! classifies one batch in isolation, while this processor carries open
//! sessions across batches through its checkpoint.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p windowing-session-wasm --target wasm32-wasip2
//! ```
//!
//! The output component lands at
//! `target/wasm32-wasip2/release/windowing_session_wasm.wasm`.

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

/// One row of the demo workload: a sale at an instant in time.
///
/// The same shape the tumbling and sliding demos declare, so all three read
/// the same NDJSON source configuration.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Sale {
    /// Unix timestamp in milliseconds; the session's event time.
    pub timestamp_ms: i64,
    /// Grouping key, e.g. a stock ticker.
    pub symbol: String,
    /// The value summed per (session, symbol).
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

/// One aggregate row the processor emits when a session closes.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SessionTotal {
    /// The session's first event, milliseconds since the epoch.
    pub session_start_ms: i64,
    /// The instant no further event could have joined: last event plus
    /// `gap_ms`.
    pub session_end_ms: i64,
    /// The grouping key.
    pub symbol: String,
    /// Rows merged into the session.
    pub count: i64,
    /// Sum of `amount` over the merged rows.
    pub sum: f64,
}

impl Component for SessionTotal {
    fn name() -> &'static str {
        "SessionTotal"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("session_start_ms", DataType::Int64, false),
            Field::new("session_end_ms", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("sum", DataType::Float64, false),
        ]))
    }
}

/// One open session, carried across batches in the checkpoint.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OpenSession {
    /// The grouping key.
    pub symbol: String,
    /// Earliest event in the session.
    pub start_ms: i64,
    /// Latest event in the session; the session ends `gap_ms` after it.
    pub last_ms: i64,
    /// Rows merged so far.
    pub count: i64,
    /// Running sum.
    pub sum: f64,
}

/// The processor's cross-batch state: the watermark plus every open session.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SessionState {
    /// Highest event timestamp observed so far, milliseconds since the epoch.
    /// Starts at `i64::MIN`, meaning nothing observed yet.
    pub watermark_ms: i64,
    /// Open (not yet closed) sessions, at most one per key per burst.
    pub open: Vec<OpenSession>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            watermark_ms: i64::MIN,
            open: Vec::new(),
        }
    }
}

impl Component for SessionState {
    fn name() -> &'static str {
        "SessionState"
    }
    fn schema() -> Arc<Schema> {
        // Traced from a sample so the nested `open: Vec<OpenSession>` column
        // matches serde_arrow's encoding exactly; a hand-written nested schema
        // would drift from it and fail the state checkpoint round trip.
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = SessionState {
            watermark_ms: 0,
            open: vec![OpenSession {
                symbol: "sample".to_string(),
                start_ms: 0,
                last_ms: 0,
                count: 0,
                sum: 0.0,
            }],
        };
        let fields = Vec::<FieldRef>::from_samples(&[sample], TracingOptions::default())
            .expect("SessionState schema traces from a sample");
        Arc::new(Schema::new(fields))
    }
}

/// The session geometry the host injected from the KDL `window` block.
///
/// A session block declares no `size_ms`, `slide_ms` or `offset_ms`, so this
/// processor reads none of them.
pub struct WindowGeometry {
    /// Silence, in milliseconds, that ends a session.
    pub gap_ms: i64,
    /// Milliseconds past the watermark a late row is still accepted.
    pub allowed_lateness_ms: i64,
}

/// Read the geometry from the injected `window.*` config keys, falling back
/// to the same defaults the KDL block's serde defaults use.
#[cfg(target_arch = "wasm32")]
fn geometry() -> WindowGeometry {
    WindowGeometry {
        gap_ms: saci_config_parse::<i64>("window.gap_ms")
            .and_then(Result::ok)
            .unwrap_or(10_000),
        allowed_lateness_ms: saci_config_parse::<i64>("window.allowed_lateness_ms")
            .and_then(Result::ok)
            .unwrap_or(0),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn geometry() -> WindowGeometry {
    WindowGeometry {
        gap_ms: 10_000,
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

/// Merge into the session at `idx` every other session of the same key it now
/// reaches, so an out-of-order row that lands between two sessions bridges
/// them into one.
///
/// Two sessions join when the silence between them is no longer than
/// `gap_ms`, which is `max(starts) - min(lasts)` for any pair, negative when
/// they overlap. Only an extended session can bridge: a new session is opened
/// exactly when no session of that key was within `gap_ms` of the row.
fn coalesce_from(open: &mut Vec<OpenSession>, idx: usize, gap_ms: i64) {
    let mut idx = idx;
    let mut j = 0;
    while j < open.len() {
        if j == idx {
            j += 1;
            continue;
        }
        let joins = open[j].symbol == open[idx].symbol
            && open[j].start_ms.max(open[idx].start_ms) - open[j].last_ms.min(open[idx].last_ms)
                <= gap_ms;
        if !joins {
            j += 1;
            continue;
        }
        let other = open.remove(j);
        if j < idx {
            idx -= 1;
        }
        let target = &mut open[idx];
        target.start_ms = target.start_ms.min(other.start_ms);
        target.last_ms = target.last_ms.max(other.last_ms);
        target.count += other.count;
        target.sum += other.sum;
        // `j` now indexes whatever shifted into the removed slot, so it is
        // not advanced.
    }
}

/// Merge the batch into the open sessions and emit every session the
/// watermark has left behind, the core of the demo's session logic.
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
        .get_resource_mut::<ProcessorState<SessionState>>()
        .ok_or_else(|| {
            SaciError::generic("windowing: ProcessorState<SessionState> resource missing")
        })?;
    // The SDK serialises the resource as rows of the state component; exactly
    // one row carries the whole SessionState.
    if state.rows.is_empty() {
        state.rows.push(SessionState::default());
    }
    let session_state = &mut state.rows[0];

    // Advance the watermark from the whole batch before classifying any row,
    // exactly like the host does: rows in the same batch as the maximum are
    // on time.
    for &ts in ts_col.values() {
        if ts > session_state.watermark_ms {
            session_state.watermark_ms = ts;
        }
    }

    // Mirror WatermarkState's lateness rule: with no watermark yet, or a
    // lateness budget at least as large as the watermark, nothing is late.
    let threshold = if session_state.watermark_ms == i64::MIN
        || geo.allowed_lateness_ms >= session_state.watermark_ms
    {
        i64::MIN
    } else {
        session_state.watermark_ms - geo.allowed_lateness_ms
    };

    let mut late_rows = 0u64;
    for i in 0..n {
        let ts = ts_col.value(i);
        if ts < threshold {
            late_rows += 1;
            continue;
        }
        let symbol = sym_col.value(i);
        let amount = amt_col.value(i);
        // The row joins the first session of its key it is within `gap_ms`
        // of, in either direction: a row before the session's start extends
        // it backwards.
        let found = session_state.open.iter().position(|s| {
            s.symbol == symbol && ts >= s.start_ms - geo.gap_ms && ts <= s.last_ms + geo.gap_ms
        });
        match found {
            Some(idx) => {
                let session = &mut session_state.open[idx];
                session.start_ms = session.start_ms.min(ts);
                session.last_ms = session.last_ms.max(ts);
                session.count += 1;
                session.sum += amount;
                coalesce_from(&mut session_state.open, idx, geo.gap_ms);
            }
            None => session_state.open.push(OpenSession {
                symbol: symbol.to_string(),
                start_ms: ts,
                last_ms: ts,
                count: 1,
                sum: amount,
            }),
        }
    }

    // Emit every session the watermark has left behind: once it passes
    // `last_ms + gap_ms`, no further event can join.
    let watermark = session_state.watermark_ms;
    let gap = geo.gap_ms;
    let mut emitted: Vec<SessionTotal> = Vec::new();
    session_state.open.retain(|s| {
        let end = s.last_ms + gap;
        if end <= watermark {
            emitted.push(SessionTotal {
                session_start_ms: s.start_ms,
                session_end_ms: end,
                symbol: s.symbol.clone(),
                count: s.count,
                sum: s.sum,
            });
            false
        } else {
            true
        }
    });
    let open_count = session_state.open.len();
    let closed = emitted.len();
    // The borrow through `state` ends here (NLL), before `data.append` below.

    data.append::<SessionTotal>(&emitted)?;
    report_metric("window.open", open_count as f64);
    report_metric("window.closed", closed as f64);
    report_metric("window.late_rows", late_rows as f64);
    Ok(())
}

/// Build the session windowing demo pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export, and constructed exactly once per component instance.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("windowing_session");
    pipeline
        .data
        .register_component::<Sale>()
        .expect("register Sale");
    pipeline
        .data
        .register_component::<SessionTotal>()
        .expect("register SessionTotal");
    pipeline.add_system(system_fn(
        SystemMeta::new("accumulate")
            .read_component("Sale")
            .write_component("SessionTotal"),
        |data| {
            let geo = geometry();
            accumulate_impl(data, &geo)
        },
    ));
    pipeline
}

#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build, state = SessionState);

#[cfg(test)]
mod tests {
    use super::*;
    use saci_processor::__rt::{ProcessorStateSpec, Stateful};

    fn geo() -> WindowGeometry {
        WindowGeometry {
            gap_ms: 10_000,
            allowed_lateness_ms: 5_000,
        }
    }

    fn dataset_with_state() -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<Sale>().unwrap();
        data.register_component::<SessionTotal>().unwrap();
        <Stateful<SessionState> as ProcessorStateSpec>::restore(&mut data, None).unwrap();
        data
    }

    fn sale(ts: i64, symbol: &str, amount: f64) -> Sale {
        Sale {
            timestamp_ms: ts,
            symbol: symbol.to_string(),
            amount,
        }
    }

    /// One emitted `SessionTotal` row: bounds, symbol, count, sum.
    type Total = (i64, i64, String, i64, f64);

    fn totals(data: &Dataset) -> Vec<Total> {
        let batch = data.batch_for("SessionTotal").expect("SessionTotal batch");
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

    /// The open sessions the state carries, as `(symbol, start, last, count)`.
    fn open_sessions(data: &Dataset) -> Vec<(String, i64, i64, i64)> {
        let state = data
            .get_resource::<ProcessorState<SessionState>>()
            .expect("state resource");
        let mut sessions: Vec<(String, i64, i64, i64)> = state.rows[0]
            .open
            .iter()
            .map(|s| (s.symbol.clone(), s.start_ms, s.last_ms, s.count))
            .collect();
        sessions.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        sessions
    }

    /// One workflow item, exactly as the stream runner drives it: a fresh
    /// dataset, the previous batch's checkpoint restored, one batch of rows,
    /// then the next checkpoint. Returns the dataset (for state assertions),
    /// the emitted totals, and the blob the next item resumes from.
    fn run_batch_with(
        geo: &WindowGeometry,
        prior: Option<&[u8]>,
        sales: &[Sale],
    ) -> (Dataset, Vec<Total>, Option<Vec<u8>>) {
        let mut data = dataset_with_state();
        <Stateful<SessionState> as ProcessorStateSpec>::restore(&mut data, prior).unwrap();
        data.append::<Sale>(sales).unwrap();
        accumulate_impl(&mut data, geo).unwrap();
        let emitted = totals(&data);
        let next = <Stateful<SessionState> as ProcessorStateSpec>::capture(&data).unwrap();
        (data, emitted, next)
    }

    fn run_batch(prior: Option<&[u8]>, sales: &[Sale]) -> (Dataset, Vec<Total>, Option<Vec<u8>>) {
        run_batch_with(&geo(), prior, sales)
    }

    #[test]
    fn events_within_the_gap_join_one_session() {
        let (_, emitted, blob) = run_batch(None, &[sale(1_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty());
        let (_, emitted, blob) = run_batch(blob.as_deref(), &[sale(6_000, "AAPL", 5.0)]);
        assert!(emitted.is_empty());
        let (data, emitted, _) = run_batch(blob.as_deref(), &[sale(11_000, "AAPL", 2.0)]);
        assert!(
            emitted.is_empty(),
            "the session ends 10 000 ms after 11 000"
        );
        assert_eq!(
            open_sessions(&data),
            vec![("AAPL".to_string(), 1_000, 11_000, 3)],
            "each arrival is within gap_ms of the last, so all three extend \
             one session"
        );
    }

    #[test]
    fn a_silence_longer_than_the_gap_starts_a_new_session() {
        let (_, emitted, blob) =
            run_batch(None, &[sale(1_000, "AAPL", 10.0), sale(6_000, "AAPL", 5.0)]);
        assert!(emitted.is_empty());

        // 20 000 is 14 000 past the session's last event: beyond the gap, so
        // it opens a second session and closes the first.
        let (data, emitted, _) = run_batch(blob.as_deref(), &[sale(20_000, "AAPL", 7.0)]);
        assert_eq!(
            emitted,
            vec![(1_000, 16_000, "AAPL".to_string(), 2, 15.0)],
            "the closed session ends at its last event plus gap_ms, not at a \
             clock boundary"
        );
        assert_eq!(
            open_sessions(&data),
            vec![("AAPL".to_string(), 20_000, 20_000, 1)],
            "the burst after the silence is its own session"
        );
    }

    #[test]
    fn a_row_exactly_gap_ms_after_the_last_event_joins() {
        let (_, emitted, blob) = run_batch(None, &[sale(1_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty());

        let (data, emitted, _) = run_batch(blob.as_deref(), &[sale(11_000, "AAPL", 5.0)]);
        assert!(emitted.is_empty());
        assert_eq!(
            open_sessions(&data),
            vec![("AAPL".to_string(), 1_000, 11_000, 2)],
            "gap_ms of silence still belongs to the session; only more than \
             gap_ms ends it"
        );

        // One millisecond further and the session is over.
        let (data, emitted, _) = run_batch(blob.as_deref(), &[sale(11_001, "AAPL", 5.0)]);
        assert_eq!(
            emitted,
            vec![(1_000, 11_000, "AAPL".to_string(), 1, 10.0)],
            "11 001 cannot join, and its own arrival closes the session"
        );
        assert_eq!(
            open_sessions(&data),
            vec![("AAPL".to_string(), 11_001, 11_001, 1)]
        );
    }

    #[test]
    fn an_out_of_order_row_merges_two_sessions() {
        // A lateness budget wider than the gap is what makes a bridge
        // reachable: the row has to arrive while both sessions are still
        // open, so it cannot be more than allowed_lateness_ms behind the
        // batch's own maximum.
        let lenient = WindowGeometry {
            gap_ms: 10_000,
            allowed_lateness_ms: 15_000,
        };
        let (data, emitted, _) = run_batch_with(
            &lenient,
            None,
            &[
                sale(1_000, "AAPL", 10.0),
                sale(15_000, "AAPL", 5.0),
                sale(11_000, "AAPL", 2.0),
            ],
        );
        assert!(emitted.is_empty());
        assert_eq!(
            open_sessions(&data),
            vec![("AAPL".to_string(), 1_000, 15_000, 3)],
            "15 000 opened a second session, and 11 000 is within gap_ms of \
             both, so the two become one"
        );
    }

    #[test]
    fn state_round_trips_through_the_checkpoint() {
        let (_, emitted, blob) = run_batch(None, &[sale(1_000, "AAPL", 10.0)]);
        assert!(emitted.is_empty());
        let (_, emitted, blob) = run_batch(blob.as_deref(), &[sale(7_000, "AAPL", 5.0)]);
        assert!(emitted.is_empty());

        // Two checkpoint hops later, the session still carries both rows.
        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(60_000, "GOOG", 1.0)]);
        assert_eq!(
            emitted,
            vec![(1_000, 17_000, "AAPL".to_string(), 2, 15.0)],
            "the open session survived two blobs and closed with both rows"
        );
    }

    #[test]
    fn sessions_are_keyed_by_symbol() {
        let (_, emitted, blob) = run_batch(
            None,
            &[sale(1_000, "AAPL", 10.0), sale(1_000, "MSFT", 20.0)],
        );
        assert!(emitted.is_empty());

        let (_, emitted, _) = run_batch(blob.as_deref(), &[sale(60_000, "GOOG", 1.0)]);
        assert_eq!(
            emitted,
            vec![
                (1_000, 11_000, "AAPL".to_string(), 1, 10.0),
                (1_000, 11_000, "MSFT".to_string(), 1, 20.0),
            ],
            "one session per key, closing independently"
        );
    }
}
