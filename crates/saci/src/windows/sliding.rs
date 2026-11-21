//! Sliding-window row expansion and the built-in reduce aggregates.
//!
//! [`expand_for_sliding`] duplicates every input row once per overlapping
//! window so the shared keyed-aggregation path can treat sliding windows
//! exactly like tumbling ones. The remaining helpers implement
//! [`ReduceAggregate`] over `Float64` columns and derive the output column
//! names for each aggregate.

use std::sync::Arc;

use arrow_arith::aggregate::{max, min, sum};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::DataType;
use arrow_select::take::take;

use crate::error::SaciError;
use crate::window::sliding_ids;

use super::function::ReduceAggregate;

const SLIDING_MAX_EXPANDED_ROWS: u64 = 100_000_000;

/// Expand `source_batch` so that each row appears `k = ceil(size_ms/slide_ms)` times.
///
/// Returns `(expanded_batch, expanded_time_ms, window_ids)`, each of length
/// `n_rows * k`. The window ID for expanded row `(i, j)` is the `j`th id
/// [`sliding_ids`] yields for `ts[i]`.
///
/// # Errors
///
/// Returns `SaciError::Generic` when `k * n_rows` exceeds the 100_000_000 row
/// amplification cap, or on any Arrow failure.
pub(super) fn expand_for_sliding(
    source_batch: &RecordBatch,
    time_ms: &Int64Array,
    n_rows: usize,
    size_ms: i64,
    slide_ms: i64,
    offset_ms: i64,
) -> Result<(RecordBatch, Int64Array, Int64Array), SaciError> {
    let k = (size_ms + slide_ms - 1) / slide_ms;

    if k as u64 * n_rows as u64 > SLIDING_MAX_EXPANDED_ROWS {
        return Err(SaciError::generic(format!(
            "sliding window amplification too high: k={k} × n_rows={n_rows} \
             exceeds the {SLIDING_MAX_EXPANDED_ROWS} row limit; \
             reduce size_ms/slide_ms ratio or use fewer input rows"
        )));
    }

    // One pass builds the repeat index, the expanded timestamps and the window
    // ids: [0,0,..(k times)..,1,1,...,N-1,..] with ts and id alongside.
    let total = n_rows * k as usize;
    let mut repeat_indices: Vec<u32> = Vec::with_capacity(total);
    let mut expanded_ts: Vec<i64> = Vec::with_capacity(total);
    let mut window_ids: Vec<i64> = Vec::with_capacity(total);
    for (i, &ts) in time_ms.values().iter().enumerate().take(n_rows) {
        for id in sliding_ids(ts, size_ms, slide_ms, offset_ms) {
            repeat_indices.push(i as u32);
            expanded_ts.push(ts);
            window_ids.push(id);
        }
    }
    let repeat_indices = UInt32Array::from(repeat_indices);

    // Expand every column.
    let expanded_batch = {
        let expanded_cols: Result<Vec<ArrayRef>, SaciError> = source_batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &repeat_indices, None)
                    .map_err(|e| SaciError::generic(format!("expand_for_sliding: take error: {e}")))
            })
            .collect();
        RecordBatch::try_new(source_batch.schema(), expanded_cols?)
            .map_err(|e| SaciError::generic(format!("expand_for_sliding: RecordBatch: {e}")))?
    };

    let expanded_time_ms = Int64Array::from(expanded_ts);
    let window_ids = Int64Array::from(window_ids);

    Ok((expanded_batch, expanded_time_ms, window_ids))
}

pub(super) fn aggregate_output_name(aggregate: ReduceAggregate, field: &str) -> String {
    match aggregate {
        ReduceAggregate::Sum => format!("sum_{field}"),
        ReduceAggregate::Min => format!("min_{field}"),
        ReduceAggregate::Max => format!("max_{field}"),
        ReduceAggregate::Count => format!("count_{field}"),
        ReduceAggregate::Mean => format!("mean_{field}"),
    }
}

/// Apply a `ReduceAggregate` to an array slice, returning a single-element `ArrayRef`.
///
/// All aggregates operate on `Float64` columns. Downcast errors return `SaciError::generic`.
pub(super) fn apply_reduce_aggregate(
    aggregate: ReduceAggregate,
    col: &ArrayRef,
) -> Result<ArrayRef, SaciError> {
    let arr = downcast_float64(col)?;
    let result = match aggregate {
        ReduceAggregate::Sum => sum(arr).unwrap_or(0.0),
        ReduceAggregate::Min => min(arr).unwrap_or(f64::INFINITY),
        ReduceAggregate::Max => max(arr).unwrap_or(f64::NEG_INFINITY),
        ReduceAggregate::Count => arr.len() as f64,
        ReduceAggregate::Mean => {
            // Divide by the non-null count so nulls do not bias the mean
            // downward. All-null input gives NaN.
            let non_null = arr.len() - arr.null_count();
            if non_null == 0 {
                f64::NAN
            } else {
                sum(arr).unwrap_or(0.0) / non_null as f64
            }
        }
    };
    Ok(Arc::new(Float64Array::from(vec![result])) as ArrayRef)
}

fn downcast_float64(col: &ArrayRef) -> Result<&Float64Array, SaciError> {
    match col.data_type() {
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SaciError::generic("WindowedSystem: downcast Float64Array failed")),
        other => Err(SaciError::generic(format!(
            "WindowedSystem: aggregate requires Float64 column, got {other:?}; \
             cast the field to Float64 first"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::dataset::Dataset;
    use crate::system::System;
    use crate::window::WindowSpec;
    use crate::windows::system::tests::Trade;

    use super::super::function::WindowFunction;
    use super::super::result::WindowResults;
    use super::super::system::WindowedSystem;

    fn sum_price(spec: WindowSpec) -> WindowedSystem {
        WindowedSystem::new(Trade::TIMESTAMP_MS, spec, WindowFunction::sum("price")).unwrap()
    }

    fn trades(rows: &[(i64, f64)]) -> Dataset {
        let trades: Vec<Trade> = rows
            .iter()
            .map(|&(timestamp_ms, price)| Trade {
                timestamp_ms,
                price,
            })
            .collect();
        let mut pipeline = Dataset::new();
        pipeline.register::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();
        pipeline
    }

    /// size=2000ms, slide=1000ms, so k=2. ts=500 lands in windows [0, -1] and
    /// ts=1500 lands in [1, 0]. Sums per window_id: -1 is 10.0, 0 is 30.0,
    /// 1 is 20.0.
    #[tokio::test]
    async fn test_sliding_window_k2_correct_window_ids_and_sums() {
        let mut pipeline = trades(&[(500, 10.0), (1500, 20.0)]);
        sum_price(WindowSpec::sliding(2000, 1000))
            .run(&mut pipeline)
            .await
            .unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        // 3 distinct window_ids: -1, 0, 1
        assert_eq!(results.on_time.len(), 3, "expected 3 window groups");

        let mut pairs: Vec<(i64, f64)> = results
            .on_time
            .iter()
            .map(|b| {
                let win_id = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                let sum = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0);
                (win_id, sum)
            })
            .collect();
        pairs.sort_by_key(|&(w, _)| w);

        let expected = [(-1, 10.0), (0, 30.0), (1, 20.0)];
        assert_eq!(pairs.len(), expected.len());
        for (&(id, sum), (want_id, want_sum)) in pairs.iter().zip(expected) {
            assert_eq!(id, want_id);
            assert!((sum - want_sum).abs() < 1e-9, "window {id} sum: {sum}");
        }
    }

    /// When k=1 (size == slide), sliding behaves identically to tumbling.
    #[tokio::test]
    async fn test_sliding_equals_tumbling_when_size_eq_slide() {
        let rows = [(100, 10.0), (1100, 20.0)];
        let mut world_sliding = trades(&rows);
        let mut world_tumbling = trades(&rows);

        sum_price(WindowSpec::sliding(1000, 1000))
            .run(&mut world_sliding)
            .await
            .unwrap();
        sum_price(WindowSpec::tumbling(1000))
            .run(&mut world_tumbling)
            .await
            .unwrap();

        let r_sliding = world_sliding.get_resource::<WindowResults>().unwrap();
        let r_tumbling = world_tumbling.get_resource::<WindowResults>().unwrap();
        assert_eq!(r_sliding.on_time.len(), r_tumbling.on_time.len());
        assert_eq!(r_sliding.total_rows(), r_tumbling.total_rows());
    }

    /// Amplification limit: k*N > 100_000_000 returns an error.
    #[tokio::test]
    async fn test_sliding_amplification_limit_returns_error() {
        // 1 row; size=10_000_000_000, slide=1 gives k=10^10, past the limit.
        let mut pipeline = trades(&[(0, 1.0)]);
        let result = sum_price(WindowSpec::sliding(10_000_000_000, 1))
            .run(&mut pipeline)
            .await;
        let msg = result
            .expect_err("expected amplification error, got Ok")
            .to_string();
        assert!(
            msg.contains("amplification"),
            "error should mention amplification, got: {msg}"
        );
    }

    /// Empty pipeline with Sliding spec produces empty results (no panic).
    #[tokio::test]
    async fn test_sliding_empty_world_no_panic() {
        let mut pipeline = Dataset::new();
        pipeline.register::<Trade>().unwrap();

        sum_price(WindowSpec::sliding(2000, 1000))
            .run(&mut pipeline)
            .await
            .unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert!(results.on_time.is_empty());
    }
}
