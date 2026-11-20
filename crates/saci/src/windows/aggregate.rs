//! Group aggregation for [`WindowedSystem`]: watermark filtering, group
//! walking, and per-group reduction.
//!
//! These are the inherent [`WindowedSystem`] methods that turn a sorted
//! `(window_id, key_hash)` ordering into one output `RecordBatch` per window
//! group, plus the schema helpers that describe those batches.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;

use crate::error::SaciError;

use super::function::{ReduceAggregate, WindowContext, WindowFunction};
use super::sliding::{aggregate_output_name, apply_reduce_aggregate};
use super::system::WindowedSystem;
use crate::window::WindowSpec;

/// Parameters for aggregating a single window group slice.
struct SliceParams<'a> {
    schema: &'a Arc<Schema>,
    /// Sorted input column for `Reduce` aggregates; a placeholder for
    /// `Process` functions.
    sorted_col: &'a ArrayRef,
    /// Full sorted source batch, built only for `WindowFunction::Process`.
    sorted_source_batch: Option<&'a RecordBatch>,
    sorted_time_ms: &'a Int64Array,
    win_id: i64,
    key_hash: i64,
    group_start: usize,
    group_end: usize,
    /// Whether this group is a late re-firing of an already-emitted window.
    is_late_firing: bool,
    /// Current watermark at the time of processing.
    watermark: i64,
    /// Window size in milliseconds (used to compute `window_start`/`window_end`).
    window_size_ms: i64,
}

/// `take` every column of `batch` at `indices`.
fn take_batch(
    batch: &RecordBatch,
    indices: &UInt32Array,
    what: &str,
) -> Result<RecordBatch, SaciError> {
    let columns = batch
        .columns()
        .iter()
        .map(|col| {
            take(col.as_ref(), indices, None)
                .map_err(|e| SaciError::generic(format!("{what} take: {e}")))
        })
        .collect::<Result<Vec<ArrayRef>, SaciError>>()?;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| SaciError::generic(format!("{what} RecordBatch: {e}")))
}

impl WindowedSystem {
    /// Advance the watermark from all timestamps in the batch, then partition
    /// rows into:
    ///
    /// - **retained** (on-time + late-but-acceptable): returned as
    ///   `(filtered_batch, filtered_time_ms)`.
    /// - **dropped** (beyond lateness): returned as one batch in the third
    ///   element.
    ///
    /// When no watermark is configured, all rows are retained unchanged.
    pub(super) fn apply_watermark_filter(
        &self,
        batch: &RecordBatch,
        time_ms: &Int64Array,
    ) -> Result<(RecordBatch, Int64Array, Vec<RecordBatch>), SaciError> {
        let Some(wm_lock) = &self.watermark else {
            // No watermark tracking: pass through unchanged.
            return Ok((batch.clone(), time_ms.clone(), Vec::new()));
        };

        // Classify rows against the CURRENT (pre-advance) watermark, then
        // advance. Advancing first would misclassify on-time rows that arrive
        // before the batch's maximum timestamp, so lateness for this batch uses
        // the watermark left by the previous batch.
        let wm = wm_lock.lock().expect("watermark lock poisoned");
        let n = time_ms.len();
        let mut keep_indices: Vec<u32> = Vec::with_capacity(n);
        let mut drop_indices: Vec<u32> = Vec::with_capacity(n);

        for i in 0..n {
            // Null timestamps are already filtered out in `run`. Guard
            // defensively in case this method is called on its own: treat null
            // as "keep", since it cannot advance the watermark.
            if time_ms.is_null(i) {
                keep_indices.push(i as u32);
                continue;
            }
            let ts = time_ms.value(i);
            if wm.is_beyond_lateness(ts) {
                drop_indices.push(i as u32);
            } else {
                keep_indices.push(i as u32);
            }
        }
        drop(wm); // release lock before advancing

        // Advance the watermark from every timestamp in this batch so later
        // runs see the updated high-water mark. `.iter()` rather than
        // `.values()` skips null slots, whose raw backing bits are arbitrary.
        {
            let mut wm = wm_lock.lock().expect("watermark lock poisoned");
            for ts in time_ms.iter().flatten() {
                wm.advance(ts);
            }
        }

        // If nothing is dropped, avoid a copy.
        if drop_indices.is_empty() {
            return Ok((batch.clone(), time_ms.clone(), Vec::new()));
        }

        let keep_arr = UInt32Array::from(keep_indices);
        let filtered_batch = take_batch(batch, &keep_arr, "watermark filter")?;
        let filtered_time = take(time_ms as &dyn Array, &keep_arr, None)
            .map_err(|e| SaciError::generic(format!("watermark filter take time_ms: {e}")))?;
        let filtered_time = filtered_time
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| SaciError::generic("watermark filter downcast time_ms"))?
            .clone();

        let drop_batch = take_batch(batch, &UInt32Array::from(drop_indices), "watermark drop")?;

        Ok((filtered_batch, filtered_time, vec![drop_batch]))
    }

    /// Walk sorted indices and produce one output `RecordBatch` per group.
    ///
    /// Returns `(schema, on_time_batches, late_batches)`.
    /// Groups whose `(window_id, key_hash)` pair has already been emitted are
    /// classified as late re-firings and placed in `late_batches`.
    #[allow(clippy::type_complexity)]
    pub(super) fn aggregate_groups(
        &self,
        source_batch: &RecordBatch,
        sorted_indices: &UInt32Array,
        sorted_win_ids: &Int64Array,
        sorted_key_hash: &Int64Array,
        sorted_time_ms: &Int64Array,
        current_wm: i64,
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>, Vec<RecordBatch>), SaciError> {
        let n = sorted_indices.len();
        let (result_schema, sorted_input_col, sorted_source_batch) =
            self.prepare_aggregate_inputs(source_batch, sorted_indices)?;

        // Compute window size for the context (tumbling/sliding only; session
        // windows use 0 since they have variable size).
        let window_size_ms: i64 = match &self.spec {
            WindowSpec::Tumbling { size_ms, .. } => *size_ms,
            WindowSpec::Sliding { size_ms, .. } => *size_ms,
            WindowSpec::Session { .. } => 0,
        };

        let mut on_time_batches = Vec::new();
        let mut late_batches = Vec::new();
        let mut group_start = 0usize;

        while group_start < n {
            let win_id = sorted_win_ids.value(group_start);
            let key_hash = sorted_key_hash.value(group_start);

            // Find the end of this group (same window_id AND key_hash).
            let mut group_end = group_start + 1;
            while group_end < n
                && sorted_win_ids.value(group_end) == win_id
                && sorted_key_hash.value(group_end) == key_hash
            {
                group_end += 1;
            }

            let group_key = (win_id, key_hash);
            let is_late_firing = {
                let emitted = self
                    .emitted_windows
                    .lock()
                    .expect("emitted_windows poisoned");
                emitted.contains(&group_key)
            };

            let params = SliceParams {
                schema: &result_schema,
                sorted_col: &sorted_input_col,
                sorted_source_batch: sorted_source_batch.as_ref(),
                sorted_time_ms,
                win_id,
                key_hash,
                group_start,
                group_end,
                is_late_firing,
                watermark: current_wm,
                window_size_ms,
            };
            let output_batch = self.aggregate_slice(params)?;

            // Mark this window as emitted for future late-firing detection.
            {
                let mut emitted = self
                    .emitted_windows
                    .lock()
                    .expect("emitted_windows poisoned");
                emitted.insert(group_key);
            }

            if is_late_firing {
                late_batches.push(output_batch);
            } else {
                on_time_batches.push(output_batch);
            }

            group_start = group_end;
        }

        Ok((result_schema, on_time_batches, late_batches))
    }

    /// Build the result schema and extract the (already-sorted) aggregate
    /// input.
    ///
    /// Returns `(result_schema, sorted_input_col, sorted_source_batch)`. For
    /// `Reduce` variants only the target field is gathered into sorted order
    /// and no batch is built. For `Process` variants the whole source batch is
    /// sorted, because that is what the user function receives, and
    /// `sorted_input_col` is a placeholder.
    fn prepare_aggregate_inputs(
        &self,
        source_batch: &RecordBatch,
        sorted_indices: &UInt32Array,
    ) -> Result<(Arc<Schema>, ArrayRef, Option<RecordBatch>), SaciError> {
        match &self.function {
            WindowFunction::Reduce {
                input_field,
                aggregate,
            } => {
                let col_idx = source_batch.schema().index_of(input_field).map_err(|_| {
                    SaciError::generic(format!(
                        "WindowedSystem: input field '{input_field}' not found \
                         in component '{}'",
                        self.source_component
                    ))
                })?;
                let sorted_col = take(source_batch.column(col_idx).as_ref(), sorted_indices, None)
                    .map_err(|e| SaciError::generic(format!("WindowedSystem: take error: {e}")))?;
                let schema =
                    self.reduce_schema(*aggregate, input_field, sorted_col.data_type().clone());
                Ok((schema, sorted_col, None))
            }
            WindowFunction::Process(_) => {
                let sorted_source_batch =
                    take_batch(source_batch, sorted_indices, "WindowedSystem: sorted batch")?;
                // A Process function's output schema comes from the batch it
                // returns, so the schema here is an empty placeholder.
                // `sorted_col` is unused on this path; the first column keeps
                // `SliceParams` holding a valid reference.
                let placeholder_col = sorted_source_batch.column(0).clone();
                Ok((
                    Arc::new(Schema::empty()),
                    placeholder_col,
                    Some(sorted_source_batch),
                ))
            }
        }
    }

    /// The result schema of a `Reduce` aggregate over `input_field`.
    fn reduce_schema(
        &self,
        aggregate: ReduceAggregate,
        input_field: &str,
        value_type: DataType,
    ) -> Arc<Schema> {
        let mut fields = vec![
            Field::new("window_id", DataType::Int64, false),
            Field::new("key_hash", DataType::Int64, false),
            Field::new(
                aggregate_output_name(aggregate, input_field),
                value_type,
                true,
            ),
        ];
        if matches!(&self.spec, WindowSpec::Session { .. }) {
            fields.push(Field::new("session_start_ts", DataType::Int64, false));
            fields.push(Field::new("session_end_ts", DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }

    /// Aggregate rows `[group_start, group_end)` from the sorted input column.
    fn aggregate_slice(&self, p: SliceParams<'_>) -> Result<RecordBatch, SaciError> {
        let window_start = p.win_id * p.window_size_ms;
        let window_end = window_start + p.window_size_ms;
        let ctx = WindowContext {
            window_id: p.win_id,
            window_start,
            window_end,
            is_late_firing: p.is_late_firing,
            watermark: p.watermark,
        };

        match &self.function {
            WindowFunction::Reduce { aggregate, .. } => {
                let slice = p
                    .sorted_col
                    .slice(p.group_start, p.group_end - p.group_start);
                let agg_value = apply_reduce_aggregate(*aggregate, &slice)?;

                let win_id_col: ArrayRef = Arc::new(Int64Array::from(vec![p.win_id]));
                let key_hash_col: ArrayRef = Arc::new(Int64Array::from(vec![p.key_hash]));

                let mut columns = vec![win_id_col, key_hash_col, agg_value];

                if matches!(&self.spec, WindowSpec::Session { .. }) {
                    // Compute min/max ts over this group's sorted time slice.
                    let ts_slice = &p.sorted_time_ms.values()[p.group_start..p.group_end];
                    let start_ts = ts_slice.iter().copied().min().unwrap_or(0);
                    let end_ts = ts_slice.iter().copied().max().unwrap_or(0);
                    columns.push(Arc::new(Int64Array::from(vec![start_ts])) as ArrayRef);
                    columns.push(Arc::new(Int64Array::from(vec![end_ts])) as ArrayRef);
                }

                let _ = ctx; // unused on the Reduce path
                RecordBatch::try_new(p.schema.clone(), columns).map_err(|e| {
                    SaciError::generic(format!("WindowedSystem: RecordBatch error: {e}"))
                })
            }
            WindowFunction::Process(f) => {
                let group_batch = p
                    .sorted_source_batch
                    .expect("prepare_aggregate_inputs sorts the batch for Process")
                    .slice(p.group_start, p.group_end - p.group_start);
                f.process(&ctx, &group_batch)
            }
        }
    }

    /// Schema published when there are no rows to aggregate: the one a
    /// non-empty run of the same system publishes.
    pub(super) fn empty_result_schema(&self) -> Arc<Schema> {
        match &self.function {
            WindowFunction::Reduce {
                input_field,
                aggregate,
            } => self.reduce_schema(*aggregate, input_field, DataType::Float64),
            WindowFunction::Process(_) => Arc::new(Schema::empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::Float64Array;

    use crate::dataset::Dataset;
    use crate::system::System;
    use crate::windows::system::tests::Trade;

    use super::super::result::WindowResults;

    async fn world_with_prices(prices: &[f64]) -> Dataset {
        let base_ts = 100i64;
        let trades: Vec<Trade> = prices
            .iter()
            .enumerate()
            .map(|(i, &price)| Trade {
                timestamp_ms: base_ts + i as i64 * 10,
                price,
            })
            .collect();

        let mut pipeline = Dataset::new();
        pipeline.register::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();
        pipeline
    }

    fn reduce(field: &str, aggregate: ReduceAggregate) -> WindowFunction {
        WindowFunction::Reduce {
            input_field: field.to_string(),
            aggregate,
        }
    }

    async fn run_aggregate(pipeline: &mut Dataset, aggregate: ReduceAggregate) -> f64 {
        let sys = WindowedSystem::new_named(
            "Trade",
            "timestamp_ms",
            WindowSpec::tumbling(10_000),
            reduce("price", aggregate),
        )
        .unwrap();

        sys.run(pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.on_time.len(), 1, "expected single window group");
        results.on_time[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn test_min_aggregate_returns_smallest_value() {
        let mut pipeline = world_with_prices(&[30.0, 10.0, 20.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Min).await;
        assert!(
            (result - 10.0).abs() < 1e-9,
            "min: expected 10.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_min_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[99.5]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Min).await;
        assert!(
            (result - 99.5).abs() < 1e-9,
            "min single row: expected 99.5, got {result}"
        );
    }

    #[tokio::test]
    async fn test_max_aggregate_returns_largest_value() {
        let mut pipeline = world_with_prices(&[30.0, 10.0, 20.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Max).await;
        assert!(
            (result - 30.0).abs() < 1e-9,
            "max: expected 30.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_max_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[7.25]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Max).await;
        assert!(
            (result - 7.25).abs() < 1e-9,
            "max single row: expected 7.25, got {result}"
        );
    }

    #[tokio::test]
    async fn test_count_aggregate_returns_row_count() {
        let mut pipeline = world_with_prices(&[1.0, 2.0, 3.0, 4.0, 5.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Count).await;
        assert!(
            (result - 5.0).abs() < 1e-9,
            "count: expected 5.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_count_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[42.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Count).await;
        assert!(
            (result - 1.0).abs() < 1e-9,
            "count single row: expected 1.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_mean_aggregate_returns_arithmetic_mean() {
        // prices: 10, 20, 30 → mean = 20
        let mut pipeline = world_with_prices(&[10.0, 20.0, 30.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Mean).await;
        assert!(
            (result - 20.0).abs() < 1e-9,
            "mean: expected 20.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_mean_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[55.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Mean).await;
        assert!(
            (result - 55.0).abs() < 1e-9,
            "mean single row: expected 55.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_mean_aggregate_non_uniform_distribution() {
        // prices: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 → mean = 5.5
        let prices: Vec<f64> = (1..=10).map(|i| i as f64).collect();
        let mut pipeline = world_with_prices(&prices).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Mean).await;
        assert!(
            (result - 5.5).abs() < 1e-9,
            "mean: expected 5.5, got {result}"
        );
    }

    /// Runs `Mean` over `prices` (nullable) in one window and returns the
    /// single result.
    async fn mean_of(component: &str, prices: Vec<Option<f64>>) -> f64 {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("price", DataType::Float64, true),
        ]));
        let ts: Int64Array = (0..prices.len() as i64).map(|i| 100 + i * 100).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts) as ArrayRef,
                Arc::new(Float64Array::from(prices)) as ArrayRef,
            ],
        )
        .unwrap();

        let mut pipeline = Dataset::new();
        pipeline.register_schema(component, schema).unwrap();
        pipeline.append_record_batch(component, batch).unwrap();

        let sys = WindowedSystem::new_named(
            component,
            "timestamp_ms",
            WindowSpec::tumbling(1000),
            WindowFunction::mean("price"),
        )
        .unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.on_time.len(), 1, "expected one window group");
        results.on_time[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    /// Mean of [10.0, null, 20.0] must be (10.0 + 20.0) / 2 = 15.0, not
    /// (10.0 + 20.0) / 3 ≈ 10.0.
    #[tokio::test]
    async fn test_mean_excludes_null_values_from_denominator() {
        let mean = mean_of("NullPriceTrade", vec![Some(10.0), None, Some(20.0)]).await;
        assert!(
            (mean - 15.0).abs() < 1e-9,
            "expected mean 15.0 (null excluded), got {mean}"
        );
    }

    /// Mean of all-null values must be NaN (not a divide-by-zero panic).
    #[tokio::test]
    async fn test_mean_all_nulls_returns_nan() {
        let mean = mean_of("AllNullPrice", vec![None, None]).await;
        assert!(
            mean.is_nan(),
            "expected NaN when all values are null, got {mean}"
        );
    }
}
