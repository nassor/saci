//! Stateful event-time tumbling, sliding, and session window aggregation operator.
//!
//! Unlike [`WindowedSystem`](crate::windows::WindowedSystem), which classifies a
//! single batch in isolation, [`WindowAggregateSystem`] carries open windows
//! across batches in [`WindowAggregateState`] (via `State<WindowAggregateState>`
//! or an internal fallback when driven standalone) and emits finalized rows to
//! `target_component` once the event-time watermark passes each window's end.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::component::Component;
use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::sdk::State;
use crate::system::{System, SystemMeta};
use crate::window::{WindowSpec, sliding_ids, tumbling_id};

use super::aggregate::{AggregateSpec, aggregate_output_field, evaluate_aggregate_spec_for_groups};
use super::hash::compute_row_keys;
use super::time::{RecordBatchWrap, batch_to_ipc, concat_non_empty, ipc_to_batch, to_ms_array};

/// Window metadata columns emitted before grouping keys and aggregate outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowColumns {
    /// Optional window bucket ID column name (emitted for tumbling/sliding windows,
    /// or session start timestamp when enabled on sessions). Default: `None`.
    pub window_id_field: Option<String>,
    /// Optional inclusive window start timestamp column name (Int64 ms).
    /// Default: `Some("window_start_ms")`.
    pub window_start_field: Option<String>,
    /// Optional exclusive window end timestamp column name (Int64 ms).
    /// Default: `Some("window_end_ms")`.
    pub window_end_field: Option<String>,
}

impl Default for WindowColumns {
    fn default() -> Self {
        Self {
            window_id_field: None,
            window_start_field: Some("window_start_ms".to_string()),
            window_end_field: Some("window_end_ms".to_string()),
        }
    }
}

/// Checkpointable state for [`WindowAggregateSystem`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowAggregateState {
    /// Highest event-time timestamp observed across batches (ms since epoch).
    pub watermark_ms: i64,
    /// Rows belonging to still-open windows, encoded as an Arrow IPC stream.
    pub buffered_ipc: Vec<u8>,
}

impl Default for WindowAggregateState {
    fn default() -> Self {
        Self {
            watermark_ms: i64::MIN,
            buffered_ipc: Vec::new(),
        }
    }
}

impl Component for WindowAggregateState {
    fn name() -> &'static str {
        "WindowAggregateState"
    }

    fn schema() -> Arc<Schema> {
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = WindowAggregateState {
            watermark_ms: 0,
            buffered_ipc: vec![0],
        };
        let fields =
            Vec::<arrow_schema::FieldRef>::from_samples(&[sample], TracingOptions::default())
                .expect("WindowAggregateState schema traces from sample");
        Arc::new(Schema::new(fields))
    }
}

/// Stateful event-time tumbling, sliding, and session window aggregator.
///
/// Accumulates incoming rows across batches using [`WindowAggregateState`],
/// evicts/emits closed windows once `window_end_ms <= watermark_ms`, and handles
/// late arrivals within `allowed_lateness_ms`.
#[derive(Debug)]
pub struct WindowAggregateSystem {
    source_component: &'static str,
    target_component: &'static str,
    spec: WindowSpec,
    time_field: String,
    key_fields: Vec<String>,
    allowed_lateness_ms: i64,
    aggregates: Vec<AggregateSpec>,
    columns: WindowColumns,
    fallback_state: Mutex<WindowAggregateState>,
}

impl WindowAggregateSystem {
    /// Create a stateful window aggregator over `source_component` writing closed
    /// windows to `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `spec` is invalid, `time_field`
    /// is empty, `aggregates` is empty, or any [`AggregateSpec`] fails validation.
    pub fn new<I, S>(
        source_component: &str,
        target_component: &str,
        time_field: impl Into<String>,
        spec: WindowSpec,
        key_fields: I,
        aggregates: Vec<AggregateSpec>,
    ) -> SaciResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        spec.validate()?;
        let time_field = time_field.into();
        if time_field.is_empty() {
            return Err(SaciError::configuration(
                "WindowAggregateSystem requires a non-empty time_field",
            ));
        }
        if aggregates.is_empty() {
            return Err(SaciError::configuration(
                "WindowAggregateSystem requires at least one AggregateSpec",
            ));
        }
        for agg in &aggregates {
            agg.validate()?;
        }
        Ok(Self {
            source_component: intern_component_name(source_component),
            target_component: intern_component_name(target_component),
            spec,
            time_field,
            key_fields: key_fields
                .into_iter()
                .map(|s| s.as_ref().to_string())
                .collect(),
            allowed_lateness_ms: 0,
            aggregates,
            columns: WindowColumns::default(),
            fallback_state: Mutex::new(WindowAggregateState::default()),
        })
    }

    /// Set the lateness budget in milliseconds beyond which rows behind the
    /// watermark are dropped. Negative values are clamped to `0`.
    pub fn allowed_lateness_ms(mut self, ms: i64) -> Self {
        self.allowed_lateness_ms = ms.max(0);
        self
    }

    /// Configure the window metadata column names emitted in output batches.
    pub fn window_columns(mut self, columns: WindowColumns) -> Self {
        self.columns = columns;
        self
    }

    /// Set or clear the optional window ID output column name.
    pub fn window_id_field(mut self, name: Option<&str>) -> Self {
        self.columns.window_id_field = name.filter(|s| !s.is_empty()).map(ToString::to_string);
        self
    }

    /// Set or clear the window start timestamp output column name.
    pub fn window_start_field(mut self, name: Option<&str>) -> Self {
        self.columns.window_start_field = name.filter(|s| !s.is_empty()).map(ToString::to_string);
        self
    }

    /// Set or clear the window end timestamp output column name.
    pub fn window_end_field(mut self, name: Option<&str>) -> Self {
        self.columns.window_end_field = name.filter(|s| !s.is_empty()).map(ToString::to_string);
        self
    }

    /// Derive the output Arrow [`Schema`] from `input_schema`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] if `time_field`, any `key_fields`, or any aggregate
    /// input field is missing from `input_schema` or has an incompatible type.
    pub fn output_schema(&self, input_schema: &Schema) -> SaciResult<Arc<Schema>> {
        let time_col = input_schema
            .field_with_name(&self.time_field)
            .map_err(|_| SaciError::field_not_found(self.source_component, &self.time_field))?;
        match time_col.data_type() {
            DataType::Int64 | DataType::Timestamp(_, _) => {}
            other => {
                return Err(SaciError::configuration(format!(
                    "WindowAggregateSystem: time_field '{}' must be Int64 or Timestamp, got {other:?}",
                    self.time_field
                )));
            }
        }

        let mut fields = Vec::new();
        if let Some(id_col) = &self.columns.window_id_field {
            fields.push(Field::new(id_col, DataType::Int64, false));
        }
        if let Some(start_col) = &self.columns.window_start_field {
            fields.push(Field::new(start_col, DataType::Int64, false));
        }
        if let Some(end_col) = &self.columns.window_end_field {
            fields.push(Field::new(end_col, DataType::Int64, false));
        }
        for key in &self.key_fields {
            let f = input_schema
                .field_with_name(key)
                .map_err(|_| SaciError::field_not_found(self.source_component, key))?;
            fields.push(f.clone());
        }
        for agg in &self.aggregates {
            fields.push(aggregate_output_field(agg, input_schema)?);
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    fn execute_with_state(
        &self,
        input: &RecordBatch,
        host_watermark_ms: Option<i64>,
        state: &mut WindowAggregateState,
    ) -> SaciResult<RecordBatch> {
        let out_schema = self.output_schema(input.schema_ref())?;
        let prev_watermark_ms = state.watermark_ms;
        let buffered_batch = ipc_to_batch(&state.buffered_ipc)?;

        let time_idx = input
            .schema_ref()
            .index_of(&self.time_field)
            .map_err(|_| SaciError::field_not_found(self.source_component, &self.time_field))?;

        let (valid_input, input_max_ts) = if input.num_rows() > 0 {
            let ts_arr = to_ms_array(input.column(time_idx))?;
            let max_ts = ts_arr.iter().flatten().max();
            if ts_arr.null_count() > 0 {
                let keep: BooleanArray = ts_arr.iter().map(|v| Some(v.is_some())).collect();
                let filtered = filter_record_batch(input, &keep).map_err(|e| {
                    SaciError::generic(format!("WindowAggregateSystem null filter: {e}"))
                })?;
                (filtered, max_ts)
            } else {
                (input.clone(), max_ts)
            }
        } else {
            (RecordBatch::new_empty(input.schema()), None)
        };

        let mut new_watermark_ms = prev_watermark_ms;
        if let Some(ts) = input_max_ts {
            new_watermark_ms = new_watermark_ms.max(ts);
        }
        if let Some(hwm) = host_watermark_ms {
            new_watermark_ms = new_watermark_ms.max(hwm);
        }

        let threshold =
            if prev_watermark_ms == i64::MIN || self.allowed_lateness_ms >= prev_watermark_ms {
                i64::MIN
            } else {
                prev_watermark_ms - self.allowed_lateness_ms
            };

        let new_valid = if valid_input.num_rows() > 0 {
            let ts_arr = to_ms_array(valid_input.column(time_idx))?;
            let on_time_count = ts_arr
                .iter()
                .flatten()
                .filter(|&ts| ts >= threshold)
                .count();
            if on_time_count == valid_input.num_rows() {
                valid_input
            } else if on_time_count == 0 {
                RecordBatch::new_empty(input.schema())
            } else {
                let keep: BooleanArray = ts_arr
                    .iter()
                    .map(|v| Some(v.is_some_and(|t| t >= threshold)))
                    .collect();
                filter_record_batch(&valid_input, &keep).map_err(|e| {
                    SaciError::generic(format!("WindowAggregateSystem lateness filter: {e}"))
                })?
            }
        } else {
            valid_input
        };

        let buffered_len = buffered_batch.as_ref().map_or(0, RecordBatch::num_rows);
        if buffered_len == 0 && new_valid.num_rows() == 0 {
            state.watermark_ms = new_watermark_ms;
            return Ok(RecordBatch::new_empty(out_schema));
        }

        let combined = match buffered_batch {
            Some(buf) if buf.num_rows() > 0 && new_valid.num_rows() > 0 => {
                concat_non_empty(&input.schema(), &[buf, new_valid])?
            }
            Some(buf) if buf.num_rows() > 0 => buf,
            _ => new_valid,
        };

        let combined_ts = to_ms_array(combined.column(time_idx))?;
        let row_keys = if self.key_fields.is_empty() {
            vec![(0_i64, String::new()); combined.num_rows()]
        } else {
            let key_cols: SaciResult<Vec<&ArrayRef>> = self
                .key_fields
                .iter()
                .map(|k| {
                    let idx = combined
                        .schema_ref()
                        .index_of(k)
                        .map_err(|_| SaciError::field_not_found(self.source_component, k))?;
                    Ok(combined.column(idx))
                })
                .collect();
            compute_row_keys(&key_cols?)?
        };

        let mut closed_groups: Vec<ClosedWindowGroup> = Vec::new();
        let mut group_index: HashMap<(i64, String), usize> = HashMap::new();
        let mut open_row_indices: Vec<u32> = Vec::new();

        match &self.spec {
            WindowSpec::Tumbling { size_ms, offset_ms } => {
                for (r, (_h, key)) in row_keys.iter().enumerate() {
                    let ts = combined_ts.value(r);
                    let wid = tumbling_id(ts, *size_ms, *offset_ms);
                    let start_ms = wid * *size_ms + *offset_ms;
                    let end_ms = start_ms + *size_ms;
                    if end_ms <= new_watermark_ms {
                        push_closed_row(
                            &mut closed_groups,
                            &mut group_index,
                            wid,
                            start_ms,
                            end_ms,
                            key.clone(),
                            r as u32,
                        );
                    } else {
                        open_row_indices.push(r as u32);
                    }
                }
            }
            WindowSpec::Sliding {
                size_ms,
                slide_ms,
                offset_ms,
            } => {
                for (r, (_h, key)) in row_keys.iter().enumerate() {
                    let is_buffered = r < buffered_len;
                    let ts = combined_ts.value(r);
                    let mut ids: Vec<i64> =
                        sliding_ids(ts, *size_ms, *slide_ms, *offset_ms).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    let mut has_open = false;
                    for wid in ids {
                        let start_ms = wid * *slide_ms + *offset_ms;
                        let end_ms = start_ms + *size_ms;
                        // Buffered rows already contributed to any window that
                        // closed at `prev_watermark_ms`, so only emit/evaluate
                        // windows that remained open past `prev_watermark_ms`,
                        // plus newly arrived late rows (within lateness).
                        if is_buffered
                            && prev_watermark_ms != i64::MIN
                            && end_ms <= prev_watermark_ms
                        {
                            continue;
                        }
                        if end_ms <= new_watermark_ms {
                            push_closed_row(
                                &mut closed_groups,
                                &mut group_index,
                                wid,
                                start_ms,
                                end_ms,
                                key.clone(),
                                r as u32,
                            );
                        } else {
                            has_open = true;
                        }
                    }
                    if has_open {
                        open_row_indices.push(r as u32);
                    }
                }
            }
            WindowSpec::Session { gap_ms } => {
                let mut key_order: Vec<String> = Vec::new();
                let mut by_key: HashMap<String, Vec<(i64, u32)>> = HashMap::new();
                for (r, (_h, k)) in row_keys.iter().enumerate() {
                    let ts = combined_ts.value(r);
                    if !by_key.contains_key(k) {
                        key_order.push(k.clone());
                    }
                    by_key.entry(k.clone()).or_default().push((ts, r as u32));
                }

                for (key_idx, key) in key_order.into_iter().enumerate() {
                    let mut entries = by_key.remove(&key).unwrap_or_default();
                    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                    let mut i = 0;
                    while i < entries.len() {
                        let session_start = entries[i].0;
                        let mut session_last = entries[i].0;
                        let mut rows = vec![entries[i].1];
                        let mut j = i + 1;
                        while j < entries.len() && entries[j].0 - session_last <= *gap_ms {
                            session_last = session_last.max(entries[j].0);
                            rows.push(entries[j].1);
                            j += 1;
                        }
                        let session_end = session_last + *gap_ms;
                        if session_end <= new_watermark_ms {
                            rows.sort_unstable();
                            closed_groups.push(ClosedWindowGroup {
                                window_id: session_start,
                                start_ms: session_start,
                                end_ms: session_end,
                                order_hint: key_idx,
                                rows,
                            });
                        } else {
                            open_row_indices.extend(rows);
                        }
                        i = j;
                    }
                }
                open_row_indices.sort_unstable();
            }
        }

        state.watermark_ms = new_watermark_ms;
        if open_row_indices.is_empty() {
            state.buffered_ipc.clear();
        } else {
            let take_idx = UInt32Array::from(open_row_indices);
            let cols: SaciResult<Vec<ArrayRef>> = combined
                .columns()
                .iter()
                .map(|c| {
                    take(c.as_ref(), &take_idx, None).map_err(|e| {
                        SaciError::generic(format!("WindowAggregate buffer take: {e}"))
                    })
                })
                .collect();
            let open_batch = RecordBatch::try_new(combined.schema(), cols?)
                .map_err(|e| SaciError::generic(format!("WindowAggregate buffer batch: {e}")))?;
            state.buffered_ipc = batch_to_ipc(&RecordBatchWrap(&open_batch))?;
        }

        if closed_groups.is_empty() {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        closed_groups.sort_by(|a, b| {
            a.start_ms
                .cmp(&b.start_ms)
                .then(a.end_ms.cmp(&b.end_ms))
                .then(a.window_id.cmp(&b.window_id))
                .then(a.order_hint.cmp(&b.order_hint))
        });

        let mut cols: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
        if self.columns.window_id_field.is_some() {
            let arr: Int64Array = closed_groups.iter().map(|g| g.window_id).collect();
            cols.push(Arc::new(arr) as ArrayRef);
        }
        if self.columns.window_start_field.is_some() {
            let arr: Int64Array = closed_groups.iter().map(|g| g.start_ms).collect();
            cols.push(Arc::new(arr) as ArrayRef);
        }
        if self.columns.window_end_field.is_some() {
            let arr: Int64Array = closed_groups.iter().map(|g| g.end_ms).collect();
            cols.push(Arc::new(arr) as ArrayRef);
        }
        if !self.key_fields.is_empty() {
            let first_rows = UInt32Array::from_iter_values(closed_groups.iter().map(|g| g.rows[0]));
            for key in &self.key_fields {
                let col_idx = combined
                    .schema_ref()
                    .index_of(key)
                    .map_err(|_| SaciError::field_not_found(self.source_component, key))?;
                let taken = take(combined.column(col_idx).as_ref(), &first_rows, None)
                    .map_err(|e| SaciError::generic(format!("WindowAggregate key take: {e}")))?;
                cols.push(taken);
            }
        }
        let group_rows: Vec<Vec<u32>> = closed_groups.into_iter().map(|g| g.rows).collect();
        for agg in &self.aggregates {
            cols.push(evaluate_aggregate_spec_for_groups(
                agg,
                &combined,
                &group_rows,
            )?);
        }

        RecordBatch::try_new(out_schema, cols)
            .map_err(|e| SaciError::generic(format!("WindowAggregateSystem output batch: {e}")))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let input = data.batch_named(self.source_component)?.clone();
        #[cfg(feature = "windows")]
        let host_wm = data
            .get_resource::<crate::windows::WindowWatermark>()
            .map(|w| w.0);
        #[cfg(not(feature = "windows"))]
        let host_wm = None;

        let out = if let Some(state_res) = data.get_resource_mut::<State<WindowAggregateState>>() {
            if state_res.rows.is_empty() {
                state_res.rows.push(WindowAggregateState::default());
            }
            self.execute_with_state(&input, host_wm, &mut state_res.rows[0])?
        } else {
            let mut guard = self
                .fallback_state
                .lock()
                .map_err(|_| SaciError::generic("WindowAggregateSystem fallback state poisoned"))?;
            self.execute_with_state(&input, host_wm, &mut guard)?
        };

        data.put_batch_named(self.target_component, out)
    }
}

struct ClosedWindowGroup {
    window_id: i64,
    start_ms: i64,
    end_ms: i64,
    order_hint: usize,
    rows: Vec<u32>,
}

fn push_closed_row(
    closed_groups: &mut Vec<ClosedWindowGroup>,
    group_index: &mut HashMap<(i64, String), usize>,
    wid: i64,
    start_ms: i64,
    end_ms: i64,
    key: String,
    row_idx: u32,
) {
    if let Some(&idx) = group_index.get(&(wid, key.clone())) {
        closed_groups[idx].rows.push(row_idx);
    } else {
        let idx = closed_groups.len();
        group_index.insert((wid, key), idx);
        closed_groups.push(ClosedWindowGroup {
            window_id: wid,
            start_ms,
            end_ms,
            order_hint: idx,
            rows: vec![row_idx],
        });
    }
}

#[async_trait]
impl System for WindowAggregateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("window_aggregate")
            .reads_all_named(self.source_component)
            .writes_all_named(self.target_component)
            .writes_resource::<State<WindowAggregateState>>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type};
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    use crate::pipeline::Pipeline;
    use crate::runtime::PipelineRuntime;

    fn sale_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn sales_batch(rows: &[(i64, &str, f64)]) -> RecordBatch {
        let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let sym: Vec<&str> = rows.iter().map(|r| r.1).collect();
        let amt: Vec<f64> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            sale_schema(),
            vec![
                Arc::new(Int64Array::from(ts)) as ArrayRef,
                Arc::new(StringArray::from(sym)) as ArrayRef,
                Arc::new(Float64Array::from(amt)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn tumbling_window_carries_state_across_batches_and_handles_lateness() {
        let sys = WindowAggregateSystem::new(
            "Sale",
            "WindowTotal",
            "timestamp_ms",
            WindowSpec::tumbling(30_000),
            ["symbol"],
            vec![
                AggregateSpec::count_all().alias("count"),
                AggregateSpec::sum("amount").alias("sum"),
            ],
        )
        .unwrap()
        .allowed_lateness_ms(5_000)
        .window_id_field(Some("window_id"))
        .window_start_field(None)
        .window_end_field(None);

        let out_schema = sys.output_schema(&sale_schema()).unwrap();
        let mut p = Pipeline::new("tumbling").state::<WindowAggregateState>();
        p.data_mut().register_schema("Sale", sale_schema()).unwrap();
        p.data_mut()
            .register_schema("WindowTotal", out_schema)
            .unwrap();
        p = p.system(sys);
        let rt: Box<dyn PipelineRuntime> = Box::new(p);

        // Batch 1: two rows in window 0 ([0, 30_000)) -> nothing emitted yet.
        let mut ds1 = rt.template_dataset();
        ds1.append_record_batch(
            "Sale",
            sales_batch(&[(29_000, "AAPL", 10.0), (29_500, "AAPL", 7.0)]),
        )
        .unwrap();
        let out1 = rt.run_batch(&mut ds1, None).await.unwrap();
        assert_eq!(ds1.batch_named("WindowTotal").unwrap().num_rows(), 0);
        let blob1 = out1.state.expect("checkpoint blob");

        // Batch 2: watermark jumps to 31_000 -> window 0 closes and emits count=2, sum=17.0.
        let mut ds2 = rt.template_dataset();
        ds2.append_record_batch("Sale", sales_batch(&[(31_000, "AAPL", 5.0)]))
            .unwrap();
        let out2 = rt.run_batch(&mut ds2, Some(&blob1)).await.unwrap();
        let b2 = ds2.batch_named("WindowTotal").unwrap();
        assert_eq!(b2.num_rows(), 1);
        assert_eq!(b2.column(0).as_primitive::<Int64Type>().value(0), 0);
        assert_eq!(b2.column(1).as_string::<i32>().value(0), "AAPL");
        assert_eq!(b2.column(2).as_primitive::<Int64Type>().value(0), 2);
        assert!((b2.column(3).as_primitive::<Float64Type>().value(0) - 17.0).abs() < 1e-9);
        let blob2 = out2.state.expect("checkpoint blob");

        // Batch 3: one row inside lateness (29_000 >= 31_000 - 5_000) re-fires window 0,
        // one row beyond lateness (20_000 < 26_000) is dropped.
        let mut ds3 = rt.template_dataset();
        ds3.append_record_batch(
            "Sale",
            sales_batch(&[(29_000, "AAPL", 4.0), (20_000, "AAPL", 99.0)]),
        )
        .unwrap();
        let _out3 = rt.run_batch(&mut ds3, Some(&blob2)).await.unwrap();
        let b3 = ds3.batch_named("WindowTotal").unwrap();
        assert_eq!(b3.num_rows(), 1);
        assert_eq!(b3.column(2).as_primitive::<Int64Type>().value(0), 1);
        assert!((b3.column(3).as_primitive::<Float64Type>().value(0) - 4.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn sliding_window_closes_overlapping_windows_in_order() {
        let sys = WindowAggregateSystem::new(
            "Sale",
            "SlidingTotal",
            "timestamp_ms",
            WindowSpec::sliding(60_000, 15_000),
            ["symbol"],
            vec![
                AggregateSpec::count_all().alias("count"),
                AggregateSpec::sum("amount").alias("sum"),
            ],
        )
        .unwrap();

        let out_schema = sys.output_schema(&sale_schema()).unwrap();
        let mut p = Pipeline::new("sliding").state::<WindowAggregateState>();
        p.data_mut().register_schema("Sale", sale_schema()).unwrap();
        p.data_mut()
            .register_schema("SlidingTotal", out_schema)
            .unwrap();
        p = p.system(sys);
        let rt: Box<dyn PipelineRuntime> = Box::new(p);

        let mut ds1 = rt.template_dataset();
        ds1.append_record_batch("Sale", sales_batch(&[(59_000, "AAPL", 10.0)]))
            .unwrap();
        let out1 = rt.run_batch(&mut ds1, None).await.unwrap();
        assert_eq!(ds1.batch_named("SlidingTotal").unwrap().num_rows(), 0);

        // 61_000 closes [0, 60_000) only; belongs to [15k, 75k)..[45k, 105k)
        let mut ds2 = rt.template_dataset();
        ds2.append_record_batch("Sale", sales_batch(&[(61_000, "AAPL", 7.0)]))
            .unwrap();
        let out2 = rt.run_batch(&mut ds2, out1.state.as_deref()).await.unwrap();
        let b2 = ds2.batch_named("SlidingTotal").unwrap();
        assert_eq!(b2.num_rows(), 1);
        assert_eq!(b2.column(0).as_primitive::<Int64Type>().value(0), 0);
        assert_eq!(b2.column(1).as_primitive::<Int64Type>().value(0), 60_000);
        assert!((b2.column(4).as_primitive::<Float64Type>().value(0) - 10.0).abs() < 1e-9);

        // 76_000 closes [15_000, 75_000) with both 59_000 and 61_000 merged (sum = 17.0)
        let mut ds3 = rt.template_dataset();
        ds3.append_record_batch("Sale", sales_batch(&[(76_000, "GOOG", 1.0)]))
            .unwrap();
        let _out3 = rt.run_batch(&mut ds3, out2.state.as_deref()).await.unwrap();
        let b3 = ds3.batch_named("SlidingTotal").unwrap();
        assert_eq!(b3.num_rows(), 1);
        assert_eq!(b3.column(0).as_primitive::<Int64Type>().value(0), 15_000);
        assert_eq!(b3.column(1).as_primitive::<Int64Type>().value(0), 75_000);
        assert_eq!(b3.column(2).as_string::<i32>().value(0), "AAPL");
        assert_eq!(b3.column(3).as_primitive::<Int64Type>().value(0), 2);
        assert!((b3.column(4).as_primitive::<Float64Type>().value(0) - 17.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn session_window_coalesces_bridges_and_closes_on_silence() {
        let sys = WindowAggregateSystem::new(
            "Sale",
            "SessionTotal",
            "timestamp_ms",
            WindowSpec::session(10_000),
            ["symbol"],
            vec![
                AggregateSpec::count_all().alias("count"),
                AggregateSpec::sum("amount").alias("sum"),
            ],
        )
        .unwrap()
        .allowed_lateness_ms(15_000)
        .window_start_field(Some("session_start_ms"))
        .window_end_field(Some("session_end_ms"));

        let out_schema = sys.output_schema(&sale_schema()).unwrap();
        let mut p = Pipeline::new("session").state::<WindowAggregateState>();
        p.data_mut().register_schema("Sale", sale_schema()).unwrap();
        p.data_mut()
            .register_schema("SessionTotal", out_schema)
            .unwrap();
        p = p.system(sys);
        let rt: Box<dyn PipelineRuntime> = Box::new(p);

        // Batch 1: 1_000 and 15_000 (gap > 10_000), watermark=15_000 (first ends at 11_000? Wait:
        // 15_000 + 15_000? Wait 1_000 + 10_000 = 11_000 <= 15_000 so it closes UNLESS bridge arrives in same batch!
        // With 1_000, 15_000, and 11_000 in batch 1, they bridge into [1_000, 25_000)!
        let mut ds1 = rt.template_dataset();
        ds1.append_record_batch(
            "Sale",
            sales_batch(&[
                (1_000, "AAPL", 10.0),
                (15_000, "AAPL", 5.0),
                (11_000, "AAPL", 2.0),
            ]),
        )
        .unwrap();
        let out1 = rt.run_batch(&mut ds1, None).await.unwrap();
        assert_eq!(ds1.batch_named("SessionTotal").unwrap().num_rows(), 0);

        // Batch 2: GOOG at 30_000 advances watermark past 15_000 + 10_000 = 25_000 -> AAPL session closes!
        let mut ds2 = rt.template_dataset();
        ds2.append_record_batch("Sale", sales_batch(&[(30_000, "GOOG", 1.0)]))
            .unwrap();
        let _out2 = rt.run_batch(&mut ds2, out1.state.as_deref()).await.unwrap();
        let b2 = ds2.batch_named("SessionTotal").unwrap();
        assert_eq!(b2.num_rows(), 1);
        assert_eq!(b2.column(0).as_primitive::<Int64Type>().value(0), 1_000);
        assert_eq!(b2.column(1).as_primitive::<Int64Type>().value(0), 25_000);
        assert_eq!(b2.column(2).as_string::<i32>().value(0), "AAPL");
        assert_eq!(b2.column(3).as_primitive::<Int64Type>().value(0), 3);
        assert!((b2.column(4).as_primitive::<Float64Type>().value(0) - 17.0).abs() < 1e-9);
    }
}
