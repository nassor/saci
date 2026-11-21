//! [`WindowedSystem`]: the core windowed aggregation system.
//!
//! Builds on [`WindowSpec`], [`WindowFunction`] and the key-hash helpers to
//! assign window IDs, sort rows into groups, and aggregate each group into a
//! result [`RecordBatch`] stored as a [`WindowResults`] resource.
//!
//! Group aggregation lives in the sibling `aggregate` module, sliding-window
//! expansion and reduce aggregates in `sliding`, and the distributed
//! accumulator flush in `accumulator`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_select::take::take;
use async_trait::async_trait;

use crate::component::Component;
use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::system::{FieldRef, System, SystemMeta};
use crate::window::{WindowSpec, tumbling_id};

use super::function::WindowFunction;
use super::hash::{compute_global_hash, compute_key_hash};
use super::result::WindowResults;
use super::session::assign_sessions;
use super::sliding::expand_for_sliding;
use super::time::to_ms_array;
use super::watermark::WatermarkState;

/// A pipeline system that performs windowed aggregation over a source component.
///
/// ```rust
/// use saci::windows::{WindowFunction, WindowedSystem};
/// use saci::window::WindowSpec;
///
/// let system = WindowedSystem::new_named(
///     "Trade",
///     "timestamp_ms",
///     WindowSpec::tumbling(60_000),
///     WindowFunction::sum("price"),
/// )
/// .unwrap()
/// .key_by(["symbol"])
/// .allowed_lateness_ms(5_000);
/// ```
///
/// On each [`run`](System::run) invocation the system:
///
/// 1. Extracts the time column from the source component and converts it to
///    milliseconds.
/// 2. Advances the internal [`WatermarkState`] from the observed timestamps.
/// 3. Moves rows beyond the allowed-lateness budget to
///    [`WindowResults::dropped`].
/// 4. Assigns a window-bucket ID to every remaining row.
/// 5. Computes a per-row key hash over the configured key fields (or a global
///    all-zero hash for non-keyed windows).
/// 6. Sorts rows by `(window_id, key_hash)` and walks adjacent equal pairs to
///    form groups.
/// 7. Applies the configured [`WindowFunction`] to each group, marking groups
///    as late-firing when the window has already been emitted.
/// 8. Inserts the [`WindowResults`] resource into the dataset for downstream
///    systems to consume.
pub struct WindowedSystem {
    pub(super) source_component: &'static str,
    pub(super) time_field: String,
    pub(super) key_fields: Vec<String>,
    pub(super) spec: WindowSpec,
    pub(super) function: WindowFunction,
    pub(super) meta: SystemMeta,
    /// Watermark state: updated on every run from observed event timestamps.
    ///
    /// `None` when watermark tracking is not configured.
    pub(super) watermark: Option<Mutex<WatermarkState>>,
    /// Set of `(window_id, key_hash)` pairs that have already been emitted at
    /// least once. Used to detect late re-firings.
    pub(super) emitted_windows: Mutex<HashSet<(i64, i64)>>,
}

// `WindowFunction` holds a `Box<dyn ProcessWindowFn>` and `Mutex` fields, hence manual `Debug`.
impl std::fmt::Debug for WindowedSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedSystem")
            .field("source_component", &self.source_component)
            .field("time_field", &self.time_field)
            .field("key_fields", &self.key_fields)
            .field("spec", &self.spec)
            .field("has_watermark", &self.watermark.is_some())
            .finish_non_exhaustive()
    }
}

impl WindowedSystem {
    /// Window component `C` on its `time_field`.
    ///
    /// The time field must be `Int64` milliseconds or a `Timestamp` of any
    /// unit. The windows are global until [`key_by`](Self::key_by) names key
    /// fields.
    ///
    /// # Errors
    ///
    /// [`SaciError::Configuration`] when `spec` is not a sane geometry
    /// ([`WindowSpec::validate`]).
    pub fn new<C: Component>(
        time_field: FieldRef<C>,
        spec: WindowSpec,
        function: WindowFunction,
    ) -> SaciResult<Self> {
        Self::new_named(C::name(), time_field.name(), spec, function)
    }

    /// Window the component named `component` on its `time_field`, for a
    /// component whose schema exists only at runtime.
    ///
    /// # Errors
    ///
    /// [`SaciError::Configuration`] when `spec` is not a sane geometry
    /// ([`WindowSpec::validate`]).
    pub fn new_named(
        component: &str,
        time_field: &str,
        spec: WindowSpec,
        function: WindowFunction,
    ) -> SaciResult<Self> {
        spec.validate()?;
        let source_component = intern_component_name(component);
        Ok(Self {
            source_component,
            time_field: time_field.to_string(),
            key_fields: Vec::new(),
            spec,
            function,
            meta: SystemMeta::new("windowed")
                .reads_all_named(source_component)
                .writes_resource::<WindowResults>(),
            watermark: None,
            emitted_windows: Mutex::new(HashSet::new()),
        })
    }

    /// Group each window by the values of `fields` (one window per distinct
    /// key). No fields means one global window per bucket.
    pub fn key_by<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.key_fields = fields.into_iter().map(|f| f.as_ref().to_string()).collect();
        self
    }

    /// Track a watermark from observed event timestamps and route each row by
    /// it: `ts >= watermark` is processed normally,
    /// `watermark - ms <= ts < watermark` re-fires its window into
    /// [`WindowResults::late`], and anything earlier lands in
    /// [`WindowResults::dropped`]. `0` drops all out-of-order data.
    pub fn allowed_lateness_ms(mut self, ms: i64) -> Self {
        self.watermark = Some(Mutex::new(WatermarkState::new(ms)));
        self
    }

    /// The key columns of `batch`, in declaration order.
    fn key_columns(&self, batch: &RecordBatch) -> SaciResult<Vec<ArrayRef>> {
        self.key_fields
            .iter()
            .map(|field| {
                let idx = batch.schema().index_of(field).map_err(|_| {
                    SaciError::generic(format!(
                        "WindowedSystem: key field '{field}' not found in component '{}'",
                        self.source_component
                    ))
                })?;
                Ok(batch.column(idx).clone())
            })
            .collect()
    }

    /// The per-row key hash of `batch`: all zero for a global window.
    fn key_hash(&self, batch: &RecordBatch) -> SaciResult<Int64Array> {
        if self.key_fields.is_empty() {
            return Ok(compute_global_hash(batch.num_rows()));
        }
        let key_cols = self.key_columns(batch)?;
        let key_refs: Vec<&ArrayRef> = key_cols.iter().collect();
        compute_key_hash(&key_refs)
    }
}

#[async_trait]
impl System for WindowedSystem {
    fn meta(&self) -> SystemMeta {
        self.meta.clone()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let pipeline = data;
        let batch = pipeline.batch_named(self.source_component)?.clone();

        let n_rows = batch.num_rows();
        if n_rows == 0 {
            let schema = self.empty_result_schema();
            pipeline.insert_resource(WindowResults::new(schema));
            return Ok(());
        }

        let time_idx = batch.schema().index_of(&self.time_field).map_err(|_| {
            SaciError::generic(format!(
                "WindowedSystem: time field '{}' not found in component '{}'",
                self.time_field, self.source_component
            ))
        })?;
        let time_col = batch.column(time_idx).clone();
        let time_ms = to_ms_array(&time_col)?;

        // Drop rows whose timestamp is null. `to_ms_array` preserves the source
        // column's nullability, and a null timestamp has no place in any
        // window: the raw backing bits (typically 0) would land the row in the
        // epoch window and could advance the watermark to an arbitrary value.
        // Dropping them here lets every later step assume a null-free
        // timestamp array.
        let (batch, time_ms) = {
            let null_count = time_ms.null_count();
            if null_count > 0 {
                let n = time_ms.len();
                let keep_indices: Vec<u32> = (0..n)
                    .filter(|&i| !time_ms.is_null(i))
                    .map(|i| i as u32)
                    .collect();

                #[cfg(feature = "tracing")]
                tracing::warn!(
                    component = self.source_component,
                    time_field = %self.time_field,
                    null_count,
                    "WindowedSystem: skipping {null_count} row(s) with null timestamp"
                );

                let keep_arr = UInt32Array::from(keep_indices);
                let filtered_cols: Result<Vec<ArrayRef>, SaciError> = batch
                    .columns()
                    .iter()
                    .map(|col| {
                        take(col.as_ref(), &keep_arr, None).map_err(|e| {
                            SaciError::generic(format!(
                                "WindowedSystem: null-timestamp filter take: {e}"
                            ))
                        })
                    })
                    .collect();
                let filtered_batch =
                    RecordBatch::try_new(batch.schema(), filtered_cols?).map_err(|e| {
                        SaciError::generic(format!(
                            "WindowedSystem: null-timestamp filter RecordBatch: {e}"
                        ))
                    })?;
                let filtered_time = take(&time_ms as &dyn Array, &keep_arr, None).map_err(|e| {
                    SaciError::generic(format!(
                        "WindowedSystem: null-timestamp filter take time_ms: {e}"
                    ))
                })?;
                let filtered_time = filtered_time
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        SaciError::generic("WindowedSystem: null-timestamp filter downcast time_ms")
                    })?
                    .clone();

                (filtered_batch, filtered_time)
            } else {
                (batch, time_ms)
            }
        };

        if batch.num_rows() == 0 {
            let schema = self.empty_result_schema();
            pipeline.insert_resource(WindowResults::new(schema));
            return Ok(());
        }

        let (batch, time_ms, dropped) = self.apply_watermark_filter(&batch, &time_ms)?;

        if batch.num_rows() == 0 {
            let mut results = WindowResults::new(self.empty_result_schema());
            results.dropped = dropped;
            pipeline.insert_resource(results);
            return Ok(());
        }

        let n_pre_slide = batch.num_rows();

        // Sliding windows expand the batch before the normal aggregation path.
        let (batch, time_ms, sliding_window_ids) = if let WindowSpec::Sliding {
            size_ms,
            slide_ms,
            offset_ms,
        } = &self.spec
        {
            let (exp_batch, exp_time_ms, win_ids) = expand_for_sliding(
                &batch,
                &time_ms,
                n_pre_slide,
                *size_ms,
                *slide_ms,
                *offset_ms,
            )?;
            (exp_batch, exp_time_ms, Some(win_ids))
        } else {
            (batch, time_ms, None)
        };

        // Hashed once: grouping and session splitting both read it.
        let key_hash = self.key_hash(&batch)?;

        let window_ids: Int64Array = match &self.spec {
            WindowSpec::Tumbling { size_ms, offset_ms } => Int64Array::from_iter_values(
                time_ms
                    .values()
                    .iter()
                    .map(|&ts| tumbling_id(ts, *size_ms, *offset_ms)),
            ),
            WindowSpec::Session { gap_ms } => assign_sessions(&time_ms, &key_hash, *gap_ms)?,
            WindowSpec::Sliding { .. } => {
                sliding_window_ids.expect("sliding_window_ids always Some for Sliding spec")
            }
        };

        // Partition filter: when a `KeyPartition` resource is present, each
        // runner keeps only the rows whose
        // `key_hash % num_instances == instance_ordinal`, so every runner
        // accumulates a disjoint key slice.
        //
        // Global (non-keyed) windows hash to 0 for every row, so only ordinal 0
        // keeps them; `num_instances == 1` is the recommended setting there.
        #[cfg(feature = "distributed")]
        let (window_ids, key_hash, batch, time_ms) = {
            use crate::partition::KeyPartition;
            match pipeline.get_resource::<KeyPartition>() {
                Some(kp) if kp.num_instances > 1 => {
                    use arrow_array::BooleanArray;
                    use arrow_select::filter::{filter, filter_record_batch};

                    let num_instances = kp.num_instances as i64;
                    let ordinal = kp.instance_ordinal as i64;
                    let keep_mask: BooleanArray = key_hash
                        .values()
                        .iter()
                        .map(|h| h.rem_euclid(num_instances) == ordinal)
                        .collect();

                    let filtered_batch = filter_record_batch(&batch, &keep_mask)
                        .map_err(|e| SaciError::generic(format!("partition filter batch: {e}")))?;
                    let filter_i64 = |arr: &Int64Array, what: &str| -> SaciResult<Int64Array> {
                        filter(arr, &keep_mask)
                            .map_err(|e| {
                                SaciError::generic(format!("partition filter {what}: {e}"))
                            })?
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .cloned()
                            .ok_or_else(|| {
                                SaciError::generic(format!("partition filter {what} downcast"))
                            })
                    };
                    let filtered_time_ms = filter_i64(&time_ms, "time_ms")?;
                    let filtered_key_hash = filter_i64(&key_hash, "key_hash")?;
                    let filtered_win_ids = filter_i64(&window_ids, "window_ids")?;

                    if filtered_batch.num_rows() == 0 {
                        let mut results = WindowResults::new(self.empty_result_schema());
                        results.dropped = dropped;
                        pipeline.insert_resource(results);
                        return Ok(());
                    }

                    (
                        filtered_win_ids,
                        filtered_key_hash,
                        filtered_batch,
                        filtered_time_ms,
                    )
                }
                _ => (window_ids, key_hash, batch, time_ms),
            }
        };

        let sort_cols = vec![
            SortColumn {
                values: Arc::new(window_ids.clone()) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(key_hash.clone()) as ArrayRef,
                options: None,
            },
        ];

        let sorted_indices: UInt32Array = lexsort_to_indices(&sort_cols, None)
            .map_err(|e| SaciError::generic(format!("WindowedSystem: sort error: {e}")))?;

        let take_i64 = |arr: &Int64Array, what: &str| -> SaciResult<Int64Array> {
            take(arr as &dyn Array, &sorted_indices, None)
                .map_err(|e| SaciError::generic(format!("WindowedSystem: take {what}: {e}")))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
                .ok_or_else(|| SaciError::generic(format!("WindowedSystem: downcast {what}")))
        };
        let sorted_win_ids = take_i64(&window_ids, "window_id")?;
        let sorted_key_hash = take_i64(&key_hash, "key_hash")?;
        let sorted_time_ms = take_i64(&time_ms, "time_ms")?;

        let current_wm = self
            .watermark
            .as_ref()
            .map(|m| {
                m.lock()
                    .expect("watermark lock poisoned")
                    .current_watermark()
            })
            .unwrap_or(i64::MIN);

        let (result_schema, on_time, late) = self.aggregate_groups(
            &batch,
            &sorted_indices,
            &sorted_win_ids,
            &sorted_key_hash,
            &sorted_time_ms,
            current_wm,
        )?;

        // If a `WindowAccumulator` component is registered, refresh it from the
        // fresh aggregation results: mark existing rows for this
        // `source_component` dead, append the new aggregate rows, then compact.
        // Gated on registration because not every pipeline needs persistence.
        #[cfg(feature = "distributed")]
        {
            use super::accumulator::WindowAccumulator;

            if pipeline.batch_named(WindowAccumulator::name()).is_ok() {
                self.flush_accumulator(pipeline, &on_time)?;
            }
        }

        let mut results = WindowResults::new(result_schema);
        results.on_time = on_time;
        results.late = late;
        results.dropped = dropped;
        pipeline.insert_resource(results);

        Ok(())
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;

    use super::super::function::WindowFunction;
    use super::super::result::WindowResults;
    use super::*;
    use crate::window::WindowSpec;

    #[derive(Serialize, Deserialize)]
    pub(crate) struct Trade {
        pub(crate) timestamp_ms: i64,
        pub(crate) price: f64,
    }

    impl Trade {
        pub(crate) const TIMESTAMP_MS: FieldRef<Trade> = FieldRef::new("timestamp_ms");
    }

    impl Component for Trade {
        fn name() -> &'static str {
            "Trade"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("timestamp_ms", DataType::Int64, false),
                Field::new("price", DataType::Float64, false),
            ]))
        }
    }

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

    fn value(batch: &arrow_array::RecordBatch, column: usize) -> f64 {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn test_non_keyed_tumbling_sum() {
        // 6 trades across two 1-second windows:
        //   window 0 (ts 0..1000ms):  prices 10.0, 20.0, 30.0  → sum = 60.0
        //   window 1 (ts 1000..2000): prices 5.0,  15.0, 25.0  → sum = 45.0
        let mut pipeline = trades(&[
            (100, 10.0),
            (200, 20.0),
            (300, 30.0),
            (1100, 5.0),
            (1200, 15.0),
            (1300, 25.0),
        ]);

        sum_price(WindowSpec::tumbling(1000))
            .run(&mut pipeline)
            .await
            .unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.on_time.len(), 2, "expected two window groups");

        // Results are sorted by window_id ascending.
        let ids: Vec<i64> = results
            .on_time
            .iter()
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        assert_eq!(ids, [0, 1]);

        let (sum0, sum1) = (value(&results.on_time[0], 2), value(&results.on_time[1], 2));
        assert!(
            (sum0 - 60.0).abs() < 1e-9,
            "window 0 sum: expected 60.0, got {sum0}"
        );
        assert!(
            (sum1 - 45.0).abs() < 1e-9,
            "window 1 sum: expected 45.0, got {sum1}"
        );
    }

    #[tokio::test]
    async fn test_empty_component_produces_empty_results() {
        let mut pipeline = Dataset::new();
        pipeline.register::<Trade>().unwrap();

        sum_price(WindowSpec::tumbling(1000))
            .run(&mut pipeline)
            .await
            .unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert!(results.on_time.is_empty());
    }

    /// An empty run and a non-empty run publish the same schema, so a
    /// consumer never sees the result columns change between batches.
    #[tokio::test]
    async fn an_empty_and_a_non_empty_run_publish_the_same_schema() {
        let system = sum_price(WindowSpec::tumbling(1000));

        let mut empty = Dataset::new();
        empty.register::<Trade>().unwrap();
        system.run(&mut empty).await.unwrap();

        let mut full = trades(&[(100, 1.0)]);
        system.run(&mut full).await.unwrap();

        let empty_schema = empty.resource::<WindowResults>().unwrap().schema.clone();
        let full_schema = full.resource::<WindowResults>().unwrap().schema.clone();
        assert_eq!(empty_schema, full_schema);
        assert_eq!(full_schema.field(2).name(), "sum_price");
    }

    #[test]
    fn a_zero_size_window_is_refused_at_construction() {
        let err =
            WindowedSystem::new_named("T", "ts", WindowSpec::tumbling(0), WindowFunction::sum("v"))
                .unwrap_err();
        assert_eq!(err.category(), "configuration");
    }

    #[test]
    fn meta_reads_the_component_and_writes_the_results() {
        let meta = sum_price(WindowSpec::tumbling(1000)).meta();
        assert_eq!(meta.name(), "windowed");
        assert!(meta.reads_components.contains(&"Trade"));
        assert!(!meta.writes_resources.is_empty());
    }

    #[tokio::test]
    async fn test_missing_component_returns_error() {
        let mut pipeline = Dataset::new();
        let result = sum_price(WindowSpec::tumbling(1000))
            .run(&mut pipeline)
            .await;
        assert_eq!(result.unwrap_err().category(), "component_not_registered");
    }

    #[tokio::test]
    async fn test_single_window_single_row_sum() {
        let mut pipeline = trades(&[(500, 42.0)]);
        sum_price(WindowSpec::tumbling(1000))
            .run(&mut pipeline)
            .await
            .unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.on_time.len(), 1);
        assert!((value(&results.on_time[0], 2) - 42.0).abs() < 1e-9);
    }

    /// Rows with a null timestamp must be dropped before window assignment.
    /// The remaining non-null rows must still produce correct aggregate output.
    #[tokio::test]
    async fn test_null_timestamps_are_skipped() {
        use arrow_array::RecordBatch;

        // Build a RecordBatch by hand so a null timestamp can be injected.
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, true),
            Field::new("price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(100), None, Some(200)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![10.0, 99.0, 20.0])) as ArrayRef,
            ],
        )
        .unwrap();

        let mut pipeline = Dataset::new();
        pipeline.register_schema("NullTrade", schema).unwrap();
        pipeline.append_record_batch("NullTrade", batch).unwrap();

        let sys = WindowedSystem::new_named(
            "NullTrade",
            "timestamp_ms",
            WindowSpec::tumbling(1000),
            WindowFunction::sum("price"),
        )
        .unwrap();
        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        // Only the two non-null rows (price 10.0 + 20.0 = 30.0) should appear.
        assert_eq!(results.on_time.len(), 1, "expected one window group");
        let sum = value(&results.on_time[0], 2);
        assert!(
            (sum - 30.0).abs() < 1e-9,
            "expected sum 30.0 (null row skipped), got {sum}"
        );
    }
}
