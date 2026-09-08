//! [`WindowedSystem`]: the core windowed aggregation system.
//!
//! Builds on [`WindowSpec`], [`WindowFunction`] and the key-hash helpers to
//! assign window IDs, sort rows into groups, and aggregate each group into a
//! result [`RecordBatch`] stored as a [`WindowResults`] resource.
//!
//! Group aggregation lives in the sibling `aggregate` module, sliding-window
//! expansion and reduce aggregates in `sliding`, the fluent constructor in
//! `builder`, and the distributed accumulator flush in `accumulator`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_select::take::take;
use async_trait::async_trait;

use crate::dataset::Dataset;
use crate::error::SaciError;
use crate::system::{System, SystemMeta};

use super::function::WindowFunction;
use super::hash::{compute_global_hash, compute_key_hash};
use super::result::WindowResults;
use super::session::assign_sessions;
use super::sliding::expand_for_sliding;
use super::time::to_ms_array;
use super::watermark::WatermarkState;
use crate::window_spec::WindowSpec;

pub use super::builder::WindowedSystemBuilder;

/// A pipeline system that performs windowed aggregation over a source component.
///
/// On each [`run`](System::run) invocation the system:
///
/// 1. Extracts the time column from the source component and converts it to
///    milliseconds via [`to_ms_array`].
/// 2. Advances the internal [`WatermarkState`] from the observed timestamps.
/// 3. Routes rows beyond the allowed-lateness budget to a
///    [`SideOutput<DroppedLate>`](super::result::SideOutput) resource in the
///    pipeline.
/// 4. Assigns a window-bucket ID to every remaining row.
/// 5. Computes a per-row key hash over the configured key fields (or a global
///    all-zero hash for non-keyed windows).
/// 6. Sorts rows by `(window_id, key_hash)` and walks adjacent equal pairs to
///    form groups.
/// 7. Applies the configured [`WindowFunction`] to each group, marking groups
///    as late-firing when the window has already been emitted.
/// 8. Inserts the [`WindowResults`] resource into the pipeline for downstream
///    systems to consume.
///
/// Build via [`WindowedSystemBuilder`].
pub struct WindowedSystem {
    pub(super) source_component: &'static str,
    pub(super) time_field: &'static str,
    pub(super) key_fields: Vec<&'static str>,
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

#[async_trait]
impl System for WindowedSystem {
    fn meta(&self) -> SystemMeta {
        self.meta.clone()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let pipeline = data;
        let batch = pipeline
            .batch_for(self.source_component)
            .ok_or_else(|| {
                SaciError::generic(format!(
                    "WindowedSystem: component '{}' not registered",
                    self.source_component
                ))
            })?
            .clone();

        let n_rows = batch.num_rows();
        if n_rows == 0 {
            let schema = self.empty_result_schema();
            pipeline.insert_resource(WindowResults::new(schema));
            return Ok(());
        }

        let time_idx = batch.schema().index_of(self.time_field).map_err(|_| {
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
                    time_field = self.time_field,
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

        let (batch, time_ms, dropped_side_output) =
            self.apply_watermark_filter(&batch, &time_ms)?;

        if batch.num_rows() == 0 {
            let schema = self.empty_result_schema();
            let mut results = WindowResults::new(schema);
            results.side_output = dropped_side_output;
            pipeline.insert_resource(results);
            return Ok(());
        }

        let n_pre_slide = batch.num_rows();

        // Session windows need key columns for per-key session splitting.
        let session_key_cols: Vec<ArrayRef> = if matches!(&self.spec, WindowSpec::Session { .. }) {
            self.key_fields
                .iter()
                .map(|&field| {
                    let idx = batch.schema().index_of(field).map_err(|_| {
                        SaciError::generic(format!(
                            "WindowedSystem: key field '{field}' not found in component '{}'",
                            self.source_component
                        ))
                    })?;
                    Ok(batch.column(idx).clone())
                })
                .collect::<Result<Vec<_>, SaciError>>()?
        } else {
            vec![]
        };

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

        let n_rows = batch.num_rows();

        let window_ids: Int64Array = match &self.spec {
            WindowSpec::Tumbling { size_ms, offset_ms } => {
                let ids: Vec<i64> = time_ms
                    .values()
                    .iter()
                    .map(|&ts| WindowSpec::assign_tumbling(ts, *size_ms, *offset_ms))
                    .collect();
                Int64Array::from(ids)
            }
            WindowSpec::Session { gap_ms } => {
                let key_refs: Vec<&ArrayRef> = session_key_cols.iter().collect();
                assign_sessions(&time_ms, &key_refs, *gap_ms)?
            }
            WindowSpec::Sliding { .. } => {
                sliding_window_ids.expect("sliding_window_ids always Some for Sliding spec")
            }
        };

        let key_hash: Int64Array = if self.key_fields.is_empty() {
            compute_global_hash(n_rows)
        } else {
            let key_cols: Vec<ArrayRef> = self
                .key_fields
                .iter()
                .map(|&field| {
                    let idx = batch.schema().index_of(field).map_err(|_| {
                        SaciError::generic(format!(
                            "WindowedSystem: key field '{field}' not found in component '{}'",
                            self.source_component
                        ))
                    })?;
                    Ok(batch.column(idx).clone())
                })
                .collect::<Result<Vec<_>, SaciError>>()?;

            let key_refs: Vec<&ArrayRef> = key_cols.iter().collect();
            compute_key_hash(&key_refs)?
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
            use super::result::{DroppedLate, SideOutput};
            use crate::partition::KeyPartition;
            if let Some(kp) = pipeline.get_resource::<KeyPartition>() {
                let num_instances = kp.num_instances as i64;
                let ordinal = kp.instance_ordinal as i64;
                if num_instances > 1 {
                    use arrow_array::BooleanArray;
                    use arrow_select::filter::filter_record_batch;

                    let keep_mask: BooleanArray = (0..key_hash.len())
                        .map(|i| key_hash.value(i).rem_euclid(num_instances) == ordinal)
                        .collect();

                    let filtered_batch = filter_record_batch(&batch, &keep_mask)
                        .map_err(|e| SaciError::generic(format!("partition filter batch: {e}")))?;
                    let filtered_time_ms = arrow_select::filter::filter(&time_ms, &keep_mask)
                        .map_err(|e| SaciError::generic(format!("partition filter time_ms: {e}")))?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| SaciError::generic("partition filter time_ms downcast"))?
                        .clone();
                    let filtered_key_hash = arrow_select::filter::filter(&key_hash, &keep_mask)
                        .map_err(|e| SaciError::generic(format!("partition filter key_hash: {e}")))?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| SaciError::generic("partition filter key_hash downcast"))?
                        .clone();
                    let filtered_win_ids = arrow_select::filter::filter(&window_ids, &keep_mask)
                        .map_err(|e| {
                            SaciError::generic(format!("partition filter window_ids: {e}"))
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| SaciError::generic("partition filter window_ids downcast"))?
                        .clone();

                    if filtered_batch.num_rows() == 0 {
                        let schema = self.empty_result_schema();
                        pipeline.insert_resource(WindowResults::new(schema));
                        pipeline.insert_resource(SideOutput::<DroppedLate>::new());
                        return Ok(());
                    }

                    (
                        filtered_win_ids,
                        filtered_key_hash,
                        filtered_batch,
                        filtered_time_ms,
                    )
                } else {
                    (window_ids, key_hash, batch, time_ms)
                }
            } else {
                (window_ids, key_hash, batch, time_ms)
            }
        };
        #[cfg(not(feature = "distributed"))]
        let (window_ids, key_hash, batch, time_ms) = (window_ids, key_hash, batch, time_ms);

        // Shadow n_rows in case the partition filter reduced the row count.
        let n_rows = batch.num_rows();
        let _ = n_rows; // used below in aggregate_groups via sorted_indices

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

        let sorted_win_ids = take(&window_ids as &dyn Array, &sorted_indices, None)
            .map_err(|e| SaciError::generic(format!("WindowedSystem: take error: {e}")))?;
        let sorted_key_hash = take(&key_hash as &dyn Array, &sorted_indices, None)
            .map_err(|e| SaciError::generic(format!("WindowedSystem: take error: {e}")))?;
        let sorted_time_ms = take(&time_ms as &dyn Array, &sorted_indices, None)
            .map_err(|e| SaciError::generic(format!("WindowedSystem: take time_ms error: {e}")))?;

        let sorted_win_ids = sorted_win_ids
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| SaciError::generic("WindowedSystem: downcast window_id failed"))?;
        let sorted_key_hash = sorted_key_hash
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| SaciError::generic("WindowedSystem: downcast key_hash failed"))?;
        let sorted_time_ms = sorted_time_ms
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| SaciError::generic("WindowedSystem: downcast time_ms failed"))?;

        let current_wm = self
            .watermark
            .as_ref()
            .map(|m| {
                m.lock()
                    .expect("watermark lock poisoned")
                    .current_watermark()
            })
            .unwrap_or(i64::MIN);

        let (result_schema, on_time_batches, late_batches) = self.aggregate_groups(
            &batch,
            &sorted_indices,
            sorted_win_ids,
            sorted_key_hash,
            sorted_time_ms,
            current_wm,
        )?;

        let mut results = WindowResults::new(result_schema.clone());
        results.batches = on_time_batches.clone();
        results.late_batches = late_batches;
        results.side_output = dropped_side_output;

        // If a `WindowAccumulator` component is registered, refresh it from the
        // fresh aggregation results: mark existing rows for this
        // `source_component` dead, append the new aggregate rows, then compact.
        // Gated on registration because not every pipeline needs persistence.
        #[cfg(feature = "distributed")]
        {
            use super::accumulator::WindowAccumulator;
            use crate::component::Component as _;

            if pipeline.batch_for(WindowAccumulator::name()).is_some() {
                self.flush_accumulator(pipeline, &on_time_batches)?;
            }
        }

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

    use super::super::function::{ReduceAggregate, WindowFunction};
    use super::super::result::WindowResults;
    use super::*;
    use crate::window_spec::WindowSpec;

    #[derive(Serialize, Deserialize)]
    pub(crate) struct Trade {
        pub(crate) timestamp_ms: i64,
        pub(crate) price: f64,
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

    #[tokio::test]
    async fn test_non_keyed_tumbling_sum() {
        // 6 trades across two 1-second windows:
        //   window 0 (ts 0..1000ms):  prices 10.0, 20.0, 30.0  → sum = 60.0
        //   window 1 (ts 1000..2000): prices 5.0,  15.0, 25.0  → sum = 45.0
        let trades = vec![
            Trade {
                timestamp_ms: 100,
                price: 10.0,
            },
            Trade {
                timestamp_ms: 200,
                price: 20.0,
            },
            Trade {
                timestamp_ms: 300,
                price: 30.0,
            },
            Trade {
                timestamp_ms: 1100,
                price: 5.0,
            },
            Trade {
                timestamp_ms: 1200,
                price: 15.0,
            },
            Trade {
                timestamp_ms: 1300,
                price: 25.0,
            },
        ];

        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 2, "expected two window groups");

        // Results are sorted by window_id ascending.
        let first = &results.batches[0];
        let second = &results.batches[1];

        let win0_id = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let win1_id = second
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        assert_eq!(win0_id, 0);
        assert_eq!(win1_id, 1);

        let sum0 = first
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let sum1 = second
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);

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
        pipeline.register_component::<Trade>().unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert!(results.batches.is_empty());
    }

    #[tokio::test]
    async fn test_missing_component_returns_error() {
        let mut pipeline = Dataset::new();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        let result = sys.run(&mut pipeline).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "generic");
    }

    #[tokio::test]
    async fn test_single_window_single_row_sum() {
        let trades = vec![Trade {
            timestamp_ms: 500,
            price: 42.0,
        }];

        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 1);
        let sum_val = results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((sum_val - 42.0).abs() < 1e-9);
    }

    /// Rows with a null timestamp must be dropped before window assignment.
    /// The remaining non-null rows must still produce correct aggregate output.
    #[tokio::test]
    async fn test_null_timestamps_are_skipped() {
        use arrow_array::builder::{Float64Builder, Int64Builder};

        // Build a RecordBatch by hand so a null timestamp can be injected.
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, true),
            Field::new("price", DataType::Float64, false),
        ]));

        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(100); // window 0
        ts_builder.append_null(); // should be dropped
        ts_builder.append_value(200); // window 0
        let ts_array = ts_builder.finish();

        let mut price_builder = Float64Builder::new();
        price_builder.append_value(10.0);
        price_builder.append_value(99.0); // this row is dropped
        price_builder.append_value(20.0);
        let price_array = price_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts_array) as ArrayRef,
                Arc::new(price_array) as ArrayRef,
            ],
        )
        .unwrap();

        let mut pipeline = Dataset::new();
        pipeline.register_raw_component("NullTrade", schema);
        pipeline.append_record_batch("NullTrade", batch).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("NullTrade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        // Only the two non-null rows (price 10.0 + 20.0 = 30.0) should appear.
        assert_eq!(results.batches.len(), 1, "expected one window group");
        let sum = results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            (sum - 30.0).abs() < 1e-9,
            "expected sum 30.0 (null row skipped), got {sum}"
        );
    }
}
