//! Stream deduplication operator (`DeduplicateSystem`) with optional
//! cross-batch state, event-time TTL expiration, and bounded key capacity.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::Schema;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::component::Component;
use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::sdk::State;
use crate::system::{System, SystemMeta};

use super::hash::compute_row_keys;
use super::time::to_ms_array;

/// Which duplicate row to retain when multiple rows share a key within one
/// batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeepPolicy {
    /// Retain the earliest occurrence (`first`).
    #[default]
    First,
    /// Retain the latest occurrence (`last`, within the current batch).
    Last,
}

/// Checkpointable cross-batch state for [`DeduplicateSystem`].
///
/// Declare `.state::<DeduplicateState>()` on a [`Pipeline`](crate::Pipeline) to
/// persist seen keys across WASM/plugin/service batch boundaries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeduplicateState {
    /// Monotonic event-time watermark observed across batches (`i64::MIN` when unset).
    pub watermark_ms: i64,
    /// Active seen keys in insertion order.
    pub keys: Vec<String>,
    /// Timestamp (ms) associated with each entry in `keys`.
    pub timestamps_ms: Vec<i64>,
}

impl Component for DeduplicateState {
    fn name() -> &'static str {
        "DeduplicateState"
    }

    fn schema() -> Arc<Schema> {
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = DeduplicateState {
            watermark_ms: 0,
            keys: vec!["k".to_string()],
            timestamps_ms: vec![0],
        };
        let fields =
            Vec::<arrow_schema::FieldRef>::from_samples(&[sample], TracingOptions::default())
                .expect("DeduplicateState schema traces from sample");
        Arc::new(Schema::new(fields))
    }
}

/// Removes duplicate rows by `key_fields` from `source_component`, writing
/// deduplicated rows to `target_component`.
///
/// Supports both in-batch stateless deduplication (`stateful(false)`) and
/// cross-batch stateful deduplication (`stateful(true)`, default) with optional
/// event-time TTL (`ttl_ms`) and bounded capacity (`max_keys`).
#[derive(Debug)]
pub struct DeduplicateSystem {
    source_component: &'static str,
    target_component: &'static str,
    key_fields: Vec<String>,
    keep: KeepPolicy,
    time_field: Option<String>,
    ttl_ms: Option<i64>,
    max_keys: usize,
    stateful: bool,
    fallback_state: Mutex<DeduplicateState>,
}

impl Clone for DeduplicateSystem {
    fn clone(&self) -> Self {
        let guard = self
            .fallback_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            source_component: self.source_component,
            target_component: self.target_component,
            key_fields: self.key_fields.clone(),
            keep: self.keep,
            time_field: self.time_field.clone(),
            ttl_ms: self.ttl_ms,
            max_keys: self.max_keys,
            stateful: self.stateful,
            fallback_state: Mutex::new(guard.clone()),
        }
    }
}

impl DeduplicateSystem {
    /// Create a stateful deduplication operator reading `source_component` and
    /// writing deduplicated rows to `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `key_fields` is empty.
    pub fn new<I, S>(
        source_component: &str,
        target_component: &str,
        key_fields: I,
    ) -> SaciResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let key_fields: Vec<String> = key_fields
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        if key_fields.is_empty() {
            return Err(SaciError::configuration(
                "DeduplicateSystem requires at least one key field",
            ));
        }
        Ok(Self {
            source_component: intern_component_name(source_component),
            target_component: intern_component_name(target_component),
            key_fields,
            keep: KeepPolicy::First,
            time_field: None,
            ttl_ms: None,
            max_keys: 100_000,
            stateful: true,
            fallback_state: Mutex::new(DeduplicateState {
                watermark_ms: i64::MIN,
                keys: Vec::new(),
                timestamps_ms: Vec::new(),
            }),
        })
    }

    /// Deduplicate `component` in place.
    ///
    /// # Errors
    ///
    /// Same as [`Self::new`].
    pub fn in_place<I, S>(component: &str, key_fields: I) -> SaciResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::new(component, component, key_fields)
    }

    /// Set which duplicate within a batch to keep (`First` or `Last`).
    pub fn keep(mut self, keep: KeepPolicy) -> Self {
        self.keep = keep;
        self
    }

    /// Specify the event-time column used for TTL eviction (`ttl_ms`).
    pub fn time_field(mut self, field: impl Into<String>) -> Self {
        self.time_field = Some(field.into());
        self
    }

    /// Expire seen keys older than `watermark_ms - ttl_ms`.
    pub fn ttl_ms(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = Some(ttl_ms.max(0));
        self
    }

    /// Cap the number of retained seen keys across batches (default `100_000`).
    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = max_keys.max(1);
        self
    }

    /// Toggle cross-batch stateful deduplication (default `true`).
    pub fn stateful(mut self, stateful: bool) -> Self {
        self.stateful = stateful;
        self
    }

    /// Deduplicate a single `batch` without cross-batch state (`DISTINCT`).
    pub fn deduplicate_batch_stateless(&self, batch: &RecordBatch) -> SaciResult<RecordBatch> {
        let mut empty = DeduplicateState {
            watermark_ms: i64::MIN,
            keys: Vec::new(),
            timestamps_ms: Vec::new(),
        };
        self.deduplicate_with_state(batch, &mut empty, false)
    }

    fn deduplicate_with_state(
        &self,
        batch: &RecordBatch,
        state: &mut DeduplicateState,
        persist_state: bool,
    ) -> SaciResult<RecordBatch> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let key_cols: SaciResult<Vec<&ArrayRef>> = self
            .key_fields
            .iter()
            .map(|k| {
                let idx = batch
                    .schema_ref()
                    .index_of(k)
                    .map_err(|_| SaciError::field_not_found(self.source_component, k))?;
                Ok(batch.column(idx))
            })
            .collect();
        let key_cols = key_cols?;
        let row_keys = compute_row_keys(&key_cols)?;

        // Evict keys already expired as of the incoming watermark (before processing this batch's rows).
        if persist_state && let (Some(ttl), true) = (self.ttl_ms, state.watermark_ms != i64::MIN) {
            let cutoff = state.watermark_ms.saturating_sub(ttl);
            let mut next_keys = Vec::with_capacity(state.keys.len());
            let mut next_ts = Vec::with_capacity(state.timestamps_ms.len());
            for (k, ts) in state.keys.drain(..).zip(state.timestamps_ms.drain(..)) {
                if ts >= cutoff {
                    next_keys.push(k);
                    next_ts.push(ts);
                }
            }
            state.keys = next_keys;
            state.timestamps_ms = next_ts;
        }

        let row_ts: Vec<Option<i64>> = if let Some(tf) = &self.time_field {
            let idx = batch
                .schema_ref()
                .index_of(tf)
                .map_err(|_| SaciError::field_not_found(self.source_component, tf))?;
            let ms = to_ms_array(batch.column(idx))?;
            (0..n)
                .map(|i| ms.is_valid(i).then(|| ms.value(i)))
                .collect()
        } else {
            vec![None; n]
        };

        let seen_idx: HashMap<&str, usize> = if persist_state {
            state
                .keys
                .iter()
                .enumerate()
                .map(|(idx, k)| (k.as_str(), idx))
                .collect()
        } else {
            HashMap::new()
        };

        let mut keep_mask = vec![false; n];
        match self.keep {
            KeepPolicy::First => {
                let mut batch_seen = HashSet::with_capacity(n);
                for (i, (_h, key)) in row_keys.iter().enumerate() {
                    if let Some(&pos) = seen_idx.get(key.as_str()) {
                        if persist_state
                            && let Some(ts) = row_ts[i]
                            && ts > state.timestamps_ms[pos]
                        {
                            state.timestamps_ms[pos] = ts;
                        }
                    } else if batch_seen.insert(key.as_str()) {
                        keep_mask[i] = true;
                    }
                }
            }
            KeepPolicy::Last => {
                let mut last_idx: HashMap<&str, usize> = HashMap::with_capacity(n);
                for (i, (_h, key)) in row_keys.iter().enumerate() {
                    if let Some(&pos) = seen_idx.get(key.as_str()) {
                        if persist_state
                            && let Some(ts) = row_ts[i]
                            && ts > state.timestamps_ms[pos]
                        {
                            state.timestamps_ms[pos] = ts;
                        }
                    } else {
                        last_idx.insert(key.as_str(), i);
                    }
                }
                for idx in last_idx.into_values() {
                    keep_mask[idx] = true;
                }
            }
        }
        drop(seen_idx);

        if persist_state {
            if self.time_field.is_some() {
                for ts in row_ts.iter().copied().flatten() {
                    if ts > state.watermark_ms {
                        state.watermark_ms = ts;
                    }
                }
            }
            let fallback_ts = if state.watermark_ms == i64::MIN {
                0
            } else {
                state.watermark_ms
            };
            for (i, ((_h, key), ts)) in row_keys.into_iter().zip(row_ts.iter().copied()).enumerate()
            {
                if keep_mask[i] {
                    state.keys.push(key);
                    state.timestamps_ms.push(ts.unwrap_or(fallback_ts));
                }
            }
            if self.time_field.is_some()
                && let Some(ttl) = self.ttl_ms
                && state.watermark_ms != i64::MIN
            {
                let cutoff = state.watermark_ms.saturating_sub(ttl);
                let mut next_keys = Vec::with_capacity(state.keys.len());
                let mut next_ts = Vec::with_capacity(state.timestamps_ms.len());
                for (k, ts) in state.keys.drain(..).zip(state.timestamps_ms.drain(..)) {
                    if ts >= cutoff {
                        next_keys.push(k);
                        next_ts.push(ts);
                    }
                }
                state.keys = next_keys;
                state.timestamps_ms = next_ts;
            }
            if state.keys.len() > self.max_keys {
                let excess = state.keys.len() - self.max_keys;
                state.keys.drain(0..excess);
                state.timestamps_ms.drain(0..excess);
            }
        }

        let mask = BooleanArray::from(keep_mask);
        filter_record_batch(batch, &mask)
            .map_err(|e| SaciError::generic(format!("DeduplicateSystem filter error: {e}")))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let input = data.batch_named(self.source_component)?.clone();
        let out_batch = if !self.stateful {
            self.deduplicate_batch_stateless(&input)?
        } else if let Some(state_res) = data.get_resource_mut::<State<DeduplicateState>>() {
            if state_res.rows.is_empty() {
                state_res.rows.push(DeduplicateState {
                    watermark_ms: i64::MIN,
                    keys: Vec::new(),
                    timestamps_ms: Vec::new(),
                });
            }
            self.deduplicate_with_state(&input, &mut state_res.rows[0], true)?
        } else {
            let mut guard = self
                .fallback_state
                .lock()
                .map_err(|_| SaciError::generic("DeduplicateSystem fallback state poisoned"))?;
            self.deduplicate_with_state(&input, &mut guard, true)?
        };

        data.put_batch_named(self.target_component, out_batch)
    }
}

#[async_trait]
impl System for DeduplicateSystem {
    fn meta(&self) -> SystemMeta {
        let mut meta = SystemMeta::new("deduplicate")
            .reads_all_named(self.source_component)
            .writes_all_named(self.target_component);
        if self.stateful {
            meta = meta.writes_resource::<State<DeduplicateState>>();
        }
        meta
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
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn event_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    fn make_events(rows: &[(&str, i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            event_schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn deduplicate_across_batches_with_ttl_and_keep_last() {
        let sys = DeduplicateSystem::in_place("Event", ["id"])
            .unwrap()
            .keep(KeepPolicy::Last)
            .time_field("ts")
            .ttl_ms(1_000);

        let mut ds = Dataset::new();
        ds.register_schema("Event", event_schema()).unwrap();
        ds.append_record_batch(
            "Event",
            make_events(&[("a", 100, 1), ("a", 200, 2), ("b", 300, 3)]),
        )
        .unwrap();
        sys.run(&mut ds).await.unwrap();

        let b1 = ds.batch_named("Event").unwrap();
        assert_eq!(b1.num_rows(), 2);
        let vals = b1.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(vals.values(), &[2, 3]);

        // Second batch at ts=500: "a" is still within TTL (watermark 300 -> cutoff -700), so dropped.
        ds.set_batch_named("Event", make_events(&[("a", 500, 9), ("c", 2_000, 4)]))
            .unwrap();
        sys.run(&mut ds).await.unwrap();
        let b2 = ds.batch_named("Event").unwrap();
        assert_eq!(b2.num_rows(), 1);
        let ids2 = b2.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ids2.value(0), "c");

        // Third batch: watermark is now 2000, cutoff = 1000, so old "a" (200) expired and is accepted again!
        ds.set_batch_named("Event", make_events(&[("a", 2_100, 10)]))
            .unwrap();
        sys.run(&mut ds).await.unwrap();
        let b3 = ds.batch_named("Event").unwrap();
        assert_eq!(b3.num_rows(), 1);
    }

    fn nullable_event_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn deduplicate_null_timestamps_negative_timestamps_and_checkpoint_roundtrip() {
        use crate::pipeline::Pipeline;
        use crate::runtime::PipelineRuntime;

        let build_pipeline = || {
            let sys = DeduplicateSystem::in_place("Event", ["id"])
                .unwrap()
                .keep(KeepPolicy::Last)
                .time_field("ts")
                .ttl_ms(1_000);
            Pipeline::new("dedup_ckpt")
                .state::<DeduplicateState>()
                .system(sys)
        };

        let make_nullable = |rows: &[(&str, Option<i64>, i64)]| {
            RecordBatch::try_new(
                nullable_event_schema(),
                vec![
                    Arc::new(StringArray::from(
                        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                    )) as ArrayRef,
                ],
            )
            .unwrap()
        };

        // Pass 1: negative epoch ts (-5000) and a NULL ts row must keep watermark at -5000 (not 0).
        let p1 = build_pipeline();
        let mut ds1 = Dataset::new();
        ds1.register_schema("Event", nullable_event_schema())
            .unwrap();
        ds1.set_batch_named(
            "Event",
            make_nullable(&[("neg", Some(-5_000), 1), ("null_k", None, 2)]),
        )
        .unwrap();
        let out1 = p1.run_batch(&mut ds1, None).await.unwrap();
        assert_eq!(ds1.batch_named("Event").unwrap().num_rows(), 2);
        let blob1 = out1.state.expect("dedup checkpoint blob");

        // Pass 2 on fresh Pipeline 2 with restored state: duplicate null_k is suppressed,
        // and positive ts=1_700_000_000_000 + null_pos keeps null_pos at fallback watermark (not 0).
        let p2 = build_pipeline();
        let mut ds2 = Dataset::new();
        ds2.register_schema("Event", nullable_event_schema())
            .unwrap();
        ds2.set_batch_named(
            "Event",
            make_nullable(&[
                ("null_k", None, 9),
                ("pos", Some(1_700_000_000_000), 3),
                ("null_pos", None, 4),
            ]),
        )
        .unwrap();
        let out2 = p2.run_batch(&mut ds2, Some(&blob1)).await.unwrap();
        let b2 = ds2.batch_named("Event").unwrap();
        assert_eq!(b2.num_rows(), 2);
        let blob2 = out2.state.expect("dedup checkpoint blob 2");

        // Pass 3: duplicate null_pos must still be recognized as already seen (did not self-evict at ts=0).
        let p3 = build_pipeline();
        let mut ds3 = Dataset::new();
        ds3.register_schema("Event", nullable_event_schema())
            .unwrap();
        ds3.set_batch_named("Event", make_nullable(&[("null_pos", None, 5)]))
            .unwrap();
        let _ = p3.run_batch(&mut ds3, Some(&blob2)).await.unwrap();
        assert_eq!(ds3.batch_named("Event").unwrap().num_rows(), 0);
    }
}
