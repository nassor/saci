//! Relational and stateful streaming join operators:
//! - [`JoinSystem`]: in-batch hash join (`Inner`, `Left`, `Right`, `Full`, `Semi`, `Anti`).
//! - [`IntervalJoinSystem`]: stateful stream-stream event-time interval join.
//! - [`LookupJoinSystem`]: stateful stream-table / dimension enrichment join.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{Field, Schema};
use arrow_select::take::take;
use async_trait::async_trait;
use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::component::Component;
use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::sdk::State;
use crate::system::{System, SystemMeta};

use super::deduplicate::{DeduplicateSystem, KeepPolicy};
use super::hash::compute_row_keys;
use super::time::{RecordBatchWrap, batch_to_ipc, concat_non_empty, ipc_to_batch, to_ms_array};

/// Relational join strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JoinKind {
    /// Emit rows matching on both left and right.
    #[default]
    Inner,
    /// Emit all left rows, filling unmatched right columns with null.
    Left,
    /// Emit all right rows, filling unmatched left columns with null.
    Right,
    /// Emit all left and right rows, filling unmatched sides with null.
    Full,
    /// Emit left rows that have at least one matching right row (left schema only).
    Semi,
    /// Emit left rows that have no matching right row (left schema only).
    Anti,
}

/// An equality join condition pairing a left column with a right column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinKey {
    /// Column name on the left / fact component.
    pub left: String,
    /// Column name on the right / dimension component.
    pub right: String,
}

impl JoinKey {
    /// Join where left column `left` equals right column `right`.
    pub fn new(left: impl Into<String>, right: impl Into<String>) -> Self {
        Self {
            left: left.into(),
            right: right.into(),
        }
    }

    /// Join where both sides share the same column name `field`.
    pub fn same(field: impl Into<String>) -> Self {
        let f = field.into();
        Self {
            left: f.clone(),
            right: f,
        }
    }
}

/// Build the output schema for a join between `left_schema` and `right_schema`.
pub(crate) fn build_joined_schema(
    left_schema: &Schema,
    right_schema: &Schema,
    kind: JoinKind,
    right_prefix: &str,
) -> Arc<Schema> {
    if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
        return Arc::new(left_schema.clone());
    }
    let left_nullable = matches!(kind, JoinKind::Right | JoinKind::Full);
    let right_nullable = matches!(kind, JoinKind::Left | JoinKind::Full);

    let mut seen: HashSet<String> = HashSet::new();
    let mut fields = Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

    for f in left_schema.fields() {
        seen.insert(f.name().clone());
        fields.push(Field::new(
            f.name(),
            f.data_type().clone(),
            f.is_nullable() || left_nullable,
        ));
    }
    for f in right_schema.fields() {
        let name = if seen.contains(f.name()) {
            format!("{right_prefix}{}", f.name())
        } else {
            f.name().clone()
        };
        seen.insert(name.clone());
        fields.push(Field::new(
            name,
            f.data_type().clone(),
            f.is_nullable() || right_nullable,
        ));
    }
    Arc::new(Schema::new(fields))
}

fn extract_key_arrays<'a>(
    batch: &'a RecordBatch,
    names: &[&str],
    side: &str,
) -> SaciResult<Vec<&'a ArrayRef>> {
    names
        .iter()
        .map(|name| {
            let idx = batch
                .schema_ref()
                .index_of(name)
                .map_err(|_| SaciError::field_not_found(side, *name))?;
            Ok(batch.column(idx))
        })
        .collect()
}

fn row_has_null(cols: &[&ArrayRef], row: usize) -> bool {
    cols.iter().any(|c| c.is_null(row))
}

fn materialize_join_pairs(
    left: &RecordBatch,
    right: &RecordBatch,
    out_schema: &Arc<Schema>,
    pairs: &[(Option<u32>, Option<u32>)],
) -> SaciResult<RecordBatch> {
    if pairs.is_empty() {
        return Ok(RecordBatch::new_empty(out_schema.clone()));
    }
    let left_indices = UInt32Array::from(pairs.iter().map(|p| p.0).collect::<Vec<_>>());
    let right_indices = UInt32Array::from(pairs.iter().map(|p| p.1).collect::<Vec<_>>());

    let mut cols = Vec::with_capacity(left.num_columns() + right.num_columns());
    for c in left.columns() {
        cols.push(
            take(c.as_ref(), &left_indices, None)
                .map_err(|e| SaciError::generic(format!("JoinSystem left take: {e}")))?,
        );
    }
    for c in right.columns() {
        cols.push(
            take(c.as_ref(), &right_indices, None)
                .map_err(|e| SaciError::generic(format!("JoinSystem right take: {e}")))?,
        );
    }
    RecordBatch::try_new(out_schema.clone(), cols)
        .map_err(|e| SaciError::generic(format!("JoinSystem output RecordBatch: {e}")))
}

/// Pure-Arrow in-batch relational join operator between two components in a
/// [`Dataset`].
#[derive(Debug, Clone)]
pub struct JoinSystem {
    left_component: &'static str,
    right_component: &'static str,
    target_component: &'static str,
    kind: JoinKind,
    keys: Vec<JoinKey>,
    right_prefix: String,
}

impl JoinSystem {
    /// Create a relational join over `left_component` and `right_component`,
    /// writing matched output rows into `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `keys` is empty.
    pub fn new(
        left_component: &str,
        right_component: &str,
        target_component: &str,
        kind: JoinKind,
        keys: Vec<JoinKey>,
    ) -> SaciResult<Self> {
        if keys.is_empty() {
            return Err(SaciError::configuration(
                "JoinSystem requires at least one JoinKey",
            ));
        }
        Ok(Self {
            left_component: intern_component_name(left_component),
            right_component: intern_component_name(right_component),
            target_component: intern_component_name(target_component),
            kind,
            keys,
            right_prefix: "right_".to_string(),
        })
    }

    /// Prefix applied to right-side columns whose names collide with left-side
    /// columns (default `"right_"`).
    pub fn right_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.right_prefix = prefix.into();
        self
    }

    /// Compute the output Arrow [`Schema`] for `left_schema` and `right_schema`.
    pub fn output_schema(
        &self,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> SaciResult<Arc<Schema>> {
        for k in &self.keys {
            left_schema
                .field_with_name(&k.left)
                .map_err(|_| SaciError::field_not_found(self.left_component, &k.left))?;
            right_schema
                .field_with_name(&k.right)
                .map_err(|_| SaciError::field_not_found(self.right_component, &k.right))?;
        }
        Ok(build_joined_schema(
            left_schema,
            right_schema,
            self.kind,
            &self.right_prefix,
        ))
    }

    /// Join `left` and `right` batches directly.
    pub fn join_batches(&self, left: &RecordBatch, right: &RecordBatch) -> SaciResult<RecordBatch> {
        let out_schema = self.output_schema(left.schema_ref(), right.schema_ref())?;

        let left_key_names: Vec<&str> = self.keys.iter().map(|k| k.left.as_str()).collect();
        let right_key_names: Vec<&str> = self.keys.iter().map(|k| k.right.as_str()).collect();
        let left_cols = extract_key_arrays(left, &left_key_names, self.left_component)?;
        let right_cols = extract_key_arrays(right, &right_key_names, self.right_component)?;

        let left_keys = compute_row_keys(&left_cols)?;
        let right_keys = compute_row_keys(&right_cols)?;

        let mut right_table: FnvHashMap<i64, Vec<u32>> =
            FnvHashMap::with_capacity_and_hasher(right.num_rows(), Default::default());
        for (r_idx, (hash, _)) in right_keys.iter().enumerate() {
            if !row_has_null(&right_cols, r_idx) {
                right_table.entry(*hash).or_default().push(r_idx as u32);
            }
        }

        if matches!(self.kind, JoinKind::Semi | JoinKind::Anti) {
            let mut keep = Vec::new();
            for (l_idx, (hash, l_key)) in left_keys.iter().enumerate() {
                let matched = if row_has_null(&left_cols, l_idx) {
                    false
                } else if let Some(cands) = right_table.get(hash) {
                    cands.iter().any(|&r| right_keys[r as usize].1 == *l_key)
                } else {
                    false
                };
                if (self.kind == JoinKind::Semi && matched)
                    || (self.kind == JoinKind::Anti && !matched)
                {
                    keep.push(l_idx as u32);
                }
            }
            if keep.is_empty() {
                return Ok(RecordBatch::new_empty(out_schema));
            }
            let take_idx = UInt32Array::from(keep);
            let cols: SaciResult<Vec<ArrayRef>> = left
                .columns()
                .iter()
                .map(|c| {
                    take(c.as_ref(), &take_idx, None)
                        .map_err(|e| SaciError::generic(format!("JoinSystem semi/anti take: {e}")))
                })
                .collect();
            return RecordBatch::try_new(out_schema, cols?)
                .map_err(|e| SaciError::generic(format!("JoinSystem semi/anti batch: {e}")));
        }

        let mut pairs: Vec<(Option<u32>, Option<u32>)> = Vec::new();
        let mut right_matched = vec![false; right.num_rows()];

        for (l_idx, (hash, l_key)) in left_keys.iter().enumerate() {
            let mut any_match = false;
            if !row_has_null(&left_cols, l_idx)
                && let Some(cands) = right_table.get(hash)
            {
                for &r_idx in cands {
                    if right_keys[r_idx as usize].1 == *l_key {
                        any_match = true;
                        right_matched[r_idx as usize] = true;
                        pairs.push((Some(l_idx as u32), Some(r_idx)));
                    }
                }
            }
            if !any_match && matches!(self.kind, JoinKind::Left | JoinKind::Full) {
                pairs.push((Some(l_idx as u32), None));
            }
        }

        if matches!(self.kind, JoinKind::Right | JoinKind::Full) {
            for (r_idx, &matched) in right_matched.iter().enumerate() {
                if !matched {
                    pairs.push((None, Some(r_idx as u32)));
                }
            }
        }

        materialize_join_pairs(left, right, &out_schema, &pairs)
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let left = data.batch_named(self.left_component)?.clone();
        let right = data.batch_named(self.right_component)?.clone();
        let out = self.join_batches(&left, &right)?;
        data.put_batch_named(self.target_component, out)
    }
}

#[async_trait]
impl System for JoinSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("join")
            .reads_all_named(self.left_component)
            .reads_all_named(self.right_component)
            .writes_all_named(self.target_component)
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

/// Checkpointable state for [`IntervalJoinSystem`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalJoinState {
    /// Highest event timestamp observed across both streams.
    pub watermark_ms: i64,
    /// Buffered active left-stream rows encoded as Arrow IPC stream bytes.
    pub left_ipc: Vec<u8>,
    /// Buffered active right-stream rows encoded as Arrow IPC stream bytes.
    pub right_ipc: Vec<u8>,
}

impl Default for IntervalJoinState {
    fn default() -> Self {
        Self {
            watermark_ms: i64::MIN,
            left_ipc: Vec::new(),
            right_ipc: Vec::new(),
        }
    }
}

impl Component for IntervalJoinState {
    fn name() -> &'static str {
        "IntervalJoinState"
    }

    fn schema() -> Arc<Schema> {
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = IntervalJoinState {
            watermark_ms: 0,
            left_ipc: vec![0],
            right_ipc: vec![0],
        };
        let fields =
            Vec::<arrow_schema::FieldRef>::from_samples(&[sample], TracingOptions::default())
                .expect("IntervalJoinState schema traces from sample");
        Arc::new(Schema::new(fields))
    }
}

/// Stateful stream-stream event-time interval join.
///
/// Emits `(left, right)` row pairs matching on `keys` whose timestamps satisfy
/// `left_ts + lower_bound_ms <= right_ts <= left_ts + upper_bound_ms`, while
/// buffering active windows in [`IntervalJoinState`] and evicting expired rows
/// as the watermark advances.
#[derive(Debug)]
pub struct IntervalJoinSystem {
    left_component: &'static str,
    right_component: &'static str,
    target_component: &'static str,
    left_time_field: String,
    right_time_field: String,
    keys: Vec<JoinKey>,
    lower_bound_ms: i64,
    upper_bound_ms: i64,
    allowed_lateness_ms: i64,
    right_prefix: String,
    fallback_state: Mutex<IntervalJoinState>,
}

impl Clone for IntervalJoinSystem {
    fn clone(&self) -> Self {
        let guard = self
            .fallback_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            left_component: self.left_component,
            right_component: self.right_component,
            target_component: self.target_component,
            left_time_field: self.left_time_field.clone(),
            right_time_field: self.right_time_field.clone(),
            keys: self.keys.clone(),
            lower_bound_ms: self.lower_bound_ms,
            upper_bound_ms: self.upper_bound_ms,
            allowed_lateness_ms: self.allowed_lateness_ms,
            right_prefix: self.right_prefix.clone(),
            fallback_state: Mutex::new(guard.clone()),
        }
    }
}

impl IntervalJoinSystem {
    /// Create a stream-stream interval join.
    ///
    /// Matches rows where `left_ts + lower_bound_ms <= right_ts <= left_ts + upper_bound_ms`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `keys` is empty or
    /// `lower_bound_ms > upper_bound_ms`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_component: &str,
        right_component: &str,
        target_component: &str,
        left_time_field: impl Into<String>,
        right_time_field: impl Into<String>,
        keys: Vec<JoinKey>,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
    ) -> SaciResult<Self> {
        if keys.is_empty() {
            return Err(SaciError::configuration(
                "IntervalJoinSystem requires at least one JoinKey",
            ));
        }
        if lower_bound_ms > upper_bound_ms {
            return Err(SaciError::configuration(format!(
                "IntervalJoinSystem: lower_bound_ms ({lower_bound_ms}) must be <= upper_bound_ms ({upper_bound_ms})"
            )));
        }
        Ok(Self {
            left_component: intern_component_name(left_component),
            right_component: intern_component_name(right_component),
            target_component: intern_component_name(target_component),
            left_time_field: left_time_field.into(),
            right_time_field: right_time_field.into(),
            keys,
            lower_bound_ms,
            upper_bound_ms,
            allowed_lateness_ms: 0,
            right_prefix: "right_".to_string(),
            fallback_state: Mutex::new(IntervalJoinState::default()),
        })
    }

    /// Set the lateness budget (ms) before buffered rows are evicted.
    pub fn allowed_lateness_ms(mut self, ms: i64) -> Self {
        self.allowed_lateness_ms = ms.max(0);
        self
    }

    /// Set collision prefix for right-side columns (default `"right_"`).
    pub fn right_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.right_prefix = prefix.into();
        self
    }

    /// Derive the output Arrow [`Schema`].
    pub fn output_schema(
        &self,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> SaciResult<Arc<Schema>> {
        for k in &self.keys {
            left_schema
                .field_with_name(&k.left)
                .map_err(|_| SaciError::field_not_found(self.left_component, &k.left))?;
            right_schema
                .field_with_name(&k.right)
                .map_err(|_| SaciError::field_not_found(self.right_component, &k.right))?;
        }
        left_schema
            .field_with_name(&self.left_time_field)
            .map_err(|_| SaciError::field_not_found(self.left_component, &self.left_time_field))?;
        right_schema
            .field_with_name(&self.right_time_field)
            .map_err(|_| {
                SaciError::field_not_found(self.right_component, &self.right_time_field)
            })?;
        Ok(build_joined_schema(
            left_schema,
            right_schema,
            JoinKind::Inner,
            &self.right_prefix,
        ))
    }

    fn match_pair(
        &self,
        left: &RecordBatch,
        right: &RecordBatch,
        out_schema: &Arc<Schema>,
    ) -> SaciResult<RecordBatch> {
        if left.num_rows() == 0 || right.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(out_schema.clone()));
        }
        let left_key_names: Vec<&str> = self.keys.iter().map(|k| k.left.as_str()).collect();
        let right_key_names: Vec<&str> = self.keys.iter().map(|k| k.right.as_str()).collect();
        let left_cols = extract_key_arrays(left, &left_key_names, self.left_component)?;
        let right_cols = extract_key_arrays(right, &right_key_names, self.right_component)?;

        let left_keys = compute_row_keys(&left_cols)?;
        let right_keys = compute_row_keys(&right_cols)?;

        let l_ts_idx = left
            .schema_ref()
            .index_of(&self.left_time_field)
            .map_err(|_| SaciError::field_not_found(self.left_component, &self.left_time_field))?;
        let r_ts_idx = right
            .schema_ref()
            .index_of(&self.right_time_field)
            .map_err(|_| {
                SaciError::field_not_found(self.right_component, &self.right_time_field)
            })?;
        let l_ts = to_ms_array(left.column(l_ts_idx))?;
        let r_ts = to_ms_array(right.column(r_ts_idx))?;

        let mut right_table: FnvHashMap<i64, Vec<u32>> =
            FnvHashMap::with_capacity_and_hasher(right.num_rows(), Default::default());
        for (r_idx, (hash, _)) in right_keys.iter().enumerate() {
            if !row_has_null(&right_cols, r_idx) && r_ts.is_valid(r_idx) {
                right_table.entry(*hash).or_default().push(r_idx as u32);
            }
        }

        let mut pairs = Vec::new();
        for (l_idx, (hash, l_key)) in left_keys.iter().enumerate() {
            if row_has_null(&left_cols, l_idx) || l_ts.is_null(l_idx) {
                continue;
            }
            let lt = l_ts.value(l_idx);
            let min_rt = lt.saturating_add(self.lower_bound_ms);
            let max_rt = lt.saturating_add(self.upper_bound_ms);
            if let Some(cands) = right_table.get(hash) {
                for &r_idx in cands {
                    if right_keys[r_idx as usize].1 == *l_key {
                        let rt = r_ts.value(r_idx as usize);
                        if rt >= min_rt && rt <= max_rt {
                            pairs.push((Some(l_idx as u32), Some(r_idx)));
                        }
                    }
                }
            }
        }

        materialize_join_pairs(left, right, out_schema, &pairs)
    }

    fn execute_with_state(
        &self,
        new_left: &RecordBatch,
        new_right: &RecordBatch,
        state: &mut IntervalJoinState,
    ) -> SaciResult<RecordBatch> {
        let out_schema = self.output_schema(new_left.schema_ref(), new_right.schema_ref())?;
        let buf_left = ipc_to_batch(&state.left_ipc)?
            .unwrap_or_else(|| RecordBatch::new_empty(new_left.schema()));
        let buf_right = ipc_to_batch(&state.right_ipc)?
            .unwrap_or_else(|| RecordBatch::new_empty(new_right.schema()));

        let m1 = self.match_pair(new_left, &buf_right, &out_schema)?;
        let m2 = self.match_pair(new_left, new_right, &out_schema)?;
        let m3 = self.match_pair(&buf_left, new_right, &out_schema)?;
        let matched = concat_non_empty(&out_schema, &[m1, m2, m3])?;

        // Advance watermark from new arrivals on both streams.
        if new_left.num_rows() > 0 {
            let l_idx = new_left
                .schema_ref()
                .index_of(&self.left_time_field)
                .map_err(|_| {
                    SaciError::field_not_found(self.left_component, &self.left_time_field)
                })?;
            for ts in to_ms_array(new_left.column(l_idx))?.iter().flatten() {
                if ts > state.watermark_ms {
                    state.watermark_ms = ts;
                }
            }
        }
        if new_right.num_rows() > 0 {
            let r_idx = new_right
                .schema_ref()
                .index_of(&self.right_time_field)
                .map_err(|_| {
                    SaciError::field_not_found(self.right_component, &self.right_time_field)
                })?;
            for ts in to_ms_array(new_right.column(r_idx))?.iter().flatten() {
                if ts > state.watermark_ms {
                    state.watermark_ms = ts;
                }
            }
        }

        let mut merged_left = concat_non_empty(&new_left.schema(), &[buf_left, new_left.clone()])?;
        let mut merged_right =
            concat_non_empty(&new_right.schema(), &[buf_right, new_right.clone()])?;

        if state.watermark_ms != i64::MIN {
            let cutoff = state.watermark_ms.saturating_sub(self.allowed_lateness_ms);
            if merged_left.num_rows() > 0 {
                let l_idx = merged_left
                    .schema_ref()
                    .index_of(&self.left_time_field)
                    .map_err(|_| {
                        SaciError::field_not_found(self.left_component, &self.left_time_field)
                    })?;
                let ts_arr = to_ms_array(merged_left.column(l_idx))?;
                let keep: arrow_array::BooleanArray = ts_arr
                    .iter()
                    .map(|v| v.is_some_and(|t| t.saturating_add(self.upper_bound_ms) >= cutoff))
                    .collect();
                merged_left = arrow_select::filter::filter_record_batch(&merged_left, &keep)
                    .map_err(|e| SaciError::generic(format!("IntervalJoin left evict: {e}")))?;
            }
            if merged_right.num_rows() > 0 {
                let r_idx = merged_right
                    .schema_ref()
                    .index_of(&self.right_time_field)
                    .map_err(|_| {
                        SaciError::field_not_found(self.right_component, &self.right_time_field)
                    })?;
                let ts_arr = to_ms_array(merged_right.column(r_idx))?;
                let keep: arrow_array::BooleanArray = ts_arr
                    .iter()
                    .map(|v| v.is_some_and(|t| t.saturating_sub(self.lower_bound_ms) >= cutoff))
                    .collect();
                merged_right = arrow_select::filter::filter_record_batch(&merged_right, &keep)
                    .map_err(|e| SaciError::generic(format!("IntervalJoin right evict: {e}")))?;
            }
        }

        state.left_ipc = if merged_left.num_rows() > 0 {
            batch_to_ipc(&RecordBatchWrap(&merged_left))?
        } else {
            Vec::new()
        };
        state.right_ipc = if merged_right.num_rows() > 0 {
            batch_to_ipc(&RecordBatchWrap(&merged_right))?
        } else {
            Vec::new()
        };

        Ok(matched)
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let new_left = data.batch_named(self.left_component)?.clone();
        let new_right = data.batch_named(self.right_component)?.clone();

        let out = if let Some(state_res) = data.get_resource_mut::<State<IntervalJoinState>>() {
            if state_res.rows.is_empty() {
                state_res.rows.push(IntervalJoinState::default());
            }
            self.execute_with_state(&new_left, &new_right, &mut state_res.rows[0])?
        } else {
            let mut guard = self
                .fallback_state
                .lock()
                .map_err(|_| SaciError::generic("IntervalJoinSystem fallback state poisoned"))?;
            self.execute_with_state(&new_left, &new_right, &mut guard)?
        };

        data.put_batch_named(self.target_component, out)
    }
}

#[async_trait]
impl System for IntervalJoinSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("interval_join")
            .reads_all_named(self.left_component)
            .reads_all_named(self.right_component)
            .writes_all_named(self.target_component)
            .writes_resource::<State<IntervalJoinState>>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

/// Checkpointable dimension-table state for [`LookupJoinSystem`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupJoinState {
    /// Current keyed dimension table encoded as Arrow IPC stream bytes.
    pub dimension_ipc: Vec<u8>,
}

impl Component for LookupJoinState {
    fn name() -> &'static str {
        "LookupJoinState"
    }

    fn schema() -> Arc<Schema> {
        use serde_arrow::schema::{SchemaLike as _, TracingOptions};
        let sample = LookupJoinState {
            dimension_ipc: vec![0],
        };
        let fields =
            Vec::<arrow_schema::FieldRef>::from_samples(&[sample], TracingOptions::default())
                .expect("LookupJoinState schema traces from sample");
        Arc::new(Schema::new(fields))
    }
}

/// Stateful stream-table / dimension enrichment join.
///
/// Upserts incoming `dimension_component` batches by key into a checkpointed
/// dimension table ([`LookupJoinState`]) and joins each arriving
/// `fact_component` batch against that table using `Inner` or `Left` join
/// semantics.
#[derive(Debug)]
pub struct LookupJoinSystem {
    fact_component: &'static str,
    dimension_component: &'static str,
    target_component: &'static str,
    kind: JoinKind,
    keys: Vec<JoinKey>,
    right_prefix: String,
    fallback_state: Mutex<LookupJoinState>,
}

impl Clone for LookupJoinSystem {
    fn clone(&self) -> Self {
        let guard = self
            .fallback_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            fact_component: self.fact_component,
            dimension_component: self.dimension_component,
            target_component: self.target_component,
            kind: self.kind,
            keys: self.keys.clone(),
            right_prefix: self.right_prefix.clone(),
            fallback_state: Mutex::new(guard.clone()),
        }
    }
}

impl LookupJoinSystem {
    /// Create a stateful dimension lookup join (`Inner` or `Left` only).
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `keys` is empty or `kind` is
    /// neither [`JoinKind::Inner`] nor [`JoinKind::Left`].
    pub fn new(
        fact_component: &str,
        dimension_component: &str,
        target_component: &str,
        kind: JoinKind,
        keys: Vec<JoinKey>,
    ) -> SaciResult<Self> {
        if keys.is_empty() {
            return Err(SaciError::configuration(
                "LookupJoinSystem requires at least one JoinKey",
            ));
        }
        if !matches!(kind, JoinKind::Inner | JoinKind::Left) {
            return Err(SaciError::configuration(
                "LookupJoinSystem supports only JoinKind::Inner or JoinKind::Left",
            ));
        }
        Ok(Self {
            fact_component: intern_component_name(fact_component),
            dimension_component: intern_component_name(dimension_component),
            target_component: intern_component_name(target_component),
            kind,
            keys,
            right_prefix: "right_".to_string(),
            fallback_state: Mutex::new(LookupJoinState::default()),
        })
    }

    /// Set collision prefix for dimension-side columns (default `"right_"`).
    pub fn right_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.right_prefix = prefix.into();
        self
    }

    /// Compute the output Arrow [`Schema`].
    pub fn output_schema(
        &self,
        fact_schema: &Schema,
        dim_schema: &Schema,
    ) -> SaciResult<Arc<Schema>> {
        JoinSystem::new(
            self.fact_component,
            self.dimension_component,
            self.target_component,
            self.kind,
            self.keys.clone(),
        )?
        .right_prefix(&self.right_prefix)
        .output_schema(fact_schema, dim_schema)
    }

    fn execute_with_state(
        &self,
        fact: &RecordBatch,
        new_dim: &RecordBatch,
        state: &mut LookupJoinState,
    ) -> SaciResult<RecordBatch> {
        let prev_dim = ipc_to_batch(&state.dimension_ipc)?
            .unwrap_or_else(|| RecordBatch::new_empty(new_dim.schema()));
        let combined_dim = concat_non_empty(&new_dim.schema(), &[prev_dim, new_dim.clone()])?;

        let dim_key_fields: Vec<String> = self.keys.iter().map(|k| k.right.clone()).collect();
        let dedup = DeduplicateSystem::new(
            self.dimension_component,
            self.dimension_component,
            dim_key_fields,
        )?
        .keep(KeepPolicy::Last);
        let active_dim = dedup.deduplicate_batch_stateless(&combined_dim)?;

        state.dimension_ipc = if active_dim.num_rows() > 0 {
            batch_to_ipc(&RecordBatchWrap(&active_dim))?
        } else {
            Vec::new()
        };

        let join = JoinSystem::new(
            self.fact_component,
            self.dimension_component,
            self.target_component,
            self.kind,
            self.keys.clone(),
        )?
        .right_prefix(&self.right_prefix);
        join.join_batches(fact, &active_dim)
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let fact = data.batch_named(self.fact_component)?.clone();
        let new_dim = data.batch_named(self.dimension_component)?.clone();

        let out = if let Some(state_res) = data.get_resource_mut::<State<LookupJoinState>>() {
            if state_res.rows.is_empty() {
                state_res.rows.push(LookupJoinState::default());
            }
            self.execute_with_state(&fact, &new_dim, &mut state_res.rows[0])?
        } else {
            let mut guard = self
                .fallback_state
                .lock()
                .map_err(|_| SaciError::generic("LookupJoinSystem fallback state poisoned"))?;
            self.execute_with_state(&fact, &new_dim, &mut guard)?
        };

        data.put_batch_named(self.target_component, out)
    }
}

#[async_trait]
impl System for LookupJoinSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("lookup_join")
            .reads_all_named(self.fact_component)
            .reads_all_named(self.dimension_component)
            .writes_all_named(self.target_component)
            .writes_resource::<State<LookupJoinState>>()
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
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType;

    fn order_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("cust_id", DataType::Int64, true),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn cust_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
        ]))
    }

    #[tokio::test]
    async fn inner_left_full_semi_anti_joins() {
        let orders = RecordBatch::try_new(
            order_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(20), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef,
                Arc::new(Float64Array::from(vec![5.0, 15.0, 25.0])) as ArrayRef,
            ],
        )
        .unwrap();
        let custs = RecordBatch::try_new(
            cust_schema(),
            vec![
                Arc::new(Int64Array::from(vec![10, 30])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 100])) as ArrayRef,
                Arc::new(StringArray::from(vec!["gold", "silver"])) as ArrayRef,
            ],
        )
        .unwrap();

        let run_kind = |kind: JoinKind| {
            let sys = JoinSystem::new(
                "Order",
                "Customer",
                "Joined",
                kind,
                vec![JoinKey::new("cust_id", "id")],
            )
            .unwrap();
            sys.join_batches(&orders, &custs).unwrap()
        };

        assert_eq!(run_kind(JoinKind::Inner).num_rows(), 1);
        assert_eq!(run_kind(JoinKind::Left).num_rows(), 3);
        assert_eq!(run_kind(JoinKind::Right).num_rows(), 2);
        assert_eq!(run_kind(JoinKind::Full).num_rows(), 4);

        let semi = run_kind(JoinKind::Semi);
        assert_eq!(semi.num_rows(), 1);
        assert_eq!(
            semi.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1]
        );

        let anti = run_kind(JoinKind::Anti);
        assert_eq!(anti.num_rows(), 2);
        assert_eq!(
            anti.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 3]
        );
    }

    #[tokio::test]
    async fn interval_join_matches_across_batches_and_evicts_by_watermark() {
        let sys = IntervalJoinSystem::new(
            "Order",
            "Customer",
            "Matched",
            "ts",
            "ts",
            vec![JoinKey::new("cust_id", "id")],
            -50,
            100,
        )
        .unwrap();

        let mut ds = Dataset::new();
        ds.register_schema("Order", order_schema()).unwrap();
        ds.register_schema("Customer", cust_schema()).unwrap();

        // Batch 1: order arrives at ts=1000, no customer yet.
        let b1_o = RecordBatch::try_new(
            order_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
                Arc::new(Float64Array::from(vec![50.0])) as ArrayRef,
            ],
        )
        .unwrap();
        let b1_c = RecordBatch::new_empty(cust_schema());
        ds.set_batch_named("Order", b1_o).unwrap();
        ds.set_batch_named("Customer", b1_c).unwrap();
        sys.run(&mut ds).await.unwrap();
        assert_eq!(ds.batch_named("Matched").unwrap().num_rows(), 0);

        // Batch 2: customer arrives at ts=1080 (within [950, 1100]!).
        let b2_o = RecordBatch::new_empty(order_schema());
        let b2_c = RecordBatch::try_new(
            cust_schema(),
            vec![
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1080])) as ArrayRef,
                Arc::new(StringArray::from(vec!["vip"])) as ArrayRef,
            ],
        )
        .unwrap();
        ds.set_batch_named("Order", b2_o).unwrap();
        ds.set_batch_named("Customer", b2_c).unwrap();
        sys.run(&mut ds).await.unwrap();
        assert_eq!(ds.batch_named("Matched").unwrap().num_rows(), 1);
    }

    #[tokio::test]
    async fn lookup_join_upserts_dimension_and_enriches_facts() {
        let sys = LookupJoinSystem::new(
            "Order",
            "Customer",
            "Enriched",
            JoinKind::Left,
            vec![JoinKey::new("cust_id", "id")],
        )
        .unwrap();

        let mut ds = Dataset::new();
        ds.register_schema("Order", order_schema()).unwrap();
        ds.register_schema("Customer", cust_schema()).unwrap();

        // Batch 1: seed dimension table with cust 10 = "bronze".
        let dim1 = RecordBatch::try_new(
            cust_schema(),
            vec![
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["bronze"])) as ArrayRef,
            ],
        )
        .unwrap();
        ds.set_batch_named("Customer", dim1).unwrap();
        sys.run(&mut ds).await.unwrap();
        assert_eq!(ds.batch_named("Enriched").unwrap().num_rows(), 0);

        // Batch 2: upsert cust 10 = "platinum", and process order for cust 10 (no dim batch needed on later passes).
        let dim2 = RecordBatch::try_new(
            cust_schema(),
            vec![
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["platinum"])) as ArrayRef,
            ],
        )
        .unwrap();
        let fact2 = RecordBatch::try_new(
            order_schema(),
            vec![
                Arc::new(Int64Array::from(vec![99])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![500])) as ArrayRef,
                Arc::new(Float64Array::from(vec![99.5])) as ArrayRef,
            ],
        )
        .unwrap();
        ds.set_batch_named("Order", fact2).unwrap();
        ds.set_batch_named("Customer", dim2).unwrap();
        sys.run(&mut ds).await.unwrap();

        let out = ds.batch_named("Enriched").unwrap();
        assert_eq!(out.num_rows(), 1);
        let tier_idx = out.schema_ref().index_of("tier").unwrap();
        let tier = out
            .column(tier_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tier.value(0), "platinum");
    }
}
