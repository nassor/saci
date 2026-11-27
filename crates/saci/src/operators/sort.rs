//! Lexicographic sorting (`SortSystem`) and bounded Top-N (`TopNSystem`)
//! operators over Arrow `RecordBatch`es.

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_select::take::take;
use async_trait::async_trait;

use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::system::{System, SystemMeta};

/// A single column sort directive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    /// Column name to sort by.
    pub field: String,
    /// `true` for descending order, `false` for ascending.
    pub descending: bool,
    /// `true` to place nulls before non-null values.
    pub nulls_first: bool,
}

impl SortKey {
    /// Ascending sort over `field` (nulls last).
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: false,
            nulls_first: false,
        }
    }

    /// Descending sort over `field` (nulls last).
    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: true,
            nulls_first: false,
        }
    }

    /// Configure whether nulls sort first (`true`) or last (`false`).
    pub fn nulls_first(mut self, first: bool) -> Self {
        self.nulls_first = first;
        self
    }
}

/// Sorts `source_component` by one or more [`SortKey`]s with optional `limit`
/// and `offset`, writing the sorted batch to `target_component`.
#[derive(Debug, Clone)]
pub struct SortSystem {
    source_component: &'static str,
    target_component: &'static str,
    keys: Vec<SortKey>,
    limit: Option<usize>,
    offset: usize,
}

impl SortSystem {
    /// Create a sort operator reading `source_component` and writing
    /// `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `keys` is empty.
    pub fn new(
        source_component: &str,
        target_component: &str,
        keys: Vec<SortKey>,
    ) -> SaciResult<Self> {
        if keys.is_empty() {
            return Err(SaciError::configuration(
                "SortSystem requires at least one SortKey",
            ));
        }
        Ok(Self {
            source_component: intern_component_name(source_component),
            target_component: intern_component_name(target_component),
            keys,
            limit: None,
            offset: 0,
        })
    }

    /// Sort `component` in place.
    ///
    /// # Errors
    ///
    /// Same as [`Self::new`].
    pub fn in_place(component: &str, keys: Vec<SortKey>) -> SaciResult<Self> {
        Self::new(component, component, keys)
    }

    /// Cap output rows to at most `limit` (after applying `offset`).
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skip the first `offset` sorted rows.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Sort `batch` and apply `offset`/`limit`.
    pub fn execute_batch(&self, batch: &RecordBatch) -> SaciResult<RecordBatch> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }
        let sort_cols: SaciResult<Vec<SortColumn>> = self
            .keys
            .iter()
            .map(|k| {
                let idx = batch
                    .schema_ref()
                    .index_of(&k.field)
                    .map_err(|_| SaciError::field_not_found(self.source_component, &k.field))?;
                Ok(SortColumn {
                    values: batch.column(idx).clone(),
                    options: Some(SortOptions {
                        descending: k.descending,
                        nulls_first: k.nulls_first,
                    }),
                })
            })
            .collect();
        let sort_cols = sort_cols?;

        let fetch_limit = self.limit.map(|l| self.offset.saturating_add(l));
        let indices: UInt32Array = lexsort_to_indices(&sort_cols, fetch_limit)
            .map_err(|e| SaciError::generic(format!("SortSystem lexsort error: {e}")))?;

        let start = self.offset.min(indices.len());
        let len = match self.limit {
            Some(l) => l.min(indices.len().saturating_sub(start)),
            None => indices.len().saturating_sub(start),
        };
        if len == 0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }
        let sliced = indices.slice(start, len);
        let cols: SaciResult<Vec<ArrayRef>> = batch
            .columns()
            .iter()
            .map(|c| {
                take(c.as_ref(), &sliced, None)
                    .map_err(|e| SaciError::generic(format!("SortSystem take error: {e}")))
            })
            .collect();
        RecordBatch::try_new(batch.schema(), cols?)
            .map_err(|e| SaciError::generic(format!("SortSystem RecordBatch error: {e}")))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let input = data.batch_named(self.source_component)?.clone();
        let sorted = self.execute_batch(&input)?;
        data.put_batch_named(self.target_component, sorted)
    }
}

#[async_trait]
impl System for SortSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("sort")
            .reads_all_named(self.source_component)
            .writes_all_named(self.target_component)
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

/// Convenience Top-N operator built on [`SortSystem`] with a mandatory row limit.
#[derive(Debug, Clone)]
pub struct TopNSystem {
    inner: SortSystem,
}

impl TopNSystem {
    /// Keep the top `limit` rows of `source_component` ordered by `keys`,
    /// writing to `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `keys` is empty.
    pub fn new(
        source_component: &str,
        target_component: &str,
        keys: Vec<SortKey>,
        limit: usize,
    ) -> SaciResult<Self> {
        Ok(Self {
            inner: SortSystem::new(source_component, target_component, keys)?.limit(limit),
        })
    }

    /// Keep the top `limit` rows of `component` in place.
    ///
    /// # Errors
    ///
    /// Same as [`Self::new`].
    pub fn in_place(component: &str, keys: Vec<SortKey>, limit: usize) -> SaciResult<Self> {
        Self::new(component, component, keys, limit)
    }

    /// Skip `offset` rows before taking `limit` rows.
    pub fn offset(mut self, offset: usize) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    /// Execute Top-N against `batch`.
    pub fn execute_batch(&self, batch: &RecordBatch) -> SaciResult<RecordBatch> {
        self.inner.execute_batch(batch)
    }
}

#[async_trait]
impl System for TopNSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("top_n")
            .reads_all_named(self.inner.source_component)
            .writes_all_named(self.inner.target_component)
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.inner.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.inner.run_impl(data))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[tokio::test]
    async fn top_n_orders_and_slices_correctly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(Float64Array::from(vec![10.0, 50.0, 30.0, 40.0])) as ArrayRef,
            ],
        )
        .unwrap();

        let mut ds = Dataset::new();
        ds.register_schema("Item", schema).unwrap();
        ds.append_record_batch("Item", batch).unwrap();

        let sys = TopNSystem::in_place("Item", vec![SortKey::desc("score")], 2)
            .unwrap()
            .offset(1);
        sys.run(&mut ds).await.unwrap();

        let out = ds.batch_named("Item").unwrap();
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[4, 3]);
    }
}
