//! `UnnestSystem`: explodes an Arrow `List` or `LargeList` column into one
//! output row per element while replicating all sibling columns.

use std::sync::Arc;

use arrow_array::{Array, LargeListArray, ListArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use async_trait::async_trait;

use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::system::{System, SystemMeta};

/// Explodes a `List` or `LargeList` column (`list_field`) into `output_field`,
/// producing one row per list item and repeating sibling columns.
#[derive(Debug, Clone)]
pub struct UnnestSystem {
    source_component: &'static str,
    target_component: &'static str,
    list_field: String,
    output_field: String,
    preserve_empty: bool,
}

impl UnnestSystem {
    /// Explode `list_field` in `source_component` into `output_field` in
    /// `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when either field name is empty.
    pub fn new(
        source_component: &str,
        target_component: &str,
        list_field: impl Into<String>,
        output_field: impl Into<String>,
    ) -> SaciResult<Self> {
        let list_field = list_field.into();
        let output_field = output_field.into();
        if list_field.is_empty() || output_field.is_empty() {
            return Err(SaciError::configuration(
                "UnnestSystem requires non-empty list_field and output_field",
            ));
        }
        Ok(Self {
            source_component: intern_component_name(source_component),
            target_component: intern_component_name(target_component),
            list_field,
            output_field,
            preserve_empty: false,
        })
    }

    /// Emit a single row with a null `output_field` when an input row's list
    /// is empty or null (outer unnest semantics). Defaults to `false`.
    pub fn preserve_empty(mut self, preserve: bool) -> Self {
        self.preserve_empty = preserve;
        self
    }

    /// Compute the output Arrow [`Schema`] for `input_schema`.
    ///
    /// Replaces `list_field` in place (at the same column position) with a
    /// field named `output_field` carrying the list's inner item type.
    pub fn output_schema(&self, input_schema: &Schema) -> SaciResult<Arc<Schema>> {
        let list_idx = input_schema
            .index_of(&self.list_field)
            .map_err(|_| SaciError::field_not_found(self.source_component, &self.list_field))?;
        if self.output_field != self.list_field
            && let Ok(existing_idx) = input_schema.index_of(&self.output_field)
            && existing_idx != list_idx
        {
            return Err(SaciError::configuration(format!(
                "UnnestSystem: output_field '{}' collides with an existing column in component '{}'",
                self.output_field, self.source_component
            )));
        }
        let item_field = match input_schema.field(list_idx).data_type() {
            DataType::List(inner) | DataType::LargeList(inner) => inner.clone(),
            other => {
                return Err(SaciError::generic(format!(
                    "UnnestSystem: field '{}' is {other:?}, expected List or LargeList",
                    self.list_field
                )));
            }
        };
        let nullable = item_field.is_nullable() || self.preserve_empty;
        let out_field = Field::new(&self.output_field, item_field.data_type().clone(), nullable);

        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields[list_idx] = out_field;
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Execute unnesting over `batch`.
    pub fn execute_batch(&self, batch: &RecordBatch) -> SaciResult<RecordBatch> {
        let out_schema = self.output_schema(batch.schema_ref())?;
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        let list_idx = batch
            .schema_ref()
            .index_of(&self.list_field)
            .map_err(|_| SaciError::field_not_found(self.source_component, &self.list_field))?;
        let list_col = batch.column(list_idx);

        let (row_indices, child_indices, values) = match list_col.data_type() {
            DataType::List(_) => {
                let arr = list_col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| SaciError::generic("UnnestSystem: downcast ListArray failed"))?;
                let offsets = arr.value_offsets();
                let mut row_idx = Vec::new();
                let mut child_idx: Vec<Option<u32>> = Vec::new();
                for r in 0..arr.len() {
                    let start = offsets[r] as usize;
                    let end = offsets[r + 1] as usize;
                    if arr.is_null(r) || start == end {
                        if self.preserve_empty {
                            row_idx.push(r as u32);
                            child_idx.push(None);
                        }
                    } else {
                        for c in start..end {
                            row_idx.push(r as u32);
                            child_idx.push(Some(c as u32));
                        }
                    }
                }
                (row_idx, child_idx, arr.values().clone())
            }
            DataType::LargeList(_) => {
                let arr = list_col
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        SaciError::generic("UnnestSystem: downcast LargeListArray failed")
                    })?;
                let offsets = arr.value_offsets();
                let mut row_idx = Vec::new();
                let mut child_idx: Vec<Option<u32>> = Vec::new();
                for r in 0..arr.len() {
                    let start = offsets[r] as usize;
                    let end = offsets[r + 1] as usize;
                    if arr.is_null(r) || start == end {
                        if self.preserve_empty {
                            row_idx.push(r as u32);
                            child_idx.push(None);
                        }
                    } else {
                        for c in start..end {
                            row_idx.push(r as u32);
                            child_idx.push(Some(c as u32));
                        }
                    }
                }
                (row_idx, child_idx, arr.values().clone())
            }
            other => {
                return Err(SaciError::generic(format!(
                    "UnnestSystem: field '{}' is {other:?}, expected List or LargeList",
                    self.list_field
                )));
            }
        };

        if row_indices.is_empty() {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        let row_take = UInt32Array::from(row_indices);
        let child_take = UInt32Array::from(child_indices);
        let mut out_cols = Vec::with_capacity(batch.num_columns());
        for (i, col) in batch.columns().iter().enumerate() {
            if i == list_idx {
                out_cols.push(take(values.as_ref(), &child_take, None).map_err(|e| {
                    SaciError::generic(format!("UnnestSystem take element error: {e}"))
                })?);
            } else {
                out_cols.push(take(col.as_ref(), &row_take, None).map_err(|e| {
                    SaciError::generic(format!("UnnestSystem take sibling error: {e}"))
                })?);
            }
        }

        RecordBatch::try_new(out_schema, out_cols)
            .map_err(|e| SaciError::generic(format!("UnnestSystem RecordBatch error: {e}")))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let input = data.batch_named(self.source_component)?.clone();
        let unnested = self.execute_batch(&input)?;
        data.put_batch_named(self.target_component, unnested)
    }
}

#[async_trait]
impl System for UnnestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("unnest")
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_buffer::OffsetBuffer;

    fn make_list_batch() -> RecordBatch {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tags", DataType::List(item_field.clone()), true),
        ]));
        let values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        // Row 0: ["a", "b"], Row 1: [], Row 2: ["c"]
        let offsets = OffsetBuffer::from_lengths([2usize, 0, 1]);
        let list = ListArray::try_new(item_field, offsets, values, None).unwrap();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(list) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn unnest_explodes_rows_and_respects_preserve_empty() {
        let batch = make_list_batch();
        let mut ds = Dataset::new();
        ds.register_schema("Src", batch.schema()).unwrap();
        ds.append_record_batch("Src", batch).unwrap();

        let sys = UnnestSystem::new("Src", "Out", "tags", "tag").unwrap();
        sys.run(&mut ds).await.unwrap();

        let out = ds.batch_named("Out").unwrap();
        assert_eq!(out.num_rows(), 3);
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let tags = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 1, 3]);
        assert_eq!(tags.value(0), "a");
        assert_eq!(tags.value(1), "b");
        assert_eq!(tags.value(2), "c");

        let outer = UnnestSystem::new("Src", "OutOuter", "tags", "tag")
            .unwrap()
            .preserve_empty(true);
        outer.run(&mut ds).await.unwrap();

        let out_outer = ds.batch_named("OutOuter").unwrap();
        assert_eq!(out_outer.num_rows(), 4);
        let tags_outer = out_outer
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(tags_outer.is_null(2));
    }

    #[test]
    fn unnest_rejects_output_field_colliding_with_existing_column() {
        let batch = make_list_batch();
        let sys = UnnestSystem::new("Src", "Src", "tags", "id").unwrap();
        let err = sys.output_schema(batch.schema_ref()).unwrap_err();
        assert_eq!(err.category(), "configuration");
    }
}
