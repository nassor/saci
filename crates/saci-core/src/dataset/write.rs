use arrow_array::{ArrayRef, RecordBatch};

use crate::component::Component;
use crate::error::SaciError;

use super::Dataset;

impl Dataset {
    /// Replace the entire `RecordBatch` for component `C` with `new_batch`.
    ///
    /// Used by systems that need to update multiple fields in a single
    /// batch write. The new batch must have the same number of rows and an
    /// identical schema to the currently stored batch.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if:
    /// - The component is not registered.
    /// - The new batch's schema does not match the registered schema.
    /// - The new batch has a different row count than the current batch.
    pub fn replace_batch<C: Component>(&mut self, new_batch: RecordBatch) -> Result<(), SaciError> {
        let name = C::name();
        let registered_schema = self.schemas.get(name).ok_or_else(|| {
            SaciError::generic(format!(
                "Dataset::replace_batch: component '{name}' is not registered"
            ))
        })?;

        if new_batch.schema_ref().fields() != registered_schema.fields() {
            return Err(SaciError::generic(format!(
                "Dataset::replace_batch: schema mismatch for '{name}': \
                 got {:?}, expected {:?}",
                new_batch.schema_ref(),
                registered_schema
            )));
        }

        self.flush_all_pending()?;

        let existing_rows: usize = self
            .components
            .get(name)
            .expect("component registered but missing")
            .iter()
            .map(|b| b.num_rows())
            .sum();

        if new_batch.num_rows() != existing_rows {
            return Err(SaciError::generic(format!(
                "Dataset::replace_batch: row count mismatch for '{name}': \
                 new batch has {} rows, existing has {} rows",
                new_batch.num_rows(),
                existing_rows
            )));
        }

        self.components.insert(name, vec![new_batch]);
        self.merged_cache.get_mut().unwrap().remove(name);
        Ok(())
    }

    /// Atomically apply a [`WriteSet`](crate::system::WriteSet) produced
    /// by a [`ParallelSystem`](crate::system::ParallelSystem).
    ///
    /// For each `(component_name, field_name) → ArrayRef` entry:
    ///
    /// 1. Validates the component is registered.
    /// 2. Validates the array length matches the current row count.
    /// 3. Replaces the field column in the component's `RecordBatch`.
    ///
    /// Resource updates attached to the `WriteSet` are applied last, in order.
    ///
    /// On error, the dataset is left **partially modified** for the successfully
    /// applied fields.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if:
    /// - A component is not registered.
    /// - An array's length differs from `self.row_count`.
    /// - Rebuilding the `RecordBatch` fails (schema mismatch).
    pub fn apply_write_set(&mut self, write_set: crate::system::WriteSet) -> Result<(), SaciError> {
        self.flush_all_pending()?;

        // Write sets touch one component in nearly all cases and a handful at
        // most, so group with a `Vec` and a linear scan. A `HashMap` here would
        // allocate and hash on a path stream mode runs once per item.
        let mut by_component: Vec<(&'static str, Vec<(&'static str, ArrayRef)>)> =
            Vec::with_capacity(1);
        for ((component, field), array) in write_set.fields {
            if array.len() != self.row_count {
                return Err(SaciError::generic(format!(
                    "Dataset::apply_write_set: field '{component}.{field}' has {} values, \
                     expected {} (dataset row_count)",
                    array.len(),
                    self.row_count
                )));
            }
            match by_component.iter_mut().find(|(name, _)| *name == component) {
                Some((_, updates)) => updates.push((field, array)),
                None => by_component.push((component, vec![(field, array)])),
            }
        }

        for (component_name, field_updates) in by_component {
            let chunks = self.components.get(component_name).ok_or_else(|| {
                SaciError::generic(format!(
                    "Dataset::apply_write_set: component '{component_name}' is not registered"
                ))
            })?;

            let existing = chunks
                .first()
                .expect("component vec empty after flush — internal inconsistency");
            let schema = existing.schema();
            let mut columns: Vec<ArrayRef> = existing.columns().to_vec();

            for (field_name, new_array) in field_updates {
                let idx = schema.index_of(field_name).map_err(|_| {
                    SaciError::generic(format!(
                        "Dataset::apply_write_set: field '{field_name}' not found in component '{component_name}'"
                    ))
                })?;
                columns[idx] = new_array;
            }

            let new_batch = RecordBatch::try_new(schema, columns)
                .map_err(|e| SaciError::generic(format!("apply_write_set rebuild error: {e}")))?;
            self.components.insert(component_name, vec![new_batch]);
            self.merged_cache.get_mut().unwrap().remove(component_name);
        }

        for update in write_set.resource_updates {
            update.apply(self);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::system::WriteSet;

    #[derive(Serialize, Deserialize)]
    struct WOrder {
        id: u64,
        total: f64,
    }

    impl Component for WOrder {
        fn name() -> &'static str {
            "WOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_apply_write_set_updates_field() {
        let mut ds = Dataset::new();
        ds.register_component::<WOrder>().unwrap();
        ds.append::<WOrder>(&[WOrder { id: 1, total: 1.0 }, WOrder { id: 2, total: 2.0 }])
            .unwrap();

        let new_totals: Arc<dyn arrow_array::Array> =
            Arc::new(Float64Array::from(vec![10.0, 20.0]));
        let ws = WriteSet::new().put("WOrder", "total", new_totals);
        ds.apply_write_set(ws).unwrap();

        let col = ds.column::<WOrder>("total").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 10.0).abs() < 1e-9);
        assert!((arr.value(1) - 20.0).abs() < 1e-9);
    }

    #[test]
    fn test_replace_batch_updates_rows() {
        use arrow_array::RecordBatch as RB;
        let mut ds = Dataset::new();
        ds.register_component::<WOrder>().unwrap();
        ds.append::<WOrder>(&[WOrder { id: 1, total: 1.0 }])
            .unwrap();

        let schema = WOrder::schema();
        let new_batch = RB::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![99u64])) as Arc<dyn arrow_array::Array>,
                Arc::new(Float64Array::from(vec![99.0])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();
        ds.replace_batch::<WOrder>(new_batch).unwrap();

        let col = ds.column::<WOrder>("id").unwrap();
        let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(arr.value(0), 99u64);
    }
}
