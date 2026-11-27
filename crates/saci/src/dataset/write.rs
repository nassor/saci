use arrow_array::{ArrayRef, RecordBatch};

use crate::component::Component;
use crate::error::SaciError;
use crate::system::{FieldRef, WriteSet};

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
    /// - [`SaciError::ComponentNotRegistered`] if `C` is not registered.
    /// - `SaciError::Generic` if the new batch's schema or row count differs
    ///   from the stored one.
    pub fn replace_batch<C: Component>(&mut self, new_batch: RecordBatch) -> Result<(), SaciError> {
        let name = C::name();
        let registered_schema = self
            .schemas
            .get(name)
            .ok_or_else(|| SaciError::component_not_registered(name))?;

        if new_batch.schema_ref().fields() != registered_schema.fields() {
            return Err(SaciError::generic(format!(
                "Dataset::replace_batch: schema mismatch for '{name}': \
                 got {:?}, expected {:?}",
                new_batch.schema_ref(),
                registered_schema
            )));
        }

        let chunks = self
            .components
            .get_mut(name)
            .expect("component registered but missing");
        let existing_rows: usize = chunks.iter().map(|b| b.num_rows()).sum();

        if new_batch.num_rows() != existing_rows {
            return Err(SaciError::generic(format!(
                "Dataset::replace_batch: row count mismatch for '{name}': \
                 new batch has {} rows, existing has {} rows",
                new_batch.num_rows(),
                existing_rows
            )));
        }

        chunks.clear();
        chunks.push(new_batch);
        self.merged_cache.get_mut().unwrap().remove(name);
        Ok(())
    }

    /// Replace the entire `RecordBatch` for component `C`, allowing its row
    /// count to differ from the current dataset row count.
    ///
    /// Updates [`Dataset::rows`](Self::rows) and the alive bitmap to the
    /// maximum row count across all registered components so IPC invariants
    /// hold when an operator grows (for example `Unnest` or `Join`) or shrinks
    /// (for example `Filter`, `Deduplicate`, `GroupBy`, or `TopN`) a component.
    ///
    /// # Errors
    ///
    /// - [`SaciError::ComponentNotRegistered`] if `C` is not registered.
    /// - `SaciError::Generic` if the new batch's schema differs from the
    ///   registered one.
    pub fn set_batch<C: Component>(&mut self, new_batch: RecordBatch) -> Result<(), SaciError> {
        let name = C::name();
        let registered_schema = self
            .schemas
            .get(name)
            .ok_or_else(|| SaciError::component_not_registered(name))?;
        if new_batch.schema_ref().fields() != registered_schema.fields() {
            return Err(SaciError::generic(format!(
                "Dataset::set_batch: schema mismatch for '{name}': \
                 got {:?}, expected {:?}",
                new_batch.schema_ref(),
                registered_schema
            )));
        }
        self.set_batch_named(name, new_batch)
    }

    /// Replace the entire `RecordBatch` for the named component, allowing its
    /// row count (or schema, for dynamic schema-changing operators) to differ
    /// from the current dataset.
    ///
    /// Updates [`Dataset::rows`](Self::rows) and the alive bitmap to the
    /// maximum row count across all registered components.
    ///
    /// # Errors
    ///
    /// - [`SaciError::ComponentNotRegistered`] if `component` is not
    ///   registered.
    pub fn set_batch_named(
        &mut self,
        component: &str,
        new_batch: RecordBatch,
    ) -> Result<(), SaciError> {
        let (&name, _) = self
            .components
            .get_key_value(component)
            .ok_or_else(|| SaciError::component_not_registered(component))?;
        let schema_changed = self
            .schemas
            .get(name)
            .is_none_or(|s| s.fields() != new_batch.schema_ref().fields());
        if schema_changed {
            self.schemas.update_schema(name, new_batch.schema());
        }

        let chunks = self
            .components
            .get_mut(name)
            .expect("component registered but missing");
        chunks.clear();
        chunks.push(new_batch);
        self.merged_cache.get_mut().unwrap().remove(name);

        let max_rows = self
            .components
            .values()
            .map(|c| c.iter().map(RecordBatch::num_rows).sum::<usize>())
            .max()
            .unwrap_or(0);
        self.row_count = max_rows;
        self.live_count = max_rows;
        self.dead_count = 0;
        self.alive.truncate(0);
        self.alive.append_n(max_rows, true);
        Ok(())
    }

    /// Inserts or replaces the `RecordBatch` for a dynamic component produced by
    /// an operator, registering the schema first if not yet present.
    pub(crate) fn put_batch_named(
        &mut self,
        component: &'static str,
        new_batch: RecordBatch,
    ) -> Result<(), SaciError> {
        if !self.components.contains_key(component) {
            let schema = new_batch.schema();
            self.schemas.register_raw(component, schema.clone(), 1);
            self.empty_templates
                .entry(component)
                .or_insert_with(|| RecordBatch::new_empty(schema));
            self.components.insert(component, Vec::new());
        }
        self.set_batch_named(component, new_batch)
    }

    /// Replace one column of component `C`, without building a
    /// [`WriteSet`].
    ///
    /// # Errors
    ///
    /// - [`SaciError::ComponentNotRegistered`] if `C` is not registered.
    /// - [`SaciError::FieldNotFound`] if `C` has no such field.
    /// - `SaciError::Generic` if `array`'s length differs from
    ///   [`rows`](Self::rows) or its type from the field's.
    pub fn set_column<C: Component>(
        &mut self,
        field: FieldRef<C>,
        array: ArrayRef,
    ) -> Result<(), SaciError> {
        self.apply_write_set(WriteSet::new().put(field, array))
    }

    /// Atomically apply a [`WriteSet`] produced by a
    /// [`ParallelSystem`](crate::system::ParallelSystem).
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
    /// - [`SaciError::ComponentNotRegistered`] if a component is not
    ///   registered.
    /// - [`SaciError::FieldNotFound`] if a component has no such field.
    /// - `SaciError::Generic` if an array's length differs from
    ///   `self.row_count` or rebuilding the `RecordBatch` fails.
    pub fn apply_write_set(&mut self, write_set: WriteSet) -> Result<(), SaciError> {
        let (fields, resource_updates) = write_set.into_parts();

        if self.components.values().any(|chunks| chunks.len() > 1) {
            self.flush_all_pending()?;
        }

        // Write sets touch one component in nearly all cases and a handful at
        // most, so group with a `Vec` and a linear scan. A `HashMap` here would
        // allocate and hash on a path stream mode runs once per item.
        let mut by_component: Vec<(&'static str, Vec<(&'static str, ArrayRef)>)> =
            Vec::with_capacity(1);
        for ((component, field), array) in fields {
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
            let chunks = self
                .components
                .get_mut(component_name)
                .ok_or_else(|| SaciError::component_not_registered(component_name))?;

            let existing = chunks
                .first()
                .expect("component vec empty after flush — internal inconsistency");
            let schema = existing.schema();
            let mut columns: Vec<ArrayRef> = existing.columns().to_vec();

            for (field_name, new_array) in field_updates {
                let idx = schema
                    .index_of(field_name)
                    .map_err(|_| SaciError::field_not_found(component_name, field_name))?;
                columns[idx] = new_array;
            }

            let new_batch = RecordBatch::try_new(schema, columns)
                .map_err(|e| SaciError::generic(format!("apply_write_set rebuild error: {e}")))?;
            chunks.clear();
            chunks.push(new_batch);
            self.merged_cache.get_mut().unwrap().remove(component_name);
        }

        for update in resource_updates {
            update.apply(self);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::system::{FieldRef, WriteSet};

    #[derive(Serialize, Deserialize)]
    struct WOrder {
        id: u64,
        total: f64,
    }

    impl WOrder {
        const ID: FieldRef<WOrder> = FieldRef::new("id");
        const TOTAL: FieldRef<WOrder> = FieldRef::new("total");
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

    fn two_orders() -> Dataset {
        let mut ds = Dataset::new();
        ds.register::<WOrder>().unwrap();
        ds.append::<WOrder>(&[WOrder { id: 1, total: 1.0 }, WOrder { id: 2, total: 2.0 }])
            .unwrap();
        ds
    }

    #[test]
    fn test_apply_write_set_updates_field() {
        let mut ds = two_orders();
        let new_totals: Arc<dyn arrow_array::Array> =
            Arc::new(Float64Array::from(vec![10.0, 20.0]));
        ds.apply_write_set(WriteSet::new().put(WOrder::TOTAL, new_totals))
            .unwrap();

        let col = ds.column(WOrder::TOTAL).unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 10.0).abs() < 1e-9);
        assert!((arr.value(1) - 20.0).abs() < 1e-9);
    }

    #[test]
    fn test_apply_write_set_unknown_field_is_field_not_found() {
        let mut ds = two_orders();
        let ws = WriteSet::new().put_named(
            "WOrder",
            "nope",
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        );
        assert_eq!(
            ds.apply_write_set(ws).unwrap_err().category(),
            "field_not_found"
        );
    }

    #[test]
    fn test_set_column_replaces_one_column() {
        let mut ds = two_orders();
        ds.set_column(WOrder::TOTAL, Arc::new(Float64Array::from(vec![5.0, 6.0])))
            .unwrap();
        let totals = ds.column(WOrder::TOTAL).unwrap();
        let totals = totals.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(totals.values(), &[5.0, 6.0]);
        let ids = ds.column(WOrder::ID).unwrap();
        let ids = ids.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2], "the other column is untouched");
    }

    #[test]
    fn test_set_column_rejects_a_wrong_length() {
        let mut ds = two_orders();
        let err = ds
            .set_column(WOrder::TOTAL, Arc::new(Float64Array::from(vec![5.0])))
            .unwrap_err();
        assert!(err.to_string().contains("has 1 values"), "{err}");
    }

    #[test]
    fn test_replace_batch_updates_rows() {
        use arrow_array::RecordBatch as RB;
        let mut ds = Dataset::new();
        ds.register::<WOrder>().unwrap();
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

        let col = ds.column(WOrder::ID).unwrap();
        let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(arr.value(0), 99u64);
    }

    #[test]
    fn test_set_batch_grows_and_shrinks_and_ipc_round_trips() {
        use arrow_array::RecordBatch as RB;
        let mut ds = Dataset::new();
        ds.register::<WOrder>().unwrap();
        ds.append::<WOrder>(&[WOrder { id: 1, total: 1.0 }, WOrder { id: 2, total: 2.0 }])
            .unwrap();
        assert_eq!(ds.rows(), 2);

        let grown = RB::try_new(
            WOrder::schema(),
            vec![
                Arc::new(UInt64Array::from(vec![10u64, 20, 30])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            ],
        )
        .unwrap();
        ds.set_batch::<WOrder>(grown).unwrap();
        assert_eq!(ds.rows(), 3);
        assert_eq!(ds.live_rows(), 3);

        let shrunk = RB::try_new(
            WOrder::schema(),
            vec![
                Arc::new(UInt64Array::from(vec![42u64])) as ArrayRef,
                Arc::new(Float64Array::from(vec![42.0])) as ArrayRef,
            ],
        )
        .unwrap();
        ds.set_batch_named("WOrder", shrunk).unwrap();
        assert_eq!(ds.rows(), 1);
        assert_eq!(ds.live_rows(), 1);

        let mut buf = Vec::new();
        ds.write_ipc(&mut buf).unwrap();
        let restored = Dataset::read_ipc(&mut &buf[..]).unwrap();
        assert_eq!(restored.rows(), 1);
        assert_eq!(restored.live_rows(), 1);
    }
}
