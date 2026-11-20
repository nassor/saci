use arrow_array::ArrayRef;
use arrow_array::RecordBatch;

use crate::column::ComponentView;
use crate::component::Component;
use crate::error::{SaciError, SaciResult};
use crate::schema::SchemaRegistry;
use crate::system::FieldRef;

use super::Dataset;

impl Dataset {
    /// The whole `RecordBatch` of component `C`.
    ///
    /// # Errors
    ///
    /// [`SaciError::ComponentNotRegistered`] if `C` is not registered.
    pub fn batch<C: Component>(&self) -> SaciResult<&RecordBatch> {
        self.batch_named(C::name())
    }

    /// The whole `RecordBatch` of the component named `component`.
    ///
    /// # Errors
    ///
    /// [`SaciError::ComponentNotRegistered`] if no component of that name is
    /// registered.
    pub fn batch_named(&self, component: &str) -> SaciResult<&RecordBatch> {
        let (&name, _) = self
            .components
            .get_key_value(component)
            .ok_or_else(|| SaciError::component_not_registered(component))?;
        Ok(self.get_or_build_merged(name))
    }

    /// One field's array of component `C`.
    ///
    /// # Errors
    ///
    /// [`SaciError::ComponentNotRegistered`] if `C` is not registered,
    /// [`SaciError::FieldNotFound`] if its schema has no such field.
    ///
    /// # Example
    ///
    /// ```rust
    /// use saci::prelude::*;
    /// use arrow_array::Float64Array;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Component, Serialize, Deserialize)]
    /// struct Temp { celsius: f64 }
    ///
    /// let mut dataset = Dataset::new();
    /// dataset.register::<Temp>().unwrap();
    /// dataset.append::<Temp>(&[Temp { celsius: 36.6 }]).unwrap();
    ///
    /// let col = dataset.column(Temp::CELSIUS).unwrap();
    /// let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
    /// assert!((arr.value(0) - 36.6).abs() < 1e-9);
    /// ```
    pub fn column<C: Component>(&self, field: FieldRef<C>) -> SaciResult<&ArrayRef> {
        self.column_named(C::name(), field.name())
    }

    /// One field's array of the component named `component`.
    ///
    /// # Errors
    ///
    /// [`SaciError::ComponentNotRegistered`] if no component of that name is
    /// registered, [`SaciError::FieldNotFound`] if its schema has no such
    /// field.
    pub fn column_named(&self, component: &str, field: &str) -> SaciResult<&ArrayRef> {
        let batch = self.batch_named(component)?;
        let idx = batch
            .schema_ref()
            .index_of(field)
            .map_err(|_| SaciError::field_not_found(component, field))?;
        Ok(batch.column(idx))
    }

    /// Return a typed [`ComponentView`] for component `C`.
    ///
    /// Column access without manual batch unpacking and downcasting.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::ComponentNotRegistered`] if `C` is not registered.
    ///
    /// # Example
    ///
    /// ```rust
    /// use saci::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Component, Serialize, Deserialize)]
    /// struct Price { value: f64 }
    ///
    /// # fn main() -> SaciResult<()> {
    /// let mut dataset = Dataset::new();
    /// dataset.register::<Price>()?;
    /// dataset.append::<Price>(&[Price { value: 42.0 }])?;
    /// let view = dataset.view::<Price>()?;
    /// let values = view.f64("value")?;
    /// assert_eq!(values.value(0), 42.0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn view<C: Component>(&self) -> SaciResult<ComponentView<'_, C>> {
        Ok(ComponentView::new(self.batch::<C>()?))
    }

    /// Return a reference to the [`SchemaRegistry`] for schema introspection
    /// and dataset validation.
    pub fn schemas(&self) -> &SchemaRegistry {
        &self.schemas
    }

    /// Total row count, including soft-deleted rows.
    pub fn rows(&self) -> usize {
        self.row_count
    }

    /// Number of alive (not soft-deleted) rows.
    pub fn live_rows(&self) -> usize {
        self.live_count
    }

    /// Return `true` if the given row has not been soft-deleted.
    ///
    /// Out-of-bounds rows always return `false`.
    pub fn is_alive(&self, row: crate::row::Row) -> bool {
        let idx = row.index();
        if idx >= self.alive.len() {
            return false;
        }
        self.alive.get_bit(idx)
    }

    /// Return the full row range `0..row_count` as a `Range<u32>`.
    ///
    /// Used by [`ParallelSystem`](crate::system::ParallelSystem)
    /// implementations to determine the slice boundaries for intra-system
    /// parallelism.
    pub fn row_range(&self) -> std::ops::Range<u32> {
        0..self.row_count as u32
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::row::Row;
    use crate::system::FieldRef;

    #[derive(Serialize, Deserialize)]
    struct ReadOrder {
        id: u64,
        total: f64,
    }

    impl ReadOrder {
        const ID: FieldRef<ReadOrder> = FieldRef::new("id");
        const TOTAL: FieldRef<ReadOrder> = FieldRef::new("total");
        const MISSING: FieldRef<ReadOrder> = FieldRef::new("nonexistent");
    }

    impl Component for ReadOrder {
        fn name() -> &'static str {
            "ReadOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_column_returns_values() {
        let mut ds = Dataset::new();
        ds.register::<ReadOrder>().unwrap();
        ds.append::<ReadOrder>(&[ReadOrder { id: 1, total: 9.9 }])
            .unwrap();
        let col = ds.column(ReadOrder::TOTAL).unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 9.9).abs() < 1e-9);
    }

    #[test]
    fn test_column_missing_field_is_field_not_found() {
        let mut ds = Dataset::new();
        ds.register::<ReadOrder>().unwrap();
        ds.append::<ReadOrder>(&[ReadOrder { id: 1, total: 1.0 }])
            .unwrap();
        let err = ds.column(ReadOrder::MISSING).unwrap_err();
        assert_eq!(err.category(), "field_not_found");
    }

    #[test]
    fn test_column_unregistered_component_is_component_not_registered() {
        let ds = Dataset::new();
        let err = ds.column(ReadOrder::ID).unwrap_err();
        assert_eq!(err.category(), "component_not_registered");
    }

    #[test]
    fn test_batch_named_takes_a_borrowed_name() {
        let mut ds = Dataset::new();
        ds.register::<ReadOrder>().unwrap();
        let owned = String::from("ReadOrder");
        assert_eq!(ds.batch_named(&owned).unwrap().num_columns(), 2);
    }

    #[test]
    fn test_is_alive_out_of_bounds() {
        let ds = Dataset::new();
        assert!(!ds.is_alive(Row::new(99)));
    }

    #[test]
    fn test_row_range() {
        let mut ds = Dataset::new();
        ds.register::<ReadOrder>().unwrap();
        ds.append::<ReadOrder>(&[
            ReadOrder { id: 0, total: 0.0 },
            ReadOrder { id: 1, total: 1.0 },
        ])
        .unwrap();
        assert_eq!(ds.row_range(), 0..2);
    }

    #[test]
    fn world_view_missing_component() {
        let ds = Dataset::new();
        let err = ds.view::<ReadOrder>().err().unwrap();
        assert_eq!(err.category(), "component_not_registered");
        assert!(err.to_string().contains("ReadOrder"), "got: {err}");
    }
}
