use arrow_array::ArrayRef;
use arrow_array::RecordBatch;

use crate::column::ComponentView;
use crate::component::Component;
use crate::error::{SaciError, SaciResult};
use crate::schema::SchemaRegistry;

use super::Dataset;

impl Dataset {
    /// Return a reference to the raw `RecordBatch` for `component_name`.
    ///
    /// Returns `None` if the component is not registered.
    pub fn batch_for(&self, component_name: &'static str) -> Option<&RecordBatch> {
        if !self.schemas.contains(component_name) {
            return None;
        }
        Some(self.get_or_build_merged(component_name))
    }

    /// Return a reference to one field's array within component `C`'s batch.
    ///
    /// Returns `None` if the component is not registered or the field name
    /// doesn't exist in the schema.
    ///
    /// # Example
    ///
    /// ```rust
    /// #
    /// # {
    /// use std::sync::Arc;
    /// use arrow_array::Float64Array;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use saci_core::dataset::Dataset;
    /// use saci_core::component::Component;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct Temp { celsius: f64 }
    /// impl Component for Temp {
    ///     fn name() -> &'static str { "Temp" }
    ///     fn schema() -> Arc<Schema> {
    ///         Arc::new(Schema::new(vec![Field::new("celsius", DataType::Float64, false)]))
    ///     }
    /// }
    ///
    /// let mut dataset = Dataset::new();
    /// dataset.register_component::<Temp>().unwrap();
    /// dataset.append::<Temp>(&[Temp { celsius: 36.6 }]).unwrap();
    ///
    /// let col = dataset.column::<Temp>("celsius").unwrap();
    /// let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
    /// assert!((arr.value(0) - 36.6).abs() < 1e-9);
    /// # }
    /// ```
    pub fn column<C: Component>(&self, field: &str) -> Option<&ArrayRef> {
        if !self.schemas.contains(C::name()) {
            return None;
        }
        let batch = self.get_or_build_merged(C::name());
        let idx = batch.schema_ref().index_of(field).ok()?;
        Some(batch.column(idx))
    }

    /// Return a reference to the whole `RecordBatch` for component `C`.
    ///
    /// Returns `None` if the component is not registered.
    pub fn columns<C: Component>(&self) -> Option<&RecordBatch> {
        if !self.schemas.contains(C::name()) {
            return None;
        }
        Some(self.get_or_build_merged(C::name()))
    }

    /// Return a typed [`ComponentView`] for component `C`.
    ///
    /// Column access without manual batch unpacking and downcasting.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::ComponentNotFound`] if `C` is not registered.
    ///
    /// # Example
    ///
    /// ```rust
    /// # {
    /// # use std::sync::Arc;
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use saci_core::component::Component;
    /// # use saci_core::dataset::Dataset;
    /// # use serde::{Serialize, Deserialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Price { value: f64 }
    /// # impl Component for Price {
    /// #     fn name() -> &'static str { "Price" }
    /// #     fn schema() -> Arc<Schema> {
    /// #         Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]))
    /// #     }
    /// # }
    /// # fn main() -> Result<(), saci_core::SaciError> {
    /// let mut dataset = Dataset::new();
    /// dataset.register_component::<Price>()?;
    /// dataset.append::<Price>(&[Price { value: 42.0 }])?;
    /// let view = dataset.view::<Price>()?;
    /// let values = view.f64("value")?;
    /// assert_eq!(values.value(0), 42.0);
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    pub fn view<C: Component>(&self) -> SaciResult<ComponentView<'_, C>> {
        let batch = self
            .columns::<C>()
            .ok_or_else(|| SaciError::component_not_found(0, C::name()))?;
        Ok(ComponentView::new(batch))
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

    /// Return the full live row range `0..row_count` as a `Range<u32>`.
    ///
    /// Used by [`ParallelSystem`](crate::system::ParallelSystem)
    /// implementations to determine the slice boundaries for intra-system
    /// parallelism.
    ///
    /// # Example
    ///
    /// ```rust
    /// #
    /// # {
    /// use std::sync::Arc;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use saci_core::dataset::Dataset;
    /// use saci_core::component::Component;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct Score { value: f64 }
    /// impl Component for Score {
    ///     fn name() -> &'static str { "Score" }
    ///     fn schema() -> Arc<Schema> {
    ///         Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]))
    ///     }
    /// }
    ///
    /// let mut dataset = Dataset::new();
    /// dataset.register_component::<Score>().unwrap();
    /// dataset.append::<Score>(&[Score { value: 1.0 }, Score { value: 2.0 }]).unwrap();
    /// assert_eq!(dataset.row_range(), 0..2);
    /// # }
    /// ```
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

    #[derive(Serialize, Deserialize)]
    struct ReadOrder {
        id: u64,
        total: f64,
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
        ds.register_component::<ReadOrder>().unwrap();
        ds.append::<ReadOrder>(&[ReadOrder { id: 1, total: 9.9 }])
            .unwrap();
        let col = ds.column::<ReadOrder>("total").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 9.9).abs() < 1e-9);
    }

    #[test]
    fn test_column_missing_field_returns_none() {
        let mut ds = Dataset::new();
        ds.register_component::<ReadOrder>().unwrap();
        ds.append::<ReadOrder>(&[ReadOrder { id: 1, total: 1.0 }])
            .unwrap();
        assert!(ds.column::<ReadOrder>("nonexistent").is_none());
    }

    #[test]
    fn test_column_unregistered_component_returns_none() {
        let ds = Dataset::new();
        assert!(ds.column::<ReadOrder>("id").is_none());
    }

    #[test]
    fn test_is_alive_out_of_bounds() {
        let ds = Dataset::new();
        assert!(!ds.is_alive(Row::new(99)));
    }

    #[test]
    fn test_row_range() {
        let mut ds = Dataset::new();
        ds.register_component::<ReadOrder>().unwrap();
        ds.append::<ReadOrder>(&[
            ReadOrder { id: 0, total: 0.0 },
            ReadOrder { id: 1, total: 1.0 },
        ])
        .unwrap();
        assert_eq!(ds.row_range(), 0..2);
    }

    #[derive(Serialize, Deserialize)]
    struct TestPrice {
        value: f64,
    }

    impl Component for TestPrice {
        fn name() -> &'static str {
            "TestPriceReads"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                false,
            )]))
        }
    }

    #[test]
    fn world_view_returns_component_view() {
        let mut ds = Dataset::new();
        ds.register_component::<TestPrice>().unwrap();
        ds.append::<TestPrice>(&[TestPrice { value: 7.5 }]).unwrap();

        let view = ds.view::<TestPrice>().unwrap();
        let col = view.f64("value").unwrap();
        assert!((col.value(0) - 7.5).abs() < 1e-9);
    }

    #[test]
    fn world_view_missing_component() {
        let ds = Dataset::new();
        let result = ds.view::<TestPrice>();
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("TestPriceReads"),
            "expected error mentioning component name, got: {err}"
        );
    }
}
