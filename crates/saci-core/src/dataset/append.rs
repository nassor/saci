use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::component::Component;
use crate::error::SaciError;
use crate::row::Row;

use super::{Dataset, MERGE_THRESHOLD};

impl Dataset {
    /// Append a batch of values for component `C`.
    ///
    /// Returns the half-open [`Row`] range `[start, end)` that was allocated.
    ///
    /// Every component must receive the same number of new rows per append
    /// cycle. Appending 100 rows to `A` and 50 to `B` leaves the dataset
    /// inconsistent, and later queries may panic.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if:
    /// - The component has not been registered.
    /// - `serde_arrow` serialisation fails.
    /// - The resulting schema does not match the registered schema.
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
    /// let scores: Vec<Score> = (0..10).map(|i| Score { value: i as f64 }).collect();
    /// let range = dataset.append::<Score>(&scores).unwrap();
    /// assert_eq!(range.start.0, 0);
    /// assert_eq!(range.end.0, 10);
    /// # }
    /// ```
    pub fn append<C>(&mut self, values: &[C]) -> Result<Range<Row>, SaciError>
    where
        C: Component + serde::Serialize,
    {
        let name = C::name();

        if !self.schemas.contains(name) {
            return Err(SaciError::generic(format!(
                "Dataset: component '{name}' is not registered; call register_component::<{name}>() first"
            )));
        }

        if values.is_empty() {
            let start = Row::new(self.row_count as u32);
            return Ok(start..start);
        }

        let new_batch = C::to_record_batch(values)?;

        let registered_schema = self
            .schemas
            .get(name)
            .expect("schema registered but get() failed — internal inconsistency");

        if new_batch.schema_ref().fields() != registered_schema.fields() {
            return Err(SaciError::generic(format!(
                "Dataset: append for '{name}' produced schema mismatch: \
                 got {:?}, expected {:?}",
                new_batch.schema_ref(),
                registered_schema
            )));
        }

        let n = values.len();
        let chunks = self
            .components
            .get_mut(name)
            .expect("component registered but not in components map — internal inconsistency");

        let old_len: usize = chunks.iter().map(|b| b.num_rows()).sum();

        chunks.push(new_batch);
        if chunks.len() >= MERGE_THRESHOLD {
            Self::compact_chunks(chunks, &registered_schema)?;
        }

        self.merged_cache.get_mut().unwrap().remove(name);

        let new_len = old_len + n;
        let start = if new_len > self.row_count {
            let start = self.row_count as u32;
            let extension = new_len - self.row_count;
            self.alive.append_n(extension, true);
            self.live_count += extension;
            self.row_count = new_len;
            start
        } else {
            old_len as u32
        };

        let end = (old_len + n) as u32;
        Ok(Row::new(start)..Row::new(end))
    }

    /// Append a raw [`RecordBatch`] for the named component, bypassing serde
    /// serialisation.
    ///
    /// Used by `Source` implementations that already produce `RecordBatch`
    /// values, such as Parquet, CSV and JSON readers. The component must be
    /// registered first, via
    /// [`register_raw_component`](Dataset::register_raw_component) or
    /// [`register_component`](Dataset::register_component).
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if:
    /// - The component name is not registered.
    /// - The batch schema does not match the registered schema.
    pub fn append_record_batch(
        &mut self,
        component_name: &'static str,
        batch: RecordBatch,
    ) -> Result<Range<Row>, SaciError> {
        if !self.schemas.contains(component_name) {
            return Err(SaciError::generic(format!(
                "Dataset: component '{component_name}' is not registered; \
                 call register_raw_component() or register_component() first"
            )));
        }

        if batch.num_rows() == 0 {
            let start = Row::new(self.row_count as u32);
            return Ok(start..start);
        }

        let registered_schema = self
            .schemas
            .get(component_name)
            .expect("schema registered but get() failed — internal inconsistency");

        // Sources build their batches from one cached schema `Arc`, so pointer
        // equality usually settles this; the fallback compares every field
        // structurally. `schema_ref` avoids the `Arc` clone that `schema`
        // would make just to hand `ptr_eq` an address.
        if !Arc::ptr_eq(batch.schema_ref(), &registered_schema)
            && batch.schema_ref().fields() != registered_schema.fields()
        {
            return Err(SaciError::generic(format!(
                "Dataset::append_record_batch: schema mismatch for '{component_name}': \
                 got {:?}, expected {:?}",
                batch.schema_ref(),
                registered_schema
            )));
        }

        let n = batch.num_rows();
        let chunks = self
            .components
            .get_mut(component_name)
            .expect("component registered but not in components map — internal inconsistency");

        // `clear()` leaves a zero-row sentinel chunk so `batch_for` keeps
        // working on an emptied dataset. Pushing onto it would leave two
        // chunks and make every later read pay `concat_batches`, so drop the
        // sentinel and keep the component single-chunk.
        if chunks.len() == 1 && chunks[0].num_rows() == 0 {
            chunks.clear();
        }
        let old_len: usize = chunks.iter().map(|b| b.num_rows()).sum();

        chunks.push(batch);
        if chunks.len() >= MERGE_THRESHOLD {
            Self::compact_chunks(chunks, &registered_schema)?;
        }

        self.merged_cache.get_mut().unwrap().remove(component_name);

        let new_len = old_len + n;
        let start = if new_len > self.row_count {
            let start = self.row_count as u32;
            let extension = new_len - self.row_count;
            self.alive.append_n(extension, true);
            self.live_count += extension;
            self.row_count = new_len;
            start
        } else {
            old_len as u32
        };

        let end = (old_len + n) as u32;
        Ok(Row::new(start)..Row::new(end))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::row::Row;

    #[derive(Serialize, Deserialize)]
    struct AppOrder {
        id: u64,
        total: f64,
    }

    impl Component for AppOrder {
        fn name() -> &'static str {
            "AppOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_append_returns_correct_range() {
        let mut ds = Dataset::new();
        ds.register_component::<AppOrder>().unwrap();
        let range = ds
            .append::<AppOrder>(&[AppOrder { id: 0, total: 0.0 }])
            .unwrap();
        assert_eq!(range.start, Row::new(0));
        assert_eq!(range.end, Row::new(1));
    }

    #[test]
    fn test_empty_append_returns_empty_range() {
        let mut ds = Dataset::new();
        ds.register_component::<AppOrder>().unwrap();
        let range = ds.append::<AppOrder>(&[]).unwrap();
        assert_eq!(range.start, range.end);
        assert_eq!(ds.rows(), 0);
    }

    #[test]
    fn test_append_to_unregistered_errors() {
        let mut ds = Dataset::new();
        let result = ds.append::<AppOrder>(&[AppOrder { id: 0, total: 0.0 }]);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("not registered"));
    }

    #[test]
    fn test_append_1000_small_batches() {
        let mut ds = Dataset::new();
        ds.register_component::<AppOrder>().unwrap();
        for i in 0..1000usize {
            ds.append::<AppOrder>(&[AppOrder {
                id: i as u64,
                total: i as f64,
            }])
            .unwrap();
        }
        assert_eq!(ds.rows(), 1000);
        assert_eq!(ds.live_rows(), 1000);
    }
}
