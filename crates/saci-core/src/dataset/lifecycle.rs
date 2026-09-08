use arrow_array::{BooleanArray, RecordBatch};
use arrow_buffer::builder::BooleanBufferBuilder;
use arrow_select::filter::filter_record_batch;

use crate::error::SaciError;
use crate::row::Row;

use super::Dataset;

impl Dataset {
    /// Mark a row as dead (lazy delete).
    ///
    /// The row's data remains in the batch until [`compact`](Self::compact) is
    /// called. Repeated calls on the same row are idempotent: `dead_count`
    /// increments once per row.
    ///
    /// Out-of-bounds rows are silently ignored.
    pub fn mark_dead(&mut self, row: Row) {
        let idx = row.index();
        if idx < self.alive.len() && self.alive.get_bit(idx) {
            self.alive.set_bit(idx, false);
            self.dead_count += 1;
            self.live_count -= 1;
        }
    }

    /// Return `true` if more than 25 % of rows are dead.
    pub fn should_compact(&self) -> bool {
        if self.row_count == 0 {
            return false;
        }
        self.dead_count > self.row_count / 4
    }

    /// Compact all component batches by filtering out dead rows.
    ///
    /// After compaction, `rows() == live_rows()` and the alive bitmap is reset
    /// to all-`true`. Row indices shift; stale [`Row`] handles must not be used
    /// after this call.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if Arrow's filter kernel fails.
    pub fn compact(&mut self) -> Result<(), SaciError> {
        if self.dead_count == 0 {
            return Ok(());
        }

        let alive_buffer = self.alive.finish_cloned();
        let filter_arr = BooleanArray::new(alive_buffer, None);

        self.flush_all_pending()?;

        for chunks in self.components.values_mut() {
            let batch = chunks
                .first_mut()
                .expect("component vec empty after flush — internal inconsistency");
            *batch = filter_record_batch(batch, &filter_arr)
                .map_err(|e| SaciError::generic(format!("compact filter error: {e}")))?;
        }

        self.merged_cache.get_mut().unwrap().clear();

        let new_count = self.live_count;
        self.alive = BooleanBufferBuilder::new(new_count);
        self.alive.append_n(new_count, true);
        self.row_count = new_count;
        self.live_count = new_count;
        self.dead_count = 0;

        Ok(())
    }

    /// Create a new empty dataset with the same registered schemas as `self`.
    ///
    /// All components are re-registered with empty `RecordBatch`es. Existing
    /// row data and resources are NOT copied.
    ///
    /// Used by the cluster runner to obtain a fresh `Dataset` for each batch
    /// while reusing the schema configuration.
    ///
    /// # Example
    ///
    /// ```rust
    /// # {
    /// use saci_core::dataset::Dataset;
    /// use saci_core::component::Component;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use std::sync::Arc;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct Price(f64);
    /// impl Component for Price {
    ///     fn name() -> &'static str { "price" }
    ///     fn schema() -> Arc<Schema> {
    ///         Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]))
    ///     }
    /// }
    ///
    /// let mut dataset = Dataset::new();
    /// dataset.register_component::<Price>().unwrap();
    ///
    /// let empty = dataset.clone_empty();
    /// assert_eq!(empty.rows(), 0);
    /// # }
    /// ```
    pub fn clone_empty(&self) -> Self {
        let mut new_dataset = Self::new();
        for (&name, chunks) in &self.components {
            let schema = if let Some(b) = chunks.first() {
                b.schema()
            } else {
                self.schemas.get(name).expect("schema missing")
            };
            let version = self.schemas.get_version(name).unwrap_or(1);
            new_dataset
                .schemas
                .register_raw(name, schema.clone(), version);
            new_dataset
                .components
                .insert(name, vec![RecordBatch::new_empty(schema)]);
        }
        new_dataset
    }

    /// Drop all rows and resources, resetting the dataset to empty.
    ///
    /// Schemas remain registered so the dataset can be re-used with the same
    /// component types without calling `register_component` again.
    pub fn clear(&mut self) {
        // Disjoint field borrows: `components` mutably, `empty_templates`
        // shared. Cloning a prebuilt zero-row batch is a few `Arc` bumps
        // instead of an empty Arrow array per column.
        let Self {
            components,
            empty_templates,
            ..
        } = self;
        for (name, chunks) in components.iter_mut() {
            match empty_templates.get(name) {
                Some(template) => {
                    chunks.clear();
                    chunks.push(template.clone());
                }
                // No prebuilt template: the component was registered by hand.
                None => {
                    let Some(schema) = chunks.first().map(|b| b.schema()) else {
                        continue;
                    };
                    *chunks = vec![RecordBatch::new_empty(schema)];
                }
            }
        }
        self.merged_cache.get_mut().unwrap().clear();
        // `truncate(0)` keeps the byte buffer, where assigning a fresh
        // `BooleanBufferBuilder::new(0)` would drop it and force the next
        // `append_record_batch` to grow from zero again. The following
        // `append_n` overwrites the whole buffer, so contents are identical.
        self.alive.truncate(0);
        self.live_count = 0;
        self.row_count = 0;
        self.dead_count = 0;
        self.resources.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::UInt64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::row::Row;

    #[derive(Serialize, Deserialize)]
    struct LcOrder {
        id: u64,
        total: f64,
    }

    impl Component for LcOrder {
        fn name() -> &'static str {
            "LcOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    fn make_orders(n: usize) -> Vec<LcOrder> {
        (0..n)
            .map(|i| LcOrder {
                id: i as u64,
                total: i as f64,
            })
            .collect()
    }

    #[test]
    fn test_mark_dead_idempotent() {
        let mut ds = Dataset::new();
        ds.register_component::<LcOrder>().unwrap();
        ds.append::<LcOrder>(&make_orders(10)).unwrap();
        ds.mark_dead(Row::new(0));
        ds.mark_dead(Row::new(0));
        assert_eq!(ds.live_rows(), 9);
    }

    #[test]
    fn test_should_compact_threshold() {
        let mut ds = Dataset::new();
        ds.register_component::<LcOrder>().unwrap();
        ds.append::<LcOrder>(&make_orders(100)).unwrap();
        for i in 0..25usize {
            ds.mark_dead(Row::new(i as u32));
        }
        assert!(!ds.should_compact());
        ds.mark_dead(Row::new(25));
        assert!(ds.should_compact());
    }

    #[test]
    fn test_compact_filters_dead_rows() {
        let mut ds = Dataset::new();
        ds.register_component::<LcOrder>().unwrap();
        ds.append::<LcOrder>(&make_orders(100)).unwrap();
        for i in 0..50usize {
            ds.mark_dead(Row::new(i as u32));
        }
        ds.compact().unwrap();
        assert_eq!(ds.rows(), 50);
        assert_eq!(ds.live_rows(), 50);
        let col = ds.column::<LcOrder>("id").unwrap();
        let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(arr.value(0), 50u64);
    }

    #[test]
    fn test_clear_resets_rows_not_schemas() {
        let mut ds = Dataset::new();
        ds.register_component::<LcOrder>().unwrap();
        ds.append::<LcOrder>(&make_orders(10)).unwrap();
        ds.clear();
        assert_eq!(ds.rows(), 0);
        ds.append::<LcOrder>(&make_orders(5)).unwrap();
        assert_eq!(ds.rows(), 5);
    }

    #[test]
    fn test_clone_empty_preserves_schemas() {
        let mut ds = Dataset::new();
        ds.register_component::<LcOrder>().unwrap();
        ds.append::<LcOrder>(&make_orders(10)).unwrap();
        let empty = ds.clone_empty();
        assert_eq!(empty.rows(), 0);
        assert!(empty.schemas().contains("LcOrder"));
    }
}
