use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;

use crate::error::SaciError;

use super::Dataset;

impl Dataset {
    /// Merge `chunks` into a single batch in-place.
    ///
    /// No-op if `chunks.len() <= 1`.
    pub(super) fn compact_chunks(
        chunks: &mut Vec<RecordBatch>,
        schema: &Arc<Schema>,
    ) -> Result<(), SaciError> {
        if chunks.len() <= 1 {
            return Ok(());
        }
        let merged = concat_batches(schema, chunks.iter())
            .map_err(|e| SaciError::generic(format!("arrow concat_batches error: {e}")))?;
        *chunks = vec![merged];
        Ok(())
    }

    /// Collapse every component that has more than one pending chunk into one.
    ///
    /// Does not warm the merged cache for single-chunk components;
    /// `get_or_build_merged` populates it on demand, and only for components
    /// something reads.
    pub(super) fn flush_all_pending(&mut self) -> Result<(), SaciError> {
        for (name, chunks) in &mut self.components {
            if chunks.len() > 1 {
                let schema = self
                    .schemas
                    .get(name)
                    .expect("schema registered but get() failed — internal inconsistency");
                let merged = concat_batches(&schema, chunks.iter())
                    .map_err(|e| SaciError::generic(format!("arrow concat_batches error: {e}")))?;
                *chunks = vec![merged];
                self.merged_cache.get_mut().unwrap().remove(name);
            }
        }
        Ok(())
    }

    /// Return a reference to the merged `RecordBatch` for `name`.
    ///
    /// # Safety (internal)
    ///
    /// The raw pointer read out of the lock guard is handed back with the
    /// lifetime of `&self`. That is sound because:
    ///
    /// 1. The `RecordBatch` lives on the heap behind a `Box` owned by
    ///    `self.merged_cache`, so its address is stable across `HashMap`
    ///    reallocations.
    /// 2. We hold `&self`, so no `&mut Dataset` can coexist.
    /// 3. The `RwLock` serialises concurrent `&self` reads that both trigger
    ///    cache population.
    pub(super) fn get_or_build_merged(&self, name: &'static str) -> &RecordBatch {
        // Fast path: a shared read. A sliced system calls `batch_for` once per
        // slice, so an exclusive lock here would serialise the whole fan-out.
        {
            let cache = self.merged_cache.read().unwrap();
            if let Some(boxed) = cache.get(name) {
                // SAFETY: see method-level doc comment.
                let ptr: *const RecordBatch = boxed.as_ref();
                drop(cache);
                return unsafe { &*ptr };
            }
        }

        let mut cache = self.merged_cache.write().unwrap();

        // Another thread may have populated the entry while this one was
        // upgrading from the read lock.
        if let Some(boxed) = cache.get(name) {
            // SAFETY: see method-level doc comment.
            let ptr: *const RecordBatch = boxed.as_ref();
            drop(cache);
            return unsafe { &*ptr };
        }

        let chunks = self
            .components
            .get(name)
            .expect("get_or_build_merged called for unregistered component");

        let merged: RecordBatch = if chunks.len() == 1 {
            chunks[0].clone()
        } else {
            let schema = self
                .schemas
                .get(name)
                .expect("schema registered but get() failed — internal inconsistency");
            concat_batches(&schema, chunks.iter())
                .expect("concat_batches failed during merged-cache build")
        };

        cache.insert(name, Box::new(merged));
        // SAFETY: see method-level doc comment.
        let ptr: *const RecordBatch = cache[name].as_ref();
        drop(cache);
        unsafe { &*ptr }
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

    #[derive(Serialize, Deserialize)]
    struct CkOrder {
        id: u64,
    }

    impl Component for CkOrder {
        fn name() -> &'static str {
            "CkOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
        }
    }

    #[test]
    fn test_many_appends_merged_correctly() {
        let mut ds = Dataset::new();
        ds.register_component::<CkOrder>().unwrap();
        for i in 0..100usize {
            ds.append::<CkOrder>(&[CkOrder { id: i as u64 }]).unwrap();
        }
        let batch = ds.columns::<CkOrder>().unwrap();
        assert_eq!(batch.num_rows(), 100);
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 0u64);
        assert_eq!(arr.value(99), 99u64);
    }
}
