use std::collections::HashMap;

use arrow_array::ArrayRef;

use crate::dataset::Dataset;

/// A boxed, type-erased resource mutation produced by a [`ParallelSystem`](super::ParallelSystem).
///
/// Parallel systems hold an immutable pipeline reference, so they cannot mutate
/// resources in place. They return `ResourceUpdate` closures that the pipeline
/// applies on the main thread once all parallel write sets are merged.
///
/// Create instances with [`ResourceUpdate::new`].
pub struct ResourceUpdate {
    apply: Box<dyn FnOnce(&mut Dataset) + Send>,
}

impl ResourceUpdate {
    /// Build a resource update that inserts or replaces resource `R` with `value`.
    ///
    /// # Example
    ///
    /// ```rust
    /// #
    /// # {
    /// use saci_core::system::ResourceUpdate;
    /// use saci_core::dataset::Dataset;
    ///
    /// struct MyCount(u32);
    ///
    /// let update = ResourceUpdate::new(MyCount(42));
    /// let mut data = Dataset::new();
    /// update.apply(&mut data);
    /// assert_eq!(data.get_resource::<MyCount>().unwrap().0, 42);
    /// # }
    /// ```
    pub fn new<R: Send + Sync + 'static>(value: R) -> Self {
        Self {
            apply: Box::new(move |data: &mut Dataset| {
                data.insert_resource(value);
            }),
        }
    }

    /// Apply the update to the dataset, consuming `self`.
    pub fn apply(self, data: &mut Dataset) {
        (self.apply)(data);
    }
}

impl std::fmt::Debug for ResourceUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceUpdate").finish_non_exhaustive()
    }
}

/// A set of column writes produced by a [`ParallelSystem`](super::ParallelSystem) during one
/// stage execution.
///
/// Each entry maps `(component_name, field_name)` to a new [`ArrayRef`].
/// The array's length must equal the pipeline's current row count, or the new
/// row count if the system is appending rows.
///
/// After every parallel system in a stage finishes, the pipeline merges their
/// `WriteSet`s, whose field keys are disjoint by construction of the
/// field-level DAG, and applies them atomically via
/// [`Dataset::apply_write_set`](crate::dataset::Dataset::apply_write_set).
///
/// ## Builder API
///
/// ```rust
/// #
/// # {
/// use std::sync::Arc;
/// use arrow_array::Float64Array;
/// use saci_core::system::WriteSet;
///
/// let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
/// let ws = WriteSet::new()
///     .put("Order", "total", array);
/// assert_eq!(ws.fields.len(), 1);
/// # }
/// ```
pub struct WriteSet {
    /// Map of (component name, field name) → new column data.
    pub fields: HashMap<(&'static str, &'static str), ArrayRef>,
    /// Resource mutations to apply on the main thread after columns are committed.
    pub resource_updates: Vec<ResourceUpdate>,
}

impl WriteSet {
    /// Create an empty `WriteSet`.
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            resource_updates: Vec::new(),
        }
    }

    /// Add a column update for `(component, field)`.
    ///
    /// The `array` must have the correct length for the pipeline's current row
    /// count. Calling `put` twice for the same key replaces the previous entry.
    pub fn put(mut self, component: &'static str, field: &'static str, array: ArrayRef) -> Self {
        self.fields.insert((component, field), array);
        self
    }

    /// Attach a resource update to this write set.
    ///
    /// The update is applied on the main thread (single-threaded) after all
    /// column writes in the stage have been committed.
    pub fn with_resource(mut self, update: ResourceUpdate) -> Self {
        self.resource_updates.push(update);
        self
    }

    /// Returns `true` if no fields and no resource updates are present.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.resource_updates.is_empty()
    }
}

impl Default for WriteSet {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSet")
            .field("fields", &self.fields.keys().collect::<Vec<_>>())
            .field("resource_updates_count", &self.resource_updates.len())
            .finish()
    }
}

/// A partial write produced by one row-range slice of a [`ParallelSystem`](super::ParallelSystem)
/// running with intra-system slice parallelism.
///
/// The `slice_rows` range identifies which rows this slice covers, so that
/// [`ParallelSystem::merge_slices`](super::ParallelSystem::merge_slices) can
/// concatenate the segments in order.
pub struct SliceWriteSet {
    /// Map of (component name, field name) → column segment for `slice_rows`.
    pub fields: HashMap<(&'static str, &'static str), ArrayRef>,
    /// The row range this slice covers within the full pipeline.
    pub slice_rows: std::ops::Range<u32>,
}

impl SliceWriteSet {
    /// Create an empty `SliceWriteSet` for the given row range.
    pub fn new(slice_rows: std::ops::Range<u32>) -> Self {
        Self {
            fields: HashMap::new(),
            slice_rows,
        }
    }

    /// Add a column segment for `(component, field)` within this slice.
    pub fn put(mut self, component: &'static str, field: &'static str, array: ArrayRef) -> Self {
        self.fields.insert((component, field), array);
        self
    }
}

impl std::fmt::Debug for SliceWriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SliceWriteSet")
            .field("fields", &self.fields.keys().collect::<Vec<_>>())
            .field("slice_rows", &self.slice_rows)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use std::sync::Arc;

    #[test]
    fn test_write_set_new_is_empty() {
        let ws = WriteSet::new();
        assert!(ws.is_empty());
        assert!(ws.fields.is_empty());
        assert!(ws.resource_updates.is_empty());
    }

    #[test]
    fn test_write_set_put_adds_field() {
        let arr = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let ws = WriteSet::new().put("Order", "total", arr);
        assert_eq!(ws.fields.len(), 1);
        assert!(!ws.is_empty());
    }

    #[test]
    fn test_write_set_put_replaces_same_key() {
        let arr1 = Arc::new(Float64Array::from(vec![1.0]));
        let arr2 = Arc::new(Float64Array::from(vec![2.0]));
        let ws = WriteSet::new()
            .put("Order", "total", arr1)
            .put("Order", "total", arr2);
        assert_eq!(ws.fields.len(), 1);
    }

    #[test]
    fn test_write_set_with_resource() {
        struct MyCount(#[allow(dead_code)] u32);
        let ws = WriteSet::new().with_resource(ResourceUpdate::new(MyCount(1)));
        assert_eq!(ws.resource_updates.len(), 1);
        assert!(!ws.is_empty());
    }

    #[test]
    fn test_resource_update_applies_to_dataset() {
        struct MyCount(u32);
        let update = ResourceUpdate::new(MyCount(42));
        let mut data = Dataset::new();
        update.apply(&mut data);
        assert_eq!(data.get_resource::<MyCount>().unwrap().0, 42);
    }

    #[test]
    fn test_slice_write_set_new() {
        let sws = SliceWriteSet::new(0..100);
        assert_eq!(sws.slice_rows, 0..100);
        assert!(sws.fields.is_empty());
    }

    #[test]
    fn test_slice_write_set_put() {
        let arr = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let sws = SliceWriteSet::new(0..2).put("Score", "value", arr);
        assert_eq!(sws.fields.len(), 1);
    }

    #[test]
    fn test_write_set_debug() {
        let ws = WriteSet::new();
        let dbg = format!("{ws:?}");
        assert!(dbg.contains("WriteSet"));
    }

    #[test]
    fn test_resource_update_debug() {
        struct Dummy;
        let ru = ResourceUpdate::new(Dummy);
        let dbg = format!("{ru:?}");
        assert!(dbg.contains("ResourceUpdate"));
    }
}
