use std::ops::Range;

use arrow_array::ArrayRef;

use crate::component::Component;
use crate::dataset::Dataset;

use super::field::FieldRef;

/// One `(component, field)` column write.
pub(crate) type FieldWrite = ((&'static str, &'static str), ArrayRef);

/// A boxed, type-erased resource mutation produced by a
/// [`ParallelSystem`](super::ParallelSystem), applied on the main thread once
/// every write set of the stage is merged.
pub(crate) struct ResourceUpdate {
    apply: Box<dyn FnOnce(&mut Dataset) + Send>,
}

impl ResourceUpdate {
    /// An update that inserts or replaces resource `R` with `value`.
    pub(crate) fn new<R: Send + Sync + 'static>(value: R) -> Self {
        Self {
            apply: Box::new(move |data: &mut Dataset| {
                data.insert_resource(value);
            }),
        }
    }

    /// Apply the update to the dataset, consuming `self`.
    pub(crate) fn apply(self, data: &mut Dataset) {
        (self.apply)(data);
    }
}

/// Insert `array` under `key`, replacing an entry already present for it.
///
/// A write set holds a handful of fields, so a linear scan beats hashing.
fn put_entry(fields: &mut Vec<FieldWrite>, key: (&'static str, &'static str), array: ArrayRef) {
    match fields.iter_mut().find(|(k, _)| *k == key) {
        Some(entry) => entry.1 = array,
        None => fields.push((key, array)),
    }
}

/// Merge `other` into `fields`, replacing any key both hold.
#[cfg(feature = "engine")]
pub(crate) fn merge_entries(fields: &mut Vec<FieldWrite>, other: Vec<FieldWrite>) {
    if fields.is_empty() {
        *fields = other;
        return;
    }
    for (key, array) in other {
        put_entry(fields, key, array);
    }
}

fn get_entry<'a>(fields: &'a [FieldWrite], component: &str, field: &str) -> Option<&'a ArrayRef> {
    fields
        .iter()
        .find(|((c, f), _)| *c == component && *f == field)
        .map(|(_, a)| a)
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
/// ```rust
/// use std::sync::Arc;
/// use arrow_array::Float64Array;
/// use saci::WriteSet;
///
/// let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
/// let ws = WriteSet::new().put_named("Order", "total", array);
/// assert_eq!(ws.len(), 1);
/// ```
#[derive(Default)]
pub struct WriteSet {
    fields: Vec<FieldWrite>,
    resource_updates: Vec<ResourceUpdate>,
}

impl WriteSet {
    /// Create an empty `WriteSet`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Write one field of `C`. A second `put` of the same field replaces the
    /// first.
    pub fn put<C: Component>(self, field: FieldRef<C>, array: ArrayRef) -> Self {
        self.put_named(C::name(), field.name(), array)
    }

    /// Write `field` of the component named `component`. A second `put` of
    /// the same field replaces the first.
    pub fn put_named(
        mut self,
        component: &'static str,
        field: &'static str,
        array: ArrayRef,
    ) -> Self {
        put_entry(&mut self.fields, (component, field), array);
        self
    }

    /// Insert or replace resource `R` with `value` once the stage's column
    /// writes are committed, on the main thread.
    pub fn with_resource<R: Send + Sync + 'static>(mut self, value: R) -> Self {
        self.resource_updates.push(ResourceUpdate::new(value));
        self
    }

    /// Number of column writes.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns `true` if no fields and no resource updates are present.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.resource_updates.is_empty()
    }

    /// The array written to `component.field`, if any.
    pub fn get_named(&self, component: &str, field: &str) -> Option<&ArrayRef> {
        get_entry(&self.fields, component, field)
    }

    /// Every column write as `(component, field, array)`, in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &'static str, &ArrayRef)> {
        self.fields.iter().map(|((c, f), a)| (*c, *f, a))
    }

    /// The column writes and the resource updates, for the stage executor.
    pub(crate) fn into_parts(self) -> (Vec<FieldWrite>, Vec<ResourceUpdate>) {
        (self.fields, self.resource_updates)
    }

    /// Rebuild a write set from parts [`into_parts`](Self::into_parts) took.
    #[cfg(feature = "engine")]
    pub(crate) fn from_parts(
        fields: Vec<FieldWrite>,
        resource_updates: Vec<ResourceUpdate>,
    ) -> Self {
        Self {
            fields,
            resource_updates,
        }
    }
}

impl std::fmt::Debug for WriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSet")
            .field(
                "fields",
                &self.fields.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("resource_updates_count", &self.resource_updates.len())
            .finish()
    }
}

/// A partial write produced by one row-range slice of a [`ParallelSystem`](super::ParallelSystem)
/// running with intra-system slice parallelism.
///
/// [`rows`](Self::rows) identifies which rows this slice covers, so that
/// [`ParallelSystem::merge_slices`](super::ParallelSystem::merge_slices) can
/// concatenate the segments in order.
pub struct SliceWriteSet {
    fields: Vec<FieldWrite>,
    rows: Range<u32>,
}

impl SliceWriteSet {
    /// Create an empty `SliceWriteSet` for the given row range.
    pub fn new(rows: Range<u32>) -> Self {
        Self {
            fields: Vec::new(),
            rows,
        }
    }

    /// Write one field of `C` for this slice's rows.
    pub fn put<C: Component>(self, field: FieldRef<C>, array: ArrayRef) -> Self {
        self.put_named(C::name(), field.name(), array)
    }

    /// Write `field` of the component named `component` for this slice's rows.
    pub fn put_named(
        mut self,
        component: &'static str,
        field: &'static str,
        array: ArrayRef,
    ) -> Self {
        put_entry(&mut self.fields, (component, field), array);
        self
    }

    /// The row range this slice covers within the full dataset.
    pub fn rows(&self) -> Range<u32> {
        self.rows.clone()
    }

    /// Number of column segments.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns `true` if the slice writes no field.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The segment written to `component.field`, if any.
    pub fn get_named(&self, component: &str, field: &str) -> Option<&ArrayRef> {
        get_entry(&self.fields, component, field)
    }

    /// Every segment as `(component, field, array)`, in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &'static str, &ArrayRef)> {
        self.fields.iter().map(|((c, f), a)| (*c, *f, a))
    }
}

impl std::fmt::Debug for SliceWriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SliceWriteSet")
            .field(
                "fields",
                &self.fields.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("rows", &self.rows)
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
        assert_eq!(ws.len(), 0);
    }

    #[test]
    fn test_write_set_put_adds_field() {
        let arr = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let ws = WriteSet::new().put_named("Order", "total", arr);
        assert_eq!(ws.len(), 1);
        assert!(!ws.is_empty());
    }

    #[test]
    fn test_write_set_put_replaces_same_key() {
        let arr1: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let arr2: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let ws = WriteSet::new().put_named("Order", "total", arr1).put_named(
            "Order",
            "total",
            Arc::clone(&arr2),
        );
        assert_eq!(ws.len(), 1);
        assert!(Arc::ptr_eq(ws.get_named("Order", "total").unwrap(), &arr2));
    }

    #[test]
    fn test_write_set_with_resource() {
        struct MyCount(u32);
        let ws = WriteSet::new().with_resource(MyCount(1));
        assert!(!ws.is_empty());
        let (_, updates) = ws.into_parts();
        let mut data = Dataset::new();
        for update in updates {
            update.apply(&mut data);
        }
        assert_eq!(data.get_resource::<MyCount>().unwrap().0, 1);
    }

    #[test]
    fn test_slice_write_set_new() {
        let sws = SliceWriteSet::new(0..100);
        assert_eq!(sws.rows(), 0..100);
        assert!(sws.is_empty());
    }

    #[test]
    fn test_slice_write_set_put() {
        let arr = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let sws = SliceWriteSet::new(0..2).put_named("Score", "value", arr);
        assert_eq!(sws.len(), 1);
        assert!(sws.get_named("Score", "value").is_some());
    }

    #[test]
    fn test_write_set_debug() {
        let ws = WriteSet::new();
        let dbg = format!("{ws:?}");
        assert!(dbg.contains("WriteSet"));
    }
}
