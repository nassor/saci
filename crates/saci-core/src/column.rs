//! Column accessor helpers for [`Pipeline`](super::pipeline::Pipeline).
//!
//! These utilities wrap Arrow's `RecordBatch` API to provide ergonomic typed
//! access to individual columns.

use std::marker::PhantomData;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::DataType;

use crate::component::Component;
use crate::{SaciError, SaciResult};

/// Downcast an [`ArrayRef`] to `&Float64Array`, returning a descriptive error on failure.
///
/// # Errors
///
/// Returns `SaciError::Generic` if the underlying array is not `Float64`.
pub fn as_f64<'a>(array: &'a ArrayRef, field_name: &str) -> Result<&'a Float64Array, SaciError> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            SaciError::generic(format!(
                "column '{}' is {:?}, expected Float64",
                field_name,
                array.data_type()
            ))
        })
}

/// Downcast an [`ArrayRef`] to `&UInt64Array`, returning a descriptive error on failure.
pub fn as_u64<'a>(array: &'a ArrayRef, field_name: &str) -> Result<&'a UInt64Array, SaciError> {
    array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
        SaciError::generic(format!(
            "column '{}' is {:?}, expected UInt64",
            field_name,
            array.data_type()
        ))
    })
}

/// Downcast an [`ArrayRef`] to `&Int64Array`, returning a descriptive error on failure.
pub fn as_i64<'a>(array: &'a ArrayRef, field_name: &str) -> Result<&'a Int64Array, SaciError> {
    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        SaciError::generic(format!(
            "column '{}' is {:?}, expected Int64",
            field_name,
            array.data_type()
        ))
    })
}

/// Downcast an [`ArrayRef`] to `&BooleanArray`, returning a descriptive error on failure.
pub fn as_bool<'a>(array: &'a ArrayRef, field_name: &str) -> Result<&'a BooleanArray, SaciError> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            SaciError::generic(format!(
                "column '{}' is {:?}, expected Boolean",
                field_name,
                array.data_type()
            ))
        })
}

/// Sum all non-null values of a `Float64` column.
///
/// # Example
///
/// ```rust
/// #
/// # {
/// use std::sync::Arc;
/// use arrow_array::{ArrayRef, Float64Array};
/// use saci_core::column::sum_f64;
///
/// let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
/// assert_eq!(sum_f64(&arr).unwrap(), 6.0);
/// # }
/// ```
pub fn sum_f64(array: &ArrayRef) -> Result<f64, SaciError> {
    if array.data_type() != &DataType::Float64 {
        return Err(SaciError::generic(format!(
            "sum_f64: expected Float64, got {:?}",
            array.data_type()
        )));
    }
    let typed = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("already checked type");
    Ok(typed.iter().flatten().sum())
}

/// A typed, borrowed view over a [`RecordBatch`] for a specific [`Component`].
///
/// Gives type-safe column access without cloning the underlying Arrow data.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_core::component::Component;
/// use saci_core::dataset::Dataset;
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Price { value: f64 }
///
/// impl Component for Price {
///     fn name() -> &'static str { "Price" }
///     fn schema() -> Arc<Schema> {
///         Arc::new(Schema::new(vec![
///             Field::new("value", DataType::Float64, false),
///         ]))
///     }
/// }
///
/// let mut dataset = Dataset::new();
/// dataset.register_component::<Price>();
/// dataset.append::<Price>(&[Price { value: 1.0 }, Price { value: 2.0 }]).unwrap();
///
/// let view = dataset.view::<Price>().unwrap();
/// assert_eq!(view.len(), 2);
/// assert_eq!(view.f64("value").unwrap().value(0), 1.0);
/// ```
#[derive(Debug)]
pub struct ComponentView<'w, C: Component> {
    batch: &'w RecordBatch,
    _p: PhantomData<C>,
}

impl<'w, C: Component> ComponentView<'w, C> {
    /// Create a new `ComponentView` wrapping the given batch.
    pub(crate) fn new(batch: &'w RecordBatch) -> Self {
        Self {
            batch,
            _p: PhantomData,
        }
    }

    /// Access a `Float64` column by field name.
    ///
    /// # Errors
    ///
    /// Returns an error if the field does not exist or is not `Float64`.
    pub fn f64(&self, field: impl AsRef<str>) -> SaciResult<&Float64Array> {
        let field = field.as_ref();
        let col = get_column(self.batch, field)?;
        as_f64(col, field)
    }

    /// Access a `UInt64` column by field name.
    ///
    /// # Errors
    ///
    /// Returns an error if the field does not exist or is not `UInt64`.
    pub fn u64(&self, field: impl AsRef<str>) -> SaciResult<&UInt64Array> {
        let field = field.as_ref();
        let col = get_column(self.batch, field)?;
        as_u64(col, field)
    }

    /// Access an `Int64` column by field name.
    ///
    /// # Errors
    ///
    /// Returns an error if the field does not exist or is not `Int64`.
    pub fn i64(&self, field: impl AsRef<str>) -> SaciResult<&Int64Array> {
        let field = field.as_ref();
        let col = get_column(self.batch, field)?;
        as_i64(col, field)
    }

    /// Access a `Boolean` column by field name.
    ///
    /// # Errors
    ///
    /// Returns an error if the field does not exist or is not `Boolean`.
    pub fn bool(&self, field: impl AsRef<str>) -> SaciResult<&BooleanArray> {
        let field = field.as_ref();
        let col = get_column(self.batch, field)?;
        as_bool(col, field)
    }

    /// Access a `Utf8` (string) column by field name.
    ///
    /// # Errors
    ///
    /// Returns an error if the field does not exist or is not `Utf8`.
    pub fn str(&self, field: impl AsRef<str>) -> SaciResult<&StringArray> {
        let field = field.as_ref();
        let col = get_column(self.batch, field)?;
        col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            SaciError::generic(format!(
                "column '{}' is {:?}, expected Utf8",
                field,
                col.data_type()
            ))
        })
    }

    /// Access a `TimestampNanosecond` column by field name.
    ///
    /// # Errors
    ///
    /// Returns an error if the field does not exist or is not `TimestampNanosecond`.
    pub fn ts_ns(&self, field: impl AsRef<str>) -> SaciResult<&TimestampNanosecondArray> {
        let field = field.as_ref();
        let col = get_column(self.batch, field)?;
        col.as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| {
                SaciError::generic(format!(
                    "column '{}' is {:?}, expected TimestampNanosecond(Nanoseconds, _)",
                    field,
                    col.data_type()
                ))
            })
    }

    /// Returns the number of rows in this view.
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns `true` if there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a reference to the underlying [`RecordBatch`] for advanced use.
    pub fn batch(&self) -> &RecordBatch {
        self.batch
    }
}

/// Look up a column by field name in a [`RecordBatch`].
///
/// # Errors
///
/// Returns `SaciError::Generic` if `field` is not present in the batch schema.
fn get_column<'a>(batch: &'a RecordBatch, field: &str) -> SaciResult<&'a ArrayRef> {
    let idx = batch
        .schema_ref()
        .index_of(field)
        .map_err(|_| SaciError::generic(format!("field '{field}' not found")))?;
    Ok(batch.column(idx))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, Int64Array, UInt64Array};

    use super::*;

    #[test]
    fn test_as_f64_success() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let result = as_f64(&arr, "price");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().value(0), 1.0);
    }

    #[test]
    fn test_as_f64_wrong_type() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
        let err = as_f64(&arr, "price").unwrap_err();
        assert!(err.to_string().contains("price"));
        assert!(err.to_string().contains("Float64"));
    }

    #[test]
    fn test_as_u64_success() {
        let arr: ArrayRef = Arc::new(UInt64Array::from(vec![100u64]));
        assert!(as_u64(&arr, "id").is_ok());
    }

    #[test]
    fn test_as_i64_success() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![-1i64]));
        assert!(as_i64(&arr, "offset").is_ok());
    }

    #[test]
    fn test_sum_f64() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        assert_eq!(sum_f64(&arr).unwrap(), 10.0);
    }

    #[test]
    fn test_sum_f64_empty() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![] as Vec<f64>));
        assert_eq!(sum_f64(&arr).unwrap(), 0.0);
    }

    #[test]
    fn test_sum_f64_wrong_type() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
        assert!(sum_f64(&arr).is_err());
    }

    /// Minimal component for tests: name and schema only.
    struct TestComp;

    impl crate::component::Component for TestComp {
        fn name() -> &'static str {
            "TestComp"
        }
        fn schema() -> Arc<arrow_schema::Schema> {
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Float64,
                false,
            )]))
        }
    }

    fn make_f64_batch(values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "value",
            arrow_schema::DataType::Float64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(values)) as ArrayRef],
        )
        .unwrap()
    }

    #[test]
    fn component_view_len() {
        let batch = make_f64_batch(vec![1.0, 2.0, 3.0]);
        let view: ComponentView<TestComp> = ComponentView::new(&batch);
        assert_eq!(view.len(), 3);
        assert!(!view.is_empty());
        let col = view.f64("value").unwrap();
        assert_eq!(col.value(0), 1.0);
        assert_eq!(col.value(1), 2.0);
        assert_eq!(col.value(2), 3.0);
    }

    #[test]
    fn component_view_field_not_found() {
        let batch = make_f64_batch(vec![1.0]);
        let view: ComponentView<TestComp> = ComponentView::new(&batch);
        let err = view.f64("nonexistent").unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "error should mention the missing field name, got: {err}"
        );
    }

    #[test]
    fn component_view_type_mismatch() {
        let batch = make_f64_batch(vec![42.0]);
        let view: ComponentView<TestComp> = ComponentView::new(&batch);
        let err = view.u64("value").unwrap_err();
        assert!(
            err.to_string().contains("UInt64"),
            "error should mention expected type, got: {err}"
        );
    }
}
