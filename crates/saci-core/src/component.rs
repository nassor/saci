//! [`Component`]: the bridge between Rust structs and Arrow columnar storage.
//!
//! Implement this trait on any struct stored in a
//! [`Pipeline`](super::pipeline::Pipeline). The default `to_record_batch` and
//! `from_record_batch` delegate to `serde_arrow`, so most types need only
//! [`serde::Serialize`], [`serde::Deserialize`] and an Arrow `Schema`.
//!
//! # Manual implementation example
//!
//! ```rust
//! #
//! # {
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_array::RecordBatch;
//! use saci_core::component::Component;
//! use saci_core::SaciError;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Clone)]
//! struct Price {
//!     symbol: String,
//!     value: f64,
//! }
//!
//! impl Component for Price {
//!     fn name() -> &'static str { "Price" }
//!     fn schema() -> Arc<Schema> {
//!         Arc::new(Schema::new(vec![
//!             Field::new("symbol", DataType::Utf8, false),
//!             Field::new("value",  DataType::Float64, false),
//!         ]))
//!     }
//! }
//! # }
//! ```

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::SaciError;

/// Marker and conversion trait for types stored columnar inside
/// [`Pipeline`](super::pipeline::Pipeline).
///
/// Implementors must supply [`name`](Component::name), a unique `&'static str`
/// used as the `RecordBatch` map key, and [`schema`](Component::schema), the
/// Arrow [`Schema`] describing the struct's fields.
///
/// `to_record_batch`, `from_record_batch`, `version` and `migrate` have
/// defaults built on `serde_arrow` and a safe version-mismatch check. Override
/// `version` and `migrate` to support IPC schema evolution.
pub trait Component: Send + Sync + 'static {
    /// Unique identifier for this component type, used as the storage key.
    ///
    /// Must be globally unique within a single `Pipeline`. Conventionally
    /// matches the struct name.
    fn name() -> &'static str;

    /// Arrow [`Schema`] that describes this component's fields.
    ///
    /// Called once during [`register_component`](super::dataset::Dataset::register_component).
    fn schema() -> Arc<Schema>;

    /// Schema version written by this binary.
    ///
    /// Increment this whenever the Arrow schema produced by [`schema`](Self::schema)
    /// changes in a way that requires migration. The default is `1`.
    fn version() -> u32 {
        1
    }

    /// Migrate a `RecordBatch` from `from_version` to the current [`version`](Self::version).
    ///
    /// The default implementation accepts batches already at the current version and
    /// returns a configuration error for any other version. Override to add upgrade logic.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `from_version != Self::version()`.
    fn migrate(from_version: u32, batch: RecordBatch) -> crate::SaciResult<RecordBatch> {
        if from_version == Self::version() {
            Ok(batch)
        } else {
            Err(crate::SaciError::configuration(format!(
                "component '{}' version mismatch: on-disk={from_version}, current={}",
                Self::name(),
                Self::version()
            )))
        }
    }

    /// Serialise a slice of `Self` into an Arrow [`RecordBatch`].
    ///
    /// The default implementation calls `serde_arrow::to_arrow` then wraps the
    /// result in a `RecordBatch`. Override for custom encodings.
    fn to_record_batch(values: &[Self]) -> Result<RecordBatch, SaciError>
    where
        Self: serde::Serialize + Sized,
    {
        let schema = Self::schema();
        let fields: Vec<_> = schema.fields().iter().cloned().collect();
        let arrays = serde_arrow::to_arrow(&fields, values)
            .map_err(|e| SaciError::generic(format!("serde_arrow serialization error: {e}")))?;
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| SaciError::generic(format!("RecordBatch construction error: {e}")))
    }

    /// Deserialise an Arrow [`RecordBatch`] back into `Vec<Self>`.
    ///
    /// The default implementation calls `serde_arrow::from_record_batch`.
    /// Override for custom decodings.
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>, SaciError>
    where
        Self: for<'de> serde::Deserialize<'de> + Sized,
    {
        serde_arrow::from_record_batch(batch)
            .map_err(|e| SaciError::generic(format!("serde_arrow deserialization error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Tick {
        id: u64,
        price: f64,
    }

    impl Component for Tick {
        fn name() -> &'static str {
            "Tick"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("price", DataType::Float64, false),
            ]))
        }
    }

    #[test]
    fn test_round_trip_via_serde_arrow() {
        let original: Vec<Tick> = (0..10)
            .map(|i| Tick {
                id: i,
                price: i as f64 * 1.5,
            })
            .collect();

        let batch = Tick::to_record_batch(&original).expect("serialization failed");
        assert_eq!(batch.num_rows(), 10);

        let recovered = Tick::from_record_batch(&batch).expect("deserialization failed");
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_name_and_schema() {
        assert_eq!(Tick::name(), "Tick");
        let schema = Tick::schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "price");
    }
}
