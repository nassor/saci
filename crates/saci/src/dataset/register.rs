use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::component::Component;
use crate::error::SaciError;

use super::{Dataset, intern_component_name};

impl Dataset {
    /// Register a component type; see [`register`](Self::register).
    #[deprecated(since = "0.3.0", note = "use `register`")]
    pub fn register_component<C: Component>(&mut self) -> Result<(), SaciError> {
        self.register::<C>()
    }

    /// Register a component type, creating an empty chunk list for it.
    ///
    /// Idempotent: repeat calls for the same `C` return `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `row_count > 0`, since the new
    /// component would then hold fewer rows than the others. Register every
    /// component before appending data, or call [`clear`](Dataset::clear)
    /// first.
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
    /// let mut dataset = Dataset::new();
    /// dataset.register::<Price>().unwrap();
    /// ```
    pub fn register<C: Component>(&mut self) -> Result<(), SaciError> {
        if self.schemas.contains(C::name()) {
            return Ok(());
        }
        self.refuse_after_rows(C::name())?;
        self.schemas.register::<C>();
        self.insert_empty(C::name(), C::schema());
        Ok(())
    }

    /// Register a component by name with an explicit Arrow [`Schema`], for a
    /// schema known only at runtime: one read from Parquet metadata, a
    /// WebAssembly processor's `describe`, a native plugin's manifest. The
    /// schema version is `1`.
    ///
    /// The name is interned, so registering the same name repeatedly reuses
    /// one allocation. Idempotent for a name already registered.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if rows have already been
    /// appended.
    pub fn register_schema(&mut self, name: &str, schema: Arc<Schema>) -> Result<(), SaciError> {
        self.register_schema_versioned(name, schema, 1)
    }

    /// Consuming, chainable form of [`register`](Self::register), so a
    /// dataset is built and registered in one expression.
    ///
    /// # Errors
    ///
    /// Same as [`register`](Self::register): returns
    /// [`SaciError::Configuration`] if rows have already been appended.
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
    /// #[derive(Component, Serialize, Deserialize)]
    /// struct Volume { shares: u64 }
    ///
    /// let dataset = Dataset::new().with::<Price>()?.with::<Volume>()?;
    /// assert!(dataset.schemas().contains("Price"));
    /// assert!(dataset.schemas().contains("Volume"));
    /// # Ok::<(), SaciError>(())
    /// ```
    pub fn with<C: Component>(mut self) -> Result<Self, SaciError> {
        self.register::<C>()?;
        Ok(self)
    }

    /// Consuming, chainable form of [`register_schema`](Self::register_schema).
    ///
    /// # Errors
    ///
    /// Same as [`register_schema`](Self::register_schema): returns
    /// [`SaciError::Configuration`] if rows have already been appended.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use arrow_schema::{DataType, Field, Schema};
    /// use saci::prelude::*;
    ///
    /// let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    /// let dataset = Dataset::new().with_schema("Sale", schema)?;
    /// assert!(dataset.schemas().contains("Sale"));
    /// # Ok::<(), SaciError>(())
    /// ```
    pub fn with_schema(mut self, name: &str, schema: Arc<Schema>) -> Result<Self, SaciError> {
        self.register_schema(name, schema)?;
        Ok(self)
    }

    /// [`register_schema`](Self::register_schema) with an explicit schema
    /// `version`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if rows have already been
    /// appended.
    pub fn register_schema_versioned(
        &mut self,
        name: &str,
        schema: Arc<Schema>,
        version: u32,
    ) -> Result<(), SaciError> {
        if self.schemas.contains(name) {
            return Ok(());
        }
        self.refuse_after_rows(name)?;
        let name = intern_component_name(name);
        self.schemas.register_raw(name, schema.clone(), version);
        self.insert_empty(name, schema);
        Ok(())
    }

    fn refuse_after_rows(&self, name: &str) -> Result<(), SaciError> {
        if self.row_count == 0 {
            return Ok(());
        }
        Err(SaciError::configuration(format!(
            "Dataset: cannot register new component '{name}' after rows have been appended \
             (row_count={}). Register all components before appending data, or call \
             clear() first.",
            self.row_count
        )))
    }

    fn insert_empty(&mut self, name: &'static str, schema: Arc<Schema>) {
        let empty = RecordBatch::new_empty(schema);
        self.empty_templates.insert(name, empty.clone());
        self.components.insert(name, vec![empty]);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;

    #[derive(Serialize, Deserialize)]
    struct Reg {
        value: f64,
    }

    impl Component for Reg {
        fn name() -> &'static str {
            "Reg"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                false,
            )]))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct RegB {
        val: u64,
    }

    impl Component for RegB {
        fn name() -> &'static str {
            "RegB"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "val",
                DataType::UInt64,
                false,
            )]))
        }
    }

    #[test]
    fn test_register_ok() {
        let mut ds = Dataset::new();
        assert!(ds.register::<Reg>().is_ok());
    }

    #[test]
    fn test_register_idempotent() {
        let mut ds = Dataset::new();
        ds.register::<Reg>().unwrap();
        assert!(ds.register::<Reg>().is_ok());
    }

    #[test]
    fn test_register_after_rows_different_type_errors() {
        let mut ds = Dataset::new();
        ds.register::<Reg>().unwrap();
        ds.append::<Reg>(&[Reg { value: 1.0 }]).unwrap();
        let result = ds.register::<RegB>();
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("RegB"));
    }

    #[test]
    fn test_register_schema() {
        let mut ds = Dataset::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        ds.register_schema("raw_test", schema).unwrap();
        assert!(ds.schemas().contains("raw_test"));
    }

    #[test]
    fn test_register_schema_after_rows_is_a_configuration_error() {
        let mut ds = Dataset::new();
        ds.register::<Reg>().unwrap();
        ds.append::<Reg>(&[Reg { value: 1.0 }]).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let err = ds.register_schema("late", schema).unwrap_err();
        assert_eq!(err.category(), "configuration");
    }

    #[test]
    fn test_with_and_with_schema_chain() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let ds = Dataset::new()
            .with::<Reg>()
            .unwrap()
            .with::<RegB>()
            .unwrap()
            .with_schema("raw_test", schema)
            .unwrap();
        assert!(ds.schemas().contains("Reg"));
        assert!(ds.schemas().contains("RegB"));
        assert!(ds.schemas().contains("raw_test"));
    }

    #[test]
    fn test_with_after_rows_is_a_configuration_error() {
        let mut ds = Dataset::new().with::<Reg>().unwrap();
        ds.append::<Reg>(&[Reg { value: 1.0 }]).unwrap();
        let err = ds.with::<RegB>().err().expect("rows already appended");
        assert_eq!(err.category(), "configuration");
    }
}
