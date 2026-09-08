//! [`SchemaRegistry`], a catalogue of Arrow schemas for registered components.
//!
//! The registry lives inside [`Dataset`](super::dataset::Dataset) and records the
//! canonical [`Schema`] for every component registered with
//! [`register_component`](super::dataset::Dataset::register_component). The
//! pipeline manages it; it is public to support schema introspection and IPC
//! round-trips.

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use super::component::Component;
use crate::SaciResult;

/// Per-component schema metadata stored in [`SchemaRegistry`].
pub struct SchemaEntry {
    /// Schema version recorded at registration time.
    pub version: u32,
    /// Arrow schema describing the component's fields.
    pub schema: Arc<Schema>,
    /// Migration function: `(from_version, batch) -> SaciResult<RecordBatch>`.
    pub migrate: Arc<dyn Fn(u32, RecordBatch) -> SaciResult<RecordBatch> + Send + Sync>,
}

impl std::fmt::Debug for SchemaEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaEntry")
            .field("version", &self.version)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// A registry mapping component names to their Arrow [`Schema`]s and version metadata.
///
/// Stores deduplicated `Arc<Schema>` handles so that multiple borrows of the
/// same schema are free after the first insert. Also captures the schema version
/// and migration function for each component, enabling IPC round-trip migration.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    schemas: HashMap<&'static str, SchemaEntry>,
}

impl SchemaRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Register the schema for component `C`, capturing its version and migrate fn.
    ///
    /// If a schema for `C::name()` already exists, this is a no-op.
    pub fn register<C: Component>(&mut self) {
        self.schemas
            .entry(C::name())
            .or_insert_with(|| SchemaEntry {
                version: C::version(),
                schema: C::schema(),
                migrate: Arc::new(|from_version, batch| C::migrate(from_version, batch)),
            });
    }

    /// Register an arbitrary schema under `name` without a typed [`Component`].
    ///
    /// Used by Source implementations where the schema is determined at runtime
    /// (e.g. from Parquet file metadata). If `name` is already registered this
    /// is a no-op. The migrate function defaults to reject-on-mismatch.
    pub fn register_raw(&mut self, name: &'static str, schema: Arc<Schema>, version: u32) {
        self.schemas.entry(name).or_insert_with(|| SchemaEntry {
            version,
            schema,
            migrate: Arc::new(move |from_version, batch| {
                if from_version == version {
                    Ok(batch)
                } else {
                    Err(crate::SaciError::configuration(format!(
                        "component '{name}' version mismatch: on-disk={from_version}, current={version}"
                    )))
                }
            }),
        });
    }

    /// Retrieve the schema for the named component, if registered.
    pub fn get(&self, name: &str) -> Option<Arc<Schema>> {
        self.schemas.get(name).map(|e| e.schema.clone())
    }

    /// Retrieve the schema version for the named component, if registered.
    pub fn get_version(&self, name: &str) -> Option<u32> {
        self.schemas.get(name).map(|e| e.version)
    }

    /// Migrate `batch` for the named component from `from_version` to the current version.
    ///
    /// # Errors
    ///
    /// Returns an error if the component is not registered or migration fails.
    pub fn migrate(
        &self,
        name: &str,
        from_version: u32,
        batch: RecordBatch,
    ) -> SaciResult<RecordBatch> {
        match self.schemas.get(name) {
            Some(entry) => (entry.migrate)(from_version, batch),
            None => Err(crate::SaciError::configuration(format!(
                "SchemaRegistry::migrate: component '{name}' is not registered"
            ))),
        }
    }

    /// Return `true` if a schema has been registered under `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.schemas.contains_key(name)
    }

    /// Iterate over all (name, entry) pairs.
    ///
    /// Note: `HashMap` does not preserve insertion order; this iterates in
    /// arbitrary order. Sort by name externally if order matters.
    pub fn iter(&self) -> impl Iterator<Item = (&&'static str, &SchemaEntry)> {
        self.schemas.iter()
    }

    /// Number of registered schemas.
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// `true` if no schemas have been registered.
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    /// Deterministic FNV-1a fingerprint over all registered (name, version, field-names) tuples.
    ///
    /// Useful as a cheap schema-identity token for checkpoint tagging. Sorted by
    /// component name for stability.
    pub fn fingerprint(&self) -> u32 {
        const FNV_OFFSET: u32 = 2166136261;
        const FNV_PRIME: u32 = 16777619;

        let mut names: Vec<&&'static str> = self.schemas.keys().collect();
        names.sort();

        let mut hash = FNV_OFFSET;
        for name in names {
            let entry = &self.schemas[name];
            for byte in name.as_bytes() {
                hash ^= *byte as u32;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
            let ver_bytes = entry.version.to_le_bytes();
            for byte in &ver_bytes {
                hash ^= *byte as u32;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
            // Include field names for structural identity.
            for field in entry.schema.fields() {
                for byte in field.name().as_bytes() {
                    hash ^= *byte as u32;
                    hash = hash.wrapping_mul(FNV_PRIME);
                }
            }
        }
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;

    #[derive(Serialize, Deserialize)]
    struct Foo {
        x: f32,
    }

    impl Component for Foo {
        fn name() -> &'static str {
            "Foo"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, false)]))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Bar {
        y: i32,
    }

    impl Component for Bar {
        fn name() -> &'static str {
            "Bar"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("y", DataType::Int32, false)]))
        }
    }

    #[test]
    fn test_register_and_get() {
        let mut reg = SchemaRegistry::new();
        assert!(!reg.contains("Foo"));
        reg.register::<Foo>();
        assert!(reg.contains("Foo"));
        let schema = reg.get("Foo").unwrap();
        assert_eq!(schema.fields().len(), 1);
    }

    #[test]
    fn test_register_idempotent() {
        let mut reg = SchemaRegistry::new();
        reg.register::<Foo>();
        reg.register::<Foo>(); // second call is a no-op
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_multiple_components() {
        let mut reg = SchemaRegistry::new();
        reg.register::<Foo>();
        reg.register::<Bar>();
        assert_eq!(reg.len(), 2);
        assert!(reg.contains("Bar"));
        let bar_schema = reg.get("Bar").unwrap();
        assert_eq!(bar_schema.field(0).name(), "y");
    }

    #[test]
    fn test_get_unknown_returns_none() {
        let reg = SchemaRegistry::new();
        assert!(reg.get("NonExistent").is_none());
    }

    #[test]
    fn test_is_empty_and_len() {
        let mut reg = SchemaRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
        reg.register::<Foo>();
        assert!(!reg.is_empty());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_get_version_typed() {
        let mut reg = SchemaRegistry::new();
        reg.register::<Foo>();
        assert_eq!(reg.get_version("Foo"), Some(1));
    }

    #[test]
    fn test_get_version_raw() {
        let mut reg = SchemaRegistry::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, false)]));
        reg.register_raw("raw_comp", schema, 3);
        assert_eq!(reg.get_version("raw_comp"), Some(3));
    }

    #[test]
    fn test_migrate_same_version_ok() {
        use arrow_array::Float32Array;
        let mut reg = SchemaRegistry::new();
        reg.register::<Foo>();
        let schema = Foo::schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float32Array::from(vec![1.0f32])) as Arc<dyn arrow_array::Array>],
        )
        .unwrap();
        let result = reg.migrate("Foo", 1, batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_migrate_version_mismatch_errors() {
        use arrow_array::Float32Array;
        let mut reg = SchemaRegistry::new();
        reg.register::<Foo>();
        let schema = Foo::schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float32Array::from(vec![1.0f32])) as Arc<dyn arrow_array::Array>],
        )
        .unwrap();
        let result = reg.migrate("Foo", 99, batch);
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("version mismatch")
        );
    }

    #[test]
    fn test_migrate_unregistered_errors() {
        let reg = SchemaRegistry::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let result = reg.migrate("ghost", 1, batch);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("not registered"));
    }

    #[test]
    fn test_fingerprint_stable() {
        let mut reg = SchemaRegistry::new();
        reg.register::<Foo>();
        reg.register::<Bar>();
        let f1 = reg.fingerprint();
        let f2 = reg.fingerprint();
        assert_eq!(f1, f2);
    }

    #[test]
    fn test_fingerprint_differs_on_version() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, false)]));
        let mut reg1 = SchemaRegistry::new();
        reg1.register_raw("comp", schema.clone(), 1);

        let mut reg2 = SchemaRegistry::new();
        reg2.register_raw("comp", schema, 2);

        assert_ne!(reg1.fingerprint(), reg2.fingerprint());
    }

    #[test]
    fn test_fingerprint_empty() {
        let reg = SchemaRegistry::new();
        // Just ensure it doesn't panic and returns a value.
        let _ = reg.fingerprint();
    }
}
