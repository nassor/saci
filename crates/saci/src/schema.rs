//! [`SchemaRegistry`], a catalogue of Arrow schemas for registered components.
//!
//! The registry lives inside [`Dataset`](super::dataset::Dataset) and records the
//! canonical [`Schema`] for every component registered with
//! [`register`](super::dataset::Dataset::register). The
//! pipeline manages it; it is public to support schema introspection and IPC
//! round-trips.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchOptions, new_null_array};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Schema, TimeUnit};
use serde::{Deserialize, Serialize};

use super::component::Component;
use crate::{SaciError, SaciResult};

/// Policy governing automatic schema drift and evolution across workflow links
/// and healed connectors.
///
/// Defaults to [`EvolutionPolicy::strict()`], where every field name, data
/// type, and nullability must match identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EvolutionPolicy {
    /// Allow downstream target schemas to include nullable columns absent from
    /// upstream batches (synthesized as all-null arrays).
    pub add_nullable_columns: bool,
    /// Allow upstream batches to include extra columns absent from downstream
    /// target schemas (projected out).
    pub drop_extra_columns: bool,
    /// Allow safe, lossless data type widening (e.g., `Int32 -> Int64`,
    /// `Float32 -> Float64`, `Utf8 -> LargeUtf8`, `Date32 -> Timestamp`).
    pub widen_types: bool,
}

impl EvolutionPolicy {
    /// Strict schema matching: no missing columns, no extra columns, no type widening.
    #[must_use]
    pub const fn strict() -> Self {
        Self {
            add_nullable_columns: false,
            drop_extra_columns: false,
            widen_types: false,
        }
    }

    /// Full backward/forward-compatible evolution: add missing nullable columns,
    /// project out extra columns, and safely widen compatible types.
    #[must_use]
    pub const fn compatible() -> Self {
        Self {
            add_nullable_columns: true,
            drop_extra_columns: true,
            widen_types: true,
        }
    }

    /// Additive evolution: add missing nullable columns and drop extra columns,
    /// without widening existing column types.
    #[must_use]
    pub const fn additive() -> Self {
        Self {
            add_nullable_columns: true,
            drop_extra_columns: true,
            widen_types: false,
        }
    }

    /// Widen-only evolution: widen compatible column types without adding or
    /// dropping columns.
    #[must_use]
    pub const fn widen() -> Self {
        Self {
            add_nullable_columns: false,
            drop_extra_columns: false,
            widen_types: true,
        }
    }

    /// Returns `true` when all evolution flags are disabled.
    #[must_use]
    pub const fn is_strict(&self) -> bool {
        !self.add_nullable_columns && !self.drop_extra_columns && !self.widen_types
    }

    /// Parse a named evolution policy (`"strict"`, `"compatible"`, `"additive"`, `"widen"`).
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] on an unrecognised policy name.
    pub fn from_name(name: &str) -> SaciResult<Self> {
        match name.to_ascii_lowercase().as_str() {
            "strict" | "none" => Ok(Self::strict()),
            "compatible" | "full" => Ok(Self::compatible()),
            "additive" => Ok(Self::additive()),
            "widen" => Ok(Self::widen()),
            _ => Err(SaciError::configuration(format!(
                "unknown schema_evolution policy '{name}' (expected strict, compatible, additive, or widen)"
            ))),
        }
    }

    /// Canonical string representation if this matches a named preset.
    #[must_use]
    pub const fn preset_name(&self) -> Option<&'static str> {
        match (
            self.add_nullable_columns,
            self.drop_extra_columns,
            self.widen_types,
        ) {
            (false, false, false) => Some("strict"),
            (true, true, true) => Some("compatible"),
            (true, true, false) => Some("additive"),
            (false, false, true) => Some("widen"),
            _ => None,
        }
    }
}

fn time_unit_rank(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// Returns `true` if values of type `from` can be losslessly widened to type `to`.
#[must_use]
pub fn can_widen_type(from: &DataType, to: &DataType) -> bool {
    if from == to {
        return true;
    }
    match (from, to) {
        // Signed integer widening
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64) => true,

        // Unsigned integer widening
        (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
        | (DataType::UInt32, DataType::UInt64) => true,

        // Unsigned -> strictly wider signed integer
        (DataType::UInt8, DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (DataType::UInt16, DataType::Int32 | DataType::Int64)
        | (DataType::UInt32, DataType::Int64) => true,

        // Floating-point widening (exact representability for small ints into f32/f64)
        (DataType::Float32, DataType::Float64)
        | (
            DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16,
            DataType::Float32,
        )
        | (
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32,
            DataType::Float64,
        ) => true,

        // Decimal widening (more scale and at least as many integral digits)
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            *s2 >= *s1 && (*p2 as i16 - *s2 as i16) >= (*p1 as i16 - *s1 as i16)
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => {
            *s2 >= *s1 && (*p2 as i16 - *s2 as i16) >= (*p1 as i16 - *s1 as i16)
        }

        // String / binary widening
        (DataType::Utf8, DataType::LargeUtf8) | (DataType::Binary, DataType::LargeBinary) => true,

        // Temporal widening
        (DataType::Date32, DataType::Date64 | DataType::Timestamp(_, _))
        | (DataType::Date64, DataType::Timestamp(_, _)) => true,
        (DataType::Timestamp(u1, tz1), DataType::Timestamp(u2, tz2)) => {
            tz1 == tz2 && time_unit_rank(*u2) >= time_unit_rank(*u1)
        }

        // Null can widen to any type
        (DataType::Null, _) => true,

        _ => false,
    }
}

/// Verify whether an `upstream` Arrow [`Schema`] can evolve into `downstream`
/// under `policy`.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] describing the incompatible field,
/// nullability tightening, missing required column, or disallowed type change.
pub fn check_schema_compatibility(
    upstream: &Schema,
    downstream: &Schema,
    policy: EvolutionPolicy,
) -> SaciResult<()> {
    if policy.is_strict() {
        if upstream.fields() == downstream.fields() {
            return Ok(());
        }
        return Err(SaciError::configuration(format!(
            "strict schema mismatch: upstream {:?} != downstream {:?}",
            upstream.fields(),
            downstream.fields()
        )));
    }

    if !policy.drop_extra_columns {
        for up_field in upstream.fields() {
            if downstream.field_with_name(up_field.name()).is_err() {
                return Err(SaciError::configuration(format!(
                    "schema evolution disallowed dropping upstream column '{}'",
                    up_field.name()
                )));
            }
        }
    }

    for down_field in downstream.fields() {
        match upstream.field_with_name(down_field.name()) {
            Ok(up_field) => {
                if !down_field.is_nullable() && up_field.is_nullable() {
                    return Err(SaciError::configuration(format!(
                        "schema evolution cannot tighten nullable column '{}' to non-nullable",
                        down_field.name()
                    )));
                }
                if up_field.data_type() != down_field.data_type()
                    && (!policy.widen_types
                        || !can_widen_type(up_field.data_type(), down_field.data_type()))
                {
                    return Err(SaciError::configuration(format!(
                        "schema evolution cannot convert column '{}' from {:?} to {:?}",
                        down_field.name(),
                        up_field.data_type(),
                        down_field.data_type()
                    )));
                }
            }
            Err(_) => {
                if !policy.add_nullable_columns {
                    return Err(SaciError::configuration(format!(
                        "schema evolution disallowed adding missing column '{}'",
                        down_field.name()
                    )));
                }
                if !down_field.is_nullable() {
                    return Err(SaciError::configuration(format!(
                        "schema evolution cannot synthesize missing non-nullable column '{}'",
                        down_field.name()
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Adapt `batch` to conform to `target` schema according to `policy`.
///
/// - Extra columns in `batch` not present in `target` are dropped when
///   `policy.drop_extra_columns` is enabled.
/// - Missing nullable columns in `target` are synthesized as all-null arrays
///   when `policy.add_nullable_columns` is enabled.
/// - Columns whose types can be safely widened are cast when `policy.widen_types`
///   is enabled.
///
/// # Errors
///
/// Returns [`SaciError`] if the schemas are incompatible under `policy` or if
/// Arrow casting fails.
pub fn evolve_batch(
    batch: &RecordBatch,
    target: &Arc<Schema>,
    policy: EvolutionPolicy,
) -> SaciResult<RecordBatch> {
    if Arc::ptr_eq(batch.schema_ref(), target) || batch.schema_ref().fields() == target.fields() {
        return Ok(batch.clone());
    }

    check_schema_compatibility(batch.schema_ref(), target, policy)?;

    let num_rows = batch.num_rows();
    let cast_opts = CastOptions {
        safe: false,
        ..Default::default()
    };

    let mut columns = Vec::with_capacity(target.fields().len());
    for target_field in target.fields() {
        match batch.column_by_name(target_field.name()) {
            Some(col) => {
                if col.data_type() == target_field.data_type() {
                    columns.push(Arc::clone(col));
                } else {
                    let intermediate_ts = match (col.data_type(), target_field.data_type()) {
                        (
                            DataType::Date32 | DataType::Date64,
                            DataType::Timestamp(unit, Some(_)),
                        ) => Some(DataType::Timestamp(*unit, None)),
                        _ => None,
                    };
                    let cast_col = if let Some(naive_ts) = intermediate_ts {
                        let widen_err = |e: arrow_schema::ArrowError| {
                            SaciError::generic(format!(
                                "evolve_batch: cannot widen column '{}' from {:?} to {:?}: {e}",
                                target_field.name(),
                                col.data_type(),
                                target_field.data_type()
                            ))
                        };
                        let step1 = cast_with_options(col.as_ref(), &naive_ts, &cast_opts)
                            .map_err(widen_err)?;
                        let data = step1
                            .to_data()
                            .into_builder()
                            .data_type(target_field.data_type().clone())
                            .build()
                            .map_err(widen_err)?;
                        arrow_array::make_array(data)
                    } else {
                        cast_with_options(col.as_ref(), target_field.data_type(), &cast_opts)
                            .map_err(|e| {
                                SaciError::generic(format!(
                                    "evolve_batch: cannot widen column '{}' from {:?} to {:?}: {e}",
                                    target_field.name(),
                                    col.data_type(),
                                    target_field.data_type()
                                ))
                            })?
                    };
                    columns.push(cast_col);
                }
            }
            None => {
                columns.push(new_null_array(target_field.data_type(), num_rows));
            }
        }
    }

    RecordBatch::try_new_with_options(
        Arc::clone(target),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .map_err(|e| SaciError::generic(format!("evolve_batch: RecordBatch rebuild error: {e}")))
}

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

    /// Updates an already-registered dynamic schema in place, or inserts it.
    pub(crate) fn update_schema(&mut self, name: &'static str, schema: Arc<Schema>) {
        if let Some(entry) = self.schemas.get_mut(name) {
            entry.schema = schema;
        } else {
            self.register_raw(name, schema, 1);
        }
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

    #[test]
    fn test_evolution_policy_and_evolve_batch() {
        use arrow_array::{Int32Array, Int64Array, StringArray};
        let upstream = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("legacy", DataType::Utf8, true),
        ]));
        let downstream = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]));

        // Strict rejects differences.
        assert!(
            check_schema_compatibility(&upstream, &downstream, EvolutionPolicy::strict()).is_err()
        );

        // Additive rejects Int32 -> Int64 widening.
        assert!(
            check_schema_compatibility(&upstream, &downstream, EvolutionPolicy::additive())
                .is_err()
        );

        // Compatible accepts dropping `legacy`, adding nullable `note`, and widening `id`.
        check_schema_compatibility(&upstream, &downstream, EvolutionPolicy::compatible())
            .expect("compatible evolution");

        let batch = RecordBatch::try_new(
            upstream,
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let evolved = evolve_batch(&batch, &downstream, EvolutionPolicy::compatible()).unwrap();
        assert_eq!(evolved.schema(), downstream);
        assert_eq!(
            evolved
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10i64, 20i64]
        );
        assert_eq!(evolved.column(1).null_count(), 2);
    }

    #[test]
    fn test_evolution_rejects_missing_non_nullable_or_narrowing() {
        let upstream = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let missing_req = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("required", DataType::Utf8, false),
        ]));
        assert!(
            check_schema_compatibility(&upstream, &missing_req, EvolutionPolicy::compatible())
                .is_err()
        );

        let tighten = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        assert!(
            check_schema_compatibility(&upstream, &tighten, EvolutionPolicy::compatible()).is_err()
        );

        let narrow = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        assert!(
            check_schema_compatibility(&upstream, &narrow, EvolutionPolicy::compatible()).is_err()
        );
    }

    #[test]
    fn test_evolve_batch_widens_date32_and_date64_to_timestamptz() {
        use arrow_array::{Date32Array, Date64Array, TimestampMillisecondArray};
        use arrow_schema::TimeUnit;

        let upstream = Arc::new(Schema::new(vec![
            Field::new("d32", DataType::Date32, false),
            Field::new("d64", DataType::Date64, false),
        ]));
        let target = Arc::new(Schema::new(vec![
            Field::new(
                "d32",
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
                false,
            ),
            Field::new(
                "d64",
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            upstream,
            vec![
                Arc::new(Date32Array::from(vec![1])),
                Arc::new(Date64Array::from(vec![86_400_000])),
            ],
        )
        .unwrap();

        let evolved = evolve_batch(&batch, &target, EvolutionPolicy::compatible())
            .expect("Date32/Date64 -> TimestampTZ widens cleanly");
        assert_eq!(evolved.schema(), target);
        assert_eq!(
            evolved
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            86_400_000
        );
        assert_eq!(
            evolved
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            86_400_000
        );
    }
}
