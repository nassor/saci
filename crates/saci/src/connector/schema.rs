//! The `field` config parser shared by every connector that declares a schema.
//!
//! A connector whose format carries no schema of its own declares one as
//! `field` nodes carrying `{id, type, nullable}`, plus `precision`/`scale` for
//! a `decimal128`, `item` for a `list` and `native_type` for a database
//! connector that asserts the column's type on the server:
//!
//! ```kdl
//! field "id" type="int64" nullable=#false
//! field "total" type="decimal128" precision=12 scale=2
//! ```
//!
//! [`FieldSpec`] is the one entry shape. A database connector holds a
//! `Vec<FieldSpec>` in its config struct and narrows [`FieldType`] to the
//! types it maps; the connectors that read the key out of an opaque table call
//! [`parse_fields`] or [`parse_optional_fields`], then [`schema_of`].

use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use serde::Deserialize;

use crate::config::ConfigValue;
use crate::error::{SaciError, SaciResult};

/// The config key a schema is declared under.
const FIELD_KEY: &str = "field";

fn default_true() -> bool {
    true
}

/// The declared type of a column.
///
/// Each maps to exactly one Arrow [`DataType`], the one [`FieldSpec::to_arrow`]
/// builds. The spelling in a config file is the lowercase name: `int64`,
/// `uint8`, `utf8`, `largeutf8`, `time64_micros`, `timestamp_micros_utc`. A
/// connector accepts the subset it maps and refuses the rest when it
/// validates its fields.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    /// Arrow `Boolean`.
    Boolean,
    /// Arrow `Int8`.
    Int8,
    /// Arrow `Int16`.
    Int16,
    /// Arrow `Int32`.
    Int32,
    /// Arrow `Int64`.
    Int64,
    /// Arrow `UInt8`.
    #[serde(rename = "uint8")]
    UInt8,
    /// Arrow `UInt16`.
    #[serde(rename = "uint16")]
    UInt16,
    /// Arrow `UInt32`.
    #[serde(rename = "uint32")]
    UInt32,
    /// Arrow `UInt64`.
    #[serde(rename = "uint64")]
    UInt64,
    /// Arrow `Float32`.
    Float32,
    /// Arrow `Float64`.
    Float64,
    /// Arrow `Utf8`.
    Utf8,
    /// Arrow `LargeUtf8`.
    #[serde(rename = "largeutf8")]
    LargeUtf8,
    /// Arrow `Utf8` carrying a JSON document as text.
    Json,
    /// Arrow `Binary`.
    Binary,
    /// Arrow `Date32`: days since the UNIX epoch.
    Date32,
    /// Arrow `Date64`: milliseconds since the UNIX epoch.
    Date64,
    /// Arrow `Time64(Microsecond)`.
    Time64Micros,
    /// Arrow `Timestamp(Microsecond, None)`.
    TimestampMicros,
    /// Arrow `Timestamp(Microsecond, "UTC")`.
    TimestampMicrosUtc,
    /// Arrow `Timestamp(Millisecond, None)`.
    TimestampMillis,
    /// Arrow `FixedSizeBinary(16)` carrying a UUID.
    Uuid,
    /// Arrow `Decimal128`. Needs `precision` and `scale`.
    Decimal128,
    /// Arrow `Interval(MonthDayNano)`.
    IntervalMonthDayNano,
    /// Arrow `List` of the field's `item` type. Needs `item`.
    List,
}

impl FieldType {
    /// The name used in config files and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            FieldType::Boolean => "boolean",
            FieldType::Int8 => "int8",
            FieldType::Int16 => "int16",
            FieldType::Int32 => "int32",
            FieldType::Int64 => "int64",
            FieldType::UInt8 => "uint8",
            FieldType::UInt16 => "uint16",
            FieldType::UInt32 => "uint32",
            FieldType::UInt64 => "uint64",
            FieldType::Float32 => "float32",
            FieldType::Float64 => "float64",
            FieldType::Utf8 => "utf8",
            FieldType::LargeUtf8 => "largeutf8",
            FieldType::Json => "json",
            FieldType::Binary => "binary",
            FieldType::Date32 => "date32",
            FieldType::Date64 => "date64",
            FieldType::Time64Micros => "time64_micros",
            FieldType::TimestampMicros => "timestamp_micros",
            FieldType::TimestampMicrosUtc => "timestamp_micros_utc",
            FieldType::TimestampMillis => "timestamp_millis",
            FieldType::Uuid => "uuid",
            FieldType::Decimal128 => "decimal128",
            FieldType::IntervalMonthDayNano => "interval_month_day_nano",
            FieldType::List => "list",
        }
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One declared column: its name, type and nullability.
///
/// Flat rather than an internally tagged enum, so `deny_unknown_fields` still
/// applies; `#[serde(flatten)]` would switch it off. A key the entry does not
/// name is an error.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    /// Column name.
    ///
    /// Read from `id`, because [`crate::config`] puts a node's leading
    /// argument, the `"total"` in `field "total" type="float64"`, under that
    /// key.
    #[serde(rename = "id")]
    pub name: String,
    /// Declared type.
    #[serde(rename = "type")]
    pub ty: FieldType,
    /// Whether the Arrow field admits NULL. Defaults to `true`.
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// Total digits. `decimal128` only, whether as the type or as the `item`,
    /// and required there.
    #[serde(default)]
    pub precision: Option<u8>,
    /// Fractional digits. `decimal128` only, and required there.
    #[serde(default)]
    pub scale: Option<i8>,
    /// Element type. `list` only, and required there.
    ///
    /// Typed as [`FieldType`], so `item="list"` parses; it is refused with a
    /// message naming the column, because Arrow's `List` here is
    /// one-dimensional.
    #[serde(default)]
    pub item: Option<FieldType>,
    /// The type this column has on the server, for a database connector that
    /// asserts it on connect or forces the codec from it.
    ///
    /// Never changes the Arrow type: `type` alone decides that. A connector
    /// with no native types refuses the key.
    #[serde(default)]
    pub native_type: Option<String>,
}

impl FieldSpec {
    /// The Arrow field this spec declares.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `decimal128` is missing
    /// `precision`/`scale` or carries an out-of-range one, when
    /// `precision`/`scale` are set on a type that carries no decimal, when
    /// `item` is set on a type other than `list`, when `list` carries no
    /// `item`, and when `item` is itself `list`: Arrow's `List` here is
    /// one-dimensional.
    pub fn to_arrow(&self) -> SaciResult<Field> {
        if self.item.is_some() && self.ty != FieldType::List {
            return Err(SaciError::configuration(format!(
                "field '{}': 'item' applies to type \"list\" only, not \"{}\"",
                self.name, self.ty
            )));
        }
        let carries_decimal = self.ty == FieldType::Decimal128
            || (self.ty == FieldType::List && self.item == Some(FieldType::Decimal128));
        if !carries_decimal && (self.precision.is_some() || self.scale.is_some()) {
            return Err(SaciError::configuration(format!(
                "field '{}': precision/scale apply to type \"decimal128\" only, not \"{}\"",
                self.name, self.ty
            )));
        }

        let data_type = match self.ty {
            FieldType::List => {
                let item = self.item.ok_or_else(|| {
                    SaciError::configuration(format!(
                        "field '{}': type \"list\" requires 'item'",
                        self.name
                    ))
                })?;
                DataType::List(Arc::new(Field::new_list_field(self.scalar(item)?, true)))
            }
            scalar => self.scalar(scalar)?,
        };

        Ok(Field::new(&self.name, data_type, self.nullable))
    }

    /// The Arrow type of a scalar declared type.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for `list`, which is not a scalar,
    /// and for a `decimal128` whose `precision`/`scale` are missing or illegal.
    fn scalar(&self, ty: FieldType) -> SaciResult<DataType> {
        Ok(match ty {
            FieldType::Boolean => DataType::Boolean,
            FieldType::Int8 => DataType::Int8,
            FieldType::Int16 => DataType::Int16,
            FieldType::Int32 => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::UInt8 => DataType::UInt8,
            FieldType::UInt16 => DataType::UInt16,
            FieldType::UInt32 => DataType::UInt32,
            FieldType::UInt64 => DataType::UInt64,
            FieldType::Float32 => DataType::Float32,
            FieldType::Float64 => DataType::Float64,
            FieldType::Utf8 | FieldType::Json => DataType::Utf8,
            FieldType::LargeUtf8 => DataType::LargeUtf8,
            FieldType::Binary => DataType::Binary,
            FieldType::Date32 => DataType::Date32,
            FieldType::Date64 => DataType::Date64,
            FieldType::Time64Micros => DataType::Time64(TimeUnit::Microsecond),
            FieldType::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
            FieldType::TimestampMicrosUtc => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            FieldType::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
            FieldType::Uuid => DataType::FixedSizeBinary(16),
            FieldType::IntervalMonthDayNano => DataType::Interval(IntervalUnit::MonthDayNano),
            FieldType::Decimal128 => {
                let (precision, scale) = self.decimal_params()?;
                DataType::Decimal128(precision, scale)
            }
            FieldType::List => {
                return Err(SaciError::configuration(format!(
                    "field '{}': 'item' must be a scalar type, not \"list\"; Arrow's List is \
                     one-dimensional",
                    self.name
                )));
            }
        })
    }

    /// The declared `(precision, scale)` of a `decimal128` field.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when either is missing or out of
    /// range: precision is within `1..=38`, scale within `0..=precision`.
    pub(crate) fn decimal_params(&self) -> SaciResult<(u8, i8)> {
        let precision = self.precision.ok_or_else(|| {
            SaciError::configuration(format!(
                "field '{}': type \"decimal128\" requires 'precision'",
                self.name
            ))
        })?;
        let scale = self.scale.ok_or_else(|| {
            SaciError::configuration(format!(
                "field '{}': type \"decimal128\" requires 'scale'",
                self.name
            ))
        })?;
        if precision == 0 || precision > 38 {
            return Err(SaciError::configuration(format!(
                "field '{}': decimal128 precision must be within 1..=38, got {precision}",
                self.name
            )));
        }
        if scale < 0 || i16::from(scale) > i16::from(precision) {
            return Err(SaciError::configuration(format!(
                "field '{}': decimal128 scale must be within 0..=precision ({precision}), \
                 got {scale}",
                self.name
            )));
        }
        Ok((precision, scale))
    }
}

/// A clone of `config` without its `field` entry.
///
/// A factory parses `field` with [`parse_optional_fields`] from the original
/// table, then deserializes its `deny_unknown_fields` config struct from this
/// copy, so the struct never has to declare the key.
pub fn without_fields(config: &ConfigValue) -> ConfigValue {
    let mut table = config.clone();
    if let ConfigValue::Object(entries) = &mut table {
        entries.remove(FIELD_KEY);
    }
    table
}

/// Deserialize a connector's `config` table into its `deny_unknown_fields`
/// struct `T`, ignoring the `field` entry.
///
/// The error names the key the problem sits under: ``unknown field `pth` ..``,
/// ``missing field `path` `` or ``buffer: invalid type: string "x", expected
/// usize``. `context` prefixes it, e.g. `"FileSource"`.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `config` does not match `T`.
pub fn deserialize_config<T: serde::de::DeserializeOwned>(
    config: &ConfigValue,
    context: &str,
) -> Result<T, SaciError> {
    serde_path_to_error::deserialize(without_fields(config))
        .map_err(|e| SaciError::configuration(format!("{context} config: {e}")))
}

/// Refuse a `0` for a config key whose floor is 1.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] reading `{connector}: '{key}' must be
/// at least 1` when `value` is zero.
pub fn at_least_one(connector: &str, key: &str, value: u64) -> Result<(), SaciError> {
    if value == 0 {
        return Err(SaciError::configuration(format!(
            "{connector}: '{key}' must be at least 1"
        )));
    }
    Ok(())
}

/// The declared `field` entries of a `config` table.
///
/// `connector` prefixes every error message, so a bad entry names the
/// connector that rejected it. `nullable` defaults to `true` when omitted.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `field` is missing, is neither
/// one field nor a list of them, or holds an entry [`FieldSpec`] rejects: no
/// `id`, no `type`, a `type` that is not one of the lowercase [`FieldType`]
/// names, or a key the entry does not know.
pub fn parse_fields(config: &ConfigValue, connector: &str) -> SaciResult<Vec<FieldSpec>> {
    parse_optional_fields(config, connector)?.ok_or_else(|| {
        SaciError::configuration(format!(
            "{connector} config requires at least one 'field' entry"
        ))
    })
}

/// The declared `field` entries, or `None` when the key is absent.
///
/// A connector whose byte format may carry its own schema uses this and lets
/// the format decide whether a declared schema is required, forbidden, or
/// optional.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `field` is present but is neither
/// one field nor a list of them, or holds an entry [`FieldSpec`] rejects.
pub fn parse_optional_fields(
    config: &ConfigValue,
    connector: &str,
) -> SaciResult<Option<Vec<FieldSpec>>> {
    let Some(value) = config.get(FIELD_KEY) else {
        return Ok(None);
    };

    // One field is written as one node, so the value is a table rather than a
    // list until a second node collapses the pair into one (see
    // `crate::config::one_or_many`, the serde-side twin of this tolerance).
    let entries = match value {
        ConfigValue::Array(entries) => entries.as_slice(),
        ConfigValue::Object(_) => std::slice::from_ref(value),
        _ => {
            return Err(SaciError::configuration(format!(
                "{connector} config.field must be a list of fields"
            )));
        }
    };

    entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            FieldSpec::deserialize(entry)
                .map_err(|e| SaciError::configuration(format!("{connector} field[{i}]: {e}")))
        })
        .collect::<SaciResult<Vec<_>>>()
        .map(Some)
}

/// The Arrow schema `fields` declare.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] from [`FieldSpec::to_arrow`] for the
/// first field it rejects.
pub fn schema_of(fields: &[FieldSpec]) -> SaciResult<Arc<Schema>> {
    let arrow = fields
        .iter()
        .map(FieldSpec::to_arrow)
        .collect::<SaciResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(arrow)))
}

/// Where the Arrow schema a source hands its codec comes from.
///
/// The `schema_from` key of the `http` and `s3` sources.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaFrom {
    /// The `field` entries are handed to the codec, including nothing when the
    /// config declared none: what `csv` needs, and what `parquet`, `avro` and
    /// `arrow-ipc` project the payload onto.
    #[default]
    Config,
    /// The codec reads its own schema out of the payload (the response body, or
    /// the object), and that schema must then equal the `field` entries field
    /// for field. What a source wants when the payload's own schema is what it
    /// reads, `parquet` and `avro` included.
    Payload,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(text: &str) -> ConfigValue {
        crate::config::from_kdl_str(text).expect("parse")
    }

    fn empty() -> ConfigValue {
        ConfigValue::Object(crate::config::ConfigMap::new())
    }

    fn built(text: &str) -> Arc<Schema> {
        schema_of(&parse_fields(&value(text), "TestFactory").expect("fields parse"))
            .expect("schema built")
    }

    #[test]
    fn a_field_table_parses_to_the_declared_arrow_schema() {
        let schema = built(
            r#"
field "id" type="int64" nullable=#false
field "label" type="utf8" nullable=#false
"#,
        );

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "label");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(!schema.field(1).is_nullable());
    }

    #[test]
    fn an_omitted_nullable_defaults_to_true() {
        let schema = built("field \"total\" type=\"float64\"\n");

        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn a_mixed_case_type_is_a_configuration_error_naming_it() {
        let err = parse_fields(&value("field \"v\" type=\"Int64\"\n"), "TestFactory")
            .expect_err("a mixed-case type must be rejected");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("Int64"), "{}", err.message());
        assert!(err.message().contains("TestFactory"), "{}", err.message());
    }

    #[test]
    fn the_old_type_aliases_are_rejected() {
        for alias in [
            "bool",
            "float",
            "double",
            "string",
            "varchar",
            "largestring",
        ] {
            let text = format!("field \"v\" type=\"{alias}\"\n");
            let err =
                parse_fields(&value(&text), "TestFactory").expect_err("an alias must be rejected");
            assert!(err.message().contains(alias), "{}", err.message());
        }
    }

    #[test]
    fn an_unknown_type_string_is_a_configuration_error_naming_it() {
        let err = parse_fields(&value("field \"v\" type=\"int128\"\n"), "TestFactory")
            .expect_err("an unknown type must be rejected");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("int128"), "{}", err.message());
    }

    #[test]
    fn an_unknown_per_field_key_is_a_configuration_error_naming_it() {
        let err = parse_fields(
            &value("field \"v\" type=\"int32\" nullible=#false\n"),
            "TestFactory",
        )
        .expect_err("an unknown key must be rejected");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("nullible"), "{}", err.message());
    }

    #[test]
    fn a_field_without_a_type_or_an_id_is_a_configuration_error() {
        let err = parse_fields(&value("field \"v\"\n"), "TestFactory")
            .expect_err("a bare name is not a field");
        assert_eq!(err.category(), "configuration");

        let err = parse_fields(&value("field type=\"int32\"\n"), "TestFactory")
            .expect_err("a field needs an id");
        assert!(err.message().contains("id"), "{}", err.message());

        let err = parse_fields(
            &value("field \"v\" nullable=#true\nfield \"w\" type=\"int32\"\n"),
            "TestFactory",
        )
        .expect_err("a field needs a type");
        assert!(err.message().contains("type"), "{}", err.message());
    }

    #[test]
    fn a_missing_field_key_is_a_configuration_error() {
        let err =
            parse_fields(&empty(), "TestFactory").expect_err("a missing field must be rejected");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'field'"), "{}", err.message());
    }

    #[test]
    fn an_absent_field_key_is_none_for_the_optional_parser() {
        let parsed =
            parse_optional_fields(&empty(), "TestFactory").expect("an absent key is not an error");
        assert!(parsed.is_none());
    }

    #[test]
    fn a_present_field_key_parses_the_same_for_both_parsers() {
        let config = value("field \"v\" type=\"int32\"\n");
        let optional = parse_optional_fields(&config, "TestFactory")
            .expect("parsed")
            .expect("present");
        let required = parse_fields(&config, "TestFactory").expect("parsed");
        assert_eq!(optional, required);
    }

    #[test]
    fn one_field_node_and_a_list_of_one_parse_the_same() {
        let single = built("field \"v\" type=\"int32\"\n");
        let listed = built("field \"v\" type=\"int32\"\nfield \"w\" type=\"int32\"\n");
        assert_eq!(single.fields().len(), 1);
        assert_eq!(listed.fields().len(), 2);
    }

    #[test]
    fn a_scalar_field_key_is_a_configuration_error() {
        let err = parse_fields(&value("field \"id\"\n"), "TestFactory")
            .expect_err("a bare scalar is not a field");
        assert_eq!(
            err.message(),
            "TestFactory config.field must be a list of fields"
        );
    }

    #[test]
    fn the_legacy_schema_fields_key_is_not_read() {
        let err = parse_fields(
            &value("schema_fields \"v\" type=\"int32\"\n"),
            "TestFactory",
        )
        .expect_err("the old key is gone");
        assert_eq!(err.category(), "configuration");
    }

    #[test]
    fn without_fields_drops_only_the_field_entry() {
        let config = value("path \"a\"\nfield \"v\" type=\"int32\"\n");
        let table = without_fields(&config);
        assert!(table.get("field").is_none());
        assert!(table.get("path").is_some());
        assert!(config.get("field").is_some(), "the original is untouched");
    }

    #[test]
    fn every_type_maps_to_its_arrow_type() {
        let cases: [(&str, DataType); 21] = [
            ("boolean", DataType::Boolean),
            ("int8", DataType::Int8),
            ("int16", DataType::Int16),
            ("int32", DataType::Int32),
            ("int64", DataType::Int64),
            ("uint8", DataType::UInt8),
            ("uint16", DataType::UInt16),
            ("uint32", DataType::UInt32),
            ("uint64", DataType::UInt64),
            ("float32", DataType::Float32),
            ("float64", DataType::Float64),
            ("utf8", DataType::Utf8),
            ("largeutf8", DataType::LargeUtf8),
            ("json", DataType::Utf8),
            ("binary", DataType::Binary),
            ("date32", DataType::Date32),
            ("date64", DataType::Date64),
            ("time64_micros", DataType::Time64(TimeUnit::Microsecond)),
            (
                "timestamp_micros",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "timestamp_micros_utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                "timestamp_millis",
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
        ];
        for (name, expected) in cases {
            let schema = built(&format!("field \"v\" type=\"{name}\"\n"));
            assert_eq!(schema.field(0).data_type(), &expected, "type {name}");
        }

        let schema =
            built("field \"u\" type=\"uuid\"\nfield \"i\" type=\"interval_month_day_nano\"\n");
        assert_eq!(schema.field(0).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn a_decimal_needs_a_legal_precision_and_scale() {
        let schema = built("field \"d\" type=\"decimal128\" precision=12 scale=2\n");
        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(12, 2));

        for (text, needle) in [
            ("field \"d\" type=\"decimal128\" scale=2\n", "'precision'"),
            ("field \"d\" type=\"decimal128\" precision=12\n", "'scale'"),
            (
                "field \"d\" type=\"decimal128\" precision=39 scale=0\n",
                "1..=38",
            ),
            (
                "field \"d\" type=\"decimal128\" precision=5 scale=6\n",
                "0..=precision",
            ),
            (
                "field \"d\" type=\"int64\" precision=5 scale=2\n",
                "decimal128",
            ),
        ] {
            let fields = parse_fields(&value(text), "T").expect("entry parses");
            let err = schema_of(&fields).expect_err(text);
            assert!(err.message().contains(needle), "{text}: {}", err.message());
        }
    }

    #[test]
    fn a_list_takes_a_scalar_item() {
        let schema = built("field \"l\" type=\"list\" item=\"int32\"\n");
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
        );

        let schema = built("field \"l\" type=\"list\" item=\"decimal128\" precision=10 scale=2\n");
        assert!(matches!(schema.field(0).data_type(), DataType::List(_)));

        for (text, needle) in [
            ("field \"l\" type=\"list\"\n", "requires 'item'"),
            (
                "field \"l\" type=\"list\" item=\"list\"\n",
                "one-dimensional",
            ),
            ("field \"l\" type=\"int32\" item=\"int32\"\n", "list"),
        ] {
            let fields = parse_fields(&value(text), "T").expect("entry parses");
            let err = schema_of(&fields).expect_err(text);
            assert!(err.message().contains(needle), "{text}: {}", err.message());
        }
    }

    #[test]
    fn native_type_is_carried_and_never_changes_the_arrow_type() {
        let fields = parse_fields(
            &value("field \"v\" type=\"int64\" native_type=\"bigint\"\n"),
            "T",
        )
        .expect("parses");
        assert_eq!(fields[0].native_type.as_deref(), Some("bigint"));
        assert_eq!(
            fields[0].to_arrow().expect("arrow").data_type(),
            &DataType::Int64
        );
    }
}
