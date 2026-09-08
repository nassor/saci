//! The `schema_fields` config parser shared by the file and channel connectors.
//!
//! A connector whose format carries no schema of its own declares one as
//! `schema_fields` nodes carrying `{name, type, nullable}`.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use saci_config::ConfigValue;
use saci_core::error::SaciError;

/// Build an Arrow schema from the `schema_fields` entries of a `config` table.
///
/// `factory_name` prefixes every error message, so a bad entry names the
/// factory that rejected it. `nullable` defaults to `true` when omitted.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `schema_fields` is missing, is
/// neither one field nor a list of them, or holds an entry without a `name`,
/// without a `type`, or with a `type` string that is not a supported Arrow
/// type.
pub fn parse_schema_fields(
    config: &ConfigValue,
    factory_name: &str,
) -> Result<Arc<Schema>, SaciError> {
    parse_optional_schema_fields(config, factory_name)?.ok_or_else(|| {
        SaciError::configuration(format!(
            "{factory_name} config requires a 'schema_fields' list"
        ))
    })
}

/// The declared `schema_fields`, or `None` when the key is absent.
///
/// A connector whose byte format may carry its own schema uses this and lets
/// the format decide whether a declared schema is required, forbidden, or
/// optional.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `schema_fields` is present but is
/// neither one field nor a list of them, or holds an entry without a `name`,
/// without a `type`, or with a `type` string that is not a supported Arrow
/// type.
pub fn parse_optional_schema_fields(
    config: &ConfigValue,
    factory_name: &str,
) -> Result<Option<Arc<Schema>>, SaciError> {
    let Some(fields_val) = config.get("schema_fields") else {
        return Ok(None);
    };

    // One field is written as one node, so the value is a table rather than a
    // list until a second node collapses the pair into one (see
    // `saci_config::one_or_many`, the serde-side twin of this tolerance).
    let fields_seq = match fields_val {
        ConfigValue::Array(entries) => entries.as_slice(),
        ConfigValue::Object(_) => std::slice::from_ref(fields_val),
        _ => {
            return Err(SaciError::configuration(format!(
                "{factory_name} config.schema_fields must be a list of fields"
            )));
        }
    };

    let mut fields = Vec::with_capacity(fields_seq.len());
    for (i, entry) in fields_seq.iter().enumerate() {
        let name = entry.get("id").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration(format!(
                "{factory_name} schema_fields[{i}] missing required 'id'"
            ))
        })?;

        let type_str = entry.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration(format!(
                "{factory_name} schema_fields[{i}] ('{name}') missing required 'type'"
            ))
        })?;

        let nullable = entry
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let data_type = parse_data_type(type_str).ok_or_else(|| {
            SaciError::configuration(format!(
                "{factory_name} schema_fields '{name}' has unknown Arrow type '{type_str}'"
            ))
        })?;

        fields.push(Field::new(name, data_type, nullable));
    }

    Ok(Some(Arc::new(Schema::new(fields))))
}

fn parse_data_type(s: &str) -> Option<DataType> {
    match s.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => Some(DataType::Boolean),
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" => Some(DataType::Int32),
        "int64" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float32" | "float" => Some(DataType::Float32),
        "float64" | "double" => Some(DataType::Float64),
        "utf8" | "string" | "varchar" => Some(DataType::Utf8),
        "largeutf8" | "largestring" => Some(DataType::LargeUtf8),
        "binary" => Some(DataType::Binary),
        "date32" => Some(DataType::Date32),
        "date64" => Some(DataType::Date64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(text: &str) -> ConfigValue {
        saci_config::from_kdl_str(text).expect("parse")
    }

    fn empty() -> ConfigValue {
        ConfigValue::Object(saci_config::ConfigMap::new())
    }

    #[test]
    fn a_field_table_parses_to_the_declared_arrow_schema() {
        let schema = parse_schema_fields(
            &value(
                r#"
schema_fields "id" type="Int64" nullable=#false
schema_fields "label" type="string" nullable=#false
"#,
            ),
            "TestFactory",
        )
        .expect("schema built");

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "label");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(!schema.field(1).is_nullable());
    }

    #[test]
    fn an_omitted_nullable_defaults_to_true() {
        let schema = parse_schema_fields(
            &value("schema_fields \"total\" type=\"float64\"\n"),
            "TestFactory",
        )
        .expect("schema built");

        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn an_unknown_type_string_is_a_configuration_error_naming_it() {
        let err = parse_schema_fields(
            &value("schema_fields \"v\" type=\"decimal128\"\n"),
            "TestFactory",
        )
        .expect_err("an unknown type must be rejected");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("decimal128"), "{}", err.message());
    }

    #[test]
    fn a_missing_schema_fields_key_is_a_configuration_error() {
        let err = parse_schema_fields(&empty(), "TestFactory")
            .expect_err("a missing schema_fields must be rejected");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "{}", err.message());
    }

    #[test]
    fn an_absent_schema_fields_key_is_none_for_the_optional_parser() {
        let parsed = parse_optional_schema_fields(&empty(), "TestFactory")
            .expect("an absent key is not an error");
        assert!(parsed.is_none());
    }

    #[test]
    fn a_present_schema_fields_key_parses_the_same_for_both_parsers() {
        let config = value("schema_fields \"v\" type=\"int32\"\n");
        let optional = parse_optional_schema_fields(&config, "TestFactory")
            .expect("parsed")
            .expect("present");
        let required = parse_schema_fields(&config, "TestFactory").expect("parsed");
        assert_eq!(optional, required);
    }

    #[test]
    fn one_field_node_and_a_list_of_one_parse_the_same() {
        let single = parse_schema_fields(&value("schema_fields \"v\" type=\"int32\"\n"), "T")
            .expect("one node");
        let listed = parse_schema_fields(
            &value("schema_fields \"v\" type=\"int32\"\nschema_fields \"w\" type=\"int32\"\n"),
            "T",
        )
        .expect("two nodes");
        assert_eq!(single.fields().len(), 1);
        assert_eq!(listed.fields().len(), 2);
    }

    #[test]
    fn a_scalar_schema_fields_key_is_a_configuration_error() {
        let err = parse_schema_fields(&value("schema_fields \"id\"\n"), "TestFactory")
            .expect_err("a bare scalar is not a field");
        assert_eq!(
            err.message(),
            "TestFactory config.schema_fields must be a list of fields"
        );
    }
}
