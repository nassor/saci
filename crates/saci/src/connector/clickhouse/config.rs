//! Serde-derived configuration for the ClickHouse source and sink.
//!
//! Every struct carries `#[serde(deny_unknown_fields)]`: an unexpected key
//! is a configuration error rather than silently dropped.
//!
//! [`ClickhouseSourceConfig::validate`] and [`ClickhouseSinkConfig::validate`]
//! enforce cross-field invariants before any connection is opened.

use std::collections::{HashMap, HashSet};

use crate::connector::schema::{FieldSpec, FieldType};
use crate::error::SaciError;
use serde::Deserialize;

fn default_batch_rows() -> usize {
    8_192
}

fn default_chunk_rows() -> usize {
    65_536
}

fn default_query_timeout_ms() -> u64 {
    30_000
}

fn default_database() -> String {
    "default".to_string()
}

/// Compression format for the ClickHouse HTTP client.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionConfig {
    /// LZ4 compression (default in ClickHouse client).
    #[default]
    Lz4,
    /// No compression.
    None,
}

/// Connection settings for ClickHouse.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// ClickHouse HTTP endpoint URL (e.g. `http://localhost:8123`).
    pub url: String,
    /// Optional username for authentication.
    #[serde(default)]
    pub user: Option<String>,
    /// Optional password for authentication.
    #[serde(default)]
    pub password: Option<String>,
    /// Target database name.
    #[serde(default = "default_database")]
    pub database: String,
    /// Query execution timeout in milliseconds.
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    /// Compression format for queries and inserts.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Extra server-side settings passed with every query.
    #[serde(default)]
    pub settings: HashMap<String, String>,
}

impl ConnectionConfig {
    /// Validates connection settings.
    pub fn validate(&self, what: &str) -> Result<(), SaciError> {
        let url = self.url.trim();
        if url.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.url must not be empty"
            )));
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(SaciError::configuration(format!(
                "{what}: connection.url must start with 'http://' or 'https://', got '{url}'"
            )));
        }
        if self.database.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.database must not be empty"
            )));
        }
        Ok(())
    }
}

/// Whether the connector maps a declared type.
fn supported(ty: FieldType) -> bool {
    matches!(
        ty,
        FieldType::Boolean
            | FieldType::Int8
            | FieldType::Int16
            | FieldType::Int32
            | FieldType::Int64
            | FieldType::UInt8
            | FieldType::UInt16
            | FieldType::UInt32
            | FieldType::UInt64
            | FieldType::Float32
            | FieldType::Float64
            | FieldType::Utf8
            | FieldType::Binary
            | FieldType::Date32
            | FieldType::Date64
            | FieldType::TimestampMicros
            | FieldType::TimestampMillis
            | FieldType::Decimal128
    )
}

/// Whether a column of this type is eligible as a polling cursor column.
pub(crate) fn is_cursor_capable(ty: FieldType) -> bool {
    matches!(
        ty,
        FieldType::Int8
            | FieldType::Int16
            | FieldType::Int32
            | FieldType::Int64
            | FieldType::UInt8
            | FieldType::UInt16
            | FieldType::UInt32
            | FieldType::UInt64
            | FieldType::Float32
            | FieldType::Float64
            | FieldType::Utf8
            | FieldType::Date32
            | FieldType::Date64
            | FieldType::TimestampMicros
            | FieldType::TimestampMillis
    )
}

/// Validates a declared field list: non-empty, every `id` non-empty and unique,
/// every type one this connector maps, and every Arrow mapping legal.
fn validate_fields(what: &str, fields: &[FieldSpec]) -> Result<(), SaciError> {
    if fields.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: at least one 'field' entry is required"
        )));
    }

    let mut seen = HashSet::new();
    for (i, field) in fields.iter().enumerate() {
        if field.name.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: field[{i}] has an empty 'id'"
            )));
        }
        if !seen.insert(&field.name) {
            return Err(SaciError::configuration(format!(
                "{what}: field '{}' is declared more than once",
                field.name
            )));
        }
        if !supported(field.ty) {
            return Err(SaciError::configuration(format!(
                "clickhouse: field '{}' has type '{}', which this connector does not map",
                field.name, field.ty
            )));
        }
        if field.native_type.is_some() {
            return Err(SaciError::configuration(format!(
                "clickhouse: field '{}' sets 'native_type', which this connector does not use",
                field.name
            )));
        }
        field.to_arrow()?;
    }
    Ok(())
}

/// Incremental polling configuration.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PollingMode {
    /// Target table name.
    pub table: String,
    /// Column name used as cursor for incremental polling.
    pub cursor_column: String,
    /// Optional starting cursor value.
    #[serde(default)]
    pub initial_cursor: Option<String>,
    /// Optional secondary column for stable ordering. Once a batch has fixed a
    /// position inside a repeated cursor value, polling resumes from its last
    /// row's tiebreak value rather than from the cursor alone.
    #[serde(default)]
    pub tiebreak_column: Option<String>,
}

/// Full table dump mode configuration.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DumpMode {
    /// Target table name.
    pub table: String,
}

/// Custom SQL query mode configuration.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryMode {
    /// SQL SELECT query to execute.
    pub query: String,
}

/// Source read strategy.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceMode {
    /// Incremental cursor-based polling.
    Polling(PollingMode),
    /// Paginated full table dump.
    Dump(DumpMode),
    /// Arbitrary SELECT query.
    Query(QueryMode),
}

impl SourceMode {
    /// Descriptive label for metrics and logging.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Polling(_) => "polling",
            Self::Dump(_) => "dump",
            Self::Query(_) => "query",
        }
    }
}

/// Configuration for [`ClickhouseSource`](crate::connector::clickhouse::source::ClickhouseSource).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseSourceConfig {
    /// Stable name for this source instance.
    pub name: String,
    /// Connection configuration.
    pub connection: ConnectionConfig,
    /// Read mode.
    pub mode: SourceMode,
    /// Declared Arrow schema fields.
    #[serde(rename = "field", deserialize_with = "crate::config::one_or_many")]
    pub fields: Vec<FieldSpec>,
    /// Rows per emitted batch.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    /// Maximum batches per drain cycle (0 = unconstrained).
    #[serde(default)]
    pub max_batches_per_cycle: usize,
}

impl ClickhouseSourceConfig {
    /// Validates all configuration invariants.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = format!("source '{}'", self.name);
        if self.name.trim().is_empty() {
            return Err(SaciError::configuration("source name must not be empty"));
        }
        self.connection.validate(&what)?;

        validate_fields(&what, &self.fields)?;

        match &self.mode {
            SourceMode::Polling(poll) => {
                if poll.table.trim().is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.table must not be empty"
                    )));
                }
                let cursor_field = self
                    .fields
                    .iter()
                    .find(|f| f.name == poll.cursor_column)
                    .ok_or_else(|| {
                        SaciError::configuration(format!(
                            "{what}: mode.cursor_column '{}' does not name a declared 'field' entry",
                            poll.cursor_column
                        ))
                    })?;
                if !is_cursor_capable(cursor_field.ty) {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.cursor_column '{}' of type '{}' is not cursor capable",
                        poll.cursor_column,
                        cursor_field.ty.as_str()
                    )));
                }
                if let Some(tie) = &poll.tiebreak_column
                    && !self.fields.iter().any(|f| f.name == *tie)
                {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.tiebreak_column '{tie}' does not name a declared 'field' entry"
                    )));
                }
            }
            SourceMode::Dump(dump) => {
                if dump.table.trim().is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.table must not be empty"
                    )));
                }
            }
            SourceMode::Query(q) => {
                let query = q.query.trim();
                if query.is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.query must not be empty"
                    )));
                }
                if query.to_ascii_uppercase().contains("FORMAT ") {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.query must not specify 'FORMAT' (ArrowStream is appended automatically)"
                    )));
                }
            }
        }

        if self.batch_rows == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: batch_rows must be greater than 0"
            )));
        }

        Ok(())
    }
}

/// Write mode for [`ClickhouseSink`](crate::connector::clickhouse::sink::ClickhouseSink).
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Append batches directly into target table.
    #[default]
    Append,
}

/// Configuration for [`ClickhouseSink`](crate::connector::clickhouse::sink::ClickhouseSink).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseSinkConfig {
    /// Stable name for this sink instance.
    pub name: String,
    /// Connection configuration.
    pub connection: ConnectionConfig,
    /// Destination table name.
    pub table: String,
    /// Declared Arrow schema fields.
    #[serde(rename = "field", deserialize_with = "crate::config::one_or_many")]
    pub fields: Vec<FieldSpec>,
    /// Write mode (defaults to `append`).
    #[serde(default)]
    pub write_mode: WriteMode,
    /// Maximum rows per chunk (0 = no limit).
    #[serde(default = "default_chunk_rows")]
    pub chunk_rows: usize,
    /// Rows to buffer before sending an insert request (0 = flush every write).
    #[serde(default)]
    pub flush_rows: usize,
    /// Runs `TRUNCATE TABLE` at the start of the first flush, and again on a
    /// retry until a flush completes. The truncate is not transactional.
    #[serde(default)]
    pub truncate_before_first_write: bool,
}

impl ClickhouseSinkConfig {
    /// Validates all sink configuration invariants.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = format!("sink '{}'", self.name);
        if self.name.trim().is_empty() {
            return Err(SaciError::configuration("sink name must not be empty"));
        }
        self.connection.validate(&what)?;

        if self.table.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: table must not be empty"
            )));
        }

        validate_fields(&what, &self.fields)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    fn spec(name: &str, ty: FieldType, nullable: bool) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            ty,
            nullable,
            precision: None,
            scale: None,
            item: None,
            native_type: None,
        }
    }

    fn connection(compression: CompressionConfig) -> ConnectionConfig {
        ConnectionConfig {
            url: "http://localhost:8123".to_string(),
            user: None,
            password: None,
            database: "default".to_string(),
            query_timeout_ms: 30000,
            compression,
            settings: HashMap::new(),
        }
    }

    #[test]
    fn all_field_types_convert_to_arrow_fields() {
        let types = [
            (FieldType::Boolean, DataType::Boolean),
            (FieldType::Int8, DataType::Int8),
            (FieldType::Int16, DataType::Int16),
            (FieldType::Int32, DataType::Int32),
            (FieldType::Int64, DataType::Int64),
            (FieldType::UInt8, DataType::UInt8),
            (FieldType::UInt16, DataType::UInt16),
            (FieldType::UInt32, DataType::UInt32),
            (FieldType::UInt64, DataType::UInt64),
            (FieldType::Float32, DataType::Float32),
            (FieldType::Float64, DataType::Float64),
            (FieldType::Utf8, DataType::Utf8),
            (FieldType::Binary, DataType::Binary),
            (FieldType::Date32, DataType::Date32),
            (FieldType::Date64, DataType::Date64),
            (
                FieldType::TimestampMicros,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                FieldType::TimestampMillis,
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
        ];

        for (ty, expected_dt) in types {
            assert!(supported(ty));
            let field = spec("f", ty, false).to_arrow().expect("converts to arrow");
            assert_eq!(field.data_type(), &expected_dt);
            assert!(!field.is_nullable());
        }
    }

    #[test]
    fn decimal128_validates_precision_and_scale() {
        let valid = FieldSpec {
            precision: Some(18),
            scale: Some(4),
            ..spec("amount", FieldType::Decimal128, true)
        };
        let field = valid.to_arrow().expect("valid decimal");
        assert_eq!(field.data_type(), &DataType::Decimal128(18, 4));

        let missing_prec = FieldSpec {
            precision: None,
            ..valid.clone()
        };
        assert!(missing_prec.to_arrow().is_err());

        let missing_scale = FieldSpec {
            scale: None,
            ..valid.clone()
        };
        assert!(missing_scale.to_arrow().is_err());

        let out_of_range_prec = FieldSpec {
            precision: Some(39),
            ..valid.clone()
        };
        assert!(out_of_range_prec.to_arrow().is_err());

        let scale_larger_than_prec = FieldSpec {
            scale: Some(19),
            ..valid.clone()
        };
        assert!(scale_larger_than_prec.to_arrow().is_err());

        let non_decimal_with_prec = FieldSpec {
            precision: Some(10),
            ..spec("n", FieldType::Int64, false)
        };
        assert!(non_decimal_with_prec.to_arrow().is_err());
    }

    #[test]
    fn connection_config_requires_valid_url() {
        let valid = ConnectionConfig {
            url: "http://127.0.0.1:8123".to_string(),
            ..connection(CompressionConfig::Lz4)
        };
        assert!(valid.validate("test").is_ok());

        let empty_url = ConnectionConfig {
            url: "   ".to_string(),
            ..valid.clone()
        };
        assert!(empty_url.validate("test").is_err());

        let bad_scheme = ConnectionConfig {
            url: "ftp://127.0.0.1".to_string(),
            ..valid.clone()
        };
        assert!(bad_scheme.validate("test").is_err());

        let empty_db = ConnectionConfig {
            database: "  ".to_string(),
            ..valid
        };
        assert!(empty_db.validate("test").is_err());
    }

    #[test]
    fn source_config_validates_polling_cursor_column() {
        let valid = ClickhouseSourceConfig {
            name: "src".to_string(),
            connection: connection(CompressionConfig::None),
            mode: SourceMode::Polling(PollingMode {
                table: "events".to_string(),
                cursor_column: "ts".to_string(),
                initial_cursor: None,
                tiebreak_column: None,
            }),
            fields: vec![
                spec("ts", FieldType::TimestampMicros, false),
                spec("data", FieldType::Binary, true),
            ],
            batch_rows: 100,
            max_batches_per_cycle: 0,
        };
        assert!(valid.validate().is_ok());

        // Binary is not cursor capable
        let mut non_cursor = valid.clone();
        non_cursor.mode = SourceMode::Polling(PollingMode {
            table: "events".to_string(),
            cursor_column: "data".to_string(),
            initial_cursor: None,
            tiebreak_column: None,
        });
        assert!(non_cursor.validate().is_err());

        // Unknown column
        let mut unknown_col = valid.clone();
        unknown_col.mode = SourceMode::Polling(PollingMode {
            table: "events".to_string(),
            cursor_column: "missing".to_string(),
            initial_cursor: None,
            tiebreak_column: None,
        });
        let message = unknown_col
            .validate()
            .expect_err("unknown cursor")
            .message();
        assert!(
            message.contains("does not name a declared 'field' entry"),
            "{message}"
        );
    }

    #[test]
    fn source_config_validation_failures() {
        let base = ClickhouseSourceConfig {
            name: "src".to_string(),
            connection: connection(CompressionConfig::None),
            mode: SourceMode::Dump(DumpMode {
                table: "events".to_string(),
            }),
            fields: vec![spec("id", FieldType::Int64, false)],
            batch_rows: 100,
            max_batches_per_cycle: 0,
        };

        // Empty name
        let mut c = base.clone();
        c.name = "   ".to_string();
        assert!(c.validate().is_err());

        // Empty field list
        let mut c = base.clone();
        c.fields = vec![];
        let message = c.validate().expect_err("no fields").message();
        assert!(
            message.contains("at least one 'field' entry is required"),
            "{message}"
        );

        // Empty field name
        let mut c = base.clone();
        c.fields = vec![spec("  ", FieldType::Int64, false)];
        let message = c.validate().expect_err("empty id").message();
        assert!(message.contains("field[0] has an empty 'id'"), "{message}");

        // Duplicate field name
        let mut c = base.clone();
        c.fields = vec![
            spec("id", FieldType::Int64, false),
            spec("id", FieldType::Utf8, true),
        ];
        let message = c.validate().expect_err("duplicate id").message();
        assert!(
            message.contains("field 'id' is declared more than once"),
            "{message}"
        );

        // A type the connector does not map
        let mut c = base.clone();
        c.fields = vec![spec("tags", FieldType::List, true)];
        let message = c.validate().expect_err("unmapped type").message();
        assert!(message.contains("tags"), "{message}");
        assert!(
            message.contains("which this connector does not map"),
            "{message}"
        );

        // batch_rows = 0
        let mut c = base.clone();
        c.batch_rows = 0;
        assert!(c.validate().is_err());

        // Dump with empty table
        let mut c = base.clone();
        c.mode = SourceMode::Dump(DumpMode {
            table: "".to_string(),
        });
        assert!(c.validate().is_err());

        // Query with empty query
        let mut c = base.clone();
        c.mode = SourceMode::Query(QueryMode {
            query: "".to_string(),
        });
        assert!(c.validate().is_err());

        // Query with explicit FORMAT
        let mut c = base.clone();
        c.mode = SourceMode::Query(QueryMode {
            query: "SELECT id FROM events FORMAT JSON".to_string(),
        });
        assert!(c.validate().is_err());

        // Polling with empty table
        let mut c = base.clone();
        c.mode = SourceMode::Polling(PollingMode {
            table: " ".to_string(),
            cursor_column: "id".to_string(),
            initial_cursor: None,
            tiebreak_column: None,
        });
        assert!(c.validate().is_err());

        // Polling with tiebreak_column not declared as a field
        let mut c = base.clone();
        c.mode = SourceMode::Polling(PollingMode {
            table: "events".to_string(),
            cursor_column: "id".to_string(),
            initial_cursor: None,
            tiebreak_column: Some("missing_tie".to_string()),
        });
        let message = c.validate().expect_err("unknown tiebreak").message();
        assert!(
            message.contains("does not name a declared 'field' entry"),
            "{message}"
        );

        // Polling with tiebreak_column declared as a field
        let mut c = base.clone();
        c.fields.push(spec("seq", FieldType::UInt64, false));
        c.mode = SourceMode::Polling(PollingMode {
            table: "events".to_string(),
            cursor_column: "id".to_string(),
            initial_cursor: None,
            tiebreak_column: Some("seq".to_string()),
        });
        assert!(c.validate().is_ok());
    }

    #[test]
    fn sink_config_validation() {
        let valid = ClickhouseSinkConfig {
            name: "sink".to_string(),
            connection: connection(CompressionConfig::Lz4),
            table: "events".to_string(),
            fields: vec![spec("id", FieldType::Int64, false)],
            write_mode: WriteMode::Append,
            chunk_rows: 1000,
            flush_rows: 10,
            truncate_before_first_write: false,
        };
        assert!(valid.validate().is_ok());

        let mut c = valid.clone();
        c.name = "".to_string();
        assert!(c.validate().is_err());

        let mut c = valid.clone();
        c.table = "  ".to_string();
        assert!(c.validate().is_err());

        let mut c = valid.clone();
        c.fields = vec![];
        assert!(c.validate().is_err());

        let mut c = valid.clone();
        c.fields = vec![spec("", FieldType::Int64, false)];
        assert!(c.validate().is_err());

        let mut c = valid.clone();
        c.fields = vec![
            spec("col", FieldType::Int64, false),
            spec("col", FieldType::Utf8, false),
        ];
        assert!(c.validate().is_err());

        let mut c = valid.clone();
        c.fields = vec![spec("tags", FieldType::List, true)];
        let message = c.validate().expect_err("unmapped type").message();
        assert!(message.contains("tags"), "{message}");
        assert!(
            message.contains("which this connector does not map"),
            "{message}"
        );

        let mut c = valid.clone();
        c.fields = vec![FieldSpec {
            native_type: Some("Int64".to_string()),
            ..spec("id", FieldType::Int64, false)
        }];
        let message = c.validate().expect_err("native_type").message();
        assert!(
            message.contains("sets 'native_type', which this connector does not use"),
            "{message}"
        );

        let mut c = valid;
        c.connection.url = "bad://url".to_string();
        assert!(c.validate().is_err());
    }

    #[test]
    fn source_mode_labels_and_field_type_properties() {
        assert_eq!(
            SourceMode::Polling(PollingMode {
                table: "t".to_string(),
                cursor_column: "c".to_string(),
                initial_cursor: None,
                tiebreak_column: None,
            })
            .label(),
            "polling"
        );
        assert_eq!(
            SourceMode::Dump(DumpMode {
                table: "t".to_string()
            })
            .label(),
            "dump"
        );
        assert_eq!(
            SourceMode::Query(QueryMode {
                query: "SELECT 1".to_string()
            })
            .label(),
            "query"
        );

        let types = [
            (FieldType::Boolean, "boolean", false),
            (FieldType::Int8, "int8", true),
            (FieldType::Int16, "int16", true),
            (FieldType::Int32, "int32", true),
            (FieldType::Int64, "int64", true),
            (FieldType::UInt8, "uint8", true),
            (FieldType::UInt16, "uint16", true),
            (FieldType::UInt32, "uint32", true),
            (FieldType::UInt64, "uint64", true),
            (FieldType::Float32, "float32", true),
            (FieldType::Float64, "float64", true),
            (FieldType::Utf8, "utf8", true),
            (FieldType::Binary, "binary", false),
            (FieldType::Date32, "date32", true),
            (FieldType::Date64, "date64", true),
            (FieldType::TimestampMicros, "timestamp_micros", true),
            (FieldType::TimestampMillis, "timestamp_millis", true),
            (FieldType::Decimal128, "decimal128", false),
        ];

        for (ty, label, cursor_capable) in types {
            assert_eq!(ty.as_str(), label);
            assert_eq!(is_cursor_capable(ty), cursor_capable);
        }
    }

    #[test]
    fn deny_unknown_fields_on_config_structs() {
        let conn_json = r#"{"url": "http://localhost:8123", "unknown_key": 123}"#;
        assert!(serde_json::from_str::<ConnectionConfig>(conn_json).is_err());

        let field_json = r#"{"id": "col", "type": "int64", "extra": "invalid"}"#;
        assert!(serde_json::from_str::<FieldSpec>(field_json).is_err());

        let poll_json = r#"{"table": "t", "cursor_column": "c", "invalid": 1}"#;
        assert!(serde_json::from_str::<PollingMode>(poll_json).is_err());

        let dump_json = r#"{"table": "t", "invalid": 1}"#;
        assert!(serde_json::from_str::<DumpMode>(dump_json).is_err());

        let query_json = r#"{"query": "SELECT 1", "invalid": 1}"#;
        assert!(serde_json::from_str::<QueryMode>(query_json).is_err());
    }
}
