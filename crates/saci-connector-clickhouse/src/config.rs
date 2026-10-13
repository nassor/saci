//! Serde-derived configuration for the ClickHouse source and sink.
//!
//! Every struct carries `#[serde(deny_unknown_fields)]`: an unexpected key
//! is a configuration error rather than silently dropped.
//!
//! [`ClickhouseSourceConfig::validate`] and [`ClickhouseSinkConfig::validate`]
//! enforce cross-field invariants before any connection is opened.

use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Field, TimeUnit};
use saci_core::error::SaciError;
use serde::Deserialize;

fn default_true() -> bool {
    true
}

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
pub struct ClickhouseConnectionConfig {
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

impl ClickhouseConnectionConfig {
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

/// Supported scalar and Arrow data types for ClickHouse fields.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClickhouseFieldType {
    /// Boolean flag.
    #[serde(alias = "bool")]
    Boolean,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 8-bit integer.
    #[serde(rename = "uint8", alias = "u_int8")]
    UInt8,
    /// Unsigned 16-bit integer.
    #[serde(rename = "uint16", alias = "u_int16")]
    UInt16,
    /// Unsigned 32-bit integer.
    #[serde(rename = "uint32", alias = "u_int32")]
    UInt32,
    /// Unsigned 64-bit integer.
    #[serde(rename = "uint64", alias = "u_int64")]
    UInt64,
    /// Single-precision 32-bit floating point.
    Float32,
    /// Double-precision 64-bit floating point.
    Float64,
    /// UTF-8 encoded string.
    #[serde(alias = "string")]
    Utf8,
    /// Raw binary bytes.
    Binary,
    /// Days elapsed since UNIX epoch.
    Date32,
    /// Milliseconds elapsed since UNIX epoch.
    Date64,
    /// Timestamp with microsecond precision.
    TimestampMicros,
    /// Timestamp with millisecond precision.
    TimestampMillis,
    /// Fixed-point 128-bit decimal with given precision and scale.
    Decimal128,
}

impl ClickhouseFieldType {
    /// Returns the canonical type label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt8 => "uint8",
            Self::UInt16 => "uint16",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Binary => "binary",
            Self::Date32 => "date32",
            Self::Date64 => "date64",
            Self::TimestampMicros => "timestamp_micros",
            Self::TimestampMillis => "timestamp_millis",
            Self::Decimal128 => "decimal128",
        }
    }

    /// Whether this type is eligible as a polling cursor column.
    pub fn is_cursor_capable(self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
                | Self::Float32
                | Self::Float64
                | Self::Utf8
                | Self::Date32
                | Self::Date64
                | Self::TimestampMicros
                | Self::TimestampMillis
        )
    }
}

/// One declared column in the schema.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    /// Column identifier.
    #[serde(rename = "id")]
    pub name: String,
    /// Target Arrow data type.
    #[serde(rename = "type")]
    pub ty: ClickhouseFieldType,
    /// Whether the field admits NULL values.
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// Total digits for `decimal128`.
    #[serde(default)]
    pub precision: Option<u8>,
    /// Fractional digits for `decimal128`.
    #[serde(default)]
    pub scale: Option<i8>,
}

impl FieldSpec {
    /// Converts this specification into an [`arrow_schema::Field`].
    pub fn to_arrow_field(&self) -> Result<Field, SaciError> {
        let dt = match self.ty {
            ClickhouseFieldType::Boolean => DataType::Boolean,
            ClickhouseFieldType::Int8 => DataType::Int8,
            ClickhouseFieldType::Int16 => DataType::Int16,
            ClickhouseFieldType::Int32 => DataType::Int32,
            ClickhouseFieldType::Int64 => DataType::Int64,
            ClickhouseFieldType::UInt8 => DataType::UInt8,
            ClickhouseFieldType::UInt16 => DataType::UInt16,
            ClickhouseFieldType::UInt32 => DataType::UInt32,
            ClickhouseFieldType::UInt64 => DataType::UInt64,
            ClickhouseFieldType::Float32 => DataType::Float32,
            ClickhouseFieldType::Float64 => DataType::Float64,
            ClickhouseFieldType::Utf8 => DataType::Utf8,
            ClickhouseFieldType::Binary => DataType::Binary,
            ClickhouseFieldType::Date32 => DataType::Date32,
            ClickhouseFieldType::Date64 => DataType::Date64,
            ClickhouseFieldType::TimestampMicros => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            ClickhouseFieldType::TimestampMillis => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            ClickhouseFieldType::Decimal128 => {
                let precision = self.precision.ok_or_else(|| {
                    SaciError::configuration(format!(
                        "field '{}': decimal128 requires precision",
                        self.name
                    ))
                })?;
                let scale = self.scale.ok_or_else(|| {
                    SaciError::configuration(format!(
                        "field '{}': decimal128 requires scale",
                        self.name
                    ))
                })?;
                if !(1..=38).contains(&precision) {
                    return Err(SaciError::configuration(format!(
                        "field '{}': precision must be within 1..=38, got {precision}",
                        self.name
                    )));
                }
                if scale < 0 || scale > precision as i8 {
                    return Err(SaciError::configuration(format!(
                        "field '{}': scale must be within 0..={precision}, got {scale}",
                        self.name
                    )));
                }
                DataType::Decimal128(precision, scale)
            }
        };

        if self.ty != ClickhouseFieldType::Decimal128
            && (self.precision.is_some() || self.scale.is_some())
        {
            return Err(SaciError::configuration(format!(
                "field '{}': precision/scale apply to type 'decimal128' only, not '{}'",
                self.name,
                self.ty.as_str()
            )));
        }

        Ok(Field::new(&self.name, dt, self.nullable))
    }
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

/// Configuration for [`ClickhouseSource`](crate::source::ClickhouseSource).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseSourceConfig {
    /// Stable name for this source instance.
    pub name: String,
    /// Connection configuration.
    pub connection: ClickhouseConnectionConfig,
    /// Read mode.
    pub mode: SourceMode,
    /// Declared Arrow schema fields.
    #[serde(deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<FieldSpec>,
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

        if self.schema_fields.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: schema_fields must declare at least one field"
            )));
        }

        let mut seen = HashSet::new();
        for field in &self.schema_fields {
            if field.name.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "{what}: schema_fields id must not be empty"
                )));
            }
            if !seen.insert(&field.name) {
                return Err(SaciError::configuration(format!(
                    "{what}: duplicate schema_fields id '{}'",
                    field.name
                )));
            }
            field.to_arrow_field()?;
        }

        match &self.mode {
            SourceMode::Polling(poll) => {
                if poll.table.trim().is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.table must not be empty"
                    )));
                }
                let cursor_field = self
                    .schema_fields
                    .iter()
                    .find(|f| f.name == poll.cursor_column)
                    .ok_or_else(|| {
                        SaciError::configuration(format!(
                            "{what}: mode.cursor_column '{}' is not declared in schema_fields",
                            poll.cursor_column
                        ))
                    })?;
                if !cursor_field.ty.is_cursor_capable() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.cursor_column '{}' of type '{}' is not cursor capable",
                        poll.cursor_column,
                        cursor_field.ty.as_str()
                    )));
                }
                if let Some(tie) = &poll.tiebreak_column
                    && !self.schema_fields.iter().any(|f| f.name == *tie)
                {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.tiebreak_column '{tie}' is not declared in schema_fields"
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

/// Write mode for [`ClickhouseSink`](crate::sink::ClickhouseSink).
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Append batches directly into target table.
    #[default]
    Append,
}

/// Configuration for [`ClickhouseSink`](crate::sink::ClickhouseSink).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseSinkConfig {
    /// Stable name for this sink instance.
    pub name: String,
    /// Connection configuration.
    pub connection: ClickhouseConnectionConfig,
    /// Destination table name.
    pub table: String,
    /// Declared Arrow schema fields.
    #[serde(deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<FieldSpec>,
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

        if self.schema_fields.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: schema_fields must declare at least one field"
            )));
        }

        let mut seen = HashSet::new();
        for field in &self.schema_fields {
            if field.name.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "{what}: schema_fields id must not be empty"
                )));
            }
            if !seen.insert(&field.name) {
                return Err(SaciError::configuration(format!(
                    "{what}: duplicate schema_fields id '{}'",
                    field.name
                )));
            }
            field.to_arrow_field()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_field_types_convert_to_arrow_fields() {
        let types = [
            (ClickhouseFieldType::Boolean, DataType::Boolean),
            (ClickhouseFieldType::Int8, DataType::Int8),
            (ClickhouseFieldType::Int16, DataType::Int16),
            (ClickhouseFieldType::Int32, DataType::Int32),
            (ClickhouseFieldType::Int64, DataType::Int64),
            (ClickhouseFieldType::UInt8, DataType::UInt8),
            (ClickhouseFieldType::UInt16, DataType::UInt16),
            (ClickhouseFieldType::UInt32, DataType::UInt32),
            (ClickhouseFieldType::UInt64, DataType::UInt64),
            (ClickhouseFieldType::Float32, DataType::Float32),
            (ClickhouseFieldType::Float64, DataType::Float64),
            (ClickhouseFieldType::Utf8, DataType::Utf8),
            (ClickhouseFieldType::Binary, DataType::Binary),
            (ClickhouseFieldType::Date32, DataType::Date32),
            (ClickhouseFieldType::Date64, DataType::Date64),
            (
                ClickhouseFieldType::TimestampMicros,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                ClickhouseFieldType::TimestampMillis,
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
        ];

        for (ty, expected_dt) in types {
            let spec = FieldSpec {
                name: "f".to_string(),
                ty,
                nullable: false,
                precision: None,
                scale: None,
            };
            let field = spec.to_arrow_field().expect("converts to arrow");
            assert_eq!(field.data_type(), &expected_dt);
            assert!(!field.is_nullable());
        }
    }

    #[test]
    fn decimal128_validates_precision_and_scale() {
        let valid = FieldSpec {
            name: "amount".to_string(),
            ty: ClickhouseFieldType::Decimal128,
            nullable: true,
            precision: Some(18),
            scale: Some(4),
        };
        let field = valid.to_arrow_field().expect("valid decimal");
        assert_eq!(field.data_type(), &DataType::Decimal128(18, 4));

        let missing_prec = FieldSpec {
            precision: None,
            ..valid.clone()
        };
        assert!(missing_prec.to_arrow_field().is_err());

        let missing_scale = FieldSpec {
            scale: None,
            ..valid.clone()
        };
        assert!(missing_scale.to_arrow_field().is_err());

        let out_of_range_prec = FieldSpec {
            precision: Some(39),
            ..valid.clone()
        };
        assert!(out_of_range_prec.to_arrow_field().is_err());

        let scale_larger_than_prec = FieldSpec {
            scale: Some(19),
            ..valid.clone()
        };
        assert!(scale_larger_than_prec.to_arrow_field().is_err());

        let non_decimal_with_prec = FieldSpec {
            name: "n".to_string(),
            ty: ClickhouseFieldType::Int64,
            nullable: false,
            precision: Some(10),
            scale: None,
        };
        assert!(non_decimal_with_prec.to_arrow_field().is_err());
    }

    #[test]
    fn connection_config_requires_valid_url() {
        let valid = ClickhouseConnectionConfig {
            url: "http://127.0.0.1:8123".to_string(),
            user: None,
            password: None,
            database: "default".to_string(),
            query_timeout_ms: 30000,
            compression: CompressionConfig::Lz4,
            settings: HashMap::new(),
        };
        assert!(valid.validate("test").is_ok());

        let empty_url = ClickhouseConnectionConfig {
            url: "   ".to_string(),
            ..valid.clone()
        };
        assert!(empty_url.validate("test").is_err());

        let bad_scheme = ClickhouseConnectionConfig {
            url: "ftp://127.0.0.1".to_string(),
            ..valid.clone()
        };
        assert!(bad_scheme.validate("test").is_err());

        let empty_db = ClickhouseConnectionConfig {
            database: "  ".to_string(),
            ..valid
        };
        assert!(empty_db.validate("test").is_err());
    }

    #[test]
    fn source_config_validates_polling_cursor_column() {
        let valid = ClickhouseSourceConfig {
            name: "src".to_string(),
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: CompressionConfig::None,
                settings: HashMap::new(),
            },
            mode: SourceMode::Polling(PollingMode {
                table: "events".to_string(),
                cursor_column: "ts".to_string(),
                initial_cursor: None,
                tiebreak_column: None,
            }),
            schema_fields: vec![
                FieldSpec {
                    name: "ts".to_string(),
                    ty: ClickhouseFieldType::TimestampMicros,
                    nullable: false,
                    precision: None,
                    scale: None,
                },
                FieldSpec {
                    name: "data".to_string(),
                    ty: ClickhouseFieldType::Binary,
                    nullable: true,
                    precision: None,
                    scale: None,
                },
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
        assert!(unknown_col.validate().is_err());
    }

    #[test]
    fn source_config_validation_failures() {
        let base = ClickhouseSourceConfig {
            name: "src".to_string(),
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: CompressionConfig::None,
                settings: HashMap::new(),
            },
            mode: SourceMode::Dump(DumpMode {
                table: "events".to_string(),
            }),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
            batch_rows: 100,
            max_batches_per_cycle: 0,
        };

        // Empty name
        let mut c = base.clone();
        c.name = "   ".to_string();
        assert!(c.validate().is_err());

        // Empty schema_fields
        let mut c = base.clone();
        c.schema_fields = vec![];
        assert!(c.validate().is_err());

        // Empty field name
        let mut c = base.clone();
        c.schema_fields = vec![FieldSpec {
            name: "  ".to_string(),
            ty: ClickhouseFieldType::Int64,
            nullable: false,
            precision: None,
            scale: None,
        }];
        assert!(c.validate().is_err());

        // Duplicate field name
        let mut c = base.clone();
        c.schema_fields = vec![
            FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            },
            FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Utf8,
                nullable: true,
                precision: None,
                scale: None,
            },
        ];
        assert!(c.validate().is_err());

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

        // Polling with tiebreak_column not in schema_fields
        let mut c = base.clone();
        c.mode = SourceMode::Polling(PollingMode {
            table: "events".to_string(),
            cursor_column: "id".to_string(),
            initial_cursor: None,
            tiebreak_column: Some("missing_tie".to_string()),
        });
        assert!(c.validate().is_err());

        // Polling with tiebreak_column present in schema_fields
        let mut c = base.clone();
        c.schema_fields.push(FieldSpec {
            name: "seq".to_string(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        });
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
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: CompressionConfig::Lz4,
                settings: HashMap::new(),
            },
            table: "events".to_string(),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
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
        c.schema_fields = vec![];
        assert!(c.validate().is_err());

        let mut c = valid.clone();
        c.schema_fields = vec![FieldSpec {
            name: "".to_string(),
            ty: ClickhouseFieldType::Int64,
            nullable: false,
            precision: None,
            scale: None,
        }];
        assert!(c.validate().is_err());

        let mut c = valid.clone();
        c.schema_fields = vec![
            FieldSpec {
                name: "col".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            },
            FieldSpec {
                name: "col".to_string(),
                ty: ClickhouseFieldType::Utf8,
                nullable: false,
                precision: None,
                scale: None,
            },
        ];
        assert!(c.validate().is_err());

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
            (ClickhouseFieldType::Boolean, "boolean", false),
            (ClickhouseFieldType::Int8, "int8", true),
            (ClickhouseFieldType::Int16, "int16", true),
            (ClickhouseFieldType::Int32, "int32", true),
            (ClickhouseFieldType::Int64, "int64", true),
            (ClickhouseFieldType::UInt8, "uint8", true),
            (ClickhouseFieldType::UInt16, "uint16", true),
            (ClickhouseFieldType::UInt32, "uint32", true),
            (ClickhouseFieldType::UInt64, "uint64", true),
            (ClickhouseFieldType::Float32, "float32", true),
            (ClickhouseFieldType::Float64, "float64", true),
            (ClickhouseFieldType::Utf8, "utf8", true),
            (ClickhouseFieldType::Binary, "binary", false),
            (ClickhouseFieldType::Date32, "date32", true),
            (ClickhouseFieldType::Date64, "date64", true),
            (
                ClickhouseFieldType::TimestampMicros,
                "timestamp_micros",
                true,
            ),
            (
                ClickhouseFieldType::TimestampMillis,
                "timestamp_millis",
                true,
            ),
            (ClickhouseFieldType::Decimal128, "decimal128", false),
        ];

        for (ty, label, cursor_capable) in types {
            assert_eq!(ty.as_str(), label);
            assert_eq!(ty.is_cursor_capable(), cursor_capable);
        }
    }

    #[test]
    fn deny_unknown_fields_on_config_structs() {
        let conn_json = r#"{"url": "http://localhost:8123", "unknown_key": 123}"#;
        assert!(serde_json::from_str::<ClickhouseConnectionConfig>(conn_json).is_err());

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
