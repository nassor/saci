//! Serde-derived configuration for the MariaDB and MySQL source and sink.
//!
//! Every struct here carries `#[serde(deny_unknown_fields)]`: a key the
//! connector cannot honour is a configuration error, not something to drop
//! silently. The mode enum is internally tagged on `kind`, matching the
//! `run_mode` table the service config already uses.
//!
//! Each top-level type exposes `validate`, which the constructors in
//! [`crate::source`](crate) and [`crate::sink`](crate) call before they build
//! anything. Validation returns [`SaciError::Configuration`] naming the
//! offending key on the first violation. A refusal reads
//! `{what}: <dotted key> <problem>[; <remedy>]`, with values in single quotes
//! and declared types in double quotes.

use std::time::Duration;

use arrow_schema::{DataType, Field, TimeUnit};
use saci_core::error::SaciError;
use serde::Deserialize;

use crate::offsets::BinlogOffset;
use crate::source::gtid::GtidSet;
use crate::types::data_type_is_known;

/// Arrow timestamp and time resolution every temporal type in this connector
/// uses. MariaDB and MySQL keep at most microseconds, so nothing is lost and
/// nothing is padded.
const UNIT: TimeUnit = TimeUnit::Microsecond;

/// The longest schema, table or trigger name either server accepts.
pub(crate) const MAX_IDENTIFIER_CHARS: usize = 64;

/// How long a hole in an `AUTO_INCREMENT` cursor holds back the rows after it
/// when `mode.gap_timeout_ms` is not set.
pub(crate) const DEFAULT_GAP_TIMEOUT_MS: u64 = 10_000;

/// The longest `mode.gap_timeout_ms` accepted: an hour. A longer wait is a
/// mistaken unit, and holds a pipeline still for as long.
pub(crate) const MAX_GAP_TIMEOUT_MS: u64 = 3_600_000;

/// How long a hole is held when `INNODB_TRX` shows a transaction that could
/// still fill it, when `mode.gap_max_wait_ms` is not set (and `gap_timeout_ms`
/// is not longer).
pub(crate) const DEFAULT_GAP_MAX_WAIT_MS: u64 = 300_000;

/// The longest `mode.gap_max_wait_ms` accepted: a day.
pub(crate) const MAX_GAP_MAX_WAIT_MS: u64 = 86_400_000;

/// The width of the offset table's `source_name` column. A longer name would be
/// cut by a non-strict session, and two sources sharing a prefix would then
/// share an offset row.
pub(crate) const MAX_SOURCE_NAME_CHARS: usize = 191;

/// Fields a `cdc_binlog` source fills from the change stream instead of from
/// the row, with the declared type each must carry.
pub(crate) const BINLOG_RESERVED: [(&str, MariadbFieldType); 6] = [
    ("__op", MariadbFieldType::Utf8),
    ("__table", MariadbFieldType::Utf8),
    ("__binlog_file", MariadbFieldType::Utf8),
    ("__binlog_pos", MariadbFieldType::UInt64),
    ("__gtid", MariadbFieldType::Utf8),
    ("__commit_ts", MariadbFieldType::TimestampMicrosUtc),
];

/// Fields a `cdc_trigger` source fills from the connector-installed changelog,
/// with the declared type each must carry.
pub(crate) const TRIGGER_RESERVED: [(&str, MariadbFieldType); 4] = [
    ("__op", MariadbFieldType::Utf8),
    ("__seq", MariadbFieldType::UInt64),
    ("__commit_ts", MariadbFieldType::TimestampMicrosUtc),
    ("__table", MariadbFieldType::Utf8),
];

// ---------------------------------------------------------------- connection

/// Shared connection settings for both the source and the sink.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// A `mysql://` or `mariadb://` URL, `mysql://user:password@host:3306/db`.
    ///
    /// Both schemes reach MariaDB and MySQL servers alike. The database in the
    /// path is what a bare table name in this configuration refers to. Never
    /// logged: errors name the target as `host:port/db` instead.
    pub dsn: String,
    /// Overrides the DSN's user.
    #[serde(default)]
    pub user: Option<String>,
    /// Overrides the DSN's password.
    #[serde(default)]
    pub password: Option<String>,
    /// Path to a file whose trimmed contents override the password.
    ///
    /// For secret mounts, so the value never appears in the config file.
    #[serde(default)]
    pub password_file: Option<String>,
    /// TCP, TLS and login handshake budget. 0 waits as long as the operating
    /// system does.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Applied on every new session as `max_execution_time` (MySQL, `SELECT`
    /// only) or `max_statement_time` (MariaDB). 0 disables.
    #[serde(default = "default_statement_timeout_ms")]
    pub statement_timeout_ms: u64,
    /// Whether and how strictly the connection is encrypted.
    #[serde(default)]
    pub ssl_mode: SslModeConfig,
    /// PEM bundle of trusted roots. Absent means the built-in web roots.
    #[serde(default)]
    pub ssl_ca: Option<String>,
    /// Backoff applied when a connection cannot be established.
    #[serde(default)]
    pub reconnect: ReconnectConfig,
}

/// How hard the connector insists on TLS.
///
/// Anything but `disabled` needs the `tls` feature.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SslModeConfig {
    /// Never negotiate TLS.
    Disabled,
    /// Encrypt, and accept any certificate. Protects against a passive
    /// listener only.
    Required,
    /// Encrypt and verify the certificate chain, meant not the host name.
    ///
    /// `mysql_async` skips the host name check by matching the text
    /// `NotValidForName` in the verifier's error, which `rustls` 0.23 no longer
    /// produces, so in this build the host name is checked as well. The mode
    /// differs from `VerifyIdentity` only once that driver defect is fixed.
    VerifyCa,
    /// Encrypt, verify the chain and the host name.
    #[default]
    VerifyIdentity,
}

impl SslModeConfig {
    /// The configured spelling, for messages.
    #[cfg(not(feature = "tls"))]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SslModeConfig::Disabled => "disabled",
            SslModeConfig::Required => "required",
            SslModeConfig::VerifyCa => "verify_ca",
            SslModeConfig::VerifyIdentity => "verify_identity",
        }
    }
}

/// Reconnect backoff.
///
/// Mirrors `saci_core::retry::RetryMode::ExponentialBackoff`'s documented
/// defaults: 3 attempts, 100 ms base, 2.0x, 30 s cap, 0.1 jitter. `RetryMode`
/// itself is not reused because it derives no serde impls.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ReconnectConfig {
    /// Total connect attempts before the error is returned.
    #[serde(default = "default_reconnect_attempts")]
    pub max_attempts: u32,
    /// Delay before the second attempt.
    #[serde(default = "default_reconnect_base_ms")]
    pub base_delay_ms: u64,
    /// Growth factor applied per attempt.
    #[serde(default = "default_reconnect_mult")]
    pub multiplier: f64,
    /// Ceiling on the computed delay.
    #[serde(default = "default_reconnect_max_ms")]
    pub max_delay_ms: u64,
    /// Fraction of the delay randomised, in `0.0..=1.0`.
    #[serde(default = "default_reconnect_jitter")]
    pub jitter: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_reconnect_attempts(),
            base_delay_ms: default_reconnect_base_ms(),
            multiplier: default_reconnect_mult(),
            max_delay_ms: default_reconnect_max_ms(),
            jitter: default_reconnect_jitter(),
        }
    }
}

impl ReconnectConfig {
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.max_attempts == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: connection.reconnect.max_attempts must be at least 1"
            )));
        }
        if self.multiplier < 1.0 || self.multiplier.is_nan() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.reconnect.multiplier must be at least 1.0, got {}",
                self.multiplier
            )));
        }
        if !(0.0..=1.0).contains(&self.jitter) {
            return Err(SaciError::configuration(format!(
                "{what}: connection.reconnect.jitter must be within 0.0..=1.0, got {}",
                self.jitter
            )));
        }
        Ok(())
    }
}

impl ConnectionConfig {
    /// The DSN with a `mariadb://` scheme rewritten to `mysql://`, the one
    /// scheme the driver parses, or `None` for any other scheme.
    pub(crate) fn normalised_dsn(&self) -> Option<String> {
        let dsn = self.dsn.trim();
        let (scheme, rest) = dsn.split_once("://")?;
        match scheme.to_ascii_lowercase().as_str() {
            "mysql" | "mariadb" => Some(format!("mysql://{rest}")),
            _ => None,
        }
    }

    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.dsn.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.dsn must not be empty"
            )));
        }
        if self.normalised_dsn().is_none() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.dsn must be a 'mysql://' or 'mariadb://' URL"
            )));
        }
        #[cfg(not(feature = "tls"))]
        if self.ssl_mode != SslModeConfig::Disabled {
            return Err(SaciError::configuration(format!(
                "{what}: connection.ssl_mode '{}' needs the 'tls' feature of \
                 saci-connector-mariadb, which is not enabled in this build; set ssl_mode \
                 \"disabled\" or enable the feature",
                self.ssl_mode.as_str()
            )));
        }
        self.reconnect.validate(what)
    }
}

// -------------------------------------------------------------------- fields

/// Which half of the connector a declared field or a connection belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// [`MariadbSourceConfig`].
    Source,
    /// [`MariadbSinkConfig`].
    Sink,
}

impl Role {
    /// The error prefix and `Connector` label for this half.
    pub(crate) fn what(self) -> &'static str {
        match self {
            Role::Source => "MariadbSource",
            Role::Sink => "MariadbSink",
        }
    }
}

/// One declared column: its name, its Arrow-facing type, and nullability.
///
/// Flat rather than an internally tagged enum, so `deny_unknown_fields` still
/// applies; `#[serde(flatten)]` would switch it off.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    /// Column name. Matched against the server by name, never by position.
    ///
    /// Read from `id`, because `saci-config` puts a node's leading argument,
    /// the `"total"` in `schema_fields "total" type="float64"`, under that
    /// key.
    #[serde(rename = "id")]
    pub name: String,
    /// Declared type. Checked against the column's server type on connect.
    #[serde(rename = "type")]
    pub ty: MariadbFieldType,
    /// Whether the Arrow field admits NULL.
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// Total digits. `decimal128` only, and required there.
    #[serde(default)]
    pub precision: Option<u8>,
    /// Fractional digits. `decimal128` only, and required there.
    #[serde(default)]
    pub scale: Option<i8>,
    /// The `information_schema` `DATA_TYPE` this column must have, such as
    /// `varchar` or `bigint`, compared without regard to case.
    ///
    /// An assertion made on first connect: it fails when the server column is
    /// another type, and changes no decoding. `type` alone decides the Arrow
    /// type.
    #[serde(default)]
    pub mariadb_type: Option<String>,
}

/// The declared type of a column.
///
/// Each maps to exactly one Arrow [`DataType`]. Which server types can fill
/// it is the table in [`crate::types`](crate).
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MariadbFieldType {
    /// `tinyint` as a flag, to Arrow `Boolean`. Only 0 and 1 are accepted.
    Boolean,
    /// To Arrow `Int8`.
    Int8,
    /// To Arrow `Int16`.
    Int16,
    /// To Arrow `Int32`.
    Int32,
    /// To Arrow `Int64`.
    Int64,
    /// To Arrow `UInt8`.
    #[serde(rename = "uint8")]
    UInt8,
    /// To Arrow `UInt16`.
    #[serde(rename = "uint16")]
    UInt16,
    /// To Arrow `UInt32`.
    #[serde(rename = "uint32")]
    UInt32,
    /// To Arrow `UInt64`.
    #[serde(rename = "uint64")]
    UInt64,
    /// `float` to Arrow `Float32`.
    Float32,
    /// `float` or `double` to Arrow `Float64`.
    Float64,
    /// `decimal` to Arrow `Decimal128`. Needs `precision` and `scale`.
    Decimal128,
    /// Text, to Arrow `Utf8`. Any column type can be read as its MySQL text
    /// form.
    Utf8,
    /// Bytes, to Arrow `Binary`.
    Binary,
    /// `date` to Arrow `Date32`.
    Date32,
    /// `time` to Arrow `Time64(Microsecond)`. A value outside a time of day is
    /// refused.
    Time64Micros,
    /// `datetime` to Arrow `Timestamp(Microsecond, None)`.
    TimestampMicros,
    /// `timestamp` to Arrow `Timestamp(Microsecond, "UTC")`.
    TimestampMicrosUtc,
    /// A JSON document to Arrow `Utf8`, carrying the document text.
    Json,
}

impl MariadbFieldType {
    /// Every declared type, for the code that walks the whole set.
    pub(crate) const ALL: [MariadbFieldType; 19] = [
        MariadbFieldType::Boolean,
        MariadbFieldType::Int8,
        MariadbFieldType::Int16,
        MariadbFieldType::Int32,
        MariadbFieldType::Int64,
        MariadbFieldType::UInt8,
        MariadbFieldType::UInt16,
        MariadbFieldType::UInt32,
        MariadbFieldType::UInt64,
        MariadbFieldType::Float32,
        MariadbFieldType::Float64,
        MariadbFieldType::Decimal128,
        MariadbFieldType::Utf8,
        MariadbFieldType::Binary,
        MariadbFieldType::Date32,
        MariadbFieldType::Time64Micros,
        MariadbFieldType::TimestampMicros,
        MariadbFieldType::TimestampMicrosUtc,
        MariadbFieldType::Json,
    ];

    /// The name used in error messages, matching the configured spelling.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            MariadbFieldType::Boolean => "boolean",
            MariadbFieldType::Int8 => "int8",
            MariadbFieldType::Int16 => "int16",
            MariadbFieldType::Int32 => "int32",
            MariadbFieldType::Int64 => "int64",
            MariadbFieldType::UInt8 => "uint8",
            MariadbFieldType::UInt16 => "uint16",
            MariadbFieldType::UInt32 => "uint32",
            MariadbFieldType::UInt64 => "uint64",
            MariadbFieldType::Float32 => "float32",
            MariadbFieldType::Float64 => "float64",
            MariadbFieldType::Decimal128 => "decimal128",
            MariadbFieldType::Utf8 => "utf8",
            MariadbFieldType::Binary => "binary",
            MariadbFieldType::Date32 => "date32",
            MariadbFieldType::Time64Micros => "time64_micros",
            MariadbFieldType::TimestampMicros => "timestamp_micros",
            MariadbFieldType::TimestampMicrosUtc => "timestamp_micros_utc",
            MariadbFieldType::Json => "json",
        }
    }

    /// The width in bits of an integer type, `None` for any other.
    pub(crate) fn int_bits(self) -> Option<u8> {
        match self {
            MariadbFieldType::Int8 | MariadbFieldType::UInt8 => Some(8),
            MariadbFieldType::Int16 | MariadbFieldType::UInt16 => Some(16),
            MariadbFieldType::Int32 | MariadbFieldType::UInt32 => Some(32),
            MariadbFieldType::Int64 | MariadbFieldType::UInt64 => Some(64),
            _ => None,
        }
    }

    /// Whether this is one of the four unsigned integer types.
    pub(crate) fn is_unsigned_int(self) -> bool {
        matches!(
            self,
            MariadbFieldType::UInt8
                | MariadbFieldType::UInt16
                | MariadbFieldType::UInt32
                | MariadbFieldType::UInt64
        )
    }

    /// Whether a column of this type can carry a cursor.
    ///
    /// A cursor must be totally ordered by the same comparison the server and
    /// the persisted text form agree on, which rules out floats, decimals,
    /// booleans, blobs, times and documents.
    pub(crate) fn is_cursor_capable(self) -> bool {
        matches!(
            self,
            MariadbFieldType::Int8
                | MariadbFieldType::Int16
                | MariadbFieldType::Int32
                | MariadbFieldType::Int64
                | MariadbFieldType::UInt8
                | MariadbFieldType::UInt16
                | MariadbFieldType::UInt32
                | MariadbFieldType::UInt64
                | MariadbFieldType::Utf8
                | MariadbFieldType::Date32
                | MariadbFieldType::TimestampMicros
                | MariadbFieldType::TimestampMicrosUtc
        )
    }
}

impl FieldSpec {
    /// The Arrow field this spec declares.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `decimal128` is missing
    /// `precision` or `scale` or carries an out-of-range one, and when
    /// `precision` or `scale` is set on another type.
    pub fn to_arrow_field(&self) -> Result<Field, SaciError> {
        if self.ty != MariadbFieldType::Decimal128
            && (self.precision.is_some() || self.scale.is_some())
        {
            return Err(SaciError::configuration(format!(
                "field '{}': precision and scale apply to type \"decimal128\" only, not \"{}\"",
                self.name,
                self.ty.as_str()
            )));
        }
        let data_type = match self.ty {
            MariadbFieldType::Boolean => DataType::Boolean,
            MariadbFieldType::Int8 => DataType::Int8,
            MariadbFieldType::Int16 => DataType::Int16,
            MariadbFieldType::Int32 => DataType::Int32,
            MariadbFieldType::Int64 => DataType::Int64,
            MariadbFieldType::UInt8 => DataType::UInt8,
            MariadbFieldType::UInt16 => DataType::UInt16,
            MariadbFieldType::UInt32 => DataType::UInt32,
            MariadbFieldType::UInt64 => DataType::UInt64,
            MariadbFieldType::Float32 => DataType::Float32,
            MariadbFieldType::Float64 => DataType::Float64,
            MariadbFieldType::Utf8 | MariadbFieldType::Json => DataType::Utf8,
            MariadbFieldType::Binary => DataType::Binary,
            MariadbFieldType::Date32 => DataType::Date32,
            MariadbFieldType::Time64Micros => DataType::Time64(UNIT),
            MariadbFieldType::TimestampMicros => DataType::Timestamp(UNIT, None),
            MariadbFieldType::TimestampMicrosUtc => DataType::Timestamp(UNIT, Some("UTC".into())),
            MariadbFieldType::Decimal128 => {
                let (precision, scale) = self.decimal_params()?;
                DataType::Decimal128(precision, scale)
            }
        };
        Ok(Field::new(&self.name, data_type, self.nullable))
    }

    /// The declared `(precision, scale)` of a `decimal128` field.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when either is missing or out of
    /// range.
    pub(crate) fn decimal_params(&self) -> Result<(u8, i8), SaciError> {
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

    /// Whether the name carries the `__` prefix this connector reserves for
    /// the metadata columns a change stream fills.
    pub(crate) fn is_reserved(&self) -> bool {
        self.name.starts_with("__")
    }
}

/// Validate a declared field list: non-empty, unique names, each field legal.
fn validate_fields(what: &str, fields: &[FieldSpec]) -> Result<(), SaciError> {
    if fields.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: schema_fields must declare at least one field"
        )));
    }
    for (i, field) in fields.iter().enumerate() {
        if field.name.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: schema_fields[{i}] has an empty 'name'"
            )));
        }
        if fields[..i].iter().any(|f| f.name == field.name) {
            return Err(SaciError::configuration(format!(
                "{what}: schema_fields declares '{}' more than once",
                field.name
            )));
        }
        field
            .to_arrow_field()
            .map_err(|e| SaciError::configuration(format!("{what}: {}", e.message())))?;
        if let Some(declared) = &field.mariadb_type
            && !data_type_is_known(declared)
        {
            return Err(SaciError::configuration(format!(
                "{what}: field '{}': mariadb_type '{declared}' is not a column type this \
                 connector reads or writes",
                field.name
            )));
        }
    }
    Ok(())
}

/// Look up a declared field by name.
pub(crate) fn find_field<'a>(fields: &'a [FieldSpec], name: &str) -> Option<&'a FieldSpec> {
    fields.iter().find(|f| f.name == name)
}

/// Validate that `column` names a declared, cursor-capable field.
fn validate_cursor_field(
    what: &str,
    key: &str,
    fields: &[FieldSpec],
    column: &str,
) -> Result<(), SaciError> {
    let field = find_field(fields, column).ok_or_else(|| {
        SaciError::configuration(format!(
            "{what}: {key} '{column}' does not name a declared schema_fields entry"
        ))
    })?;
    if !field.ty.is_cursor_capable() {
        return Err(SaciError::configuration(format!(
            "{what}: {key} '{column}' has type \"{}\", which cannot order a cursor; use one of \
             int8, int16, int32, int64, uint8, uint16, uint32, uint64, utf8, date32, \
             timestamp_micros, timestamp_micros_utc",
            field.ty.as_str()
        )));
    }
    Ok(())
}

/// Validate a connector name. It keys the offset row, labels every metric and
/// names the changelog table and triggers, so the character set is restricted
/// rather than trusted to quoting.
fn validate_name(what: &str, name: &str) -> Result<(), SaciError> {
    if name.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: 'name' must not be empty"
        )));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(SaciError::configuration(format!(
            "{what}: 'name' '{name}' contains '{bad}'; only ASCII letters, digits, '_' and '-' \
             are allowed"
        )));
    }
    if name.len() > MAX_SOURCE_NAME_CHARS {
        return Err(SaciError::configuration(format!(
            "{what}: 'name' is {} characters; the offset table keeps at most \
             {MAX_SOURCE_NAME_CHARS}",
            name.len()
        )));
    }
    Ok(())
}

/// Split `schema.table` or a bare `table`, naming `key` when it is malformed.
///
/// A bare name carries no schema here: which database it belongs to is the
/// DSN's, known only once the connection details are parsed.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] on more than one `.`, or an empty part.
pub(crate) fn split_qualified<'a>(
    what: &str,
    key: &str,
    table: &'a str,
) -> Result<(Option<&'a str>, &'a str), SaciError> {
    let mut parts = table.split('.');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() {
        return Err(SaciError::configuration(format!(
            "{what}: {key} '{table}' must be 'table' or 'schema.table', not a longer path"
        )));
    }
    let (schema, name) = match second {
        Some(name) => (Some(first), name),
        None => (None, first),
    };
    if name.is_empty() || schema.is_some_and(str::is_empty) {
        return Err(SaciError::configuration(format!(
            "{what}: {key} '{table}' has an empty schema or table part"
        )));
    }
    Ok((schema, name))
}

/// Refuse any `__`-prefixed field no change stream fills, for a mode that has
/// no reserved columns at all.
fn reject_reserved(what: &str, fields: &[FieldSpec]) -> Result<(), SaciError> {
    if let Some(field) = fields.iter().find(|f| f.is_reserved()) {
        return Err(SaciError::configuration(format!(
            "{what}: schema_fields '{}' uses the reserved __ prefix, which only cdc_binlog and \
             an installed cdc_trigger fill",
            field.name
        )));
    }
    Ok(())
}

/// Check every `__`-prefixed field against the names `allowed` lists and the
/// type each carries. `filler` names what fills them, for the message.
fn check_reserved(
    what: &str,
    fields: &[FieldSpec],
    allowed: &[(&str, MariadbFieldType)],
    filler: &str,
) -> Result<(), SaciError> {
    for field in fields.iter().filter(|f| f.is_reserved()) {
        let Some((_, expected)) = allowed.iter().find(|(name, _)| *name == field.name) else {
            let names: Vec<&str> = allowed.iter().map(|(name, _)| *name).collect();
            return Err(SaciError::configuration(format!(
                "{what}: schema_fields '{}' uses the reserved __ prefix, and {filler} fills only \
                 {}",
                field.name,
                names.join(", ")
            )));
        };
        if field.ty != *expected {
            return Err(SaciError::configuration(format!(
                "{what}: reserved column '{}' must be declared type \"{}\", not \"{}\"",
                field.name,
                expected.as_str(),
                field.ty.as_str()
            )));
        }
    }
    Ok(())
}

// -------------------------------------------------------------------- source

/// Everything the MariaDB and MySQL source needs.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct MariadbSourceConfig {
    /// Stable identity for this source.
    ///
    /// `SourceFactory::build` receives only the `config` sub-table, never the
    /// node's own name, so the name is declared here too. It keys the persisted
    /// offset row, labels every metric, prefixes the default changelog table
    /// and the default replica `server_id`.
    pub name: String,
    /// Where and how to connect.
    pub connection: ConnectionConfig,
    /// Which read strategy to use.
    pub mode: SourceMode,
    /// The Arrow schema every emitted batch conforms to.
    #[serde(deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<FieldSpec>,
    /// Rows per emitted `RecordBatch`, and the `LIMIT` on each query.
    ///
    /// Only in force when the host asks for no size:
    /// `Source::request_batch_rows` overwrites it before every query, and
    /// `saci-service` sends that hint from the first query onward unless the
    /// source's `flow_control` block sets `enabled #false`, or the source is
    /// on a path to a windowed node, where the runner withholds the hint so
    /// this value alone decides how large an arrival is.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    /// Batches per drain cycle. 0 drains until caught up.
    #[serde(default)]
    pub max_batches_per_cycle: usize,
}

/// The read strategy, chosen by `kind`.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceMode {
    /// Incremental query over a live table.
    Polling(CursorMode),
    /// A changelog table: the operator's own outbox, or one the connector
    /// fills with triggers it installs.
    CdcTrigger(TriggerMode),
    /// The row binlog, by file position or by GTID, with an optional
    /// consistent snapshot first.
    CdcBinlog(BinlogMode),
}

impl SourceMode {
    /// The metric label for this mode.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            SourceMode::Polling(_) => "polling",
            SourceMode::CdcTrigger(_) => "cdc_trigger",
            SourceMode::CdcBinlog(_) => "cdc_binlog",
        }
    }

    /// The table this source reads or watches, as configured.
    pub(crate) fn table(&self) -> &str {
        match self {
            SourceMode::Polling(mode) => &mode.table,
            SourceMode::CdcTrigger(mode) => &mode.table,
            SourceMode::CdcBinlog(mode) => &mode.table,
        }
    }

    /// The offset table, as configured.
    pub(crate) fn offset_table(&self) -> &str {
        match self {
            SourceMode::Polling(mode) => &mode.offset_table,
            SourceMode::CdcTrigger(mode) => &mode.offset_table,
            SourceMode::CdcBinlog(mode) => &mode.offset_table,
        }
    }

    /// Whether the offset table is created when it is missing.
    pub(crate) fn offset_table_autocreate(&self) -> bool {
        match self {
            SourceMode::Polling(mode) => mode.offset_table_autocreate,
            SourceMode::CdcTrigger(mode) => mode.offset_table_autocreate,
            SourceMode::CdcBinlog(mode) => mode.offset_table_autocreate,
        }
    }

    /// How long a hole in an `AUTO_INCREMENT` cursor holds back the rows after
    /// it. `cdc_binlog` has no such cursor, so it holds nothing back.
    pub(crate) fn gap_timeout(&self) -> Duration {
        match self {
            SourceMode::Polling(mode) => mode.gap_timeout(),
            SourceMode::CdcTrigger(mode) => mode.gap_timeout(),
            SourceMode::CdcBinlog(_) => Duration::ZERO,
        }
    }

    /// How long a hole is held while a transaction that could fill it is still
    /// open. `None` holds without limit.
    pub(crate) fn gap_max_wait(&self) -> Option<Duration> {
        match self {
            SourceMode::Polling(mode) => mode.gap_max_wait(),
            SourceMode::CdcTrigger(mode) => mode.gap_max_wait(),
            SourceMode::CdcBinlog(_) => None,
        }
    }
}

/// Settings of the `polling` mode.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CursorMode {
    /// `schema.table`, or a bare `table` in the DSN's database.
    pub table: String,
    /// The ordering column. Its declared type drives both the SQL cast and the
    /// text encoding of the persisted offset.
    pub cursor_column: String,
    /// Second sort and compare key, for a non-unique cursor.
    ///
    /// Without it, a batch boundary landing inside a run of equal cursor values
    /// skips the rest of that run.
    #[serde(default)]
    pub tiebreak_column: Option<String>,
    /// `"beginning"`, `"now"`, or a literal starting cursor value. Used only
    /// when the offset table holds nothing for this source name.
    #[serde(default = "default_cursor_initial")]
    pub initial: String,
    /// Table holding one durable offset row per source name.
    #[serde(default = "default_offset_table")]
    pub offset_table: String,
    /// Whether to `CREATE TABLE IF NOT EXISTS` the offset table on connect.
    #[serde(default = "default_true")]
    pub offset_table_autocreate: bool,
    /// Extra predicate AND-ed into every query, interpolated verbatim.
    ///
    /// This is the one place operator-supplied SQL reaches the query text;
    /// every identifier the connector derives from config is quoted.
    #[serde(default)]
    pub where_clause: Option<String>,
    /// Milliseconds a hole in an `AUTO_INCREMENT` cursor holds back the rows
    /// after it before the source gives the hole up as skipped. `0` reads past
    /// a hole at once. Defaults to 10000.
    #[serde(default)]
    pub gap_timeout_ms: Option<u64>,
    /// Milliseconds a hole is held while `INNODB_TRX` shows a transaction that
    /// could still fill it. `0` holds without limit. Defaults to 300000, or
    /// `gap_timeout_ms` when that is longer.
    #[serde(default)]
    pub gap_max_wait_ms: Option<u64>,
}

/// Settings of the `cdc_trigger` mode.
///
/// With `install_triggers #false` the mode reads the operator's own outbox
/// table the way `polling` reads a table, and `cursor_column` names its
/// ordering column. With `install_triggers #true` the connector creates a
/// changelog table and three triggers on `table`, and the cursor is the
/// changelog's own `__seq`.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TriggerMode {
    /// The operator's outbox, or with `install_triggers #true` the watched
    /// table. `schema.table`, or a bare `table` in the DSN's database.
    pub table: String,
    /// Whether the connector installs the changelog table and the triggers.
    #[serde(default)]
    pub install_triggers: bool,
    /// The changelog table the triggers write. Only with `install_triggers
    /// #true`. Defaults to `saci_cdc_<name>` with `-` written as `_`; a bare
    /// name lives in the watched table's schema.
    #[serde(default)]
    pub changelog_table: Option<String>,
    /// The ordering column of the outbox. Required with `install_triggers
    /// #false`, refused with `#true`.
    #[serde(default)]
    pub cursor_column: Option<String>,
    /// Second sort and compare key, for a non-unique outbox cursor. Refused
    /// with `install_triggers #true`.
    #[serde(default)]
    pub tiebreak_column: Option<String>,
    /// Extra predicate AND-ed into every query, interpolated verbatim. Refused
    /// with `install_triggers #true`.
    #[serde(default)]
    pub where_clause: Option<String>,
    /// `"beginning"`, `"now"`, or a literal starting cursor value. Used only
    /// when the offset table holds nothing for this source name.
    #[serde(default = "default_cursor_initial")]
    pub initial: String,
    /// Table holding one durable offset row per source name.
    #[serde(default = "default_offset_table")]
    pub offset_table: String,
    /// Whether to `CREATE TABLE IF NOT EXISTS` the offset table on connect.
    #[serde(default = "default_true")]
    pub offset_table_autocreate: bool,
    /// What happens to rows the source has acknowledged. Defaults to `keep`
    /// for an operator outbox and to `delete_acked` for an installed
    /// changelog, which would otherwise grow without bound.
    #[serde(default)]
    pub retention: Option<Retention>,
    /// Milliseconds a hole in an `AUTO_INCREMENT` cursor (the outbox's, or the
    /// installed changelog's `__seq`) holds back the rows after it before the
    /// source gives the hole up as skipped. `0` reads past a hole at once.
    /// Defaults to 10000.
    #[serde(default)]
    pub gap_timeout_ms: Option<u64>,
    /// Milliseconds a hole is held while `INNODB_TRX` shows a transaction that
    /// could still fill it. `0` holds without limit. Defaults to 300000, or
    /// `gap_timeout_ms` when that is longer.
    #[serde(default)]
    pub gap_max_wait_ms: Option<u64>,
}

/// What happens to changelog rows the source has acknowledged.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    /// Leave them in place.
    #[default]
    Keep,
    /// `DELETE` them when the offset is committed.
    DeleteAcked,
}

/// Settings of the `cdc_binlog` mode.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct BinlogMode {
    /// The one `schema.table` this source emits, or a bare `table` in the
    /// DSN's database. Rows of every other table in the binlog are skipped and
    /// counted.
    pub table: String,
    /// The replica id the source registers with. Every replica of one server
    /// needs its own. Defaults to a value derived from `name`, in
    /// `4000000000..4100000000`.
    #[serde(default)]
    pub server_id: Option<u32>,
    /// Whether the stream resumes by binlog file and position, or by GTID set.
    #[serde(default)]
    pub position: PositionMode,
    /// Where to start when the offset table holds nothing for this source:
    /// `"now"`, `"beginning"`, or a literal. A literal is
    /// `file:<binlog name>:<position>` under `position "file"` and a GTID set
    /// under `position "gtid"`.
    #[serde(default = "default_binlog_initial")]
    pub initial: String,
    /// `"initial"` reads the table's existing rows first, as one consistent
    /// snapshot, then streams every change made after it.
    #[serde(default)]
    pub snapshot: SnapshotMode,
    /// Table holding one durable offset row per source name.
    #[serde(default = "default_offset_table")]
    pub offset_table: String,
    /// Whether to `CREATE TABLE IF NOT EXISTS` the offset table on connect.
    #[serde(default = "default_true")]
    pub offset_table_autocreate: bool,
    /// Whether to keep the history of the table's definition beside the offset
    /// table, so a restart from a stored position decodes every row with the
    /// definition in force when it was written. `#false` decodes every row
    /// with the table's current definition.
    #[serde(default = "default_true")]
    pub schema_history: bool,
    /// The table holding that history, `schema.table` or a bare name in the
    /// DSN's database. Defaults to the offset table's name with `_schema`
    /// appended.
    #[serde(default)]
    pub schema_history_table: Option<String>,
    /// Decoded changes after which a cycle ends on the next transaction
    /// boundary, however much binlog is left.
    #[serde(default = "default_max_changes")]
    pub max_changes_per_cycle: usize,
}

/// How a `cdc_binlog` source names where it stopped.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PositionMode {
    /// Binlog file name and byte position. Works on MariaDB and MySQL.
    #[default]
    File,
    /// A set of MySQL GTIDs. MySQL with `gtid_mode = ON` only.
    Gtid,
}

/// Whether a `cdc_binlog` source reads the existing rows first.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    /// Stream changes only.
    #[default]
    Never,
    /// Read the table once as `R` rows, then stream.
    Initial,
}

/// The default replica id for a source named `name`: `4000000000` plus the
/// FNV-1a hash of the name modulo `100000000`, so it stays in the top of the
/// `u32` range a manually configured replica is unlikely to use.
pub(crate) fn default_server_id(name: &str) -> u32 {
    let mut hash: u32 = 2_166_136_261;
    for byte in name.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    4_000_000_000 + hash % 100_000_000
}

impl BinlogMode {
    /// The replica id to register with: `server_id`, or the name's default.
    pub(crate) fn effective_server_id(&self, source_name: &str) -> u32 {
        self.server_id
            .unwrap_or_else(|| default_server_id(source_name))
    }

    /// The schema history table with the default applied: the offset table's
    /// name with `_schema` appended.
    pub(crate) fn effective_schema_history_table(&self) -> String {
        self.schema_history_table
            .clone()
            .unwrap_or_else(|| format!("{}_schema", self.offset_table))
    }
}

impl CursorMode {
    /// How long a hole in the sequence holds back the rows after it, with the
    /// default applied. [`Duration::ZERO`] turns the hold off.
    pub(crate) fn gap_timeout(&self) -> Duration {
        gap_timeout(self.gap_timeout_ms)
    }

    /// How long a hole is held while a transaction that could fill it is open.
    pub(crate) fn gap_max_wait(&self) -> Option<Duration> {
        gap_max_wait(self.gap_timeout_ms, self.gap_max_wait_ms)
    }
}

impl TriggerMode {
    /// What happens to acknowledged rows, with the mode's default applied.
    pub(crate) fn effective_retention(&self) -> Retention {
        self.retention.unwrap_or(if self.install_triggers {
            Retention::DeleteAcked
        } else {
            Retention::Keep
        })
    }

    /// The changelog table with the default applied: `schema.table` or a bare
    /// name, which lives in the watched table's schema.
    pub(crate) fn effective_changelog_table(&self, source_name: &str) -> String {
        self.changelog_table
            .clone()
            .unwrap_or_else(|| default_changelog_table(source_name))
    }

    /// How long a hole in the sequence holds back the rows after it, with the
    /// default applied. [`Duration::ZERO`] turns the hold off.
    pub(crate) fn gap_timeout(&self) -> Duration {
        gap_timeout(self.gap_timeout_ms)
    }

    /// How long a hole is held while a transaction that could fill it is open.
    pub(crate) fn gap_max_wait(&self) -> Option<Duration> {
        gap_max_wait(self.gap_timeout_ms, self.gap_max_wait_ms)
    }
}

/// `gap_timeout_ms` with the default applied.
fn gap_timeout(configured: Option<u64>) -> Duration {
    Duration::from_millis(configured.unwrap_or(DEFAULT_GAP_TIMEOUT_MS))
}

/// `gap_max_wait_ms` with the default applied: unset is the default or the
/// fallback timeout when that is longer, `0` is no limit.
fn gap_max_wait(timeout_ms: Option<u64>, configured: Option<u64>) -> Option<Duration> {
    let timeout = gap_timeout(timeout_ms);
    match configured {
        None => Some(Duration::from_millis(DEFAULT_GAP_MAX_WAIT_MS).max(timeout)),
        Some(0) => None,
        Some(ms) => Some(Duration::from_millis(ms)),
    }
}

/// `saci_cdc_<name>` with `-` written as `_`.
fn default_changelog_table(source_name: &str) -> String {
    format!("saci_cdc_{}", source_name.replace('-', "_"))
}

impl MariadbSourceConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = Role::Source.what();
        validate_name(what, &self.name)?;
        self.connection.validate(what)?;
        validate_fields(what, &self.schema_fields)?;

        if self.batch_rows == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: batch_rows must be at least 1"
            )));
        }

        split_qualified(what, "mode.table", self.mode.table())?;
        split_qualified(what, "mode.offset_table", self.mode.offset_table())?;

        match &self.mode {
            SourceMode::Polling(cursor) => {
                validate_cursor_field(
                    what,
                    "mode.cursor_column",
                    &self.schema_fields,
                    &cursor.cursor_column,
                )?;
                validate_cursor_extras(
                    what,
                    &self.schema_fields,
                    &cursor.cursor_column,
                    cursor.tiebreak_column.as_deref(),
                    cursor.where_clause.as_deref(),
                    &cursor.initial,
                )?;
                validate_gap_timeout(
                    what,
                    &self.schema_fields,
                    Some(&cursor.cursor_column),
                    (cursor.gap_timeout_ms, cursor.gap_max_wait_ms),
                )?;
                reject_reserved(what, &self.schema_fields)?;
            }
            SourceMode::CdcTrigger(trigger) => {
                self.validate_trigger(what, trigger)?;
            }
            SourceMode::CdcBinlog(binlog) => {
                self.validate_binlog(what, binlog)?;
            }
        }

        Ok(())
    }

    fn validate_trigger(&self, what: &str, trigger: &TriggerMode) -> Result<(), SaciError> {
        if trigger.initial.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: mode.initial must be \"beginning\", \"now\", or a literal cursor value"
            )));
        }
        if trigger.install_triggers {
            for (key, present) in [
                ("mode.cursor_column", trigger.cursor_column.is_some()),
                ("mode.tiebreak_column", trigger.tiebreak_column.is_some()),
                ("mode.where_clause", trigger.where_clause.is_some()),
            ] {
                if present {
                    return Err(SaciError::configuration(format!(
                        "{what}: {key} cannot be set with install_triggers #true; the cursor \
                         is the changelog's own __seq"
                    )));
                }
            }
            let changelog = trigger.effective_changelog_table(&self.name);
            let (schema, table) = split_qualified(what, "mode.changelog_table", &changelog)?;
            for part in schema.into_iter().chain([table]) {
                if part.chars().count() > MAX_IDENTIFIER_CHARS {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.changelog_table '{part}' is {} characters, over the \
                         {MAX_IDENTIFIER_CHARS} a table name may have; set mode.changelog_table \
                         to a shorter name",
                        part.chars().count()
                    )));
                }
            }
            check_reserved(
                what,
                &self.schema_fields,
                &TRIGGER_RESERVED,
                "an installed cdc_trigger",
            )?;
            validate_gap_timeout(
                what,
                &self.schema_fields,
                None,
                (trigger.gap_timeout_ms, trigger.gap_max_wait_ms),
            )?;
        } else {
            if trigger.changelog_table.is_some() {
                return Err(SaciError::configuration(format!(
                    "{what}: mode.changelog_table applies with install_triggers #true only; an \
                     outbox is the table itself"
                )));
            }
            let Some(cursor) = &trigger.cursor_column else {
                return Err(SaciError::configuration(format!(
                    "{what}: mode.cursor_column is required with install_triggers #false; name \
                     the outbox's ordering column"
                )));
            };
            validate_cursor_field(what, "mode.cursor_column", &self.schema_fields, cursor)?;
            validate_cursor_extras(
                what,
                &self.schema_fields,
                cursor,
                trigger.tiebreak_column.as_deref(),
                trigger.where_clause.as_deref(),
                &trigger.initial,
            )?;
            validate_gap_timeout(
                what,
                &self.schema_fields,
                Some(cursor.as_str()),
                (trigger.gap_timeout_ms, trigger.gap_max_wait_ms),
            )?;
            // An outbox is read as ordinary columns, so a `__op` it carries
            // for `apply_changes` is just another column.
        }
        Ok(())
    }

    fn validate_binlog(&self, what: &str, binlog: &BinlogMode) -> Result<(), SaciError> {
        if binlog.server_id == Some(0) {
            return Err(SaciError::configuration(format!(
                "{what}: mode.server_id must not be 0; the server refuses a replica with id 0"
            )));
        }
        if binlog.max_changes_per_cycle == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: mode.max_changes_per_cycle must be at least 1"
            )));
        }
        if binlog.schema_history {
            let history = binlog.effective_schema_history_table();
            let (schema, table) = split_qualified(what, "mode.schema_history_table", &history)?;
            for part in schema.into_iter().chain([table]) {
                if part.chars().count() > MAX_IDENTIFIER_CHARS {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.schema_history_table '{part}' is {} characters, over the \
                         {MAX_IDENTIFIER_CHARS} a table name may have; set \
                         mode.schema_history_table to a shorter name",
                        part.chars().count()
                    )));
                }
            }
        } else if binlog.schema_history_table.is_some() {
            return Err(SaciError::configuration(format!(
                "{what}: mode.schema_history_table applies with schema_history #true only; drop \
                 it, or keep the history"
            )));
        }
        let initial = binlog.initial.as_str();
        if initial.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: mode.initial must be \"now\", \"beginning\", or a position literal"
            )));
        }
        if binlog.snapshot == SnapshotMode::Initial && initial != "now" {
            return Err(SaciError::configuration(format!(
                "{what}: mode.snapshot \"initial\" pins its own start position, so mode.initial \
                 must stay \"now\", got '{initial}'"
            )));
        }
        let literal = !matches!(initial, "now" | "beginning");
        match binlog.position {
            PositionMode::File if literal => match BinlogOffset::parse(initial) {
                Ok(BinlogOffset::File { .. }) => {}
                _ => {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.initial '{initial}' must be \"now\", \"beginning\" or \
                         'file:<binlog name>:<position>' under position \"file\""
                    )));
                }
            },
            PositionMode::Gtid if literal => {
                if initial.starts_with("file:") {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.initial '{initial}' is a file position, but position is \
                         \"gtid\"; give a GTID set, \"now\" or \"beginning\""
                    )));
                }
                if let Err(reason) = initial.parse::<GtidSet>() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.initial '{initial}' is not a GTID set ({reason}); give a \
                         GTID set, \"now\" or \"beginning\" under position \"gtid\""
                    )));
                }
            }
            _ => {}
        }
        check_reserved(
            what,
            &self.schema_fields,
            &BINLOG_RESERVED,
            "a cdc_binlog source",
        )
    }
}

/// The tiebreak, `where_clause` and `initial` rules `polling` and an outbox
/// share.
fn validate_cursor_extras(
    what: &str,
    fields: &[FieldSpec],
    cursor_column: &str,
    tiebreak_column: Option<&str>,
    where_clause: Option<&str>,
    initial: &str,
) -> Result<(), SaciError> {
    if let Some(tiebreak) = tiebreak_column {
        validate_cursor_field(what, "mode.tiebreak_column", fields, tiebreak)?;
        if tiebreak == cursor_column {
            return Err(SaciError::configuration(format!(
                "{what}: mode.tiebreak_column '{tiebreak}' repeats mode.cursor_column"
            )));
        }
    }
    if where_clause.is_some_and(|clause| clause.trim().is_empty()) {
        return Err(SaciError::configuration(format!(
            "{what}: mode.where_clause must not be empty; leave it out to read every row"
        )));
    }
    if initial.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: mode.initial must be \"beginning\", \"now\", or a literal cursor value"
        )));
    }
    Ok(())
}

/// The `gap_timeout_ms` and `gap_max_wait_ms` rules `polling` and both
/// `cdc_trigger` forms share. An installed changelog's cursor is `__seq`, which
/// is always an integer, so it passes no `cursor_column`.
fn validate_gap_timeout(
    what: &str,
    fields: &[FieldSpec],
    cursor_column: Option<&str>,
    (timeout_ms, max_wait_ms): (Option<u64>, Option<u64>),
) -> Result<(), SaciError> {
    if let Some(ms) = timeout_ms
        && ms > MAX_GAP_TIMEOUT_MS
    {
        return Err(SaciError::configuration(format!(
            "{what}: mode.gap_timeout_ms {ms} is over the {MAX_GAP_TIMEOUT_MS} (one hour) a \
             hole may be waited for; give a shorter wait, or 0 to read past holes"
        )));
    }
    if let Some(ms) = max_wait_ms {
        if ms > MAX_GAP_MAX_WAIT_MS {
            return Err(SaciError::configuration(format!(
                "{what}: mode.gap_max_wait_ms {ms} is over the {MAX_GAP_MAX_WAIT_MS} (one day) \
                 a hole may be held; give a shorter wait, or 0 for no limit"
            )));
        }
        if timeout_ms == Some(0) {
            return Err(SaciError::configuration(format!(
                "{what}: mode.gap_max_wait_ms has no effect with mode.gap_timeout_ms 0, which \
                 reads past holes at once; drop the key"
            )));
        }
        let timeout = gap_timeout(timeout_ms).as_millis();
        if ms != 0 && u128::from(ms) < timeout {
            return Err(SaciError::configuration(format!(
                "{what}: mode.gap_max_wait_ms {ms} is below mode.gap_timeout_ms {timeout}; the \
                 limit on a hole a transaction may still fill cannot be shorter than the \
                 fallback wait (or give 0 for no limit)"
            )));
        }
    }
    if (timeout_ms.is_some() || max_wait_ms.is_some())
        && let Some(column) = cursor_column
        && let Some(field) = find_field(fields, column)
        && field.ty.int_bits().is_none()
    {
        let key = if timeout_ms.is_some() {
            "gap_timeout_ms"
        } else {
            "gap_max_wait_ms"
        };
        return Err(SaciError::configuration(format!(
            "{what}: mode.{key} applies to an integer cursor column, and '{column}' is \
             declared \"{}\"; drop the key",
            field.ty.as_str()
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------- sink

/// Everything the MariaDB and MySQL sink needs.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct MariadbSinkConfig {
    /// Stable identity for this sink; see [`MariadbSourceConfig::name`].
    ///
    /// It labels every metric and prefixes every error.
    pub name: String,
    /// Where and how to connect.
    pub connection: ConnectionConfig,
    /// `schema.table`, or a bare `table` in the DSN's database.
    pub table: String,
    /// The Arrow schema every accepted batch must match.
    #[serde(deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<FieldSpec>,
    /// How rows reach the target table.
    #[serde(default)]
    pub write_mode: WriteMode,
    /// The key a row collides on. Required for every mode but `append`, and
    /// the same set of columns as the table's primary key or one of its unique
    /// indexes, so the server resolves a duplicate on exactly this key.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub conflict_columns: Vec<String>,
    /// `upsert` and `apply_changes` only. Empty means every written column
    /// except the conflict columns.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub update_columns: Vec<String>,
    /// `upsert` and `ignore_conflicts` only: keep, per conflict key, the row of
    /// a batch with the greatest value of this column. Without it the rows
    /// apply in batch order: the last one wins under `upsert`, the first under
    /// `ignore_conflicts`.
    #[serde(default)]
    pub dedupe_order_column: Option<String>,
    /// Rows per `INSERT` statement, capped so a statement never carries more
    /// than 65535 parameters.
    #[serde(default = "default_chunk_rows")]
    pub chunk_rows: usize,
    /// Buffer batches until this many rows accumulate. 0 flushes on every
    /// `write_batch`, which is one transaction per pipeline iteration.
    #[serde(default)]
    pub flush_rows: usize,
    /// `DELETE FROM` the target inside the first flush's transaction.
    #[serde(default)]
    pub truncate_before_first_write: bool,
}

/// How the sink resolves a row that collides with an existing one.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// A plain `INSERT`. A collision is an error.
    #[default]
    Append,
    /// `INSERT ... ON DUPLICATE KEY UPDATE`.
    Upsert,
    /// `INSERT ... ON DUPLICATE KEY UPDATE k = k`, which leaves the existing
    /// row as it is.
    IgnoreConflicts,
    /// Apply the declared `__op` column in order: `I`, `U` and `R` write the
    /// row, `D` deletes it.
    ApplyChanges,
}

impl WriteMode {
    /// The configured spelling, for messages.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            WriteMode::Append => "append",
            WriteMode::Upsert => "upsert",
            WriteMode::IgnoreConflicts => "ignore_conflicts",
            WriteMode::ApplyChanges => "apply_changes",
        }
    }
}

impl MariadbSinkConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = Role::Sink.what();
        validate_name(what, &self.name)?;
        self.connection.validate(what)?;
        validate_fields(what, &self.schema_fields)?;
        split_qualified(what, "table", &self.table)?;

        if self.chunk_rows == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: chunk_rows must be at least 1"
            )));
        }

        let mode = self.write_mode.as_str();
        if self.write_mode == WriteMode::Append {
            for (key, present) in [
                ("conflict_columns", !self.conflict_columns.is_empty()),
                ("update_columns", !self.update_columns.is_empty()),
                ("dedupe_order_column", self.dedupe_order_column.is_some()),
            ] {
                if present {
                    return Err(SaciError::configuration(format!(
                        "{what}: {key} applies to write_mode \"upsert\", \"ignore_conflicts\" \
                         or \"apply_changes\", not \"append\""
                    )));
                }
            }
            return Ok(());
        }

        if self.conflict_columns.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: write_mode \"{mode}\" requires a non-empty conflict_columns"
            )));
        }
        for column in &self.conflict_columns {
            self.check_column("conflict_columns", column)?;
        }
        // A repeated column would let `a, a` pass for the key `(a, b)`, and a
        // `DELETE` by it would remove every row with that `a`.
        if let Some((_, column)) = self
            .conflict_columns
            .iter()
            .enumerate()
            .find(|(i, column)| self.conflict_columns[..*i].contains(column))
        {
            return Err(SaciError::configuration(format!(
                "{what}: conflict_columns names '{column}' twice"
            )));
        }
        if self.write_mode == WriteMode::IgnoreConflicts && !self.update_columns.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: update_columns applies to write_mode \"upsert\" or \"apply_changes\", \
                 not \"ignore_conflicts\""
            )));
        }
        for column in &self.update_columns {
            self.check_column("update_columns", column)?;
            if self.conflict_columns.iter().any(|c| c == column) {
                return Err(SaciError::configuration(format!(
                    "{what}: update_columns '{column}' is also a conflict column; a conflict \
                     key cannot be rewritten by its own upsert"
                )));
            }
        }
        if let Some(column) = &self.dedupe_order_column {
            if self.write_mode == WriteMode::ApplyChanges {
                return Err(SaciError::configuration(format!(
                    "{what}: dedupe_order_column cannot be set with write_mode \"apply_changes\"; \
                     the rows apply in the order they arrive"
                )));
            }
            self.check_column("dedupe_order_column", column)?;
        }

        if self.write_mode == WriteMode::ApplyChanges {
            match find_field(&self.schema_fields, "__op") {
                None => {
                    return Err(SaciError::configuration(format!(
                        "{what}: write_mode \"apply_changes\" needs a declared '__op' field of \
                         type \"utf8\" naming each row's change"
                    )));
                }
                Some(op) if op.ty != MariadbFieldType::Utf8 => {
                    return Err(SaciError::configuration(format!(
                        "{what}: reserved column '__op' must be declared type \"utf8\", not \
                         \"{}\"",
                        op.ty.as_str()
                    )));
                }
                Some(_) => {}
            }
            // Every `__` field is read for its meaning and never written, so
            // a key or update column among them would name nothing.
            for (key, columns) in [
                ("conflict_columns", &self.conflict_columns),
                ("update_columns", &self.update_columns),
            ] {
                if let Some(column) = columns.iter().find(|c| c.starts_with("__")) {
                    return Err(SaciError::configuration(format!(
                        "{what}: {key} '{column}' is a reserved __ column, which write_mode \
                         \"apply_changes\" reads and never writes"
                    )));
                }
            }
        }

        Ok(())
    }

    /// Check that `column` of the list `key` names a declared field.
    fn check_column(&self, key: &str, column: &str) -> Result<(), SaciError> {
        if find_field(&self.schema_fields, column).is_none() {
            return Err(SaciError::configuration(format!(
                "{}: {key} '{column}' does not name a declared schema_fields entry",
                Role::Sink.what()
            )));
        }
        Ok(())
    }

    /// The declared fields the sink writes, in declared order: every field,
    /// except under `apply_changes`, where the `__` fields carry the change and
    /// are never columns of the target.
    pub(crate) fn written_fields(&self) -> Vec<&FieldSpec> {
        self.schema_fields
            .iter()
            .filter(|f| !(self.write_mode == WriteMode::ApplyChanges && f.is_reserved()))
            .collect()
    }

    /// The columns the update clause rewrites: `update_columns` when given,
    /// every written column that is not a conflict column otherwise.
    ///
    /// Empty for a table that holds nothing but its key. The sink then
    /// resolves a collision the way `ignore_conflicts` does.
    pub(crate) fn effective_update_columns(&self) -> Vec<&str> {
        if !self.update_columns.is_empty() {
            return self.update_columns.iter().map(String::as_str).collect();
        }
        self.written_fields()
            .into_iter()
            .map(|f| f.name.as_str())
            .filter(|name| !self.conflict_columns.iter().any(|c| c == name))
            .collect()
    }
}

// ------------------------------------------------------------------ defaults

fn default_true() -> bool {
    true
}
fn default_connect_timeout_ms() -> u64 {
    5_000
}
fn default_statement_timeout_ms() -> u64 {
    30_000
}
fn default_reconnect_attempts() -> u32 {
    3
}
fn default_reconnect_base_ms() -> u64 {
    100
}
fn default_reconnect_mult() -> f64 {
    2.0
}
fn default_reconnect_max_ms() -> u64 {
    30_000
}
fn default_reconnect_jitter() -> f64 {
    0.1
}
fn default_batch_rows() -> usize {
    8_192
}
fn default_cursor_initial() -> String {
    "beginning".to_string()
}
fn default_binlog_initial() -> String {
    "now".to_string()
}
fn default_offset_table() -> String {
    "saci_source_offsets".to_string()
}
fn default_max_changes() -> usize {
    10_000
}
fn default_chunk_rows() -> usize {
    1_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;

    fn source(kdl_text: &str) -> Result<MariadbSourceConfig, String> {
        let value = from_kdl_str(kdl_text).map_err(|e| e.to_string())?;
        MariadbSourceConfig::deserialize(value).map_err(|e| e.to_string())
    }

    fn sink(kdl_text: &str) -> Result<MariadbSinkConfig, String> {
        let value = from_kdl_str(kdl_text).map_err(|e| e.to_string())?;
        MariadbSinkConfig::deserialize(value).map_err(|e| e.to_string())
    }

    const POLLING: &str = r#"
name "orders_in"
batch_rows 512
max_batches_per_cycle 4

connection dsn="mysql://reader:pw@db:3306/shop" user="saci" password="secret" connect_timeout_ms=1000 statement_timeout_ms=2000 ssl_mode="disabled" ssl_ca="ca.pem" {
    reconnect max_attempts=5 base_delay_ms=50 multiplier=1.5 max_delay_ms=1000 jitter=0.25
}

mode kind="polling" table="shop.orders" cursor_column="updated_at" tiebreak_column="id" initial="now" offset_table="saci_source_offsets" where_clause="status <> 'draft'"

schema_fields "id" type="int64" nullable=#false
schema_fields "updated_at" type="timestamp_micros" nullable=#false
schema_fields "total" type="decimal128" precision=12 scale=2 mariadb_type="decimal"
"#;

    const TRIGGER_OUTBOX: &str = r#"
name "outbox_in"

connection dsn="mariadb://localhost/shop" ssl_mode="disabled"

mode kind="cdc_trigger" table="outbox" cursor_column="seq" retention="delete_acked" offset_table_autocreate=#false

schema_fields "seq" type="uint64" nullable=#false
schema_fields "__op" type="utf8"
schema_fields "payload" type="json"
"#;

    const TRIGGER_INSTALL: &str = r#"
name "orders-cdc"

connection dsn="mysql://localhost/shop" ssl_mode="disabled"

mode kind="cdc_trigger" table="orders" install_triggers=#true

schema_fields "__op" type="utf8" nullable=#false
schema_fields "__seq" type="uint64" nullable=#false
schema_fields "__commit_ts" type="timestamp_micros_utc"
schema_fields "__table" type="utf8"
schema_fields "id" type="int64" nullable=#false
"#;

    const BINLOG: &str = r#"
name "orders-binlog"

connection dsn="mysql://localhost/shop" ssl_mode="disabled"

mode kind="cdc_binlog" table="shop.orders" server_id=4100 max_changes_per_cycle=100

schema_fields "__op" type="utf8" nullable=#false
schema_fields "__table" type="utf8"
schema_fields "__binlog_file" type="utf8"
schema_fields "__binlog_pos" type="uint64"
schema_fields "__gtid" type="utf8"
schema_fields "__commit_ts" type="timestamp_micros_utc"
schema_fields "id" type="int64" nullable=#false
schema_fields "total" type="float64"
"#;

    const SINK: &str = r#"
name "orders_out"
table "shop.enriched"
write_mode "upsert"
conflict_columns "id"
dedupe_order_column "seq"
chunk_rows 128
flush_rows 1000
truncate_before_first_write #true

connection dsn="mysql://localhost/shop" ssl_mode="disabled"

schema_fields "id" type="int64" nullable=#false
schema_fields "seq" type="int64" nullable=#false
schema_fields "total" type="decimal128" precision=12 scale=2
"#;

    /// The sink as an `apply_changes` consumer of a binlog source.
    const SINK_APPLY: &str = r#"
name "orders_mirror"
table "orders"
write_mode "apply_changes"
conflict_columns "id"

connection dsn="mysql://localhost/shop" ssl_mode="disabled"

schema_fields "__op" type="utf8" nullable=#false
schema_fields "__binlog_pos" type="uint64"
schema_fields "id" type="int64" nullable=#false
schema_fields "total" type="float64"
"#;

    /// Assert `validate` rejected the config as a configuration error naming
    /// `needle`.
    fn rejects(err: SaciError, needle: &str) {
        assert_eq!(err.category(), "configuration", "wrong category: {err}");
        assert!(
            err.message().contains(needle),
            "message {:?} does not name {needle:?}",
            err.message()
        );
    }

    fn invalid_source(text: &str) -> SaciError {
        source(text)
            .expect("the document parses")
            .validate()
            .expect_err("the document must be refused")
    }

    fn invalid_sink(text: &str) -> SaciError {
        sink(text)
            .expect("the document parses")
            .validate()
            .expect_err("the document must be refused")
    }

    // ------------------------------------------------------- whole documents

    #[test]
    fn polling_document_parses_and_validates() {
        let cfg = source(POLLING).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.batch_rows, 512);
        assert_eq!(cfg.max_batches_per_cycle, 4);
        assert_eq!(cfg.connection.reconnect.max_attempts, 5);
        assert_eq!(cfg.connection.ssl_mode, SslModeConfig::Disabled);
        assert_eq!(cfg.connection.ssl_ca.as_deref(), Some("ca.pem"));
        let SourceMode::Polling(cursor) = &cfg.mode else {
            panic!("expected polling mode");
        };
        assert_eq!(cursor.table, "shop.orders");
        assert_eq!(cursor.tiebreak_column.as_deref(), Some("id"));
        assert_eq!(cursor.where_clause.as_deref(), Some("status <> 'draft'"));
        assert!(cursor.offset_table_autocreate);
        assert_eq!(cfg.mode.label(), "polling");
    }

    #[test]
    fn a_connection_with_no_optional_keys_takes_the_documented_defaults() {
        let cfg = sink(&SINK.replace(" ssl_mode=\"disabled\"", "")).expect("parse");
        assert_eq!(cfg.connection.ssl_mode, SslModeConfig::VerifyIdentity);
        assert_eq!(cfg.connection.connect_timeout_ms, 5_000);
        assert_eq!(cfg.connection.statement_timeout_ms, 30_000);
        let reconnect = &cfg.connection.reconnect;
        assert_eq!(reconnect.max_attempts, 3);
        assert_eq!(reconnect.base_delay_ms, 100);
        assert!((reconnect.multiplier - 2.0).abs() < f64::EPSILON);
        assert_eq!(reconnect.max_delay_ms, 30_000);
        assert!((reconnect.jitter - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn trigger_outbox_document_parses_and_validates() {
        let cfg = source(TRIGGER_OUTBOX).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcTrigger(trigger) = &cfg.mode else {
            panic!("expected cdc_trigger mode");
        };
        assert!(!trigger.install_triggers);
        assert_eq!(trigger.effective_retention(), Retention::DeleteAcked);
        assert!(!trigger.offset_table_autocreate);
        assert_eq!(trigger.initial, "beginning");
        assert_eq!(trigger.offset_table, "saci_source_offsets");
        assert_eq!(cfg.mode.label(), "cdc_trigger");
        assert_eq!(cfg.batch_rows, default_batch_rows());
    }

    #[test]
    fn an_outbox_keeps_acknowledged_rows_unless_told_otherwise() {
        let cfg = source(&TRIGGER_OUTBOX.replace(" retention=\"delete_acked\"", "")).unwrap();
        cfg.validate().expect("validate");
        let SourceMode::CdcTrigger(trigger) = &cfg.mode else {
            panic!("expected cdc_trigger mode");
        };
        assert_eq!(trigger.effective_retention(), Retention::Keep);
    }

    #[test]
    fn trigger_install_document_parses_and_validates() {
        let cfg = source(TRIGGER_INSTALL).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcTrigger(trigger) = &cfg.mode else {
            panic!("expected cdc_trigger mode");
        };
        assert!(trigger.install_triggers);
        assert_eq!(trigger.effective_retention(), Retention::DeleteAcked);
        assert_eq!(
            trigger.effective_changelog_table(&cfg.name),
            "saci_cdc_orders_cdc"
        );
    }

    #[test]
    fn an_installed_changelog_may_keep_its_rows_and_name_its_own_table() {
        let cfg = source(&TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            "install_triggers=#true changelog_table=\"audit.orders_log\" retention=\"keep\"",
        ))
        .unwrap();
        cfg.validate().expect("validate");
        let SourceMode::CdcTrigger(trigger) = &cfg.mode else {
            panic!("expected cdc_trigger mode");
        };
        assert_eq!(trigger.effective_retention(), Retention::Keep);
        assert_eq!(
            trigger.effective_changelog_table(&cfg.name),
            "audit.orders_log"
        );
    }

    #[test]
    fn binlog_document_parses_and_validates() {
        let cfg = source(BINLOG).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcBinlog(binlog) = &cfg.mode else {
            panic!("expected cdc_binlog mode");
        };
        assert_eq!(binlog.position, PositionMode::File);
        assert_eq!(binlog.initial, "now");
        assert_eq!(binlog.snapshot, SnapshotMode::Never);
        assert_eq!(binlog.max_changes_per_cycle, 100);
        assert_eq!(binlog.effective_server_id(&cfg.name), 4100);
        assert_eq!(cfg.mode.label(), "cdc_binlog");
    }

    #[test]
    fn the_schema_history_table_defaults_to_one_beside_the_offset_table() {
        let history = |text: &str| {
            let cfg = source(text).expect("parse");
            cfg.validate().expect("validate");
            let SourceMode::CdcBinlog(binlog) = &cfg.mode else {
                panic!("expected cdc_binlog mode");
            };
            assert!(binlog.schema_history);
            binlog.effective_schema_history_table()
        };
        assert_eq!(history(BINLOG), "saci_source_offsets_schema");
        let moved = BINLOG.replace(
            "server_id=4100",
            "server_id=4100 offset_table=\"ops.offsets\"",
        );
        assert_eq!(history(&moved), "ops.offsets_schema");
        let named = BINLOG.replace(
            "server_id=4100",
            "server_id=4100 schema_history_table=\"ops.shape\"",
        );
        assert_eq!(history(&named), "ops.shape");
    }

    #[test]
    fn the_schema_history_can_be_switched_off_and_then_takes_no_table() {
        let off = BINLOG.replace("server_id=4100", "server_id=4100 schema_history=#false");
        let cfg = source(&off).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcBinlog(binlog) = &cfg.mode else {
            panic!("expected cdc_binlog mode");
        };
        assert!(!binlog.schema_history);

        let err = invalid_source(&off.replace(
            "schema_history=#false",
            "schema_history=#false schema_history_table=\"h\"",
        ));
        rejects(
            err,
            "schema_history_table applies with schema_history #true only",
        );
    }

    #[test]
    fn a_bad_or_too_long_schema_history_table_is_refused() {
        let err = invalid_source(&BINLOG.replace(
            "server_id=4100",
            "server_id=4100 schema_history_table=\"a.b.c\"",
        ));
        rejects(err, "mode.schema_history_table");

        // The default name is the offset table's and `_schema`: over the limit
        // for a long offset table, which an explicit name gets around.
        let long = "x".repeat(MAX_IDENTIFIER_CHARS - "_schema".len() + 1);
        let long_offsets = BINLOG.replace(
            "server_id=4100",
            &format!("server_id=4100 offset_table=\"{long}\""),
        );
        rejects(invalid_source(&long_offsets), "mode.schema_history_table");
        let named = long_offsets.replace(
            "offset_table=",
            "schema_history_table=\"hist\" offset_table=",
        );
        source(&named)
            .expect("parse")
            .validate()
            .expect("an explicit name is short enough");
    }

    #[test]
    fn sink_document_parses_and_validates() {
        let cfg = sink(SINK).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.write_mode, WriteMode::Upsert);
        assert_eq!(cfg.chunk_rows, 128);
        assert_eq!(cfg.flush_rows, 1000);
        assert!(cfg.truncate_before_first_write);
        // update_columns defaults to every written non-conflict column.
        assert_eq!(cfg.effective_update_columns(), vec!["seq", "total"]);
    }

    #[test]
    fn a_sink_with_no_optional_keys_takes_the_documented_defaults() {
        let cfg = sink(
            "name \"s\"\ntable \"t\"\nconnection dsn=\"mysql://h/d\" ssl_mode=\"disabled\"\n\
             schema_fields \"id\" type=\"int64\"\n",
        )
        .expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.write_mode, WriteMode::Append);
        assert_eq!(cfg.chunk_rows, 1000);
        assert_eq!(cfg.flush_rows, 0);
        assert!(!cfg.truncate_before_first_write);
    }

    #[test]
    fn apply_changes_document_parses_and_writes_no_reserved_column() {
        let cfg = sink(SINK_APPLY).expect("parse");
        cfg.validate().expect("validate");
        let written: Vec<&str> = cfg
            .written_fields()
            .into_iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(written, vec!["id", "total"]);
        assert_eq!(cfg.effective_update_columns(), vec!["total"]);
    }

    #[test]
    fn every_declared_field_is_written_outside_apply_changes() {
        let cfg = sink(SINK).expect("parse");
        assert_eq!(cfg.written_fields().len(), 3);
        let cfg = sink(&SINK_APPLY.replace("apply_changes", "upsert")).expect("parse");
        cfg.validate().expect("an upsert may write a __ column");
        assert_eq!(cfg.written_fields().len(), 4);
    }

    #[test]
    fn a_repeated_conflict_column_is_refused() {
        let err = invalid_sink(
            &SINK.replace("conflict_columns \"id\"", "conflict_columns \"id\" \"id\""),
        );
        rejects(err, "conflict_columns names 'id' twice");
    }

    // -------------------------------------------------------------- generic

    #[test]
    fn an_unknown_key_is_a_parse_error_naming_it() {
        let err = source(&POLLING.replace("batch_rows 512", "batch_row 512")).unwrap_err();
        assert!(err.contains("batch_row"), "{err}");
    }

    #[test]
    fn retention_on_polling_is_an_unknown_key() {
        let err = source(&POLLING.replace("initial=\"now\"", "initial=\"now\" retention=\"keep\""))
            .unwrap_err();
        assert!(err.contains("retention"), "{err}");
    }

    #[test]
    fn an_unknown_mode_kind_is_a_parse_error_naming_the_choices() {
        let err = source(&POLLING.replace("kind=\"polling\"", "kind=\"cdc_logical\"")).unwrap_err();
        assert!(err.contains("cdc_logical"), "{err}");
        assert!(err.contains("cdc_binlog"), "{err}");
    }

    #[test]
    fn an_empty_name_is_refused() {
        let err = invalid_source(&POLLING.replace("name \"orders_in\"", "name \"\""));
        rejects(err, "'name' must not be empty");
    }

    #[test]
    fn a_name_with_a_sql_metacharacter_is_refused() {
        let err = invalid_source(&POLLING.replace("name \"orders_in\"", "name \"o`rders\""));
        rejects(err, "contains '`'");
    }

    #[test]
    fn a_name_too_long_for_the_offset_table_is_refused() {
        let long = "n".repeat(192);
        let err = invalid_source(&POLLING.replace("orders_in", &long));
        rejects(err, "at most 191");
    }

    #[test]
    fn an_empty_dsn_is_refused() {
        let err =
            invalid_source(&POLLING.replace("dsn=\"mysql://reader:pw@db:3306/shop\"", "dsn=\"\""));
        rejects(err, "connection.dsn must not be empty");
    }

    #[test]
    fn a_dsn_with_another_scheme_is_refused_without_echoing_it() {
        let err = invalid_source(&POLLING.replace(
            "dsn=\"mysql://reader:pw@db:3306/shop\"",
            "dsn=\"postgres://reader:hunter2@db/shop\"",
        ));
        rejects(err.clone(), "'mysql://' or 'mariadb://'");
        assert!(!err.message().contains("hunter2"), "{}", err.message());
    }

    #[test]
    fn both_dsn_schemes_normalise_to_the_one_the_driver_parses() {
        let cfg = sink(SINK).expect("parse");
        let mut connection = cfg.connection;
        for (dsn, expected) in [
            ("mysql://u:p@h:3307/d", Some("mysql://u:p@h:3307/d")),
            ("mariadb://u:p@h:3307/d", Some("mysql://u:p@h:3307/d")),
            ("MariaDB://h/d", Some("mysql://h/d")),
            ("  mysql://h/d  ", Some("mysql://h/d")),
            ("mysqlx://h/d", None),
            ("h:3306/d", None),
        ] {
            connection.dsn = dsn.to_string();
            assert_eq!(connection.normalised_dsn().as_deref(), expected, "{dsn}");
        }
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn a_tls_ssl_mode_without_the_tls_feature_is_refused() {
        for mode in ["required", "verify_ca", "verify_identity"] {
            let err = invalid_source(
                &POLLING.replace("ssl_mode=\"disabled\"", &format!("ssl_mode=\"{mode}\"")),
            );
            rejects(err.clone(), "'tls' feature");
            rejects(err, mode);
        }
    }

    #[cfg(feature = "tls")]
    #[test]
    fn every_ssl_mode_validates_with_the_tls_feature() {
        for mode in ["disabled", "required", "verify_ca", "verify_identity"] {
            source(&POLLING.replace("ssl_mode=\"disabled\"", &format!("ssl_mode=\"{mode}\"")))
                .expect("parse")
                .validate()
                .expect(mode);
        }
    }

    #[test]
    fn an_unknown_ssl_mode_is_a_parse_error() {
        let err =
            source(&POLLING.replace("ssl_mode=\"disabled\"", "ssl_mode=\"prefer\"")).unwrap_err();
        assert!(err.contains("prefer"), "{err}");
    }

    #[test]
    fn a_multiplier_below_one_is_refused() {
        let err = invalid_source(&POLLING.replace("multiplier=1.5", "multiplier=0.5"));
        rejects(err, "multiplier");
    }

    #[test]
    fn a_jitter_out_of_range_is_refused() {
        let err = invalid_source(&POLLING.replace("jitter=0.25", "jitter=1.5"));
        rejects(err, "jitter");
    }

    #[test]
    fn zero_reconnect_attempts_are_refused() {
        let err = invalid_source(&POLLING.replace("max_attempts=5", "max_attempts=0"));
        rejects(err, "max_attempts");
    }

    #[test]
    fn empty_schema_fields_are_refused() {
        // The configuration language has no empty-list literal, so the only
        // way to declare nothing is to clear the parsed list.
        let mut cfg = source(POLLING).unwrap();
        cfg.schema_fields.clear();
        rejects(
            cfg.validate().unwrap_err(),
            "schema_fields must declare at least one field",
        );
    }

    #[test]
    fn a_duplicate_field_name_is_refused() {
        let err = invalid_source(&format!("{POLLING}\nschema_fields \"id\" type=\"int32\"\n"));
        rejects(err, "'id' more than once");
    }

    #[test]
    fn a_decimal_without_precision_is_refused() {
        let err = invalid_source(&POLLING.replace(" precision=12", ""));
        rejects(err, "requires 'precision'");
    }

    #[test]
    fn a_decimal_without_scale_is_refused() {
        let err = invalid_source(&POLLING.replace(" scale=2", ""));
        rejects(err, "requires 'scale'");
    }

    #[test]
    fn a_decimal_precision_out_of_range_is_refused() {
        let err = invalid_source(&POLLING.replace("precision=12", "precision=39"));
        rejects(err, "1..=38");
    }

    #[test]
    fn a_decimal_scale_beyond_its_precision_is_refused() {
        let err = invalid_source(&POLLING.replace("scale=2", "scale=13"));
        rejects(err, "0..=precision (12)");
    }

    #[test]
    fn a_negative_decimal_scale_is_refused() {
        let err = invalid_source(&POLLING.replace("scale=2", "scale=-1"));
        rejects(err, "0..=precision");
    }

    #[test]
    fn precision_on_a_type_other_than_decimal_is_refused() {
        let err = invalid_source(&POLLING.replace(
            "schema_fields \"id\" type=\"int64\"",
            "schema_fields \"id\" type=\"int64\" precision=4",
        ));
        rejects(err, "decimal128");
    }

    #[test]
    fn a_mariadb_type_the_connector_does_not_know_is_refused() {
        let err =
            invalid_source(&POLLING.replace("mariadb_type=\"decimal\"", "mariadb_type=\"vachar\""));
        rejects(err.clone(), "'vachar'");
        rejects(err, "'total'");
    }

    #[test]
    fn a_mariadb_type_names_a_column_type_in_any_case() {
        source(&POLLING.replace("mariadb_type=\"decimal\"", "mariadb_type=\"DECIMAL\""))
            .expect("parse")
            .validate()
            .expect("the assertion ignores case");
    }

    #[test]
    fn batch_rows_zero_is_refused() {
        let err = invalid_source(&POLLING.replace("batch_rows 512", "batch_rows 0"));
        rejects(err, "batch_rows");
    }

    // ---------------------------------------------------------------- tables

    #[test]
    fn a_table_with_too_many_dots_is_refused() {
        let err = invalid_source(&POLLING.replace("table=\"shop.orders\"", "table=\"a.b.c\""));
        rejects(err.clone(), "a.b.c");
        rejects(err, "mode.table");
    }

    #[test]
    fn an_empty_table_part_is_refused() {
        let err = invalid_source(&POLLING.replace("table=\"shop.orders\"", "table=\"shop.\""));
        rejects(err, "empty schema or table part");
    }

    #[test]
    fn a_bad_offset_table_is_refused() {
        let err = invalid_source(&POLLING.replace(
            "offset_table=\"saci_source_offsets\"",
            "offset_table=\"a.b.c\"",
        ));
        rejects(err, "mode.offset_table");
    }

    #[test]
    fn splitting_a_table_keeps_a_bare_name_schema_free() {
        assert_eq!(
            split_qualified("T", "k", "orders").unwrap(),
            (None, "orders")
        );
        assert_eq!(
            split_qualified("T", "k", "shop.orders").unwrap(),
            (Some("shop"), "orders")
        );
    }

    // --------------------------------------------------------------- polling

    #[test]
    fn an_unknown_cursor_column_is_refused() {
        let err = invalid_source(
            &POLLING.replace("cursor_column=\"updated_at\"", "cursor_column=\"nope\""),
        );
        rejects(err, "mode.cursor_column 'nope'");
    }

    #[test]
    fn a_cursor_column_that_cannot_order_is_refused() {
        let err = invalid_source(
            &POLLING.replace("cursor_column=\"updated_at\"", "cursor_column=\"total\""),
        );
        rejects(err, "cannot order a cursor");
    }

    #[test]
    fn a_tiebreak_column_that_cannot_order_is_refused() {
        let err =
            invalid_source(&POLLING.replace("tiebreak_column=\"id\"", "tiebreak_column=\"total\""));
        rejects(err, "mode.tiebreak_column");
    }

    #[test]
    fn a_tiebreak_repeating_the_cursor_is_refused() {
        let err = invalid_source(
            &POLLING.replace("tiebreak_column=\"id\"", "tiebreak_column=\"updated_at\""),
        );
        rejects(err, "repeats mode.cursor_column");
    }

    #[test]
    fn an_empty_where_clause_is_refused() {
        let err = invalid_source(
            &POLLING.replace("where_clause=\"status <> 'draft'\"", "where_clause=\" \""),
        );
        rejects(err, "mode.where_clause must not be empty");
    }

    #[test]
    fn an_empty_initial_is_refused() {
        let err = invalid_source(&POLLING.replace("initial=\"now\"", "initial=\"\""));
        rejects(err, "mode.initial");
    }

    #[test]
    fn a_reserved_field_in_polling_mode_is_refused_with_the_exact_text() {
        let err = invalid_source(&format!("{POLLING}\nschema_fields \"__x\" type=\"utf8\"\n"));
        assert_eq!(
            err.message(),
            "MariadbSource: schema_fields '__x' uses the reserved __ prefix, which only \
             cdc_binlog and an installed cdc_trigger fill"
        );
    }

    #[test]
    fn a_polling_cursor_may_be_any_orderable_type() {
        for ty in [
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
            "utf8",
            "date32",
            "timestamp_micros",
            "timestamp_micros_utc",
        ] {
            let cfg = source(&POLLING.replace(
                "schema_fields \"updated_at\" type=\"timestamp_micros\"",
                &format!("schema_fields \"updated_at\" type=\"{ty}\""),
            ))
            .expect("parse");
            cfg.validate().unwrap_or_else(|e| panic!("{ty}: {e}"));
        }
    }

    // --------------------------------------------------------------- trigger

    #[test]
    fn an_outbox_without_a_cursor_column_is_refused() {
        let err = invalid_source(&TRIGGER_OUTBOX.replace(" cursor_column=\"seq\"", ""));
        rejects(err, "mode.cursor_column is required");
    }

    #[test]
    fn an_outbox_with_a_changelog_table_is_refused() {
        let err = invalid_source(&TRIGGER_OUTBOX.replace(
            "table=\"outbox\"",
            "table=\"outbox\" changelog_table=\"log\"",
        ));
        rejects(
            err,
            "mode.changelog_table applies with install_triggers #true only",
        );
    }

    #[test]
    fn an_outbox_may_declare_the_reserved_op_as_an_ordinary_column() {
        // `TRIGGER_OUTBOX` declares `__op`; it parsed and validated above, and
        // so does any other `__` name, because an outbox is read as plain
        // columns.
        let cfg = source(&format!(
            "{TRIGGER_OUTBOX}\nschema_fields \"__anything\" type=\"int64\"\n"
        ))
        .expect("parse");
        cfg.validate().expect("an outbox column is just a column");
    }

    #[test]
    fn an_outbox_cursor_must_be_declared_and_orderable() {
        let err = invalid_source(
            &TRIGGER_OUTBOX.replace("cursor_column=\"seq\"", "cursor_column=\"payload\""),
        );
        rejects(err, "cannot order a cursor");
    }

    #[test]
    fn an_installed_trigger_refuses_a_cursor_column() {
        let err = invalid_source(&TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            "install_triggers=#true cursor_column=\"id\"",
        ));
        rejects(
            err,
            "mode.cursor_column cannot be set with install_triggers #true",
        );
    }

    #[test]
    fn an_installed_trigger_refuses_a_tiebreak_column() {
        let err = invalid_source(&TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            "install_triggers=#true tiebreak_column=\"id\"",
        ));
        rejects(
            err,
            "mode.tiebreak_column cannot be set with install_triggers #true",
        );
    }

    #[test]
    fn an_installed_trigger_refuses_a_where_clause() {
        let err = invalid_source(&TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            "install_triggers=#true where_clause=\"id > 5\"",
        ));
        rejects(
            err,
            "mode.where_clause cannot be set with install_triggers #true",
        );
    }

    fn trigger_timeout(text: &str) -> Duration {
        let cfg = source(text).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcTrigger(trigger) = cfg.mode else {
            panic!("a cdc_trigger source");
        };
        trigger.gap_timeout()
    }

    #[test]
    fn gap_timeout_ms_defaults_to_ten_seconds_and_accepts_zero_and_the_cap() {
        for text in [TRIGGER_OUTBOX, TRIGGER_INSTALL] {
            assert_eq!(trigger_timeout(text), Duration::from_secs(10));
        }
        let outbox = TRIGGER_OUTBOX.replace(
            "offset_table_autocreate",
            "gap_timeout_ms=2500 offset_table_autocreate",
        );
        assert_eq!(trigger_timeout(&outbox), Duration::from_millis(2500));
        let installed = TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            "install_triggers=#true gap_timeout_ms=0",
        );
        assert!(trigger_timeout(&installed).is_zero());
        let capped = TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            "install_triggers=#true gap_timeout_ms=3600000",
        );
        assert_eq!(
            trigger_timeout(&capped),
            Duration::from_millis(MAX_GAP_TIMEOUT_MS)
        );

        let polling = POLLING.replace(
            "cursor_column=\"updated_at\" tiebreak_column=\"id\"",
            "cursor_column=\"id\" gap_timeout_ms=1200",
        );
        let cfg = source(&polling).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::Polling(cursor) = cfg.mode else {
            panic!("a polling source");
        };
        assert_eq!(cursor.gap_timeout(), Duration::from_millis(1200));
    }

    #[test]
    fn a_gap_timeout_over_an_hour_is_refused() {
        for text in [TRIGGER_OUTBOX, TRIGGER_INSTALL] {
            let text = text
                .replace(
                    "install_triggers=#true",
                    "install_triggers=#true gap_timeout_ms=3600001",
                )
                .replace(
                    "retention=\"delete_acked\"",
                    "retention=\"delete_acked\" gap_timeout_ms=3600001",
                );
            rejects(
                invalid_source(&text),
                "mode.gap_timeout_ms 3600001 is over the 3600000 (one hour)",
            );
        }
    }

    #[test]
    fn a_gap_timeout_on_a_cursor_that_is_not_an_integer_is_refused() {
        for value in ["0", "5000"] {
            let text = POLLING.replace(
                "initial=\"now\"",
                &format!("initial=\"now\" gap_timeout_ms={value}"),
            );
            rejects(
                invalid_source(&text),
                "mode.gap_timeout_ms applies to an integer cursor column, and 'updated_at' is \
                 declared \"timestamp_micros\"",
            );
        }
    }

    #[test]
    fn a_binlog_source_has_no_gap_timeout() {
        let text = BINLOG.replace(
            "mode kind=\"cdc_binlog\"",
            "mode kind=\"cdc_binlog\" gap_timeout_ms=5",
        );
        let err = source(&text).expect_err("an unknown key");
        assert!(err.contains("gap_timeout_ms"), "{err}");
    }

    fn trigger_max_wait(text: &str) -> Option<Duration> {
        let cfg = source(text).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcTrigger(trigger) = cfg.mode else {
            panic!("a cdc_trigger source");
        };
        trigger.gap_max_wait()
    }

    fn installed_with(extra: &str) -> String {
        TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            &format!("install_triggers=#true {extra}"),
        )
    }

    #[test]
    fn gap_max_wait_ms_defaults_to_five_minutes_and_follows_a_longer_timeout() {
        assert_eq!(
            trigger_max_wait(TRIGGER_INSTALL),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            trigger_max_wait(&installed_with("gap_timeout_ms=600000")),
            Some(Duration::from_secs(600)),
            "the limit is never shorter than the fallback timeout"
        );
        assert_eq!(
            trigger_max_wait(&installed_with("gap_max_wait_ms=90000")),
            Some(Duration::from_secs(90))
        );
        assert_eq!(trigger_max_wait(&installed_with("gap_max_wait_ms=0")), None);
        assert_eq!(
            trigger_max_wait(&installed_with("gap_max_wait_ms=86400000")),
            Some(Duration::from_millis(MAX_GAP_MAX_WAIT_MS))
        );
    }

    #[test]
    fn a_gap_max_wait_over_a_day_is_refused() {
        rejects(
            invalid_source(&installed_with("gap_max_wait_ms=86400001")),
            "mode.gap_max_wait_ms 86400001 is over the 86400000 (one day)",
        );
    }

    #[test]
    fn a_gap_max_wait_shorter_than_the_timeout_is_refused() {
        rejects(
            invalid_source(&installed_with("gap_timeout_ms=5000 gap_max_wait_ms=4000")),
            "mode.gap_max_wait_ms 4000 is below mode.gap_timeout_ms 5000",
        );
        rejects(
            invalid_source(&installed_with("gap_max_wait_ms=9000")),
            "mode.gap_max_wait_ms 9000 is below mode.gap_timeout_ms 10000",
        );
    }

    #[test]
    fn a_gap_max_wait_with_the_hold_off_is_refused() {
        rejects(
            invalid_source(&installed_with("gap_timeout_ms=0 gap_max_wait_ms=0")),
            "mode.gap_max_wait_ms has no effect with mode.gap_timeout_ms 0",
        );
    }

    #[test]
    fn a_gap_max_wait_on_a_cursor_that_is_not_an_integer_is_refused() {
        let text = POLLING.replace("initial=\"now\"", "initial=\"now\" gap_max_wait_ms=20000");
        rejects(
            invalid_source(&text),
            "mode.gap_max_wait_ms applies to an integer cursor column, and 'updated_at' is \
             declared \"timestamp_micros\"",
        );
    }

    #[test]
    fn a_default_changelog_name_over_sixty_four_characters_is_refused() {
        let err = invalid_source(&TRIGGER_INSTALL.replace("orders-cdc", &"x".repeat(60)));
        rejects(err.clone(), "mode.changelog_table");
        rejects(err, "shorter name");
    }

    #[test]
    fn a_changelog_name_of_exactly_sixty_four_characters_is_accepted() {
        // `saci_cdc_` is nine characters.
        let name = "x".repeat(MAX_IDENTIFIER_CHARS - "saci_cdc_".len());
        source(&TRIGGER_INSTALL.replace("orders-cdc", &name))
            .expect("parse")
            .validate()
            .expect("sixty four characters fit");
    }

    #[test]
    fn an_installed_trigger_may_declare_only_its_four_reserved_columns() {
        let err = invalid_source(&format!(
            "{TRIGGER_INSTALL}\nschema_fields \"__binlog_pos\" type=\"uint64\"\n"
        ));
        rejects(err.clone(), "'__binlog_pos'");
        rejects(err, "__op, __seq, __commit_ts, __table");
    }

    #[test]
    fn an_installed_trigger_reserved_column_with_the_wrong_type_names_the_required_one() {
        let err = invalid_source(
            &TRIGGER_INSTALL.replace("\"__seq\" type=\"uint64\"", "\"__seq\" type=\"int64\""),
        );
        rejects(err.clone(), "'__seq'");
        rejects(err, "\"uint64\"");
    }

    #[test]
    fn a_trigger_mode_bad_changelog_table_path_is_refused() {
        let err = invalid_source(&TRIGGER_INSTALL.replace(
            "install_triggers=#true",
            "install_triggers=#true changelog_table=\"a.b.c\"",
        ));
        rejects(err, "mode.changelog_table");
    }

    // ---------------------------------------------------------------- binlog

    #[test]
    fn a_binlog_server_id_of_zero_is_refused() {
        let err = invalid_source(&BINLOG.replace("server_id=4100", "server_id=0"));
        rejects(err, "mode.server_id must not be 0");
    }

    #[test]
    fn zero_changes_per_cycle_are_refused() {
        let err =
            invalid_source(&BINLOG.replace("max_changes_per_cycle=100", "max_changes_per_cycle=0"));
        rejects(err, "mode.max_changes_per_cycle");
    }

    #[test]
    fn a_snapshot_with_a_start_other_than_now_is_refused() {
        for initial in ["beginning", "file:binlog.000003:154"] {
            let err = invalid_source(&BINLOG.replace(
                "server_id=4100",
                &format!("server_id=4100 snapshot=\"initial\" initial=\"{initial}\""),
            ));
            rejects(err.clone(), "pins its own start position");
            rejects(err, initial);
        }
    }

    #[test]
    fn a_snapshot_with_the_default_start_is_accepted() {
        let cfg = source(&BINLOG.replace("server_id=4100", "server_id=4100 snapshot=\"initial\""))
            .expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcBinlog(binlog) = &cfg.mode else {
            panic!("expected cdc_binlog mode");
        };
        assert_eq!(binlog.snapshot, SnapshotMode::Initial);
    }

    #[test]
    fn a_file_position_literal_is_accepted_under_position_file() {
        let cfg = source(&BINLOG.replace(
            "server_id=4100",
            "server_id=4100 initial=\"file:mysql-bin.000003:154\"",
        ))
        .expect("parse");
        cfg.validate().expect("validate");
    }

    #[test]
    fn a_malformed_file_position_literal_is_refused() {
        for initial in [
            "mysql-bin.000003",
            "file:mysql-bin.000003",
            "file::154",
            "file:a:x",
        ] {
            let err = invalid_source(&BINLOG.replace(
                "server_id=4100",
                &format!("server_id=4100 initial=\"{initial}\""),
            ));
            rejects(err.clone(), "file:<binlog name>:<position>");
            rejects(err, initial);
        }
    }

    #[test]
    fn a_file_position_literal_under_position_gtid_is_refused() {
        let err = invalid_source(&BINLOG.replace(
            "server_id=4100",
            "server_id=4100 position=\"gtid\" initial=\"file:mysql-bin.000003:154\"",
        ));
        rejects(err, "position is \"gtid\"");
    }

    #[test]
    fn a_gtid_set_literal_is_accepted_under_position_gtid() {
        let cfg = source(&BINLOG.replace(
            "server_id=4100",
            "server_id=4100 position=\"gtid\" \
             initial=\"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7,4f3a3f1a-71ca-11e1-9e33-c80aa9429563:1-9\"",
        ))
        .expect("parse");
        cfg.validate().expect("validate");
    }

    #[test]
    fn a_malformed_gtid_literal_is_refused() {
        for initial in [
            "garbage",
            "3e11fa47-71ca-11e1-9e33-c80aa9429562",
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:x",
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:0",
        ] {
            let err = invalid_source(&BINLOG.replace(
                "server_id=4100",
                &format!("server_id=4100 position=\"gtid\" initial=\"{initial}\""),
            ));
            rejects(err.clone(), "is not a GTID set");
            rejects(err, initial);
        }
    }

    #[test]
    fn now_and_beginning_need_no_parsing_under_either_position() {
        for position in ["file", "gtid"] {
            for initial in ["now", "beginning"] {
                source(&BINLOG.replace(
                    "server_id=4100",
                    &format!("server_id=4100 position=\"{position}\" initial=\"{initial}\""),
                ))
                .expect("parse")
                .validate()
                .unwrap_or_else(|e| panic!("{position} {initial}: {e}"));
            }
        }
    }

    #[test]
    fn a_binlog_reserved_column_with_the_wrong_type_names_the_required_one() {
        let err = invalid_source(&BINLOG.replace(
            "\"__binlog_pos\" type=\"uint64\"",
            "\"__binlog_pos\" type=\"int64\"",
        ));
        rejects(err.clone(), "'__binlog_pos'");
        rejects(err, "\"uint64\"");
    }

    #[test]
    fn an_unknown_binlog_reserved_column_is_refused() {
        let err = invalid_source(&BINLOG.replace("\"__gtid\"", "\"__nope\""));
        rejects(err, "'__nope'");
    }

    #[test]
    fn the_default_server_id_stays_in_range_and_is_stable() {
        for name in ["a", "orders-binlog", "x".repeat(191).as_str(), "", "日本語"] {
            let id = default_server_id(name);
            assert!(
                (4_000_000_000..4_100_000_000).contains(&id),
                "{name:?} gave {id}"
            );
            assert_eq!(id, default_server_id(name));
        }
        assert_ne!(default_server_id("a"), default_server_id("b"));
    }

    #[test]
    fn the_default_server_id_follows_fnv1a() {
        // FNV-1a of the empty string is its offset basis, and of "a" is
        // 0xe40c292c, so both are fixed points of the published algorithm.
        assert_eq!(
            default_server_id(""),
            4_000_000_000 + 2_166_136_261 % 100_000_000
        );
        assert_eq!(
            default_server_id("a"),
            4_000_000_000 + 0xe40c_292c_u32 % 100_000_000
        );
    }

    #[test]
    fn an_absent_server_id_takes_the_default_for_the_name() {
        let cfg = source(&BINLOG.replace(" server_id=4100", "")).expect("parse");
        let SourceMode::CdcBinlog(binlog) = &cfg.mode else {
            panic!("expected cdc_binlog mode");
        };
        assert_eq!(
            binlog.effective_server_id(&cfg.name),
            default_server_id("orders-binlog")
        );
    }

    // ------------------------------------------------------------------ sink

    #[test]
    fn a_sink_table_with_too_many_dots_is_refused() {
        let err = invalid_sink(&SINK.replace("table \"shop.enriched\"", "table \"a.b.c\""));
        rejects(err, "table 'a.b.c'");
    }

    #[test]
    fn a_sink_chunk_rows_of_zero_is_refused() {
        let err = invalid_sink(&SINK.replace("chunk_rows 128", "chunk_rows 0"));
        rejects(err, "chunk_rows");
    }

    #[test]
    fn upsert_without_conflict_columns_is_refused() {
        // The language has no empty-list literal, so an absent key is how a
        // config declares no conflict columns.
        let err = invalid_sink(
            &SINK
                .replace("conflict_columns \"id\"\n", "")
                .replace("dedupe_order_column \"seq\"\n", ""),
        );
        rejects(
            err,
            "write_mode \"upsert\" requires a non-empty conflict_columns",
        );
    }

    #[test]
    fn every_mode_but_append_requires_conflict_columns() {
        for mode in ["ignore_conflicts", "apply_changes"] {
            let err = invalid_sink(
                &SINK_APPLY
                    .replace("apply_changes", mode)
                    .replace("conflict_columns \"id\"\n", ""),
            );
            rejects(err, &format!("write_mode \"{mode}\" requires"));
        }
    }

    #[test]
    fn append_with_conflict_columns_is_refused() {
        let err = invalid_sink(
            &SINK
                .replace("write_mode \"upsert\"", "write_mode \"append\"")
                .replace("dedupe_order_column \"seq\"\n", ""),
        );
        rejects(err, "conflict_columns applies to write_mode");
    }

    #[test]
    fn append_with_update_columns_is_refused() {
        let err = invalid_sink(
            &SINK
                .replace("write_mode \"upsert\"", "write_mode \"append\"")
                .replace("conflict_columns \"id\"\n", "update_columns \"total\"\n")
                .replace("dedupe_order_column \"seq\"\n", ""),
        );
        rejects(err, "update_columns applies to write_mode");
    }

    #[test]
    fn append_with_a_dedupe_order_column_is_refused() {
        let err = invalid_sink(
            &SINK
                .replace("write_mode \"upsert\"", "write_mode \"append\"")
                .replace("conflict_columns \"id\"\n", ""),
        );
        rejects(err, "dedupe_order_column applies to write_mode");
    }

    #[test]
    fn an_unknown_conflict_column_is_refused() {
        let err =
            invalid_sink(&SINK.replace("conflict_columns \"id\"", "conflict_columns \"nope\""));
        rejects(err, "conflict_columns 'nope'");
    }

    #[test]
    fn an_unknown_update_column_is_refused() {
        let err = invalid_sink(&SINK.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"id\"\nupdate_columns \"nope\"",
        ));
        rejects(err, "update_columns 'nope'");
    }

    #[test]
    fn an_update_column_that_is_also_a_conflict_column_is_refused() {
        let err = invalid_sink(&SINK.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"id\"\nupdate_columns \"id\"",
        ));
        rejects(err, "also a conflict column");
    }

    #[test]
    fn an_unknown_dedupe_order_column_is_refused() {
        let err = invalid_sink(&SINK.replace(
            "dedupe_order_column \"seq\"",
            "dedupe_order_column \"nope\"",
        ));
        rejects(err, "dedupe_order_column 'nope'");
    }

    #[test]
    fn ignore_conflicts_with_update_columns_is_refused() {
        let err = invalid_sink(&SINK.replace(
            "write_mode \"upsert\"",
            "write_mode \"ignore_conflicts\"\nupdate_columns \"total\"",
        ));
        rejects(err, "update_columns applies to write_mode");
    }

    #[test]
    fn explicit_update_columns_replace_the_default_set() {
        let cfg = sink(&SINK.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"id\"\nupdate_columns \"total\"",
        ))
        .expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.effective_update_columns(), vec!["total"]);
    }

    #[test]
    fn a_key_only_table_has_no_update_columns() {
        let cfg = sink(
            "name \"s\"\ntable \"t\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"\n\
             connection dsn=\"mysql://h/d\" ssl_mode=\"disabled\"\n\
             schema_fields \"id\" type=\"int64\"\n",
        )
        .expect("parse");
        cfg.validate().expect("validate");
        assert!(cfg.effective_update_columns().is_empty());
    }

    #[test]
    fn apply_changes_without_an_op_field_is_refused() {
        let err = invalid_sink(
            &SINK_APPLY.replace("schema_fields \"__op\" type=\"utf8\" nullable=#false\n", ""),
        );
        rejects(err, "needs a declared '__op' field");
    }

    #[test]
    fn apply_changes_with_an_op_field_of_the_wrong_type_is_refused() {
        let err =
            invalid_sink(&SINK_APPLY.replace("\"__op\" type=\"utf8\"", "\"__op\" type=\"int64\""));
        rejects(err.clone(), "'__op'");
        rejects(err, "\"utf8\"");
    }

    #[test]
    fn apply_changes_with_a_dedupe_order_column_is_refused() {
        let err = invalid_sink(&SINK_APPLY.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"id\"\ndedupe_order_column \"total\"",
        ));
        rejects(
            err,
            "dedupe_order_column cannot be set with write_mode \"apply_changes\"",
        );
    }

    #[test]
    fn apply_changes_refuses_a_reserved_conflict_column() {
        let err = invalid_sink(&SINK_APPLY.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"__binlog_pos\"",
        ));
        rejects(err, "'__binlog_pos' is a reserved __ column");
    }

    #[test]
    fn apply_changes_refuses_a_reserved_update_column() {
        let err = invalid_sink(&SINK_APPLY.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"id\"\nupdate_columns \"__binlog_pos\"",
        ));
        rejects(err, "update_columns '__binlog_pos' is a reserved __ column");
    }

    // ------------------------------------------------------------ type table

    #[test]
    fn every_declared_type_deserialises_from_its_own_spelling() {
        for ty in MariadbFieldType::ALL {
            let parsed =
                MariadbFieldType::deserialize(serde_json::Value::String(ty.as_str().to_string()))
                    .unwrap_or_else(|e| panic!("{}: {e}", ty.as_str()));
            assert_eq!(parsed, ty);
        }
    }

    #[test]
    fn arrow_types_match_declared_types() {
        let cfg = source(POLLING).unwrap();
        let fields: Vec<Field> = cfg
            .schema_fields
            .iter()
            .map(|f| f.to_arrow_field().unwrap())
            .collect();
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert!(!fields[0].is_nullable());
        assert_eq!(
            fields[1].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(fields[2].data_type(), &DataType::Decimal128(12, 2));
        assert!(fields[2].is_nullable());
    }

    #[test]
    fn every_declared_type_maps_to_the_documented_arrow_type() {
        let utc: std::sync::Arc<str> = "UTC".into();
        for (ty, expected) in [
            (MariadbFieldType::Boolean, DataType::Boolean),
            (MariadbFieldType::Int8, DataType::Int8),
            (MariadbFieldType::Int16, DataType::Int16),
            (MariadbFieldType::Int32, DataType::Int32),
            (MariadbFieldType::Int64, DataType::Int64),
            (MariadbFieldType::UInt8, DataType::UInt8),
            (MariadbFieldType::UInt16, DataType::UInt16),
            (MariadbFieldType::UInt32, DataType::UInt32),
            (MariadbFieldType::UInt64, DataType::UInt64),
            (MariadbFieldType::Float32, DataType::Float32),
            (MariadbFieldType::Float64, DataType::Float64),
            (MariadbFieldType::Decimal128, DataType::Decimal128(10, 2)),
            (MariadbFieldType::Utf8, DataType::Utf8),
            (MariadbFieldType::Binary, DataType::Binary),
            (MariadbFieldType::Date32, DataType::Date32),
            (
                MariadbFieldType::Time64Micros,
                DataType::Time64(TimeUnit::Microsecond),
            ),
            (
                MariadbFieldType::TimestampMicros,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                MariadbFieldType::TimestampMicrosUtc,
                DataType::Timestamp(TimeUnit::Microsecond, Some(utc.clone())),
            ),
            (MariadbFieldType::Json, DataType::Utf8),
        ] {
            let decimal = ty == MariadbFieldType::Decimal128;
            let spec = FieldSpec {
                name: "c".to_string(),
                ty,
                nullable: true,
                precision: decimal.then_some(10),
                scale: decimal.then_some(2),
                mariadb_type: None,
            };
            assert_eq!(
                spec.to_arrow_field().expect("arrow field").data_type(),
                &expected,
                "{}",
                ty.as_str()
            );
        }
    }

    #[test]
    fn cursor_capability_follows_the_declared_type() {
        for (ty, capable) in [
            (MariadbFieldType::Int8, true),
            (MariadbFieldType::Int64, true),
            (MariadbFieldType::UInt8, true),
            (MariadbFieldType::UInt64, true),
            (MariadbFieldType::Date32, true),
            (MariadbFieldType::TimestampMicros, true),
            (MariadbFieldType::TimestampMicrosUtc, true),
            (MariadbFieldType::Utf8, true),
            (MariadbFieldType::Float64, false),
            (MariadbFieldType::Decimal128, false),
            (MariadbFieldType::Boolean, false),
            (MariadbFieldType::Binary, false),
            (MariadbFieldType::Json, false),
            (MariadbFieldType::Time64Micros, false),
        ] {
            assert_eq!(ty.is_cursor_capable(), capable, "{}", ty.as_str());
        }
    }

    #[test]
    fn integer_widths_and_signedness_are_reported() {
        assert_eq!(MariadbFieldType::Int8.int_bits(), Some(8));
        assert_eq!(MariadbFieldType::UInt32.int_bits(), Some(32));
        assert_eq!(MariadbFieldType::Utf8.int_bits(), None);
        assert!(MariadbFieldType::UInt16.is_unsigned_int());
        assert!(!MariadbFieldType::Int16.is_unsigned_int());
        assert!(!MariadbFieldType::Boolean.is_unsigned_int());
    }
}
