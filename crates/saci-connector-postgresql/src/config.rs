//! Serde-derived configuration for the PostgreSQL source and sink.
//!
//! Every struct here carries `#[serde(deny_unknown_fields)]`: a key the
//! connector cannot honour is a configuration error, not something to drop
//! silently. The mode enum is internally tagged on `kind`, matching the
//! `run_mode` table the service config already uses.
//!
//! Each top-level type exposes `validate`, which the constructors in
//! [`crate::source`](crate) and [`crate::sink`](crate) call before they build
//! anything. Validation returns [`SaciError::Configuration`] naming the
//! offending key on the first violation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use saci_core::error::SaciError;
use serde::Deserialize;

use crate::types::{PgTypeRef, resolve_pg_type, validate_forced};

/// Arrow timestamp/time resolution every temporal type in this connector uses.
///
/// PostgreSQL stores `timestamp`, `timestamptz` and `time` as microseconds
/// since its own epoch, so microseconds is the only resolution that neither
/// loses precision nor pads zeros.
const UNIT: TimeUnit = TimeUnit::Microsecond;

/// Field names the `cdc_logical` mode fills from the change stream rather than
/// from the row.
pub(crate) const RESERVED_FIELDS: [&str; 5] = ["__op", "__lsn", "__xid", "__commit_ts", "__table"];

// ---------------------------------------------------------------- connection

/// Shared connection settings for both the source and the sink.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// libpq connection URI or keyword string, parsed by
    /// `tokio_postgres::Config::from_str`.
    ///
    /// Never logged. Errors name the target as `host:port/dbname` instead.
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
    /// `application_name` the session reports, visible in `pg_stat_activity`.
    #[serde(default)]
    pub application_name: Option<String>,
    /// TCP + startup handshake budget.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Applied as `SET statement_timeout` on every new session. 0 disables.
    #[serde(default = "default_statement_timeout_ms")]
    pub statement_timeout_ms: u64,
    /// Whether the connection is encrypted.
    #[serde(default)]
    pub sslmode: SslModeConfig,
    /// PEM bundle of trusted roots. Absent means the OS trust store.
    #[serde(default)]
    pub sslrootcert: Option<String>,
    /// Backoff applied when a connection cannot be established.
    #[serde(default)]
    pub reconnect: ReconnectConfig,
}

/// How hard the connector insists on TLS.
///
/// There is no unverified-TLS setting: hostname verification is always on.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SslModeConfig {
    /// Never negotiate TLS.
    Disable,
    /// Negotiate TLS, fall back to plaintext if the server refuses.
    #[default]
    Prefer,
    /// Refuse to connect without TLS. Needs the `tls` feature.
    Require,
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
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.dsn.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.dsn must not be empty"
            )));
        }
        #[cfg(not(feature = "tls"))]
        if self.sslmode == SslModeConfig::Require {
            return Err(SaciError::configuration(format!(
                "{what}: connection.sslmode = \"require\" needs the 'tls' feature of \
                 saci-connector-postgresql, which is not enabled in this build"
            )));
        }
        self.reconnect.validate(what)
    }
}

// -------------------------------------------------------------------- fields

/// Which half of the connector a declared field belongs to.
///
/// The two halves differ in one rule: a sink narrows an integer width with a
/// per-value range check, and a source never widens on read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// [`PostgresSourceConfig`].
    Source,
    /// [`PostgresSinkConfig`].
    Sink,
}

/// One declared column: its name, its Postgres-facing type, and nullability.
///
/// Flat rather than an internally tagged enum, so `deny_unknown_fields` still
/// applies; `#[serde(flatten)]` would switch it off.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    /// Column name. Matched against the server by name, never by position.
    ///
    /// Read from `id`, because `saci-config` puts a node's leading argument —
    /// the `"total"` in `schema_fields "total" type="float64"` — under that
    /// key.
    #[serde(rename = "id")]
    pub name: String,
    /// Declared type. Checked against the column's Postgres OID on connect.
    #[serde(rename = "type")]
    pub ty: PgFieldType,
    /// Whether the Arrow field admits NULL.
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
    /// Typed as the same enum, so `item = "list"` parses; it is refused with a
    /// message naming the column, because Arrow's `List` here is
    /// one-dimensional.
    #[serde(default)]
    pub item: Option<PgFieldType>,
    /// The PostgreSQL type this column really has, forcing the codec.
    ///
    /// Accepts every `Type::name()`, the SQL aliases, both array spellings and
    /// a modifier anywhere after the type name: `int`, `bigint`, `double
    /// precision`, `timestamp with time zone`, `text[]`, `_text`,
    /// `numeric(12,2)`, `timestamp(3) with time zone`, `public.mood`. A quoted
    /// part keeps its case and takes no alias, so `public."Mood"` names a
    /// mixed-case enum and `"char"` the one-byte internal type. On a source it
    /// asserts the server's own type and picks the decoder; on a sink it picks
    /// the parameter type the value is encoded to. It never changes the Arrow
    /// type: `type` alone decides that.
    #[serde(default)]
    pub pg_type: Option<String>,
}

/// The declared type of a column.
///
/// Each maps to exactly one Arrow [`DataType`]. Which PostgreSQL types can
/// fill it is the canonical table in [`crate::types`](crate); a type with no
/// Arrow counterpart reaches `utf8` in PostgreSQL's canonical text form.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PgFieldType {
    /// `bool` to Arrow `Boolean`.
    Boolean,
    /// `int2` to Arrow `Int16`.
    Int16,
    /// `int4` to Arrow `Int32`.
    Int32,
    /// `int8` to Arrow `Int64`.
    Int64,
    /// `float4` to Arrow `Float32`.
    Float32,
    /// `float8` to Arrow `Float64`.
    Float64,
    /// `text`/`varchar`/`bpchar`/`name` to Arrow `Utf8`.
    Utf8,
    /// `bytea` to Arrow `Binary`.
    Binary,
    /// `date` to Arrow `Date32`.
    Date32,
    /// `time` to Arrow `Time64(Microsecond)`.
    Time64Micros,
    /// `timestamp` to Arrow `Timestamp(Microsecond, None)`.
    TimestampMicros,
    /// `timestamptz` to Arrow `Timestamp(Microsecond, Some("UTC"))`.
    TimestampMicrosUtc,
    /// `uuid` to Arrow `FixedSizeBinary(16)`.
    Uuid,
    /// `json`/`jsonb` to Arrow `Utf8`, carrying the document text.
    Json,
    /// `numeric` to Arrow `Decimal128`. Needs `precision` and `scale`.
    Decimal128,
    /// `interval` to Arrow `Interval(MonthDayNano)`.
    ///
    /// The three components stay separate because PostgreSQL does not treat
    /// them as interchangeable: `'1 month'` is not `'30 days'`.
    IntervalMonthDayNano,
    /// A one-dimensional PostgreSQL array to Arrow `List`. Needs `item`.
    List,
}

impl PgFieldType {
    /// Whether a column of this type can carry a cursor.
    ///
    /// A cursor must be totally ordered by the same comparison Postgres and the
    /// persisted text form agree on, which rules out floats, booleans, blobs and
    /// documents.
    pub(crate) fn is_cursor_capable(self) -> bool {
        matches!(
            self,
            PgFieldType::Int16
                | PgFieldType::Int32
                | PgFieldType::Int64
                | PgFieldType::Utf8
                | PgFieldType::Date32
                | PgFieldType::TimestampMicros
                | PgFieldType::TimestampMicrosUtc
        )
    }

    /// The SQL cast applied to a bound cursor parameter, so a `text` offset
    /// column compares against a typed table column.
    pub(crate) fn sql_cast(self) -> &'static str {
        match self {
            PgFieldType::Int16 => "int2",
            PgFieldType::Int32 => "int4",
            PgFieldType::Int64 => "int8",
            PgFieldType::Date32 => "date",
            PgFieldType::TimestampMicros => "timestamp",
            PgFieldType::TimestampMicrosUtc => "timestamptz",
            _ => "text",
        }
    }

    /// The name used in error messages, matching the configured spelling.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            PgFieldType::Boolean => "boolean",
            PgFieldType::Int16 => "int16",
            PgFieldType::Int32 => "int32",
            PgFieldType::Int64 => "int64",
            PgFieldType::Float32 => "float32",
            PgFieldType::Float64 => "float64",
            PgFieldType::Utf8 => "utf8",
            PgFieldType::Binary => "binary",
            PgFieldType::Date32 => "date32",
            PgFieldType::Time64Micros => "time64_micros",
            PgFieldType::TimestampMicros => "timestamp_micros",
            PgFieldType::TimestampMicrosUtc => "timestamp_micros_utc",
            PgFieldType::Uuid => "uuid",
            PgFieldType::Json => "json",
            PgFieldType::Decimal128 => "decimal128",
            PgFieldType::IntervalMonthDayNano => "interval_month_day_nano",
            PgFieldType::List => "list",
        }
    }
}

impl FieldSpec {
    /// The Arrow field this spec declares.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `decimal128` is missing
    /// `precision`/`scale` or carries an out-of-range precision, when
    /// `precision`/`scale` are set on a type that carries no decimal, when
    /// `item` is set on a type other than `list`, when `list` carries no
    /// `item`, and when `item` is itself `"list"` -- Arrow's `List` here is
    /// one-dimensional.
    pub fn to_arrow_field(&self) -> Result<Field, SaciError> {
        if self.item.is_some() && self.ty != PgFieldType::List {
            return Err(SaciError::configuration(format!(
                "field '{}': 'item' applies to type \"list\" only, not \"{}\"",
                self.name,
                self.ty.as_str()
            )));
        }
        let carries_decimal = self.ty == PgFieldType::Decimal128
            || (self.ty == PgFieldType::List && self.item == Some(PgFieldType::Decimal128));
        if !carries_decimal && (self.precision.is_some() || self.scale.is_some()) {
            return Err(SaciError::configuration(format!(
                "field '{}': precision/scale apply to type \"decimal128\" only, not \"{}\"",
                self.name,
                self.ty.as_str()
            )));
        }

        let data_type = match self.ty {
            PgFieldType::List => {
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
    fn scalar(&self, ty: PgFieldType) -> Result<DataType, SaciError> {
        Ok(match ty {
            PgFieldType::Boolean => DataType::Boolean,
            PgFieldType::Int16 => DataType::Int16,
            PgFieldType::Int32 => DataType::Int32,
            PgFieldType::Int64 => DataType::Int64,
            PgFieldType::Float32 => DataType::Float32,
            PgFieldType::Float64 => DataType::Float64,
            PgFieldType::Utf8 | PgFieldType::Json => DataType::Utf8,
            PgFieldType::Binary => DataType::Binary,
            PgFieldType::Date32 => DataType::Date32,
            PgFieldType::Time64Micros => DataType::Time64(UNIT),
            PgFieldType::TimestampMicros => DataType::Timestamp(UNIT, None),
            PgFieldType::TimestampMicrosUtc => DataType::Timestamp(UNIT, Some("UTC".into())),
            PgFieldType::Uuid => DataType::FixedSizeBinary(16),
            PgFieldType::IntervalMonthDayNano => DataType::Interval(IntervalUnit::MonthDayNano),
            PgFieldType::Decimal128 => {
                let (precision, scale) = self.decimal_params()?;
                DataType::Decimal128(precision, scale)
            }
            PgFieldType::List => {
                return Err(SaciError::configuration(format!(
                    "field '{}': 'item' must be a scalar type, not \"list\"; Arrow's List is \
                     one-dimensional",
                    self.name
                )));
            }
        })
    }

    /// The parsed `pg_type`, when the field carries one.
    ///
    /// # Errors
    ///
    /// Returns the reason the spelling is not a PostgreSQL type name.
    pub(crate) fn forced_pg_type(&self) -> Result<Option<PgTypeRef>, String> {
        self.pg_type.as_deref().map(resolve_pg_type).transpose()
    }

    /// The declared `(precision, scale)` of a `decimal128` field.
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
}

/// Validate a declared field list: non-empty, unique names, each field legal.
fn validate_fields(what: &str, role: Role, fields: &[FieldSpec]) -> Result<(), SaciError> {
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

        let forced = field.forced_pg_type().map_err(|reason| {
            SaciError::configuration(format!("{what}: field '{}': {reason}", field.name))
        })?;
        if let Some(forced) = &forced {
            validate_forced(field, forced, role).map_err(|reason| {
                SaciError::configuration(format!("{what}: field '{}': {reason}", field.name))
            })?;
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
            "{what}: {key} = '{column}' does not name a declared schema_fields entry"
        ))
    })?;
    if !field.ty.is_cursor_capable() {
        return Err(SaciError::configuration(format!(
            "{what}: {key} = '{column}' has type \"{}\", which cannot order a cursor; use one of \
             int16, int32, int64, utf8, date32, timestamp_micros, timestamp_micros_utc",
            field.ty.as_str()
        )));
    }
    Ok(())
}

/// Validate a connector name: it reaches a SQL identifier through the staging
/// table, so the character set is restricted rather than trusted to quoting.
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
            "{what}: 'name' = '{name}' contains '{bad}'; only ASCII letters, digits, '_' and '-' \
             are allowed"
        )));
    }
    Ok(())
}

/// Split `schema.table`, defaulting the schema to `public`.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] on more than one `.`, or an empty part.
pub(crate) fn split_qualified(what: &str, table: &str) -> Result<(String, String), SaciError> {
    let mut parts = table.split('.');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() {
        return Err(SaciError::configuration(format!(
            "{what}: table '{table}' must be 'table' or 'schema.table', not a longer path"
        )));
    }
    let (schema, name) = match second {
        Some(name) => (first, name),
        None => ("public", first),
    };
    if schema.is_empty() || name.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: table '{table}' has an empty schema or table part"
        )));
    }
    Ok((schema.to_string(), name.to_string()))
}

// -------------------------------------------------------------------- source

/// Everything the PostgreSQL source needs.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    /// Stable identity for this source.
    ///
    /// `SourceFactory::build` receives only the `config` sub-table, never the
    /// `[[sources]]` `name`, so the name is declared here too. It keys the
    /// persisted offset row, labels every metric, and prefixes every error.
    pub name: String,
    /// Where and how to connect.
    pub connection: ConnectionConfig,
    /// Which read strategy to use.
    pub mode: SourceMode,
    /// The Arrow schema every emitted batch conforms to.
    #[serde(deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<FieldSpec>,
    /// Rows per emitted `RecordBatch`, and the `LIMIT` on each cursor query.
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
    /// Bounded long-poll on a LISTEN channel. Rejected for `cdc_logical`.
    #[serde(default)]
    pub notify: Option<NotifyConfig>,
}

/// The read strategy, chosen by `kind`.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceMode {
    /// Incremental query over a live table.
    Polling(CursorMode),
    /// The same mechanics over an append-only table a trigger writes, plus
    /// optional pruning of acknowledged rows.
    CdcTrigger(CursorMode),
    /// `pgoutput` logical decoding through the SQL slot interface.
    CdcLogical(LogicalMode),
}

impl SourceMode {
    /// The metric label and error prefix for this mode.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            SourceMode::Polling(_) => "polling",
            SourceMode::CdcTrigger(_) => "cdc_trigger",
            SourceMode::CdcLogical(_) => "cdc_logical",
        }
    }
}

/// Settings shared by the `polling` and `cdc_trigger` modes.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CursorMode {
    /// `schema.table`, or bare `table` meaning `public.table`.
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
    #[serde(default = "default_initial")]
    pub initial: String,
    /// Table holding one durable cursor row per source name.
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
    /// `cdc_trigger` only: whether to prune acknowledged outbox rows.
    #[serde(default)]
    pub retention: Retention,
}

/// What happens to outbox rows the source has acknowledged.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    /// Leave them in place.
    #[default]
    Keep,
    /// `DELETE` them when the offset is committed.
    DeleteAcked,
}

/// Settings for the `cdc_logical` mode.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct LogicalMode {
    /// Logical replication slot name.
    pub slot: String,
    /// Publication whose relations the slot decodes.
    pub publication: String,
    /// The one `schema.table` this source emits. Changes for other relations in
    /// the publication are skipped and counted.
    pub table: String,
    /// Whether to create the slot when it does not exist.
    #[serde(default = "default_true")]
    pub slot_autocreate: bool,
    /// `upto_nchanges` for each peek.
    #[serde(default = "default_max_changes")]
    pub max_changes_per_cycle: i32,
}

/// Bounded long-poll on a `LISTEN` channel.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct NotifyConfig {
    /// Channel passed to `LISTEN`.
    pub channel: String,
    /// How long to wait for a notification before ending the drain cycle.
    ///
    /// Must be at least 1: an unbounded wait would stop the source ever
    /// reaching EOF, which the batch runners cannot drive.
    #[serde(default = "default_notify_timeout_ms")]
    pub timeout_ms: u64,
}

impl PostgresSourceConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = "PostgresSource";
        validate_name(what, &self.name)?;
        self.connection.validate(what)?;
        validate_fields(what, Role::Source, &self.schema_fields)?;

        if self.batch_rows == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: batch_rows must be at least 1"
            )));
        }

        if let Some(notify) = &self.notify {
            if matches!(self.mode, SourceMode::CdcLogical(_)) {
                return Err(SaciError::configuration(format!(
                    "{what}: notify is not supported with mode kind = \"cdc_logical\"; the slot \
                     interface has no notification channel"
                )));
            }
            if notify.channel.is_empty() {
                return Err(SaciError::configuration(format!(
                    "{what}: notify.channel must not be empty"
                )));
            }
            if notify.timeout_ms < 1 {
                return Err(SaciError::configuration(format!(
                    "{what}: notify.timeout_ms must be at least 1; an unbounded wait would stop \
                     the source ever reaching EOF"
                )));
            }
        }

        match &self.mode {
            SourceMode::Polling(cursor) => {
                self.validate_cursor(what, cursor)?;
                if cursor.retention != Retention::Keep {
                    return Err(SaciError::configuration(format!(
                        "{what}: retention = \"delete_acked\" applies to kind = \"cdc_trigger\" \
                         only, not \"polling\": deleting rows from a live table would destroy data"
                    )));
                }
                self.reject_reserved_fields(what, "polling")?;
            }
            SourceMode::CdcTrigger(cursor) => {
                self.validate_cursor(what, cursor)?;
                self.reject_reserved_fields(what, "cdc_trigger")?;
            }
            SourceMode::CdcLogical(logical) => {
                if logical.slot.is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.slot must not be empty"
                    )));
                }
                if logical.publication.is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.publication must not be empty"
                    )));
                }
                split_qualified(what, &logical.table)?;
                if logical.max_changes_per_cycle < 1 {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.max_changes_per_cycle must be at least 1"
                    )));
                }
                self.validate_reserved_fields(what)?;
            }
        }

        Ok(())
    }

    fn validate_cursor(&self, what: &str, cursor: &CursorMode) -> Result<(), SaciError> {
        split_qualified(what, &cursor.table)?;
        split_qualified(what, &cursor.offset_table)?;
        validate_cursor_field(
            what,
            "mode.cursor_column",
            &self.schema_fields,
            &cursor.cursor_column,
        )?;
        if let Some(tiebreak) = &cursor.tiebreak_column {
            validate_cursor_field(what, "mode.tiebreak_column", &self.schema_fields, tiebreak)?;
            if *tiebreak == cursor.cursor_column {
                return Err(SaciError::configuration(format!(
                    "{what}: mode.tiebreak_column = '{tiebreak}' repeats mode.cursor_column"
                )));
            }
        }
        if cursor.initial.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: mode.initial must be \"beginning\", \"now\", or a literal cursor value"
            )));
        }
        Ok(())
    }

    /// Reserved names belong to `cdc_logical`; no other mode can fill them.
    fn reject_reserved_fields(&self, what: &str, kind: &str) -> Result<(), SaciError> {
        if let Some(field) = self.schema_fields.iter().find(|f| f.name.starts_with("__")) {
            return Err(SaciError::configuration(format!(
                "{what}: schema_fields '{}' uses the reserved '__' prefix, which only \
                 kind = \"cdc_logical\" fills, not \"{kind}\"",
                field.name
            )));
        }
        Ok(())
    }

    /// Every `__`-prefixed field must be one of the five the decoder fills, and
    /// must carry the type that metadata column has.
    fn validate_reserved_fields(&self, what: &str) -> Result<(), SaciError> {
        for field in self
            .schema_fields
            .iter()
            .filter(|f| f.name.starts_with("__"))
        {
            let expected = match field.name.as_str() {
                "__op" | "__table" => PgFieldType::Utf8,
                "__lsn" | "__xid" => PgFieldType::Int64,
                "__commit_ts" => PgFieldType::TimestampMicrosUtc,
                other => {
                    return Err(SaciError::configuration(format!(
                        "{what}: schema_fields '{other}' is not a reserved metadata column; the \
                         '__' prefix is reserved, and the known names are {}",
                        RESERVED_FIELDS.join(", ")
                    )));
                }
            };
            if field.ty != expected {
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
}

// ---------------------------------------------------------------------- sink

/// Everything the PostgreSQL sink needs.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkConfig {
    /// Stable identity for this sink; see [`PostgresSourceConfig::name`].
    ///
    /// It labels every metric, prefixes every error, and suffixes the staging
    /// table name in the upsert path.
    pub name: String,
    /// Where and how to connect.
    pub connection: ConnectionConfig,
    /// `schema.table`, or bare `table` meaning `public.table`.
    pub table: String,
    /// The Arrow schema every accepted batch must match.
    #[serde(deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<FieldSpec>,
    /// How rows reach the target table.
    #[serde(default)]
    pub write_mode: WriteMode,
    /// Conflict key. Required for `upsert` and `ignore_conflicts`.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub conflict_columns: Vec<String>,
    /// `upsert` only. Empty means every declared column except the conflict
    /// columns.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub update_columns: Vec<String>,
    /// De-duplicate staged rows with
    /// `DISTINCT ON (conflict_columns) … ORDER BY conflict_columns, <col> DESC`.
    ///
    /// Without it, duplicate keys inside one batch make Postgres raise
    /// "ON CONFLICT DO UPDATE command cannot affect row a second time".
    #[serde(default)]
    pub dedupe_order_column: Option<String>,
    /// Rows per COPY chunk inside one transaction.
    #[serde(default = "default_chunk_rows")]
    pub chunk_rows: usize,
    /// Buffer batches until this many rows accumulate. 0 flushes on every
    /// `write_batch`, which is one transaction per pipeline iteration.
    #[serde(default)]
    pub flush_rows: usize,
    /// `TRUNCATE` the target inside the first flush's transaction.
    #[serde(default)]
    pub truncate_before_first_write: bool,
}

/// How the sink resolves a row that collides with an existing one.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// `COPY` straight into the target. A conflict is an error.
    #[default]
    Append,
    /// Stage, then `INSERT … ON CONFLICT DO UPDATE`.
    Upsert,
    /// Stage, then `INSERT … ON CONFLICT DO NOTHING`.
    IgnoreConflicts,
}

impl PostgresSinkConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = "PostgresSink";
        validate_name(what, &self.name)?;
        self.connection.validate(what)?;
        validate_fields(what, Role::Sink, &self.schema_fields)?;
        split_qualified(what, &self.table)?;

        if self.chunk_rows == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: chunk_rows must be at least 1"
            )));
        }

        match self.write_mode {
            WriteMode::Append => {
                if !self.conflict_columns.is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: conflict_columns applies to write_mode \"upsert\" or \
                         \"ignore_conflicts\", not \"append\""
                    )));
                }
                if !self.update_columns.is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: update_columns applies to write_mode \"upsert\", not \"append\""
                    )));
                }
                if self.dedupe_order_column.is_some() {
                    return Err(SaciError::configuration(format!(
                        "{what}: dedupe_order_column applies to write_mode \"upsert\" or \
                         \"ignore_conflicts\", not \"append\""
                    )));
                }
            }
            WriteMode::Upsert | WriteMode::IgnoreConflicts => {
                if self.conflict_columns.is_empty() {
                    return Err(SaciError::configuration(format!(
                        "{what}: write_mode \"{}\" requires a non-empty conflict_columns",
                        if self.write_mode == WriteMode::Upsert {
                            "upsert"
                        } else {
                            "ignore_conflicts"
                        }
                    )));
                }
                for column in &self.conflict_columns {
                    if find_field(&self.schema_fields, column).is_none() {
                        return Err(SaciError::configuration(format!(
                            "{what}: conflict_columns '{column}' does not name a declared \
                             schema_fields entry"
                        )));
                    }
                }
                if self.write_mode == WriteMode::IgnoreConflicts && !self.update_columns.is_empty()
                {
                    return Err(SaciError::configuration(format!(
                        "{what}: update_columns applies to write_mode \"upsert\", not \
                         \"ignore_conflicts\""
                    )));
                }
                for column in &self.update_columns {
                    if find_field(&self.schema_fields, column).is_none() {
                        return Err(SaciError::configuration(format!(
                            "{what}: update_columns '{column}' does not name a declared \
                             schema_fields entry"
                        )));
                    }
                    if self.conflict_columns.iter().any(|c| c == column) {
                        return Err(SaciError::configuration(format!(
                            "{what}: update_columns '{column}' is also a conflict column; a \
                             conflict key cannot be rewritten by its own upsert"
                        )));
                    }
                }
                if let Some(column) = &self.dedupe_order_column
                    && find_field(&self.schema_fields, column).is_none()
                {
                    return Err(SaciError::configuration(format!(
                        "{what}: dedupe_order_column '{column}' does not name a declared \
                         schema_fields entry"
                    )));
                }
            }
        }

        Ok(())
    }

    /// The columns `DO UPDATE SET` rewrites: `update_columns` when given, every
    /// declared non-conflict column otherwise.
    pub(crate) fn effective_update_columns(&self) -> Vec<&str> {
        if !self.update_columns.is_empty() {
            return self.update_columns.iter().map(String::as_str).collect();
        }
        self.schema_fields
            .iter()
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
fn default_initial() -> String {
    "beginning".to_string()
}
fn default_offset_table() -> String {
    "saci_source_offsets".to_string()
}
fn default_max_changes() -> i32 {
    10_000
}
fn default_notify_timeout_ms() -> u64 {
    30_000
}
fn default_chunk_rows() -> usize {
    65_536
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;

    fn source(kdl_text: &str) -> Result<PostgresSourceConfig, String> {
        let value = from_kdl_str(kdl_text).map_err(|e| e.to_string())?;
        PostgresSourceConfig::deserialize(value).map_err(|e| e.to_string())
    }

    fn sink(kdl_text: &str) -> Result<PostgresSinkConfig, String> {
        let value = from_kdl_str(kdl_text).map_err(|e| e.to_string())?;
        PostgresSinkConfig::deserialize(value).map_err(|e| e.to_string())
    }

    const POLLING: &str = r#"
name "pg_prices"
batch_rows 512
max_batches_per_cycle 4

connection dsn="postgres://localhost/app" user="saci" password="secret" application_name="saci-prices" connect_timeout_ms=1000 statement_timeout_ms=2000 sslmode="prefer" {
    reconnect max_attempts=5 base_delay_ms=50 multiplier=1.5 max_delay_ms=1000 jitter=0.25
}

mode kind="polling" table="market.prices" cursor_column="updated_at" tiebreak_column="id" initial="now" offset_table="saci_source_offsets" where_clause="symbol IS NOT NULL"

notify channel="prices_changed" timeout_ms=2500

schema_fields "id" type="int64" nullable=#false
schema_fields "updated_at" type="timestamp_micros_utc" nullable=#false
schema_fields "value" type="decimal128" precision=18 scale=4
"#;

    const LOGICAL: &str = r#"
name "pg_orders"

connection dsn="postgres://localhost/app"

mode kind="cdc_logical" slot="saci_orders_slot" publication="saci_orders_pub" table="public.orders" max_changes_per_cycle=100

schema_fields "__op" type="utf8" nullable=#false
schema_fields "__lsn" type="int64" nullable=#false
schema_fields "__commit_ts" type="timestamp_micros_utc"
schema_fields "id" type="int64" nullable=#false
"#;

    const TRIGGER: &str = r#"
name "pg_outbox"

connection dsn="postgres://localhost/app"

mode kind="cdc_trigger" table="outbox" cursor_column="seq" retention="delete_acked" offset_table_autocreate=#false

schema_fields "seq" type="int64" nullable=#false
schema_fields "payload" type="json"
"#;

    const SINK: &str = r#"
name "pg_enriched"
table "public.enriched"
write_mode "upsert"
conflict_columns "id"
dedupe_order_column "seq"
chunk_rows 128
flush_rows 1000
truncate_before_first_write #true

connection dsn="postgres://localhost/app"

schema_fields "id" type="int64" nullable=#false
schema_fields "seq" type="int64" nullable=#false
schema_fields "total" type="decimal128" precision=12 scale=2
"#;

    #[test]
    fn polling_document_parses_and_validates() {
        let cfg = source(POLLING).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.batch_rows, 512);
        assert_eq!(cfg.max_batches_per_cycle, 4);
        assert_eq!(cfg.connection.reconnect.max_attempts, 5);
        assert_eq!(cfg.notify.as_ref().unwrap().timeout_ms, 2500);
        let SourceMode::Polling(cursor) = &cfg.mode else {
            panic!("expected polling mode");
        };
        assert_eq!(cursor.table, "market.prices");
        assert_eq!(cursor.tiebreak_column.as_deref(), Some("id"));
        assert_eq!(cursor.where_clause.as_deref(), Some("symbol IS NOT NULL"));
        assert!(cursor.offset_table_autocreate);
        assert_eq!(cursor.retention, Retention::Keep);
    }

    #[test]
    fn logical_document_parses_and_validates() {
        let cfg = source(LOGICAL).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.batch_rows, default_batch_rows());
        let SourceMode::CdcLogical(logical) = &cfg.mode else {
            panic!("expected cdc_logical mode");
        };
        assert_eq!(logical.slot, "saci_orders_slot");
        assert!(logical.slot_autocreate);
        assert_eq!(logical.max_changes_per_cycle, 100);
    }

    #[test]
    fn trigger_document_parses_and_validates() {
        let cfg = source(TRIGGER).expect("parse");
        cfg.validate().expect("validate");
        let SourceMode::CdcTrigger(cursor) = &cfg.mode else {
            panic!("expected cdc_trigger mode");
        };
        assert_eq!(cursor.retention, Retention::DeleteAcked);
        assert!(!cursor.offset_table_autocreate);
        assert_eq!(cursor.initial, "beginning");
        assert_eq!(cursor.offset_table, "saci_source_offsets");
    }

    #[test]
    fn sink_document_parses_and_validates() {
        let cfg = sink(SINK).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.write_mode, WriteMode::Upsert);
        assert_eq!(cfg.chunk_rows, 128);
        assert_eq!(cfg.flush_rows, 1000);
        assert!(cfg.truncate_before_first_write);
        // update_columns defaults to every declared non-conflict column.
        assert_eq!(cfg.effective_update_columns(), vec!["seq", "total"]);
    }

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

    #[test]
    fn unknown_key_is_a_parse_error() {
        let err = source(&POLLING.replace("batch_rows 512", "batch_row 512")).unwrap_err();
        assert!(err.contains("batch_row"), "{err}");
    }

    #[test]
    fn empty_name_rejected() {
        let cfg = source(&POLLING.replace("name \"pg_prices\"", "name \"\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "'name' must not be empty");
    }

    #[test]
    fn name_with_sql_metacharacter_rejected() {
        let cfg = source(&POLLING.replace("name \"pg_prices\"", "name \"pg\\\"prices\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "contains '\"'");
    }

    #[test]
    fn empty_dsn_rejected() {
        let cfg = source(&POLLING.replace("dsn=\"postgres://localhost/app\"", "dsn=\"\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "connection.dsn");
    }

    #[test]
    fn empty_schema_fields_rejected() {
        // The configuration language has no empty-list literal, so the only
        // way to declare nothing is to clear the parsed list; an empty list
        // must fail validation, not parsing.
        let text = "name \"s\"\n\nconnection dsn=\"postgres://h/d\"\n\n\
             mode kind=\"polling\" table=\"t\" cursor_column=\"id\"\n\n\
             schema_fields \"id\" type=\"int64\"\n";
        let mut cfg = source(text).unwrap();
        cfg.schema_fields.clear();
        rejects(
            cfg.validate().unwrap_err(),
            "schema_fields must declare at least one field",
        );
    }

    #[test]
    fn duplicate_field_name_rejected() {
        let cfg = source(&format!("{POLLING}\nschema_fields \"id\" type=\"int32\"\n")).unwrap();
        rejects(cfg.validate().unwrap_err(), "'id' more than once");
    }

    #[test]
    fn decimal_without_precision_rejected() {
        let cfg = source(&POLLING.replace(" precision=18", "")).unwrap();
        rejects(cfg.validate().unwrap_err(), "requires 'precision'");
    }

    #[test]
    fn decimal_without_scale_rejected() {
        let cfg = source(&POLLING.replace(" scale=4", "")).unwrap();
        rejects(cfg.validate().unwrap_err(), "requires 'scale'");
    }

    #[test]
    fn decimal_precision_out_of_range_rejected() {
        let cfg = source(&POLLING.replace("precision=18", "precision=39")).unwrap();
        rejects(cfg.validate().unwrap_err(), "1..=38");
    }

    #[test]
    fn precision_on_non_decimal_rejected() {
        let cfg = source(&POLLING.replace(
            "schema_fields \"id\" type=\"int64\"",
            "schema_fields \"id\" type=\"int64\" precision=4",
        ))
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "decimal128");
    }

    #[test]
    fn batch_rows_zero_rejected() {
        let cfg = source(&POLLING.replace("batch_rows 512", "batch_rows 0")).unwrap();
        rejects(cfg.validate().unwrap_err(), "batch_rows");
    }

    #[test]
    fn unknown_cursor_column_rejected() {
        let cfg =
            source(&POLLING.replace("cursor_column=\"updated_at\"", "cursor_column=\"nope\""))
                .unwrap();
        rejects(cfg.validate().unwrap_err(), "mode.cursor_column = 'nope'");
    }

    #[test]
    fn non_orderable_cursor_column_rejected() {
        let cfg =
            source(&POLLING.replace("cursor_column=\"updated_at\"", "cursor_column=\"value\""))
                .unwrap();
        rejects(cfg.validate().unwrap_err(), "cannot order a cursor");
    }

    #[test]
    fn non_orderable_tiebreak_column_rejected() {
        let cfg = source(&POLLING.replace("tiebreak_column=\"id\"", "tiebreak_column=\"value\""))
            .unwrap();
        rejects(cfg.validate().unwrap_err(), "mode.tiebreak_column");
    }

    #[test]
    fn tiebreak_repeating_cursor_rejected() {
        let cfg =
            source(&POLLING.replace("tiebreak_column=\"id\"", "tiebreak_column=\"updated_at\""))
                .unwrap();
        rejects(cfg.validate().unwrap_err(), "repeats mode.cursor_column");
    }

    #[test]
    fn delete_acked_with_polling_rejected() {
        let cfg = source(&POLLING.replace(
            "initial=\"now\"",
            "initial=\"now\" retention=\"delete_acked\"",
        ))
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "cdc_trigger");
    }

    #[test]
    fn notify_with_cdc_logical_rejected() {
        let cfg = source(&format!("{LOGICAL}\nnotify channel=\"c\" timeout_ms=100\n")).unwrap();
        rejects(cfg.validate().unwrap_err(), "cdc_logical");
    }

    #[test]
    fn notify_timeout_zero_rejected() {
        let cfg = source(&POLLING.replace("timeout_ms=2500", "timeout_ms=0")).unwrap();
        rejects(cfg.validate().unwrap_err(), "notify.timeout_ms");
    }

    #[test]
    fn max_changes_zero_rejected() {
        let cfg = source(&LOGICAL.replace("max_changes_per_cycle=100", "max_changes_per_cycle=0"))
            .unwrap();
        rejects(cfg.validate().unwrap_err(), "max_changes_per_cycle");
    }

    #[test]
    fn multiplier_below_one_rejected() {
        let cfg = source(&POLLING.replace("multiplier=1.5", "multiplier=0.5")).unwrap();
        rejects(cfg.validate().unwrap_err(), "multiplier");
    }

    #[test]
    fn jitter_out_of_range_rejected() {
        let cfg = source(&POLLING.replace("jitter=0.25", "jitter=1.5")).unwrap();
        rejects(cfg.validate().unwrap_err(), "jitter");
    }

    #[test]
    fn reserved_field_outside_cdc_logical_rejected() {
        let cfg = source(&format!(
            "{POLLING}\nschema_fields \"__op\" type=\"utf8\"\n"
        ))
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "__op");
    }

    #[test]
    fn unknown_reserved_field_rejected() {
        let cfg = source(&LOGICAL.replace("\"__lsn\"", "\"__nope\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "__nope");
    }

    #[test]
    fn reserved_field_wrong_type_rejected() {
        let cfg = source(&LOGICAL.replace("\"__lsn\" type=\"int64\"", "\"__lsn\" type=\"utf8\""))
            .unwrap();
        rejects(cfg.validate().unwrap_err(), "__lsn");
    }

    #[test]
    fn too_many_dots_in_table_rejected() {
        let cfg = source(&POLLING.replace("table=\"market.prices\"", "table=\"a.b.c\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "a.b.c");
    }

    #[test]
    fn empty_table_part_rejected() {
        let cfg = source(&POLLING.replace("table=\"market.prices\"", "table=\"market.\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "empty schema or table part");
    }

    #[test]
    fn sink_chunk_rows_zero_rejected() {
        let cfg = sink(&SINK.replace("chunk_rows 128", "chunk_rows 0")).unwrap();
        rejects(cfg.validate().unwrap_err(), "chunk_rows");
    }

    #[test]
    fn upsert_without_conflict_columns_rejected() {
        // The language has no empty-list literal, so an absent key is how a
        // config declares no conflict columns.
        let cfg = sink(
            &SINK
                .replace("conflict_columns \"id\"\n", "")
                .replace("dedupe_order_column \"seq\"\n", ""),
        )
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "conflict_columns");
    }

    #[test]
    fn append_with_conflict_columns_rejected() {
        let cfg = sink(
            &SINK
                .replace("write_mode \"upsert\"", "write_mode \"append\"")
                .replace("dedupe_order_column \"seq\"\n", ""),
        )
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "conflict_columns");
    }

    #[test]
    fn append_with_dedupe_order_column_rejected() {
        let cfg = sink(
            &SINK
                .replace("write_mode \"upsert\"", "write_mode \"append\"")
                .replace("conflict_columns \"id\"\n", ""),
        )
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "dedupe_order_column");
    }

    #[test]
    fn unknown_conflict_column_rejected() {
        let cfg =
            sink(&SINK.replace("conflict_columns \"id\"", "conflict_columns \"nope\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "conflict_columns 'nope'");
    }

    #[test]
    fn unknown_update_column_rejected() {
        let cfg = sink(&SINK.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"id\"\nupdate_columns \"nope\"",
        ))
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "update_columns 'nope'");
    }

    #[test]
    fn update_column_intersecting_conflict_columns_rejected() {
        let cfg = sink(&SINK.replace(
            "conflict_columns \"id\"",
            "conflict_columns \"id\"\nupdate_columns \"id\"",
        ))
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "also a conflict column");
    }

    #[test]
    fn unknown_dedupe_order_column_rejected() {
        let cfg = sink(&SINK.replace(
            "dedupe_order_column \"seq\"",
            "dedupe_order_column \"nope\"",
        ))
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "dedupe_order_column 'nope'");
    }

    #[test]
    fn ignore_conflicts_with_update_columns_rejected() {
        let cfg = sink(&SINK.replace(
            "write_mode \"upsert\"",
            "write_mode \"ignore_conflicts\"\nupdate_columns \"total\"",
        ))
        .unwrap();
        rejects(cfg.validate().unwrap_err(), "update_columns");
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn sslmode_require_without_tls_feature_rejected() {
        let cfg = source(&POLLING.replace("sslmode=\"prefer\"", "sslmode=\"require\"")).unwrap();
        rejects(cfg.validate().unwrap_err(), "'tls' feature");
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
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(fields[2].data_type(), &DataType::Decimal128(18, 4));
        assert!(fields[2].is_nullable());
    }

    #[test]
    fn split_qualified_defaults_schema_to_public() {
        assert_eq!(
            split_qualified("T", "orders").unwrap(),
            ("public".to_string(), "orders".to_string())
        );
        assert_eq!(
            split_qualified("T", "sales.orders").unwrap(),
            ("sales".to_string(), "orders".to_string())
        );
    }

    #[test]
    fn cursor_casts_match_declared_types() {
        assert_eq!(PgFieldType::Int64.sql_cast(), "int8");
        assert_eq!(PgFieldType::TimestampMicrosUtc.sql_cast(), "timestamptz");
        assert_eq!(PgFieldType::Utf8.sql_cast(), "text");
        assert!(!PgFieldType::Float64.is_cursor_capable());
        assert!(!PgFieldType::Uuid.is_cursor_capable());
    }

    #[test]
    fn a_list_field_maps_to_an_arrow_list() {
        let cfg = source(&POLLING.replace(
            "schema_fields \"value\" type=\"decimal128\" precision=18 scale=4",
            "schema_fields \"tags\" type=\"list\" item=\"utf8\" pg_type=\"text[]\"",
        ))
        .expect("parse");
        cfg.validate().expect("validate");
        let field = cfg.schema_fields[2].to_arrow_field().expect("arrow field");
        assert_eq!(
            field.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
        );
    }

    #[test]
    fn an_interval_field_maps_to_month_day_nano() {
        let spec = FieldSpec {
            name: "span".to_string(),
            ty: PgFieldType::IntervalMonthDayNano,
            nullable: true,
            precision: None,
            scale: None,
            item: None,
            pg_type: None,
        };
        assert_eq!(
            spec.to_arrow_field().expect("arrow field").data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert!(!spec.ty.is_cursor_capable());
        assert_eq!(spec.ty.sql_cast(), "text");
    }

    #[test]
    fn a_decimal_list_item_takes_the_precision_and_scale() {
        let cfg = sink(&SINK.replace(
            "schema_fields \"total\" type=\"decimal128\" precision=12 scale=2",
            "schema_fields \"total\" type=\"list\" item=\"decimal128\" precision=12 scale=2",
        ))
        .expect("parse");
        cfg.validate().expect("validate");
        let field = cfg.schema_fields[2].to_arrow_field().expect("arrow field");
        assert_eq!(
            field.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(
                DataType::Decimal128(12, 2),
                true
            )))
        );
    }

    #[test]
    fn the_list_keys_must_agree_with_the_declared_type() {
        let cases = [
            (
                "schema_fields \"tags\" type=\"utf8\" item=\"utf8\"",
                "'item' applies to type \"list\" only",
            ),
            ("schema_fields \"tags\" type=\"list\"", "requires 'item'"),
            (
                "schema_fields \"tags\" type=\"list\" item=\"list\"",
                "must be a scalar type",
            ),
            (
                "schema_fields \"tags\" type=\"utf8\" precision=4",
                "precision/scale apply to type \"decimal128\" only",
            ),
        ];
        for (fields, fragment) in cases {
            let cfg = source(&POLLING.replace(
                "schema_fields \"value\" type=\"decimal128\" precision=18 scale=4",
                fields,
            ))
            .expect("parse");
            let err = cfg.validate().expect_err(fields);
            assert!(err.message().contains(fragment), "{fields}: {err}");
            assert!(err.message().contains("'tags'"), "{fields}: {err}");
        }
    }

    #[test]
    fn a_forced_type_the_declared_type_cannot_carry_is_a_load_time_error() {
        let cfg = source(&POLLING.replace(
            "schema_fields \"value\" type=\"decimal128\" precision=18 scale=4",
            "schema_fields \"value\" type=\"float64\" pg_type=\"float4\"",
        ))
        .expect("parse");
        let err = cfg
            .validate()
            .expect_err("float64 into float4 loses precision");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'value'"), "{err}");
        assert!(err.message().contains("float32"), "{err}");
    }

    #[test]
    fn a_sink_may_force_a_narrower_integer_than_a_source_may() {
        let narrow = "schema_fields \"seq\" type=\"int64\" pg_type=\"int2\"";
        let cfg = sink(&SINK.replace(
            "schema_fields \"seq\" type=\"int64\" nullable=#false",
            narrow,
        ))
        .expect("parse");
        cfg.validate()
            .expect("a sink range-checks the narrowing per value");

        let cfg = source(&POLLING.replace(
            "schema_fields \"value\" type=\"decimal128\" precision=18 scale=4",
            "schema_fields \"value\" type=\"int64\" pg_type=\"int2\"",
        ))
        .expect("parse");
        let err = cfg.validate().expect_err("a source never widens on read");
        assert!(err.message().contains("int16"), "{err}");
    }

    #[test]
    fn an_unparsable_forced_type_names_the_column() {
        let cfg = source(&POLLING.replace(
            "schema_fields \"value\" type=\"decimal128\" precision=18 scale=4",
            "schema_fields \"value\" type=\"utf8\" pg_type=\"int4[][]\"",
        ))
        .expect("parse");
        let err = cfg
            .validate()
            .expect_err("two dimensions are not supported");
        assert!(err.message().contains("'value'"), "{err}");
        assert!(err.message().contains("one-dimensional"), "{err}");
    }

    #[test]
    fn a_forced_type_needing_the_catalog_parses_at_load_time() {
        let cfg = source(&POLLING.replace(
            "schema_fields \"value\" type=\"decimal128\" precision=18 scale=4",
            "schema_fields \"mood\" type=\"utf8\" pg_type=\"public.mood\"",
        ))
        .expect("parse");
        cfg.validate()
            .expect("an enum is resolved when the connection opens");
    }

    #[test]
    fn an_unsigned_declared_type_is_still_not_a_postgres_type() {
        let err = source(&POLLING.replace(
            "schema_fields \"value\" type=\"decimal128\" precision=18 scale=4",
            "schema_fields \"value\" type=\"uint64\"",
        ))
        .expect_err("PgFieldType has no unsigned variant");
        assert!(err.contains("unknown variant `uint64`"), "{err}");
    }
}
