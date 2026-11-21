//! Serde-derived configuration for the Turso source and sink.
//!
//! Every struct here carries `#[serde(deny_unknown_fields)]`: a key the
//! connector cannot honour is a configuration error, not something to drop
//! silently. The source-mode enum is internally tagged on `kind`, like the
//! `mode` child of the other connectors.
//!
//! Each top-level type exposes `validate`, which the constructors call before
//! they build anything. Validation returns [`SaciError::Configuration`] naming
//! the offending key on the first violation.

use std::collections::HashSet;

use serde::Deserialize;

use crate::connector::schema::{FieldSpec, FieldType};
use crate::error::SaciError;

// ------------------------------------------------------------------ defaults

fn default_true() -> bool {
    true
}

fn default_batch_rows() -> usize {
    8_192
}

fn default_offset_table() -> String {
    "saci_source_offsets".to_string()
}

fn default_cdc_table() -> String {
    "turso_cdc".to_string()
}

fn default_chunk_rows() -> usize {
    65_536
}

fn default_conflict_retries() -> u32 {
    8
}

fn default_capture_mode() -> CaptureMode {
    CaptureMode::Full
}

// ---------------------------------------------------------------- connection

/// How the connector reaches its database.
///
/// `path` alone opens an embedded local database. Adding `remote` turns it into
/// a synced replica: `path` is the local file (Turso keeps a full replica on
/// disk) and `remote` names the Turso Cloud or self-hosted sqld endpoint.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// Local database file, or `":memory:"`. Never logged verbatim.
    pub path: String,
    /// Local at-rest encryption. Embedded connections only.
    #[serde(default)]
    pub encryption: Option<EncryptionConfig>,
    /// Synced-remote endpoint. Absent ⇒ embedded-only.
    #[serde(default)]
    pub remote: Option<RemoteConfig>,
}

/// Local encryption at rest: one cipher plus its hex key.
///
/// Both fields go straight to `turso::EncryptionOpts`, which the engine applies
/// page by page. The key is never logged.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct EncryptionConfig {
    /// Cipher name: `aes128gcm`, `aes256gcm`, `aegis128l`, `aegis128x2`,
    /// `aegis128x4`, `aegis256`, `aegis256x2` or `aegis256x4`.
    pub cipher: String,
    /// Hex-encoded key: 32 digits for a 128-bit cipher, 64 for a 256-bit one.
    pub hexkey: String,
}

/// The ciphers the engine accepts, each with its key length in hex digits.
pub(crate) const CIPHERS: [(&str, usize); 8] = [
    ("aes128gcm", 32),
    ("aes256gcm", 64),
    ("aegis128l", 32),
    ("aegis128x2", 32),
    ("aegis128x4", 32),
    ("aegis256", 64),
    ("aegis256x2", 64),
    ("aegis256x4", 64),
];

/// Refuse an unknown cipher or a key of the wrong length or alphabet.
fn validate_cipher_and_key(what: &str, encryption: &EncryptionConfig) -> Result<(), SaciError> {
    let cipher = encryption.cipher.trim().to_ascii_lowercase();
    let Some((name, digits)) = CIPHERS.iter().find(|(name, _)| *name == cipher) else {
        return Err(SaciError::configuration(format!(
            "{what}: connection.encryption.cipher '{}' is unknown; expected one of {}",
            encryption.cipher,
            CIPHERS
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    };
    let key = encryption.hexkey.trim();
    if key.len() != *digits || !key.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(SaciError::configuration(format!(
            "{what}: connection.encryption.hexkey for '{name}' must be {digits} hex digits"
        )));
    }
    Ok(())
}

/// The remote half of a synced replica.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct RemoteConfig {
    /// `https://`, `http://`, `libsql://` or `turso://` endpoint. The connector
    /// rewrites the last two to `https://`.
    pub url: String,
    /// Bearer token. Never logged.
    pub token: String,
    /// Download the schema and initial data on the first sync of an empty
    /// local replica.
    #[serde(default = "default_true")]
    pub bootstrap_if_empty: bool,
    /// Server-side long-poll budget for a pull, in milliseconds.
    #[serde(default)]
    pub long_poll_timeout_ms: Option<u64>,
    /// Force MVCC logical-log pulls. Absent auto-detects from the first
    /// response; `#true` is the escape hatch for a server that answers
    /// `MVCC incremental pull-updates requires stream_kind=mvcc_logical_log`.
    #[serde(default)]
    pub logical_mvcc_pull: Option<bool>,
}

/// Sync behaviour for the synced half. Ignored for an embedded connection.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SyncConfig {
    /// Pull remote changes before a read cycle. Source only.
    #[serde(default = "default_true")]
    pub pull_before_read: bool,
    /// Push local changes after a flush. Sink only.
    #[serde(default = "default_true")]
    pub push_after_write: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            pull_before_read: true,
            push_after_write: true,
        }
    }
}

// -------------------------------------------------------------------- fields

/// Whether the connector maps a declared type.
///
/// SQLite is dynamically typed, so a declared type is the type the connector
/// coerces each value to; a value that cannot be coerced is a loud error rather
/// than a silent widening. There is deliberately no unsigned type: the engine's
/// only integer is a signed 64-bit one.
fn supported(ty: FieldType) -> bool {
    matches!(
        ty,
        FieldType::Utf8
            | FieldType::Int64
            | FieldType::Float64
            | FieldType::Boolean
            | FieldType::Binary
            | FieldType::Decimal128
    )
}

/// Whether a column of this type can carry a `polling` cursor.
///
/// A cursor must be totally ordered by the comparison SQLite applies to it
/// and by the text form the offset table stores, which rules out booleans,
/// blobs and scaled decimals.
pub(crate) fn is_cursor_capable(ty: FieldType) -> bool {
    matches!(ty, FieldType::Int64 | FieldType::Float64 | FieldType::Utf8)
}

/// The `__`-prefixed fields the `cdc` mode fills from the change stream,
/// with the declared type each requires.
pub(crate) const RESERVED_CDC_FIELDS: [(&str, FieldType); 6] = [
    ("__op", FieldType::Utf8),
    ("__change_id", FieldType::Int64),
    ("__change_time", FieldType::Int64),
    ("__txn_id", FieldType::Int64),
    ("__table", FieldType::Utf8),
    ("__rowid", FieldType::Int64),
];

/// The required type of a reserved CDC field name, if it is one.
pub(crate) fn reserved_cdc_type(name: &str) -> Option<FieldType> {
    RESERVED_CDC_FIELDS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, ty)| *ty)
}

/// Validate a declared field list: non-empty, unique names, each field legal,
/// and `__`-prefixed names legal only in `cdc` mode.
fn validate_fields(what: &str, cdc: bool, fields: &[FieldSpec]) -> Result<(), SaciError> {
    if fields.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: at least one 'field' entry is required"
        )));
    }
    let mut seen = HashSet::new();
    for (i, field) in fields.iter().enumerate() {
        if field.name.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: field[{i}] has an empty 'id'"
            )));
        }
        validate_ident(what, "field id", &field.name)?;
        if !seen.insert(field.name.as_str()) {
            return Err(SaciError::configuration(format!(
                "{what}: field '{}' is declared more than once",
                field.name
            )));
        }
        if !supported(field.ty) {
            return Err(SaciError::configuration(format!(
                "turso: field '{}' has type '{}', which this connector does not map",
                field.name, field.ty
            )));
        }
        if field.native_type.is_some() {
            return Err(SaciError::configuration(format!(
                "turso: field '{}' sets 'native_type', which this connector does not use",
                field.name
            )));
        }
        if field.name.starts_with("__") {
            match (cdc, reserved_cdc_type(&field.name)) {
                (true, Some(required)) if required == field.ty => {}
                (true, Some(required)) => {
                    return Err(SaciError::configuration(format!(
                        "{what}: field '{}' must be declared type \"{}\"",
                        field.name,
                        required.as_str()
                    )));
                }
                _ => {
                    return Err(SaciError::configuration(format!(
                        "{what}: '{}' is a reserved field name, available only in the \
                         \"cdc\" mode",
                        field.name
                    )));
                }
            }
        }
        field.to_arrow()?;
    }
    Ok(())
}

/// Validate an identifier that will reach SQL unquoted.
///
/// Table and connection names are configuration, so the character set is
/// restricted rather than trusted to quoting.
fn validate_ident(what: &str, kind: &str, name: &str) -> Result<(), SaciError> {
    if name.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: {kind} must not be empty"
        )));
    }
    let mut chars = name.chars();
    let first = chars.next().expect("checked non-empty");
    if first.is_ascii_digit() {
        return Err(SaciError::configuration(format!(
            "{what}: {kind} '{name}' must not start with a digit"
        )));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_'))
    {
        return Err(SaciError::configuration(format!(
            "{what}: {kind} '{name}' contains '{bad}'; only ASCII letters, digits and '_' \
             are allowed"
        )));
    }
    Ok(())
}

impl ConnectionConfig {
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.path.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.path must not be empty"
            )));
        }
        if let Some(encryption) = &self.encryption {
            if self.remote.is_some() {
                return Err(SaciError::configuration(format!(
                    "{what}: connection.encryption applies to an embedded database only; a \
                     synced replica takes no local encryption"
                )));
            }
            validate_cipher_and_key(what, encryption)?;
        }
        if let Some(remote) = &self.remote {
            if remote.url.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "{what}: connection.remote.url must not be empty"
                )));
            }
            if remote.token.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "{what}: connection.remote.token must not be empty"
                )));
            }
        }
        Ok(())
    }
}

// -------------------------------------------------------------------- source

/// Everything the Turso source needs.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TursoSourceConfig {
    /// Connector name, used in metrics and error prefixes.
    pub name: String,
    /// Rows per emitted `RecordBatch`.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    /// Batches emitted per drain cycle; 0 drains until caught up.
    #[serde(default)]
    pub max_batches_per_cycle: usize,
    /// How the connector reaches the database.
    pub connection: ConnectionConfig,
    /// The read strategy, chosen by `kind`.
    pub mode: SourceMode,
    /// Sync behaviour for a synced connection.
    #[serde(default)]
    pub sync: SyncConfig,
    /// The declared Arrow schema.
    #[serde(
        default,
        rename = "field",
        deserialize_with = "crate::config::one_or_many"
    )]
    pub fields: Vec<FieldSpec>,
}

/// The read strategy, chosen by `kind`.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceMode {
    /// Incremental read ordered by a cursor column.
    Polling(CursorMode),
    /// Full-table read, re-run every cycle.
    Dump(DumpMode),
    /// The `turso_cdc` change table.
    Cdc(CdcMode),
}

impl SourceMode {
    /// The mode name, for metrics and error messages.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            SourceMode::Polling(_) => "polling",
            SourceMode::Dump(_) => "dump",
            SourceMode::Cdc(_) => "cdc",
        }
    }
}

/// Settings for the `polling` mode.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CursorMode {
    /// The table to read.
    pub table: String,
    /// The ordering column; the offset resumes after its last committed value.
    pub cursor_column: String,
    /// A column that breaks ties within one cursor value, so a batch cut short
    /// mid-value resumes without skipping rows.
    #[serde(default)]
    pub tiebreak_column: Option<String>,
    /// Table the committed cursor is stored in, created on demand.
    #[serde(default = "default_offset_table")]
    pub offset_table: String,
    /// Literal starting cursor value when no offset row exists yet. Absent
    /// means start at the beginning of the table.
    #[serde(default)]
    pub initial: Option<String>,
}

/// Settings for the `dump` mode.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct DumpMode {
    /// The table to read.
    pub table: String,
}

/// Settings for the `cdc` mode.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CdcMode {
    /// The source table whose change records are decoded.
    pub table: String,
    /// The change table itself.
    #[serde(default = "default_cdc_table")]
    pub cdc_table: String,
    /// Table the committed `change_id` is stored in, created on demand.
    #[serde(default = "default_offset_table")]
    pub offset_table: String,
    /// What happens to change rows the source has acknowledged.
    #[serde(default)]
    pub retention: Retention,
}

/// What happens to change rows the source has acknowledged.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    /// Leave them; an operator prunes the change table.
    #[default]
    Keep,
    /// Delete acknowledged rows as the cursor advances. Refused on a synced
    /// connection, whose sync engine consumes the same table.
    DeleteAcked,
}

impl TursoSourceConfig {
    /// Validate every field before anything is built.
    pub fn validate(&self) -> Result<(), SaciError> {
        validate_ident("source", "name", &self.name)?;
        if self.batch_rows == 0 {
            return Err(SaciError::configuration(format!(
                "source '{}': batch_rows must be at least 1",
                self.name
            )));
        }
        self.connection
            .validate(&format!("source '{}'", self.name))?;
        validate_fields(
            &format!("source '{}'", self.name),
            matches!(self.mode, SourceMode::Cdc(_)),
            &self.fields,
        )?;

        match &self.mode {
            SourceMode::Polling(polling) => {
                let what = format!("source '{}'", self.name);
                validate_ident(&what, "table", &polling.table)?;
                validate_ident(&what, "offset_table", &polling.offset_table)?;
                match self.field(&polling.cursor_column) {
                    Some(field) if is_cursor_capable(field.ty) => {}
                    Some(field) => {
                        return Err(SaciError::configuration(format!(
                            "{what}: cursor_column '{}' is \"{}\"; a cursor must be \
                             int64, float64 or utf8",
                            polling.cursor_column,
                            field.ty.as_str()
                        )));
                    }
                    None => {
                        return Err(SaciError::configuration(format!(
                            "{what}: cursor_column '{}' does not name a declared 'field' \
                             entry",
                            polling.cursor_column
                        )));
                    }
                }
                if let Some(tie) = &polling.tiebreak_column {
                    match self.field(tie) {
                        Some(field) if is_cursor_capable(field.ty) => {}
                        Some(field) => {
                            return Err(SaciError::configuration(format!(
                                "{what}: tiebreak_column '{tie}' is \"{}\"; a cursor must be \
                                 int64, float64 or utf8",
                                field.ty.as_str()
                            )));
                        }
                        None => {
                            return Err(SaciError::configuration(format!(
                                "{what}: tiebreak_column '{tie}' does not name a declared \
                                 'field' entry"
                            )));
                        }
                    }
                }
            }
            SourceMode::Dump(dump) => {
                validate_ident(&format!("source '{}'", self.name), "table", &dump.table)?;
            }
            SourceMode::Cdc(cdc) => {
                let what = format!("source '{}'", self.name);
                validate_ident(&what, "table", &cdc.table)?;
                validate_ident(&what, "cdc_table", &cdc.cdc_table)?;
                validate_ident(&what, "offset_table", &cdc.offset_table)?;
                if cdc.retention == Retention::DeleteAcked && self.connection.remote.is_some() {
                    return Err(SaciError::configuration(format!(
                        "{what}: retention \"delete_acked\" is not allowed on a synced \
                         connection: the sync engine consumes the same change table"
                    )));
                }
            }
        }
        Ok(())
    }

    fn field(&self, name: &str) -> Option<&FieldSpec> {
        self.fields.iter().find(|f| f.name == name)
    }
}

// ---------------------------------------------------------------------- sink

/// Everything the Turso sink needs.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TursoSinkConfig {
    /// Connector name, used in metrics and error prefixes.
    pub name: String,
    /// The target table.
    pub table: String,
    /// How the sink resolves a row that collides with an existing one.
    #[serde(default)]
    pub write_mode: WriteMode,
    /// The columns the conflict target is defined on. Required by `upsert` and
    /// `ignore_conflicts`.
    #[serde(default, deserialize_with = "crate::connector::one_or_many")]
    pub conflict_columns: Vec<String>,
    /// Rows per transaction.
    #[serde(default = "default_chunk_rows")]
    pub chunk_rows: usize,
    /// Deletes every existing row in the transaction of the first flush, so a
    /// flush that fails leaves them in place.
    #[serde(default)]
    pub truncate_before_first_write: bool,
    /// How each flush's transaction begins. `concurrent` turns on MVCC.
    #[serde(default)]
    pub transaction: TransactionMode,
    /// Retries after a write-write conflict. `transaction "concurrent"` only.
    #[serde(default = "default_conflict_retries")]
    pub conflict_retries: u32,
    /// How the connector reaches the database.
    pub connection: ConnectionConfig,
    /// Sync behaviour for a synced connection.
    #[serde(default)]
    pub sync: SyncConfig,
    /// Enable change capture on this connection, so the sink's own writes are
    /// recorded in the change table.
    #[serde(default)]
    pub capture: Option<CaptureConfig>,
    /// The declared Arrow schema.
    #[serde(
        default,
        rename = "field",
        deserialize_with = "crate::config::one_or_many"
    )]
    pub fields: Vec<FieldSpec>,
}

/// How the sink resolves a row that collides with an existing one.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Plain `INSERT`.
    #[default]
    Append,
    /// `INSERT … ON CONFLICT (…) DO UPDATE`.
    Upsert,
    /// `INSERT … ON CONFLICT (…) DO NOTHING`.
    IgnoreConflicts,
}

/// How each flush's transaction begins.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransactionMode {
    /// `BEGIN DEFERRED`: no lock until the first statement.
    #[default]
    Deferred,
    /// `BEGIN IMMEDIATE`: take the write lock up front.
    Immediate,
    /// `BEGIN CONCURRENT`: MVCC row-level concurrency, retried on conflict.
    ///
    /// Enabling it sets `journal_mode = 'mvcc'` on the sink's connection, which
    /// the engine makes mutually exclusive with change capture.
    Concurrent,
}

impl TransactionMode {
    /// The `BEGIN` form this mode flushes with.
    pub(crate) fn begin_sql(self) -> &'static str {
        match self {
            TransactionMode::Deferred => "BEGIN DEFERRED",
            TransactionMode::Immediate => "BEGIN IMMEDIATE",
            TransactionMode::Concurrent => "BEGIN CONCURRENT",
        }
    }
}

/// Per-connection change capture, so a CDC consumer can see this sink's writes.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CaptureConfig {
    /// How much of each change is recorded.
    #[serde(default = "default_capture_mode")]
    pub mode: CaptureMode,
    /// Change table to record into. Absent means `turso_cdc`.
    #[serde(default)]
    pub table: Option<String>,
}

/// How much of each change capture records.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMode {
    /// Only the rowid.
    Id,
    /// Row state before updates and deletes.
    Before,
    /// Row state after inserts and updates.
    After,
    /// Both images plus per-column details.
    Full,
}

impl CaptureMode {
    /// The mode token the `capture_data_changes_conn` pragma takes.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CaptureMode::Id => "id",
            CaptureMode::Before => "before",
            CaptureMode::After => "after",
            CaptureMode::Full => "full",
        }
    }
}

impl TursoSinkConfig {
    /// Validate every field before anything is built.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = format!("sink '{}'", self.name);
        validate_ident(&what, "name", &self.name)?;
        validate_ident(&what, "table", &self.table)?;
        if self.chunk_rows == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: chunk_rows must be at least 1"
            )));
        }
        self.connection.validate(&what)?;
        validate_fields(&what, false, &self.fields)?;

        if self.write_mode != WriteMode::Append {
            if self.conflict_columns.is_empty() {
                return Err(SaciError::configuration(format!(
                    "{what}: write_mode \"{}\" requires conflict_columns",
                    match self.write_mode {
                        WriteMode::Upsert => "upsert",
                        WriteMode::IgnoreConflicts => "ignore_conflicts",
                        WriteMode::Append => "append",
                    }
                )));
            }
            for column in &self.conflict_columns {
                if !self.fields.iter().any(|f| &f.name == column) {
                    return Err(SaciError::configuration(format!(
                        "{what}: conflict_columns entry '{column}' does not name a declared \
                         'field' entry"
                    )));
                }
            }
            if self.write_mode == WriteMode::Upsert
                && self
                    .fields
                    .iter()
                    .all(|f| self.conflict_columns.contains(&f.name))
            {
                return Err(SaciError::configuration(format!(
                    "{what}: write_mode \"upsert\" needs at least one declared field outside \
                     conflict_columns to update"
                )));
            }
        }
        if self.transaction == TransactionMode::Concurrent && self.capture.is_some() {
            return Err(SaciError::configuration(format!(
                "{what}: transaction \"concurrent\" needs MVCC, which the engine makes \
                 mutually exclusive with change capture"
            )));
        }
        if let Some(capture) = &self.capture
            && let Some(table) = &capture.table
        {
            validate_ident(&what, "capture.table", table)?;
        }
        Ok(())
    }
}
