//! One connection per connector instance, plus the SQL-identifier quoting every
//! statement in this crate goes through.
//!
//! There is no pool. Both [`Source`](saci_core::io::source::Source) and
//! [`Sink`](saci_core::io::sink::Sink) serialise all access through `&mut self`,
//! so a second connection would sit idle. The one exception is the binlog
//! stream, which consumes its connection and so opens a second one beside the
//! catalog and offset connection.
//!
//! [`Connector`] is built synchronously, because `SourceFactory::build` is
//! synchronous: it parses the DSN, reads the password file and the CA bundle,
//! and builds the TLS options, but opens no socket. [`Connector::connect`] is
//! the async half, called on the first `next_batch` or `write_batch` and again
//! after a connection error that [`mysql_async::Error::is_fatal`] reports.
//!
//! It also owns the catalog lookups both halves share: the columns of a table
//! ([`columns_of`]) and its primary and unique keys ([`primary_key_of`],
//! [`unique_keys_of`]), read from `information_schema`.
//!
//! # Redaction
//!
//! [`Connector::target`] is `host:port/db` and is the **only** form of the
//! connection details that may appear in an error, a log line or a metric. The
//! DSN, the user and the password are never interpolated anywhere in this crate.

use std::fmt;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Params, Row};
use saci_core::error::SaciError;

use crate::config::{ConnectionConfig, ReconnectConfig, Role, SslModeConfig, split_qualified};
use crate::types::CatalogColumn;

/// `ER_ACCESS_DENIED_ERROR`: the user may not log in or may not run the
/// command.
pub(crate) const ER_ACCESS_DENIED_ERROR: u16 = 1045;
/// `ER_NO_SUCH_TABLE`.
pub(crate) const ER_NO_SUCH_TABLE: u16 = 1146;
/// `ER_SPECIFIC_ACCESS_DENIED_ERROR`: the user lacks one named privilege.
pub(crate) const ER_SPECIFIC_ACCESS_DENIED_ERROR: u16 = 1227;
/// `ER_MASTER_FATAL_ERROR_READING_BINLOG`: the requested binlog position is
/// not on the server.
pub(crate) const ER_MASTER_FATAL_ERROR_READING_BINLOG: u16 = 1236;
/// `ER_TRG_ALREADY_EXISTS`.
pub(crate) const ER_TRG_ALREADY_EXISTS: u16 = 1359;
/// `ER_BINLOG_CREATE_ROUTINE_NEED_SUPER`: with binary logging on, creating a
/// trigger needs `SUPER` or `log_bin_trust_function_creators`.
pub(crate) const ER_BINLOG_CREATE_ROUTINE_NEED_SUPER: u16 = 1419;

/// The session mode a sink pins, so an out-of-range value or a truncation is an
/// error and never a silent clamp.
const SINK_SQL_MODE: &str = "STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
     ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

/// Which server software answered, which decides the SQL a few statements use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flavor {
    /// MariaDB.
    Mariadb,
    /// Oracle MySQL.
    Mysql,
}

impl Flavor {
    /// The flavour a `SELECT VERSION()` banner names. MariaDB reports
    /// `11.8.2-MariaDB-ubu2404`; MySQL reports a bare `8.4.5`.
    pub(crate) fn from_banner(banner: &str) -> Self {
        if banner.to_ascii_lowercase().contains("mariadb") {
            Flavor::Mariadb
        } else {
            Flavor::Mysql
        }
    }

    /// The product name, for messages.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Flavor::Mariadb => "MariaDB",
            Flavor::Mysql => "MySQL",
        }
    }
}

/// A live session: the connection, which server answered, and its version.
pub(crate) struct Session {
    /// The connection. Statements run on it directly.
    pub(crate) conn: Conn,
    /// Which server software answered.
    pub(crate) flavor: Flavor,
    /// The server's `(major, minor, patch)`.
    pub(crate) version: (u16, u16, u16),
}

impl Session {
    /// `MariaDB 11.8.2` or `MySQL 8.4.5`, for messages.
    pub(crate) fn describe(&self) -> String {
        describe_version(self.flavor, self.version)
    }
}

/// `MariaDB 11.8.2` or `MySQL 8.4.5`.
fn describe_version(flavor: Flavor, (major, minor, patch): (u16, u16, u16)) -> String {
    format!("{} {major}.{minor}.{patch}", flavor.name())
}

/// A table with its schema resolved, so no statement depends on which database
/// the session happens to be in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRef {
    /// The schema, which MySQL calls a database.
    pub(crate) schema: String,
    /// The table name.
    pub(crate) name: String,
}

impl TableRef {
    /// `` `schema`.`table` ``, ready to interpolate.
    pub(crate) fn quoted(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// `schema.table`, unquoted, for messages.
impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// Everything needed to open a session, resolved once and reused.
pub(crate) struct Connector {
    opts: Opts,
    /// The database the DSN names, which a bare table name belongs to.
    database: Option<String>,
    /// `host:port/db`. The only form of the target that may be logged.
    target: String,
    connect_timeout: Duration,
    reconnect: ReconnectConfig,
    statement_timeout_ms: u64,
    role: Role,
}

impl Connector {
    /// Parse and validate everything that can be checked without a server.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the DSN does not parse, when
    /// `password_file` or `ssl_ca` cannot be read, or when `ssl_mode` needs TLS
    /// this build lacks. The message carries the parse error, never the DSN.
    pub(crate) fn new(role: Role, cfg: &ConnectionConfig) -> Result<Self, SaciError> {
        let what = role.what();
        let dsn = cfg.normalised_dsn().ok_or_else(|| {
            SaciError::configuration(format!(
                "{what}: connection.dsn must be a 'mysql://' or 'mariadb://' URL"
            ))
        })?;
        let parsed = Opts::from_url(&dsn).map_err(|e| {
            SaciError::configuration(format!("{what}: cannot parse connection.dsn: {e}"))
        })?;
        let database = parsed.db_name().map(str::to_string);
        let target = format!(
            "{}:{}/{}",
            parsed.ip_or_hostname(),
            parsed.tcp_port(),
            database.as_deref().unwrap_or("?")
        );

        let password = match &cfg.password_file {
            Some(path) => {
                let secret = std::fs::read_to_string(path).map_err(|e| {
                    SaciError::configuration(format!(
                        "{what}: cannot read connection.password_file '{path}': {e}"
                    ))
                })?;
                Some(secret.trim().to_string())
            }
            None => cfg.password.clone(),
        };

        // `from_opts` panics on a value that does not parse; `parsed` did.
        let mut builder = OptsBuilder::from_opts(parsed);
        if let Some(user) = &cfg.user {
            builder = builder.user(Some(user.as_str()));
        }
        if let Some(password) = &password {
            builder = builder.pass(Some(password.as_str()));
        }
        // TCP only, so TLS applies to every connection and a local socket never
        // bypasses it. A source keeps a small statement cache, which also closes
        // the statements `mysql_async` would otherwise leak; the cursor reader
        // and the offset store prepare and close their per-batch statements
        // themselves, so a table redefined under them is never read through a
        // cached handle. A sink keeps none and owns its statement handles (it
        // prepares and closes them inside each flush): MySQL 8.4 and 9 reprepare
        // a cached statement against a redefined table with stale parameter
        // state, and an `INSERT` then stores wrong values without an error.
        let builder = builder
            .ssl_opts(ssl_opts(what, cfg)?)
            .prefer_socket(false)
            .stmt_cache_size(if role == Role::Sink { 0usize } else { 32usize });

        Ok(Self {
            opts: Opts::from(builder),
            database,
            target,
            connect_timeout: Duration::from_millis(cfg.connect_timeout_ms),
            reconnect: cfg.reconnect.clone(),
            statement_timeout_ms: cfg.statement_timeout_ms,
            role,
        })
    }

    /// `host:port/db`, the redacted form used in every message.
    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    /// Resolve a configured `schema.table` or bare `table`, `key` naming the
    /// configuration key it came from.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for a malformed name, and for a
    /// bare name when the DSN names no database to put it in.
    pub(crate) fn resolve_table(&self, key: &str, table: &str) -> Result<TableRef, SaciError> {
        let what = self.role.what();
        let (schema, name) = split_qualified(what, key, table)?;
        let schema = match schema {
            Some(schema) => schema.to_string(),
            None => self.database.clone().ok_or_else(|| {
                SaciError::configuration(format!(
                    "{what}: connection.dsn names no database, so {key} '{table}' must be \
                     written schema.table"
                ))
            })?,
        };
        Ok(TableRef {
            schema,
            name: name.to_string(),
        })
    }

    /// Open one session, pin its settings and learn which server answered.
    ///
    /// The session runs at `+00:00` in `utf8mb4`, so a `TIMESTAMP` reads and
    /// writes as UTC, carries the configured statement timeout, and on a sink
    /// also a strict `sql_mode`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming [`target`](Self::target) when the
    /// connection or one of the setup statements fails.
    pub(crate) async fn connect(&self) -> Result<Session, SaciError> {
        let attempt = Conn::new(self.opts.clone());
        let mut conn = if self.connect_timeout.is_zero() {
            attempt
                .await
                .map_err(|e| self.connect_error(mysql_detail(&e)))?
        } else {
            match tokio::time::timeout(self.connect_timeout, attempt).await {
                Ok(connected) => connected.map_err(|e| self.connect_error(mysql_detail(&e)))?,
                Err(_) => {
                    return Err(self.connect_error(format!(
                        "no answer within {} ms",
                        self.connect_timeout.as_millis()
                    )));
                }
            }
        };

        let banner: Option<String> = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| self.setup_error("SELECT VERSION()", &e))?;
        let flavor = Flavor::from_banner(banner.as_deref().unwrap_or_default());
        let version = conn.server_version();

        for statement in init_statements(self.role == Role::Sink, flavor, self.statement_timeout_ms)
        {
            conn.query_drop(statement.as_str())
                .await
                .map_err(|e| self.setup_error(&statement, &e))?;
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            target_db = %self.target,
            connector = self.role.what(),
            server = %format!("{} {}.{}.{}", flavor.name(), version.0, version.1, version.2),
            "mariadb connection established"
        );

        Ok(Session {
            conn,
            flavor,
            version,
        })
    }

    /// [`connect`](Self::connect) wrapped in the configured backoff.
    ///
    /// # Errors
    ///
    /// Returns the last attempt's error after `max_attempts` tries.
    pub(crate) async fn connect_with_retry(&self) -> Result<Session, SaciError> {
        let mut last = None;
        for attempt in 0..self.reconnect.max_attempts {
            match self.connect().await {
                Ok(session) => return Ok(session),
                Err(e) => {
                    let is_last = attempt + 1 == self.reconnect.max_attempts;
                    if is_last {
                        last = Some(e);
                        break;
                    }
                    let delay = self.backoff(attempt);
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        target_db = %self.target,
                        connector = self.role.what(),
                        attempt = attempt + 1,
                        max_attempts = self.reconnect.max_attempts,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "mariadb connect failed, retrying"
                    );
                    #[cfg(not(feature = "tracing"))]
                    let _ = &e;
                    last = Some(e);
                    tokio::time::sleep(delay).await;
                }
            }
        }
        Err(last.unwrap_or_else(|| {
            SaciError::configuration(format!(
                "{}: connection.reconnect.max_attempts must be at least 1",
                self.role.what()
            ))
        }))
    }

    /// `min(base · multiplier^attempt, max)`, jittered by `± jitter`.
    fn backoff(&self, attempt: u32) -> Duration {
        use rand::RngExt;

        let base = self.reconnect.base_delay_ms as f64;
        let grown = base * self.reconnect.multiplier.powi(attempt as i32);
        let capped = grown.min(self.reconnect.max_delay_ms as f64);
        let jittered = if self.reconnect.jitter > 0.0 {
            let factor: f64 = rand::rng().random_range(-1.0..=1.0);
            capped * (1.0 + self.reconnect.jitter * factor)
        } else {
            capped
        };
        Duration::from_millis(jittered.max(0.0) as u64)
    }

    /// A connect failure, naming the redacted target and nothing else.
    fn connect_error(&self, e: impl fmt::Display) -> SaciError {
        SaciError::generic(format!(
            "{}: cannot connect to {}: {e}",
            self.role.what(),
            self.target
        ))
    }

    /// A failed session setup statement.
    fn setup_error(&self, statement: &str, e: &mysql_async::Error) -> SaciError {
        SaciError::generic(format!(
            "{}: cannot run '{statement}' on {}: {}",
            self.role.what(),
            self.target,
            mysql_detail(e)
        ))
    }
}

/// The statements that pin a fresh session, in order.
///
/// `max_execution_time` (MySQL, `SELECT` only, milliseconds) and
/// `max_statement_time` (MariaDB, any statement, decimal seconds) are the two
/// servers' spellings of one limit. A timeout of 0 sets nothing.
fn init_statements(sink: bool, flavor: Flavor, statement_timeout_ms: u64) -> Vec<String> {
    let mut statements = vec![
        "SET time_zone = '+00:00'".to_string(),
        "SET NAMES utf8mb4".to_string(),
    ];
    if statement_timeout_ms > 0 {
        statements.push(match flavor {
            Flavor::Mysql => format!("SET SESSION max_execution_time = {statement_timeout_ms}"),
            Flavor::Mariadb => format!(
                "SET SESSION max_statement_time = {}.{:03}",
                statement_timeout_ms / 1000,
                statement_timeout_ms % 1000
            ),
        });
    }
    if sink {
        statements.push(format!("SET SESSION sql_mode = '{SINK_SQL_MODE}'"));
    }
    statements
}

/// The workspace's `ring` crypto provider, installed on first force.
///
/// Process global and idempotent: a provider another crate installed first, or
/// a racing install from another thread, is left in place.
#[cfg(feature = "tls")]
static RING_PROVIDER: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
});

/// The TLS options `ssl_mode` and `ssl_ca` ask for, or `None` for `disabled`.
///
/// `required` encrypts and trusts any certificate, `verify_ca` is meant to check
/// the chain but not the host name (`mysql_async` cannot skip it with the
/// `rustls` in use, see [`SslModeConfig::VerifyCa`]), `verify_identity` checks
/// both. A `ssl_ca` bundle
/// replaces the built-in web roots; it is read here so a missing file fails the
/// build and not the first batch.
#[cfg(feature = "tls")]
fn ssl_opts(what: &str, cfg: &ConnectionConfig) -> Result<Option<mysql_async::SslOpts>, SaciError> {
    use mysql_async::SslOpts;

    let mut opts = match cfg.ssl_mode {
        SslModeConfig::Disabled => return Ok(None),
        SslModeConfig::Required => SslOpts::default().with_danger_accept_invalid_certs(true),
        SslModeConfig::VerifyCa => SslOpts::default().with_danger_skip_domain_validation(true),
        SslModeConfig::VerifyIdentity => SslOpts::default(),
    };
    // `mysql_async` builds its rustls configuration without naming a provider,
    // so one must be the process default before the first handshake.
    std::sync::LazyLock::force(&RING_PROVIDER);
    if let Some(path) = &cfg.ssl_ca {
        let pem = std::fs::read(path).map_err(|e| {
            SaciError::configuration(format!(
                "{what}: cannot read connection.ssl_ca '{path}': {e}"
            ))
        })?;
        if pem.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.ssl_ca '{path}' is empty"
            )));
        }
        // PEM is text and DER is not, and a text file with no certificate block
        // would load as nothing and then fail every handshake.
        if std::str::from_utf8(&pem).is_ok_and(|text| !text.contains("-----BEGIN CERTIFICATE-----"))
        {
            return Err(SaciError::configuration(format!(
                "{what}: connection.ssl_ca '{path}' contains no PEM certificate"
            )));
        }
        opts = opts
            .with_root_certs(vec![pem.into()])
            .with_disable_built_in_roots(true);
    }
    Ok(Some(opts))
}

/// Without the `tls` feature only `disabled` builds, which
/// [`ConnectionConfig`]'s validation has already enforced.
#[cfg(not(feature = "tls"))]
fn ssl_opts(what: &str, cfg: &ConnectionConfig) -> Result<Option<mysql_async::SslOpts>, SaciError> {
    if cfg.ssl_mode != SslModeConfig::Disabled {
        return Err(SaciError::configuration(format!(
            "{what}: connection.ssl_mode '{}' needs the 'tls' feature of saci-connector-mariadb, \
             which is not enabled in this build",
            cfg.ssl_mode.as_str()
        )));
    }
    Ok(None)
}

/// Render a `mysql_async::Error` with the server's own message.
///
/// A server error reads `{message} [{code}/{state}]`. Every other error is the
/// driver or the socket speaking, and its `Display` already folds in the text
/// of its cause behind a generic label (`Input/output error: Input/output
/// error: Connection refused`), so the chain is walked to its innermost cause,
/// which is the specific one.
pub(crate) fn mysql_detail(e: &mysql_async::Error) -> String {
    if let mysql_async::Error::Server(server) = e {
        return format!("{} [{}/{}]", server.message, server.code, server.state);
    }
    let mut innermost: &(dyn std::error::Error + 'static) = e;
    while let Some(cause) = innermost.source() {
        innermost = cause;
    }
    innermost.to_string()
}

/// The server's error code, when `e` is a server error.
pub(crate) fn server_code(e: &mysql_async::Error) -> Option<u16> {
    match e {
        mysql_async::Error::Server(server) => Some(server.code),
        _ => None,
    }
}

/// Run one prepared statement and close it, returning every row.
///
/// The connection keeps no statement cache, and `mysql_async` leaks a server
/// statement for every `exec` without one, so a statement that runs once per
/// batch or cycle is prepared and closed around its use. The execution error,
/// not the close error, is the one reported.
pub(crate) async fn exec_closing(
    conn: &mut Conn,
    sql: &str,
    params: impl Into<Params> + Send,
) -> Result<Vec<Row>, mysql_async::Error> {
    let statement = conn.prep(sql).await?;
    let rows = conn.exec::<Row, _, _>(&statement, params).await;
    let closed = conn.close(statement).await;
    let rows = rows?;
    closed?;
    Ok(rows)
}

// -------------------------------------------------------------------- quoting

/// Quote one identifier with backticks, doubling a backtick inside it.
pub(crate) fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

/// Quote a column list as `` `a`, `b`, `c` ``.
pub(crate) fn quote_columns<'a, I>(columns: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    columns
        .into_iter()
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(", ")
}

// --------------------------------------------------------------- upsert syntax

/// The clause that turns an `INSERT ... VALUES (...)` into an upsert rewriting
/// `update_columns` from the proposed row, with a leading space.
///
/// MariaDB has no row alias and reads the proposed value with `VALUES(c)`.
/// MySQL deprecates `VALUES()` in favour of an alias on the row. The sink and
/// the offset store both write through here.
///
/// A table that holds nothing but its key has no column to rewrite, and
/// `ON DUPLICATE KEY UPDATE` with no assignment is a syntax error, so an empty
/// `update_columns` yields [`ignore_tail`] over `key_column`: a collision
/// leaves the existing row as it is, which is all an upsert of a key-only row
/// can mean.
pub(crate) fn upsert_tail(flavor: Flavor, update_columns: &[&str], key_column: &str) -> String {
    if update_columns.is_empty() {
        return ignore_tail(key_column);
    }
    let assignments = update_columns
        .iter()
        .map(|column| {
            let quoted = quote_ident(column);
            match flavor {
                Flavor::Mariadb => format!("{quoted} = VALUES({quoted})"),
                Flavor::Mysql => format!("{quoted} = __saci_new.{quoted}"),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    match flavor {
        Flavor::Mariadb => format!(" ON DUPLICATE KEY UPDATE {assignments}"),
        Flavor::Mysql => format!(" AS __saci_new ON DUPLICATE KEY UPDATE {assignments}"),
    }
}

/// The clause that leaves an existing row exactly as it is, with a leading
/// space: `ON DUPLICATE KEY UPDATE k = k`.
///
/// Never `INSERT IGNORE`, which also hides truncation and type errors.
pub(crate) fn ignore_tail(key_column: &str) -> String {
    let quoted = quote_ident(key_column);
    format!(" ON DUPLICATE KEY UPDATE {quoted} = {quoted}")
}

// -------------------------------------------------------------------- catalog

/// The columns of `table`, in table order.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the table has no visible columns,
/// which is a missing table or a user that cannot see it, and
/// [`SaciError::Generic`] when the catalog cannot be read.
pub(crate) async fn columns_of(
    conn: &mut Conn,
    what: &str,
    table: &TableRef,
    target: &str,
) -> Result<Vec<CatalogColumn>, SaciError> {
    type Row = (
        String,
        u64,
        String,
        String,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<String>,
    );
    let rows: Vec<Row> = conn
        .exec(
            "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, \
             NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (table.schema.as_str(), table.name.as_str()),
        )
        .await
        .map_err(|e| {
            SaciError::generic(format!(
                "{what}: cannot read the columns of '{table}': {}",
                mysql_detail(&e)
            ))
        })?;

    if rows.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: table '{table}' does not exist on {target}, or the connecting user cannot \
             see it"
        )));
    }

    Ok(rows
        .into_iter()
        .map(
            |(
                name,
                position,
                data_type,
                column_type,
                numeric_precision,
                numeric_scale,
                datetime_precision,
                charset,
            )| CatalogColumn {
                name,
                ordinal: usize::try_from(position.saturating_sub(1)).unwrap_or(usize::MAX),
                data_type: data_type.to_ascii_lowercase(),
                column_type,
                numeric_precision,
                numeric_scale,
                datetime_precision,
                charset,
            },
        )
        .collect())
}

/// One unique index of a table, in key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UniqueIndex {
    /// `INDEX_NAME`; the primary key is `PRIMARY`.
    pub(crate) name: String,
    /// The indexed columns, by `SEQ_IN_INDEX`.
    pub(crate) columns: Vec<String>,
    /// Whether every part indexes a whole column. A prefix index (`name(10)`)
    /// or a functional index collides on less than its columns, so a key that
    /// resolves duplicates through it is not the key it names.
    pub(crate) exact: bool,
}

/// Every unique index of `table`, `PRIMARY` included.
async fn unique_indexes(
    conn: &mut Conn,
    what: &str,
    table: &TableRef,
) -> Result<Vec<UniqueIndex>, SaciError> {
    let rows: Vec<(String, Option<String>, Option<u64>)> = conn
        .exec(
            "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (table.schema.as_str(), table.name.as_str()),
        )
        .await
        .map_err(|e| {
            SaciError::generic(format!(
                "{what}: cannot read the keys of '{table}': {}",
                mysql_detail(&e)
            ))
        })?;
    Ok(group_indexes(rows))
}

/// Group `(INDEX_NAME, COLUMN_NAME, SUB_PART)` rows, already ordered by index
/// name and position, into one [`UniqueIndex`] each.
fn group_indexes(rows: Vec<(String, Option<String>, Option<u64>)>) -> Vec<UniqueIndex> {
    let mut indexes: Vec<UniqueIndex> = Vec::new();
    for (name, column, sub_part) in rows {
        if indexes.last().is_none_or(|index| index.name != name) {
            indexes.push(UniqueIndex {
                name,
                columns: Vec::new(),
                exact: true,
            });
        }
        let Some(index) = indexes.last_mut() else {
            continue;
        };
        match column {
            Some(column) => index.columns.push(column),
            None => index.exact = false,
        }
        if sub_part.is_some() {
            index.exact = false;
        }
    }
    indexes
}

/// The primary key columns of `table` in key order, empty when it has none.
///
/// A prefix part is kept: the key still orders and pages the table, because
/// distinct prefixes mean distinct values.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the catalog cannot be read.
pub(crate) async fn primary_key_of(
    conn: &mut Conn,
    what: &str,
    table: &TableRef,
) -> Result<Vec<String>, SaciError> {
    Ok(unique_indexes(conn, what, table)
        .await?
        .into_iter()
        .find(|index| index.name == "PRIMARY")
        .map(|index| index.columns)
        .unwrap_or_default())
}

/// The column sets of every unique index of `table`, primary key included,
/// that resolves a duplicate on exactly its columns.
///
/// This is what a sink's `conflict_columns` must equal: `ON DUPLICATE KEY
/// UPDATE` fires on whichever unique index the row collides on, so naming a
/// different set would resolve conflicts on a key the configuration does not
/// show. A prefix or functional index is left out for that reason.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the catalog cannot be read.
pub(crate) async fn unique_keys_of(
    conn: &mut Conn,
    what: &str,
    table: &TableRef,
) -> Result<Vec<Vec<String>>, SaciError> {
    Ok(unique_indexes(conn, what, table)
        .await?
        .into_iter()
        .filter(|index| index.exact)
        .map(|index| index.columns)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ConnectionConfig {
        ConnectionConfig {
            dsn: "mysql://someone:hunter2@db.example:6543/app".to_string(),
            user: None,
            password: None,
            password_file: None,
            connect_timeout_ms: 1000,
            statement_timeout_ms: 0,
            ssl_mode: SslModeConfig::Disabled,
            ssl_ca: None,
            reconnect: ReconnectConfig::default(),
        }
    }

    fn connector(cfg: &ConnectionConfig) -> Connector {
        let Ok(connector) = Connector::new(Role::Source, cfg) else {
            panic!("the connection config builds");
        };
        connector
    }

    fn refused(role: Role, cfg: &ConnectionConfig) -> SaciError {
        let Err(err) = Connector::new(role, cfg) else {
            panic!("the connection config must be refused");
        };
        err
    }

    // --------------------------------------------------------------- the target

    #[test]
    fn target_is_host_port_db_with_no_credentials() {
        let connector = connector(&base());
        assert_eq!(connector.target(), "db.example:6543/app");
        assert!(!connector.target().contains("hunter2"));
        assert!(!connector.target().contains("someone"));
    }

    #[test]
    fn a_dsn_without_a_port_reports_the_default() {
        let mut cfg = base();
        cfg.dsn = "mysql://db.example/app".to_string();
        assert_eq!(connector(&cfg).target(), "db.example:3306/app");
    }

    #[test]
    fn a_dsn_without_a_database_reports_a_question_mark() {
        let mut cfg = base();
        cfg.dsn = "mysql://db.example:3307".to_string();
        assert_eq!(connector(&cfg).target(), "db.example:3307/?");
    }

    #[test]
    fn a_mariadb_scheme_is_rewritten_to_the_one_the_driver_parses() {
        let mut cfg = base();
        cfg.dsn = "mariadb://u:p@maria.example:3310/shop".to_string();
        let connector = connector(&cfg);
        assert_eq!(connector.target(), "maria.example:3310/shop");
        assert_eq!(connector.opts.user(), Some("u"));
    }

    #[test]
    fn an_unparseable_dsn_is_a_configuration_error_without_the_dsn() {
        let mut cfg = base();
        cfg.dsn = "mysql://user:secret@host:notaport/db".to_string();
        let err = refused(Role::Source, &cfg);
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("connection.dsn"),
            "{}",
            err.message()
        );
        assert!(!err.message().contains("secret"), "{}", err.message());
    }

    #[test]
    fn a_dsn_with_another_scheme_is_refused_without_echoing_it() {
        let mut cfg = base();
        cfg.dsn = "postgres://user:secret@host/db".to_string();
        let err = refused(Role::Sink, &cfg);
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().starts_with("MariadbSink: "),
            "{}",
            err.message()
        );
        assert!(!err.message().contains("secret"), "{}", err.message());
    }

    // ------------------------------------------------------------- credentials

    #[test]
    fn the_user_and_password_keys_override_the_dsn() {
        let mut cfg = base();
        cfg.user = Some("override".to_string());
        cfg.password = Some("pw".to_string());
        let connector = connector(&cfg);
        assert_eq!(connector.opts.user(), Some("override"));
        assert_eq!(connector.opts.pass(), Some("pw"));
    }

    #[test]
    fn the_dsn_credentials_stand_when_no_key_overrides_them() {
        let connector = connector(&base());
        assert_eq!(connector.opts.user(), Some("someone"));
        assert_eq!(connector.opts.pass(), Some("hunter2"));
    }

    #[test]
    fn the_trimmed_password_file_wins_over_the_password_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("secret");
        std::fs::write(&path, "  from-the-file\n").expect("write");
        let mut cfg = base();
        cfg.password = Some("inline".to_string());
        cfg.password_file = Some(path.display().to_string());
        assert_eq!(connector(&cfg).opts.pass(), Some("from-the-file"));
    }

    #[test]
    fn a_missing_password_file_names_the_path() {
        let mut cfg = base();
        cfg.password_file = Some("no/such/secret".to_string());
        let err = refused(Role::Source, &cfg);
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no/such/secret"),
            "{}",
            err.message()
        );
    }

    // -------------------------------------------------------------------- TLS

    #[test]
    fn a_disabled_ssl_mode_builds_no_tls_options() {
        assert!(connector(&base()).opts.ssl_opts().is_none());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn each_ssl_mode_maps_to_the_options_that_enforce_it() {
        let mut cfg = base();
        for (mode, accept_invalid, skip_domain) in [
            (SslModeConfig::Required, true, false),
            (SslModeConfig::VerifyCa, false, true),
            (SslModeConfig::VerifyIdentity, false, false),
        ] {
            cfg.ssl_mode = mode;
            let connector = connector(&cfg);
            let ssl = connector.opts.ssl_opts().expect("tls options");
            assert_eq!(ssl.accept_invalid_certs(), accept_invalid, "{mode:?}");
            assert_eq!(ssl.skip_domain_validation(), skip_domain, "{mode:?}");
            assert!(!ssl.disable_built_in_roots(), "{mode:?}");
            assert!(ssl.root_certs().is_empty(), "{mode:?}");
        }
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_tls_ssl_mode_leaves_a_ring_crypto_provider_installed_for_the_driver() {
        let mut cfg = base();
        cfg.ssl_mode = SslModeConfig::VerifyIdentity;
        let _ = connector(&cfg);
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "mysql_async builds its rustls config from the process default provider"
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_ca_bundle_replaces_the_built_in_roots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ca.pem");
        std::fs::write(
            &path,
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        )
        .expect("write");
        let mut cfg = base();
        cfg.ssl_mode = SslModeConfig::VerifyIdentity;
        cfg.ssl_ca = Some(path.display().to_string());
        let connector = connector(&cfg);
        let ssl = connector.opts.ssl_opts().expect("tls options");
        assert!(ssl.disable_built_in_roots());
        assert_eq!(ssl.root_certs().len(), 1);
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_missing_ca_bundle_names_the_path() {
        let mut cfg = base();
        cfg.ssl_mode = SslModeConfig::VerifyCa;
        cfg.ssl_ca = Some("no/such/ca.pem".to_string());
        let err = refused(Role::Source, &cfg);
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no/such/ca.pem"),
            "{}",
            err.message()
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn an_empty_or_certificate_free_ca_bundle_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = base();
        cfg.ssl_mode = SslModeConfig::VerifyCa;
        for (file, content, needle) in [
            ("empty.pem", "", "is empty"),
            ("text.pem", "# no certificates here\n", "no PEM certificate"),
        ] {
            let path = dir.path().join(file);
            std::fs::write(&path, content).expect("write");
            cfg.ssl_ca = Some(path.display().to_string());
            let err = refused(Role::Source, &cfg);
            assert!(err.message().contains(needle), "{file}: {}", err.message());
        }
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn a_tls_ssl_mode_without_the_tls_feature_is_refused_at_build() {
        let mut cfg = base();
        cfg.ssl_mode = SslModeConfig::Required;
        let err = refused(Role::Source, &cfg);
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'tls' feature"), "{}", err.message());
    }

    // ----------------------------------------------------------------- tables

    #[test]
    fn a_bare_table_takes_the_dsn_database_and_a_qualified_one_keeps_its_own() {
        let connector = connector(&base());
        let bare = connector
            .resolve_table("mode.table", "orders")
            .expect("bare");
        assert_eq!(
            bare,
            TableRef {
                schema: "app".to_string(),
                name: "orders".to_string()
            }
        );
        assert_eq!(bare.to_string(), "app.orders");
        let qualified = connector
            .resolve_table("mode.table", "shop.orders")
            .expect("qualified");
        assert_eq!(qualified.schema, "shop");
    }

    #[test]
    fn a_bare_table_with_no_database_in_the_dsn_must_be_qualified() {
        let mut cfg = base();
        cfg.dsn = "mysql://db.example".to_string();
        let err = connector(&cfg)
            .resolve_table("mode.table", "orders")
            .expect_err("no database");
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "MariadbSource: connection.dsn names no database, so mode.table 'orders' must be \
             written schema.table"
        );
    }

    #[test]
    fn the_sink_names_its_own_key_when_the_database_is_missing() {
        let mut cfg = base();
        cfg.dsn = "mysql://db.example".to_string();
        let Ok(sink) = Connector::new(Role::Sink, &cfg) else {
            panic!("builds");
        };
        let err = sink
            .resolve_table("table", "orders")
            .expect_err("no database");
        assert!(
            err.message()
                .starts_with("MariadbSink: connection.dsn names no database, so table 'orders'"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_sink_keeps_no_statement_cache_and_a_source_keeps_one() {
        let cfg = base();
        let Ok(sink) = Connector::new(Role::Sink, &cfg) else {
            panic!("builds");
        };
        let Ok(source) = Connector::new(Role::Source, &cfg) else {
            panic!("builds");
        };
        assert_eq!(sink.opts.stmt_cache_size(), 0);
        assert_eq!(source.opts.stmt_cache_size(), 32);
    }

    #[test]
    fn a_table_resolved_from_a_longer_path_is_refused() {
        let err = connector(&base())
            .resolve_table("mode.table", "a.b.c")
            .expect_err("three parts");
        assert!(
            err.message().contains("mode.table 'a.b.c'"),
            "{}",
            err.message()
        );
    }

    // --------------------------------------------------------------- reconnect

    #[test]
    fn connect_errors_name_the_redacted_target_only() {
        let message = connector(&base())
            .connect_error("connection refused")
            .message();
        assert!(message.contains("db.example:6543/app"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
        assert!(!message.contains("someone"), "{message}");
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let mut cfg = base();
        cfg.reconnect = ReconnectConfig {
            max_attempts: 8,
            base_delay_ms: 100,
            multiplier: 2.0,
            max_delay_ms: 500,
            jitter: 0.0,
        };
        let connector = connector(&cfg);
        assert_eq!(connector.backoff(0), Duration::from_millis(100));
        assert_eq!(connector.backoff(1), Duration::from_millis(200));
        assert_eq!(connector.backoff(2), Duration::from_millis(400));
        assert_eq!(connector.backoff(3), Duration::from_millis(500));
        assert_eq!(connector.backoff(20), Duration::from_millis(500));
    }

    #[test]
    fn jitter_stays_within_its_band() {
        let mut cfg = base();
        cfg.reconnect = ReconnectConfig {
            max_attempts: 3,
            base_delay_ms: 1000,
            multiplier: 1.0,
            max_delay_ms: 1000,
            jitter: 0.1,
        };
        let connector = connector(&cfg);
        for _ in 0..64 {
            let delay = connector.backoff(0).as_millis();
            assert!((900..=1100).contains(&delay), "delay {delay} out of band");
        }
    }

    #[tokio::test]
    async fn an_unreachable_server_fails_with_the_target_after_every_attempt() {
        let mut cfg = base();
        // Port 1 is reserved and refuses at once.
        cfg.dsn = "mysql://u:hunter2@127.0.0.1:1/app".to_string();
        cfg.reconnect = ReconnectConfig {
            max_attempts: 2,
            base_delay_ms: 1,
            multiplier: 1.0,
            max_delay_ms: 1,
            jitter: 0.0,
        };
        let Err(err) = connector(&cfg).connect_with_retry().await else {
            panic!("nothing listens on port 1");
        };
        assert_eq!(err.category(), "generic");
        assert!(
            err.message().contains("cannot connect to 127.0.0.1:1/app"),
            "{}",
            err.message()
        );
        assert!(!err.message().contains("hunter2"), "{}", err.message());
    }

    // ------------------------------------------------------------ session setup

    #[test]
    fn every_session_pins_utc_and_utf8mb4() {
        let statements = init_statements(false, Flavor::Mysql, 0);
        assert_eq!(
            statements,
            vec!["SET time_zone = '+00:00'", "SET NAMES utf8mb4"]
        );
    }

    #[test]
    fn the_statement_timeout_uses_each_servers_own_variable() {
        let mysql = init_statements(false, Flavor::Mysql, 30_000);
        assert_eq!(
            mysql.last().map(String::as_str),
            Some("SET SESSION max_execution_time = 30000")
        );
        let mariadb = init_statements(false, Flavor::Mariadb, 30_000);
        assert_eq!(
            mariadb.last().map(String::as_str),
            Some("SET SESSION max_statement_time = 30.000")
        );
        let fractional = init_statements(false, Flavor::Mariadb, 1_500);
        assert_eq!(
            fractional.last().map(String::as_str),
            Some("SET SESSION max_statement_time = 1.500")
        );
        let tiny = init_statements(false, Flavor::Mariadb, 7);
        assert_eq!(
            tiny.last().map(String::as_str),
            Some("SET SESSION max_statement_time = 0.007")
        );
    }

    #[test]
    fn a_zero_statement_timeout_sets_nothing() {
        for flavor in [Flavor::Mysql, Flavor::Mariadb] {
            assert_eq!(init_statements(false, flavor, 0).len(), 2);
        }
    }

    #[test]
    fn only_a_sink_session_turns_on_the_strict_sql_mode() {
        let sink = init_statements(true, Flavor::Mysql, 0);
        assert_eq!(
            sink.last().map(String::as_str),
            Some(
                "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
                 ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
            )
        );
        assert!(
            !init_statements(false, Flavor::Mysql, 0)
                .iter()
                .any(|s| s.contains("sql_mode"))
        );
    }

    #[test]
    fn the_flavour_follows_the_version_banner() {
        for (banner, flavor) in [
            ("11.8.2-MariaDB-ubu2404", Flavor::Mariadb),
            ("10.11.9-MariaDB", Flavor::Mariadb),
            ("5.5.5-10.6.18-MariaDB-log", Flavor::Mariadb),
            ("8.4.5", Flavor::Mysql),
            ("9.3.0", Flavor::Mysql),
            ("8.0.40-0ubuntu0.22.04.1", Flavor::Mysql),
            ("", Flavor::Mysql),
        ] {
            assert_eq!(Flavor::from_banner(banner), flavor, "{banner:?}");
        }
        assert_eq!(Flavor::Mariadb.name(), "MariaDB");
        assert_eq!(Flavor::Mysql.name(), "MySQL");
    }

    // ------------------------------------------------------------------ errors

    #[test]
    fn a_server_error_reads_message_code_and_state() {
        let error = mysql_async::Error::Server(mysql_async::ServerError {
            code: 1146,
            message: "Table 'app.t' doesn't exist".to_string(),
            state: "42S02".to_string(),
        });
        assert_eq!(
            mysql_detail(&error),
            "Table 'app.t' doesn't exist [1146/42S02]"
        );
        assert_eq!(server_code(&error), Some(1146));
    }

    #[test]
    fn a_socket_error_reads_as_its_innermost_cause() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let error = mysql_async::Error::from(io);
        assert_eq!(mysql_detail(&error), "connection refused");
        assert_eq!(server_code(&error), None);
        assert!(error.is_fatal());
    }

    #[test]
    fn a_boxed_driver_error_reads_as_its_own_text() {
        let error =
            mysql_async::Error::Other(Box::new(std::io::Error::other("packet stream broke")));
        assert_eq!(mysql_detail(&error), "packet stream broke");
    }

    #[test]
    fn a_driver_error_reads_without_its_label() {
        let error = mysql_async::Error::Driver(mysql_async::DriverError::ConnectionClosed);
        assert_eq!(mysql_detail(&error), "Connection to the server is closed.");
    }

    // ----------------------------------------------------------------- quoting

    #[test]
    fn identifiers_are_quoted_with_backticks_and_a_backtick_is_doubled() {
        assert_eq!(quote_ident("orders"), "`orders`");
        assert_eq!(quote_ident("we`ird"), "`we``ird`");
        assert_eq!(quote_ident(""), "``");
        assert_eq!(quote_columns(["a", "b`c"]), "`a`, `b``c`");
        assert_eq!(quote_columns(std::iter::empty()), "");
    }

    #[test]
    fn a_qualified_name_splits_into_schema_and_table() {
        assert_eq!(
            split_qualified("T", "k", "orders").unwrap(),
            (None, "orders")
        );
        assert_eq!(
            split_qualified("T", "k", "sales.orders").unwrap(),
            (Some("sales"), "orders")
        );
    }

    #[test]
    fn an_injection_payload_in_a_name_stays_inside_its_quotes() {
        let payload = "t`; DROP TABLE x; --";
        let quoted = quote_ident(payload);
        assert_eq!(quoted, "`t``; DROP TABLE x; --`");
        // Every backtick the payload carried is doubled, so the identifier
        // closes only at the final one.
        let table = TableRef {
            schema: "s`; DROP DATABASE d; --".to_string(),
            name: payload.to_string(),
        };
        assert_eq!(
            table.quoted(),
            "`s``; DROP DATABASE d; --`.`t``; DROP TABLE x; --`"
        );
    }

    #[test]
    fn a_multi_dot_name_is_refused() {
        let err = split_qualified("MariadbSink", "table", "a.b.c").unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("table 'a.b.c'"), "{}", err.message());
    }

    // ------------------------------------------------------------------ upserts

    #[test]
    fn mariadb_upserts_read_the_proposed_row_with_values() {
        assert_eq!(
            upsert_tail(Flavor::Mariadb, &["a", "b"], "id"),
            " ON DUPLICATE KEY UPDATE `a` = VALUES(`a`), `b` = VALUES(`b`)"
        );
    }

    #[test]
    fn mysql_upserts_alias_the_row_instead_of_using_the_deprecated_values() {
        assert_eq!(
            upsert_tail(Flavor::Mysql, &["a", "b"], "id"),
            " AS __saci_new ON DUPLICATE KEY UPDATE `a` = __saci_new.`a`, `b` = __saci_new.`b`"
        );
    }

    #[test]
    fn an_upsert_quotes_a_column_name_with_a_backtick() {
        assert_eq!(
            upsert_tail(Flavor::Mariadb, &["we`ird"], "id"),
            " ON DUPLICATE KEY UPDATE `we``ird` = VALUES(`we``ird`)"
        );
    }

    #[test]
    fn an_ignored_conflict_rewrites_a_key_column_to_itself() {
        assert_eq!(ignore_tail("id"), " ON DUPLICATE KEY UPDATE `id` = `id`");
    }

    #[test]
    fn an_upsert_with_nothing_to_rewrite_keeps_the_row_instead_of_emitting_no_assignment() {
        for flavor in [Flavor::Mariadb, Flavor::Mysql] {
            let tail = upsert_tail(flavor, &[], "we`ird_key");
            assert_eq!(
                tail,
                " ON DUPLICATE KEY UPDATE `we``ird_key` = `we``ird_key`"
            );
            assert!(
                !tail.ends_with("UPDATE "),
                "{flavor:?} would be a syntax error"
            );
        }
    }

    // ------------------------------------------------------------------ catalog

    fn index_row(
        name: &str,
        column: Option<&str>,
        sub_part: Option<u64>,
    ) -> (String, Option<String>, Option<u64>) {
        (name.to_string(), column.map(str::to_string), sub_part)
    }

    #[test]
    fn index_rows_group_by_name_in_key_order() {
        let grouped = group_indexes(vec![
            index_row("PRIMARY", Some("a"), None),
            index_row("PRIMARY", Some("b"), None),
            index_row("by_email", Some("email"), None),
        ]);
        assert_eq!(
            grouped,
            vec![
                UniqueIndex {
                    name: "PRIMARY".to_string(),
                    columns: vec!["a".to_string(), "b".to_string()],
                    exact: true
                },
                UniqueIndex {
                    name: "by_email".to_string(),
                    columns: vec!["email".to_string()],
                    exact: true
                },
            ]
        );
    }

    #[test]
    fn a_prefix_or_functional_index_is_not_exact() {
        let grouped = group_indexes(vec![
            index_row("by_name", Some("name"), Some(10)),
            index_row("by_expr", None, None),
            index_row("by_mixed", Some("a"), None),
            index_row("by_mixed", Some("b"), Some(4)),
        ]);
        assert!(grouped.iter().all(|index| !index.exact), "{grouped:?}");
        assert_eq!(grouped[1].columns, Vec::<String>::new());
        assert_eq!(grouped[2].columns, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn a_table_with_no_unique_index_groups_to_nothing() {
        assert!(group_indexes(Vec::new()).is_empty());
    }

    #[test]
    fn a_server_version_reads_with_its_product() {
        assert_eq!(
            describe_version(Flavor::Mariadb, (10, 11, 9)),
            "MariaDB 10.11.9"
        );
        assert_eq!(describe_version(Flavor::Mysql, (8, 4, 5)), "MySQL 8.4.5");
    }
}
