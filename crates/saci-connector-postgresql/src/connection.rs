//! One connection per connector instance, plus the SQL-identifier quoting every
//! statement in this crate goes through.
//!
//! There is no pool. Both [`Source`](saci_core::io::source::Source) and
//! [`Sink`](saci_core::io::sink::Sink) serialise all access through `&mut self`,
//! so a second connection would sit idle.
//!
//! [`Connector`] is built synchronously, because `SourceFactory::build` is
//! synchronous: it parses the DSN, reads the password file, and builds the TLS
//! configuration, but opens no socket. [`Connector::connect_with_retry`] is the
//! async half, called on the first `next_batch`/`write_batch` and again whenever
//! [`PgConnection::is_closed`] reports the session gone.
//!
//! It also owns the one catalog lookup both halves share
//! ([`column_types`]): the declared schema is checked against
//! `pg_attribute`, not against a statement's own result columns, and a type
//! the canonical table has no row for is resolved through `pg_type` -- a
//! domain to its base type and its own modifier, an enum or a composite to
//! its `schema.typname`.
//!
//! # Redaction
//!
//! [`Connector::target`] is `host:port/dbname` and is the **only** form of the
//! connection details that may appear in an error, a log line or a metric. The
//! DSN, the user and the password are never interpolated anywhere in this crate.

use std::collections::HashMap;
use std::time::Duration;

use futures_util::StreamExt;
use postgres_protocol::escape::escape_identifier;
use saci_core::error::SaciError;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::{AsyncMessage, Client, Connection, Notification};

use crate::config::{ConnectionConfig, ReconnectConfig, SslModeConfig, split_qualified};
use crate::types::{ColumnType, is_static_type, unconstrained_type_name};

/// Notifications buffered before the driver task starts dropping them.
const NOTIFICATION_BUFFER: usize = 256;

/// Output settings pinned on every session, so PostgreSQL's canonical text is
/// deterministic no matter how the server or the role is configured.
///
/// The connector reads whole families of types through their text form (an
/// enum, a range, a geometric, `inet`, `tsvector`, …) and writes the cursor
/// offset through `::text`, so every one of these settings is load-bearing:
///
/// - `datestyle` fixes `date`/`timestamp` to ISO with a year-month-day order.
/// - `intervalstyle` fixes `interval` to the `1 year 2 mons 03:04:05` form.
/// - `extra_float_digits` asks for shortest-round-trip float text.
/// - `bytea_output` fixes `bytea` to `\x` hex rather than escape format.
/// - `lc_monetary` fixes `money`'s scale to two fractional digits.
/// - `timezone` makes every `timestamptz` render with a `+00` offset.
///
/// The output functions run in this backend, including the ones the logical
/// slot interface drives, so these settings govern `pgoutput`'s text tuples
/// too -- which a walsender-based reader could not rely on.
const OUTPUT_SETTINGS: &str = "SET datestyle = 'ISO, YMD'; \
     SET intervalstyle = 'postgres'; \
     SET extra_float_digits = 3; \
     SET bytea_output = 'hex'; \
     SET lc_monetary = 'C'; \
     SET timezone = 'UTC'";

/// A live session: the client, its notification stream, and the driver task.
pub(crate) struct PgConnection {
    client: Client,
    /// `LISTEN`/`NOTIFY` payloads the driver task forwards.
    ///
    /// Always present. A bare `tokio::spawn(connection)` discards
    /// notifications, so the driver polls `poll_message` unconditionally; the
    /// modes that do not use `NOTIFY` simply never read this.
    notifications: mpsc::Receiver<Notification>,
    task: Option<JoinHandle<()>>,
}

impl PgConnection {
    /// The client for issuing statements.
    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    /// The client, mutably, which is what `Client::transaction` requires.
    pub(crate) fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Whether the session is gone and the caller must reconnect.
    pub(crate) fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Take the next buffered notification without waiting.
    pub(crate) fn try_notification(&mut self) -> Option<Notification> {
        self.notifications.try_recv().ok()
    }

    /// Wait up to `timeout` for a notification.
    ///
    /// `Ok(None)` means the driver task ended, which is a closed connection.
    pub(crate) async fn next_notification(
        &mut self,
        timeout: Duration,
    ) -> Option<Option<Notification>> {
        tokio::time::timeout(timeout, self.notifications.recv())
            .await
            .ok()
    }
}

impl Drop for PgConnection {
    fn drop(&mut self) {
        // The driver task owns the socket and outlives the client otherwise.
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// TLS backend selected at construction, so no per-connect decision remains.
#[cfg(feature = "tls")]
enum TlsChoice {
    /// `sslmode = "disable"`.
    None,
    /// `sslmode = "prefer"` or `"require"`; the mode itself lives in the
    /// `tokio_postgres::Config`.
    Rustls(tokio_postgres_rustls::MakeRustlsConnect),
}

/// Everything needed to open a session, resolved once and reused.
pub(crate) struct Connector {
    pg: tokio_postgres::Config,
    /// `host:port/dbname`. The only form of the target that may be logged.
    target: String,
    reconnect: ReconnectConfig,
    statement_timeout_ms: u64,
    /// `PostgresSource` or `PostgresSink`, for error prefixes.
    what: &'static str,
    #[cfg(feature = "tls")]
    tls: TlsChoice,
}

impl Connector {
    /// Parse and validate everything that can be checked without a server.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the DSN does not parse, when
    /// `password_file` cannot be read, or when the TLS configuration cannot be
    /// built. The message carries the parse error, never the DSN.
    pub(crate) fn new(what: &'static str, cfg: &ConnectionConfig) -> Result<Self, SaciError> {
        let mut pg: tokio_postgres::Config = cfg.dsn.parse().map_err(|e| {
            SaciError::configuration(format!("{what}: cannot parse connection.dsn: {e}"))
        })?;

        if let Some(user) = &cfg.user {
            pg.user(user.as_str());
        }
        if let Some(password) = &cfg.password {
            pg.password(password.as_str());
        }
        if let Some(path) = &cfg.password_file {
            let secret = std::fs::read_to_string(path).map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: cannot read connection.password_file '{path}': {e}"
                ))
            })?;
            pg.password(secret.trim());
        }
        if let Some(name) = &cfg.application_name {
            pg.application_name(name.as_str());
        }
        pg.connect_timeout(Duration::from_millis(cfg.connect_timeout_ms));
        pg.ssl_mode(match cfg.sslmode {
            SslModeConfig::Disable => SslMode::Disable,
            SslModeConfig::Prefer => SslMode::Prefer,
            SslModeConfig::Require => SslMode::Require,
        });

        let target = describe_target(&pg);

        #[cfg(feature = "tls")]
        let tls = if cfg.sslmode == SslModeConfig::Disable {
            TlsChoice::None
        } else {
            TlsChoice::Rustls(tokio_postgres_rustls::MakeRustlsConnect::new(
                build_client_config(what, cfg)?,
            ))
        };

        #[cfg(not(feature = "tls"))]
        if cfg.sslmode == SslModeConfig::Require {
            return Err(SaciError::configuration(format!(
                "{what}: connection.sslmode = \"require\" needs the 'tls' feature of \
                 saci-connector-postgresql, which is not enabled in this build"
            )));
        }

        Ok(Self {
            pg,
            target,
            reconnect: cfg.reconnect.clone(),
            statement_timeout_ms: cfg.statement_timeout_ms,
            what,
            #[cfg(feature = "tls")]
            tls,
        })
    }

    /// `host:port/dbname`, the redacted form used in every message.
    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    /// Open one session, apply `statement_timeout`, and spawn its driver task.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming [`target`](Self::target) when the
    /// connection or the `SET` fails.
    pub(crate) async fn connect(&self) -> Result<PgConnection, SaciError> {
        #[cfg(feature = "tls")]
        let connection = match &self.tls {
            TlsChoice::None => {
                let (client, driver) = self
                    .pg
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(|e| self.connect_error(pg_detail(&e)))?;
                spawn_driver(client, driver)
            }
            TlsChoice::Rustls(tls) => {
                let (client, driver) = self
                    .pg
                    .connect(tls.clone())
                    .await
                    .map_err(|e| self.connect_error(pg_detail(&e)))?;
                spawn_driver(client, driver)
            }
        };

        #[cfg(not(feature = "tls"))]
        let connection = {
            let (client, driver) = self
                .pg
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| self.connect_error(pg_detail(&e)))?;
            spawn_driver(client, driver)
        };

        if self.statement_timeout_ms > 0 {
            connection
                .client()
                .batch_execute(&format!(
                    "SET statement_timeout = {}",
                    self.statement_timeout_ms
                ))
                .await
                .map_err(|e| {
                    SaciError::generic(format!(
                        "{}: cannot set statement_timeout on {}: {}",
                        self.what,
                        self.target,
                        pg_detail(&e)
                    ))
                })?;
        }

        connection
            .client()
            .batch_execute(OUTPUT_SETTINGS)
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "{}: cannot pin the output settings on {}: {}",
                    self.what,
                    self.target,
                    pg_detail(&e)
                ))
            })?;

        #[cfg(feature = "tracing")]
        tracing::info!(
            target_db = %self.target,
            connector = self.what,
            "postgres connection established"
        );

        Ok(connection)
    }

    /// [`connect`](Self::connect) wrapped in the configured backoff.
    ///
    /// # Errors
    ///
    /// Returns the last attempt's error after `max_attempts` tries.
    pub(crate) async fn connect_with_retry(&self) -> Result<PgConnection, SaciError> {
        let mut last = None;
        for attempt in 0..self.reconnect.max_attempts {
            match self.connect().await {
                Ok(connection) => return Ok(connection),
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
                        connector = self.what,
                        attempt = attempt + 1,
                        max_attempts = self.reconnect.max_attempts,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "postgres connect failed, retrying"
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
                self.what
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
    fn connect_error(&self, e: impl std::fmt::Display) -> SaciError {
        SaciError::generic(format!(
            "{}: cannot connect to {}: {e}",
            self.what, self.target
        ))
    }
}

/// Build `host:port/dbname` from a parsed config, with no credentials.
fn describe_target(pg: &tokio_postgres::Config) -> String {
    let hosts = pg.get_hosts();
    let ports = pg.get_ports();
    let host = match hosts.first() {
        Some(Host::Tcp(name)) => name.clone(),
        #[cfg(unix)]
        Some(Host::Unix(path)) => path.display().to_string(),
        None => "localhost".to_string(),
    };
    let port = ports.first().copied().unwrap_or(5432);
    let dbname = pg.get_dbname().unwrap_or("?");
    format!("{host}:{port}/{dbname}")
}

/// Drive the connection on a task, forwarding notifications.
///
/// Polling `poll_message` rather than awaiting the bare `Connection` future is
/// the only way `AsyncMessage::Notification` becomes observable.
fn spawn_driver<S, T>(client: Client, mut driver: Connection<S, T>) -> PgConnection
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(NOTIFICATION_BUFFER);
    let task = tokio::spawn(async move {
        let mut messages = futures_util::stream::poll_fn(move |cx| driver.poll_message(cx));
        while let Some(message) = messages.next().await {
            match message {
                Ok(AsyncMessage::Notification(notification)) => {
                    if tx.try_send(notification).is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            "postgres notification dropped: the connector's channel is full"
                        );
                    }
                }
                // Notices are server chatter, not data.
                Ok(_) => {}
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %_e, "postgres connection ended");
                    break;
                }
            }
        }
    });

    PgConnection {
        client,
        notifications: rx,
        task: Some(task),
    }
}

/// Build the rustls client configuration.
///
/// Uses `builder_with_provider` rather than `ClientConfig::builder` so the
/// connector cannot panic on "no process-level CryptoProvider" when another
/// dependency has also installed one. Hostname verification is always on.
#[cfg(feature = "tls")]
fn build_client_config(
    what: &'static str,
    cfg: &ConnectionConfig,
) -> Result<rustls::ClientConfig, SaciError> {
    let mut roots = rustls::RootCertStore::empty();

    if let Some(path) = &cfg.sslrootcert {
        use rustls_pki_types::pem::PemObject;

        let pem = std::fs::read(path).map_err(|e| {
            SaciError::configuration(format!(
                "{what}: cannot read connection.sslrootcert '{path}': {e}"
            ))
        })?;
        for certificate in rustls_pki_types::CertificateDer::pem_slice_iter(&pem) {
            let certificate = certificate.map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: connection.sslrootcert '{path}' is not a valid PEM bundle: {e}"
                ))
            })?;
            roots.add(certificate).map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: connection.sslrootcert '{path}' holds an unusable certificate: {e}"
                ))
            })?;
        }
        if roots.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.sslrootcert '{path}' contains no certificates"
            )));
        }
    } else {
        let loaded = rustls_native_certs::load_native_certs();
        if loaded.certs.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: no usable certificates in the OS trust store ({:?}); set \
                 connection.sslrootcert to a PEM bundle",
                loaded.errors
            )));
        }
        let _ = roots.add_parsable_certificates(loaded.certs);
    }

    Ok(
        rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            SaciError::configuration(format!("{what}: cannot build a rustls configuration: {e}"))
        })?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}

/// Render a `tokio_postgres::Error` with the server's own message.
///
/// `Error`'s own `Display` is just "db error"; everything a reader needs sits in
/// the attached `DbError`. Every message in this crate goes through here so a
/// failing statement names the SQLSTATE, the detail and the server's hint.
///
/// An error raised on *this* side of the socket carries no `DbError` at all --
/// a value the encoder refused during a `COPY` reaches the driver as
/// "error serializing parameter N", with the real reason hanging off
/// [`Error::source`](std::error::Error::source) and nothing of it in the
/// `Display`. That reason is the one naming the column, the row and the
/// value, so the chain is walked rather than dropped.
pub(crate) fn pg_detail(e: &tokio_postgres::Error) -> String {
    let Some(db) = e.as_db_error() else {
        return with_causes(e.to_string(), std::error::Error::source(e));
    };
    let mut out = format!("{} [{}]", db.message(), db.code().code());
    if let Some(detail) = db.detail() {
        out.push_str(": ");
        out.push_str(detail);
    }
    if let Some(hint) = db.hint() {
        out.push_str(" (hint: ");
        out.push_str(hint);
        out.push(')');
    }
    out
}

/// Append every `source()` level to `display`, `": "`-joined.
fn with_causes(display: String, source: Option<&(dyn std::error::Error + 'static)>) -> String {
    let mut out = display;
    let mut cause = source;
    while let Some(current) = cause {
        out.push_str(": ");
        out.push_str(&current.to_string());
        cause = current.source();
    }
    out
}

/// Quote a `schema.table` reference, defaulting the schema to `public`.
///
/// Every table and column name in this connector comes from configuration and
/// passes through [`escape_identifier`] before it reaches a statement.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] on more than one `.`, or an empty part.
pub(crate) fn quote_qualified(what: &str, table: &str) -> Result<String, SaciError> {
    let (schema, name) = split_qualified(what, table)?;
    Ok(format!(
        "{}.{}",
        escape_identifier(&schema),
        escape_identifier(&name)
    ))
}

/// Quote one identifier.
pub(crate) fn quote(identifier: &str) -> String {
    escape_identifier(identifier)
}

/// Quote a column list as `"a", "b", "c"`.
pub(crate) fn quote_columns<'a, I>(columns: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    columns
        .into_iter()
        .map(escape_identifier)
        .collect::<Vec<_>>()
        .join(", ")
}

// ------------------------------------------------------- catalog type lookup

/// How deep a chain of domains over domains is followed before giving up.
const MAX_DOMAIN_DEPTH: usize = 8;

/// One `pg_type` row, for a type the canonical table has no entry for.
struct CatalogInfo {
    /// `pg_type.typtype`: `d` domain, `e` enum, `c` composite, `b` base.
    typtype: i8,
    /// `pg_type.typbasetype`, meaningful for a domain.
    basetype: u32,
    /// `pg_type.typtypmod`: the modifier a domain applies to its base type,
    /// `-1` for every other kind. A domain column's `pg_attribute.atttypmod`
    /// is always `-1`, so this is the only place a
    /// `DOMAIN … AS numeric(12,2)` states its own scale.
    typtypmod: i32,
    /// The schema the type lives in.
    schema: String,
    /// `pg_type.typname`.
    name: String,
    /// `format_type`, for error messages.
    display: String,
}

/// The columns of `namespace.relation`, with every type resolved.
///
/// This is the one catalog path both halves share. The source uses it rather
/// than the prepared statement's own result columns, because the statement it
/// executes casts `Wire::Text` columns to `text` and would report `text` for
/// them.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the relation has no visible
/// columns, when a column's type OID is one the catalog does not describe, and
/// when a domain nests deeper than [`MAX_DOMAIN_DEPTH`]; returns
/// [`SaciError::Generic`] when the catalog cannot be read.
pub(crate) async fn column_types(
    client: &Client,
    what: &str,
    namespace: &str,
    relation: &str,
    display: &str,
    target: &str,
) -> Result<Vec<ColumnType>, SaciError> {
    let rows = client
        .query(
            "SELECT a.attname, a.atttypid, pg_catalog.format_type(a.atttypid, a.atttypmod), \
             a.atttypmod \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 \
             AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&namespace, &relation],
        )
        .await
        .map_err(|e| {
            SaciError::generic(format!(
                "{what}: cannot read the columns of '{display}': {}",
                pg_detail(&e)
            ))
        })?;

    if rows.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: table '{display}' does not exist on {target}, or the connecting role cannot \
             see it"
        )));
    }

    // Positions follow the SELECT list above: attname, atttypid,
    // format_type(atttypid, atttypmod), atttypmod.
    let columns: Vec<AttributeType> = rows
        .iter()
        .map(|row| AttributeType {
            name: row.get::<_, String>(0),
            oid: row.get::<_, u32>(1),
            display: row.get::<_, String>(2),
            typmod: row.get::<_, i32>(3),
        })
        .collect();
    resolve_types(client, what, columns).await
}

/// One `pg_attribute` row, before a non-built-in type is resolved.
struct AttributeType {
    /// `attname`.
    name: String,
    /// `atttypid`, the column's own type.
    oid: u32,
    /// `format_type(atttypid, atttypmod)`.
    display: String,
    /// `atttypmod`, or `-1`.
    typmod: i32,
}

/// Replace every non-built-in OID with what the catalog says it is, reading
/// `pg_type` first for the OIDs the canonical table does not know.
async fn resolve_types(
    client: &Client,
    what: &str,
    columns: Vec<AttributeType>,
) -> Result<Vec<ColumnType>, SaciError> {
    let unknown: Vec<u32> = columns
        .iter()
        .map(|column| column.oid)
        .filter(|oid| !is_static_type(*oid))
        .collect();

    let info = if unknown.is_empty() {
        HashMap::new()
    } else {
        catalog_info(client, what, unknown).await?
    };

    resolve_against_catalog(what, columns, &info)
}

/// Settle each column against `info`: a domain becomes its base type's OID and
/// its own modifier, an enum or a composite keeps its `schema.typname`. Every
/// column carries `pg_attribute.atttypid` out unchanged in `attribute_oid`,
/// whatever the OID it decodes as becomes.
///
/// Separate from the query so the domain-chain walk -- the one loop here whose
/// termination is not obvious, and one real PostgreSQL cannot exercise, since
/// its DDL forbids a cyclic `typbasetype` -- is unit-testable from a
/// hand-built `info`.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] for a type OID `info` does not describe
/// and for a domain chain longer than [`MAX_DOMAIN_DEPTH`].
fn resolve_against_catalog(
    what: &str,
    columns: Vec<AttributeType>,
    info: &HashMap<u32, CatalogInfo>,
) -> Result<Vec<ColumnType>, SaciError> {
    columns
        .into_iter()
        .map(|column| {
            let AttributeType {
                name,
                oid,
                display,
                typmod,
            } = column;
            if is_static_type(oid) {
                return Ok(ColumnType {
                    name,
                    oid,
                    attribute_oid: oid,
                    display,
                    // The bare spelling of the very same type: the sink casts
                    // to it and lets the `INSERT` apply the modifier.
                    base_display: unconstrained_type_name(oid),
                    typmod,
                    catalog: None,
                });
            }
            let Some(own) = info.get(&oid) else {
                return Err(SaciError::configuration(format!(
                    "{what}: column '{name}' has type oid {oid}, which the server's catalog does \
                     not describe"
                )));
            };
            // A domain decodes exactly like the type it is built over, so the
            // chain is walked to a base type and that OID is what the decoder
            // and the declared-type check both see. The modifier comes with
            // it: `pg_attribute.atttypmod` is `-1` for a domain column, so a
            // `DOMAIN … AS numeric(12,2)` states its scale only here, and
            // without it the sink's scale check would see an unconstrained
            // `numeric` and let a wider declared scale round.
            let mut decode = oid;
            let mut modifier = typmod;
            let mut depth = 0;
            while let Some(current) = info.get(&decode) {
                if modifier < 0 {
                    modifier = current.typtypmod;
                }
                if current.typtype != b'd' as i8 {
                    break;
                }
                depth += 1;
                if depth > MAX_DOMAIN_DEPTH {
                    return Err(SaciError::configuration(format!(
                        "{what}: column '{name}' is a domain nested more than \
                         {MAX_DOMAIN_DEPTH} deep"
                    )));
                }
                decode = current.basetype;
            }
            // A domain's base type is what the staged value is cast to, so its
            // own spelling is needed too: the base is either built in, and the
            // canonical table names it, or it is itself a catalog type this
            // query already fetched.
            let base_display = info.get(&decode).map_or_else(
                || unconstrained_type_name(decode),
                |base| base.display.clone(),
            );
            Ok(ColumnType {
                name,
                oid: decode,
                attribute_oid: oid,
                display: own.display.clone(),
                base_display,
                typmod: modifier,
                catalog: Some((own.schema.clone(), own.name.clone())),
            })
        })
        .collect()
}

/// Read `pg_type` for `oids`, following domain chains until every base type is
/// either built in or described.
async fn catalog_info(
    client: &Client,
    what: &str,
    oids: Vec<u32>,
) -> Result<HashMap<u32, CatalogInfo>, SaciError> {
    let mut info: HashMap<u32, CatalogInfo> = HashMap::new();
    let mut pending = oids;

    for _ in 0..=MAX_DOMAIN_DEPTH {
        if pending.is_empty() {
            break;
        }
        let rows = client
            .query(
                "SELECT t.oid, t.typtype, t.typbasetype, t.typtypmod, n.nspname, t.typname, \
                 pg_catalog.format_type(t.oid, NULL) \
                 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace \
                 WHERE t.oid = ANY($1)",
                &[&pending],
            )
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "{what}: cannot read pg_type for the columns' types: {}",
                    pg_detail(&e)
                ))
            })?;

        let mut next = Vec::new();
        // Positions follow the SELECT list above.
        for row in &rows {
            let oid: u32 = row.get(0);
            let entry = CatalogInfo {
                typtype: row.get(1),
                basetype: row.get(2),
                typtypmod: row.get(3),
                schema: row.get(4),
                name: row.get(5),
                display: row.get(6),
            };
            if entry.typtype == b'd' as i8
                && entry.basetype != 0
                && !is_static_type(entry.basetype)
                && !info.contains_key(&entry.basetype)
            {
                next.push(entry.basetype);
            }
            info.insert(oid, entry);
        }
        pending = next;
    }

    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ConnectionConfig {
        ConnectionConfig {
            dsn: "postgres://someone:hunter2@db.example:6543/app".to_string(),
            user: None,
            password: None,
            password_file: None,
            application_name: None,
            connect_timeout_ms: 1000,
            statement_timeout_ms: 0,
            sslmode: SslModeConfig::Disable,
            sslrootcert: None,
            reconnect: ReconnectConfig::default(),
        }
    }

    #[test]
    fn target_is_host_port_dbname_with_no_credentials() {
        let connector = Connector::new("PostgresSource", &base()).unwrap();
        assert_eq!(connector.target(), "db.example:6543/app");
        assert!(!connector.target().contains("hunter2"));
        assert!(!connector.target().contains("someone"));
    }

    #[test]
    fn a_dsn_without_a_port_reports_the_default() {
        let mut cfg = base();
        cfg.dsn = "postgres://db.example/app".to_string();
        let connector = Connector::new("PostgresSource", &cfg).unwrap();
        assert_eq!(connector.target(), "db.example:5432/app");
    }

    #[test]
    fn an_unparseable_dsn_is_a_configuration_error_without_the_dsn() {
        let mut cfg = base();
        cfg.dsn = "this is not a dsn".to_string();
        // `Connector` is not `Debug` (the rustls connector is not), so the
        // rejection is destructured rather than unwrapped.
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("an unparseable dsn must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("connection.dsn"),
            "{}",
            err.message()
        );
        assert!(
            !err.message().contains("this is not a dsn"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_missing_password_file_names_the_path() {
        let mut cfg = base();
        cfg.password_file = Some("no/such/secret".to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("a missing password file must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no/such/secret"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn connect_errors_name_the_redacted_target_only() {
        let connector = Connector::new("PostgresSink", &base()).unwrap();
        let message = connector.connect_error("connection refused").message();
        assert!(message.contains("db.example:6543/app"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
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
        let connector = Connector::new("PostgresSource", &cfg).unwrap();
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
        let connector = Connector::new("PostgresSource", &cfg).unwrap();
        for _ in 0..64 {
            let delay = connector.backoff(0).as_millis();
            assert!((900..=1100).contains(&delay), "delay {delay} out of band");
        }
    }

    #[test]
    fn identifiers_are_quoted_and_a_quote_is_doubled() {
        assert_eq!(
            quote_qualified("T", "orders").unwrap(),
            "\"public\".\"orders\""
        );
        assert_eq!(
            quote_qualified("T", "sales.orders").unwrap(),
            "\"sales\".\"orders\""
        );
        assert_eq!(
            quote_qualified("T", "my\"table").unwrap(),
            "\"public\".\"my\"\"table\""
        );
        assert_eq!(quote_columns(["a", "b\"c"]), "\"a\", \"b\"\"c\"");
    }

    #[test]
    fn a_multi_dot_table_is_rejected() {
        let err = quote_qualified("PostgresSink", "a.b.c").unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("a.b.c"), "{}", err.message());
    }

    /// Encode a DER certificate as a PEM block, so a certificate from the OS
    /// trust store can be fed back through `sslrootcert` without committing a
    /// certificate fixture.
    #[cfg(feature = "tls")]
    fn as_pem(der: &[u8]) -> String {
        use base64::Engine;

        let body = base64::engine::general_purpose::STANDARD.encode(der);
        let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
        for chunk in body.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
        pem
    }

    #[cfg(feature = "tls")]
    #[test]
    fn the_default_ssl_mode_builds_a_rustls_config_from_the_os_trust_store() {
        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Prefer;
        let connector = Connector::new("PostgresSource", &cfg).expect("prefer must build TLS");
        assert!(matches!(connector.tls, TlsChoice::Rustls(_)));

        cfg.sslmode = SslModeConfig::Require;
        let connector = Connector::new("PostgresSource", &cfg).expect("require must build TLS");
        assert!(matches!(connector.tls, TlsChoice::Rustls(_)));

        cfg.sslmode = SslModeConfig::Disable;
        let connector = Connector::new("PostgresSource", &cfg).expect("disable needs no TLS");
        assert!(matches!(connector.tls, TlsChoice::None));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_pem_bundle_replaces_the_os_trust_store() {
        let native = rustls_native_certs::load_native_certs();
        let Some(der) = native.certs.first() else {
            eprintln!("SKIP: the OS trust store holds no certificates");
            return;
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("roots.pem");
        std::fs::write(&path, as_pem(der.as_ref())).expect("write pem");

        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some(path.display().to_string());
        let connector =
            Connector::new("PostgresSource", &cfg).expect("a real certificate must load");
        assert!(matches!(connector.tls, TlsChoice::Rustls(_)));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_missing_root_bundle_names_the_path() {
        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some("no/such/roots.pem".to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("a missing sslrootcert must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no/such/roots.pem"),
            "{}",
            err.message()
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_root_bundle_with_no_certificates_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.pem");
        std::fs::write(&path, "# no certificates here\n").expect("write");

        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some(path.display().to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("an empty bundle must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("no certificates"),
            "{}",
            err.message()
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn a_corrupt_pem_block_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("corrupt.pem");
        std::fs::write(
            &path,
            "-----BEGIN CERTIFICATE-----\nnot base64 at all!!\n-----END CERTIFICATE-----\n",
        )
        .expect("write");

        let mut cfg = base();
        cfg.sslmode = SslModeConfig::Require;
        cfg.sslrootcert = Some(path.display().to_string());
        let Err(err) = Connector::new("PostgresSource", &cfg) else {
            panic!("a corrupt PEM block must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("corrupt.pem"), "{}", err.message());
    }

    /// `numeric(12,2)`'s packed `atttypmod`: precision high, scale low, plus
    /// the varlena header length.
    const NUMERIC_12_2: i32 = ((12 << 16) | 2) + 4;
    use crate::types::OID_NUMERIC;

    fn attribute(name: &str, oid: u32, display: &str, typmod: i32) -> AttributeType {
        AttributeType {
            name: name.to_string(),
            oid,
            display: display.to_string(),
            typmod,
        }
    }

    fn domain(basetype: u32, typtypmod: i32, name: &str) -> CatalogInfo {
        CatalogInfo {
            typtype: b'd' as i8,
            basetype,
            typtypmod,
            schema: "public".to_string(),
            name: name.to_string(),
            display: format!("public.{name}"),
        }
    }

    /// A domain column's own `atttypmod` is always -1, so without the
    /// domain's `typtypmod` the sink would see an unconstrained `numeric` and
    /// let a wider declared scale round. The base type's own spelling comes
    /// with it, because that -- not the domain -- is what the sink's staging
    /// route casts to: an explicit cast to the domain would apply the base
    /// modifier with explicit-cast semantics.
    #[test]
    fn a_domain_contributes_its_own_modifier_and_its_base_oid() {
        let mut info = HashMap::new();
        info.insert(9001, domain(OID_NUMERIC, NUMERIC_12_2, "money_amt"));

        let resolved = resolve_against_catalog(
            "PostgresSink",
            vec![attribute("total", 9001, "public.money_amt", -1)],
            &info,
        )
        .expect("resolve");

        assert_eq!(resolved[0].oid, OID_NUMERIC, "decoded as its base type");
        assert_eq!(
            resolved[0].attribute_oid, 9001,
            "`pg_attribute.atttypid` is the domain itself, the OID a pgoutput \
             `Relation` message publishes"
        );
        assert_eq!(resolved[0].typmod, NUMERIC_12_2);
        assert_eq!(
            resolved[0]
                .catalog
                .as_ref()
                .map(|(schema, name)| (schema.as_str(), name.as_str())),
            Some(("public", "money_amt"))
        );
        assert_eq!(
            resolved[0].base_display, "pg_catalog.\"numeric\"",
            "the unconstrained base type, which is the sink's cast target"
        );
        assert_eq!(resolved[0].display, "public.money_amt", "for messages");
    }

    #[test]
    fn a_domain_over_a_domain_walks_to_the_innermost_base_type() {
        let mut info = HashMap::new();
        info.insert(9001, domain(9002, -1, "outer"));
        info.insert(9002, domain(OID_NUMERIC, NUMERIC_12_2, "inner"));

        let resolved = resolve_against_catalog(
            "PostgresSink",
            vec![attribute("total", 9001, "public.outer", -1)],
            &info,
        )
        .expect("resolve");
        assert_eq!(resolved[0].oid, OID_NUMERIC);
        // The modifier comes from whichever link in the chain declares one.
        assert_eq!(resolved[0].typmod, NUMERIC_12_2);
    }

    /// PostgreSQL's DDL cannot build a cyclic domain, so the depth cap is the
    /// only thing standing between a corrupt catalog and a spin.
    #[test]
    fn a_cyclic_or_over_deep_domain_chain_is_refused_by_name() {
        let mut info = HashMap::new();
        info.insert(9001, domain(9001, -1, "loops"));
        let err = resolve_against_catalog(
            "PostgresSink",
            vec![attribute("c", 9001, "public.loops", -1)],
            &info,
        )
        .expect_err("a cycle must not spin");
        assert!(err.message().contains("'c'"), "{}", err.message());
        assert!(
            err.message().contains("nested more than"),
            "{}",
            err.message()
        );

        // A chain longer than the cap, with no cycle at all.
        let mut info = HashMap::new();
        for step in 0..=MAX_DOMAIN_DEPTH as u32 {
            info.insert(9001 + step, domain(9002 + step, -1, "deep"));
        }
        let err = resolve_against_catalog(
            "PostgresSink",
            vec![attribute("c", 9001, "public.deep", -1)],
            &info,
        )
        .expect_err("deeper than the cap");
        assert!(
            err.message().contains("nested more than"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_type_the_catalog_does_not_describe_names_the_column() {
        let err = resolve_against_catalog(
            "PostgresSource",
            vec![attribute("mystery", 9999, "???", -1)],
            &HashMap::new(),
        )
        .expect_err("an undescribed oid cannot be decoded");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'mystery'"), "{}", err.message());
        assert!(err.message().contains("9999"), "{}", err.message());
    }

    /// A built-in column keeps the attribute's own modifier, which is what the
    /// sink compares a forced `pg_type` modifier against.
    #[test]
    fn a_built_in_column_keeps_its_attribute_modifier_and_needs_no_catalog() {
        let resolved = resolve_against_catalog(
            "PostgresSink",
            vec![attribute(
                "total",
                OID_NUMERIC,
                "numeric(12,2)",
                NUMERIC_12_2,
            )],
            &HashMap::new(),
        )
        .expect("resolve");
        assert_eq!(resolved[0].typmod, NUMERIC_12_2);
        assert!(resolved[0].catalog.is_none());
    }

    /// A value the encoder refuses during a `COPY` arrives with its reason on
    /// the source chain and nothing of it in `Display`, so dropping the chain
    /// would lose the column, the row and the value.
    #[test]
    fn a_client_side_error_keeps_the_reason_hanging_off_its_source() {
        #[derive(Debug)]
        struct Layer(&'static str, Option<Box<Layer>>);
        impl std::fmt::Display for Layer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Layer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1
                    .as_deref()
                    .map(|next| next as &(dyn std::error::Error + 'static))
            }
        }

        let inner = Layer("PostgresSink: column 'span' row 7: 1 nanosecond(s)", None);
        let outer = Layer("error serializing parameter 3", Some(Box::new(inner)));
        assert_eq!(
            with_causes(outer.to_string(), std::error::Error::source(&outer)),
            "error serializing parameter 3: PostgresSink: column 'span' row 7: 1 nanosecond(s)"
        );

        // No chain, no change.
        assert_eq!(with_causes("bare".to_string(), None), "bare");
    }
}
