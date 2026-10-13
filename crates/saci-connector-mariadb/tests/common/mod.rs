//! Testcontainers harness for real MariaDB and MySQL servers.
//!
//! Every shared test is an `async fn case(server: &Server)` listed once in the
//! file's [`on_every_server!`], so it runs as `mariadb::case` and
//! `mysql::case`:
//!
//! ```rust,ignore
//! common::on_every_server!(append_writes_every_declared_type_and_reads_back_equal);
//! ```
//!
//! A flavour whose container cannot start soft-skips with a printed `SKIP:`
//! line, so `cargo test --workspace --all-features` still passes on a machine
//! without Docker. Environment:
//!
//! - `SACI_MARIADB_TEST_MARIADB_IMAGE` (default `mariadb:11.8`) and
//!   `SACI_MARIADB_TEST_MYSQL_IMAGE` (default `mysql:8.4`) pick the server
//!   versions; CI sweeps them.
//! - `SACI_MARIADB_TEST_ONLY` = `mariadb` or `mysql` skips the other flavour.
//!
//! Both servers run with the row binlog on (`ROW` format, `FULL` row image)
//! and `server_id` 1; MySQL also runs with GTIDs on, because the `gtid`
//! position mode needs them.
//!
//! The source and sink builders at the end ([`source`], [`sink`], [`drain`])
//! are the only items that depend on the connector's own types.

#![allow(dead_code)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Root password of both images.
const PASSWORD: &str = "saci";
/// Database both images create, and the one every DSN names.
const DATABASE: &str = "saci";
/// Where `try_start_tls` places the certificate and key inside the container.
const TLS_DIR: &str = "/etc/saci-tls";

const MARIADB_IMAGE_VAR: &str = "SACI_MARIADB_TEST_MARIADB_IMAGE";
const MYSQL_IMAGE_VAR: &str = "SACI_MARIADB_TEST_MYSQL_IMAGE";
const ONLY_VAR: &str = "SACI_MARIADB_TEST_ONLY";

/// The server family a test runs against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    /// A MariaDB server.
    Mariadb,
    /// An Oracle MySQL server.
    Mysql,
}

impl Flavor {
    /// Lower-case label, also the accepted `SACI_MARIADB_TEST_ONLY` value.
    pub fn label(self) -> &'static str {
        match self {
            Flavor::Mariadb => "mariadb",
            Flavor::Mysql => "mysql",
        }
    }

    /// Image `name` and `tag`, from the environment or the default.
    fn image(self) -> (String, String) {
        let (var, default) = match self {
            Flavor::Mariadb => (MARIADB_IMAGE_VAR, "mariadb:11.8"),
            Flavor::Mysql => (MYSQL_IMAGE_VAR, "mysql:8.4"),
        };
        let full = std::env::var(var)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| default.to_string());
        match full.rsplit_once(':') {
            Some((name, tag)) if !tag.contains('/') => (name.to_string(), tag.to_string()),
            _ => (full, "latest".to_string()),
        }
    }

    /// The server command line: binlog settings both CDC modes need.
    fn args(self) -> Vec<&'static str> {
        match self {
            Flavor::Mariadb => vec![
                "--log-bin=mariadb-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--server-id=1",
                "--log-bin-trust-function-creators=1",
                "--max-connections=500",
                // Dozens of containers start at once; native AIO would exhaust the
                // host's `fs.aio-max-nr` and fail InnoDB's start (EAGAIN).
                "--innodb-use-native-aio=0",
            ],
            Flavor::Mysql => vec![
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--server-id=1",
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
                "--log-bin-trust-function-creators=1",
                "--max-connections=500",
                // Dozens of containers start at once; native AIO would exhaust the
                // host's `fs.aio-max-nr` and fail InnoDB's start (EAGAIN).
                "--innodb-use-native-aio=0",
            ],
        }
    }

    /// The `(ROOT_PASSWORD, DATABASE)` variable names the image reads.
    fn env_names(self) -> (&'static str, &'static str) {
        match self {
            Flavor::Mariadb => ("MARIADB_ROOT_PASSWORD", "MARIADB_DATABASE"),
            Flavor::Mysql => ("MYSQL_ROOT_PASSWORD", "MYSQL_DATABASE"),
        }
    }
}

impl std::fmt::Display for Flavor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// A running server container plus its host port.
pub struct Server {
    /// Which server family this is.
    pub flavor: Flavor,
    /// Host port mapped to the server's 3306.
    pub port: u16,
    /// Whether the server refuses plaintext connections (`try_start_tls`).
    tls: bool,
    /// Held so the container lives as long as the test.
    _container: ContainerAsync<GenericImage>,
}

impl Server {
    /// URL for the root user, naming the `saci` database.
    pub fn dsn(&self) -> String {
        self.dsn_as("root", PASSWORD)
    }

    /// URL for an arbitrary user, for the privilege tests.
    pub fn dsn_as(&self, user: &str, password: &str) -> String {
        self.dsn_at("127.0.0.1", user, password)
    }

    /// URL for `user` reaching the server as `host`, for the TLS name tests.
    pub fn dsn_at(&self, host: &str, user: &str, password: &str) -> String {
        format!("mysql://{user}:{password}@{host}:{}/{DATABASE}", self.port)
    }

    /// A fresh root connection. Against a TLS-only server it encrypts without
    /// verifying the certificate, since it only seeds and reads fixtures.
    pub async fn conn(&self) -> Conn {
        Conn::new(connect_opts(&self.dsn(), self.tls))
            .await
            .expect("connect to the container")
    }
}

/// Connection options for `dsn`, accepting any certificate when `tls`.
fn connect_opts(dsn: &str, tls: bool) -> OptsBuilder {
    let opts = OptsBuilder::from_opts(Opts::from_url(dsn).expect("the harness DSN parses"))
        .prefer_socket(false);
    if tls {
        // A workspace build unifies both rustls backends, so rustls cannot pick
        // a process default itself; the connector installs ring on its own
        // connections, and the harness's direct ones need the same.
        #[cfg(feature = "tls")]
        let _ = rustls::crypto::ring::default_provider().install_default();
        opts.ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true))
    } else {
        opts
    }
}

/// Start a server, or return `None` with a printed reason.
pub async fn try_start(flavor: Flavor) -> Option<Server> {
    try_start_with(flavor, None).await
}

/// Start a server that serves TLS with the given PEM certificate and key and
/// refuses plaintext connections.
pub async fn try_start_tls(flavor: Flavor, cert_pem: &str, key_pem: &str) -> Option<Server> {
    try_start_with(flavor, Some((cert_pem, key_pem))).await
}

async fn try_start_with(flavor: Flavor, tls: Option<(&str, &str)>) -> Option<Server> {
    if let Ok(only) = std::env::var(ONLY_VAR) {
        let only = only.trim();
        if !only.is_empty() && !only.eq_ignore_ascii_case(flavor.label()) {
            eprintln!("SKIP: {flavor} container not started ({ONLY_VAR}={only})");
            return None;
        }
    }
    match start(flavor, tls).await {
        Ok(server) => Some(server),
        Err(e) => {
            eprintln!("SKIP: {flavor} container unavailable: {e}");
            None
        }
    }
}

async fn start(flavor: Flavor, tls: Option<(&str, &str)>) -> anyhow::Result<Server> {
    let (name, tag) = flavor.image();
    let (password_var, database_var) = flavor.env_names();
    let mut args: Vec<String> = flavor.args().into_iter().map(str::to_string).collect();

    let mut image = GenericImage::new(name, tag)
        .with_exposed_port(3306_u16.tcp())
        // Both images log "ready for connections" for the init-time server
        // too, so this only waits for the first start; the poll below waits for
        // the real one.
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var(password_var, PASSWORD)
        .with_env_var(database_var, DATABASE)
        .with_startup_timeout(Duration::from_secs(180));

    if let Some((cert_pem, key_pem)) = tls {
        // The server runs as an unprivileged user; copied files are owned by
        // root and testcontainers writes in-memory data as 0644, so the
        // server can read the key.
        image = image
            .with_copy_to(format!("{TLS_DIR}/cert.pem"), cert_pem.as_bytes().to_vec())
            .with_copy_to(format!("{TLS_DIR}/key.pem"), key_pem.as_bytes().to_vec());
        args.extend([
            format!("--ssl-ca={TLS_DIR}/cert.pem"),
            format!("--ssl-cert={TLS_DIR}/cert.pem"),
            format!("--ssl-key={TLS_DIR}/key.pem"),
            "--require-secure-transport=ON".to_string(),
        ]);
    }
    let image = image.with_cmd(args);

    let container = image.start().await?;
    let port = container.get_host_port_ipv4(3306_u16.tcp()).await?;
    let server = Server {
        flavor,
        port,
        tls: tls.is_some(),
        _container: container,
    };

    // Both images start a throwaway server to run the init scripts, then
    // restart, so the wait strategy alone can fire too early. Poll until the
    // real server answers.
    let deadline = Instant::now() + Duration::from_secs(120);
    let dsn = server.dsn();
    let mut last = None;
    while Instant::now() < deadline {
        match Conn::new(connect_opts(&dsn, server.tls)).await {
            Ok(mut conn) => {
                let ready = conn.query_drop("SELECT 1").await;
                let _ = conn.disconnect().await;
                match ready {
                    Ok(()) => return Ok(server),
                    Err(e) => last = Some(e),
                }
            }
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    match last {
        Some(e) => Err(anyhow::anyhow!("server never accepted a connection: {e}")),
        None => Err(anyhow::anyhow!("server never accepted a connection")),
    }
}

/// Quote `value` as a configuration string, for building configs in tests.
pub fn quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

/// A fresh connection with a transaction open that has run `statement`, so the
/// row it wrote (and any `AUTO_INCREMENT` value or changelog row that write
/// took) is not committed until the caller sends `COMMIT` or `ROLLBACK`.
pub async fn open_transaction(server: &Server, statement: &str) -> Conn {
    let mut conn = server.conn().await;
    conn.query_drop("BEGIN").await.expect("begin");
    conn.query_drop(statement)
        .await
        .expect("the statement inside the open transaction");
    conn
}

/// Create the user `name` with every privilege a source needs on the test
/// database but no `PROCESS`, and return its DSN. `INNODB_TRX` refuses such a
/// user on both servers, so a source it runs as falls back to its timeout.
pub async fn user_without_process(server: &Server, name: &str) -> String {
    let mut root = server.conn().await;
    root.query_drop(format!("CREATE USER '{name}'@'%' IDENTIFIED BY 'pw'"))
        .await
        .expect("create user");
    root.query_drop(format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, TRIGGER ON {DATABASE}.* TO '{name}'@'%'"
    ))
    .await
    .expect("grant");
    server.dsn_as(name, "pw")
}

/// Run each listed `async fn case(server: &Server)` of the including file
/// against a MariaDB and a MySQL server, as `mariadb::case` and `mysql::case`.
/// A flavour whose container cannot start passes without running the case.
#[allow(unused_macros)]
macro_rules! on_every_server {
    ($($case:ident),* $(,)?) => {
        mod mariadb {
            $(
                #[tokio::test]
                async fn $case() {
                    let Some(server) =
                        crate::common::try_start(crate::common::Flavor::Mariadb).await
                    else {
                        return;
                    };
                    super::$case(&server).await;
                }
            )*
        }
        mod mysql {
            $(
                #[tokio::test]
                async fn $case() {
                    let Some(server) =
                        crate::common::try_start(crate::common::Flavor::Mysql).await
                    else {
                        return;
                    };
                    super::$case(&server).await;
                }
            )*
        }
    };
}
#[allow(unused_imports)]
pub(crate) use on_every_server;

// ============================================================================
// SOURCE AND SINK BUILDERS: depend on `MariadbSource`, `MariadbSink`,
// `MariadbSourceConfig` and `MariadbSinkConfig`.
// ============================================================================

/// Build a source from a KDL fragment plus the server's DSN. The connection
/// block disables TLS; a test that needs it writes its own `connection`.
pub fn source(dsn: &str, body: &str) -> saci_connector_mariadb::MariadbSource {
    use serde::Deserialize as _;
    let text = format!(
        "{body}\n\nconnection dsn={} ssl_mode=\"disabled\"\n",
        quoted(dsn)
    );
    let cfg = saci_connector_mariadb::MariadbSourceConfig::deserialize(
        saci_connector::from_kdl_str(&text).expect("parse kdl"),
    )
    .expect("parse config");
    saci_connector_mariadb::MariadbSource::new(cfg).expect("build source")
}

/// Build a sink from a KDL fragment plus the server's DSN.
pub fn sink(dsn: &str, body: &str) -> saci_connector_mariadb::MariadbSink {
    use serde::Deserialize as _;
    let text = format!(
        "{body}\n\nconnection dsn={} ssl_mode=\"disabled\"\n",
        quoted(dsn)
    );
    let cfg = saci_connector_mariadb::MariadbSinkConfig::deserialize(
        saci_connector::from_kdl_str(&text).expect("parse kdl"),
    )
    .expect("parse config");
    saci_connector_mariadb::MariadbSink::new(cfg).expect("build sink")
}

/// Drain one cycle, returning every batch until `Ok(None)`.
pub async fn drain(
    source: &mut saci_connector_mariadb::MariadbSource,
) -> Vec<arrow_array::RecordBatch> {
    use saci_core::io::source::Source as _;
    let mut batches = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("next_batch") {
        batches.push(batch);
    }
    batches
}
