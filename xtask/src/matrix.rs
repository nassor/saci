//! `cargo xtask matrix`: the connector matrix's slices against one shared set
//! of containers.
//!
//! `crates/saci-service/tests/connector_matrix.rs` declares one test per
//! `{source, processor, format}` slice, and nextest runs every test in its own
//! process, so a container started inside a test would be one per slice. This
//! task starts one container per external resource for the whole run instead,
//! and names each endpoint in a `SACI_MATRIX_*` variable the harness
//! (`crates/saci-service/tests/common/matrix.rs`) reads.
//!
//! Steps:
//!
//! 1. Without `--archive-file`, build the two processor fixtures: the
//!    `saci-processor-smoketest` component and the `saci-plugin-smoketest`
//!    cdylib. An archive built with `--profile matrix` carries both.
//! 2. Start Kafka, NATS, PostgreSQL, RustFS, ClickHouse, MariaDB and MySQL
//!    with `docker run`, all at once, each published on a loopback port, then
//!    create the run's S3 bucket through RustFS's own curl, which also waits
//!    out its startup. Every other server is waited for by the slices
//!    themselves.
//! 3. Run `cargo nextest run --profile matrix --run-ignored all` with the
//!    variables set, followed by every argument given here, so `-E`,
//!    `--partition`, `--archive-file` and the rest pass straight through.
//! 4. Remove the containers, whether nextest passed or not. A run killed
//!    with Ctrl-C leaves them running; each carries the `saci.matrix` label,
//!    which `docker ps --filter label=saci.matrix` lists.
//!
//! Exit codes: 10 docker not found, 11 a container did not start or publish
//! its port, 12 the S3 bucket was never created. Any other failure exits with
//! the failing tool's own code, nextest's included.

use std::net::TcpListener;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::sh::{Ctx, Result};

const USAGE: &str = "\
usage: cargo xtask matrix [NEXTEST ARGS...]

Starts one container per external resource the connector matrix needs, then
runs its slices with `cargo nextest run --profile matrix --run-ignored all`
followed by NEXTEST ARGS. With --archive-file, runs the archived binaries
instead of building them.

Examples
  cargo xtask matrix
  cargo xtask matrix -E 'test(/^live::kafka::/)'
  cargo xtask matrix --archive-file connector-matrix.tar.zst -E 'test(/^live::s3::/)'";

/// The label every container this task starts carries.
const LABEL: &str = "saci.matrix";

const POSTGRES_PASSWORD: &str = "saci";
/// The root password of both the MariaDB and the MySQL container.
const MARIADB_PASSWORD: &str = "saci";
const S3_ACCESS_KEY_ID: &str = "saciaccesskey";
const S3_SECRET_ACCESS_KEY: &str = "sacisecretkey";

/// How long RustFS may take to accept the signed CreateBucket.
const BUCKET_BUDGET: Duration = Duration::from_secs(180);

/// Run the matrix.
pub fn run(args: &[String]) -> Result<()> {
    let ctx = Ctx::new("matrix");
    if args
        .first()
        .is_some_and(|arg| arg == "-h" || arg == "--help")
    {
        println!("{USAGE}");
        return Ok(());
    }
    let archived = args
        .iter()
        .any(|arg| arg == "--archive-file" || arg.starts_with("--archive-file="));

    if !archived {
        build_fixtures(&ctx)?;
    }

    ctx.require(
        "docker",
        10,
        "https://docs.docker.com/get-started/get-docker/",
    )?;
    let containers = Containers {
        ctx: &ctx,
        ids: Mutex::new(Vec::new()),
    };
    let env = containers.start()?;

    let mut nextest = ctx.cargo()?.args([
        "nextest",
        "run",
        "--profile",
        "matrix",
        "--run-ignored",
        "all",
    ]);
    if !archived {
        nextest = nextest.args([
            "-p",
            "saci-service",
            "--all-features",
            "--test",
            "connector_matrix",
        ]);
    }
    for (key, value) in &env {
        nextest = nextest.env(key, value);
    }
    nextest.args(args).run()
}

/// Build the wasm component and the native plugin the slices load.
fn build_fixtures(ctx: &Ctx) -> Result<()> {
    ctx.cmd("rustup")?
        .args(["target", "add", "wasm32-wasip2"])
        .run()?;
    ctx.log("building saci-processor-smoketest (wasm32-wasip2, release)...");
    ctx.cargo()?
        .args([
            "build",
            "--release",
            "-p",
            "saci-processor-smoketest",
            "--target",
            "wasm32-wasip2",
        ])
        .run()?;
    ctx.log("building saci-plugin-smoketest...");
    ctx.cargo()?
        .args(["build", "-p", "saci-plugin-smoketest"])
        .run()
}

/// One container this task starts.
struct Service {
    name: &'static str,
    /// The port the server listens on inside the container.
    port: u16,
    /// The host port to publish on, for a server that has to advertise it;
    /// any free one otherwise.
    host_port: Option<u16>,
    /// What follows the publish flag: environment, image and command.
    args: Vec<String>,
}

/// The seven servers, in the order [`Containers::start`] destructures them,
/// with the images and settings the harness's readiness checks and cases
/// expect.
fn services() -> Result<[Service; 7], std::io::Error> {
    // Kafka advertises the address clients reconnect to after bootstrap, so
    // the host port has to be known before the broker starts.
    let kafka_port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
    let env = |pairs: &[(&str, &str)]| -> Vec<String> {
        pairs
            .iter()
            .flat_map(|(key, value)| ["-e".to_owned(), format!("{key}={value}")])
            .collect()
    };
    let with = |mut args: Vec<String>, rest: &[&str]| {
        args.extend(rest.iter().map(|arg| (*arg).to_owned()));
        args
    };
    let advertised = format!("PLAINTEXT://127.0.0.1:{kafka_port}");
    let postgres_password = format!("POSTGRES_PASSWORD={POSTGRES_PASSWORD}");
    Ok([
        Service {
            name: "kafka",
            port: 9092,
            host_port: Some(kafka_port),
            args: with(
                env(&[
                    ("KAFKA_NODE_ID", "1"),
                    ("KAFKA_PROCESS_ROLES", "broker,controller"),
                    ("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093"),
                    ("KAFKA_ADVERTISED_LISTENERS", &advertised),
                    ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
                    (
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                    ),
                    ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"),
                    ("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"),
                    ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
                    ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
                    ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
                    ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
                    ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
                ]),
                &["apache/kafka:3.9.0"],
            ),
        },
        Service {
            name: "nats",
            port: 4222,
            host_port: None,
            args: with(Vec::new(), &["nats:2.11-alpine", "-js", "-sd", "/tmp/nats"]),
        },
        Service {
            name: "postgres",
            port: 5432,
            host_port: None,
            // Every slice of the run connects at once, several connections
            // per PostgreSQL case.
            args: with(
                vec!["-e".to_owned(), postgres_password],
                &[
                    "postgres:18-alpine",
                    "postgres",
                    "-c",
                    "fsync=off",
                    "-c",
                    "max_connections=1000",
                ],
            ),
        },
        Service {
            name: "s3",
            port: 9000,
            host_port: None,
            args: with(
                env(&[
                    ("RUSTFS_ACCESS_KEY", S3_ACCESS_KEY_ID),
                    ("RUSTFS_SECRET_KEY", S3_SECRET_ACCESS_KEY),
                    ("RUSTFS_ADDRESS", "0.0.0.0:9000"),
                    ("RUSTFS_CONSOLE_ENABLE", "false"),
                ]),
                &["rustfs/rustfs:1.0.0-rc.3"],
            ),
        },
        Service {
            name: "clickhouse",
            port: 8123,
            host_port: None,
            // The image pins the `default` user to loopback inside the
            // container unless access management is on, and a published port
            // arrives from the gateway, so every query would be refused with
            // `AUTHENTICATION_FAILED`. Access management opens `::/0` with no
            // password.
            args: with(
                env(&[("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")]),
                &["clickhouse/clickhouse-server:24.8-alpine"],
            ),
        },
        Service {
            name: "mariadb",
            port: 3306,
            host_port: None,
            // Sources read from this server and sinks write into the MySQL one
            // below, so every case holds a connection to each.
            args: with(
                env(&[
                    ("MARIADB_ROOT_PASSWORD", MARIADB_PASSWORD),
                    ("MARIADB_DATABASE", "saci"),
                ]),
                &["mariadb:11.8", "--max-connections=1000"],
            ),
        },
        Service {
            name: "mysql",
            port: 3306,
            host_port: None,
            args: with(
                env(&[
                    ("MYSQL_ROOT_PASSWORD", MARIADB_PASSWORD),
                    ("MYSQL_DATABASE", "saci"),
                ]),
                &["mysql:8.4", "--max-connections=1000"],
            ),
        },
    ])
}

/// The containers of one run, removed when this is dropped.
struct Containers<'a> {
    ctx: &'a Ctx,
    /// Filled as each `docker run` returns, so a container whose port lookup
    /// failed afterwards is still removed.
    ids: Mutex<Vec<String>>,
}

impl Containers<'_> {
    /// Start every service and return the `SACI_MATRIX_*` variables naming
    /// them.
    fn start(&self) -> Result<Vec<(&'static str, String)>> {
        let services = services().map_err(|e| {
            self.ctx
                .error(11, &[&format!("reserving the kafka port: {e}")])
        })?;
        self.ctx
            .log("starting kafka, nats, postgres, s3, clickhouse, mariadb and mysql...");

        // Concurrently, because the first `docker run` of an image pulls it.
        let started: [Result<(String, u16)>; 7] = thread::scope(|scope| {
            services
                .each_ref()
                .map(|service| scope.spawn(move || self.start_one(service)))
                .map(|handle| {
                    handle.join().unwrap_or_else(|_| {
                        Err(self.ctx.error(11, &["a container start thread panicked"]))
                    })
                })
        });
        let [kafka, nats, postgres, s3, clickhouse, mariadb, mysql] = started;
        let (kafka, nats, postgres, (s3_id, s3), clickhouse, mariadb, mysql) = (
            kafka?.1,
            nats?.1,
            postgres?.1,
            s3?,
            clickhouse?.1,
            mariadb?.1,
            mysql?.1,
        );

        let bucket = format!("saci-matrix-{}", nanos());
        self.create_bucket(&s3_id, &bucket)?;

        let env = vec![
            ("SACI_MATRIX_KAFKA_BROKERS", format!("127.0.0.1:{kafka}")),
            ("SACI_MATRIX_NATS_URL", format!("nats://127.0.0.1:{nats}")),
            (
                "SACI_MATRIX_POSTGRES_DSN",
                format!("postgres://postgres:{POSTGRES_PASSWORD}@127.0.0.1:{postgres}/postgres"),
            ),
            ("SACI_MATRIX_S3_ENDPOINT", format!("http://127.0.0.1:{s3}")),
            ("SACI_MATRIX_S3_BUCKET", bucket),
            ("SACI_MATRIX_S3_ACCESS_KEY_ID", S3_ACCESS_KEY_ID.to_owned()),
            (
                "SACI_MATRIX_S3_SECRET_ACCESS_KEY",
                S3_SECRET_ACCESS_KEY.to_owned(),
            ),
            (
                "SACI_MATRIX_CLICKHOUSE_URL",
                format!("http://127.0.0.1:{clickhouse}"),
            ),
            (
                "SACI_MATRIX_MARIADB_DSN",
                format!("mysql://root:{MARIADB_PASSWORD}@127.0.0.1:{mariadb}/saci"),
            ),
            (
                "SACI_MATRIX_MYSQL_DSN",
                format!("mysql://root:{MARIADB_PASSWORD}@127.0.0.1:{mysql}/saci"),
            ),
        ];
        for (key, value) in &env {
            self.ctx.log(format!("{key}={value}"));
        }
        Ok(env)
    }

    /// `docker run` one service and look up the host port it was published
    /// on.
    fn start_one(&self, service: &Service) -> Result<(String, u16)> {
        let publish = match service.host_port {
            Some(host) => format!("127.0.0.1:{host}:{}", service.port),
            None => format!("127.0.0.1::{}", service.port),
        };
        let (ok, stdout) = self
            .ctx
            .cmd("docker")?
            .args(["run", "-d", "--label", LABEL, "-p", &publish])
            .args(&service.args)
            .output_status()?;
        let id = stdout.trim().to_owned();
        if !ok || id.is_empty() {
            return self
                .ctx
                .fail(11, &[&format!("docker run for {} failed", service.name)]);
        }
        lock(&self.ids).push(id.clone());

        let (ok, stdout) = self
            .ctx
            .cmd("docker")?
            .args(["port", &id, &format!("{}/tcp", service.port)])
            .output_status()?;
        // One line per published address, `127.0.0.1:49153`.
        let port = stdout
            .lines()
            .next()
            .and_then(|line| line.rsplit(':').next())
            .and_then(|port| port.trim().parse().ok());
        match port {
            Some(port) if ok => Ok((id, port)),
            _ => self.ctx.fail(
                11,
                &[&format!(
                    "{} published no port for {}/tcp: {stdout}",
                    service.name, service.port
                )],
            ),
        }
    }

    /// Create the run's one bucket through a signed `PUT` from inside the
    /// RustFS container, retried until the server takes it. The container's
    /// own port is used because the published one exists only on the host.
    fn create_bucket(&self, id: &str, bucket: &str) -> Result<()> {
        let user = format!("{S3_ACCESS_KEY_ID}:{S3_SECRET_ACCESS_KEY}");
        let url = format!("http://127.0.0.1:9000/{bucket}");
        let deadline = Instant::now() + BUCKET_BUDGET;
        loop {
            let (ok, output) = self
                .ctx
                .cmd("docker")?
                .args([
                    "exec",
                    id,
                    "curl",
                    "-fsS",
                    "-o",
                    "/dev/null",
                    "-X",
                    "PUT",
                    "--aws-sigv4",
                    "aws:amz:us-east-1:s3",
                    "--user",
                    &user,
                    &url,
                ])
                .output_merged()?;
            if ok {
                self.ctx.log(format!("created bucket {bucket}"));
                return Ok(());
            }
            if Instant::now() >= deadline {
                return self.ctx.fail(
                    12,
                    &[
                        &format!("RustFS did not create bucket {bucket} within {BUCKET_BUDGET:?}"),
                        output.trim(),
                    ],
                );
            }
            thread::sleep(Duration::from_millis(500));
        }
    }
}

impl Drop for Containers<'_> {
    fn drop(&mut self) {
        let ids = std::mem::take(&mut *lock(&self.ids));
        if ids.is_empty() {
            return;
        }
        self.ctx.log(format!("removing {} containers", ids.len()));
        let removed = self
            .ctx
            .cmd("docker")
            .and_then(|docker| docker.args(["rm", "-f"]).args(&ids).output_merged());
        if !matches!(removed, Ok((true, _))) {
            self.ctx.log(format!(
                "could not remove every container; `docker ps --filter label={LABEL}` lists them"
            ));
        }
    }
}

/// Lock through a poisoned mutex: a panicked start thread must not keep its
/// siblings' containers from being removed.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}
