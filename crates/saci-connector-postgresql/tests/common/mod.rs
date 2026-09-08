//! Testcontainers harness for a real PostgreSQL server.
//!
//! Every test opens with
//!
//! ```rust,ignore
//! let Some(pg) = common::try_start().await else { return; };
//! ```
//!
//! so the suite soft-skips when no Docker daemon is reachable. CI runs
//! `cargo test --workspace --all-features`, which must pass on a machine
//! without Docker.
//!
//! The container is started with `wal_level=logical` and spare replication
//! slots, because the `cdc_logical` tests need both. `fsync=off` is safe here
//! and cuts the insert time.

#![allow(dead_code)]

use std::time::{Duration, Instant};

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_postgres::{Client, NoTls};

/// Superuser the image creates, which also carries the REPLICATION attribute.
const USER: &str = "postgres";
const PASSWORD: &str = "saci";
const DBNAME: &str = "postgres";

/// A running PostgreSQL container plus its host port.
pub struct PgContainer {
    /// Held so the container lives as long as the test.
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

impl PgContainer {
    /// libpq connection string for the superuser.
    pub fn dsn(&self) -> String {
        format!(
            "postgres://{USER}:{PASSWORD}@127.0.0.1:{}/{DBNAME}",
            self.port
        )
    }

    /// Connection string for an arbitrary role, for the privilege tests.
    pub fn dsn_as(&self, user: &str, password: &str) -> String {
        format!(
            "postgres://{user}:{password}@127.0.0.1:{}/{DBNAME}",
            self.port
        )
    }

    /// A fresh client, with its driver task spawned.
    pub async fn connect(&self) -> Client {
        let (client, connection) = tokio_postgres::connect(&self.dsn(), NoTls)
            .await
            .expect("connect to the container");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
}

/// Start PostgreSQL, or return `None` with a printed reason.
pub async fn try_start() -> Option<PgContainer> {
    match start().await {
        Ok(container) => Some(container),
        Err(e) => {
            eprintln!("SKIP: postgres container unavailable: {e}");
            None
        }
    }
}

async fn start() -> anyhow::Result<PgContainer> {
    let image = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(5432_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", PASSWORD)
        .with_env_var("POSTGRES_USER", USER)
        .with_env_var("POSTGRES_DB", DBNAME)
        // Replaces the image's default command, so every setting the CDC tests
        // need is explicit here.
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "fsync=off",
        ]);

    let container = image.start().await?;
    let port = container.get_host_port_ipv4(5432_u16.tcp()).await?;

    // initdb logs the readiness line once before the real server starts, so the
    // wait strategy alone can fire too early. Poll until a connection succeeds.
    let deadline = Instant::now() + Duration::from_secs(60);
    let dsn = format!("postgres://{USER}:{PASSWORD}@127.0.0.1:{port}/{DBNAME}");
    let mut last = None;
    while Instant::now() < deadline {
        match tokio_postgres::connect(&dsn, NoTls).await {
            Ok((client, connection)) => {
                let driver = tokio::spawn(async move {
                    let _ = connection.await;
                });
                let ready = client.simple_query("SELECT 1").await.is_ok();
                driver.abort();
                if ready {
                    return Ok(PgContainer {
                        _container: container,
                        port,
                    });
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
