//! Testcontainers harness for a real ClickHouse server.
//!
//! Every test opens with
//!
//! ```rust,ignore
//! let Some(ch) = common::try_start().await else { return; };
//! ```
//!
//! so the suite soft-skips when no Docker daemon is reachable.

#![allow(dead_code)]

use std::time::{Duration, Instant};

use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// A running ClickHouse container plus its HTTP URL.
pub struct ChContainer {
    _container: ContainerAsync<GenericImage>,
    pub url: String,
    pub port: u16,
}

impl ChContainer {
    /// Creates a [`clickhouse::Client`] targeting this container.
    pub fn client(&self) -> clickhouse::Client {
        clickhouse::Client::default().with_url(&self.url)
    }
}

/// Start ClickHouse, or return `None` with a printed reason.
pub async fn try_start() -> Option<ChContainer> {
    match start().await {
        Ok(container) => Some(container),
        Err(e) => {
            eprintln!("SKIP: clickhouse container unavailable: {e}");
            None
        }
    }
}

async fn start() -> anyhow::Result<ChContainer> {
    // The image pins the `default` user to `127.0.0.1`/`::1` unless a
    // user/password or access management is configured, and a published port
    // reaches the container from the gateway, so the connection below would be
    // refused with `AUTHENTICATION_FAILED`. Setting access management opens
    // `<ip>::/0</ip>` and needs no password.
    //
    // No log wait either: ClickHouse writes `Ready for connections` to a file
    // inside the container, not to its stdout or stderr, so a log-wait strategy
    // here would time out with the server already up. The `SELECT 1` loop below
    // is the readiness gate instead.
    let image = GenericImage::new("clickhouse/clickhouse-server", "24.8-alpine")
        .with_exposed_port(8123_u16.tcp())
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");

    let container = image.start().await?;
    let port = container.get_host_port_ipv4(8123_u16.tcp()).await?;
    let url = format!("http://127.0.0.1:{port}");

    let client = clickhouse::Client::default().with_url(&url);
    // Wider than a light container's: ClickHouse is the largest of the images
    // and the slowest to answer a query on a cold, loaded runner.
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut last_err = None;

    while Instant::now() < deadline {
        match client.query("SELECT 1").execute().await {
            Ok(()) => {
                return Ok(ChContainer {
                    _container: container,
                    url,
                    port,
                });
            }
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    match last_err {
        Some(e) => Err(anyhow::anyhow!(
            "clickhouse server never accepted a query: {e}"
        )),
        None => Err(anyhow::anyhow!("clickhouse server timed out")),
    }
}
