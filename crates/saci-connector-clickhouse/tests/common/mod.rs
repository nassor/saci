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

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

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
    let image = GenericImage::new("clickhouse/clickhouse-server", "24.8-alpine")
        .with_exposed_port(8123_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Ready for connections"));

    let container = image.start().await?;
    let port = container.get_host_port_ipv4(8123_u16.tcp()).await?;
    let url = format!("http://127.0.0.1:{port}");

    let client = clickhouse::Client::default().with_url(&url);
    let deadline = Instant::now() + Duration::from_secs(60);
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
