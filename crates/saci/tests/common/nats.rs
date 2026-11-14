//! Testcontainers harness for a real single-node NATS server with JetStream on.
//!
//! Every test opens with
//!
//! ```rust,ignore
//! let Some(nats) = common::try_start().await else { return; };
//! ```
//!
//! so the suite soft-skips when no Docker daemon is reachable. CI runs
//! `cargo test --workspace --all-features`, which must pass on a machine
//! without Docker.
//!
//! The server runs with `-js`, so JetStream is available, and `-sd /tmp/nats`
//! puts its store inside the container's writable layer.

#![allow(dead_code)]

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_nats::jetstream;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// A running NATS server plus its host port.
pub struct NatsContainer {
    /// Held so the container lives as long as the test.
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

impl NatsContainer {
    /// The server URL a `[connection] servers` entry uses.
    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    /// A subject unique to this test run.
    pub fn subject(&self, stem: &str) -> String {
        format!("{stem}.{}", unique_suffix())
    }

    /// A stream name unique to this test run. Stream names admit no `.`, so
    /// this joins with `_`.
    pub fn stream(&self, stem: &str) -> String {
        format!("{stem}_{}", unique_suffix())
    }

    /// A client of this server, for a test that reads or writes raw messages.
    pub async fn client(&self) -> async_nats::Client {
        async_nats::connect(self.url())
            .await
            .expect("connect to the container")
    }

    /// A JetStream context on this server.
    pub async fn jetstream(&self) -> jetstream::Context {
        jetstream::new(self.client().await)
    }
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos()
}

/// Start a NATS server, or return `None` with a printed reason.
pub async fn try_start() -> Option<NatsContainer> {
    match start().await {
        Ok(container) => Some(container),
        Err(e) => {
            eprintln!("SKIP: nats container unavailable: {e}");
            None
        }
    }
}

async fn start() -> anyhow::Result<NatsContainer> {
    // No `with_wait_for`: a real client connect plus a JetStream API round-trip
    // is a stricter readiness gate than any log line, and `-js` enables the
    // JetStream subsystem after the client port is already open.
    // 2.12 is the floor for atomic batch publishes, which
    // `jetstream_atomic_batch_*` exercises; every earlier behavior this suite
    // covers is unchanged there.
    let image = GenericImage::new("nats", "2.12-alpine")
        .with_exposed_port(4222_u16.tcp())
        .with_cmd(["-js", "-sd", "/tmp/nats"]);

    let container = image.start().await?;
    let port = container.get_host_port_ipv4(4222_u16.tcp()).await?;
    let url = format!("nats://127.0.0.1:{port}");

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = None;
    while Instant::now() < deadline {
        match async_nats::connect(&url).await {
            Ok(client) => {
                // Any reply proves the JetStream subsystem answers, the
                // not-found error included: only a timeout means it is still
                // starting.
                match jetstream::new(client).get_stream("SACI_PROBE").await {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("timed out") => {
                        last = Some(e.to_string());
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                    Err(_) => {}
                }
                return Ok(NatsContainer {
                    _container: container,
                    port,
                });
            }
            Err(e) => last = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(anyhow::anyhow!(
        "server never accepted a connection: {}",
        last.unwrap_or_default()
    ))
}
