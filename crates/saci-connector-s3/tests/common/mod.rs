//! Testcontainers harness for a real RustFS S3-compatible server.
//!
//! Every test opens with
//!
//! ```rust,ignore
//! let Some(s3) = common::try_start().await else { return; };
//! ```
//!
//! so the suite soft-skips when no Docker daemon is reachable, matching the
//! PostgreSQL, NATS and Kafka harnesses.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use object_store::ObjectStore;
use testcontainers::core::{ExecCommand, IntoContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use saci_connector_s3::S3ConnectionConfig;

const IMAGE: &str = "rustfs/rustfs";
const TAG: &str = "1.0.0-rc.3";
const ACCESS_KEY: &str = "saciaccesskey";
const SECRET_KEY: &str = "sacisecretkey";

/// A running RustFS container plus its host port and a freshly created bucket.
pub struct S3Container {
    /// Held so the container lives as long as the test.
    _container: ContainerAsync<GenericImage>,
    port: u16,
    bucket: String,
}

impl S3Container {
    /// The service endpoint, `http://127.0.0.1:<host port>`.
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The bucket this container created, unique per start.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The access key the server was started with.
    pub fn access_key(&self) -> &str {
        ACCESS_KEY
    }

    /// The secret key the server was started with.
    pub fn secret_key(&self) -> &str {
        SECRET_KEY
    }

    /// A connection config pointing at this container's bucket, credentials
    /// and HTTP endpoint.
    pub fn connection(&self) -> S3ConnectionConfig {
        S3ConnectionConfig {
            bucket: self.bucket.clone(),
            endpoint: Some(self.endpoint()),
            access_key_id: Some(ACCESS_KEY.to_string()),
            secret_access_key: Some(SECRET_KEY.to_string()),
            allow_http: true,
            ..Default::default()
        }
    }

    /// A store over this container, so assertions can list and get directly
    /// rather than through the connector under test.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.connection().build_store("test").expect("store builds")
    }
}

/// Start RustFS, or return `None` with a printed reason.
pub async fn try_start() -> Option<S3Container> {
    match start().await {
        Ok(container) => Some(container),
        Err(e) => {
            eprintln!("SKIP: rustfs container unavailable: {e}");
            None
        }
    }
}

async fn start() -> anyhow::Result<S3Container> {
    // The image already defaults `RUSTFS_VOLUMES=/data` and declares /data a
    // volume, so a single-disk node needs no mount. No WaitFor log strategy:
    // the image sets `RUSTFS_OBS_LOGGER_LEVEL=warn`, so there is no reliable
    // readiness line; the CreateBucket retry loop below is the readiness probe.
    let image = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(9000_u16.tcp())
        .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
        .with_env_var("RUSTFS_ADDRESS", "0.0.0.0:9000")
        .with_env_var("RUSTFS_CONSOLE_ENABLE", "false");
    let container = image.start().await?;
    let port = container.get_host_port_ipv4(9000_u16.tcp()).await?;

    // Lowercase, 23 chars, inside S3's 3-63 name bound.
    let bucket = format!("saci-{}", nanos_since_epoch());
    // `object_store` exposes no bucket-creation API and its `AwsAuthorizer`
    // signs a private `HttpRequest` type, so the container's own `curl` (which
    // the project's compose healthcheck uses too) does the signing. Both
    // readiness and bucket creation are this one call: a still-booting server
    // just retries, and a persistent failure surfaces as a SKIP.
    let deadline = Instant::now() + Duration::from_secs(90);
    let user = format!("{ACCESS_KEY}:{SECRET_KEY}");
    loop {
        // Inside the container the server listens on 9000; the mapped port
        // exists only on the host, so a probe run through `docker exec` names
        // the container's own port.
        let url = format!("http://127.0.0.1:9000/{bucket}");
        let cmd = ExecCommand::new([
            "curl",
            "-fsS",
            "-o",
            "/dev/null",
            "-X",
            "PUT",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            user.as_str(),
            url.as_str(),
        ]);
        let mut result = container.exec(cmd).await?;
        // The exit code is only final once the exec's output streams have
        // been consumed; reading it straight away reports `None` for a
        // command that has in fact succeeded.
        let _ = result.stdout_to_vec().await;
        let _ = result.stderr_to_vec().await;
        if result.exit_code().await? == Some(0) {
            break;
        }
        if Instant::now() >= deadline {
            anyhow::bail!("rustfs never accepted a signed CreateBucket within 90s");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Ok(S3Container {
        _container: container,
        port,
        bucket,
    })
}

fn nanos_since_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos()
}
