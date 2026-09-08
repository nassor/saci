//! The whole standalone service path over S3: config file, factories, source
//! drain, pipeline run, sink drain.
//!
//! `run_standalone` is what the `saci-service serve` subcommand calls, so this is
//! the end to end proof that a `connector-s3` build moves rows from one S3
//! prefix through a pipeline into another via the registered
//! `S3SourceFactory`/`S3SinkFactory`, not just via the connector crate's own
//! direct API (already covered by `saci-connector-s3`'s own tests).
//!
//! The first test needs no daemon: neither factory opens a connection at build
//! time, so a config pointing at an unreachable endpoint still builds. The
//! second soft-skips without Docker.

#![cfg(all(feature = "service", feature = "connector-s3"))]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use saci_connector_s3::{
    Flush, S3ConnectionConfig, S3Sink, S3SinkConfig, S3Source, S3SourceConfig, SchemaFrom,
};
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use testcontainers::core::{ExecCommand, IntoContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;

use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;
use saci_transformer_ndjson::NdjsonTransformer;

const COMPONENT: &str = "Order";

/// The component schema both the source and the sink speak.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
    ]))
}

/// A passthrough pipeline: it registers the component and runs no systems, so
/// this test proves the registry-driven IO path, not transform logic (already
/// covered by `saci-connector-s3`'s own tests).
fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("s3_service");
    pipeline.data.register_raw_component(COMPONENT, schema());
    pipeline
}

fn config_kdl(connection: &S3ConnectionConfig, in_prefix: &str, out_prefix: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-s3-service" data_dir="/tmp/saci-s3-service"

run_mode kind="one_shot"

workflow "s3-service-test" {{
    transformer "ndjson_fmt" format="ndjson"

    source "orders_in" type="S3Source" component="{COMPONENT}" transformer="ndjson_fmt" {{
        config {{
            prefix "{in_prefix}"

            connection {{
                bucket "{bucket}"
                endpoint "{endpoint}"
                access_key_id "{access_key}"
                secret_access_key "{secret_key}"
                allow_http #true
            }}

            schema_fields "id" type="int64" nullable=#false
            schema_fields "name" type="utf8"
            schema_fields "total" type="float64"
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="S3Sink" component="{COMPONENT}" transformer="ndjson_fmt" {{
        config {{
            prefix "{out_prefix}"
            suffix ".ndjson"
            flush max_rows=0 max_bytes=0 max_age_ms=0

            connection {{
                bucket "{bucket}"
                endpoint "{endpoint}"
                access_key_id "{access_key}"
                secret_access_key "{secret_key}"
                allow_http #true
            }}

            schema_fields "id" type="int64" nullable=#false
            schema_fields "name" type="utf8"
            schema_fields "total" type="float64"
        }}
    }}

    link from="orders_in" to="transform"
    link from="transform" to="orders_out"
}}

http disabled=#true

observability log_level="warn"
"#,
        bucket = connection.bucket,
        endpoint = connection.endpoint.as_deref().unwrap_or(""),
        access_key = connection.access_key_id.as_deref().unwrap_or(""),
        secret_key = connection.secret_access_key.as_deref().unwrap_or(""),
    )
}

/// Neither factory opens a connection at build time, so a config naming an S3
/// source and sink against an unreachable endpoint must still build. No daemon
/// needed.
#[tokio::test]
async fn the_s3_factories_build_without_reaching_the_endpoint() {
    let connection = S3ConnectionConfig {
        bucket: "test".to_string(),
        endpoint: Some("http://127.0.0.1:1".to_string()),
        access_key_id: Some("key".to_string()),
        secret_access_key: Some("secret".to_string()),
        allow_http: true,
        ..Default::default()
    };
    let raw = config_kdl(&connection, "in", "out");
    let path = tempfile::NamedTempFile::new().expect("temp config");
    std::fs::write(path.path(), &raw).expect("write config");
    let config = ServiceConfig::load(path.path()).expect("config loads");
    register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("service builds without reaching the endpoint");
}

/// Start a RustFS container, or return `None` with a printed reason.
async fn start_rustfs() -> Option<(ContainerAsync<GenericImage>, S3ConnectionConfig)> {
    const ACCESS_KEY: &str = "saciaccesskey";
    const SECRET_KEY: &str = "sacisecretkey";

    let image = GenericImage::new("rustfs/rustfs", "1.0.0-rc.3")
        .with_exposed_port(9000_u16.tcp())
        .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
        .with_env_var("RUSTFS_ADDRESS", "0.0.0.0:9000")
        .with_env_var("RUSTFS_CONSOLE_ENABLE", "false");
    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: rustfs container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(9000_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: rustfs container port unavailable: {e}");
            return None;
        }
    };
    let bucket = format!("saci-{}", nanos_since_epoch());
    let connection = S3ConnectionConfig {
        bucket,
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        access_key_id: Some(ACCESS_KEY.to_string()),
        secret_access_key: Some(SECRET_KEY.to_string()),
        allow_http: true,
        ..Default::default()
    };

    // Readiness is the retry loop: a signed CreateBucket (via the container's
    // own curl, which the project's compose healthcheck uses too) succeeds only
    // once the server is up. It also creates the bucket the test uses.
    let deadline = Instant::now() + Duration::from_secs(90);
    let user = format!("{ACCESS_KEY}:{SECRET_KEY}");
    loop {
        // Inside the container the server listens on 9000; the mapped port
        // exists only on the host, so a probe run through `docker exec` names
        // the container's own port.
        let url = format!("http://127.0.0.1:9000/{}", connection.bucket);
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
        let mut result = match container.exec(cmd).await {
            Ok(result) => result,
            Err(e) => {
                eprintln!("SKIP: rustfs exec failed: {e}");
                return None;
            }
        };
        // The exit code is only final once the exec's output streams have been
        // consumed; reading it straight away reports `None` for a command that
        // has in fact succeeded.
        let _ = result.stdout_to_vec().await;
        let _ = result.stderr_to_vec().await;
        match result.exit_code().await {
            Ok(Some(0)) => return Some((container, connection)),
            Ok(_) => {}
            Err(e) => {
                eprintln!("SKIP: rustfs exec exit code unavailable: {e}");
                return None;
            }
        }
        if Instant::now() >= deadline {
            eprintln!("SKIP: rustfs never accepted a signed CreateBucket within 90s");
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn nanos_since_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos()
}

#[tokio::test]
async fn an_s3_source_and_sink_move_rows_through_the_standalone_runner() {
    let Some((_container, connection)) = start_rustfs().await else {
        return;
    };
    let in_prefix = unique("saci_service_in");
    let out_prefix = unique("saci_service_out");
    let schema = schema();

    // Seed the input prefix directly through the connector, not the registry:
    // the registry path is what this test exists to prove, so seeding must not
    // depend on it.
    let mut seed_sink = S3Sink::new(
        S3SinkConfig {
            connection: connection.clone(),
            prefix: in_prefix.clone(),
            suffix: ".ndjson".to_string(),
            flush: Flush {
                max_rows: 0,
                max_bytes: 0,
                max_age_ms: 0,
            },
            schema_fields: vec![],
        },
        schema.clone(),
        Arc::new(NdjsonTransformer::default()),
    )
    .expect("seed sink builds");
    let seed_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
        ],
    )
    .expect("valid batch");
    {
        use saci_core::io::sink::Sink;
        seed_sink
            .write_batch(&seed_batch)
            .await
            .expect("seed write");
        seed_sink.finish().await.expect("seed finish");
    }

    let path = tempfile::NamedTempFile::new().expect("temp config");
    std::fs::write(
        path.path(),
        config_kdl(&connection, &in_prefix, &out_prefix),
    )
    .expect("write config");
    let config = ServiceConfig::load(path.path()).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("service builds")
        .remove(0);

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("one-shot run");
    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 3, "every seeded row was drained");
    assert_eq!(stats.iteration_errors, 0);

    // Read the output prefix back through the connector, proving the
    // registry-built `S3Sink` actually reached the server.
    let mut read_source = S3Source::new(
        S3SourceConfig {
            connection,
            prefix: out_prefix,
            schema_from: SchemaFrom::Config,
            schema_fields: vec![],
        },
        schema.clone(),
        Arc::new(NdjsonTransformer::default()),
    )
    .expect("readback source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema);
    let rows = saci_core::io::source::drain_into_dataset(&mut read_source, &mut dataset, COMPONENT)
        .await
        .expect("drain output prefix");
    assert_eq!(rows, 3);

    let out = dataset.batch_for(COMPONENT).expect("component present");
    let ids = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64");
    assert_eq!(ids.values(), &[1, 2, 3]);
}

/// The pipeline must actually declare the component the config names, or the
/// assertions above would pass on an empty run.
#[test]
fn the_pipeline_declares_the_component_the_config_names() {
    let pipeline = build_pipeline();
    let declared = saci_core::runtime::PipelineRuntime::declared_components(&pipeline);
    assert!(declared.contains(&COMPONENT), "declared: {declared:?}");
}

fn unique(stem: &str) -> String {
    format!("{stem}-{}", nanos_since_epoch())
}
