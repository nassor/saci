//! The whole standalone service path over ClickHouse: config file, factories,
//! source drain, pipeline run, sink drain.
//!
//! Soft-skips without Docker.

#![cfg(all(feature = "service", feature = "connector-clickhouse"))]

use std::sync::Arc;

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{SystemMeta, WriteSet, system_fn};
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;
use testcontainers::GenericImage;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;

const COMPONENT: &str = "Order";

/// Start ClickHouse and return its URL, or `None` when Docker is unavailable.
async fn start_clickhouse() -> Option<(testcontainers::ContainerAsync<GenericImage>, String)> {
    let image = GenericImage::new("clickhouse/clickhouse-server", "24.8-alpine")
        .with_exposed_port(8123_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Ready for connections"));

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: clickhouse container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(8123_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: cannot map the clickhouse port: {e}");
            return None;
        }
    };
    let url = format!("http://127.0.0.1:{port}");

    let client = clickhouse::Client::default().with_url(&url);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if client.query("SELECT 1").execute().await.is_ok() {
            return Some((container, url));
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: clickhouse never accepted a query");
    None
}

/// The component schema both the source and the sink speak.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("total", DataType::Int64, false),
    ]))
}

/// A pipeline that doubles `total` in place.
fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("clickhouse_service");
    pipeline.data.register_raw_component(COMPONENT, schema());
    pipeline.add_system(system_fn(
        SystemMeta::new("double_total")
            .read(COMPONENT, "total")
            .write(COMPONENT, "total"),
        |data: &mut Dataset| {
            let Some(batch) = data.batch_for(COMPONENT) else {
                return Ok(());
            };
            if batch.num_rows() == 0 {
                return Ok(());
            }
            let totals = batch
                .column_by_name("total")
                .expect("total column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 total");
            let doubled: Int64Array = totals.iter().map(|v| v.map(|v| v * 2)).collect();
            data.apply_write_set(WriteSet::new().put(COMPONENT, "total", Arc::new(doubled)))
        },
    ));
    pipeline
}

fn config_kdl(url: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-clickhouse-service" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "clickhouse-service-test" {{
    source "orders_in" type="ClickhouseSource" component="{COMPONENT}" {{
        config {{
            name "orders_in"
            batch_rows 40

            connection url="{url}" database="default"

            mode kind="polling" table="orders_in" cursor_column="id"

            schema_fields "id" type="int64" nullable=#false
            schema_fields "label" type="utf8"
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="ClickhouseSink" component="{COMPONENT}" {{
        config {{
            name "orders_out"
            table "orders_out"
            truncate_before_first_write #true

            connection url="{url}" database="default"

            schema_fields "id" type="int64" nullable=#false
            schema_fields "label" type="utf8"
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    link from="orders_in" to="transform"
    link from="transform" to="orders_out"
}}

http disabled=#true

observability log_level="warn"
"#
    )
}

#[test]
fn test_clickhouse_service_config_builds_without_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");
    std::fs::write(&path, config_kdl("http://127.0.0.1:8123", &data_dir)).expect("write config");

    let config = ServiceConfig::load(&path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config);
    assert!(
        built.is_ok(),
        "ClickHouse service config builds cleanly without database connection"
    );
}

#[test]
fn test_clickhouse_service_config_invalid_url_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");
    std::fs::write(&path, config_kdl("tcp://127.0.0.1:9000", &data_dir)).expect("write config");

    let config = ServiceConfig::load(&path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config);
    assert!(
        built.is_err(),
        "ClickHouse service config with invalid url must be rejected at build time"
    );
}

#[tokio::test]
async fn a_clickhouse_source_and_sink_move_rows_through_the_standalone_runner() {
    let Some((_container, url)) = start_clickhouse().await else {
        return;
    };

    let client = clickhouse::Client::default().with_url(&url);

    client
        .query(
            "CREATE TABLE IF NOT EXISTS orders_in (
                id Int64,
                label String,
                total Int64
            ) ENGINE = MergeTree ORDER BY id",
        )
        .execute()
        .await
        .expect("create orders_in");

    client
        .query(
            "CREATE TABLE IF NOT EXISTS orders_out (
                id Int64,
                label String,
                total Int64
            ) ENGINE = MergeTree ORDER BY id",
        )
        .execute()
        .await
        .expect("create orders_out");

    // Seed 100 rows
    let mut insert_vals = Vec::new();
    for i in 1..=100 {
        insert_vals.push(format!("({i}, 'order-{i}', {})", i * 10));
    }
    let insert_sql = format!(
        "INSERT INTO orders_in (id, label, total) VALUES {}",
        insert_vals.join(", ")
    );
    client
        .query(&insert_sql)
        .execute()
        .await
        .expect("seed orders_in");

    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");
    std::fs::write(&path, config_kdl(&url, &data_dir)).expect("write config");

    let config = ServiceConfig::load(&path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("service builds")
        .remove(0);

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("standalone run succeeds");

    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 100, "all 100 rows processed");
    assert_eq!(stats.iteration_errors, 0);

    // Read back from orders_out
    let bytes = client
        .query("SELECT id, label, total FROM orders_out ORDER BY id")
        .fetch_bytes("ArrowStream")
        .expect("fetch_bytes")
        .collect()
        .await
        .expect("collect bytes");

    let cursor = std::io::Cursor::new(bytes);
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None).expect("StreamReader");
    let mut count = 0;
    for batch in reader {
        let b = batch.expect("batch");
        let id_col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let total_col = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for i in 0..b.num_rows() {
            count += 1;
            let id = count as i64;
            assert_eq!(id_col.value(i), id);
            assert_eq!(total_col.value(i), id * 20, "pipeline doubled the total");
        }
    }
    assert_eq!(count, 100, "all 100 doubled rows read back from orders_out");
}
