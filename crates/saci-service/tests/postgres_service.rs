//! The whole standalone service path over PostgreSQL: config file, factories,
//! source drain, pipeline run, sink drain.
//!
//! `run_standalone` is what the `saci-service serve` subcommand calls, so this is
//! the end to end proof that a `connector-postgresql` build moves rows from one
//! table through a pipeline and into another. The runtime is a native
//! [`Pipeline`] rather than a WASM processor, because `impl PipelineRuntime for
//! Pipeline` makes one available without a component fixture.
//!
//! Soft-skips without Docker.

#![cfg(all(feature = "service", feature = "connector-postgresql"))]

use std::sync::Arc;

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{SystemMeta, WriteSet, system_fn};
use saci_service::service::builder::{BuiltNodeKind, ServiceBuilder};
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;

const COMPONENT: &str = "Order";
const PASSWORD: &str = "saci";

/// Start PostgreSQL and return its DSN, or `None` when Docker is unavailable.
async fn start_postgres() -> Option<(testcontainers::ContainerAsync<GenericImage>, String)> {
    let image = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(5432_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", PASSWORD)
        .with_cmd(["postgres", "-c", "fsync=off"]);

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: postgres container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(5432_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: cannot map the postgres port: {e}");
            return None;
        }
    };
    let dsn = format!("postgres://postgres:{PASSWORD}@127.0.0.1:{port}/postgres");

    // The readiness line is logged once during initdb, so poll until the real
    // server answers.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if let Ok((client, connection)) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await
        {
            let driver = tokio::spawn(async move {
                let _ = connection.await;
            });
            let ready = client.simple_query("SELECT 1").await.is_ok();
            driver.abort();
            if ready {
                return Some((container, dsn));
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: postgres never accepted a connection");
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

/// A pipeline that doubles `total` in place, so the sink's rows differ from the
/// source's and the run is observable in the output table.
fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("postgres_service");
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

fn config_kdl(dsn: &str, data_dir: &str) -> String {
    let dsn = format!("\"{}\"", dsn.replace('\\', "\\\\").replace('"', "\\\""));
    format!(
        r#"
mode "standalone"

node id=1 name="saci-postgres-service" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "postgres-service-test" {{
    source "orders_in" type="PostgresSource" component="{COMPONENT}" {{
        config {{
            name "orders_in"
            batch_rows 40

            connection {{
                dsn {dsn}
                sslmode "disable"
            }}

            mode kind="polling" table="orders_in" cursor_column="id"

            schema_fields "id" type="int64" nullable=#false
            schema_fields "label" type="utf8"
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="PostgresSink" component="{COMPONENT}" {{
        config {{
            name "orders_out"
            table "orders_out"
            write_mode "upsert"
            conflict_columns "id"

            connection {{
                dsn {dsn}
                sslmode "disable"
            }}

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

#[tokio::test]
async fn a_postgres_source_and_sink_move_rows_through_the_standalone_runner() {
    let Some((_container, dsn)) = start_postgres().await else {
        return;
    };

    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "CREATE TABLE orders_in ( \
                 id bigint PRIMARY KEY, label text, total bigint NOT NULL); \
             CREATE TABLE orders_out ( \
                 id bigint PRIMARY KEY, label text, total bigint NOT NULL); \
             INSERT INTO orders_in \
                 SELECT g, 'order-' || g, g * 10 FROM generate_series(1, 100) g",
        )
        .await
        .expect("fixture");

    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");
    std::fs::write(&path, config_kdl(&dsn, &data_dir)).expect("write config");

    let config = ServiceConfig::load(&path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("service builds")
        .remove(0);

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("one-shot run");
    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 100, "every input row was drained");
    assert_eq!(stats.iteration_errors, 0);
    assert!(stats.sink_batches_written >= 1);

    let rows = client
        .query("SELECT id, label, total FROM orders_out ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 100);
    for (index, row) in rows.iter().enumerate() {
        let id = index as i64 + 1;
        assert_eq!(row.get::<_, i64>(0), id);
        assert_eq!(row.get::<_, &str>(1), format!("order-{id}"));
        assert_eq!(
            row.get::<_, i64>(2),
            id * 20,
            "the pipeline must have doubled the total"
        );
    }

    // A one-shot run never reaches a second drain cycle, and the offset is
    // committed at the *start* of the next cycle, so a rerun replays. That is
    // the documented at-least-once contract, and `write_mode = "upsert"` is
    // what makes the replay harmless.
    let config = ServiceConfig::load(&path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("service builds")
        .remove(0);
    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("second run");
    assert_eq!(
        stats.rows_processed, 100,
        "a one-shot rerun replays: the offset is committed one cycle late"
    );

    let rows = client
        .query("SELECT id, total FROM orders_out ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 100, "the upsert must not duplicate the replay");
    assert_eq!(
        rows[0].get::<_, i64>(1),
        20,
        "and it must not double an already doubled value"
    );

    // A source that does reach a second cycle commits its offset, so a third
    // run moves nothing.
    let mut source_only = ServiceConfig::load(&path).expect("config loads");
    source_only.workflows[0].sinks.clear();
    source_only.workflows[0]
        .links
        .retain(|l| l.from != "orders_out" && l.to != "orders_out");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&source_only)
        .expect("service builds")
        .remove(0);
    let mut source = built
        .nodes
        .into_iter()
        .find_map(|n| match n.kind {
            BuiltNodeKind::Source(s) => Some(s),
            _ => None,
        })
        .expect("source node present");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema());
    let first = saci_core::io::source::drain_into_dataset(source.as_mut(), &mut dataset, COMPONENT)
        .await
        .expect("first cycle");
    dataset.clear();
    let second =
        saci_core::io::source::drain_into_dataset(source.as_mut(), &mut dataset, COMPONENT)
            .await
            .expect("second cycle");
    assert_eq!(first, 100);
    assert_eq!(second, 0, "the second cycle is caught up");

    let config = ServiceConfig::load(&path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("service builds")
        .remove(0);
    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("third run");
    assert_eq!(
        stats.rows_processed, 0,
        "the committed offset must stop the third run replaying"
    );
}

/// The doubling system must actually see the column, or the assertion above
/// would pass on an untouched value.
#[test]
fn the_pipeline_declares_the_component_the_config_names() {
    let pipeline = build_pipeline();
    let declared = saci_core::runtime::PipelineRuntime::declared_components(&pipeline);
    assert!(declared.contains(&COMPONENT), "declared: {declared:?}");
    let schema = schema();
    let fields: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(fields, vec!["id", "label", "total"]);
}
