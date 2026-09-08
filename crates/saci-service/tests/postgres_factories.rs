//! The config to factory path for `PostgresSource` and `PostgresSink`.
//!
//! No database: both factories are synchronous and open no connection, so this
//! exercises exactly what `saci-service validate` does — env-var substitution,
//! parsing, cross-field validation, and schema construction.

#![cfg(all(feature = "service", feature = "connector-postgresql"))]

use std::path::PathBuf;

use arrow_schema::{DataType, TimeUnit};
use saci_core::pipeline::Pipeline;
use saci_service::service::builder::{BuiltNodeKind, ServiceBuilder};
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;

/// Write `body` to a temp file and load it as a service config.
fn load(body: &str) -> Result<ServiceConfig, saci_core::error::SaciError> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path: PathBuf = dir.path().join("service.kdl");
    std::fs::write(&path, body).expect("write config");
    ServiceConfig::load(&path)
}

fn config_body(data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-postgres-test" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "postgres-factories-test" {{
    source "pg_orders" type="PostgresSource" component="OrderChange" {{
        config {{
            name "pg_orders"
            batch_rows 256

            connection {{
                dsn "${{SACI_TEST_PG_DSN}}"
                sslmode "disable"
            }}

            mode kind="cdc_logical" slot="saci_test_slot" publication="saci_test_pub" table="public.orders"

            schema_fields "__op" type="utf8" nullable=#false
            schema_fields "__commit_ts" type="timestamp_micros_utc"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "amount" type="decimal128" precision=18 scale=4
        }}
    }}

    wasm "transform" name="Transform"

    sink "pg_enriched" type="PostgresSink" component="EnrichedOrder" {{
        config {{
            name "pg_enriched"
            table "public.enriched_orders"
            write_mode "upsert"
            conflict_columns "id"

            connection {{
                dsn "${{SACI_TEST_PG_DSN}}"
                sslmode "disable"
            }}

            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="decimal128" precision=12 scale=2
        }}
    }}

    link from="pg_orders" to="transform"
    link from="transform" to="pg_enriched"
}}

http disabled=#true
"#
    )
}

/// `${VAR}` substitution happens over the raw text before parsing, so the DSN
/// never has to appear in the file. The test sets it for the whole process.
fn set_dsn() {
    // SAFETY: single-threaded test setup, before any source is constructed.
    unsafe {
        std::env::set_var(
            "SACI_TEST_PG_DSN",
            "postgres://someone:hunter2@db.example:5432/app",
        );
    }
}

#[test]
fn a_declared_source_and_sink_are_built_with_their_declared_schemas() {
    set_dsn();
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let config = load(&config_body(&data_dir)).expect("config loads");

    // `impl PipelineRuntime for Pipeline` means no wasm fixture is needed.
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(Pipeline::new("test")))
        .build_all(&config)
        .expect("service builds")
        .remove(0);

    let source_node = built
        .nodes
        .iter()
        .find(|n| matches!(n.kind, BuiltNodeKind::Source(_)))
        .expect("source node");
    let sink_node = built
        .nodes
        .iter()
        .find(|n| matches!(n.kind, BuiltNodeKind::Sink(_)))
        .expect("sink node");
    assert_eq!(source_node.id, "pg_orders");
    assert_eq!(source_node.component, Some("OrderChange"));
    assert_eq!(sink_node.id, "pg_enriched");
    assert_eq!(sink_node.component, Some("EnrichedOrder"));

    let source_schema = match &source_node.kind {
        BuiltNodeKind::Source(s) => s.schema(),
        _ => unreachable!(),
    };
    let names: Vec<&str> = source_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, vec!["__op", "__commit_ts", "id", "amount"]);
    assert_eq!(source_schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        source_schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(source_schema.field(2).data_type(), &DataType::Int64);
    assert_eq!(
        source_schema.field(3).data_type(),
        &DataType::Decimal128(18, 4)
    );
    assert!(!source_schema.field(0).is_nullable());
    assert!(source_schema.field(1).is_nullable());

    let sink_schema = match &sink_node.kind {
        BuiltNodeKind::Sink(s) => s.schema(),
        _ => unreachable!(),
    };
    assert_eq!(sink_schema.fields().len(), 2);
    assert_eq!(sink_schema.field(1).name(), "total");
    assert_eq!(
        sink_schema.field(1).data_type(),
        &DataType::Decimal128(12, 2)
    );
}

#[test]
fn a_misspelled_key_fails_the_build_as_a_configuration_error() {
    set_dsn();
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let body = config_body(&data_dir).replace("batch_rows 256", "batch_row 256");
    let config = load(&body).expect("config still parses: the typo is inside the config node");

    // `BuiltService` is not `Debug`, so the rejection is destructured.
    let Err(err) = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(Pipeline::new("test")))
        .build_all(&config)
    else {
        panic!("the typo must fail the build");
    };
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("batch_row"), "{}", err.message());
}

#[test]
fn a_cross_field_violation_fails_the_build() {
    set_dsn();
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    // `notify` is meaningless for cdc_logical: the slot interface has no channel.
    let body = config_body(&data_dir).replace(
        "batch_rows 256",
        "batch_rows 256\n\n        notify channel=\"c\"",
    );
    let config = load(&body).expect("config loads");

    let Err(err) = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(Pipeline::new("test")))
        .build_all(&config)
    else {
        panic!("the mode/notify combination must be rejected");
    };
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("cdc_logical"), "{}", err.message());
}

/// The shipped `examples/configs/postgresql.kdl` must parse and its two IO
/// tables must be accepted by the factories.
///
/// Gated on `wasm` because the example declares a `wasm` node, which the
/// config parser only knows about with that feature on. The factories are
/// driven directly rather than through `ServiceBuilder::build_all`, which would
/// also try to load the module the example points at.
#[cfg(feature = "wasm")]
#[test]
fn the_shipped_example_config_loads_and_its_io_tables_are_accepted() {
    use saci_service::service::ConnectorContext;
    use saci_service::service::factories::{PostgresSinkFactory, PostgresSourceFactory};
    use saci_service::service::registry::{SinkFactory, SourceFactory};

    // SAFETY: single-threaded test setup.
    unsafe {
        std::env::set_var("SACI_PG_DSN", "postgres://saci@localhost:5432/app");
    }
    let config = ServiceConfig::load("../../examples/configs/postgresql.kdl")
        .expect("the shipped example must load");
    assert_eq!(config.workflows[0].sources.len(), 1);
    assert_eq!(config.workflows[0].sinks.len(), 1);
    assert_eq!(config.workflows[0].sources[0].type_name, "PostgresSource");
    assert_eq!(config.workflows[0].sinks[0].type_name, "PostgresSink");

    // PostgreSQL declares its own schema, so it reads no `format` key and no
    // bound transformer is needed.
    let ctx = ConnectorContext::new(None);
    let source = PostgresSourceFactory
        .build(&config.workflows[0].sources[0].config, &ctx)
        .expect("the example source table must be accepted");
    let sink = PostgresSinkFactory
        .build(&config.workflows[0].sinks[0].config, &ctx)
        .expect("the example sink table must be accepted");
    // Three reserved cdc_logical columns plus the example's five declared
    // ones, against the sink's five.
    assert_eq!(source.schema().fields().len(), 8);
    assert_eq!(sink.schema().fields().len(), 5);
}
