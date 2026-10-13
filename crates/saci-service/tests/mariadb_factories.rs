//! The config to factory path for `MariadbSource` and `MariadbSink`.
//!
//! No database: both factories are synchronous and open no connection, so this
//! exercises exactly what `saci-service validate` does: env-var substitution,
//! parsing, cross-field validation, and schema construction.

#![cfg(all(feature = "service", feature = "connector-mariadb"))]

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

node id=1 name="saci-mariadb-test" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "mariadb-factories-test" {{
    source "maria_orders" type="MariadbSource" component="OrderChange" {{
        config {{
            name "maria_orders"
            batch_rows 256

            connection {{
                dsn "${{SACI_TEST_MARIADB_DSN}}"
                ssl_mode "disabled"
            }}

            mode kind="cdc_binlog" table="shop.orders" snapshot="initial" server_id=4100

            schema_fields "__op" type="utf8" nullable=#false
            schema_fields "__commit_ts" type="timestamp_micros_utc"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "amount" type="decimal128" precision=18 scale=4
        }}
    }}

    wasm "transform" name="Transform"

    sink "maria_enriched" type="MariadbSink" component="EnrichedOrder" {{
        config {{
            name "maria_enriched"
            table "shop.enriched_orders"
            write_mode "apply_changes"
            conflict_columns "id"

            connection {{
                dsn "${{SACI_TEST_MARIADB_DSN}}"
                ssl_mode "disabled"
            }}

            schema_fields "__op" type="utf8" nullable=#false
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="decimal128" precision=12 scale=2
        }}
    }}

    link from="maria_orders" to="transform"
    link from="transform" to="maria_enriched"
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
            "SACI_TEST_MARIADB_DSN",
            "mysql://someone:hunter2@db.example:3306/app",
        );
    }
}

fn data_dir() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().display().to_string().replace('\\', "/");
    (dir, path)
}

#[test]
fn a_declared_source_and_sink_are_built_with_their_declared_schemas() {
    set_dsn();
    let (_dir, data_dir) = data_dir();
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
    assert_eq!(source_node.id, "maria_orders");
    assert_eq!(source_node.component, Some("OrderChange"));
    assert_eq!(sink_node.id, "maria_enriched");
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
    assert_eq!(sink_schema.fields().len(), 3);
    assert_eq!(sink_schema.field(2).name(), "total");
    assert_eq!(
        sink_schema.field(2).data_type(),
        &DataType::Decimal128(12, 2)
    );
}

#[test]
fn a_misspelled_key_fails_the_build_as_a_configuration_error() {
    set_dsn();
    let (_dir, data_dir) = data_dir();
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
    let (_dir, data_dir) = data_dir();
    // A snapshot pins its own start position, so an explicit `initial` clashes.
    let body = config_body(&data_dir).replace(
        "snapshot=\"initial\"",
        "snapshot=\"initial\" initial=\"beginning\"",
    );
    let config = load(&body).expect("config loads");

    let Err(err) = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(Pipeline::new("test")))
        .build_all(&config)
    else {
        panic!("the snapshot/initial combination must be rejected");
    };
    assert_eq!(err.category(), "configuration");
    assert!(
        err.message().contains("pins its own start position"),
        "{}",
        err.message()
    );
}

#[test]
fn apply_changes_without_a_declared_op_field_fails_the_build() {
    set_dsn();
    let (_dir, data_dir) = data_dir();
    let body = config_body(&data_dir).replace(
        "            schema_fields \"__op\" type=\"utf8\" nullable=#false\n            schema_fields \"id\" type=\"int64\" nullable=#false\n            schema_fields \"total\"",
        "            schema_fields \"id\" type=\"int64\" nullable=#false\n            schema_fields \"total\"",
    );
    let config = load(&body).expect("config loads");

    let Err(err) = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(Pipeline::new("test")))
        .build_all(&config)
    else {
        panic!("apply_changes without __op must be rejected");
    };
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("'__op'"), "{}", err.message());
}

/// Both shipped `examples/configs/mariadb*.kdl` must parse and their IO tables
/// must be accepted by the factories.
///
/// Gated on `wasm` because the examples declare a `wasm` node, which the
/// config parser only knows about with that feature on. The factories are
/// driven directly rather than through `ServiceBuilder::build_all`, which would
/// also try to load the module the examples point at.
#[cfg(feature = "wasm")]
#[test]
fn the_shipped_example_configs_load_and_their_io_tables_are_accepted() {
    use saci_service::service::ConnectorContext;
    use saci_service::service::factories::{MariadbSinkFactory, MariadbSourceFactory};
    use saci_service::service::registry::{SinkFactory, SourceFactory};

    // SAFETY: single-threaded test setup.
    unsafe {
        std::env::set_var("SACI_MARIADB_DSN", "mysql://saci@localhost:3306/shop");
        std::env::set_var("SACI_MARIADB_SINK_DSN", "mysql://saci@localhost:3307/shop");
    }
    // The binlog example declares four reserved columns and three of its own
    // (seven); the trigger example three reserved and three of its own (six).
    // Both sinks declare four columns.
    for (file, source_fields, sink_fields) in [
        ("../../examples/configs/mariadb.kdl", 7, 4),
        ("../../examples/configs/mariadb_trigger.kdl", 6, 4),
    ] {
        let config = ServiceConfig::load(file)
            .unwrap_or_else(|e| panic!("the shipped example {file} must load: {e}"));
        assert_eq!(config.workflows[0].sources.len(), 1, "{file}");
        assert_eq!(config.workflows[0].sinks.len(), 1, "{file}");
        assert_eq!(config.workflows[0].sources[0].type_name, "MariadbSource");
        assert_eq!(config.workflows[0].sinks[0].type_name, "MariadbSink");

        // MariaDB declares its own schema, so it reads no `format` key and no
        // bound transformer is needed.
        let ctx = ConnectorContext::new(None);
        let source = MariadbSourceFactory
            .build(&config.workflows[0].sources[0].config, &ctx)
            .unwrap_or_else(|e| panic!("{file}: the example source must be accepted: {e}"));
        let sink = MariadbSinkFactory
            .build(&config.workflows[0].sinks[0].config, &ctx)
            .unwrap_or_else(|e| panic!("{file}: the example sink must be accepted: {e}"));
        assert_eq!(source.schema().fields().len(), source_fields, "{file}");
        assert_eq!(sink.schema().fields().len(), sink_fields, "{file}");
    }
}
