//! The whole standalone service path over MariaDB and MySQL: config file,
//! factories, source drain, pipeline run, sink drain.
//!
//! `run_standalone` is what the `saci-service serve` subcommand calls, so this
//! is the end to end proof that a `connector-mariadb` build moves rows from one
//! table through a pipeline and into another, and that a binlog snapshot on a
//! MariaDB server replicates into a MySQL server through `apply_changes`. The
//! runtime is a native [`Pipeline`] rather than a WASM processor, because
//! `impl PipelineRuntime for Pipeline` makes one available without a component
//! fixture.
//!
//! Soft-skips without Docker. `SACI_MARIADB_TEST_MARIADB_IMAGE` and
//! `SACI_MARIADB_TEST_MYSQL_IMAGE` pick the server images, as in the
//! connector crate's own suites.

#![cfg(all(feature = "service", feature = "connector-mariadb"))]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use mysql_async::prelude::Queryable;
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{SystemMeta, WriteSet, system_fn};
use saci_service::service::builder::{BuiltNodeKind, ServiceBuilder};
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;

const COMPONENT: &str = "Order";
const CHANGE_COMPONENT: &str = "Change";
const PASSWORD: &str = "saci";

/// The server family a test starts.
#[derive(Clone, Copy)]
enum Flavor {
    Mariadb,
    Mysql,
}

/// A running server and the DSN that reaches it.
struct Server {
    _container: ContainerAsync<GenericImage>,
    dsn: String,
}

impl Flavor {
    /// Image name and tag, from the connector suites' environment variable or
    /// the default.
    fn image(self) -> (String, String) {
        let (var, default) = match self {
            Self::Mariadb => ("SACI_MARIADB_TEST_MARIADB_IMAGE", "mariadb:11.8"),
            Self::Mysql => ("SACI_MARIADB_TEST_MYSQL_IMAGE", "mysql:8.4"),
        };
        let full = std::env::var(var)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| default.to_string());
        match full.rsplit_once(':') {
            Some((name, tag)) if !tag.contains('/') => (name.to_string(), tag.to_string()),
            _ => (full, "latest".to_string()),
        }
    }

    /// Row binlog on, which the binlog source needs on its server, and the
    /// trigger privilege check relaxed.
    fn args(self) -> Vec<&'static str> {
        let mut args = match self {
            Self::Mariadb => vec!["--log-bin=mariadb-bin"],
            Self::Mysql => vec!["--log-bin=mysql-bin"],
        };
        args.extend([
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--server-id=1",
            "--log-bin-trust-function-creators=1",
            // Several containers start at once; native AIO would exhaust the
            // host's `fs.aio-max-nr` and fail InnoDB's start.
            "--innodb-use-native-aio=0",
        ]);
        args
    }

    fn env_names(self) -> (&'static str, &'static str) {
        match self {
            Self::Mariadb => ("MARIADB_ROOT_PASSWORD", "MARIADB_DATABASE"),
            Self::Mysql => ("MYSQL_ROOT_PASSWORD", "MYSQL_DATABASE"),
        }
    }
}

/// Start a server and return it, or `None` when Docker is unavailable.
async fn start(flavor: Flavor) -> Option<Server> {
    let (name, tag) = flavor.image();
    let (password_var, database_var) = flavor.env_names();
    let image = GenericImage::new(name, tag)
        .with_exposed_port(3306_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var(password_var, PASSWORD)
        .with_env_var(database_var, "saci")
        .with_startup_timeout(Duration::from_secs(180))
        .with_cmd(flavor.args());

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: mariadb/mysql container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(3306_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: cannot map the server port: {e}");
            return None;
        }
    };
    let dsn = format!("mysql://root:{PASSWORD}@127.0.0.1:{port}/saci");

    // Both images log the readiness line for the throwaway init server too, so
    // poll until the real server answers.
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        if let Ok(mut conn) = mysql_async::Conn::from_url(&dsn).await {
            let ready = conn.query_drop("SELECT 1").await.is_ok();
            let _ = conn.disconnect().await;
            if ready {
                return Some(Server {
                    _container: container,
                    dsn,
                });
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: the server never accepted a connection");
    None
}

/// A config string literal.
fn quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

/// The component schema the polling test's source and sink speak.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("total", DataType::Int64, false),
    ]))
}

/// The component schema of the binlog test: the change operation, then the row.
fn change_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("__op", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("total", DataType::Int64, false),
    ]))
}

/// A pipeline that doubles `total` in place, so the sink's rows differ from the
/// source's and the run is observable in the output table.
fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("mariadb_service");
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

/// A pipeline that declares the change component and touches nothing, so the
/// sink receives exactly what the source emitted.
fn passthrough_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("mariadb_service_passthrough");
    pipeline
        .data
        .register_raw_component(CHANGE_COMPONENT, change_schema());
    pipeline
}

fn polling_config_kdl(dsn: &str, data_dir: &str) -> String {
    let dsn = quoted(dsn);
    format!(
        r#"
mode "standalone"

node id=1 name="saci-mariadb-service" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "mariadb-service-test" {{
    source "orders_in" type="MariadbSource" component="{COMPONENT}" {{
        config {{
            name "orders_in"
            batch_rows 40

            connection {{
                dsn {dsn}
                ssl_mode "disabled"
            }}

            mode kind="polling" table="orders_in" cursor_column="id"

            schema_fields "id" type="int64" nullable=#false
            schema_fields "label" type="utf8"
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="MariadbSink" component="{COMPONENT}" {{
        config {{
            name "orders_out"
            table "orders_out"
            write_mode "upsert"
            conflict_columns "id"

            connection {{
                dsn {dsn}
                ssl_mode "disabled"
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

fn binlog_config_kdl(source_dsn: &str, sink_dsn: &str, data_dir: &str) -> String {
    let source_dsn = quoted(source_dsn);
    let sink_dsn = quoted(sink_dsn);
    format!(
        r#"
mode "standalone"

node id=1 name="saci-mariadb-binlog-service" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "mariadb-binlog-test" {{
    source "orders_in" type="MariadbSource" component="{CHANGE_COMPONENT}" {{
        config {{
            name "orders_in"
            batch_rows 40

            connection {{
                dsn {source_dsn}
                ssl_mode "disabled"
            }}

            mode kind="cdc_binlog" table="orders_src" snapshot="initial"

            schema_fields "__op" type="utf8" nullable=#false
            schema_fields "id" type="int64" nullable=#false
            schema_fields "label" type="utf8"
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="MariadbSink" component="{CHANGE_COMPONENT}" {{
        config {{
            name "orders_out"
            table "orders_dst"
            write_mode "apply_changes"
            conflict_columns "id"

            connection {{
                dsn {sink_dsn}
                ssl_mode "disabled"
            }}

            schema_fields "__op" type="utf8" nullable=#false
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

/// Build the config at `path` with `pipeline` as the `transform` runtime.
fn build(
    path: &std::path::Path,
    pipeline: Pipeline,
) -> (ServiceConfig, saci_service::service::builder::BuiltService) {
    let config = ServiceConfig::load(path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(pipeline))
        .build_all(&config)
        .expect("service builds")
        .remove(0);
    (config, built)
}

#[tokio::test]
async fn a_mariadb_polling_source_and_sink_move_rows_through_the_standalone_runner() {
    let Some(server) = start(Flavor::Mariadb).await else {
        return;
    };
    let dsn = server.dsn.clone();

    let mut conn = mysql_async::Conn::from_url(&dsn).await.expect("connect");
    conn.query_drop(
        "CREATE TABLE orders_in (id BIGINT PRIMARY KEY, label VARCHAR(64), total BIGINT NOT NULL)",
    )
    .await
    .expect("source table");
    conn.query_drop(
        "CREATE TABLE orders_out (id BIGINT PRIMARY KEY, label VARCHAR(64), total BIGINT NOT NULL)",
    )
    .await
    .expect("sink table");
    let values = (1..=100)
        .map(|g| format!("({g}, 'order-{g}', {})", g * 10))
        .collect::<Vec<_>>()
        .join(", ");
    conn.query_drop(format!("INSERT INTO orders_in VALUES {values}"))
        .await
        .expect("seed");

    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");
    std::fs::write(&path, polling_config_kdl(&dsn, &data_dir)).expect("write config");

    let (config, built) = build(&path, build_pipeline());
    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("one-shot run");
    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 100, "every input row was drained");
    assert_eq!(stats.iteration_errors, 0);
    assert!(stats.sink_batches_written >= 1);

    let rows: Vec<(i64, String, i64)> = conn
        .query("SELECT id, label, total FROM orders_out ORDER BY id")
        .await
        .expect("read back");
    assert_eq!(rows.len(), 100);
    for (index, (id, label, total)) in rows.iter().enumerate() {
        let expected = index as i64 + 1;
        assert_eq!(*id, expected);
        assert_eq!(label, &format!("order-{expected}"));
        assert_eq!(
            *total,
            expected * 20,
            "the pipeline must have doubled the total"
        );
    }

    // A one-shot run never reaches a second drain cycle, and the offset is
    // committed at the *start* of the next cycle, so a rerun replays. That is
    // the documented at-least-once contract, and `write_mode = "upsert"` is
    // what makes the replay harmless.
    let (config, built) = build(&path, build_pipeline());
    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("second run");
    assert_eq!(
        stats.rows_processed, 100,
        "a one-shot rerun replays: the offset is committed one cycle late"
    );

    let rows: Vec<(i64, i64)> = conn
        .query("SELECT id, total FROM orders_out ORDER BY id")
        .await
        .expect("read back");
    assert_eq!(rows.len(), 100, "the upsert must not duplicate the replay");
    assert_eq!(
        rows[0].1, 20,
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

    let (config, built) = build(&path, build_pipeline());
    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("third run");
    assert_eq!(
        stats.rows_processed, 0,
        "the committed offset must stop the third run replaying"
    );
}

/// Every row of `table`, in key order.
async fn table_rows(conn: &mut mysql_async::Conn, table: &str) -> Vec<(i64, Option<String>, i64)> {
    conn.query(format!("SELECT id, label, total FROM {table} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {table}: {e}"))
}

#[tokio::test]
async fn a_binlog_snapshot_replicates_mariadb_into_mysql_with_apply_changes() {
    let Some(mariadb) = start(Flavor::Mariadb).await else {
        return;
    };
    let Some(mysql) = start(Flavor::Mysql).await else {
        return;
    };

    let mut source_conn = mysql_async::Conn::from_url(&mariadb.dsn)
        .await
        .expect("connect to MariaDB");
    let mut sink_conn = mysql_async::Conn::from_url(&mysql.dsn)
        .await
        .expect("connect to MySQL");
    let ddl = "(id BIGINT PRIMARY KEY, label VARCHAR(64), total BIGINT NOT NULL)";
    source_conn
        .query_drop(format!("CREATE TABLE orders_src {ddl}"))
        .await
        .expect("source table");
    sink_conn
        .query_drop(format!("CREATE TABLE orders_dst {ddl}"))
        .await
        .expect("sink table");
    let values = (1..=50)
        .map(|g| format!("({g}, 'order-{g}', {})", g * 10))
        .collect::<Vec<_>>()
        .join(", ");
    source_conn
        .query_drop(format!("INSERT INTO orders_src VALUES {values}"))
        .await
        .expect("seed");

    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");
    std::fs::write(
        &path,
        binlog_config_kdl(&mariadb.dsn, &mysql.dsn, &data_dir),
    )
    .expect("write config");

    // The snapshot reads the 50 existing rows as `R` and the sink applies them.
    let (config, built) = build(&path, passthrough_pipeline());
    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("snapshot run");
    assert_eq!(stats.iteration_errors, 0);
    assert_eq!(stats.rows_processed, 50, "the snapshot read every row");
    assert_eq!(
        table_rows(&mut sink_conn, "orders_dst").await,
        table_rows(&mut source_conn, "orders_src").await,
        "the snapshot must copy the table"
    );

    // A one-shot run commits its position one cycle late, so a source that
    // reaches a second cycle stores the position the snapshot pinned. The
    // changes below then arrive from the binlog alone.
    let mut source_only = ServiceConfig::load(&path).expect("config loads");
    source_only.workflows[0].sinks.clear();
    source_only.workflows[0]
        .links
        .retain(|l| l.from != "orders_out" && l.to != "orders_out");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(passthrough_pipeline()))
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
    dataset.register_raw_component(CHANGE_COMPONENT, change_schema());
    saci_core::io::source::drain_into_dataset(source.as_mut(), &mut dataset, CHANGE_COMPONENT)
        .await
        .expect("first cycle");
    dataset.clear();
    let caught_up =
        saci_core::io::source::drain_into_dataset(source.as_mut(), &mut dataset, CHANGE_COMPONENT)
            .await
            .expect("second cycle");
    assert_eq!(caught_up, 0, "the position is stored and nothing is new");
    drop(source);

    // 5 inserts, 3 updates (one of them a key change, which arrives as a
    // delete and an insert) and 2 deletes.
    for statement in [
        "INSERT INTO orders_src VALUES (51, 'order-51', 510), (52, 'order-52', 520), \
         (53, 'order-53', 530), (54, 'order-54', 540), (55, 'order-55', 550)",
        "UPDATE orders_src SET total = 1, label = 'changed' WHERE id = 1",
        "UPDATE orders_src SET total = 2 WHERE id = 2",
        "UPDATE orders_src SET id = 1003 WHERE id = 3",
        "DELETE FROM orders_src WHERE id = 20",
        "DELETE FROM orders_src WHERE id = 21",
    ] {
        source_conn.query_drop(statement).await.expect(statement);
    }

    let (config, built) = build(&path, passthrough_pipeline());
    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("change run");
    assert_eq!(stats.iteration_errors, 0);
    // 5 inserts, 2 plain updates, the key change as a delete and an insert,
    // and 2 deletes.
    assert_eq!(
        stats.rows_processed, 11,
        "the binlog carried exactly the changes"
    );

    let source_rows = table_rows(&mut source_conn, "orders_src").await;
    let sink_rows = table_rows(&mut sink_conn, "orders_dst").await;
    assert_eq!(source_rows.len(), 53);
    assert_eq!(
        sink_rows, source_rows,
        "the MySQL table must equal the MariaDB table row for row"
    );
}

/// The doubling system must actually see the column, or the polling test's
/// assertion would pass on an untouched value.
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

    let passthrough = passthrough_pipeline();
    let declared = saci_core::runtime::PipelineRuntime::declared_components(&passthrough);
    assert!(
        declared.contains(&CHANGE_COMPONENT),
        "declared: {declared:?}"
    );
}
