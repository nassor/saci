//! One transport, two formats, chosen in config: a CSV `FileSource` and an Avro
//! `FileSink` over one component, driven by `run_standalone`.
//!
//! One run covers the whole path a `format = "avro"` key travels: registration
//! as a builtin, resolution out of the codec registry, writing a
//! deflate-compressed object container file, and reading it back.

#![cfg(all(
    feature = "connector-file",
    feature = "service",
    feature = "codec-avro",
    feature = "codec-csv"
))]

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array};
use arrow_schema::{DataType, Field, Schema};
use tokio_util::sync::CancellationToken;

use saci::codec::avro::AvroCodec;
use saci::connector::file::FileSource;
use saci::io::Source;
use saci::pipeline::Pipeline;
use saci::service::builder::ServiceBuilder;
use saci::service::config::ServiceConfig;
use saci::service::factories::register_builtin_factories;
use saci::service::run_standalone;

const COMPONENT: &str = "Order";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Float64, false),
    ]))
}

fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("avro_connector");
    pipeline
        .data_mut()
        .register_schema(COMPONENT, schema())
        .unwrap();
    pipeline
}

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn config_kdl(input: &str, output: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-avro-connector" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "avro-connector-test" {{
    transformer "csv_fmt" format="csv" {{
        options has_headers=#true
    }}
    transformer "avro_fmt" format="avro" {{
        options compression="deflate"
    }}

    source "orders_in" type="FileSource" component="{COMPONENT}" transformer="csv_fmt" {{
        config {{
            path "{input}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="FileSink" component="{COMPONENT}" transformer="avro_fmt" {{
        config {{
            path "{output}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
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
async fn a_csv_source_and_an_avro_sink_share_one_file_connector() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_path = dir.path().join("orders.csv");
    let output_path = dir.path().join("orders.avro");

    std::fs::write(&input_path, "id,total\n1,10.5\n2,20.25\n3,30.75\n4,40.0\n")
        .expect("write the csv fixture");

    let config_path = dir.path().join("service.kdl");
    std::fs::write(
        &config_path,
        config_kdl(
            &config_path_text(&input_path),
            &config_path_text(&output_path),
            &config_path_text(dir.path()),
        ),
    )
    .expect("write config");

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("both formats resolve through the codec registry")
        .remove(0);

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("one-shot run");
    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 4, "every csv row was drained");
    assert_eq!(stats.iteration_errors, 0);

    // A reader takes the codec from the container file header, so reading this
    // back with no compression of its own also proves the writer honoured the
    // `deflate` option.
    let mut readback = FileSource::open(&output_path, Arc::new(AvroCodec::new()), None)
        .expect("the sink wrote a readable Avro file");

    let batch = readback
        .next_batch()
        .await
        .expect("read")
        .expect("one batch");
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(1).name(), "total");

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64");
    assert_eq!(ids.values(), &[1, 2, 3, 4]);

    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total is Float64");
    assert_eq!(totals.values(), &[10.5, 20.25, 30.75, 40.0]);
}
