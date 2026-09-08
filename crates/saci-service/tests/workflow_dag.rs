//! Proves a two-processor `link` chain runs through the real
//! `ServiceConfig` -> `ServiceBuilder` -> `PipelineRuntimeLoader` -> per-node
//! runner path with actual wasmtime execution: two real component instances,
//! not the test doubles `saci_core::runtime`'s own unit tests use.
//!
//! Needs the smoketest artifact:
//!
//! ```bash
//! cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
//! cargo test --test workflow_dag -p saci-service --features wasm
//! ```

#![cfg(all(
    feature = "service",
    feature = "connector-file",
    feature = "transformer-csv",
    feature = "wasm"
))]

use std::sync::Arc;

use arrow_array::UInt64Array;
use saci_connector_file::FileSource;
use saci_core::io::source::Source;
use saci_service::component::Component;
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;
use saci_transformer_csv::CsvTransformer;
use tokio_util::sync::CancellationToken;

#[path = "common/smoketest.rs"]
mod smoketest;

use smoketest::{Ping, smoketest_wasm_path};

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Two `wasm` nodes pointing at the same real artifact, linked in sequence:
/// the smoketest component doesn't care that it is loaded twice, the same way
/// `examples/quickstart`'s Go and C# stages don't care that they run in one
/// process instead of two. `Ping` has no system attached, so each processor is
/// an identity function and the output must equal the input exactly.
fn config_kdl(wasm_path: &str, input: &str, output: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-workflow-dag" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "smoketest-chain" {{
    transformer "csv_fmt" format="csv" {{
        options has_headers=#true
    }}

    source "pings_in" type="FileSource" component="Ping" transformer="csv_fmt" {{
        config {{
            path "{input}"
            schema_fields "seq" type="uint64" nullable=#false
        }}
    }}

    wasm "first" module="{wasm_path}"
    wasm "second" module="{wasm_path}"

    sink "pings_out" type="FileSink" component="Ping" transformer="csv_fmt" {{
        config {{
            path "{output}"
            schema_fields "seq" type="uint64" nullable=#false
        }}
    }}

    link from="pings_in" to="first"
    link from="first" to="second"
    link from="second" to="pings_out"
}}

http disabled=#true

observability log_level="warn"
"#
    )
}

#[tokio::test]
async fn a_two_processor_link_runs_two_real_wasmtime_instances() {
    let wasm_path = smoketest_wasm_path();
    assert!(
        wasm_path.exists(),
        "smoketest .wasm not found at {}; run \
         `cargo build --release -p saci-processor-smoketest --target wasm32-wasip2` first",
        wasm_path.display()
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let input_path = dir.path().join("pings.csv");
    std::fs::write(&input_path, "seq\n0\n1\n2\n3\n").expect("write csv fixture");
    let output_path = dir.path().join("pings_out.csv");

    let config_path = dir.path().join("service.kdl");
    std::fs::write(
        &config_path,
        config_kdl(
            &config_path_text(&wasm_path),
            &config_path_text(&input_path),
            &config_path_text(&output_path),
            &config_path_text(dir.path()),
        ),
    )
    .expect("write config");

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .build_all(&config)
        .expect("both wasm nodes load as real wasmtime components")
        .remove(0);
    assert_eq!(built.nodes.len(), 4, "source, two processors, sink");

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("one-shot run");
    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 4, "every csv row was drained");
    assert_eq!(stats.iteration_errors, 0);
    assert!(stats.sink_batches_written >= 1);

    // `Ping` has no system attached, so both processors are an identity
    // function: the output rows must equal the input rows exactly, proving
    // both real component instances actually ran the batch through the link
    // rather than the config merely parsing.
    let mut readback = FileSource::open(
        &output_path,
        Arc::new(CsvTransformer::new(true)),
        Some(Ping::schema()),
    )
    .expect("the sink wrote a readable csv file");
    let batch = readback
        .next_batch()
        .await
        .expect("read")
        .expect("one batch");
    assert_eq!(batch.num_rows(), 4);
    let seqs = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("seq is UInt64");
    assert_eq!(seqs.values(), &[0, 1, 2, 3]);
}
