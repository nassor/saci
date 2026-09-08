//! End-to-end proof that a `ChannelSink` in one workflow and a
//! `ChannelSource` in another meet on one shared channel: `ServiceBuilder::
//! build_all` builds both workflows from one config, sharing the default
//! channel registry, and running them concurrently moves a row from the
//! producer's `FileSource` through the bridge to the consumer's `FileSink`.

#![cfg(all(
    feature = "service",
    feature = "connector-channel",
    feature = "connector-file",
    feature = "transformer-csv"
))]

use tokio_util::sync::CancellationToken;

use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Two workflows in one config: `producer` reads a CSV file and writes into
/// the `bridge` channel; `consumer` reads the `bridge` channel and writes a
/// CSV file. Neither workflow names the other; they meet by declared channel
/// `name` alone, through the registry `register_builtin_factories` attaches.
/// Every declared id, including a `transformer`'s, is unique across the whole
/// config (`ServiceConfig::validate`), so each workflow names its own
/// `transformer` id even though both declare the same `format`.
fn config_kdl(input: &str, output: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-channel-bridge-test" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "producer" {{
    transformer "producer_csv" format="csv" {{
        options has_headers=#true
    }}

    source "orders_in" type="FileSource" component="Order" transformer="producer_csv" {{
        config {{
            path "{input}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    sink "bridge_out" type="ChannelSink" component="Order" {{
        config name="bridge" {{
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    link from="orders_in" to="bridge_out"
}}

workflow "consumer" {{
    transformer "consumer_csv" format="csv" {{
        options has_headers=#true
    }}

    source "bridge_in" type="ChannelSource" component="Order" {{
        config name="bridge" {{
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    sink "orders_out" type="FileSink" component="Order" transformer="consumer_csv" {{
        config {{
            path "{output}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    link from="bridge_in" to="orders_out"
}}

http disabled=#true

observability log_level="warn"
"#
    )
}

#[tokio::test]
async fn a_channel_sink_in_one_workflow_bridges_to_a_channel_source_in_another() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_path = dir.path().join("orders.csv");
    let output_path = dir.path().join("orders_out.csv");

    std::fs::write(&input_path, "id,total\n1,10.5\n").expect("write the csv fixture");

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
    let mut built = register_builtin_factories(ServiceBuilder::new())
        .build_all(&config)
        .expect("both workflows build, sharing one default channel registry");
    assert_eq!(built.len(), 2, "one BuiltService per declared workflow");
    // `build_all` builds in declaration order: `producer` then `consumer`.
    let consumer = built.remove(1);
    let producer = built.remove(0);

    let results = futures::future::join_all([
        run_standalone(producer, &config, CancellationToken::new(), None, None),
        run_standalone(consumer, &config, CancellationToken::new(), None, None),
    ])
    .await;
    let mut results = results.into_iter();
    results
        .next()
        .expect("two runs")
        .expect("producer run succeeds");
    results
        .next()
        .expect("two runs")
        .expect("consumer run succeeds");

    let output = std::fs::read_to_string(&output_path).expect("read consumer output");
    assert!(
        output.contains("1,10.5"),
        "the row the producer read must reach the consumer's file through the bridge: {output}"
    );
}
