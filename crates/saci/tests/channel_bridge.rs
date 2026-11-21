//! End-to-end proof that a `ChannelSink` in one workflow and a
//! `ChannelSource` in another meet on one shared channel: `ServiceBuilder::build`
//! builds both workflows from one config, sharing the default
//! channel registry, and running them concurrently moves a row from the
//! producer's `FileSource` through the bridge to the consumer's `FileSink`.

#![cfg(all(
    feature = "connector-channel",
    feature = "connector-file",
    feature = "service",
    feature = "codec-csv"
))]

use tokio_util::sync::CancellationToken;

use saci::service::ServiceBuilder;
use saci::service::ServiceConfig;

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Two workflows in one config: `producer` reads a CSV file and writes into
/// the `bridge` channel; `consumer` reads the `bridge` channel and writes a
/// CSV file. Neither workflow names the other; they meet by declared channel
/// `name` alone, through the registry `ServiceBuilder::new` attaches.
/// Every declared id, including a `codec`'s, is unique across the whole
/// config (`ServiceConfig::validate`), so each workflow names its own
/// `codec` id even though both declare the same `format`.
fn config_kdl(input: &str, output: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-channel-bridge-test" data_dir="{data_dir}"

run "once"

workflow "producer" {{
    codec "producer_csv" format="csv" {{
        config has_headers=#true
    }}

    source "orders_in" connector="file" component="Order" codec="producer_csv" {{
        config {{
            path "{input}"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}

    sink "bridge_out" connector="channel" component="Order" {{
        config name="bridge" {{
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}

    link from="orders_in" to="bridge_out"
}}

workflow "consumer" {{
    codec "consumer_csv" format="csv" {{
        config has_headers=#true
    }}

    source "bridge_in" connector="channel" component="Order" {{
        config name="bridge" {{
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}

    sink "orders_out" connector="file" component="Order" codec="consumer_csv" {{
        config {{
            path "{output}"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}

    link from="bridge_in" to="orders_out"
}}

http enabled=#false

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

    let config = ServiceConfig::from_kdl(&config_kdl(
        &config_path_text(&input_path),
        &config_path_text(&output_path),
        &config_path_text(dir.path()),
    ))
    .expect("config loads");
    let mut built = ServiceBuilder::new()
        .build(&config)
        .expect("both workflows build, sharing one default channel registry");
    assert_eq!(built.len(), 2, "one BuiltWorkflow per declared workflow");
    // `build` builds in declaration order: `producer` then `consumer`.
    let consumer = built.remove(1);
    let producer = built.remove(0);

    let results = futures::future::join_all([
        producer.run(CancellationToken::new()),
        consumer.run(CancellationToken::new()),
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
