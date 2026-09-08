//! Both TCP halves in one config: a `tcp` source listening for frames and a
//! `tcp` sink dialling out with them, over one component, driven by the stream
//! runner.
//!
//! The frame on the wire is the same in both directions, so the rows a producer
//! pushes into the source come back out of the sink byte for byte.

#![cfg(all(
    feature = "service",
    feature = "connector-tcp",
    feature = "transformer-arrow-ipc",
    feature = "wasm"
))]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use saci_core::pipeline::Pipeline;
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;
use saci_transformer::Transformer;
use saci_transformer_arrow_ipc::ArrowIpcTransformer;

const COMPONENT: &str = "Tick";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("tcp_connector");
    pipeline.data.register_raw_component(COMPONENT, schema());
    pipeline
}

/// One frame: a `u32` big-endian length, then one Arrow IPC stream payload.
fn frame(values: &[i64]) -> Vec<u8> {
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))])
        .expect("build batch");
    let mut payload: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &schema()).expect("ipc writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish stream");
    }
    let mut out = Vec::with_capacity(payload.len() + 4);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Read one frame the sink wrote, with the same header rule the source applies.
async fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut header))
        .await
        .expect("frame header timed out")
        .expect("frame header");
    let len = u32::from_be_bytes(header) as usize;
    let mut payload = vec![0u8; len];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut payload))
        .await
        .expect("frame payload timed out")
        .expect("frame payload");
    payload
}

/// Decode a frame payload through the same transformer the config names.
fn decode(payload: &[u8]) -> RecordBatch {
    let mut decoder = ArrowIpcTransformer::new()
        .open_message_decoder(schema())
        .expect("decoder");
    decoder.push(payload).expect("push");
    decoder.flush().expect("flush").expect("one batch")
}

fn values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("v is Int64")
        .values()
        .to_vec()
}

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn config_kdl(bind: &str, connect: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-tcp-connector" data_dir="{data_dir}"

run_mode kind="stream"

workflow "tcp-connector-test" {{
    transformer "ipc" format="arrow-ipc"

    source "frames_in" type="tcp" component="{COMPONENT}" transformer="ipc" {{
        config {{
            bind "{bind}"
            buffer 8
            max_frame_bytes 65536
            schema_fields "v" type="int64" nullable=#false
        }}
    }}

    wasm "relay" name="Relay"

    sink "frames_out" type="tcp" component="{COMPONENT}" transformer="ipc" {{
        config {{
            connect "{connect}"
            schema_fields "v" type="int64" nullable=#false
        }}
    }}

    link from="frames_in" to="relay"
    link from="relay" to="frames_out"
}}

http disabled=#true

observability log_level="warn"
"#
    )
}

/// A free port for the source's `bind`, which the config needs before the
/// factory binds it for real.
fn reserved_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    port
}

#[tokio::test]
async fn frames_pushed_into_the_tcp_source_come_back_out_of_the_tcp_sink() {
    // The sink's peer: bound here so the config can name a real port, and left
    // unaccepted until the sink dials it.
    let collector = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let collector_addr = collector.local_addr().expect("collector addr");
    let bind_addr = format!("127.0.0.1:{}", reserved_port());

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("service.kdl");
    std::fs::write(
        &config_path,
        config_kdl(
            &bind_addr,
            &collector_addr.to_string(),
            &config_path_text(dir.path()),
        ),
    )
    .expect("write config");

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("relay", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("both tcp halves resolve through the registry")
        .remove(0);

    let cancel = CancellationToken::new();
    let runner_cancel = cancel.clone();
    let runner_config = config.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_standalone(built, &runner_config, runner_cancel, None, None).await
    });

    let stats = local
        .run_until(async move {
            // The listener is already bound by the factory, so this connection
            // waits in the backlog until the accept loop starts.
            let mut producer = TcpStream::connect(&bind_addr)
                .await
                .expect("connect to the source");
            producer
                .write_all(&frame(&[1, 2]))
                .await
                .expect("frame one");
            producer.write_all(&frame(&[3])).await.expect("frame two");
            producer.flush().await.expect("flush");

            let (mut peer, _) = tokio::time::timeout(Duration::from_secs(5), collector.accept())
                .await
                .expect("the sink never dialled")
                .expect("accept");

            let first = decode(&read_frame(&mut peer).await);
            let second = decode(&read_frame(&mut peer).await);
            assert_eq!(values(&first), vec![1, 2], "first item's rows, in order");
            assert_eq!(values(&second), vec![3], "one frame per item");

            drop(producer);
            cancel.cancel();
            handle.await.expect("runner task").expect("stream run")
        })
        .await;

    assert_eq!(stats.iterations, 2, "one item per received frame");
    assert_eq!(stats.iteration_errors, 0);
}

fn validation_kdl(run_mode: &str, source_type: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-tcp-validate"

run_mode {run_mode}

workflow "w" {{
    source "in" type="{source_type}" component="{COMPONENT}"
    sink "out" type="tcp" component="{COMPONENT}"
    link from="in" to="out"
}}
"#
    )
}

fn load_from_text(dir: &tempfile::TempDir, name: &str, text: &str) -> saci_core::SaciResult<()> {
    let path = dir.path().join(name);
    std::fs::write(&path, text).expect("write config");
    ServiceConfig::load(&path).map(|_| ())
}

/// The no-EOF rule reads `source` nodes, so a `tcp` sink is legal in every run
/// mode even though a `tcp` source is stream-only.
#[test]
fn a_tcp_sink_is_accepted_outside_stream_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    for run_mode in [
        r#"kind="one_shot""#,
        r#"kind="interval" interval_ms=1000"#,
        r#"kind="stream""#,
    ] {
        load_from_text(
            &dir,
            "sink_only.kdl",
            &validation_kdl(run_mode, "NoopSource"),
        )
        .unwrap_or_else(|e| panic!("a tcp sink under {run_mode} must validate: {e}"));
    }
}

/// The mirror of the rule above: the source half is still stream-only.
#[test]
fn a_tcp_source_is_still_rejected_outside_stream_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let err = load_from_text(
        &dir,
        "source_one_shot.kdl",
        &validation_kdl(r#"kind="one_shot""#, "tcp"),
    )
    .expect_err("a tcp source outside stream mode must fail");
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(err.to_string().contains("never reaches EOF"), "got: {err}");
}
