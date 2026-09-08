//! Both HTTP halves in one config: an `HttpSource` GET feeding a workflow whose
//! `HttpSink` posts the result, driven by `run_standalone` in one-shot mode.
//!
//! The source reaches EOF after its one GET, which is what lets `one_shot` drive
//! it, and the sink's captured body decodes back to the rows that were served.
//! The server is hand rolled on a `std::net::TcpListener`, so the test needs no
//! HTTP framework and the request count is exact.

#![cfg(all(
    feature = "service",
    feature = "connector-http",
    feature = "transformer-csv",
    feature = "wasm"
))]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread::JoinHandle;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use tokio_util::sync::CancellationToken;

use saci_core::pipeline::Pipeline;
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::run_standalone;
use saci_transformer::Transformer;
use saci_transformer_csv::CsvTransformer;

const COMPONENT: &str = "Order";
const CSV_BODY: &[u8] = b"id,total\n1,10\n2,20\n3,30\n4,40\n";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
    ]))
}

fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("http_connector");
    pipeline.data.register_raw_component(COMPONENT, schema());
    pipeline
}

/// Bind a server that answers `connections` requests with `200 OK` and `body`.
///
/// Returns the url a config node aims at and the handle carrying every request
/// body, in arrival order.
fn serve(connections: usize, body: Vec<u8>) -> (String, JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/data", listener.local_addr().expect("local_addr"));
    let handle = std::thread::spawn(move || {
        let mut captured = Vec::with_capacity(connections);
        for _ in 0..connections {
            let (mut stream, _) = listener.accept().expect("accept");
            captured.push(exchange(&mut stream, &body));
        }
        captured
    });
    (url, handle)
}

/// Read one request off `stream`, answer with `body`, and return the request
/// body.
///
/// `connection: close` keeps the accept count equal to the request count: the
/// client cannot pool a socket the server said it would close.
fn exchange(stream: &mut std::net::TcpStream, body: &[u8]) -> Vec<u8> {
    let mut buffered = Vec::new();
    let mut chunk = [0u8; 1024];
    let head_end = loop {
        if let Some(at) = buffered.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        let read = stream.read(&mut chunk).expect("read request head");
        if read == 0 {
            break buffered.len();
        }
        buffered.extend_from_slice(&chunk[..read]);
    };

    let head = String::from_utf8_lossy(&buffered[..head_end]);
    let length = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| {
            value.trim().parse().expect("content-length is a number")
        });

    let mut request_body = buffered[head_end..].to_vec();
    while request_body.len() < length {
        let read = stream.read(&mut chunk).expect("read request body");
        if read == 0 {
            break;
        }
        request_body.extend_from_slice(&chunk[..read]);
    }

    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).expect("write head");
    stream.write_all(body).expect("write body");
    stream.flush().expect("flush");

    request_body
}

/// Decode a captured request body through the same format the sink wrote it
/// with, which is what makes the assertion a round trip.
fn decode_body(body: &[u8]) -> Vec<RecordBatch> {
    let mut spool = tempfile::tempfile().expect("temp file");
    spool.write_all(body).expect("write body");
    std::io::Seek::rewind(&mut spool).expect("rewind");

    let mut reader = CsvTransformer::new(true)
        .open_reader(spool, Some(schema()))
        .expect("the body is a whole csv document");
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch().expect("decode") {
        batches.push(batch);
    }
    batches
}

fn column(batch: &RecordBatch, name: &str) -> Vec<i64> {
    batch
        .column_by_name(name)
        .expect("column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column")
        .values()
        .to_vec()
}

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn config_kdl(source_url: &str, sink_url: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-http-connector" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "http-connector-test" {{
    transformer "csv_fmt" format="csv" {{
        options has_headers=#true
    }}

    source "orders_in" type="HttpSource" component="{COMPONENT}" transformer="csv_fmt" {{
        config {{
            url "{source_url}"
            timeout_ms 5000
            headers {{
                accept "text/csv"
            }}
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="HttpSink" component="{COMPONENT}" transformer="csv_fmt" {{
        config {{
            url "{sink_url}"
            method "POST"
            timeout_ms 5000
            schema_fields "id" type="int64" nullable=#false
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
async fn rows_served_over_http_come_back_out_of_the_http_sink() {
    let (source_url, source_server) = serve(1, CSV_BODY.to_vec());
    let (sink_url, sink_server) = serve(1, Vec::new());

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("service.kdl");
    std::fs::write(
        &config_path,
        config_kdl(&source_url, &sink_url, &config_path_text(dir.path())),
    )
    .expect("write config");

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("both http halves resolve through the registry")
        .remove(0);

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("one-shot run");
    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 4, "every served row was drained");
    assert_eq!(stats.iteration_errors, 0);

    let served = source_server.join().expect("source server thread");
    assert_eq!(served.len(), 1, "the source GETs once per run");

    let posted = sink_server.join().expect("sink server thread");
    assert_eq!(posted.len(), 1, "one batch out, one request");

    let batches = decode_body(&posted[0]);
    assert_eq!(batches.len(), 1);
    assert_eq!(column(&batches[0], "id"), vec![1, 2, 3, 4]);
    assert_eq!(column(&batches[0], "total"), vec![10, 20, 30, 40]);
}
