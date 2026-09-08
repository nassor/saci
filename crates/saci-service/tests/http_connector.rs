//! Every HTTP node type in one test binary.
//!
//! `rows_served_over_http_come_back_out_of_the_http_sink` drives an
//! `HttpSource` GET into a workflow whose `HttpSink` posts the result, under
//! `one_shot`. The source reaches EOF after its one GET, which is what lets
//! `one_shot` drive it, and the sink's captured body decodes back to the rows
//! that were served.
//!
//! The `push` module drives an `HttpPushSource` under `stream`: three POSTed
//! ndjson documents become three sink requests, and the connector's own
//! `saci_http_*` series are read back out of a Prometheus registry.
//!
//! The servers the tests answer with are hand rolled on a
//! `std::net::TcpListener`, so nothing here needs an HTTP framework and the
//! request count is exact.

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

/// The listening half under `run_mode kind="stream"`.
///
/// `HttpPushSource` never reaches EOF, so the stream runner is the only one
/// that may drive it, and one POSTed document is one item. The format is
/// `ndjson`, so one request body is a batch of json lines and the sink writes
/// the same shape back out.
#[cfg(feature = "transformer-ndjson")]
mod push {
    use std::io::Write;
    use std::time::Duration;

    use arrow_array::RecordBatch;
    use prometheus::{Registry, TextEncoder};
    use tokio::task::LocalSet;
    use tokio_util::sync::CancellationToken;

    use saci_service::service::builder::ServiceBuilder;
    use saci_service::service::config::ServiceConfig;
    use saci_service::service::factories::register_builtin_factories;
    use saci_service::service::install_ring_provider;
    use saci_service::service::run_standalone;
    use saci_service::service::standalone::StandaloneStats;
    use saci_transformer::Transformer;
    use saci_transformer_ndjson::NdjsonTransformer;

    use super::{COMPONENT, build_pipeline, column, config_path_text, schema, serve};

    /// Three documents of two rows each, so one item carries a batch rather
    /// than a single row.
    fn documents() -> [String; 3] {
        [[1, 2], [3, 4], [5, 6]].map(|ids| {
            ids.iter()
                .map(|id| format!("{{\"id\":{id},\"total\":{}}}\n", id * 10))
                .collect()
        })
    }

    /// A free port for the source's `bind`, which the config needs before the
    /// factory binds it for real.
    fn reserved_port() -> u16 {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
        let port = probe.local_addr().expect("probe addr").port();
        drop(probe);
        port
    }

    fn config_kdl(bind: &str, sink_url: &str, data_dir: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 name="saci-http-push" data_dir="{data_dir}"

run_mode kind="stream"

workflow "http-push-test" {{
    transformer "json_fmt" format="ndjson"

    source "orders_in" type="HttpPushSource" component="{COMPONENT}" transformer="json_fmt" {{
        config {{
            bind "{bind}"
            path "/orders"
            methods "POST"
            buffer 8
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    wasm "relay" name="Relay"

    sink "orders_out" type="HttpSink" component="{COMPONENT}" transformer="json_fmt" {{
        config {{
            url "{sink_url}"
            method "POST"
            timeout_ms 5000
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="int64" nullable=#false
        }}
    }}

    link from="orders_in" to="relay"
    link from="relay" to="orders_out"
}}

http disabled=#true

observability log_level="warn"
"#
        )
    }

    /// Decode a captured request body through the same format the sink wrote
    /// it with, which is what makes the assertion a round trip.
    fn decode_body(body: &[u8]) -> Vec<RecordBatch> {
        let mut spool = tempfile::tempfile().expect("temp file");
        spool.write_all(body).expect("write body");
        std::io::Seek::rewind(&mut spool).expect("rewind");

        let mut reader = NdjsonTransformer::default()
            .open_reader(spool, Some(schema()))
            .expect("the body is a whole ndjson document");
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().expect("decode") {
            batches.push(batch);
        }
        batches
    }

    /// POST every document into the push source and collect what the sink
    /// sent on.
    ///
    /// The listener is already bound by the factory, so a POST issued before
    /// the accept loop starts waits in the backlog instead of being refused.
    async fn drive(dir: &std::path::Path) -> (StandaloneStats, Vec<Vec<u8>>) {
        let bind_addr = format!("127.0.0.1:{}", reserved_port());
        let (sink_url, sink_server) = serve(documents().len(), Vec::new());

        let config_path = dir.join("service.kdl");
        std::fs::write(
            &config_path,
            config_kdl(&bind_addr, &sink_url, &config_path_text(dir)),
        )
        .expect("write config");

        let config = ServiceConfig::load(&config_path).expect("config loads");
        let built = register_builtin_factories(ServiceBuilder::new())
            .with_runtime("relay", Box::new(build_pipeline()))
            .build_all(&config)
            .expect("the push source and the sink resolve through the registry")
            .remove(0);

        let cancel = CancellationToken::new();
        let runner_cancel = cancel.clone();
        let runner_config = config.clone();
        let local = LocalSet::new();
        let handle = local.spawn_local(async move {
            run_standalone(built, &runner_config, runner_cancel, None, None).await
        });

        local
            .run_until(async move {
                // reqwest 0.13's `rustls-no-provider` refuses to build a
                // client until a crypto provider is installed.
                install_ring_provider();
                let client = reqwest::Client::new();
                for document in documents() {
                    let response = client
                        .post(format!("http://{bind_addr}/orders"))
                        .body(document)
                        .timeout(Duration::from_secs(10))
                        .send()
                        .await
                        .expect("the push source answered");
                    assert_eq!(
                        response.status().as_u16(),
                        202,
                        "a queued document is accepted"
                    );
                }

                let posted = tokio::time::timeout(
                    Duration::from_secs(20),
                    tokio::task::spawn_blocking(move || {
                        sink_server.join().expect("sink server thread")
                    }),
                )
                .await
                .expect("the sink never sent every request")
                .expect("the join task");

                cancel.cancel();
                let stats = handle.await.expect("runner task").expect("stream run");
                (stats, posted)
            })
            .await
    }

    #[tokio::test]
    async fn documents_posted_to_the_push_source_come_back_out_of_the_http_sink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (stats, posted) = drive(dir.path()).await;

        assert_eq!(stats.iterations, 3, "one item per accepted document");
        assert_eq!(stats.rows_processed, 6, "every posted row was drained");
        assert_eq!(stats.iteration_errors, 0);

        assert_eq!(posted.len(), 3, "one request out per document in");
        let rows: Vec<(Vec<i64>, Vec<i64>)> = posted
            .iter()
            .map(|body| {
                let batches = decode_body(body);
                assert_eq!(batches.len(), 1, "one batch per document");
                (column(&batches[0], "id"), column(&batches[0], "total"))
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (vec![1, 2], vec![10, 20]),
                (vec![3, 4], vec![30, 40]),
                (vec![5, 6], vec![50, 60]),
            ]
        );
    }

    /// The connector's four series reach `/metrics` through the process-global
    /// meter, so the provider has to be installed before a connector is built.
    #[tokio::test]
    async fn both_halves_record_their_requests_into_prometheus() {
        let registry = Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .without_counter_suffixes()
            .with_registry(registry.clone())
            .build()
            .expect("build prometheus exporter");
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(exporter)
            .build();
        opentelemetry::global::set_meter_provider(provider);

        let dir = tempfile::tempdir().expect("tempdir");
        let (stats, posted) = drive(dir.path()).await;
        assert_eq!(stats.iteration_errors, 0);
        assert_eq!(posted.len(), 3);

        let text = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .expect("encode prometheus text");

        let accepted = total(
            &text,
            "saci_http_requests_total",
            &[
                r#"workflow="http-push-test""#,
                r#"role="push""#,
                r#"method="POST""#,
                r#"status="202""#,
            ],
        )
        .unwrap_or_else(|| panic!("the push source's 202s are missing from:\n{text}"));
        assert!(
            accepted >= 3.0,
            "three documents were accepted, got {accepted} in:\n{text}"
        );

        let written = total(
            &text,
            "saci_http_requests_total",
            &[
                r#"workflow="http-push-test""#,
                r#"role="sink""#,
                r#"status="200""#,
            ],
        )
        .unwrap_or_else(|| panic!("the sink's 200s are missing from:\n{text}"));
        assert!(
            written >= 3.0,
            "three requests were answered, got {written} in:\n{text}"
        );
    }

    /// Sum every sample whose series is `name` and whose label set carries
    /// each fragment in `labels`, or `None` when no line matches.
    ///
    /// One label set per node, and a test binary running several workflows
    /// carries a line each, so a lookup that took the first match would read
    /// whichever workflow sorted first.
    ///
    /// The exporter attaches an `otel_scope_name` label to everything, so the
    /// series name is followed by `{...}` rather than a space.
    fn total(text: &str, name: &str, labels: &[&str]) -> Option<f64> {
        let mut matched = false;
        let sum = text
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter(|line| line.starts_with(name))
            .filter(|line| labels.iter().all(|label| line.contains(label)))
            .filter_map(|line| line.rsplit(' ').next())
            .filter_map(|value| value.parse::<f64>().ok())
            .inspect(|_| matched = true)
            .sum();
        matched.then_some(sum)
    }
}
