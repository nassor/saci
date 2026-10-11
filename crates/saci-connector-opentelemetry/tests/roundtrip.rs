//! The source and the sink, against each other and against a stand-in
//! endpoint.
//!
//! Every case here drives the real listener, the real client and the real
//! OTLP encoding: a batch is posted by the sink and read back out of the
//! source, or a hand-built request is posted at the listener to exercise one
//! answer of the protocol.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::metrics::v1::{Metric, ResourceMetrics, ScopeMetrics};
use prost::Message as _;
use saci_connector_opentelemetry::{
    AuthConfig, Encoding, OpenTelemetryMetricsSink, OpenTelemetryMetricsSinkConfig,
    OpenTelemetryMetricsSource, OpenTelemetryMetricsSourceConfig, PullMode, SourceModeConfig,
    schema,
};
use saci_core::io::sink::Sink as _;
use saci_core::io::source::Source as _;
use serde_json::json;

/// A canonical batch: one gauge row and one histogram row.
///
/// The two kinds between them carry every column: the gauge has no count and
/// no start timestamp, and the histogram carries a distribution, a
/// temporality, a scope and a count.
fn canonical_batch() -> RecordBatch {
    let schema = schema::metrics_schema();
    let attributes = vec![
        json!({"http.request.method": "GET"}).to_string(),
        json!({
            "otel.aggregation_temporality": "delta",
            "otel.distribution": {
                "bucket_counts": [3, 4],
                "explicit_bounds": [1.0, 2.0],
                "max": 9.0,
                "min": 0.5,
            },
            "otel.scope": {"name": "saci", "version": "0.1.0"},
        })
        .to_string(),
    ];
    let resource = json!({"service.name": "checkout"}).to_string();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["http.requests", "saci.queue.depth"])),
            Arc::new(Float64Array::from(vec![1.5, 7.0])),
            Arc::new(UInt64Array::from(vec![
                1_700_000_000_000_000_000,
                1_700_000_000_000_000_001,
            ])),
            Arc::new(StringArray::from(vec!["gauge", "histogram"])),
            Arc::new(StringArray::from(vec![Some("1"), None])),
            Arc::new(StringArray::from(vec![Some("requests"), None])),
            Arc::new(StringArray::from(
                attributes.into_iter().map(Some).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![
                Some(resource.clone()),
                Some(resource),
            ])),
            Arc::new(UInt64Array::from(vec![None, Some(7)])),
            Arc::new(UInt64Array::from(vec![None, Some(10)])),
        ],
    )
    .expect("the fixture matches the canonical schema")
}

/// A source config listening on an ephemeral port.
fn push_config(path: &str) -> OpenTelemetryMetricsSourceConfig {
    OpenTelemetryMetricsSourceConfig {
        bind: Some("127.0.0.1:0".to_string()),
        path: Some(path.to_string()),
        ..OpenTelemetryMetricsSourceConfig::default()
    }
}

/// A sink config pointing at one endpoint, sending no header of its own.
fn sink_config(url: &str, encoding: Encoding) -> OpenTelemetryMetricsSinkConfig {
    OpenTelemetryMetricsSinkConfig {
        url: url.to_string(),
        encoding,
        headers: HashMap::new(),
        timeout_ms: 10_000,
        metric_name: None,
        service_name: None,
        batch_rows: 8_192,
        schema_fields: Vec::new(),
    }
}

/// A push source adopts its socket, and spawns its accept loop, on its first
/// read, so a case that posts without reading anything would wait for a
/// response nothing is serving. This starts that read and leaves it blocked;
/// the task dies with the test's runtime.
fn start_listening(source: OpenTelemetryMetricsSource) {
    tokio::spawn(async move {
        let mut source = source;
        let _ = source.next_batch().await;
    });
}

/// Post `batch` at `source` and read the rows back out of it.
async fn round_trip(
    source: OpenTelemetryMetricsSource,
    encoding: Encoding,
    batch: &RecordBatch,
) -> RecordBatch {
    let endpoint = format!(
        "http://{}{}",
        source.local_addr().expect("the push mode binds"),
        saci_connector_opentelemetry::config::DEFAULT_PATH
    );
    let mut sink = OpenTelemetryMetricsSink::new(sink_config(&endpoint, encoding), None)
        .expect("the sink builds");
    let mut source = source;
    let reader = tokio::spawn(async move {
        source
            .next_batch()
            .await
            .expect("the read succeeds")
            .expect("a request arrives")
    });
    // The socket is listening from construction, so the export queues in its
    // backlog and is answered once the read above has adopted it.
    sink.write_batch(batch).await.expect("the export is taken");
    sink.finish().await.expect("nothing to flush");
    reader.await.expect("the read task finishes")
}

#[tokio::test]
async fn a_canonical_batch_survives_the_protobuf_round_trip() {
    let source = OpenTelemetryMetricsSource::new(push_config("/v1/metrics"), None)
        .expect("the listener binds");
    let sent = canonical_batch();
    let received = round_trip(source, Encoding::Protobuf, &sent).await;
    assert_eq!(received, sent, "every column comes back as it was sent");
}

#[tokio::test]
async fn a_canonical_batch_survives_the_json_round_trip() {
    let source = OpenTelemetryMetricsSource::new(push_config("/v1/metrics"), None)
        .expect("the listener binds");
    let sent = canonical_batch();
    let received = round_trip(source, Encoding::Json, &sent).await;
    assert_eq!(received, sent, "every column comes back as it was sent");
}

#[tokio::test]
async fn a_declared_schema_decides_the_columns_the_source_emits() {
    // `id` is the first numeric column no canonical name claims, so the metric
    // value lands there, `label` comes back from the attribute the sink
    // encodes it as, and `total` likewise.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("total", DataType::Float64, false),
    ]));
    let source = OpenTelemetryMetricsSource::new(push_config("/v1/metrics"), Some(schema.clone()))
        .expect("the listener binds");
    let sent = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
        ],
    )
    .expect("the fixture matches its own schema");

    let endpoint = format!(
        "http://{}{}",
        source.local_addr().expect("the push mode binds"),
        saci_connector_opentelemetry::config::DEFAULT_PATH
    );
    let mut sink = OpenTelemetryMetricsSink::new(sink_config(&endpoint, Encoding::Protobuf), None)
        .expect("the sink builds");
    let mut source = source;
    let reader = tokio::spawn(async move {
        source
            .next_batch()
            .await
            .expect("the read succeeds")
            .expect("a request arrives")
    });
    sink.write_batch(&sent).await.expect("the export is taken");
    let received = reader.await.expect("the read task finishes");

    assert_eq!(received.schema(), schema, "the declared schema is emitted");
    assert_eq!(received, sent, "every declared column comes back");
}

#[tokio::test]
async fn a_wrong_bearer_token_is_refused_and_the_right_one_is_taken() {
    let mut config = push_config("/v1/metrics");
    config.auth = Some(AuthConfig {
        bearer: "s3cret".to_string(),
    });
    let source = OpenTelemetryMetricsSource::new(config, None).expect("the listener binds");
    let endpoint = format!(
        "http://{}{}",
        source.local_addr().expect("the push mode binds"),
        saci_connector_opentelemetry::config::DEFAULT_PATH
    );

    let mut source = source;
    // The read is the runner's own loop: it adopts the listener, so every
    // request below is judged, and it returns the one batch the right token
    // lets through.
    let reader = tokio::spawn(async move {
        source
            .next_batch()
            .await
            .expect("the read succeeds")
            .expect("a request arrives")
    });

    let sent = canonical_batch();
    let mut sink = OpenTelemetryMetricsSink::new(sink_config(&endpoint, Encoding::Protobuf), None)
        .expect("the sink builds");
    let error = sink
        .write_batch(&sent)
        .await
        .expect_err("a request with no token is refused");
    assert!(error.to_string().contains("401"), "{error}");

    let mut wrong = sink_config(&endpoint, Encoding::Protobuf);
    wrong
        .headers
        .insert("authorization".to_string(), "Bearer nope".to_string());
    let mut sink = OpenTelemetryMetricsSink::new(wrong, None).expect("the sink builds");
    let error = sink
        .write_batch(&sent)
        .await
        .expect_err("a request with the wrong token is refused");
    assert!(error.to_string().contains("401"), "{error}");

    let mut right = sink_config(&endpoint, Encoding::Protobuf);
    right
        .headers
        .insert("authorization".to_string(), "Bearer s3cret".to_string());
    let mut sink = OpenTelemetryMetricsSink::new(right, None).expect("the sink builds");
    sink.write_batch(&sent).await.expect("the export is taken");
    let received = reader.await.expect("the read task finishes");
    assert_eq!(received, sent, "the right token is taken");
}

#[tokio::test]
async fn a_pull_read_takes_one_body_and_then_reaches_eof() {
    let sent = canonical_batch();
    let request =
        schema::record_batch_to_export_request(&sent, None, None).expect("the rows build");
    let body = schema::encode(Encoding::Json, &request).expect("the body encodes");
    let (url, server) = serve_once(body, schema::CONTENT_TYPE_JSON.to_string()).await;

    let config = OpenTelemetryMetricsSourceConfig {
        mode: Some(SourceModeConfig::Pull(PullMode {
            url,
            interval_ms: 0,
            headers: HashMap::new(),
            stop_at_end: true,
            max_body_bytes: 1 << 20,
        })),
        ..OpenTelemetryMetricsSourceConfig::default()
    };
    let mut source = OpenTelemetryMetricsSource::new(config, None).expect("the config resolves");
    let received = source
        .next_batch()
        .await
        .expect("the read succeeds")
        .expect("the body holds a request");
    assert_eq!(received, sent, "every column comes back as it was sent");
    assert!(
        source
            .next_batch()
            .await
            .expect("the second read succeeds")
            .is_none(),
        "a stop_at_end read reports EOF once its body is handed over"
    );
    server.await.expect("the stand-in endpoint finishes");
}

#[tokio::test]
async fn a_metric_with_no_data_is_reported_as_a_partial_success() {
    let source = OpenTelemetryMetricsSource::new(push_config("/v1/metrics"), None)
        .expect("the listener binds");
    let endpoint = format!(
        "http://{}{}",
        source.local_addr().expect("the push mode binds"),
        saci_connector_opentelemetry::config::DEFAULT_PATH
    );
    start_listening(source);
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "saci".to_string(),
                    ..Default::default()
                }),
                metrics: vec![Metric {
                    name: "broken".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let response = post(
        &endpoint,
        schema::CONTENT_TYPE_PROTOBUF,
        request.encode_to_vec(),
    )
    .await
    .expect("the request is answered");
    assert_eq!(response.status(), 200);
    let body = response.bytes().await.expect("the response body");
    let response =
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse::decode(
            body,
        )
        .expect("the response decodes");
    let partial = response
        .partial_success
        .expect("a dropped metric is reported");
    assert_eq!(partial.rejected_data_points, 0, "no data point was dropped");
    assert!(
        partial.error_message.contains("carries no data"),
        "{}",
        partial.error_message
    );
}

#[tokio::test]
async fn a_json_body_written_to_the_specification_decodes() {
    // The OTLP JSON mapping writes every 64-bit integer as a decimal string
    // and every enum as a number, which is not what the derived deserializer
    // reads on its own. This body is that shape, from a sender that is not
    // this connector.
    let source = OpenTelemetryMetricsSource::new(push_config("/v1/metrics"), None)
        .expect("the listener binds");
    let endpoint = format!(
        "http://{}{}",
        source.local_addr().expect("the push mode binds"),
        saci_connector_opentelemetry::config::DEFAULT_PATH
    );
    let mut source = source;
    let reader = tokio::spawn(async move {
        source
            .next_batch()
            .await
            .expect("the read succeeds")
            .expect("a request arrives")
    });
    let body = json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "checkout"}}
                ]
            },
            "scopeMetrics": [{
                "scope": {"name": "saci", "version": "0.1.0"},
                "metrics": [{
                    "name": "requests",
                    "description": "requests served",
                    "unit": "1",
                    "sum": {
                        "dataPoints": [{
                            "attributes": [
                                {"key": "http.request.method", "value": {"stringValue": "GET"}},
                                {"key": "retry.count", "value": {"stringValue": "3"}}
                            ],
                            "startTimeUnixNano": "1700000000000000000",
                            "timeUnixNano": "1700000000000000001",
                            "asInt": "5"
                        }],
                        "aggregationTemporality": 2,
                        "isMonotonic": true
                    }
                }]
            }]
        }]
    })
    .to_string();
    let response = post(&endpoint, schema::CONTENT_TYPE_JSON, body.into_bytes())
        .await
        .expect("the request is answered");
    assert_eq!(response.status(), 200);
    let received = reader.await.expect("the read task finishes");

    let schema = schema::metrics_schema();
    let text = |name: &str| -> String {
        received
            .column_by_name(name)
            .expect("the column is in the batch")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a Utf8 column")
            .value(0)
            .to_string()
    };
    let number = |name: &str| -> u64 {
        received
            .column_by_name(name)
            .expect("the column is in the batch")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("a UInt64 column")
            .value(0)
    };
    assert_eq!(received.schema(), schema);
    assert_eq!(received.num_rows(), 1);
    assert_eq!(text("name"), "requests");
    assert_eq!(text("kind"), "sum");
    assert_eq!(text("unit"), "1");
    assert_eq!(
        received
            .column_by_name("value")
            .expect("the value column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("a Float64 column")
            .value(0),
        5.0,
        "asInt decodes as the point's value"
    );
    assert_eq!(number("timestamp"), 1_700_000_000_000_000_001);
    assert_eq!(number("start_timestamp"), 1_700_000_000_000_000_000);
    let attributes: serde_json::Value =
        serde_json::from_str(&text("attributes")).expect("the attributes column is JSON");
    assert_eq!(attributes["http.request.method"], json!("GET"));
    assert_eq!(
        attributes["retry.count"],
        json!("3"),
        "a string attribute that spells a number stays a string"
    );
    assert_eq!(
        attributes["otel.scope"],
        json!({"name": "saci", "version": "0.1.0"})
    );
    let resource: serde_json::Value =
        serde_json::from_str(&text("resource_attributes")).expect("the column is JSON");
    assert_eq!(resource["service.name"], json!("checkout"));
}

#[tokio::test]
async fn a_media_type_the_connector_does_not_read_is_refused() {
    let source = OpenTelemetryMetricsSource::new(push_config("/v1/metrics"), None)
        .expect("the listener binds");
    let endpoint = format!(
        "http://{}{}",
        source.local_addr().expect("the push mode binds"),
        saci_connector_opentelemetry::config::DEFAULT_PATH
    );
    start_listening(source);
    let response = post(&endpoint, "text/plain", b"not a metrics body".to_vec())
        .await
        .expect("the request is answered");
    assert_eq!(
        response.status(),
        415,
        "a body of another media type is refused"
    );
}

#[tokio::test]
async fn a_body_over_the_cap_is_refused_before_it_is_read() {
    let mut config = push_config("/v1/metrics");
    config.max_body_bytes = Some(16);
    let source = OpenTelemetryMetricsSource::new(config, None).expect("the listener binds");
    let endpoint = format!(
        "http://{}{}",
        source.local_addr().expect("the push mode binds"),
        saci_connector_opentelemetry::config::DEFAULT_PATH
    );
    start_listening(source);
    let response = post(&endpoint, schema::CONTENT_TYPE_PROTOBUF, vec![0_u8; 64])
        .await
        .expect("the request is answered");
    assert_eq!(response.status(), 413, "an oversized body is refused");
}

#[tokio::test]
async fn a_request_for_another_path_is_refused() {
    let source = OpenTelemetryMetricsSource::new(push_config("/v1/metrics"), None)
        .expect("the listener binds");
    let endpoint = format!(
        "http://{}/v1/traces",
        source.local_addr().expect("the push mode binds")
    );
    start_listening(source);
    let response = post(&endpoint, schema::CONTENT_TYPE_PROTOBUF, Vec::new())
        .await
        .expect("the request is answered");
    assert_eq!(response.status(), 404, "another path is refused");
}

/// Install the workspace's `ring` crypto provider, once per process.
///
/// `reqwest` is built with `rustls-no-provider`, so its client refuses to build
/// until a provider is installed. The connector installs one in `lib.rs` before
/// every client it builds; this helper stands in for the raw client the cases
/// below build themselves, which never reaches the connector's own install.
fn install_ring() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// POST one body at `endpoint` with `content_type`.
async fn post(
    endpoint: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Result<reqwest::Response, reqwest::Error> {
    install_ring();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .post(endpoint)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body)
        .send()
        .await
}

/// A stand-in endpoint that answers one request with `body` and stops.
///
/// Hand written rather than a server framework: the source reads a status, a
/// `content-type` and a body, and nothing else about the response matters.
async fn serve_once(body: Vec<u8>, content_type: String) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the stand-in endpoint binds");
    let addr = listener.local_addr().expect("it has an address");
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("one request arrives");
        let mut head = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.expect("the head is readable");
            if read == 0 {
                break;
            }
            head.extend_from_slice(&chunk[..read]);
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("the head is writable");
        stream.write_all(&body).await.expect("the body is writable");
        stream.flush().await.expect("the response is flushed");
    });
    (format!("http://{addr}/v1/metrics"), handle)
}
