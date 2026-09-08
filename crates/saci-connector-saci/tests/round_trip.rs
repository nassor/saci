//! One socket, two services: a [`SaciSink`] pushes batches to a [`SaciSource`]
//! and the source's own series name the peer that fed it.
//!
//! Each test installs its own meter provider and reads the Prometheus registry
//! that provider writes into. `set_meter_provider` replaces the global one, so
//! the instruments a test builds after its own call are the ones it reads back.

#![cfg(feature = "metrics")]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema};
use prometheus::{Registry, TextEncoder};
use saci_connector::NodeIdentity;
use saci_connector_saci::wire::{self, Frame, PROTOCOL_VERSION};
use saci_connector_saci::{SaciSink, SaciSource};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).expect("batch")
}

fn sending() -> NodeIdentity {
    NodeIdentity {
        service: "svc-a".to_string(),
        workflow: "w1".to_string(),
        node: "out".to_string(),
    }
}

fn receiving() -> NodeIdentity {
    NodeIdentity {
        service: "svc-b".to_string(),
        workflow: "w2".to_string(),
        node: "in".to_string(),
    }
}

/// The line of `text` whose series is `name`, or a panic naming what was there.
fn series_line(text: &str, name: &str) -> String {
    text.lines()
        .find(|line| line.starts_with(name))
        .unwrap_or_else(|| panic!("no {name} line in:\n{text}"))
        .to_string()
}

/// The line of `text` whose series is `name` and whose labels carry `label`.
fn series_line_with(text: &str, name: &str, label: &str) -> String {
    text.lines()
        .find(|line| line.starts_with(name) && line.contains(label))
        .unwrap_or_else(|| panic!("no {name} line carrying {label} in:\n{text}"))
        .to_string()
}

/// Install a meter provider and hand back the registry its exporter fills.
fn prometheus_registry() -> Registry {
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
    registry
}

/// Render everything the registry holds.
fn gather(registry: &Registry) -> String {
    TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encode prometheus text")
}

#[tokio::test]
async fn batches_cross_a_socket_and_the_peer_shows_up_in_the_series() {
    let registry = prometheus_registry();

    let mut source = SaciSource::bind("127.0.0.1:0", schema(), 8, 1 << 20, receiving())
        .expect("bind the receiving source");
    let addr = source.local_addr();

    // The unreachable first peer proves the failover: the sink must skip it
    // and take the second.
    let mut sink = SaciSink::connect(
        &["127.0.0.1:1".to_string(), addr.to_string()],
        schema(),
        sending(),
        Duration::from_secs(5),
    )
    .expect("resolve both peers");

    let collector = tokio::spawn(async move {
        let mut out = Vec::new();
        while out.len() < 2 {
            match source.next_batch().await.expect("next_batch") {
                Some(batch) => out.push(batch),
                None => break,
            }
        }
        out
    });

    let first = batch(vec![1, 2, 3]);
    let second = batch(vec![4, 5, 6]);
    sink.write_batch(&first).await.expect("write the first");
    sink.write_batch(&second).await.expect("write the second");
    sink.finish().await.expect("finish");

    let received = collector.await.expect("collector task");
    assert_eq!(received, vec![first, second]);

    let text = gather(&registry);

    let rows = series_line(&text, "saci_peer_source_rows_total");
    for label in [
        r#"peer_service="svc-a""#,
        r#"peer_workflow="w1""#,
        r#"peer_sink="out""#,
        r#"source="in""#,
        r#"workflow="w2""#,
    ] {
        assert!(rows.contains(label), "{label} missing from: {rows}");
    }
    assert!(rows.ends_with(" 6"), "six rows crossed, got: {rows}");

    let sessions = series_line(&text, "saci_peer_source_sessions_total");
    assert!(
        sessions.contains(r#"outcome="accepted""#),
        "got: {sessions}"
    );
    assert!(sessions.ends_with(" 1"), "one session, got: {sessions}");
}

/// A sink whose schema is not the source's is refused at the handshake, and
/// the refusal names the mismatch rather than a socket error. Both that
/// refusal and a later frame above the cap land in the source's own series,
/// labelled with the peer that caused them.
#[tokio::test]
async fn a_mismatched_schema_is_refused_at_the_handshake() {
    let registry = prometheus_registry();
    // The cap the second session announces more than.
    let cap = 4096usize;
    let mut source = SaciSource::bind("127.0.0.1:0", schema(), 8, cap, receiving())
        .expect("bind the receiving source");
    let addr = source.local_addr();
    // The accept loop starts on the first `next_batch`, so drive one.
    let listening = tokio::spawn(async move { source.next_batch().await });

    let other = Arc::new(Schema::new(vec![Field::new("other", DataType::Utf8, true)]));
    let mut sink = SaciSink::connect(
        &[addr.to_string()],
        Arc::clone(&other),
        sending(),
        Duration::from_secs(5),
    )
    .expect("resolve");

    let wrong = RecordBatch::try_new(
        other,
        vec![Arc::new(arrow_array::StringArray::from(vec!["x"]))],
    )
    .expect("batch");
    let err = sink
        .write_batch(&wrong)
        .await
        .expect_err("the source must refuse this session");
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(
        err.to_string().contains("refused the session"),
        "got: {err}"
    );
    assert!(err.to_string().contains("schema mismatch"), "got: {err}");

    // A second session, accepted on the source's own schema, then a frame
    // announcing more than the cap.
    let mut peer = TcpStream::connect(addr).await.expect("dial the source");
    let mut hello = Vec::new();
    wire::encode_frame(
        &mut hello,
        &Frame::Hello {
            version: PROTOCOL_VERSION,
            identity: sending(),
            schema: schema(),
        },
    )
    .expect("encode hello");
    peer.write_all(&hello).await.expect("write hello");
    peer.flush().await.expect("flush hello");
    let mut len = [0u8; 4];
    peer.read_exact(&mut len).await.expect("read reply length");
    let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
    peer.read_exact(&mut body).await.expect("read reply body");
    assert!(matches!(
        wire::decode_frame(Buffer::from(body)).expect("decode reply"),
        Frame::Accept
    ));

    let oversized = u32::try_from(cap).expect("a cap that fits") + 1;
    peer.write_all(&oversized.to_be_bytes())
        .await
        .expect("announce an oversized frame");
    peer.flush().await.expect("flush the announcement");
    // The source records the violation before it drops the socket, so reading
    // to the close is the sync point rather than a sleep.
    let mut rest = Vec::new();
    let _ = peer.read_to_end(&mut rest).await;

    let text = gather(&registry);
    let rejected = series_line_with(
        &text,
        "saci_peer_source_sessions_total",
        r#"outcome="rejected""#,
    );
    for label in [
        r#"peer_service="svc-a""#,
        r#"peer_workflow="w1""#,
        r#"peer_sink="out""#,
        r#"source="in""#,
        r#"workflow="w2""#,
    ] {
        assert!(rejected.contains(label), "{label} missing from: {rejected}");
    }
    assert!(
        rejected.ends_with(" 1"),
        "one refused session, got: {rejected}"
    );

    let frame_errors = series_line_with(&text, "saci_peer_source_errors_total", r#"kind="frame""#);
    for label in [
        r#"peer_service="svc-a""#,
        r#"peer_workflow="w1""#,
        r#"peer_sink="out""#,
    ] {
        assert!(
            frame_errors.contains(label),
            "{label} missing from: {frame_errors}"
        );
    }
    assert!(
        frame_errors.ends_with(" 1"),
        "one frame violation, got: {frame_errors}"
    );

    listening.abort();
}
