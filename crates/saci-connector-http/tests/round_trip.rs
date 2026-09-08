//! One transport, every direction: an [`HttpSource`] read decoded by a
//! transformer, an [`HttpSink`] request whose body decodes back to the rows
//! that went in, and an [`HttpPushSource`] that accepts what the sink sends.
//!
//! Three servers, each the smallest thing that proves its point.
//! [`serve`] is hand rolled on a `std::net::TcpListener`: a spawned thread
//! accepts a fixed number of connections, captures each request, and answers
//! with one canned response. No HTTP framework, nothing to configure, and the
//! request count is exact, which is what pins "one request per batch".
//! [`serve_versioned`] is a `hyper_util` auto server, which is what can answer
//! HTTP/2, and it records the version each request arrived on.
//! [`serve_h3`] is an `h3` server on a QUIC endpoint. Every listener binds
//! `127.0.0.1:0`, so the suite runs concurrently with itself.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;

use saci_connector_http::{
    ContentEncoding, HttpPushOptions, HttpPushSource, HttpSink, HttpSinkOptions, HttpSource,
    HttpSourceOptions, HttpVersion, Pagination, ReadMode, SchemaFrom, ServerTls, TlsFiles, Window,
};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_transformer::Transformer;
use saci_transformer_csv::CsvTransformer;
use saci_transformer_ndjson::NdjsonTransformer;
use saci_transformer_parquet::ParquetTransformer;

// ── the hand-rolled HTTP/1.1 test server ────────────────────────────────────

/// One captured request: the method from the request line, the header names and
/// values, and the body.
struct Captured {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Captured {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// One canned reply: a status line, extra headers, and a body.
#[derive(Clone)]
struct Reply {
    status: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    fn ok(body: Vec<u8>) -> Self {
        Self {
            status: "200 OK".to_string(),
            headers: Vec::new(),
            body,
        }
    }

    fn status(status: &str) -> Self {
        Self {
            status: status.to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn with(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

/// Bind a server that answers `replies.len()` requests, in order.
///
/// Returns the url to aim a connector at and the handle carrying every captured
/// request, in arrival order.
fn serve_each(replies: Vec<Reply>) -> (String, JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/data", listener.local_addr().expect("local_addr"));
    let handle = std::thread::spawn(move || {
        let mut captured = Vec::with_capacity(replies.len());
        for reply in replies {
            let (mut stream, _) = listener.accept().expect("accept");
            captured.push(exchange(&mut stream, &reply));
        }
        captured
    });
    (url, handle)
}

/// The common case: `connections` identical replies.
fn serve(connections: usize, status: &str, body: Vec<u8>) -> (String, JoinHandle<Vec<Captured>>) {
    let reply = Reply {
        status: status.to_string(),
        headers: Vec::new(),
        body,
    };
    serve_each(vec![reply; connections])
}

/// Read one request off `stream` and write `reply` back.
///
/// `connection: close` on the response is what makes the connection count
/// deterministic: the client cannot pool a socket the server said it would
/// close, so N requests are N accepts.
fn exchange(stream: &mut std::net::TcpStream, reply: &Reply) -> Captured {
    let captured = read_request(stream);
    write_reply(stream, reply);
    captured
}

/// Read one whole request: the head, then `content-length` body bytes.
fn read_request(stream: &mut std::net::TcpStream) -> Captured {
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
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut words = request_line.split(' ');
    let method = words.next().unwrap_or_default().to_string();
    let target = words.next().unwrap_or_default().to_string();

    let mut headers = Vec::new();
    let mut length = 0usize;
    for line in lines.filter(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "content-length" {
            length = value.parse().expect("content-length is a number");
        }
        headers.push((name, value));
    }

    // Whatever arrived past the head is already body; read the rest. Every
    // request this server sees carries a length: the sink sends one whole
    // document per request, so reqwest never falls back to chunked.
    let mut body = buffered[head_end..].to_vec();
    while body.len() < length {
        let read = stream.read(&mut chunk).expect("read request body");
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }

    Captured {
        method,
        target,
        headers,
        body,
    }
}

/// Write one canned reply and flush it.
fn write_reply(stream: &mut std::net::TcpStream, reply: &Reply) {
    let mut head = format!(
        "HTTP/1.1 {}\r\ncontent-length: {}\r\nconnection: close\r\n",
        reply.status,
        reply.body.len()
    );
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(&reply.body).expect("write body");
    stream.flush().expect("flush");
}

/// A url nothing is listening on: bound, its port read, then released.
fn dead_url() -> String {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe bind");
    let addr = probe.local_addr().expect("probe addr");
    drop(probe);
    format!("http://{addr}/data")
}

// ── fixtures ────────────────────────────────────────────────────────────────

const CSV_BODY: &[u8] = b"id,total\n1,10\n2,20\n3,30\n";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
    ]))
}

fn csv() -> Arc<dyn Transformer> {
    Arc::new(CsvTransformer::new(true))
}

fn ndjson() -> Arc<dyn Transformer> {
    Arc::new(NdjsonTransformer::default())
}

fn parquet() -> Arc<dyn Transformer> {
    Arc::new(ParquetTransformer::new())
}

fn batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from_iter_values(ids.iter().map(|id| id * 10))),
        ],
    )
    .expect("batch")
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

/// Decode a captured request body back through the same format the sink wrote
/// it with, which is what makes the round trip a round trip.
fn decode_body(body: &[u8]) -> Vec<RecordBatch> {
    let mut spool = tempfile::tempfile().expect("temp file");
    spool.write_all(body).expect("write body");
    std::io::Seek::rewind(&mut spool).expect("rewind");

    let mut reader = csv()
        .open_reader(spool, Some(schema()))
        .expect("the body is a whole csv document");
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch().expect("decode") {
        batches.push(batch);
    }
    batches
}

/// One whole parquet document, the way a real endpoint would serve one: the
/// format carries its own schema, so nothing outside the bytes describes it.
fn parquet_body(schema: Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
    let file = tempfile::NamedTempFile::new().expect("temp file");
    let mut writer = parquet()
        .open_writer(Box::new(file.reopen().expect("reopen")), schema)
        .expect("writer");
    writer.write_batch(batch).expect("write");
    writer.finish().expect("finish");
    std::fs::read(file.path()).expect("read back the document")
}

/// A csv document source with the declared schema, the shape most tests want.
fn source(url: &str, headers: Vec<(String, String)>) -> HttpSource {
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        ..HttpSourceOptions::default()
    };
    options.client.headers = headers;
    options.client.timeout = Duration::from_secs(5);
    HttpSource::new(url, csv(), options).expect("source builds")
}

/// A csv sink, the shape most tests want.
fn sink(url: &str, method: &str) -> HttpSink {
    let mut options = HttpSinkOptions {
        method: method.to_string(),
        ..HttpSinkOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    HttpSink::new(url, schema(), csv(), options).expect("sink builds")
}

/// A self-signed certificate for `localhost` and `127.0.0.1`, PEM, in a temp
/// directory that lives as long as the returned handle.
struct Certificate {
    _dir: tempfile::TempDir,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
}

fn certificate() -> Certificate {
    let dir = tempfile::tempdir().expect("temp dir");
    let issued =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("self-signed certificate");
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, issued.cert.pem()).expect("write cert");
    std::fs::write(&key, issued.signing_key.serialize_pem()).expect("write key");
    Certificate {
        _dir: dir,
        cert,
        key,
    }
}

/// Drain every batch a source yields until it reports EOF.
async fn drain(source: &mut HttpSource) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("read") {
        batches.push(batch);
    }
    batches
}

// ── the source half, document mode ──────────────────────────────────────────

#[tokio::test]
async fn one_get_yields_the_rows_in_the_body_then_eof() {
    let (url, server) = serve(1, "200 OK", CSV_BODY.to_vec());
    let mut source = source(&url, vec![("accept".to_string(), "text/csv".to_string())]);

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the body holds rows");
    assert_eq!(column(&batch, "id"), vec![1, 2, 3]);
    assert_eq!(column(&batch, "total"), vec![10, 20, 30]);
    assert!(
        source.next_batch().await.expect("read").is_none(),
        "one GET reaches EOF, which is what makes the source finite"
    );

    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 1, "the body is fetched once, not per batch");
    assert_eq!(captured[0].method, "GET");
    assert_eq!(captured[0].header("accept"), Some("text/csv"));
}

/// EOF re-arms the source, so the call after it issues a fresh GET. That is
/// what makes `interval` re-read the resource on every tick instead of
/// reporting EOF forever after the first read.
#[tokio::test]
async fn a_read_after_eof_issues_a_fresh_get() {
    let (url, server) = serve(2, "200 OK", CSV_BODY.to_vec());
    let mut source = source(&url, Vec::new());

    assert_eq!(drain(&mut source).await.len(), 1, "the first read");
    let second = drain(&mut source).await;
    assert_eq!(second.len(), 1, "the source re-armed after EOF");
    assert_eq!(column(&second[0], "id"), vec![1, 2, 3]);

    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 2, "one GET per read, not one per source");
}

/// `estimated_rows` forwards the reader's, so it is `None` before the fetch and
/// still `None` for a format that counts nothing.
#[tokio::test]
async fn estimated_rows_forwards_what_the_reader_reported() {
    let (url, server) = serve(1, "200 OK", CSV_BODY.to_vec());
    let mut source = source(&url, Vec::new());
    assert_eq!(source.estimated_rows(), None, "nothing has been fetched");

    drain(&mut source).await;
    assert_eq!(source.estimated_rows(), None, "csv counts no rows");
    server.join().expect("server thread");
}

#[tokio::test]
async fn a_non_2xx_status_names_the_status_and_the_url() {
    let (url, server) = serve(1, "500 Internal Server Error", b"boom".to_vec());
    let mut source = source(&url, Vec::new());

    let Err(err) = source.next_batch().await else {
        panic!("a 500 is not a body to decode");
    };
    assert!(
        err.message().starts_with("HttpSource: status 500"),
        "got: {err}"
    );
    assert!(err.message().contains(&url), "got: {err}");
    server.join().expect("server thread");
}

#[tokio::test]
async fn a_refused_connection_names_the_url() {
    let url = dead_url();
    let mut source = source(&url, Vec::new());

    let Err(err) = source.next_batch().await else {
        panic!("nothing is listening");
    };
    assert!(
        err.message()
            .starts_with(&format!("HttpSource: cannot GET {url}")),
        "got: {err}"
    );
}

/// A body far larger than any single read decodes the same: it reaches the
/// spool file chunk by chunk rather than as one whole-body copy.
#[tokio::test]
async fn a_body_much_larger_than_one_read_decodes_whole() {
    let rows = 400_000;
    let mut body = b"id,total\n".to_vec();
    for id in 1..=rows {
        body.extend_from_slice(format!("{id},{}\n", id * 10).as_bytes());
    }
    assert!(body.len() > 4 * 1024 * 1024, "several megabytes of csv");

    let (url, server) = serve(1, "200 OK", body);
    let mut source = source(&url, Vec::new());
    let total: usize = drain(&mut source)
        .await
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(total, rows as usize);
    server.join().expect("server thread");
}

/// A `content-length` above the cap is refused before the body is read, so the
/// bytes never reach the disk at all.
#[tokio::test]
async fn an_announced_length_over_the_cap_is_refused() {
    let (url, server) = serve(1, "200 OK", CSV_BODY.to_vec());
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        max_body_bytes: Some(8),
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let Err(err) = source.next_batch().await else {
        panic!("a 24-byte body does not fit in 8 bytes");
    };
    assert!(
        err.message().contains("exceeds max_body_bytes (8)"),
        "got: {err}"
    );
    server.join().expect("server thread");
}

/// The same cap, discovered rather than announced: a response with no
/// `content-length` is refused as soon as it overruns.
#[tokio::test]
async fn a_body_that_overruns_the_cap_is_refused() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/data", listener.local_addr().expect("addr"));
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        read_request(&mut stream);
        // No `content-length` and no chunking: the body is whatever arrives
        // until the connection closes, so nothing announces its size.
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n")
            .expect("head");
        let _ = stream.write_all(b"id,total\n1,10\n2,20\n3,30\n4,40\n");
        let _ = stream.flush();
    });

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        max_body_bytes: Some(20),
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let Err(err) = source.next_batch().await else {
        panic!("29 bytes do not fit in 20");
    };
    assert!(
        err.message().contains("exceeds max_body_bytes (20)"),
        "got: {err}"
    );
    server.join().expect("server thread");
}

/// The cap is a limit, not a budget to stay under: a body exactly its size is
/// read like any other.
#[tokio::test]
async fn a_body_exactly_the_size_of_the_cap_is_accepted() {
    let (url, server) = serve(1, "200 OK", CSV_BODY.to_vec());
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        max_body_bytes: Some(CSV_BODY.len()),
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("24 bytes fit in a 24-byte cap");
    assert_eq!(column(&batch, "id"), vec![1, 2, 3]);
    server.join().expect("server thread");
}

/// A body that stops short of the length it announced fails the read, naming
/// the url it could not be read from. The rows that did arrive are never
/// handed on as though they were all of them.
#[tokio::test]
async fn a_body_that_stops_short_of_its_content_length_is_an_error() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/data", listener.local_addr().expect("addr"));
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        read_request(&mut stream);
        // 100 bytes announced, 17 written, then the connection goes away.
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\nconnection: close\r\n\r\n")
            .expect("head");
        let _ = stream.write_all(b"id,total\n1,10\n2,2");
        let _ = stream.flush();
    });

    let mut source = source(&url, Vec::new());
    let Err(err) = source.next_batch().await else {
        panic!("a body cut short of its announced length is not a document");
    };
    assert!(
        err.message()
            .starts_with(&format!("HttpSource: cannot read {url}:")),
        "got: {err}"
    );
    assert!(
        !err.message().contains("max_body_bytes"),
        "the read failed on the truncation, not on a cap: {err}"
    );
    server.join().expect("server thread");
}

/// A gzip-encoded body decodes to the same rows with no key at all: reqwest
/// asks for the encoding and decodes it transparently.
#[tokio::test]
async fn a_compressed_body_decodes_transparently() {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(CSV_BODY).expect("gzip");
    let gzipped = encoder.finish().expect("gzip");

    let (url, server) = serve_each(vec![Reply::ok(gzipped).with("content-encoding", "gzip")]);
    let mut source = source(&url, Vec::new());
    let batches = drain(&mut source).await;
    assert_eq!(column(&batches[0], "id"), vec![1, 2, 3]);

    let captured = server.join().expect("server thread");
    assert!(
        captured[0]
            .header("accept-encoding")
            .is_some_and(|v| v.contains("gzip")),
        "the request asked for it: {:?}",
        captured[0].header("accept-encoding")
    );
}

/// `conditional` remembers the validators the last response carried and sends
/// them back, so an unchanged resource answers `304` and nothing is decoded.
#[tokio::test]
async fn a_conditional_read_sends_the_validators_and_honours_a_304() {
    let (url, server) = serve_each(vec![
        Reply::ok(CSV_BODY.to_vec())
            .with("etag", "\"v1\"")
            .with("last-modified", "Tue, 01 Jan 2030 00:00:00 GMT"),
        Reply::status("304 Not Modified"),
    ]);
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Document {
            pagination: Pagination::None,
            conditional: true,
        },
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    assert_eq!(drain(&mut source).await.len(), 1, "the first read");
    assert!(
        source.next_batch().await.expect("read").is_none(),
        "a 304 decodes nothing"
    );

    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].header("if-none-match"), None);
    assert_eq!(captured[1].header("if-none-match"), Some("\"v1\""));
    assert_eq!(
        captured[1].header("if-modified-since"),
        Some("Tue, 01 Jan 2030 00:00:00 GMT")
    );
}

/// `pagination "link"` follows `rel="next"` and reports EOF only once a page
/// carries no next link, so both pages' rows are one read.
#[tokio::test]
async fn a_paginated_read_follows_the_next_link_across_pages() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{addr}/data");
    let server = std::thread::spawn(move || {
        let mut captured = Vec::new();
        // The first page links the second by a relative target, which the
        // source has to resolve against the page it came from.
        let replies = [
            Reply::ok(b"id,total\n1,10\n2,20\n3,30\n".to_vec())
                .with("link", "</data?page=2>; rel=\"next\""),
            Reply::ok(b"id,total\n4,40\n5,50\n6,60\n".to_vec()),
        ];
        for reply in replies {
            let (mut stream, _) = listener.accept().expect("accept");
            captured.push(exchange(&mut stream, &reply));
        }
        captured
    });

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Document {
            pagination: Pagination::Link,
            conditional: false,
        },
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let ids: Vec<i64> = drain(&mut source)
        .await
        .iter()
        .flat_map(|batch| column(batch, "id"))
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6], "both pages, one read");

    let captured = server.join().expect("server thread");
    assert_eq!(captured[0].target, "/data");
    assert_eq!(captured[1].target, "/data?page=2");
}

/// A page linking itself would loop forever, so it is an error naming the page.
#[tokio::test]
async fn a_next_link_pointing_at_the_same_page_is_an_error() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{addr}/data");
    let link = format!("<{url}>; rel=\"next\"");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        exchange(
            &mut stream,
            &Reply::ok(CSV_BODY.to_vec()).with("link", &link),
        );
    });

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Document {
            pagination: Pagination::Link,
            conditional: false,
        },
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let Err(err) = source.next_batch().await else {
        panic!("a self-link is an infinite read");
    };
    assert!(
        err.message().contains("points back at the same page"),
        "got: {err}"
    );
    server.join().expect("server thread");
}

/// A resource with no `etag` is conditional all the same: the one validator
/// it does carry goes back, and nothing stands in for the one it does not.
#[tokio::test]
async fn a_conditional_read_sends_last_modified_alone_when_there_is_no_etag() {
    let (url, server) = serve_each(vec![
        Reply::ok(CSV_BODY.to_vec()).with("last-modified", "Tue, 01 Jan 2030 00:00:00 GMT"),
        Reply::status("304 Not Modified"),
    ]);
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Document {
            pagination: Pagination::None,
            conditional: true,
        },
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    assert_eq!(drain(&mut source).await.len(), 1, "the first read");
    assert!(
        source.next_batch().await.expect("read").is_none(),
        "a 304 decodes nothing"
    );

    let captured = server.join().expect("server thread");
    assert_eq!(
        captured[1].header("if-modified-since"),
        Some("Tue, 01 Jan 2030 00:00:00 GMT")
    );
    assert_eq!(
        captured[1].header("if-none-match"),
        None,
        "there was no etag to send back"
    );
}

/// The validators describe the resource, which is its first page: the pages
/// after it carry none, and a `304` on the first ends the read there.
#[tokio::test]
async fn a_paginated_conditional_read_validates_the_first_page_only() {
    let (url, server) = serve_each(vec![
        Reply::ok(b"id,total\n1,10\n".to_vec())
            .with("etag", "\"v1\"")
            .with("link", "</data?page=2>; rel=\"next\""),
        Reply::ok(b"id,total\n2,20\n".to_vec()),
        Reply::status("304 Not Modified"),
        Reply::status("304 Not Modified"),
    ]);

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Document {
            pagination: Pagination::Link,
            conditional: true,
        },
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let ids: Vec<i64> = drain(&mut source)
        .await
        .iter()
        .flat_map(|batch| column(batch, "id"))
        .collect();
    assert_eq!(ids, vec![1, 2], "both pages, one read");
    assert!(
        source.next_batch().await.expect("read").is_none(),
        "the 304 ends the second read"
    );
    assert!(
        source.next_batch().await.expect("read").is_none(),
        "and the third starts over at the first page"
    );

    let captured = server.join().expect("server thread");
    assert_eq!(captured[0].header("if-none-match"), None);
    assert_eq!(captured[1].target, "/data?page=2");
    assert_eq!(
        captured[1].header("if-none-match"),
        None,
        "a page is not the resource"
    );
    assert_eq!(captured[2].header("if-none-match"), Some("\"v1\""));
    assert_eq!(
        captured[3].target, "/data",
        "the 304 left no page pending behind it"
    );
}

/// Every page's `Link` resolves against the page that carried it, so a
/// relative target moves with the pages instead of against the configured
/// url: `../c/page2` from `/a/b/page1` is `/a/c/page2`, and `page3` from
/// there is `/a/c/page3`.
#[tokio::test]
async fn a_relative_next_link_resolves_against_the_page_that_carried_it() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{addr}/a/b/page1");
    let server = std::thread::spawn(move || {
        let replies = [
            Reply::ok(b"id,total\n1,10\n".to_vec()).with("link", "<../c/page2>; rel=\"next\""),
            Reply::ok(b"id,total\n2,20\n".to_vec()).with("link", "<page3>; rel=\"next\""),
            Reply::ok(b"id,total\n3,30\n".to_vec()),
        ];
        let mut captured = Vec::new();
        for reply in replies {
            let (mut stream, _) = listener.accept().expect("accept");
            captured.push(exchange(&mut stream, &reply));
        }
        captured
    });

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Document {
            pagination: Pagination::Link,
            conditional: false,
        },
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let ids: Vec<i64> = drain(&mut source)
        .await
        .iter()
        .flat_map(|batch| column(batch, "id"))
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);

    let captured = server.join().expect("server thread");
    assert_eq!(captured[1].target, "/a/c/page2");
    assert_eq!(captured[2].target, "/a/c/page3");
}

/// A read that was redirected resolves the next link against the page it
/// ended on rather than the url that was asked for: `/data` answers
/// `302 Found` for `/a/b/page1`, whose relative `<page2>` is `/a/b/page2`
/// and not `/page2`.
#[tokio::test]
async fn a_next_link_resolves_against_the_url_a_redirect_ended_on() {
    let (url, server) = serve_each(vec![
        Reply::status("302 Found").with("location", "/a/b/page1"),
        Reply::ok(b"id,total\n1,10\n2,20\n".to_vec()).with("link", "<page2>; rel=\"next\""),
        Reply::ok(b"id,total\n3,30\n".to_vec()),
    ]);
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Document {
            pagination: Pagination::Link,
            conditional: false,
        },
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    // `drain` returns on EOF, which is the page with no next link ending the
    // read rather than a fourth request.
    let ids: Vec<i64> = drain(&mut source)
        .await
        .iter()
        .flat_map(|batch| column(batch, "id"))
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "both pages, one read");

    let captured = server.join().expect("server thread");
    let targets: Vec<&str> = captured
        .iter()
        .map(|request| request.target.as_str())
        .collect();
    assert_eq!(targets, ["/data", "/a/b/page1", "/a/b/page2"]);
}

/// A row the format cannot read ends that read where it happened: the
/// batches before it are delivered, the error is reported once, and the
/// source re-arms rather than reporting the same failure forever.
#[tokio::test]
async fn a_decode_error_mid_document_ends_the_read_and_re_arms() {
    // arrow-csv reads 1024 records per batch, so the good rows are already a
    // batch of their own by the time the bad row is parsed.
    let mut body = b"id,total\n".to_vec();
    for id in 1..=1024 {
        body.extend_from_slice(format!("{id},{}\n", id * 10).as_bytes());
    }
    body.extend_from_slice(b"oops,10\n");

    let (url, server) = serve(2, "200 OK", body);
    let mut source = source(&url, Vec::new());

    let first = source
        .next_batch()
        .await
        .expect("read")
        .expect("the rows before the bad one");
    assert_eq!(first.num_rows(), 1024);
    let Err(err) = source.next_batch().await else {
        panic!("'oops' is not an Int64");
    };
    assert!(
        err.message().starts_with("csv: read error"),
        "the format's own error, not a wrapped one: {err}"
    );
    assert!(
        source.next_batch().await.expect("read").is_none(),
        "the failed read is over"
    );

    let again = source
        .next_batch()
        .await
        .expect("read")
        .expect("a fresh GET of the same resource");
    assert_eq!(again.num_rows(), 1024);
    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 2, "one GET per read");
}

/// `schema_from "body"` takes the body's own schema rather than projecting
/// onto the declared one. The declared schema is still what the graph is
/// validated against, so it is checked rather than dropped.
#[tokio::test]
async fn a_self_describing_body_is_read_with_its_own_schema() {
    let (url, server) = serve(1, "200 OK", parquet_body(schema(), &batch(&[1, 2, 3])));
    let mut source = HttpSource::new(
        &url,
        parquet(),
        HttpSourceOptions {
            declared: Some(schema()),
            schema_from: SchemaFrom::Body,
            ..HttpSourceOptions::default()
        },
    )
    .expect("source builds");

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the body holds rows");
    assert_eq!(column(&batch, "id"), vec![1, 2, 3]);
    assert_eq!(column(&batch, "total"), vec![10, 20, 30]);
    assert_eq!(
        source.schema().fields(),
        schema().fields(),
        "the declared schema is what the graph was validated against"
    );
    server.join().expect("server thread");
}

/// The default `schema_from "config"` projects the body onto the declared
/// schema instead, so a self-describing body carrying extra columns needs no
/// `schema_from` at all: the graph was validated against the declaration and
/// the declaration is what arrives.
#[tokio::test]
async fn a_self_describing_body_is_projected_onto_the_declared_schema() {
    let wide = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
        Field::new("extra", DataType::Int64, false),
    ]));
    let body = parquet_body(
        Arc::clone(&wide),
        &RecordBatch::try_new(
            wide,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .expect("batch"),
    );
    let (url, server) = serve(1, "200 OK", body);
    let mut source = HttpSource::new(
        &url,
        parquet(),
        HttpSourceOptions {
            declared: Some(schema()),
            ..HttpSourceOptions::default()
        },
    )
    .expect("source builds");

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the body holds rows");
    assert_eq!(batch.schema_ref().fields(), schema().fields());
    assert_eq!(column(&batch, "id"), vec![1, 2, 3]);
    assert_eq!(column(&batch, "total"), vec![10, 20, 30]);
    assert!(
        batch.schema().field_with_name("extra").is_err(),
        "the undeclared column is projected away"
    );
    server.join().expect("server thread");
}

/// The declared schema is a promise about the body. A body that carries
/// something else is a configuration error naming both, not rows cast into
/// shape.
#[tokio::test]
async fn a_body_schema_that_differs_from_the_declared_one_is_a_configuration_error() {
    let wide = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
        Field::new("extra", DataType::Int64, false),
    ]));
    let body = parquet_body(
        Arc::clone(&wide),
        &RecordBatch::try_new(
            wide,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .expect("batch"),
    );
    let (url, server) = serve(1, "200 OK", body);
    let mut source = HttpSource::new(
        &url,
        parquet(),
        HttpSourceOptions {
            declared: Some(schema()),
            schema_from: SchemaFrom::Body,
            ..HttpSourceOptions::default()
        },
    )
    .expect("source builds");

    let Err(err) = source.next_batch().await else {
        panic!("a body carrying another schema is not rows to deliver");
    };
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(
        err.message()
            .contains("carries schema [id: Int64, total: Int64, extra: Int64]"),
        "got: {err}"
    );
    assert!(
        err.message()
            .contains("the config declared [id: Int64, total: Int64]"),
        "got: {err}"
    );
    server.join().expect("server thread");
}

/// With no declared schema there is nothing for the graph check to compare, so
/// the source refuses while it builds rather than failing the link later.
#[test]
fn schema_from_body_without_a_declared_schema_is_a_configuration_error() {
    let Err(err) = HttpSource::new(
        "https://example.invalid/orders.parquet",
        parquet(),
        HttpSourceOptions {
            schema_from: SchemaFrom::Body,
            ..HttpSourceOptions::default()
        },
    ) else {
        panic!("schema_from body needs a schema to check against");
    };
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(err.message().contains("schema_fields"), "got: {err}");
}

// ── the source half, streaming modes ────────────────────────────────────────

/// One live connection's answer: a status line, then the chunks its body
/// arrives in.
#[derive(Clone)]
struct Live {
    status: String,
    chunks: Vec<Vec<u8>>,
}

impl Live {
    fn ok(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            status: "200 OK".to_string(),
            chunks,
        }
    }

    fn status(status: &str) -> Self {
        Self {
            status: status.to_string(),
            chunks: Vec::new(),
        }
    }
}

/// A live body that writes a few records, pauses, writes more, and closes.
///
/// `chunks` are written in order, sleeping `gap` between them. The server
/// accepts `connections` times, so a reconnect is observable.
fn serve_live(
    connections: usize,
    content_type: &str,
    chunks: Vec<Vec<u8>>,
    gap: Duration,
) -> (String, JoinHandle<Vec<Captured>>) {
    serve_live_each(content_type, vec![Live::ok(chunks); connections], gap)
}

/// The same server with one answer per connection, so a reconnect can be
/// served something other than what the connection before it got.
fn serve_live_each(
    content_type: &str,
    answers: Vec<Live>,
    gap: Duration,
) -> (String, JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/live", listener.local_addr().expect("addr"));
    let content_type = content_type.to_string();
    let handle = std::thread::spawn(move || {
        let mut captured = Vec::with_capacity(answers.len());
        let mut writers = Vec::new();
        for answer in answers {
            let (mut stream, _) = listener.accept().expect("accept");
            captured.push(read_request(&mut stream));
            let head = format!(
                "HTTP/1.1 {}\r\ncontent-type: {content_type}\r\n\
                 transfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                answer.status
            );
            // One thread per body, so the next connection is accepted while
            // this one is still writing: a reader that gave up on a stalled
            // body reconnects instead of queueing behind it.
            writers.push(std::thread::spawn(move || {
                if stream.write_all(head.as_bytes()).is_err() {
                    return;
                }
                for chunk in &answer.chunks {
                    let framed = format!("{:x}\r\n", chunk.len());
                    if stream.write_all(framed.as_bytes()).is_err() {
                        break;
                    }
                    if stream.write_all(chunk).is_err() {
                        break;
                    }
                    if stream.write_all(b"\r\n").is_err() {
                        break;
                    }
                    let _ = stream.flush();
                    std::thread::sleep(gap);
                }
                let _ = stream.write_all(b"0\r\n\r\n");
                let _ = stream.flush();
            }));
        }
        for writer in writers {
            writer.join().expect("writer thread");
        }
        captured
    });
    (url, handle)
}

fn live_source(url: &str, mode: ReadMode) -> HttpSource {
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode,
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    HttpSource::new(url, ndjson(), options).expect("source builds")
}

/// A window closes on `poll_timeout` as well as on `batch_size`, so a live
/// body that pauses hands over what it has instead of holding it.
#[tokio::test]
async fn a_line_stream_closes_a_window_on_the_poll_timeout() {
    let (url, server) = serve_live(
        1,
        "application/x-ndjson",
        vec![
            b"{\"id\":1,\"total\":10}\n{\"id\":2,\"total\":20}\n{\"id\":3,\"total\":30}\n".to_vec(),
            b"{\"id\":4,\"total\":40}\n{\"id\":5,\"total\":50}\n".to_vec(),
        ],
        Duration::from_millis(400),
    );
    let mut source = live_source(
        &url,
        ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_secs(30),
        }),
    );

    let first = source
        .next_batch()
        .await
        .expect("read")
        .expect("three lines arrived together");
    assert_eq!(column(&first, "id"), vec![1, 2, 3]);
    let second = source
        .next_batch()
        .await
        .expect("read")
        .expect("the pause closed the first window");
    assert_eq!(column(&second, "id"), vec![4, 5]);

    // The body ends, which a line stream reports before it reconnects.
    let Err(err) = source.next_batch().await else {
        panic!("a line stream reports the end of its body");
    };
    assert!(err.message().contains("ended"), "got: {err}");
    drop(source);
    server.join().expect("server thread");
}

/// `batch_size` closes a window before `poll_timeout` does.
#[tokio::test]
async fn a_line_stream_closes_a_window_on_batch_size() {
    let (url, server) = serve_live(
        1,
        "application/x-ndjson",
        vec![
            b"{\"id\":1,\"total\":10}\n{\"id\":2,\"total\":20}\n{\"id\":3,\"total\":30}\n".to_vec(),
        ],
        Duration::from_millis(10),
    );
    let mut source = live_source(
        &url,
        ReadMode::Lines(Window {
            batch_size: 2,
            poll_timeout: Duration::from_secs(30),
            reconnect: Duration::from_secs(30),
        }),
    );

    let first = source.next_batch().await.expect("read").expect("two rows");
    assert_eq!(column(&first, "id"), vec![1, 2], "the window filled");
    drop(source);
    server.join().expect("server thread");
}

/// A body that ends is re-requested after `reconnect`, so a server that drops
/// the connection does not end the source.
#[tokio::test]
async fn a_line_stream_reconnects_after_the_body_ends() {
    let (url, server) = serve_live(
        2,
        "application/x-ndjson",
        vec![b"{\"id\":7,\"total\":70}\n".to_vec()],
        Duration::from_millis(10),
    );
    let mut source = live_source(
        &url,
        ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_millis(50),
        }),
    );

    let first = source.next_batch().await.expect("read").expect("one row");
    assert_eq!(column(&first, "id"), vec![7]);
    let Err(_) = source.next_batch().await else {
        panic!("the body ended");
    };
    let second = source
        .next_batch()
        .await
        .expect("read")
        .expect("the reader reconnected");
    assert_eq!(column(&second, "id"), vec![7]);

    drop(source);
    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 2, "two connections, so one reconnect");
}

/// The SSE rules all at once: a comment is ignored, an event whose name the
/// filter does not want is skipped, a two-line `data` is joined, and the `id`
/// and `retry` fields govern the reconnect.
#[tokio::test]
async fn an_event_stream_filters_by_name_and_remembers_the_last_id() {
    let body = concat!(
        ": keepalive\n\n",
        "event: ping\ndata: {\"id\":99,\"total\":990}\n\n",
        "event: order\nid: 7\nretry: 50\n",
        "data: {\"id\":1,\n",
        "data: \"total\":10}\n\n",
    )
    .as_bytes()
    .to_vec();
    let (url, server) = serve_live(
        2,
        "text/event-stream",
        vec![body],
        Duration::from_millis(10),
    );
    let mut source = live_source(
        &url,
        ReadMode::Events {
            event: Some("order".to_string()),
            window: Window {
                batch_size: 1_000,
                poll_timeout: Duration::from_millis(100),
                // Replaced by the stream's own `retry: 50`.
                reconnect: Duration::from_secs(30),
            },
        },
    );

    let first = source
        .next_batch()
        .await
        .expect("read")
        .expect("the order event");
    assert_eq!(column(&first, "id"), vec![1], "the ping was filtered out");
    assert_eq!(column(&first, "total"), vec![10], "the data lines joined");

    // The body ends silently for an event stream, so the next batch is the
    // reconnected one, and it must arrive within the stream's own retry.
    let second = tokio::time::timeout(Duration::from_secs(5), source.next_batch())
        .await
        .expect("the reader reconnected inside the retry")
        .expect("read")
        .expect("the second connection");
    assert_eq!(column(&second, "id"), vec![1]);

    drop(source);
    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 2);
    assert_eq!(
        captured[0].header("accept"),
        Some("text/event-stream"),
        "the mode asks for the media type"
    );
    assert_eq!(captured[0].header("last-event-id"), None);
    assert_eq!(
        captured[1].header("last-event-id"),
        Some("7"),
        "the reconnect resumes where the stream said"
    );
}

/// A live read reports a non-2xx the way a document read does, and then asks
/// again: one bad answer does not end the source.
#[tokio::test]
async fn a_live_non_2xx_status_is_reported_and_the_reader_reconnects() {
    let (url, server) = serve_live_each(
        "application/x-ndjson",
        vec![
            Live::status("503 Service Unavailable"),
            Live::ok(vec![b"{\"id\":3,\"total\":30}\n".to_vec()]),
        ],
        Duration::from_millis(10),
    );
    let mut source = live_source(
        &url,
        ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_millis(50),
        }),
    );

    let Err(err) = source.next_batch().await else {
        panic!("a 503 is not a body to decode");
    };
    assert!(
        err.message().starts_with("HttpSource: status 503"),
        "got: {err}"
    );
    assert!(err.message().contains(&url), "got: {err}");

    let batch = tokio::time::timeout(Duration::from_secs(5), source.next_batch())
        .await
        .expect("the reader asked again")
        .expect("read")
        .expect("the second connection served a line");
    assert_eq!(column(&batch, "id"), vec![3]);

    drop(source);
    let captured = server.join().expect("server thread");
    assert_eq!(
        captured.len(),
        2,
        "the 503 cost one connection, not the run"
    );
}

/// Nothing is listening, so every attempt fails the same way. The reader
/// keeps asking rather than ending the source on the first refusal.
#[tokio::test]
async fn a_live_transport_failure_is_reported_on_every_attempt() {
    let url = dead_url();
    let mut source = live_source(
        &url,
        ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_millis(50),
        }),
    );

    for attempt in 1..=2 {
        // A refused connect costs a couple of seconds on some platforms, so
        // this bound is about the reader still answering, not about speed.
        let outcome = tokio::time::timeout(Duration::from_secs(20), source.next_batch())
            .await
            .expect("the reader answers rather than going quiet");
        let Err(err) = outcome else {
            panic!("nothing is listening, attempt {attempt}");
        };
        assert!(
            err.message()
                .starts_with(&format!("HttpSource: cannot GET {url}")),
            "got: {err}"
        );
    }
}

/// In a streaming mode `timeout` is the idle gap between chunks rather than a
/// deadline on a body that never ends, so a stalled body fails that read and
/// the reader takes a fresh one.
#[tokio::test]
async fn a_live_body_that_stalls_past_the_timeout_fails_and_reconnects() {
    let (url, server) = serve_live(
        2,
        "application/x-ndjson",
        vec![b"{\"id\":1,\"total\":10}\n".to_vec()],
        Duration::from_secs(1),
    );
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_millis(50),
        }),
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_millis(300);
    let mut source = HttpSource::new(&url, ndjson(), options).expect("source builds");

    let first = source.next_batch().await.expect("read").expect("one line");
    assert_eq!(column(&first, "id"), vec![1]);

    let Err(err) = source.next_batch().await else {
        panic!("the body went quiet for longer than the idle gap");
    };
    assert!(
        err.message()
            .starts_with(&format!("HttpSource: cannot read {url}")),
        "got: {err}"
    );

    let second = tokio::time::timeout(Duration::from_secs(5), source.next_batch())
        .await
        .expect("the reader asked again")
        .expect("read")
        .expect("the second connection served a line");
    assert_eq!(column(&second, "id"), vec![1]);

    drop(source);
    server.join().expect("server thread");
}

/// `Events` mode asks for `text/event-stream`, and a configured `accept` is
/// the author saying what this endpoint wants instead.
#[tokio::test]
async fn a_configured_accept_wins_over_the_event_stream_media_type() {
    let (url, server) = serve_live(
        1,
        "text/event-stream",
        vec![b"data: {\"id\":2,\"total\":20}\n\n".to_vec()],
        Duration::from_millis(10),
    );
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Events {
            event: None,
            window: Window {
                batch_size: 1_000,
                poll_timeout: Duration::from_millis(100),
                reconnect: Duration::from_secs(30),
            },
        },
        ..HttpSourceOptions::default()
    };
    options.client.headers = vec![("accept".to_string(), "application/custom".to_string())];
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, ndjson(), options).expect("source builds");

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the event decoded");
    assert_eq!(column(&batch, "id"), vec![2]);

    drop(source);
    let captured = server.join().expect("server thread");
    assert_eq!(captured[0].header("accept"), Some("application/custom"));
    assert_eq!(
        captured[0]
            .headers
            .iter()
            .filter(|(name, _)| name == "accept")
            .count(),
        1,
        "the mode's media type was not sent alongside it"
    );
}

/// With no `event` filter every event is wanted, named or not: an absent
/// filter is not a name to match.
#[tokio::test]
async fn an_event_stream_with_no_filter_decodes_every_event() {
    let body = concat!(
        "event: ping\ndata: {\"id\":1,\"total\":10}\n\n",
        "data: {\"id\":2,\"total\":20}\n\n",
    )
    .as_bytes()
    .to_vec();
    let (url, server) = serve_live(
        1,
        "text/event-stream",
        vec![body],
        Duration::from_millis(10),
    );
    let mut source = live_source(
        &url,
        ReadMode::Events {
            event: None,
            window: Window {
                batch_size: 1_000,
                poll_timeout: Duration::from_millis(100),
                reconnect: Duration::from_secs(30),
            },
        },
    );

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("both events");
    assert_eq!(
        column(&batch, "id"),
        vec![1, 2],
        "the named one and the unnamed one"
    );

    drop(source);
    server.join().expect("server thread");
}

/// Dropping a streaming source aborts its reader task, which closes the
/// connection under the server. The write loop there ends on that error, so
/// the thread is joinable long before it would have run out of chunks.
#[tokio::test]
async fn dropping_a_streaming_source_ends_the_reader_task() {
    let chunks: Vec<Vec<u8>> = (1..=200)
        .map(|id| format!("{{\"id\":{id},\"total\":{id}0}}\n").into_bytes())
        .collect();
    let (url, server) = serve_live(1, "application/x-ndjson", chunks, Duration::from_millis(50));
    let mut source = live_source(
        &url,
        ReadMode::Lines(Window {
            batch_size: 1,
            poll_timeout: Duration::from_secs(30),
            reconnect: Duration::from_secs(30),
        }),
    );

    let first = source.next_batch().await.expect("read").expect("one line");
    assert_eq!(column(&first, "id"), vec![1]);
    drop(source);

    // 200 chunks 50 ms apart is a ten-second body, so a join inside five
    // seconds is the closed connection ending it, not the chunks running out.
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || server.join().expect("server thread")),
    )
    .await
    .expect("the write loop ended when the reader task went away")
    .expect("join");
}

/// A message decoder is opened before the first byte arrives, so a streaming
/// mode with nothing to decode against is refused while the source is built.
#[test]
fn a_streaming_mode_without_a_declared_schema_is_a_configuration_error() {
    let Err(err) = HttpSource::new(
        "http://127.0.0.1:1/live",
        ndjson(),
        HttpSourceOptions {
            mode: ReadMode::Lines(Window::default()),
            ..HttpSourceOptions::default()
        },
    ) else {
        panic!("a decoder needs a schema");
    };
    assert_eq!(err.category(), "configuration");
    assert!(
        err.message().contains("needs a 'schema_fields' list"),
        "got: {err}"
    );
}

/// A live body's schema is the declared one: there is no document whose own
/// schema could be read instead.
#[test]
fn schema_from_body_with_a_streaming_mode_is_a_configuration_error() {
    let Err(err) = HttpSource::new(
        "http://127.0.0.1:1/live",
        ndjson(),
        HttpSourceOptions {
            declared: Some(schema()),
            schema_from: SchemaFrom::Body,
            mode: ReadMode::Lines(Window::default()),
            ..HttpSourceOptions::default()
        },
    ) else {
        panic!("a live body carries no document schema");
    };
    assert_eq!(
        err.message(),
        "HttpSource: schema_from \"body\" applies to mode kind=\"document\" only"
    );
}

/// A frame the decoder refuses does not poison the body: the frames after it
/// are read against a fresh decoder, so the stream keeps delivering rows.
///
/// The three rows before the bad frame are lost, and that is the ndjson
/// decoder's contract rather than this source's: `arrow-json`'s tape holds
/// the record it could not read, so the flush that would have handed those
/// rows over fails too. A format whose decoder can still flush keeps them.
#[tokio::test]
async fn a_refused_frame_does_not_poison_the_rest_of_a_line_stream() {
    let chunk = concat!(
        "{\"id\":1,\"total\":10}\n",
        "{\"id\":2,\"total\":20}\n",
        "{\"id\":3,\"total\":30}\n",
        "not json\n",
        "{\"id\":5,\"total\":50}\n",
        "{\"id\":6,\"total\":60}\n",
    )
    .as_bytes()
    .to_vec();
    // The gap holds the body open past the poll timeout, so the window after
    // the bad frame closes on its own rather than on the body ending.
    let (url, server) = serve_live(
        1,
        "application/x-ndjson",
        vec![chunk],
        Duration::from_millis(400),
    );
    let mut source = live_source(
        &url,
        ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_secs(30),
        }),
    );

    let Err(err) = source.next_batch().await else {
        panic!("'not json' is not a record");
    };
    assert!(err.message().starts_with("ndjson: decode"), "got: {err}");

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the frames after the bad one");
    assert_eq!(
        column(&batch, "id"),
        vec![5, 6],
        "the read carried on instead of failing frame after frame"
    );

    let Err(err) = source.next_batch().await else {
        panic!("the body ended");
    };
    assert!(err.message().contains("ended"), "got: {err}");

    drop(source);
    server.join().expect("server thread");
}

/// `max_body_bytes` bounds one frame, not the chunk that carried it: a
/// hundred short lines arriving together are all read under a cap far below
/// the chunk's own size.
#[tokio::test]
async fn a_frame_cap_measures_one_frame_not_the_chunk_that_carried_it() {
    let mut chunk = Vec::new();
    for id in 1..=100 {
        chunk.extend_from_slice(format!("{{\"id\":{id},\"total\":{id}0}}\n").as_bytes());
    }
    assert!(chunk.len() > 1_000, "the chunk dwarfs the 64-byte cap");
    let (url, server) = serve_live(
        1,
        "application/x-ndjson",
        vec![chunk],
        Duration::from_millis(400),
    );

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_secs(30),
        }),
        max_body_bytes: Some(64),
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, ndjson(), options).expect("source builds");

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("every line of the chunk");
    assert_eq!(batch.num_rows(), 100);
    assert_eq!(column(&batch, "id")[99], 100);

    drop(source);
    server.join().expect("server thread");
}

/// One frame over the cap ends that body: the frames before it are handed
/// over first, the refusal names the cap, and the reader reconnects.
#[tokio::test]
async fn a_frame_over_the_cap_ends_the_body_and_the_reader_reconnects() {
    let mut first = b"{\"id\":1,\"total\":10}\n{\"id\":2,\"total\":20}\n".to_vec();
    first.extend_from_slice(b"{\"id\":3,\"total\":30,\"pad\":\"");
    first.extend(std::iter::repeat_n(b'x', 200));
    first.extend_from_slice(b"\"}\n");
    let (url, server) = serve_live_each(
        "application/x-ndjson",
        vec![
            Live::ok(vec![first]),
            Live::ok(vec![b"{\"id\":9,\"total\":90}\n".to_vec()]),
        ],
        Duration::from_millis(400),
    );

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        mode: ReadMode::Lines(Window {
            batch_size: 1_000,
            poll_timeout: Duration::from_millis(100),
            reconnect: Duration::from_millis(50),
        }),
        max_body_bytes: Some(64),
        ..HttpSourceOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, ndjson(), options).expect("source builds");

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the frames before the long one");
    assert_eq!(column(&batch, "id"), vec![1, 2]);
    let Err(err) = source.next_batch().await else {
        panic!("a 200-byte line does not fit in 64");
    };
    assert!(
        err.message().contains("exceeds max_body_bytes (64)"),
        "got: {err}"
    );

    let second = tokio::time::timeout(Duration::from_secs(5), source.next_batch())
        .await
        .expect("the reader asked again")
        .expect("read")
        .expect("the second connection");
    assert_eq!(column(&second, "id"), vec![9]);

    drop(source);
    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 2, "the refusal cost one body, not the run");
}

/// A block carrying only `retry:` dispatches no event and still governs the
/// reconnect: the body ends straight after it, and the configured 50 ms is
/// not what the reader waits.
#[tokio::test]
async fn a_retry_block_with_no_data_still_governs_the_reconnect() {
    let (url, _server) = serve_live_each(
        "text/event-stream",
        vec![
            Live::ok(vec![b"retry: 60000\n\n".to_vec()]),
            // Only reached by a reader that ignored the stream's own retry.
            Live::ok(vec![b"data: {\"id\":1,\"total\":10}\n\n".to_vec()]),
        ],
        Duration::from_millis(10),
    );
    let mut source = live_source(
        &url,
        ReadMode::Events {
            event: None,
            window: Window {
                batch_size: 1_000,
                poll_timeout: Duration::from_millis(100),
                reconnect: Duration::from_millis(50),
            },
        },
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(1_500), source.next_batch())
            .await
            .is_err(),
        "a minute's retry was asked for, so the second connection is not made"
    );
    // The server thread is left waiting on a second connection that never
    // comes, and the test process ends with it: joining it would be the very
    // wait this asserts against.
    drop(source);
}

/// The `id:` a stream last sent is what the reconnect echoes, and an empty
/// `id:` clears it, so the next request carries no `last-event-id` at all.
#[tokio::test]
async fn an_empty_event_id_clears_the_one_the_reconnect_sends() {
    let (url, server) = serve_live_each(
        "text/event-stream",
        vec![
            Live::ok(vec![b"id: 7\ndata: {\"id\":7,\"total\":70}\n\n".to_vec()]),
            Live::ok(vec![b"id:\ndata: {\"id\":8,\"total\":80}\n\n".to_vec()]),
            Live::ok(vec![b"data: {\"id\":9,\"total\":90}\n\n".to_vec()]),
        ],
        Duration::from_millis(10),
    );
    let mut source = live_source(
        &url,
        ReadMode::Events {
            event: None,
            window: Window {
                batch_size: 1_000,
                poll_timeout: Duration::from_millis(100),
                reconnect: Duration::from_millis(50),
            },
        },
    );

    for id in [7, 8, 9] {
        let batch = tokio::time::timeout(Duration::from_secs(5), source.next_batch())
            .await
            .expect("the reader reconnected")
            .expect("read")
            .expect("one event");
        assert_eq!(column(&batch, "id"), vec![id]);
    }

    drop(source);
    let captured = server.join().expect("server thread");
    assert_eq!(captured[0].header("last-event-id"), None);
    assert_eq!(captured[1].header("last-event-id"), Some("7"));
    assert_eq!(
        captured[2].header("last-event-id"),
        None,
        "an empty id asks for none"
    );
}

// ── the sink half ───────────────────────────────────────────────────────────

#[tokio::test]
async fn each_batch_is_one_request_whose_body_decodes_back() {
    let (url, server) = serve(2, "204 No Content", Vec::new());
    let mut sink = sink(&url, "POST");

    sink.write_batch(&batch(&[1, 2])).await.expect("first");
    sink.write_batch(&batch(&[3])).await.expect("second");
    sink.finish().await.expect("finish");

    let captured = server.join().expect("server thread");
    assert_eq!(captured.len(), 2, "one request per batch, no accumulation");

    for request in &captured {
        assert_eq!(request.method, "POST");
    }

    let first = decode_body(&captured[0].body);
    let second = decode_body(&captured[1].body);
    assert_eq!(column(&first[0], "id"), vec![1, 2]);
    assert_eq!(column(&first[0], "total"), vec![10, 20]);
    assert_eq!(column(&second[0], "id"), vec![3]);

    // A fresh writer per request means each body carries its own csv header
    // row, so it is a whole document rather than a fragment of a stream.
    assert!(
        captured[1].body.starts_with(b"id,total\n"),
        "got: {:?}",
        String::from_utf8_lossy(&captured[1].body)
    );
}

#[tokio::test]
async fn the_configured_method_is_the_one_on_the_wire() {
    let (url, server) = serve(1, "200 OK", Vec::new());
    let mut sink = sink(&url, "PUT");
    sink.write_batch(&batch(&[7])).await.expect("write");

    let captured = server.join().expect("server thread");
    assert_eq!(captured[0].method, "PUT");
}

/// The format decides the media type, and a configured header wins over it.
#[tokio::test]
async fn the_content_type_follows_the_format_unless_a_header_names_one() {
    let (url, server) = serve(1, "200 OK", Vec::new());
    let mut sink = sink(&url, "POST");
    sink.write_batch(&batch(&[1])).await.expect("write");
    let captured = server.join().expect("server thread");
    assert_eq!(captured[0].header("content-type"), Some("text/csv"));

    let (url, server) = serve(1, "200 OK", Vec::new());
    let mut options = HttpSinkOptions::default();
    options.client.headers = vec![("content-type".to_string(), "text/plain".to_string())];
    let mut sink = HttpSink::new(&url, schema(), csv(), options).expect("sink builds");
    sink.write_batch(&batch(&[1])).await.expect("write");
    let captured = server.join().expect("server thread");
    assert_eq!(captured[0].header("content-type"), Some("text/plain"));
}

/// Each encoding compresses the whole document and declares itself, so a peer
/// decompresses once and then reads the same csv.
#[tokio::test]
async fn content_encoding_compresses_the_whole_document() {
    for (encoding, token) in [
        (ContentEncoding::Gzip, "gzip"),
        (ContentEncoding::Br, "br"),
        (ContentEncoding::Zstd, "zstd"),
    ] {
        let (url, server) = serve(1, "200 OK", Vec::new());
        let mut options = HttpSinkOptions {
            content_encoding: Some(encoding),
            ..HttpSinkOptions::default()
        };
        options.client.timeout = Duration::from_secs(5);
        let mut sink = HttpSink::new(&url, schema(), csv(), options).expect("sink builds");
        sink.write_batch(&batch(&[1, 2])).await.expect("write");

        let captured = server.join().expect("server thread");
        assert_eq!(captured[0].header("content-encoding"), Some(token));
        let decoded = match encoding {
            ContentEncoding::Gzip => {
                let mut out = Vec::new();
                flate2::read::GzDecoder::new(&captured[0].body[..])
                    .read_to_end(&mut out)
                    .expect("gunzip");
                out
            }
            ContentEncoding::Br => {
                let mut out = Vec::new();
                brotli::Decompressor::new(&captured[0].body[..], 4096)
                    .read_to_end(&mut out)
                    .expect("unbrotli");
                out
            }
            ContentEncoding::Zstd => zstd::decode_all(&captured[0].body[..]).expect("unzstd"),
        };
        assert_eq!(
            String::from_utf8_lossy(&decoded),
            "id,total\n1,10\n2,20\n",
            "{token} decompresses to the document the format wrote"
        );
    }
}

#[tokio::test]
async fn a_sink_non_2xx_status_names_the_status_and_the_url() {
    let (url, server) = serve(1, "500 Internal Server Error", b"boom".to_vec());
    let mut sink = sink(&url, "POST");

    let Err(err) = sink.write_batch(&batch(&[1])).await else {
        panic!("a 500 is not a successful write");
    };
    assert!(
        err.message().starts_with("HttpSink: status 500"),
        "got: {err}"
    );
    assert!(err.message().contains(&url), "got: {err}");
    server.join().expect("server thread");
}

#[tokio::test]
async fn a_sink_against_a_refused_connection_names_the_method_and_the_url() {
    let url = dead_url();
    let mut sink = sink(&url, "POST");

    let Err(err) = sink.write_batch(&batch(&[1])).await else {
        panic!("nothing is listening");
    };
    assert!(
        err.message()
            .starts_with(&format!("HttpSink: cannot POST {url}")),
        "got: {err}"
    );
}

/// Only a `307` and a `308` keep a POST and the body it carries, so every
/// other 3xx is handed back as the status it is: a `302` fails the write
/// naming that status, and the target it pointed at is never asked for
/// anything.
#[tokio::test]
async fn a_sink_reports_a_302_and_never_reaches_the_target_it_named() {
    let unreached = TcpListener::bind("127.0.0.1:0").expect("bind");
    let moved = format!("http://{}/moved", unreached.local_addr().expect("addr"));
    let (url, redirector) = serve_each(vec![Reply::status("302 Found").with("location", &moved)]);
    let mut sink = sink(&url, "POST");

    let Err(err) = sink.write_batch(&batch(&[1])).await else {
        panic!("a 302 on a POST is not a batch that arrived");
    };
    assert_eq!(
        err.message(),
        format!("HttpSink: status 302 Found from {url}")
    );

    let captured = redirector.join().expect("server thread");
    assert_eq!(captured[0].method, "POST");
    // Nothing was ever dialled, so there is no connection queued on the
    // listener the `location` named.
    unreached.set_nonblocking(true).expect("nonblocking");
    assert!(
        matches!(unreached.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
        "the target the 302 named was asked for something"
    );
}

/// Post one batch to a listener that answers `status` with a `location`
/// naming a second one, and hand back what each of the two captured.
///
/// The write is judged on the second listener's `204`: a sink judging the
/// first listener's 3xx instead would fail here rather than return.
async fn redirected_post(status: &str) -> (Vec<Captured>, Vec<Captured>) {
    let (moved, target) = serve(1, "204 No Content", Vec::new());
    let (url, redirector) = serve_each(vec![Reply::status(status).with("location", &moved)]);
    let mut sink = sink(&url, "POST");

    tokio::time::timeout(Duration::from_secs(10), sink.write_batch(&batch(&[1, 2])))
        .await
        .expect("the chain ends at the second listener")
        .expect("the status the second listener answered is what the sink judged");

    (
        redirector.join().expect("server thread"),
        target.join().expect("server thread"),
    )
}

/// A `307` is followed with the method and the body intact, so the batch
/// arrives whole at the target the first listener named.
#[tokio::test]
async fn a_sink_follows_a_307_with_the_post_and_its_body() {
    let (first, second) = redirected_post("307 Temporary Redirect").await;
    assert_eq!(second[0].method, "POST");
    assert_eq!(second[0].header("content-type"), Some("text/csv"));
    assert_eq!(
        second[0].body, first[0].body,
        "the document the first listener saw is the one the second got"
    );
    assert_eq!(column(&decode_body(&second[0].body)[0], "id"), vec![1, 2]);
}

/// A `308` is the permanent one of the pair and is followed the same way.
#[tokio::test]
async fn a_sink_follows_a_308_with_the_post_and_its_body() {
    let (first, second) = redirected_post("308 Permanent Redirect").await;
    assert_eq!(second[0].method, "POST");
    assert_eq!(second[0].header("content-type"), Some("text/csv"));
    assert_eq!(
        second[0].body, first[0].body,
        "the document the first listener saw is the one the second got"
    );
    assert_eq!(column(&decode_body(&second[0].body)[0], "id"), vec![1, 2]);
}

/// The chain is bounded: a listener answering `307` with its own url on every
/// request fails the write instead of being followed forever.
#[tokio::test]
async fn an_endless_chain_of_307s_fails_the_write_rather_than_looping() {
    // Twelve answers is more than the ten hops the policy allows, so what
    // ends the chain is the limit rather than the server running dry.
    let answer = Reply::status("307 Temporary Redirect").with("location", "/data");
    let (url, _redirector) = serve_each(vec![answer; 12]);
    let mut sink = sink(&url, "POST");

    let outcome = tokio::time::timeout(Duration::from_secs(10), sink.write_batch(&batch(&[1])))
        .await
        .expect("the hop limit ends the write, not this bound");
    let Err(err) = outcome else {
        panic!("a chain of 307s never arrives anywhere");
    };
    assert!(
        err.message()
            .starts_with(&format!("HttpSink: cannot POST {url}")),
        "got: {err}"
    );
    assert!(
        err.message().contains("redirect"),
        "the chain was refused as a redirect chain, not as a dead peer: {err}"
    );
    // The server thread is left on the accept the limit stopped the client
    // from making, and the test process ends with it: joining it would be a
    // wait for the very request this asserts was not sent.
}

/// The whole document is compressed before the request is built, so the
/// length the peer is told is the compressed one. What the server captures is
/// delimited by that length, so gunzipping it back to the plain request's csv
/// is what shows the number covered the whole compressed body.
#[tokio::test]
async fn the_announced_length_covers_the_whole_compressed_document() {
    let rows: Vec<i64> = (1..=500).collect();

    let (url, server) = serve(1, "204 No Content", Vec::new());
    let mut plain = sink(&url, "POST");
    plain.write_batch(&batch(&rows)).await.expect("write");
    let document = server.join().expect("server thread").remove(0).body;

    let (url, server) = serve(1, "204 No Content", Vec::new());
    let mut options = HttpSinkOptions {
        content_encoding: Some(ContentEncoding::Gzip),
        ..HttpSinkOptions::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut sink = HttpSink::new(&url, schema(), csv(), options).expect("sink builds");
    sink.write_batch(&batch(&rows)).await.expect("write");

    let compressed = server.join().expect("server thread").remove(0).body;
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(&compressed[..])
        .read_to_end(&mut decoded)
        .expect("gunzip");
    assert_eq!(
        decoded, document,
        "the announced length carried the whole compressed document"
    );
    assert!(
        compressed.len() < document.len(),
        "{} compressed bytes against {} plain ones",
        compressed.len(),
        document.len()
    );
}

/// The two halves are wire compatible: what the sink sends, the source reads.
#[tokio::test]
async fn what_the_sink_wrote_is_what_the_source_reads_back() {
    let (sink_url, sink_server) = serve(1, "200 OK", Vec::new());
    let mut sink = sink(&sink_url, "POST");
    sink.write_batch(&batch(&[4, 5])).await.expect("write");
    let captured = sink_server.join().expect("server thread");

    // The captured body is served back verbatim to a source.
    let (source_url, source_server) = serve(1, "200 OK", captured[0].body.clone());
    let mut source = source(&source_url, Vec::new());

    let read_back = source
        .next_batch()
        .await
        .expect("read")
        .expect("one batch")
        .clone();
    source_server.join().expect("server thread");

    assert_eq!(column(&read_back, "id"), vec![4, 5]);
    assert_eq!(column(&read_back, "total"), vec![40, 50]);
}

// ── the push source ─────────────────────────────────────────────────────────

/// A push source on a free port, with `options` applied on top of the csv
/// defaults every test here wants.
fn push(options: HttpPushOptions) -> (HttpPushSource, String) {
    let path = options.path.clone();
    let source = HttpPushSource::new("127.0.0.1:0", csv(), options).expect("listener binds");
    let url = format!("http://{}{path}", source.local_addr());
    (source, url)
}

fn push_options() -> HttpPushOptions {
    HttpPushOptions {
        declared: schema(),
        path: "/ingest".to_string(),
        ..HttpPushOptions::default()
    }
}

/// The test client: one plain reqwest client, no TLS, no pooling assumptions.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client")
}

/// Read one hand-written request's status line off `stream`.
///
/// The status line only: hyper may keep the connection alive after a
/// refusal, so reading to EOF would wait for its idle timeout.
async fn status_line(stream: &mut tokio::net::TcpStream) -> String {
    let mut status = Vec::new();
    let mut byte = [0u8; 1];
    while !status.ends_with(b"\r\n") {
        let read = tokio::io::AsyncReadExt::read(stream, &mut byte)
            .await
            .expect("read answer");
        if read == 0 {
            break;
        }
        status.push(byte[0]);
    }
    String::from_utf8_lossy(&status).into_owned()
}

#[tokio::test]
async fn a_posted_document_becomes_batches_and_answers_202() {
    let (mut source, url) = push(push_options());
    let post = tokio::spawn({
        let url = url.clone();
        async move {
            client()
                .post(&url)
                .body(CSV_BODY.to_vec())
                .send()
                .await
                .expect("post")
        }
    });

    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the document held rows");
    assert_eq!(column(&batch, "id"), vec![1, 2, 3]);

    let response = post.await.expect("join");
    assert_eq!(response.status(), 202);
    assert!(
        response.headers().get("alt-svc").is_none(),
        "no quic_bind, so nothing to advertise"
    );
}

/// Every refusal, one request each, all against one listener.
#[tokio::test]
async fn every_refusal_has_its_own_status() {
    let (mut source, url) = push(HttpPushOptions {
        methods: vec!["POST".to_string(), "PUT".to_string()],
        bearer: Some("s3cret".to_string()),
        ..push_options()
    });
    // Nothing is read off the channel until the last request, so the listener
    // has to be spawned; `ensure_listening` runs on the first `next_batch`.
    let listening = tokio::spawn(async move {
        let outcome = source.next_batch().await;
        (source, outcome)
    });
    // Give the accept loop a moment to exist before the first request.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = client();
    let other_path = url.replace("/ingest", "/elsewhere");
    let response = client
        .post(&other_path)
        .bearer_auth("s3cret")
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 404);
    assert!(
        response
            .text()
            .await
            .expect("body")
            .contains("no such path")
    );

    let response = client
        .get(&url)
        .bearer_auth("s3cret")
        .send()
        .await
        .expect("get");
    assert_eq!(response.status(), 405);
    assert_eq!(
        response.headers().get("allow").expect("allow"),
        "POST, PUT",
        "a 405 names what is accepted"
    );

    let response = client
        .post(&url)
        .body(CSV_BODY.to_vec())
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 401, "no token at all");

    let response = client
        .post(&url)
        .bearer_auth("s3cre7")
        .body(CSV_BODY.to_vec())
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 401, "a token of the right length, wrong");
    // The cap is its own listener, below: a 16-byte cap would refuse the
    // malformed document that follows for its size instead of its content.

    // A malformed document is a `400` whose body is the format's own reason,
    // and the same error reaches the source.
    let response = client
        .post(&url)
        .bearer_auth("s3cret")
        .body(b"id,total\nnot-a-number,10\n".to_vec())
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 400);
    assert!(!response.text().await.expect("body").is_empty());

    let (_source, batch) = listening.await.expect("join");
    let Err(err) = batch else {
        panic!("the refused document reached the source as an error");
    };
    assert!(!err.message().is_empty(), "got: {err}");
}

/// A body over the cap is refused, whether it announced its size or not. The
/// announced case is one `content-length` check; the chunked case is
/// `Limited` stopping the read rather than buffering the rest.
#[tokio::test]
async fn a_body_over_the_cap_is_refused_announced_or_not() {
    let (mut source, url) = push(HttpPushOptions {
        max_body_bytes: 16,
        ..push_options()
    });
    let address = source.local_addr();
    let listening = tokio::spawn(async move { source.next_batch().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = client()
        .post(&url)
        .body(CSV_BODY.to_vec())
        .send()
        .await
        .expect("post");
    assert_eq!(
        response.status(),
        413,
        "24 announced bytes do not fit in 16"
    );
    assert!(
        response
            .text()
            .await
            .expect("body")
            .contains("exceeds max_body_bytes (16)")
    );

    // Written by hand rather than through reqwest: a chunked request needs a
    // body with no length, which reqwest only builds from a stream, and the
    // point here is that a cap with nothing to check up front still holds.
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    let request = concat!(
        "POST /ingest HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\n\r\n",
        "e\r\nid,total\n1,10\n\r\nf\r\n2,20\n3,30\n4,40\n\r\n0\r\n\r\n",
    );
    tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
        .await
        .expect("write request");
    let status = status_line(&mut stream).await;
    assert!(
        status.starts_with("HTTP/1.1 413"),
        "29 chunked bytes do not fit in 16, got: {status}"
    );

    listening.abort();
}

/// A document that decodes to no rows is still accepted: an empty batch of
/// work is not an error.
#[tokio::test]
async fn an_empty_document_is_accepted_and_queues_nothing() {
    let (mut source, url) = push(push_options());
    let listening = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(400), source.next_batch()).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = client()
        .post(&url)
        .body(b"id,total\n".to_vec())
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 202);

    assert!(
        listening.await.expect("join").is_err(),
        "a header-only csv queues nothing, so the source waits"
    );
}

/// The channel is the backpressure: with room for one batch and nothing
/// reading, a third request cannot complete until the source drains.
#[tokio::test]
async fn a_full_channel_holds_the_client_until_the_source_reads() {
    let (mut source, url) = push(HttpPushOptions {
        buffer: 1,
        ..push_options()
    });
    // Bind the accept loop without draining: one `next_batch` starts it and
    // takes the first batch, after which nothing reads.
    let first = source.next_batch();
    let posted = Arc::new(AtomicUsize::new(0));
    let sender = tokio::spawn({
        let url = url.clone();
        let posted = Arc::clone(&posted);
        async move {
            let client = client();
            for _ in 0..3 {
                let response = client
                    .post(&url)
                    .body(CSV_BODY.to_vec())
                    .send()
                    .await
                    .expect("post");
                assert_eq!(response.status(), 202);
                posted.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    first.await.expect("read").expect("the first batch");
    // One in flight plus one queued is all the listener can hold, so the
    // third request is still waiting.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        posted.load(Ordering::SeqCst) < 3,
        "the third POST completed with nothing reading"
    );

    // Three documents, one batch each: draining the other two lets the last
    // request answer. The count is exact, so nothing waits on a batch that
    // was never going to arrive.
    source
        .next_batch()
        .await
        .expect("read")
        .expect("the second batch");
    source
        .next_batch()
        .await
        .expect("read")
        .expect("the third batch");
    sender.await.expect("join");
    assert_eq!(posted.load(Ordering::SeqCst), 3);
}

/// What the sink sends is what the push source accepts, with no configuration
/// between them beyond naming the same format.
#[tokio::test]
async fn what_the_sink_posts_the_push_source_reads() {
    let (mut source, url) = push(push_options());
    let mut sink = sink(&url, "POST");

    let writer = tokio::spawn(async move {
        sink.write_batch(&batch(&[4, 5])).await.expect("write");
        sink.finish().await.expect("finish");
    });

    let read_back = source.next_batch().await.expect("read").expect("one batch");
    assert_eq!(column(&read_back, "id"), vec![4, 5]);
    assert_eq!(column(&read_back, "total"), vec![40, 50]);
    writer.await.expect("join");
}

/// A token of the wrong length is refused like any other wrong one, whether
/// it stops short of the real one or carries extra on the end.
#[tokio::test]
async fn a_bearer_token_of_the_wrong_length_is_refused() {
    let (mut source, url) = push(HttpPushOptions {
        bearer: Some("s3cret".to_string()),
        ..push_options()
    });
    let listening = tokio::spawn(async move { source.next_batch().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = client();
    for token in ["s3cre", "s3cretx"] {
        let response = client
            .post(&url)
            .bearer_auth(token)
            .body(CSV_BODY.to_vec())
            .send()
            .await
            .expect("post");
        assert_eq!(response.status(), 401, "'{token}' is not the token");
    }

    listening.abort();
}

/// A refusal's body is the reason as text, so a client can print it. An
/// accepted document has no body and says nothing about one.
#[tokio::test]
async fn a_refusal_declares_its_media_type_and_the_202_carries_none() {
    let (mut source, url) = push(push_options());
    let listening = tokio::spawn(async move {
        let outcome = source.next_batch().await;
        (source, outcome)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = client();
    let response = client
        .post(url.replace("/ingest", "/elsewhere"))
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 404);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("content-type"),
        "text/plain; charset=utf-8"
    );

    let response = client
        .post(&url)
        .body(CSV_BODY.to_vec())
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 202);
    assert_eq!(response.headers().get("content-type"), None);
    assert!(response.text().await.expect("body").is_empty());

    let (_source, outcome) = listening.await.expect("join");
    let batch = outcome.expect("read").expect("the document held rows");
    assert_eq!(column(&batch, "id"), vec![1, 2, 3]);
}

/// `schema_from "body"` on a listener judges each document by the schema it
/// turned out to carry: one that differs is a `400` naming it, and the same
/// error reaches the source.
#[tokio::test]
async fn a_pushed_body_is_judged_by_the_schema_it_carries() {
    let mut source = HttpPushSource::new(
        "127.0.0.1:0",
        parquet(),
        HttpPushOptions {
            schema_from: SchemaFrom::Body,
            ..push_options()
        },
    )
    .expect("listener binds");
    let url = format!("http://{}/ingest", source.local_addr());

    let wide = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
        Field::new("extra", DataType::Int64, false),
    ]));
    let wrong = parquet_body(
        Arc::clone(&wide),
        &RecordBatch::try_new(
            wide,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .expect("batch"),
    );

    let listening = tokio::spawn(async move {
        let outcome = source.next_batch().await;
        (source, outcome)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = client().post(&url).body(wrong).send().await.expect("post");
    assert_eq!(response.status(), 400);
    let refusal = response.text().await.expect("body");
    assert!(refusal.contains("carries schema"), "got: {refusal}");

    let (mut source, outcome) = listening.await.expect("join");
    let Err(err) = outcome else {
        panic!("the refused document reached the source as an error");
    };
    assert_eq!(err.category(), "configuration", "got: {err}");

    let right = parquet_body(schema(), &batch(&[1, 2, 3]));
    let posting = tokio::spawn({
        let url = url.clone();
        async move { client().post(&url).body(right).send().await.expect("post") }
    });
    let read_back = source.next_batch().await.expect("read").expect("one batch");
    assert_eq!(column(&read_back, "id"), vec![1, 2, 3]);
    assert_eq!(posting.await.expect("join").status(), 202);
}

/// Rebuilding a connector in place needs its address back, so a dropped push
/// source leaves nothing holding the port it bound.
#[tokio::test]
async fn a_dropped_push_source_frees_its_address_for_the_next_one() {
    let (mut source, url) = push(push_options());
    let address = source.local_addr();
    let posting = tokio::spawn(async move {
        let client = client();
        let status = client
            .post(&url)
            .body(CSV_BODY.to_vec())
            .send()
            .await
            .expect("post")
            .status();
        // Closing from this side keeps the listener's port out of TIME_WAIT,
        // which is the client's doing rather than the source's.
        drop(client);
        status
    });

    let read_back = source.next_batch().await.expect("read").expect("one batch");
    assert_eq!(column(&read_back, "id"), vec![1, 2, 3]);
    assert_eq!(posting.await.expect("join"), 202);
    drop(source);

    // The accept loop is aborted rather than joined, so it lets go of the
    // listener on one of the next turns of the runtime.
    let rebuilt = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match HttpPushSource::new(&address.to_string(), csv(), push_options()) {
                Ok(source) => return source,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("the address the dropped source held is bindable again");
    assert_eq!(rebuilt.local_addr(), address);
}

/// A document is queued whole or not at all, which is what lets a client
/// retry a request it never got an answer to.
///
/// Two things here are load-bearing. The first document is answered `202`
/// with nothing reading and room for one message, so a listener that queued
/// one batch per message would block on its second batch and never answer
/// at all. And the two documents never interleave in what the source reads.
/// The second request gives up while its send waits on the full channel;
/// whether its document still lands is hyper's business, since it polls the
/// abandoned work to completion on HTTP/1.1 and on h2 alike, and either way
/// nothing partial is queued and the retry adds one more whole copy.
#[tokio::test]
async fn a_document_whose_client_gave_up_is_queued_whole_or_not_at_all() {
    let mut source = HttpPushSource::new(
        "127.0.0.1:0",
        ndjson(),
        HttpPushOptions {
            buffer: 1,
            ..push_options()
        },
    )
    .expect("listener binds");
    let url = format!("http://{}/ingest", source.local_addr());

    // arrow-json reads 1024 rows per batch, so 3000 lines is one document of
    // three batches: what is atomic here is all three, not one send.
    let document = |first: i64| {
        let mut body = Vec::new();
        for id in first..first + 3_000 {
            body.extend_from_slice(format!("{{\"id\":{id},\"total\":{id}}}\n").as_bytes());
        }
        body
    };
    let rows_of = |batches: &[RecordBatch], first: i64| {
        batches
            .iter()
            .flat_map(|batch| column(batch, "id"))
            .filter(|id| (first..first + 3_000).contains(id))
            .count()
    };
    let drain_queued = async |source: &mut HttpPushSource| {
        let mut batches = Vec::new();
        while let Ok(batch) =
            tokio::time::timeout(Duration::from_millis(500), source.next_batch()).await
        {
            batches.push(batch.expect("read").expect("a batch"));
        }
        batches
    };

    // Starts the accept loop with nothing to read, so the channel's one slot
    // is still free when the first document arrives.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), source.next_batch())
            .await
            .is_err(),
        "nothing has been posted yet"
    );

    let response = client()
        .post(&url)
        .body(document(0))
        .send()
        .await
        .expect("post");
    assert_eq!(
        response.status(),
        202,
        "the free slot took the whole document"
    );

    let impatient = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .expect("client");
    assert!(
        impatient
            .post(&url)
            .body(document(10_000))
            .send()
            .await
            .is_err(),
        "the channel is full, so this request cannot be answered in time"
    );

    let first_pass = drain_queued(&mut source).await;
    assert_eq!(rows_of(&first_pass, 0), 3_000, "the first document, whole");
    assert_eq!(
        first_pass
            .iter()
            .filter(|batch| column(batch, "id")[0] < 3_000)
            .count(),
        3,
        "one document, three batches"
    );
    let mut runs: Vec<bool> = first_pass
        .iter()
        .map(|batch| column(batch, "id")[0] < 3_000)
        .collect();
    runs.dedup();
    assert!(
        runs.len() <= 2,
        "the documents interleaved instead of arriving one at a time: {runs:?}"
    );
    let abandoned = rows_of(&first_pass, 10_000);
    assert!(
        abandoned == 0 || abandoned == 3_000,
        "a prefix of the second document was queued: {abandoned} rows"
    );

    let response = client()
        .post(&url)
        .body(document(10_000))
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 202);
    let second_pass = drain_queued(&mut source).await;
    assert_eq!(
        rows_of(&second_pass, 10_000),
        3_000,
        "the retry queued one more whole copy"
    );
    assert_eq!(
        rows_of(&second_pass, 0),
        0,
        "nothing of the first document arrived twice"
    );
}

/// An announced length over the cap is refused before the body is sent: this
/// request promises a hundred bytes and sends none, and the answer arrives
/// anyway.
#[tokio::test]
async fn a_push_body_over_the_cap_is_refused_before_it_is_sent() {
    let (mut source, _url) = push(HttpPushOptions {
        max_body_bytes: 16,
        ..push_options()
    });
    let address = source.local_addr();
    let listening = tokio::spawn(async move { source.next_batch().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        b"POST /ingest HTTP/1.1\r\nhost: localhost\r\ncontent-length: 100\r\n\r\n",
    )
    .await
    .expect("write head");

    let status = tokio::time::timeout(Duration::from_secs(1), status_line(&mut stream))
        .await
        .expect("the refusal does not wait for a body");
    assert!(status.starts_with("HTTP/1.1 413"), "got: {status}");

    listening.abort();
}

/// A chunked body whose connection dies mid-frame queues nothing, and the
/// listener answers the next good request as though nothing had happened.
#[tokio::test]
async fn a_body_whose_connection_dies_mid_chunk_queues_nothing() {
    let (mut source, url) = push(push_options());
    let address = source.local_addr();
    let listening = tokio::spawn(async move {
        let outcome = tokio::time::timeout(Duration::from_millis(500), source.next_batch()).await;
        (source, outcome)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        b"POST /ingest HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\n\r\n\
          e\r\nid,total\n1,10\n\r\n",
    )
    .await
    .expect("write the first chunk");
    drop(stream);

    let (mut source, outcome) = listening.await.expect("join");
    assert!(outcome.is_err(), "half a document is not a batch");

    let posted = tokio::spawn({
        let url = url.clone();
        async move {
            client()
                .post(&url)
                .body(CSV_BODY.to_vec())
                .send()
                .await
                .expect("post")
        }
    });
    let batch = source
        .next_batch()
        .await
        .expect("read")
        .expect("the good document");
    assert_eq!(column(&batch, "id"), vec![1, 2, 3]);
    assert_eq!(posted.await.expect("join").status(), 202);
}

// ── HTTP/2 ──────────────────────────────────────────────────────────────────

/// What one [`serve_versioned`] listener saw: the HTTP version of every
/// request in arrival order, and how many TCP connections carried them.
#[derive(Default)]
struct Seen {
    versions: std::sync::Mutex<Vec<http::Version>>,
    connections: AtomicUsize,
}

impl Seen {
    fn versions(&self) -> Vec<http::Version> {
        self.versions.lock().expect("lock").clone()
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

/// A `hyper_util` auto server that records the HTTP version of every request
/// and answers each with `body`.
///
/// This is what can answer HTTP/2: the hand-rolled server above speaks only
/// HTTP/1.1.
fn serve_versioned(body: Vec<u8>) -> (String, Arc<Seen>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/data", listener.local_addr().expect("addr"));
    let seen = Arc::new(Seen::default());
    let recorder = Arc::clone(&seen);
    tokio::spawn(async move {
        listener.set_nonblocking(true).expect("nonblocking");
        let listener = tokio::net::TcpListener::from_std(listener).expect("adopt");
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            recorder.connections.fetch_add(1, Ordering::SeqCst);
            let body = body.clone();
            let recorder = Arc::clone(&recorder);
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |request: http::Request<hyper::body::Incoming>| {
                        let body = body.clone();
                        let recorder = Arc::clone(&recorder);
                        async move {
                            recorder
                                .versions
                                .lock()
                                .expect("lock")
                                .push(request.version());
                            // The body is read so the peer's write completes.
                            let _ = http_body_util::BodyExt::collect(request.into_body()).await;
                            Ok::<_, std::convert::Infallible>(http::Response::new(
                                http_body_util::Full::new(Bytes::from(body)),
                            ))
                        }
                    },
                );
                let _ = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                .await;
            });
        }
    });
    (url, seen)
}

/// `http_version "2"` is prior knowledge, so the request arrives as HTTP/2 on
/// a cleartext url with no upgrade dance.
#[tokio::test]
async fn a_source_speaking_http2_arrives_as_http2() {
    let (url, seen) = serve_versioned(CSV_BODY.to_vec());
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        ..HttpSourceOptions::default()
    };
    options.client.http_version = HttpVersion::Http2;
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let batches = drain(&mut source).await;
    assert_eq!(column(&batches[0], "id"), vec![1, 2, 3]);
    assert_eq!(seen.versions(), [http::Version::HTTP_2]);
}

/// The default `auto` sends HTTP/1.1 on a cleartext url, which is what keeps
/// the hand-rolled server above a valid peer.
#[tokio::test]
async fn an_auto_source_arrives_as_http11_on_cleartext() {
    let (url, seen) = serve_versioned(CSV_BODY.to_vec());
    let mut source = source(&url, Vec::new());
    drain(&mut source).await;
    assert_eq!(seen.versions(), [http::Version::HTTP_11]);
}

/// An HTTP/2 sink posting to a push source: h2c end to end, both halves of
/// this crate, no TLS.
#[tokio::test]
async fn an_http2_sink_posts_to_the_push_source() {
    let (mut source, url) = push(push_options());
    let mut options = HttpSinkOptions::default();
    options.client.http_version = HttpVersion::Http2;
    options.client.timeout = Duration::from_secs(5);
    let mut sink = HttpSink::new(&url, schema(), csv(), options).expect("sink builds");

    let writer = tokio::spawn(async move {
        sink.write_batch(&batch(&[8, 9])).await.expect("write");
    });
    let read_back = source.next_batch().await.expect("read").expect("one batch");
    assert_eq!(column(&read_back, "id"), vec![8, 9]);
    writer.await.expect("join");
}

/// One HTTP/2 connection carries every batch: three writes are three requests
/// multiplexed on the connection the client keeps, not three dials.
#[tokio::test]
async fn an_http2_sink_sends_every_batch_on_one_connection() {
    let (url, seen) = serve_versioned(Vec::new());
    let mut options = HttpSinkOptions::default();
    options.client.http_version = HttpVersion::Http2;
    options.client.timeout = Duration::from_secs(5);
    let mut sink = HttpSink::new(&url, schema(), csv(), options).expect("sink builds");

    for id in 1..=3 {
        sink.write_batch(&batch(&[id])).await.expect("write");
    }

    assert_eq!(seen.versions(), [http::Version::HTTP_2; 3]);
    assert_eq!(seen.connections(), 1, "one connection carried all three");
}

// ── TLS on TCP ──────────────────────────────────────────────────────────────

/// A TLS push source and a sink that trusts its certificate through
/// `tls.ca_file`, and the same sink without it, which the platform verifier
/// refuses.
#[tokio::test]
async fn tls_needs_the_ca_file_to_trust_a_private_certificate() {
    let certificate = certificate();
    let mut source = HttpPushSource::new(
        "127.0.0.1:0",
        csv(),
        HttpPushOptions {
            tls: Some(ServerTls {
                cert_file: certificate.cert.clone(),
                key_file: certificate.key.clone(),
            }),
            ..push_options()
        },
    )
    .expect("listener binds");
    // The certificate names `127.0.0.1` as well as `localhost`, so the
    // resolved address is a name the handshake accepts.
    let url = format!("https://{}/ingest", source.local_addr());

    let mut trusting = HttpSinkOptions::default();
    trusting.client.tls = TlsFiles {
        ca_file: Some(certificate.cert.clone()),
        ..TlsFiles::default()
    };
    trusting.client.timeout = Duration::from_secs(10);
    let mut sink = HttpSink::new(&url, schema(), csv(), trusting).expect("sink builds");
    let writer = tokio::spawn(async move {
        sink.write_batch(&batch(&[1, 2])).await.expect("write");
    });
    let read_back = source.next_batch().await.expect("read").expect("one batch");
    assert_eq!(column(&read_back, "id"), vec![1, 2]);
    writer.await.expect("join");

    let mut untrusting = HttpSinkOptions::default();
    untrusting.client.timeout = Duration::from_secs(10);
    let mut sink = HttpSink::new(&url, schema(), csv(), untrusting).expect("sink builds");
    let Err(err) = sink.write_batch(&batch(&[3])).await else {
        panic!("the platform store does not know this certificate");
    };
    assert!(
        err.message().starts_with("HttpSink: cannot POST"),
        "got: {err}"
    );
}

/// A client identity is loaded and presented; the listener here does not ask
/// for one, so what this pins is that the pair reaches reqwest intact.
#[tokio::test]
async fn a_client_identity_is_accepted_at_build_and_used() {
    let certificate = certificate();
    let mut source = HttpPushSource::new(
        "127.0.0.1:0",
        csv(),
        HttpPushOptions {
            tls: Some(ServerTls {
                cert_file: certificate.cert.clone(),
                key_file: certificate.key.clone(),
            }),
            ..push_options()
        },
    )
    .expect("listener binds");
    let url = format!("https://{}/ingest", source.local_addr());

    let mut options = HttpSinkOptions::default();
    options.client.tls = TlsFiles {
        ca_file: Some(certificate.cert.clone()),
        cert_file: Some(certificate.cert.clone()),
        key_file: Some(certificate.key.clone()),
    };
    options.client.timeout = Duration::from_secs(10);
    let mut sink =
        HttpSink::new(&url, schema(), csv(), options).expect("the identity is a usable pair");

    let writer = tokio::spawn(async move {
        sink.write_batch(&batch(&[5])).await.expect("write");
    });
    let read_back = source.next_batch().await.expect("read").expect("one batch");
    assert_eq!(column(&read_back, "id"), vec![5]);
    writer.await.expect("join");
}

/// The commonest way to fill the block in wrong: both keys pointing at the
/// certificate. Refused while the sink is built, before any connection.
#[test]
fn a_key_file_that_is_a_certificate_is_refused_at_build() {
    let certificate = certificate();
    let mut options = HttpSinkOptions::default();
    options.client.tls = TlsFiles {
        cert_file: Some(certificate.cert.clone()),
        key_file: Some(certificate.cert.clone()),
        ..TlsFiles::default()
    };

    let Err(err) = HttpSink::new("https://example.invalid/", schema(), csv(), options) else {
        panic!("a certificate is not a private key");
    };
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("holds no private key"), "got: {err}");
}

// ── HTTP/3 ──────────────────────────────────────────────────────────────────

/// An `h3` server on a QUIC endpoint that answers every request with `status`
/// and `body`.
///
/// Returns the url to aim a client at and the endpoint it runs on. Closing
/// that endpoint ends every connection on it and the accept loop with them.
/// The url names `127.0.0.1`, a SAN of the certificate, rather than
/// `localhost`: that name resolves to `::1` first, and the HTTP/3 client
/// splits its timeout across the resolved addresses, so every read would
/// spend half its budget on an address this endpoint never bound.
fn serve_h3(certificate: &Certificate, status: u16, body: Vec<u8>) -> (String, quinn::Endpoint) {
    let cert = std::fs::read(&certificate.cert).expect("read cert");
    let key = std::fs::read(&certificate.key).expect("read key");
    let chain: Vec<rustls_pki_types::CertificateDer<'static>> =
        rustls_pki_types::pem::PemObject::pem_slice_iter(&cert[..])
            .collect::<Result<_, _>>()
            .expect("cert pem");
    let signing: rustls_pki_types::PrivateKeyDer<'static> =
        rustls_pki_types::pem::PemObject::from_pem_slice(&key[..]).expect("key pem");

    let mut config = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .expect("versions")
    .with_no_client_auth()
    .with_single_cert(chain, signing)
    .expect("pair");
    config.alpn_protocols = vec![b"h3".to_vec()];

    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(config))
        .expect("quic server config");
    let endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(crypto)),
        "127.0.0.1:0".parse().expect("addr"),
    )
    .expect("endpoint");
    let port = endpoint.local_addr().expect("addr").port();

    let accepting = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accepting.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let Ok(connection) = incoming.await else {
                    return;
                };
                let Ok(mut h3) =
                    h3::server::Connection::new(h3_quinn::Connection::new(connection)).await
                else {
                    return;
                };
                while let Ok(Some(resolver)) = h3.accept().await {
                    let body = body.clone();
                    tokio::spawn(async move {
                        let Ok((_request, mut stream)) = resolver.resolve_request().await else {
                            return;
                        };
                        // Drain the request body so a POST completes.
                        while let Ok(Some(_)) = stream.recv_data().await {}
                        let head = http::Response::builder()
                            .status(status)
                            .body(())
                            .expect("head");
                        if stream.send_response(head).await.is_err() {
                            return;
                        }
                        if !body.is_empty() && stream.send_data(Bytes::from(body)).await.is_err() {
                            return;
                        }
                        let _ = stream.finish().await;
                    });
                }
            });
        }
    });
    (format!("https://127.0.0.1:{port}/data"), endpoint)
}

/// The HTTP/3 client reads a document from an `h3` server: only HTTP/3 can
/// reach a QUIC port at all, so arriving there is the version proof.
#[tokio::test]
async fn a_source_reads_a_document_over_http3() {
    let certificate = certificate();
    let (url, server) = serve_h3(&certificate, 200, CSV_BODY.to_vec());

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        ..HttpSourceOptions::default()
    };
    options.client.http_version = HttpVersion::Http3;
    options.client.tls = TlsFiles {
        ca_file: Some(certificate.cert.clone()),
        ..TlsFiles::default()
    };
    options.client.timeout = Duration::from_secs(10);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let batches = drain(&mut source).await;
    assert_eq!(column(&batches[0], "id"), vec![1, 2, 3]);
    assert_eq!(column(&batches[0], "total"), vec![10, 20, 30]);
    server.close(0u32.into(), b"done");
}

/// A non-2xx over HTTP/3 reads the same as one over TCP: the status and the
/// url it came from, not a body to decode.
#[tokio::test]
async fn a_non_2xx_over_http3_names_the_status_and_the_url() {
    let certificate = certificate();
    let (url, server) = serve_h3(&certificate, 500, Vec::new());

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        ..HttpSourceOptions::default()
    };
    options.client.http_version = HttpVersion::Http3;
    options.client.tls = TlsFiles {
        ca_file: Some(certificate.cert.clone()),
        ..TlsFiles::default()
    };
    options.client.timeout = Duration::from_secs(10);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");

    let Err(err) = source.next_batch().await else {
        panic!("a 500 is not a body to decode");
    };
    assert_eq!(
        err.message(),
        format!("HttpSource: status 500 Internal Server Error from {url}")
    );
    server.close(0u32.into(), b"done");
}

/// The HTTP/3 client keeps one connection for the next request. A peer that
/// has gone away in the meantime is an error on that request rather than a
/// read that never returns.
#[tokio::test]
async fn a_read_after_the_http3_peer_is_gone_fails_rather_than_hanging() {
    let certificate = certificate();
    let (url, server) = serve_h3(&certificate, 200, CSV_BODY.to_vec());

    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        ..HttpSourceOptions::default()
    };
    options.client.http_version = HttpVersion::Http3;
    options.client.tls = TlsFiles {
        ca_file: Some(certificate.cert.clone()),
        ..TlsFiles::default()
    };
    options.client.timeout = Duration::from_secs(5);
    let mut source = HttpSource::new(&url, csv(), options).expect("source builds");
    assert_eq!(drain(&mut source).await.len(), 1, "the first read");

    server.close(0u32.into(), b"gone");

    let outcome = tokio::time::timeout(Duration::from_secs(15), source.next_batch())
        .await
        .expect("the read gives up inside its own timeout");
    let Err(err) = outcome else {
        panic!("the peer is gone, so there is nothing to read");
    };
    assert!(
        err.message()
            .starts_with(&format!("HttpSource: cannot GET {url}")),
        "got: {err}"
    );
}

/// The sink's half of the same: a batch posted over QUIC to the push source's
/// own HTTP/3 endpoint, which also advertises itself on the TCP listener.
#[tokio::test]
async fn an_http3_sink_posts_to_the_push_sources_quic_endpoint() {
    let certificate = certificate();
    let mut source = HttpPushSource::new(
        "127.0.0.1:0",
        csv(),
        HttpPushOptions {
            tls: Some(ServerTls {
                cert_file: certificate.cert.clone(),
                key_file: certificate.key.clone(),
            }),
            quic_bind: Some("127.0.0.1:0".to_string()),
            ..push_options()
        },
    )
    .expect("both sockets bind");
    let tcp_address = source.local_addr();
    let quic_port = source.quic_local_addr().expect("quic addr").port();
    assert_ne!(quic_port, 0, "the UDP socket is bound at build");

    let mut options = HttpSinkOptions::default();
    options.client.http_version = HttpVersion::Http3;
    options.client.tls = TlsFiles {
        ca_file: Some(certificate.cert.clone()),
        ..TlsFiles::default()
    };
    options.client.timeout = Duration::from_secs(10);
    let mut sink = HttpSink::new(
        &format!("https://127.0.0.1:{quic_port}/ingest"),
        schema(),
        csv(),
        options,
    )
    .expect("sink builds");

    let writer = tokio::spawn(async move {
        sink.write_batch(&batch(&[6, 7])).await.expect("write");
    });
    let read_back = source.next_batch().await.expect("read").expect("one batch");
    assert_eq!(column(&read_back, "id"), vec![6, 7]);
    writer.await.expect("join");

    // A TCP client on the same listener is told where HTTP/3 lives.
    let client = reqwest::Client::builder()
        .tls_certs_merge(
            reqwest::Certificate::from_pem_bundle(&std::fs::read(&certificate.cert).expect("read"))
                .expect("bundle"),
        )
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");
    let response = client
        .post(format!("https://{tcp_address}/ingest"))
        .body(CSV_BODY.to_vec())
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 202);
    assert_eq!(
        response.headers().get("alt-svc").expect("alt-svc"),
        &format!("h3=\":{quic_port}\"; ma=86400")
    );
    source.next_batch().await.expect("read").expect("one batch");
}

/// The QUIC endpoint judges a request the way the TCP listener does: a path
/// nothing serves is a `404`, and a body over the cap a `413`.
#[tokio::test]
async fn the_quic_endpoint_refuses_a_wrong_path_and_an_oversized_body() {
    let certificate = certificate();
    let mut source = HttpPushSource::new(
        "127.0.0.1:0",
        csv(),
        HttpPushOptions {
            tls: Some(ServerTls {
                cert_file: certificate.cert.clone(),
                key_file: certificate.key.clone(),
            }),
            quic_bind: Some("127.0.0.1:0".to_string()),
            max_body_bytes: 16,
            ..push_options()
        },
    )
    .expect("both sockets bind");
    let quic_port = source.quic_local_addr().expect("quic addr").port();
    // The accept loops start on the first read, and nothing below is ever
    // queued for it: both requests are refused before a document exists.
    let listening = tokio::spawn(async move { source.next_batch().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let h3_sink = |path: &str| {
        let mut options = HttpSinkOptions::default();
        options.client.http_version = HttpVersion::Http3;
        options.client.tls = TlsFiles {
            ca_file: Some(certificate.cert.clone()),
            ..TlsFiles::default()
        };
        options.client.timeout = Duration::from_secs(10);
        HttpSink::new(
            &format!("https://127.0.0.1:{quic_port}{path}"),
            schema(),
            csv(),
            options,
        )
        .expect("sink builds")
    };

    let mut sink = h3_sink("/elsewhere");
    let Err(err) = sink.write_batch(&batch(&[1])).await else {
        panic!("nothing serves that path");
    };
    assert!(
        err.message().starts_with("HttpSink: status 404"),
        "got: {err}"
    );

    // 14 bytes of csv fit in the cap, so the refusal above was the path; 24
    // do not, which is this one.
    let mut sink = h3_sink("/ingest");
    let Err(err) = sink.write_batch(&batch(&[1, 2, 3])).await else {
        panic!("24 bytes do not fit in 16");
    };
    assert!(
        err.message().starts_with("HttpSink: status 413"),
        "got: {err}"
    );

    listening.abort();
}

/// HTTP/3 runs over TLS, so a cleartext url is refused while the half is
/// built rather than attempted and failed.
#[test]
fn http3_over_a_cleartext_url_is_refused_at_build() {
    let mut options = HttpSourceOptions {
        declared: Some(schema()),
        ..HttpSourceOptions::default()
    };
    options.client.http_version = HttpVersion::Http3;
    let Err(err) = HttpSource::new("http://example.invalid/data.csv", csv(), options) else {
        panic!("HTTP/3 has no cleartext form");
    };
    assert_eq!(err.category(), "configuration");
    assert_eq!(
        err.message(),
        "HttpSource: http_version \"3\" needs an https url: HTTP/3 runs over TLS"
    );

    let mut options = HttpSinkOptions::default();
    options.client.http_version = HttpVersion::Http3;
    let Err(err) = HttpSink::new("http://example.invalid/ingest", schema(), csv(), options) else {
        panic!("HTTP/3 has no cleartext form");
    };
    assert_eq!(
        err.message(),
        "HttpSink: http_version \"3\" needs an https url: HTTP/3 runs over TLS"
    );
}
