//! One transport, both directions: an [`HttpSource`] GET decoded by a
//! transformer, and an [`HttpSink`] request whose body decodes back to the rows
//! that went in.
//!
//! The server is hand rolled on a `std::net::TcpListener`: a spawned thread
//! accepts a fixed number of connections, captures each request, and answers
//! with one canned response. No HTTP framework, nothing to configure, and the
//! request count is exact, which is what pins "one request per batch".

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use saci_connector_http::{HttpSink, HttpSource, SchemaFrom};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_transformer::Transformer;
use saci_transformer_csv::CsvTransformer;
use saci_transformer_parquet::ParquetTransformer;

// ── the test server ─────────────────────────────────────────────────────────

/// One captured request: the method from the request line, the header names and
/// values, and the body `content-length` announced.
struct Captured {
    method: String,
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

/// Bind a server that answers `connections` requests with `status` and `body`.
///
/// Returns the url to aim a connector at and the handle carrying every captured
/// request, in arrival order.
fn serve(connections: usize, status: &str, body: Vec<u8>) -> (String, JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/data", listener.local_addr().expect("local_addr"));
    let status = status.to_string();
    let handle = std::thread::spawn(move || {
        let mut captured = Vec::with_capacity(connections);
        for _ in 0..connections {
            let (mut stream, _) = listener.accept().expect("accept");
            captured.push(exchange(&mut stream, &status, &body));
        }
        captured
    });
    (url, handle)
}

/// Read one request off `stream` and write the canned response back.
///
/// `connection: close` on the response is what makes the connection count
/// deterministic: the client cannot pool a socket the server said it would
/// close, so N requests are N accepts.
fn exchange(stream: &mut std::net::TcpStream, status: &str, body: &[u8]) -> Captured {
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
    let method = lines
        .next()
        .and_then(|line| line.split(' ').next())
        .unwrap_or_default()
        .to_string();

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

    // Whatever arrived past the head is already body; read the rest.
    let mut request_body = buffered[head_end..].to_vec();
    while request_body.len() < length {
        let read = stream.read(&mut chunk).expect("read request body");
        if read == 0 {
            break;
        }
        request_body.extend_from_slice(&chunk[..read]);
    }

    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).expect("write head");
    stream.write_all(body).expect("write body");
    stream.flush().expect("flush");

    Captured {
        method,
        headers,
        body: request_body,
    }
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

fn parquet() -> Arc<dyn Transformer> {
    Arc::new(ParquetTransformer::new())
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

fn source(url: &str, headers: Vec<(String, String)>) -> HttpSource {
    HttpSource::new(
        url,
        Some(schema()),
        SchemaFrom::Config,
        csv(),
        headers,
        Duration::from_secs(5),
    )
    .expect("source builds")
}

fn sink(url: &str, method: &str) -> HttpSink {
    HttpSink::new(
        url,
        schema(),
        csv(),
        method,
        Vec::new(),
        Duration::from_secs(5),
    )
    .expect("sink builds")
}

// ── the source half ─────────────────────────────────────────────────────────

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

/// `estimated_rows` forwards the reader's, so it is `None` before the fetch and
/// still `None` for a format that counts nothing.
#[tokio::test]
async fn estimated_rows_forwards_what_the_reader_reported() {
    let (url, server) = serve(1, "200 OK", CSV_BODY.to_vec());
    let mut source = source(&url, Vec::new());
    assert_eq!(source.estimated_rows(), None, "nothing has been fetched");

    while source.next_batch().await.expect("read").is_some() {}
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

/// `schema_from "body"` takes the body's own schema rather than projecting
/// onto the declared one. The declared schema is still what the graph is
/// validated against, so it is checked rather than dropped.
#[tokio::test]
async fn a_self_describing_body_is_read_with_its_own_schema() {
    let (url, server) = serve(1, "200 OK", parquet_body(schema(), &batch(&[1, 2, 3])));
    let mut source = HttpSource::new(
        &url,
        Some(schema()),
        SchemaFrom::Body,
        parquet(),
        Vec::new(),
        Duration::from_secs(5),
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
        Some(schema()),
        SchemaFrom::Config,
        parquet(),
        Vec::new(),
        Duration::from_secs(5),
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
        Some(schema()),
        SchemaFrom::Body,
        parquet(),
        Vec::new(),
        Duration::from_secs(5),
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
        None,
        SchemaFrom::Body,
        parquet(),
        Vec::new(),
        Duration::from_secs(5),
    ) else {
        panic!("schema_from body needs a schema to check against");
    };
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(err.message().contains("schema_fields"), "got: {err}");
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
