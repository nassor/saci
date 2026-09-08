//! [`HttpSink`]: one HTTP request per batch, the body written by a transformer.
//!
//! Each batch gets a fresh writer over a fresh in-memory buffer, so every
//! request body is a self-contained document in the configured format: a csv
//! with its header row, a block of ndjson lines, one whole parquet or avro
//! container. There is no connection and no state to carry between batches,
//! which is why [`Sink::finish`] has nothing to do.
//!
//! The `content-type` a peer needs to read that document follows from the
//! transformer's format, so one is supplied unless `headers` already names it.
//! `content_encoding` compresses the finished document and declares what it
//! did; the encoding is the whole body's, so a peer decompresses once and then
//! reads the same document.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::Transformer;

use crate::client::{self, HttpClient};
use crate::metrics::Instruments;
use crate::source::HttpClientOptions;

/// The connector name every error and metric label of this half carries.
const WHAT: &str = "HttpSink";

/// How a request body is compressed before it is sent.
///
/// Every one of the three is an encoding reqwest itself decodes on the way
/// back, so a SACI sink and a SACI source agree with no extra configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentEncoding {
    /// `content-encoding: gzip`.
    Gzip,
    /// `content-encoding: br`.
    Br,
    /// `content-encoding: zstd`.
    Zstd,
}

impl ContentEncoding {
    /// The `content-encoding` token this encoding announces itself as.
    pub(crate) fn token(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Br => "br",
            Self::Zstd => "zstd",
        }
    }
}

/// Everything [`HttpSink::new`] takes past the url, schema and transformer.
///
/// The url is followed across `307` and `308` only, up to ten hops, because
/// those are the two redirects that keep the method and the body. A `301`,
/// `302` or `303` is reported as the status it is, since following it would
/// send a bodyless `GET` and report the new target's `200` for a batch that
/// never arrived. Over HTTP/3 nothing is followed at all.
#[derive(Debug, Clone)]
pub struct HttpSinkOptions {
    /// Transport, TLS, headers and the budget.
    pub client: HttpClientOptions,
    /// The HTTP method, `"POST"` in the usual case.
    pub method: String,
    /// How to compress each body, `None` to send it as written.
    pub content_encoding: Option<ContentEncoding>,
}

impl Default for HttpSinkOptions {
    fn default() -> Self {
        Self {
            client: HttpClientOptions::default(),
            method: "POST".to_string(),
            content_encoding: None,
        }
    }
}

/// The media type one whole document of `format` is.
///
/// `None` for a format with no registered media type, which sends no
/// `content-type` at all rather than a guess.
pub(crate) fn content_type_for(format: &str) -> Option<&'static str> {
    match format {
        "csv" => Some("text/csv"),
        "ndjson" => Some("application/x-ndjson"),
        "parquet" => Some("application/vnd.apache.parquet"),
        // The Avro specification's own HTTP media type; no IANA registration
        // exists for the container file.
        "avro" => Some("avro/binary"),
        // The transformer writes the stream encapsulation, not the file one.
        "arrow-ipc" => Some("application/vnd.apache.arrow.stream"),
        _ => None,
    }
}

/// A `std::io::Write` handle onto a shared byte buffer.
///
/// [`Transformer::open_writer`] takes ownership of the handle and
/// [`BatchWriter::finish`](saci_transformer::BatchWriter::finish) consumes the
/// writer, so the sink keeps a second handle on the same bytes to read the
/// finished document back out. The same trick `S3Sink` plays, and `FileSink`
/// with `File::try_clone`.
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    /// Drain the buffer, leaving it empty.
    fn take(&self) -> Vec<u8> {
        let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *inner)
    }
}

impl std::io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        inner.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// HTTP [`Sink`]: one request per batch, the transformer writes the body.
///
/// [`new`](Self::new) opens nothing, so `saci-service validate` passes while the
/// endpoint is down. The first request happens on the first batch, which is
/// where an unreachable endpoint surfaces. There is no batching across calls
/// and no retry: a request the server refuses fails the run.
///
/// A format with no stream write surface fails on the first batch with the
/// transformer contract's own error rather than at build.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_http::{HttpSink, HttpSinkOptions, HttpVersion};
/// use saci_transformer_csv::CsvTransformer;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let mut options = HttpSinkOptions::default();
/// options.client.http_version = HttpVersion::Http2;
/// let sink = HttpSink::new(
///     "https://example.invalid/ingest",
///     schema,
///     Arc::new(CsvTransformer::new(true)),
///     options,
/// )
/// .unwrap();
/// ```
pub struct HttpSink {
    client: HttpClient,
    url: String,
    method: Method,
    /// Cloned onto every request: the map is built once in the constructor.
    headers: HeaderMap,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    content_encoding: Option<ContentEncoding>,
    metrics: Instruments,
}

impl HttpSink {
    /// Prepare the sink without making a request.
    ///
    /// `schema` is the schema the rows are written with. A `content-type` is
    /// added from the transformer's format unless `options.client.headers`
    /// already carries one.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `options.method` is not a
    /// valid HTTP method, when `content_encoding` is set next to a
    /// `content-encoding` header, when `http_version "3"` is pointed at a
    /// non-https url, when a header name or value is malformed, when a `tls`
    /// file is missing or malformed, or when the HTTP client cannot be built.
    /// No request is made.
    pub fn new(
        url: &str,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
        options: HttpSinkOptions,
    ) -> Result<Self, SaciError> {
        let HttpSinkOptions {
            client: client_options,
            method,
            content_encoding,
        } = options;

        let parsed_method = Method::from_bytes(method.as_bytes()).map_err(|e| {
            SaciError::configuration(format!(
                "HttpSink: '{method}' is not a valid HTTP method: {e}"
            ))
        })?;
        client::require_https(WHAT, client_options.http_version, url)?;

        let mut headers = client_options.headers.clone();
        let declares = |name: &str| {
            headers
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case(name))
        };
        let declares_encoding = declares("content-encoding");
        let declares_type = declares("content-type");
        // Two sources of truth for one header is the kind of thing that only
        // shows up as a peer rejecting every other request.
        if content_encoding.is_some() && declares_encoding {
            return Err(SaciError::configuration(
                "HttpSink: 'content-encoding' is set by content_encoding; remove it from headers",
            ));
        }
        if !declares_type && let Some(media) = content_type_for(transformer.format()) {
            headers.push(("content-type".to_string(), media.to_string()));
        }
        if let Some(encoding) = content_encoding {
            headers.push(("content-encoding".to_string(), encoding.token().to_string()));
        }

        Ok(Self {
            client: HttpClient::build(client::ClientOptions {
                what: WHAT,
                version: client_options.http_version,
                tls: &client_options.tls,
                timeout: client_options.timeout,
                streaming: false,
                // A batch is the request body, so only a redirect that keeps
                // the body is safe to follow.
                redirects: client::Redirects::MethodPreserving,
            })?,
            url: url.to_string(),
            method: parsed_method,
            headers: client::header_map(WHAT, headers)?,
            schema,
            transformer,
            content_encoding,
            metrics: Instruments::new("sink", client_options.identity.as_ref()),
        })
    }

    /// Encode `batch` into one self-contained document, compressed when
    /// `content_encoding` asks for it.
    ///
    /// Called inline, exactly as `FileSink::write_batch` and `S3Sink` call
    /// their encoders: the format is CPU-bound, not IO-bound, so a blocking
    /// thread buys nothing here. Compression is CPU-bound for the same reason
    /// and happens in the same place.
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<u8>, SaciError> {
        let buffer = SharedBuffer::default();
        let mut writer = self
            .transformer
            .open_writer(Box::new(buffer.clone()), Arc::clone(&self.schema))?;
        writer.write_batch(batch)?;
        // Consumes the writer, so the format's own trailer reaches the buffer.
        writer.finish()?;
        let document = buffer.take();
        match self.content_encoding {
            None => Ok(document),
            Some(encoding) => compress(encoding, &document),
        }
    }

    /// Accept 2xx, reject everything else by naming the status and the url.
    fn check_status(&self, status: StatusCode) -> Result<(), SaciError> {
        if status.is_success() {
            Ok(())
        } else {
            self.metrics.error("status");
            Err(SaciError::generic(format!(
                "HttpSink: status {status} from {}",
                self.url
            )))
        }
    }
}

/// Compress one whole document.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the encoder fails, which for an
/// in-memory sink means it ran out of memory.
fn compress(encoding: ContentEncoding, document: &[u8]) -> Result<Vec<u8>, SaciError> {
    use std::io::Write as _;

    let failed = |e: std::io::Error| {
        SaciError::generic(format!(
            "HttpSink: cannot {} the body: {e}",
            encoding.token()
        ))
    };
    match encoding {
        ContentEncoding::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(document).map_err(failed)?;
            encoder.finish().map_err(failed)
        }
        ContentEncoding::Br => {
            let mut out = Vec::new();
            {
                // Quality 5 at a 22-bit window: brotli's own default quality
                // is 11, which costs an order of magnitude more CPU for a few
                // percent on a document that is about to leave the machine.
                let mut encoder = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
                encoder.write_all(document).map_err(failed)?;
            }
            Ok(out)
        }
        ContentEncoding::Zstd => zstd::encode_all(document, 3).map_err(failed),
    }
}

#[async_trait]
impl Sink for HttpSink {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Send one request carrying `batch` as its whole body.
    ///
    /// # Errors
    ///
    /// Returns the transformer's error when the format cannot write a stream or
    /// the batch cannot be encoded, and [`SaciError::Generic`] when the request
    /// fails or the server answers outside 2xx.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        let body = self.encode(batch)?;
        self.metrics.bytes("out", body.len() as u64);
        let started = Instant::now();
        let exchange = self
            .client
            .send(
                &self.method,
                &self.url,
                &self.headers,
                Some(Bytes::from(body)),
            )
            .await
            .map_err(|e| {
                self.metrics.error("transport");
                SaciError::generic(format!(
                    "HttpSink: cannot {} {}: {}",
                    self.method,
                    self.url,
                    e.message()
                ))
            })?;
        self.metrics
            .request(self.method.as_str(), exchange.status.as_u16());
        self.metrics
            .elapsed(self.method.as_str(), started.elapsed().as_secs_f64());
        self.check_status(exchange.status)
    }

    /// Nothing to flush and no connection to close: each batch was one whole
    /// request. Idempotent, so `run_with_io`'s unconditional finish is safe.
    async fn finish(&mut self) -> Result<(), SaciError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))])
            .expect("batch")
    }

    fn sink(options: HttpSinkOptions) -> Result<HttpSink, SaciError> {
        HttpSink::new(
            "http://127.0.0.1:1/ingest",
            schema(),
            Arc::new(NoStream),
            options,
        )
    }

    /// A transformer with no surfaces at all. Every registered format
    /// implements `open_writer`, so the contract's defaulted refusal is
    /// reached through a stand-in declared here.
    struct NoStream;

    impl Transformer for NoStream {
        fn format(&self) -> &'static str {
            "no-stream"
        }
    }

    #[test]
    fn a_shared_buffer_hands_back_what_was_written_through_the_clone() {
        let buffer = SharedBuffer::default();
        let mut handle = buffer.clone();
        handle.write_all(b"id\n1\n").expect("write");
        assert_eq!(buffer.take(), b"id\n1\n");
        assert!(buffer.take().is_empty(), "take drains the buffer");
    }

    /// Every registered format has one, and a format with none sends no
    /// `content-type` rather than a guess a peer would act on.
    #[test]
    fn every_registered_format_maps_to_its_media_type() {
        assert_eq!(content_type_for("csv"), Some("text/csv"));
        assert_eq!(content_type_for("ndjson"), Some("application/x-ndjson"));
        assert_eq!(
            content_type_for("parquet"),
            Some("application/vnd.apache.parquet")
        );
        assert_eq!(content_type_for("avro"), Some("avro/binary"));
        assert_eq!(
            content_type_for("arrow-ipc"),
            Some("application/vnd.apache.arrow.stream")
        );
        assert_eq!(content_type_for("no-stream"), None);
    }

    #[test]
    fn an_invalid_method_is_a_configuration_error_naming_it() {
        let Err(err) = sink(HttpSinkOptions {
            method: "PO ST".to_string(),
            ..HttpSinkOptions::default()
        }) else {
            panic!("a space is not legal in a method name");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'PO ST'"), "got: {err}");
    }

    #[test]
    fn content_encoding_next_to_its_own_header_is_refused() {
        let mut options = HttpSinkOptions {
            content_encoding: Some(ContentEncoding::Gzip),
            ..HttpSinkOptions::default()
        };
        options.client.headers = vec![("Content-Encoding".to_string(), "gzip".to_string())];

        let Err(err) = sink(options) else {
            panic!("one header, one source of truth");
        };
        assert_eq!(
            err.message(),
            "HttpSink: 'content-encoding' is set by content_encoding; remove it from headers"
        );
    }

    #[test]
    fn each_encoding_round_trips_through_its_own_decoder() {
        let document = b"id\n1\n2\n3\n".repeat(64);

        let gzipped = compress(ContentEncoding::Gzip, &document).expect("gzip");
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(&gzipped[..])
            .read_to_end(&mut out)
            .expect("gunzip");
        assert_eq!(out, document);

        let brotlied = compress(ContentEncoding::Br, &document).expect("br");
        let mut out = Vec::new();
        brotli::Decompressor::new(&brotlied[..], 4096)
            .read_to_end(&mut out)
            .expect("unbrotli");
        assert_eq!(out, document);

        let zstded = compress(ContentEncoding::Zstd, &document).expect("zstd");
        assert_eq!(zstd::decode_all(&zstded[..]).expect("unzstd"), document);

        assert!(
            gzipped.len() < document.len(),
            "a repetitive document compresses"
        );
    }

    /// The build succeeds and the format's missing capability surfaces on the
    /// first batch, before any request is attempted.
    #[tokio::test]
    async fn a_format_with_no_stream_write_surface_fails_on_the_first_batch() {
        let mut sink =
            sink(HttpSinkOptions::default()).expect("the sink builds whatever the format can do");

        let Err(err) = sink.write_batch(&batch(&[1])).await else {
            panic!("a format with no stream write surface cannot write a body");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("writing a byte stream"),
            "got: {err}"
        );
        assert!(err.message().contains("'no-stream'"), "got: {err}");
    }

    #[tokio::test]
    async fn finish_is_a_no_op_and_idempotent() {
        let mut sink = sink(HttpSinkOptions::default()).expect("sink builds");
        sink.finish().await.expect("finish");
        sink.finish().await.expect("finish is idempotent");
    }
}
