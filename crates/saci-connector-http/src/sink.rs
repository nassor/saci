//! [`HttpSink`]: one HTTP request per batch, the body written by a transformer.
//!
//! Each batch gets a fresh writer over a fresh in-memory buffer, so every
//! request body is a self-contained document in the configured format: a csv
//! with its header row, a block of ndjson lines, one whole parquet or avro
//! container. There is no connection and no state to carry between batches,
//! which is why [`Sink::finish`] has nothing to do.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::Transformer;

use crate::client;

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
/// use std::time::Duration;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_http::HttpSink;
/// use saci_transformer_csv::CsvTransformer;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let sink = HttpSink::new(
///     "https://example.invalid/ingest",
///     schema,
///     Arc::new(CsvTransformer::new(true)),
///     "POST",
///     vec![("content-type".to_string(), "text/csv".to_string())],
///     Duration::from_secs(30),
/// )
/// .unwrap();
/// ```
pub struct HttpSink {
    client: reqwest::Client,
    url: String,
    method: Method,
    /// Cloned onto every request: reqwest's `headers` takes the map by value.
    headers: HeaderMap,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
}

impl HttpSink {
    /// Prepare the sink without making a request.
    ///
    /// `method` is an HTTP method name, `"POST"` in the usual case. `schema` is
    /// the schema the rows are written with, `headers` are sent with every
    /// request, and `timeout` is the whole-request budget.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `method` is not a valid HTTP
    /// method, when a header name or value is malformed, or when the HTTP
    /// client cannot be built. No request is made.
    pub fn new(
        url: &str,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
        method: &str,
        headers: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Self, SaciError> {
        let method = Method::from_bytes(method.as_bytes()).map_err(|e| {
            SaciError::configuration(format!(
                "HttpSink: '{method}' is not a valid HTTP method: {e}"
            ))
        })?;
        Ok(Self {
            client: client::build_client("HttpSink", timeout)?,
            url: url.to_string(),
            method,
            headers: client::header_map("HttpSink", headers)?,
            schema,
            transformer,
        })
    }

    /// Encode `batch` into one self-contained document.
    ///
    /// Called inline, exactly as `FileSink::write_batch` and `S3Sink` call
    /// their encoders: the format is CPU-bound, not IO-bound, so a blocking
    /// thread buys nothing here.
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<u8>, SaciError> {
        let buffer = SharedBuffer::default();
        let mut writer = self
            .transformer
            .open_writer(Box::new(buffer.clone()), Arc::clone(&self.schema))?;
        writer.write_batch(batch)?;
        // Consumes the writer, so the format's own trailer reaches the buffer.
        writer.finish()?;
        Ok(buffer.take())
    }

    /// Accept 2xx, reject everything else by naming the status and the url.
    fn check_status(&self, status: StatusCode) -> Result<(), SaciError> {
        if status.is_success() {
            Ok(())
        } else {
            Err(SaciError::generic(format!(
                "HttpSink: status {status} from {}",
                self.url
            )))
        }
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
        let response = self
            .client
            .request(self.method.clone(), &self.url)
            .headers(self.headers.clone())
            .body(body)
            .send()
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "HttpSink: cannot {} {}: {e}",
                    self.method, self.url
                ))
            })?;
        self.check_status(response.status())
    }

    /// Nothing to flush and no connection to close: each batch was one whole
    /// request. Idempotent, so `run_with_io`'s unconditional finish is safe.
    async fn finish(&mut self) -> Result<(), SaciError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

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

    #[test]
    fn an_invalid_method_is_a_configuration_error_naming_it() {
        let Err(err) = HttpSink::new(
            "http://127.0.0.1:1/ingest",
            schema(),
            Arc::new(NoStream),
            "PO ST",
            Vec::new(),
            Duration::from_secs(1),
        ) else {
            panic!("a space is not legal in a method name");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'PO ST'"), "got: {err}");
    }

    /// The build succeeds and the format's missing capability surfaces on the
    /// first batch, before any request is attempted.
    #[tokio::test]
    async fn a_format_with_no_stream_write_surface_fails_on_the_first_batch() {
        let mut sink = HttpSink::new(
            "http://127.0.0.1:1/ingest",
            schema(),
            Arc::new(NoStream),
            "POST",
            Vec::new(),
            Duration::from_secs(1),
        )
        .expect("the sink builds whatever the format can do");

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
        let mut sink = HttpSink::new(
            "http://127.0.0.1:1/ingest",
            schema(),
            Arc::new(NoStream),
            "POST",
            Vec::new(),
            Duration::from_secs(1),
        )
        .expect("sink builds");
        sink.finish().await.expect("finish");
        sink.finish().await.expect("finish is idempotent");
    }
}
