//! [`OpenTelemetryMetricsSink`]: pipeline rows out as OTLP/HTTP metrics.
//!
//! One `write_batch` is one export, or several when the batch holds more rows
//! than `batch_rows`: the request is built from the batch's columns by
//! [`schema::record_batch_to_export_request`], encoded as Protobuf or JSON, and
//! posted to the configured endpoint. A batch whose columns hold no row exports
//! nothing and still succeeds.
//!
//! Delivery is at-least-once. A status outside 2xx is an error the runner
//! retries under its own policy, and a batch the sender posted but whose
//! response was lost is posted again, which OTLP's data-point identity makes
//! idempotent for a cumulative metric and not for a delta one.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;

use crate::config::OpenTelemetryMetricsSinkConfig;
use crate::schema;

/// The connector name every error of this half opens with.
const WHAT: &str = "OpenTelemetryMetricsSink";

/// An OTLP/HTTP metrics exporter.
///
/// # Example
///
/// ```rust,no_run
/// use saci_connector_opentelemetry::{
///     OpenTelemetryMetricsSink, OpenTelemetryMetricsSinkConfig,
/// };
///
/// let sink = OpenTelemetryMetricsSink::new(OpenTelemetryMetricsSinkConfig {
///     url: "http://localhost:4318".to_string(),
///     encoding: Default::default(),
///     headers: Default::default(),
///     timeout_ms: 10_000,
///     metric_name: None,
///     service_name: None,
///     batch_rows: 8_192,
///     schema_fields: Vec::new(),
/// }, None)
/// .expect("the config is usable");
/// assert_eq!(sink.endpoint(), "http://localhost:4318/v1/metrics");
/// ```
pub struct OpenTelemetryMetricsSink {
    schema: Arc<Schema>,
    client: reqwest::Client,
    endpoint: String,
    headers: http::HeaderMap,
    encoding: crate::config::Encoding,
    metric_name: Option<String>,
    service_name: Option<String>,
    batch_rows: usize,
}

impl OpenTelemetryMetricsSink {
    /// Build the sink, resolving its endpoint and opening its client.
    ///
    /// `declared` is the Arrow schema the node declares, which
    /// [`Sink::schema`] reports so the workflow graph check compares it against
    /// the source's; `None` reports [`schema::metrics_schema`].
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `url` is not an HTTP url,
    /// when a count or timeout is zero, when a header is not a valid HTTP
    /// header pair, when a header names a `content-type`, or when the client
    /// cannot be built.
    pub fn new(
        config: OpenTelemetryMetricsSinkConfig,
        declared: Option<Arc<Schema>>,
    ) -> Result<Self, SaciError> {
        let endpoint = config.endpoint(WHAT)?;
        let mut headers = http::HeaderMap::with_capacity(config.headers.len() + 1);
        for (name, value) in &config.headers {
            // The `encoding` key owns the body's `content-type`, and two of
            // them on one request leaves the receiver to pick: the same
            // separation `content_encoding` and a `content-encoding` header
            // have in the HTTP sink.
            if name.eq_ignore_ascii_case("content-type") {
                return Err(SaciError::configuration(format!(
                    "{WHAT}: headers must not name a content-type, which `encoding` decides"
                )));
            }
            let name = http::header::HeaderName::try_from(name.as_str()).map_err(|e| {
                SaciError::configuration(format!(
                    "{WHAT}: '{name}' is not a valid header name: {e}"
                ))
            })?;
            let value = http::header::HeaderValue::try_from(value.as_str()).map_err(|e| {
                SaciError::configuration(format!(
                    "{WHAT}: header '{name}' has an invalid value: {e}"
                ))
            })?;
            headers.append(name, value);
        }
        // Every request and response body is one whole OTLP payload, so the
        // timeout is the whole exchange, and a gzip'd response is decoded by
        // the client without a key of its own.
        crate::install_ring();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| SaciError::configuration(format!("{WHAT}: cannot build a client: {e}")))?;
        Ok(Self {
            schema: declared.unwrap_or_else(schema::metrics_schema),
            client,
            endpoint,
            headers,
            encoding: config.encoding,
            metric_name: config.metric_name.clone(),
            service_name: config.service_name.clone(),
            batch_rows: config.batch_rows,
        })
    }

    /// The url every export is posted to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Post one built request.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the request cannot be encoded, and
    /// [`SaciError::Store`] when the endpoint does not answer or answers a
    /// status outside 2xx.
    async fn export(&self, batch: &RecordBatch) -> Result<(), SaciError> {
        let request = schema::record_batch_to_export_request(
            batch,
            self.metric_name.as_deref(),
            self.service_name.as_deref(),
        )?;
        let body = schema::encode(self.encoding, &request)?;
        let mut call = self
            .client
            .post(&self.endpoint)
            .header(
                http::header::CONTENT_TYPE,
                schema::media_type(self.encoding),
            )
            .body(body);
        for (name, value) in &self.headers {
            call = call.header(name, value);
        }
        let response = call.send().await.map_err(|e| {
            SaciError::store(format!("{WHAT}: {} did not answer: {e}", self.endpoint))
        })?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        // The collector's own reason is what an operator needs, and it is
        // short: a full page of HTML from a proxy in front of it is not.
        let reason = response.text().await.unwrap_or_default();
        let reason: String = reason.trim().chars().take(200).collect();
        Err(SaciError::store(format!(
            "{WHAT}: {} answered {status}: {reason}",
            self.endpoint
        )))
    }
}

#[async_trait]
impl Sink for OpenTelemetryMetricsSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if batch.num_rows() <= self.batch_rows {
            return self.export(batch).await;
        }
        // A batch larger than `batch_rows` goes out as several requests, so one
        // export stays inside a collector's body limit.
        let mut offset = 0;
        while offset < batch.num_rows() {
            let rows = self.batch_rows.min(batch.num_rows() - offset);
            let slice = batch.slice(offset, rows);
            self.export(&slice).await?;
            offset += rows;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // Every export is awaited in `write_batch`, so nothing is buffered to
        // flush here.
        Ok(())
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn pending_rows(&self) -> Option<usize> {
        // A batch is posted before `write_batch` returns, so the backlog is
        // always empty; the flow controller reads zero rather than nothing.
        Some(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Encoding;

    fn config(url: &str) -> OpenTelemetryMetricsSinkConfig {
        OpenTelemetryMetricsSinkConfig {
            url: url.to_string(),
            encoding: Encoding::Protobuf,
            headers: Default::default(),
            timeout_ms: crate::config::DEFAULT_TIMEOUT_MS,
            metric_name: None,
            service_name: None,
            batch_rows: crate::config::DEFAULT_BATCH_ROWS,
            schema_fields: Vec::new(),
        }
    }

    #[test]
    fn an_invalid_header_is_a_configuration_error() {
        let mut bad = config("http://127.0.0.1:1");
        bad.headers
            .insert("not a name".to_string(), "value".to_string());
        let error = match OpenTelemetryMetricsSink::new(bad, None) {
            Ok(_) => panic!("the header is refused"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("header name"), "{error}");
    }

    #[test]
    fn a_configured_content_type_is_refused() {
        let mut bad = config("http://127.0.0.1:1");
        bad.headers
            .insert("Content-Type".to_string(), "application/json".to_string());
        let error = match OpenTelemetryMetricsSink::new(bad, None) {
            Ok(_) => panic!("the header is refused"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("encoding"), "{error}");
    }

    #[test]
    fn an_empty_batch_exports_nothing_and_succeeds() {
        let sink =
            OpenTelemetryMetricsSink::new(config("http://127.0.0.1:1"), None).expect("builds");
        let empty = RecordBatch::new_empty(sink.schema());
        // The endpoint is a closed port: a request would fail, so a success
        // here is the empty batch short-circuiting before it.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime")
            .block_on(async {
                let mut sink = sink;
                sink.write_batch(&empty).await.expect("nothing to send");
                sink.finish().await.expect("nothing to flush");
                assert_eq!(sink.pending_rows(), Some(0));
            });
    }
}
