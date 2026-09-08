//! [`HttpSource`]: one HTTP GET read through a transformer.
//!
//! The request is made on the first [`Source::next_batch`], never in the
//! constructor, so `saci-service validate` touches no network. The response body
//! is spooled to an unnamed temp file and handed to the transformer's stream
//! read surface: [`Transformer::open_reader`] takes a `std::fs::File` because
//! parquet reads its footer before any row group. One dedicated OS thread then
//! drives the [`BatchReader`](saci_transformer::BatchReader) and pushes batches
//! down a bounded channel, so the executor never blocks on the decode.
//!
//! [`SchemaFrom`] decides what the format is handed: `schema_fields` verbatim,
//! which `csv` needs and which `parquet`, `avro` and `arrow-ipc` project the
//! body onto, or nothing at all, which leaves a self-describing format reading
//! its own. The declared schema is what [`Source::schema`] reports either way,
//! because the graph is validated before a request is made; with
//! [`SchemaFrom::Body`] it is checked field for field against the schema the
//! body turned out to carry, rather than used to project it.

use std::io::{Seek, Write};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema};
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use tokio::sync::mpsc;

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

use crate::client;

/// Batches the reader thread may queue before it blocks. Matches
/// `saci-connector-file`'s.
const CHANNEL_CAPACITY: usize = 4;

/// Where the Arrow schema a source hands the format comes from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SchemaFrom {
    /// `schema_fields` is handed to the format, including nothing when the
    /// config declared none: what `csv` needs, and what `parquet`, `avro` and
    /// `arrow-ipc` project the body onto.
    #[default]
    Config,
    /// The format reads its own schema out of the response body, and that
    /// schema must then equal `schema_fields` field for field. What a source
    /// wants when the body's own schema is what it reads, `parquet` and `avro`
    /// included.
    Body,
}

/// HTTP [`Source`]: one GET, decoded by a transformer, then EOF.
///
/// The body is fetched once, on the first batch, and the source reports EOF
/// when the decoded stream ends. It is finite, so every run mode can drive it:
/// `one_shot` reads the resource once, and `interval` re-reads it on the run it
/// is built for.
///
/// [`schema`](Source::schema) reports the declared schema, or an empty one when
/// the config declared none, and never changes: nothing can be known about a
/// body that has not arrived. Under [`SchemaFrom::Config`] a body carrying
/// columns the declaration does not name is projected onto it;
/// [`SchemaFrom::Body`] instead withholds the declaration, so the body's own
/// schema governs and is compared against it once the body arrives.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_http::{HttpSource, SchemaFrom};
/// use saci_transformer_csv::CsvTransformer;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// // csv carries no schema of its own, so the declared one is required.
/// let src = HttpSource::new(
///     "https://example.invalid/orders.csv",
///     Some(schema),
///     SchemaFrom::Config,
///     Arc::new(CsvTransformer::new(true)),
///     vec![("accept".to_string(), "text/csv".to_string())],
///     Duration::from_secs(30),
/// )
/// .unwrap();
/// ```
pub struct HttpSource {
    client: reqwest::Client,
    url: String,
    /// Cloned onto every request: reqwest's `headers` takes the map by value.
    headers: HeaderMap,
    transformer: Arc<dyn Transformer>,
    /// The schema the config declared, `None` when it declared none. What the
    /// format is handed depends on [`SchemaFrom`]: `Config` hands it over
    /// verbatim, `Body` withholds it so the body's own schema governs.
    declared: Option<Arc<Schema>>,
    /// Where the schema handed to the format comes from.
    schema_from: SchemaFrom,
    /// What [`Source::schema`] reports: `declared` when there is one, an empty
    /// schema when there is not. Resolved here so the accessor is one
    /// `Arc::clone`, and fixed for the source's lifetime.
    schema: Arc<Schema>,
    /// `None` until the first `next_batch` fetches the body.
    batches: Option<mpsc::Receiver<Result<RecordBatch, SaciError>>>,
    /// What the reader reported once it was opened, so it is `None` until the
    /// body has arrived.
    estimated_rows: Option<usize>,
}

impl HttpSource {
    /// Prepare the source without making a request.
    ///
    /// `url` is fetched once, on the first batch. `declared` is the schema the
    /// config named, `None` when it named none; what that means is the
    /// format's business, and `schema_from` decides whether the format sees it
    /// at all. `headers` are sent with the request, and `timeout` is the
    /// whole-request budget, connect through body.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when [`SchemaFrom::Body`] is asked
    /// for without a declared schema to check the body against, when a header
    /// name or value is malformed, or when the HTTP client cannot be built. No
    /// request is made.
    pub fn new(
        url: &str,
        declared: Option<Arc<Schema>>,
        schema_from: SchemaFrom,
        transformer: Arc<dyn Transformer>,
        headers: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Self, SaciError> {
        // Without a declared schema there is nothing for `Source::schema` to
        // report, so `validate_workflow_graph` would reject the link before a
        // request is ever made. Refusing here names the missing key instead.
        if schema_from == SchemaFrom::Body && declared.is_none() {
            return Err(SaciError::configuration(
                "HttpSource: schema_from \"body\" needs a 'schema_fields' list to check the \
                 body's own schema against",
            ));
        }
        Ok(Self {
            client: client::build_client("HttpSource", timeout)?,
            url: url.to_string(),
            headers: client::header_map("HttpSource", headers)?,
            transformer,
            schema: match &declared {
                Some(schema) => Arc::clone(schema),
                None => Arc::new(Schema::empty()),
            },
            declared,
            schema_from,
            batches: None,
            estimated_rows: None,
        })
    }

    /// GET the body, spool it, open the reader, and start the decode thread.
    ///
    /// Returns the receiver the decode thread feeds and the row estimate the
    /// reader reported.
    async fn fetch(
        &self,
    ) -> Result<
        (
            mpsc::Receiver<Result<RecordBatch, SaciError>>,
            Option<usize>,
        ),
        SaciError,
    > {
        let response = self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .send()
            .await
            .map_err(|e| self.transport_error(&e))?;

        let status = response.status();
        if !status.is_success() {
            return Err(SaciError::generic(format!(
                "HttpSource: status {status} from {}",
                self.url
            )));
        }

        // The whole body is buffered once here: reqwest hands it over as one
        // `Bytes`, and the read surface needs a seekable file anyway.
        let body = response
            .bytes()
            .await
            .map_err(|e| self.transport_error(&e))?;

        // Spooling and the metadata read are disk IO, and a parquet footer read
        // is a seek, so both happen off the executor.
        let transformer = Arc::clone(&self.transformer);
        let declared = match self.schema_from {
            SchemaFrom::Config => self.declared.clone(),
            SchemaFrom::Body => None,
        };
        let (mut reader, body_schema, estimated_rows) = tokio::task::spawn_blocking(move || {
            // Unnamed: the OS reclaims the file when the last handle closes,
            // which is when the reader below is dropped.
            let mut spool = tempfile::tempfile()
                .map_err(|e| SaciError::generic(format!("HttpSource: spool file: {e}")))?;
            spool
                .write_all(&body)
                .map_err(|e| SaciError::generic(format!("HttpSource: spool write: {e}")))?;
            spool
                .rewind()
                .map_err(|e| SaciError::generic(format!("HttpSource: spool rewind: {e}")))?;
            let reader = transformer.open_reader(spool, declared)?;
            let schema = reader.schema();
            let estimated_rows = reader.estimated_rows();
            Ok::<_, SaciError>((reader, schema, estimated_rows))
        })
        .await
        .map_err(|e| SaciError::generic(format!("HttpSource: spawn_blocking panic: {e}")))??;

        // The graph was validated against the declared schema before the body
        // existed, so a body carrying anything else is a configuration error
        // rather than rows to cast.
        if self.schema_from == SchemaFrom::Body && body_schema.fields() != self.schema.fields() {
            return Err(SaciError::configuration(format!(
                "HttpSource: body from {} carries schema [{}] but the config declared [{}]",
                self.url,
                render_fields(body_schema.fields()),
                render_fields(self.schema.fields()),
            )));
        }

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        std::thread::spawn(move || {
            loop {
                match reader.next_batch() {
                    Ok(None) => break,
                    Ok(Some(batch)) => {
                        // A send error means the source was dropped (pipeline
                        // aborted), so this thread has nowhere left to go.
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        break;
                    }
                }
            }
        });

        Ok((rx, estimated_rows))
    }

    /// The one wording every transport failure of the GET carries: a refused
    /// connection, a TLS rejection, a timeout, and a body that stops early are
    /// all the same request failing.
    fn transport_error(&self, error: &reqwest::Error) -> SaciError {
        SaciError::generic(format!("HttpSource: cannot GET {}: {error}", self.url))
    }
}

/// `Fields`'s own `Debug` is unreadable in an error a config author has to act
/// on; this renders `name: data_type` pairs.
fn render_fields(fields: &Fields) -> String {
    fields
        .iter()
        .map(|f| format!("{}: {}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[async_trait]
impl Source for HttpSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if self.batches.is_none() {
            let (rx, estimated_rows) = self.fetch().await?;
            self.batches = Some(rx);
            self.estimated_rows = estimated_rows;
        }
        let rx = self.batches.as_mut().expect("the receiver was just set");
        match rx.recv().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}
