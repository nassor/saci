//! [`HttpSource`]: one HTTP read, in one of three shapes.
//!
//! [`ReadMode::Document`] is one GET of a finite resource. The body is spooled
//! to an unnamed temp file as it arrives and handed to the transformer's stream
//! read surface: [`Transformer::open_reader`] takes a `std::fs::File` because
//! parquet reads its footer before any row group. One dedicated OS thread then
//! drives the [`BatchReader`](saci_transformer::BatchReader) and pushes batches
//! down a bounded channel, so the executor never blocks on the decode. The
//! source reports EOF when that stream ends and re-arms, so the next call
//! issues a fresh GET: that is what makes `interval` re-read the resource on
//! every tick. `pagination` follows `Link` headers across pages before
//! reporting EOF, and `conditional` sends the validators the last response
//! carried so an unchanged resource answers `304` and decodes nothing.
//!
//! [`ReadMode::Lines`] and [`ReadMode::Events`] read a body that never ends:
//! newline-delimited records, and `text/event-stream`. Each frame goes through
//! the transformer's *message* surface rather than its stream surface, a window
//! closes on `batch_size` frames or `poll_timeout`, and a body that does end is
//! reconnected to after `reconnect`. Both are live sources: only the stream
//! runner can drive them, and `saci-service`'s config validator refuses any
//! other run mode.
//!
//! [`SchemaFrom`] decides what a document mode read hands the format:
//! `schema_fields` verbatim, which `csv` needs and which `parquet`, `avro` and
//! `arrow-ipc` project the body onto, or nothing at all, which leaves a
//! self-describing format reading its own. The declared schema is what
//! [`Source::schema`] reports either way, because the graph is validated before
//! a request is made; with [`SchemaFrom::Body`] it is checked field for field
//! against the schema the body turned out to carry, rather than used to project
//! it. A streaming mode has no such choice: the message surface takes a
//! required schema, opened before the first byte arrives.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Method, Url};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use saci_connector::NodeIdentity;
use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

use crate::client::{self, Exchange, HttpClient, HttpVersion};
use crate::document::{self, CHANNEL_CAPACITY};
use crate::framing::{EventFramer, LineFramer, OverCap};
use crate::metrics::Instruments;
use crate::tls::TlsFiles;

/// The connector name every error and metric label of this half carries.
const WHAT: &str = "HttpSource";

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

/// Whether a document mode read follows `Link` headers past the first page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Pagination {
    /// One request, one document.
    #[default]
    None,
    /// Follow `Link: <...>; rel="next"` until a page carries no next link,
    /// feeding every page's rows through as one read.
    Link,
}

/// How a streaming read closes one batch.
///
/// A window ends on whichever comes first: `batch_size` frames, or
/// `poll_timeout` since the window's first frame. `reconnect` is how long the
/// reader waits before re-requesting a body that ended.
#[derive(Debug, Clone, Copy)]
pub struct Window {
    /// Frames per batch. A decoder that cannot flush after refusing a frame,
    /// ndjson's among them, loses the open window's other frames with it, so a
    /// smaller `batch_size` bounds that loss.
    pub batch_size: usize,
    /// How long a partially filled window waits for its next frame.
    pub poll_timeout: Duration,
    /// How long to wait before re-requesting a body that ended or failed.
    pub reconnect: Duration,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            poll_timeout: Duration::from_secs(1),
            reconnect: Duration::from_secs(1),
        }
    }
}

/// What shape the resource being read is.
#[derive(Debug, Clone)]
pub enum ReadMode {
    /// One finite document per read, optionally paginated and conditional.
    Document {
        /// Whether to follow `Link` headers past the first page.
        pagination: Pagination,
        /// Whether to send `if-none-match`/`if-modified-since` from the last
        /// response's validators.
        conditional: bool,
    },
    /// A body that never ends, one record per line.
    Lines(Window),
    /// A `text/event-stream` body that never ends.
    Events {
        /// When set, only events whose `event:` field equals this are decoded;
        /// every other event, and every unnamed one, is skipped.
        event: Option<String>,
        /// Batch and reconnect geometry.
        window: Window,
    },
}

impl Default for ReadMode {
    fn default() -> Self {
        Self::Document {
            pagination: Pagination::None,
            conditional: false,
        }
    }
}

/// What both client halves take: transport, TLS, headers and the budget.
#[derive(Debug, Clone)]
pub struct HttpClientOptions {
    /// Which HTTP version to speak.
    pub http_version: HttpVersion,
    /// The `tls` block: a private CA to trust, a client identity to present.
    pub tls: TlsFiles,
    /// Headers sent with every request.
    pub headers: Vec<(String, String)>,
    /// The request budget. On a streaming read this bounds the connect and the
    /// gap between body chunks instead of the whole request.
    pub timeout: Duration,
    /// The node this connector is, for metric labels. `None` for a library
    /// caller that built one outside a service.
    pub identity: Option<NodeIdentity>,
}

impl Default for HttpClientOptions {
    fn default() -> Self {
        Self {
            http_version: HttpVersion::default(),
            tls: TlsFiles::default(),
            headers: Vec::new(),
            timeout: Duration::from_secs(30),
            identity: None,
        }
    }
}

/// Everything [`HttpSource::new`] takes past the url and the transformer.
#[derive(Debug, Clone, Default)]
pub struct HttpSourceOptions {
    /// Transport, TLS, headers and the budget.
    pub client: HttpClientOptions,
    /// The schema the config declared, `None` when it declared none.
    pub declared: Option<Arc<Schema>>,
    /// Where the schema handed to the format comes from.
    pub schema_from: SchemaFrom,
    /// What shape the resource is.
    pub mode: ReadMode,
    /// The largest body to accept, `None` for no cap.
    pub max_body_bytes: Option<usize>,
}

/// What the source and the reader task share: the transport and the target.
struct Request {
    client: HttpClient,
    url: String,
    headers: HeaderMap,
    metrics: Instruments,
}

impl Request {
    /// Send one GET and return its response, recording the exchange.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the url when the request cannot be
    /// made or the response head does not arrive.
    async fn get(&self, url: &str, extra: &HeaderMap) -> Result<Exchange, SaciError> {
        let mut headers = self.headers.clone();
        for (name, value) in extra {
            headers.insert(name.clone(), value.clone());
        }
        let started = Instant::now();
        let exchange = self
            .client
            .send(&Method::GET, url, &headers, None)
            .await
            .map_err(|e| {
                self.metrics.error("transport");
                SaciError::generic(format!("{WHAT}: cannot GET {url}: {}", e.message()))
            })?;
        self.metrics.request("GET", exchange.status.as_u16());
        self.metrics.elapsed("GET", started.elapsed().as_secs_f64());
        Ok(exchange)
    }
}

/// Document mode state: the page being drained and what the last page said.
struct DocumentState {
    pagination: Pagination,
    conditional: bool,
    /// `None` between reads, which is what re-arms the next `next_batch` into
    /// a fresh GET.
    batches: Option<mpsc::Receiver<Result<RecordBatch, SaciError>>>,
    /// The `Link rel="next"` target of the page being drained.
    next_page: Option<String>,
    /// The last response's `etag`, sent back as `if-none-match`.
    etag: Option<HeaderValue>,
    /// The last response's `last-modified`, sent back as `if-modified-since`.
    last_modified: Option<HeaderValue>,
    /// What the first page's reader reported.
    estimated_rows: Option<usize>,
}

/// Streaming mode state: the reader task and the channel it feeds.
struct StreamState {
    window: Window,
    /// `Some` in [`ReadMode::Events`], carrying the optional event filter.
    events: Option<Option<String>>,
    batches: Option<mpsc::Receiver<Result<RecordBatch, SaciError>>>,
    task: Option<JoinHandle<()>>,
}

/// Which read the source is doing, fixed at construction.
enum Engine {
    Document(DocumentState),
    Stream(StreamState),
}

/// HTTP [`Source`]: one document per read, or a live stream of frames.
///
/// No request is made while the source is built, so `saci-service validate`
/// touches no network. HTTPS needs no configuration: rustls verifies the peer
/// against the platform trust store, and a `tls` block only adds to that.
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
///
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_http::{HttpSource, HttpSourceOptions, SchemaFrom};
/// use saci_transformer_csv::CsvTransformer;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// // csv carries no schema of its own, so the declared one is required.
/// let src = HttpSource::new(
///     "https://example.invalid/orders.csv",
///     Arc::new(CsvTransformer::new(true)),
///     HttpSourceOptions {
///         declared: Some(schema),
///         schema_from: SchemaFrom::Config,
///         ..HttpSourceOptions::default()
///     },
/// )
/// .unwrap();
/// ```
pub struct HttpSource {
    request: Arc<Request>,
    transformer: Arc<dyn Transformer>,
    /// The schema the config declared, `None` when it declared none. What the
    /// format is handed depends on [`SchemaFrom`]: `Config` hands it over
    /// verbatim, `Body` withholds it so the body's own schema governs.
    declared: Option<Arc<Schema>>,
    schema_from: SchemaFrom,
    /// What [`Source::schema`] reports: `declared` when there is one, an empty
    /// schema when there is not. Resolved here so the accessor is one
    /// `Arc::clone`, and fixed for the source's lifetime.
    schema: Arc<Schema>,
    max_body_bytes: Option<usize>,
    engine: Engine,
}

impl HttpSource {
    /// Prepare the source without making a request.
    ///
    /// `url` is read on the first batch. In a streaming mode the transformer's
    /// message decoder is opened here, against the declared schema, so a
    /// format that cannot decode discrete messages is a configuration error
    /// before a connection is opened rather than a reader that connects,
    /// closes, and delivers nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when [`SchemaFrom::Body`] is asked
    /// for without a declared schema to check the body against, when a
    /// streaming mode is asked for without one at all, when a streaming mode
    /// is combined with `schema_from "body"`, when `http_version "3"` is
    /// pointed at a non-https url, when a header name or value is malformed,
    /// when a `tls` file is missing or malformed, or when the HTTP client
    /// cannot be built. No request is made.
    pub fn new(
        url: &str,
        transformer: Arc<dyn Transformer>,
        options: HttpSourceOptions,
    ) -> Result<Self, SaciError> {
        let HttpSourceOptions {
            client: client_options,
            declared,
            schema_from,
            mode,
            max_body_bytes,
        } = options;

        // Without a declared schema there is nothing for `Source::schema` to
        // report, so `validate_workflow_graph` would reject the link before a
        // request is ever made. Refusing here names the missing key instead.
        if schema_from == SchemaFrom::Body && declared.is_none() {
            return Err(SaciError::configuration(
                "HttpSource: schema_from \"body\" needs a 'schema_fields' list to check the \
                 body's own schema against",
            ));
        }
        let streaming = !matches!(mode, ReadMode::Document { .. });
        if streaming && schema_from == SchemaFrom::Body {
            return Err(SaciError::configuration(
                "HttpSource: schema_from \"body\" applies to mode kind=\"document\" only",
            ));
        }
        let declared_for_stream = match (streaming, &declared) {
            (false, _) => None,
            (true, Some(schema)) => Some(Arc::clone(schema)),
            (true, None) => {
                let kind = match mode {
                    ReadMode::Lines(_) => "lines",
                    _ => "events",
                };
                return Err(SaciError::configuration(format!(
                    "HttpSource: mode kind=\"{kind}\" needs a 'schema_fields' list: a message \
                     decoder is built before the first byte arrives"
                )));
            }
        };
        client::require_https(WHAT, client_options.http_version, url)?;

        // The answer depends on the schema and nothing else, so every
        // connection would get the same one. Asking here is what makes a
        // format with no message decoder a configuration error.
        if let Some(schema) = declared_for_stream {
            transformer.open_message_decoder(schema)?;
        }

        let mut headers = client_options.headers.clone();
        // Servers key `text/event-stream` off `Accept`, and a configured
        // `accept` is the author saying what they want instead.
        if matches!(mode, ReadMode::Events { .. })
            && !headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("accept"))
        {
            headers.push(("accept".to_string(), "text/event-stream".to_string()));
        }

        let client = HttpClient::build(client::ClientOptions {
            what: WHAT,
            version: client_options.http_version,
            tls: &client_options.tls,
            timeout: client_options.timeout,
            streaming,
            // A redirected `GET` delivers nothing and asks for nothing, so
            // the read half follows whatever the peer points it at.
            redirects: client::Redirects::Any,
        })?;

        let engine = match mode {
            ReadMode::Document {
                pagination,
                conditional,
            } => Engine::Document(DocumentState {
                pagination,
                conditional,
                batches: None,
                next_page: None,
                etag: None,
                last_modified: None,
                estimated_rows: None,
            }),
            ReadMode::Lines(window) => Engine::Stream(StreamState {
                window,
                events: None,
                batches: None,
                task: None,
            }),
            ReadMode::Events { event, window } => Engine::Stream(StreamState {
                window,
                events: Some(event),
                batches: None,
                task: None,
            }),
        };

        Ok(Self {
            request: Arc::new(Request {
                client,
                url: url.to_string(),
                headers: client::header_map(WHAT, headers)?,
                metrics: Instruments::new("source", client_options.identity.as_ref()),
            }),
            transformer,
            schema: match &declared {
                Some(schema) => Arc::clone(schema),
                None => Arc::new(Schema::empty()),
            },
            declared,
            schema_from,
            max_body_bytes,
            engine,
        })
    }
}

/// One fetched page: the batches it decodes to, and what its headers said.
struct Page {
    batches: mpsc::Receiver<Result<RecordBatch, SaciError>>,
    estimated_rows: Option<usize>,
    next_page: Option<String>,
    etag: Option<HeaderValue>,
    last_modified: Option<HeaderValue>,
}

/// What decoding one page needs, the same for every page of one read.
struct Decode {
    transformer: Arc<dyn Transformer>,
    /// What the format is handed, already resolved from the `schema_from`
    /// rule: the declaration verbatim, or nothing so a self-describing format
    /// reads its own.
    declared: Option<Arc<Schema>>,
    /// Whether the body's own schema is checked against `expected`.
    check: bool,
    /// What the graph was validated against.
    expected: Arc<Schema>,
    /// The largest body to accept.
    cap: Option<usize>,
}

/// GET one page, spool it, open the reader, and start the decode thread.
///
/// `validators` is the conditional-request pair to send, empty for a page that
/// is not the first. `Ok(None)` is a `304 Not Modified`: nothing was spooled,
/// and the stored validators stay as they were.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the request fails, the status is outside
/// 2xx and not `304`, the body exceeds the cap, or the spool cannot be written;
/// and the format's own error, or [`SaciError::Configuration`] for a body whose
/// schema differs from the declaration, when the reader cannot be opened.
async fn fetch_page(
    request: &Arc<Request>,
    decode: Decode,
    url: &str,
    validators: &HeaderMap,
) -> Result<Option<Page>, SaciError> {
    let Decode {
        transformer,
        declared,
        check,
        expected,
        cap,
    } = decode;
    let mut exchange = request.get(url, validators).await?;

    if exchange.status == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }
    if !exchange.status.is_success() {
        request.metrics.error("status");
        return Err(SaciError::generic(format!(
            "{WHAT}: status {} from {url}",
            exchange.status
        )));
    }

    // RFC 8288's link context is the url the representation came from, which
    // is the last hop when the request was redirected. Both ends of that
    // redirect are the same page: a next link naming the url that was asked
    // for would be answered by the same redirect, forever.
    let next_page = match next_link(&exchange.headers, &exchange.url)? {
        Some(next) if next == exchange.url || next == url => {
            return Err(SaciError::generic(format!(
                "{WHAT}: Link rel=\"next\" from {} points back at the same page",
                exchange.url
            )));
        }
        next => next,
    };
    let etag = exchange.headers.get("etag").cloned();
    let last_modified = exchange.headers.get("last-modified").cloned();
    let announced = exchange
        .headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let spool = match document::spool(WHAT, url, announced, &mut exchange.body, cap).await {
        Ok((spool, read)) => {
            request.metrics.bytes("in", read);
            spool
        }
        Err(e) => {
            if let Some(kind) = e.kind() {
                request.metrics.error(kind);
            }
            return Err(e.into_error());
        }
    };

    // Opening the reader is blocking: a parquet footer read is a seek.
    let source = url.to_string();
    let reader = tokio::task::spawn_blocking(move || {
        document::open(
            WHAT,
            &source,
            &transformer,
            spool,
            declared,
            check,
            &expected,
        )
    })
    .await
    .map_err(|e| SaciError::generic(format!("{WHAT}: spawn_blocking panic: {e}")))?
    .inspect_err(|_| request.metrics.error("decode"))?;

    let estimated_rows = reader.estimated_rows();
    let (tx, batches) = mpsc::channel(CHANNEL_CAPACITY);
    document::spawn_reader(reader, tx);

    Ok(Some(Page {
        batches,
        estimated_rows,
        next_page,
        etag,
        last_modified,
    }))
}

/// The `Link rel="next"` target of a response, resolved against `base`.
///
/// RFC 8288: a `Link` field value is a comma-separated list, each entry
/// `<target>` followed by `; name=value` parameters whose value may be quoted,
/// and whose `rel` may name several relations separated by spaces.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the target is not a url that resolves
/// against the page it came from.
fn next_link(headers: &HeaderMap, base: &str) -> Result<Option<String>, SaciError> {
    let base = match Url::parse(base) {
        Ok(base) => base,
        // A page url this crate itself sent cannot be unparseable, but a
        // caller-supplied one reaches here before any request is judged.
        Err(_) => return Ok(None),
    };
    for value in headers.get_all("link") {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for entry in value.split(',') {
            let mut parts = entry.split(';');
            let Some(target) = parts.next() else {
                continue;
            };
            let target = target.trim();
            let Some(target) = target
                .strip_prefix('<')
                .and_then(|rest| rest.strip_suffix('>'))
            else {
                continue;
            };
            let is_next = parts.any(|param| {
                let Some((name, raw)) = param.split_once('=') else {
                    return false;
                };
                name.trim().eq_ignore_ascii_case("rel")
                    && raw
                        .trim()
                        .trim_matches('"')
                        .split_whitespace()
                        .any(|rel| rel.eq_ignore_ascii_case("next"))
            });
            if is_next {
                return base
                    .join(target)
                    .map(|url| Some(url.to_string()))
                    .map_err(|e| {
                        SaciError::generic(format!(
                            "{WHAT}: Link rel=\"next\" from {base} is not a valid url: {e}"
                        ))
                    });
            }
        }
    }
    Ok(None)
}

/// The conditional-request headers to send for a first-page GET.
fn validators(state: &DocumentState) -> HeaderMap {
    let mut map = HeaderMap::new();
    if !state.conditional {
        return map;
    }
    if let Some(etag) = &state.etag {
        map.insert("if-none-match", etag.clone());
    }
    if let Some(modified) = &state.last_modified {
        map.insert("if-modified-since", modified.clone());
    }
    map
}

/// Read one live body into `tx`, forever, reconnecting after every end.
///
/// Runs as one spawned task per source, aborted when the source is dropped.
/// Every outcome reaches the consumer: a transport failure, a non-2xx status
/// and a decode error are all sent as `Err` so the runner counts them, and the
/// reader then waits `reconnect` and asks again.
async fn read_stream(
    request: Arc<Request>,
    transformer: Arc<dyn Transformer>,
    schema: Arc<Schema>,
    window: Window,
    events: Option<Option<String>>,
    cap: Option<usize>,
    tx: mpsc::Sender<Result<RecordBatch, SaciError>>,
) {
    let mut reconnect = window.reconnect;
    let mut last_event_id: Option<HeaderValue> = None;

    loop {
        let mut extra = HeaderMap::new();
        if let Some(id) = &last_event_id {
            extra.insert("last-event-id", id.clone());
        }
        let exchange = match request.get(&request.url, &extra).await {
            Ok(exchange) => exchange,
            Err(e) => {
                if tx.send(Err(e)).await.is_err() {
                    return;
                }
                tokio::time::sleep(reconnect).await;
                continue;
            }
        };
        if !exchange.status.is_success() {
            request.metrics.error("status");
            let status = exchange.status;
            let url = &request.url;
            if tx
                .send(Err(SaciError::generic(format!(
                    "{WHAT}: status {status} from {url}"
                ))))
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(reconnect).await;
            continue;
        }

        let outcome = read_body(
            &request,
            &transformer,
            &schema,
            window,
            events.as_ref(),
            cap,
            &tx,
            exchange,
            &mut reconnect,
            &mut last_event_id,
        )
        .await;
        match outcome {
            BodyEnd::ConsumerGone => return,
            BodyEnd::Reconnect => {}
        }
        tokio::time::sleep(reconnect).await;
    }
}

/// Why one live body's read stopped.
#[derive(Debug)]
enum BodyEnd {
    /// The source was dropped; the task has nowhere left to go.
    ConsumerGone,
    /// The body ended or failed; ask again.
    Reconnect,
}

/// Read one live body to its end, closing a window per `batch_size` frames or
/// per `poll_timeout`.
#[allow(clippy::too_many_arguments)]
async fn read_body(
    request: &Arc<Request>,
    transformer: &Arc<dyn Transformer>,
    schema: &Arc<Schema>,
    window: Window,
    events: Option<&Option<String>>,
    cap: Option<usize>,
    tx: &mpsc::Sender<Result<RecordBatch, SaciError>>,
    mut exchange: Exchange,
    reconnect: &mut Duration,
    last_event_id: &mut Option<HeaderValue>,
) -> BodyEnd {
    let mut frames = match Frames::open(&request.metrics, transformer, schema, tx, window) {
        Ok(frames) => frames,
        Err(e) => {
            request.metrics.error("decode");
            return if tx.send(Err(e)).await.is_err() {
                BodyEnd::ConsumerGone
            } else {
                BodyEnd::Reconnect
            };
        }
    };
    let mut lines = LineFramer::new(cap);
    let mut sse = EventFramer::new(cap);

    loop {
        let chunk = match frames.deadline {
            Some(at) => {
                match tokio::time::timeout_at(at.into(), exchange.body.next_chunk()).await {
                    Ok(read) => read,
                    Err(_) => match frames.close().await {
                        Ok(()) => continue,
                        Err(end) => return end,
                    },
                }
            }
            None => exchange.body.next_chunk().await,
        };

        let chunk = match chunk {
            Ok(Some(chunk)) => chunk,
            Ok(None) => {
                let _ = frames.close().await;
                if events.is_some() {
                    #[cfg(feature = "tracing")]
                    tracing::info!(url = %request.url, "HttpSource: event stream ended, reconnecting");
                    return BodyEnd::Reconnect;
                }
                return if tx
                    .send(Err(SaciError::generic(format!(
                        "{WHAT}: stream from {} ended",
                        request.url
                    ))))
                    .await
                    .is_err()
                {
                    BodyEnd::ConsumerGone
                } else {
                    BodyEnd::Reconnect
                };
            }
            Err(e) => {
                request.metrics.error("transport");
                let _ = frames.close().await;
                let url = &request.url;
                return if tx
                    .send(Err(SaciError::generic(format!(
                        "{WHAT}: cannot read {url}: {}",
                        e.message()
                    ))))
                    .await
                    .is_err()
                {
                    BodyEnd::ConsumerGone
                } else {
                    BodyEnd::Reconnect
                };
            }
        };

        request.metrics.bytes("in", chunk.len() as u64);
        let mut drained: Vec<Vec<u8>> = Vec::new();
        // The cap is one frame's, and a chunk carries as many frames as the
        // transport happened to hand over, so it is judged on what draining
        // leaves behind.
        let refused = match events {
            None => {
                lines.push(&chunk);
                let mut refused = false;
                loop {
                    match lines.next() {
                        // A blank line separates records for a human, not for
                        // a decoder: no format reads one as a message.
                        Ok(Some(line)) => {
                            if !line.is_empty() {
                                drained.push(line);
                            }
                        }
                        Ok(None) => break,
                        Err(OverCap) => {
                            refused = true;
                            break;
                        }
                    }
                }
                refused || lines.over_cap()
            }
            Some(filter) => {
                sse.push(&chunk);
                let mut refused = false;
                loop {
                    match sse.next() {
                        Ok(Some(event)) => {
                            let wanted = match filter {
                                None => true,
                                Some(name) => event.event.as_deref() == Some(name.as_str()),
                            };
                            if wanted && !event.data.is_empty() {
                                drained.push(event.data);
                            }
                        }
                        Ok(None) => break,
                        Err(OverCap) => {
                            refused = true;
                            break;
                        }
                    }
                }
                // Both belong to the stream: the server's reconnect advice,
                // and the id the next request echoes. An empty id asks for no
                // `last-event-id` at all.
                if let Some(retry) = sse.take_retry() {
                    *reconnect = retry;
                }
                if let Some(id) = sse.last_id() {
                    *last_event_id = if id.is_empty() {
                        None
                    } else {
                        HeaderValue::from_str(id).ok()
                    };
                }
                refused || sse.over_cap()
            }
        };

        if let Err(end) = frames.push(drained).await {
            return end;
        }
        if refused {
            // Every frame before the oversized one is already handed over.
            if let Err(end) = frames.close().await {
                return end;
            }
            return over_cap(request, tx, cap).await;
        }
    }
}

/// The decode half of one live body: the message decoder, the window it is
/// filling, and where a finished batch goes.
///
/// The sends are bounded, so a consumer that falls behind stalls this read and
/// the transport's own flow control stalls the server.
struct Frames<'a> {
    metrics: &'a Instruments,
    transformer: &'a Arc<dyn Transformer>,
    schema: &'a Arc<Schema>,
    tx: &'a mpsc::Sender<Result<RecordBatch, SaciError>>,
    window: Window,
    decoder: Box<dyn saci_transformer::MessageDecoder>,
    /// Frames pushed since the last close.
    pushed: usize,
    /// When the open window's `poll_timeout` runs out, `None` while the
    /// window is empty.
    deadline: Option<Instant>,
}

impl<'a> Frames<'a> {
    /// Open the decoder one body's frames are read through.
    ///
    /// # Errors
    ///
    /// Returns the format's own error when the decoder cannot be built.
    fn open(
        metrics: &'a Instruments,
        transformer: &'a Arc<dyn Transformer>,
        schema: &'a Arc<Schema>,
        tx: &'a mpsc::Sender<Result<RecordBatch, SaciError>>,
        window: Window,
    ) -> Result<Self, SaciError> {
        Ok(Self {
            metrics,
            transformer,
            schema,
            tx,
            window,
            decoder: transformer.open_message_decoder(Arc::clone(schema))?,
            pushed: 0,
            deadline: None,
        })
    }

    /// Decode every frame one chunk yielded, closing a window each time it
    /// reaches `batch_size`.
    ///
    /// # Errors
    ///
    /// Returns [`BodyEnd::ConsumerGone`] once the source is gone, and
    /// [`BodyEnd::Reconnect`] when a decoder cannot be opened again.
    async fn push(&mut self, frames: Vec<Vec<u8>>) -> Result<(), BodyEnd> {
        for frame in frames {
            if let Err(e) = self.decoder.push(&frame) {
                self.metrics.error("decode");
                self.refused(e).await?;
                continue;
            }
            self.pushed += 1;
            if self.deadline.is_none() {
                self.deadline = Some(Instant::now() + self.window.poll_timeout);
            }
            if self.pushed >= self.window.batch_size {
                self.close().await?;
            }
        }
        Ok(())
    }

    /// Flush the open window and hand its batch over.
    ///
    /// # Errors
    ///
    /// Returns [`BodyEnd::ConsumerGone`] once the source is gone, and
    /// [`BodyEnd::Reconnect`] when a decoder cannot be opened again.
    async fn close(&mut self) -> Result<(), BodyEnd> {
        self.deadline = None;
        if self.pushed == 0 {
            return Ok(());
        }
        self.pushed = 0;
        match self.decoder.flush() {
            Ok(None) => Ok(()),
            Ok(Some(batch)) => self.send(Ok(batch)).await,
            Err(e) => {
                self.metrics.error("decode");
                self.send(Err(e)).await?;
                self.reopen().await
            }
        }
    }

    /// Report a frame the decoder refused without dropping the window.
    ///
    /// A decoder that read a partial record keeps it: ndjson's `arrow-json`
    /// tape refuses every later frame and its own flush after one, so the
    /// window's rows reach the consumer only for a decoder that can still
    /// flush, and a flush error here would be this one frame reported twice.
    /// Either way the decoder is replaced, so the next frame is read against
    /// a clean one.
    async fn refused(&mut self, e: SaciError) -> Result<(), BodyEnd> {
        if self.pushed > 0
            && let Ok(Some(batch)) = self.decoder.flush()
        {
            self.send(Ok(batch)).await?;
        }
        self.pushed = 0;
        self.deadline = None;
        self.send(Err(e)).await?;
        self.reopen().await
    }

    /// Replace the decoder, which a refused frame leaves holding part of one.
    async fn reopen(&mut self) -> Result<(), BodyEnd> {
        match self
            .transformer
            .open_message_decoder(Arc::clone(self.schema))
        {
            Ok(decoder) => {
                self.decoder = decoder;
                Ok(())
            }
            Err(e) => {
                self.metrics.error("decode");
                self.send(Err(e)).await?;
                Err(BodyEnd::Reconnect)
            }
        }
    }

    /// Hand one outcome to the consumer.
    async fn send(&mut self, result: Result<RecordBatch, SaciError>) -> Result<(), BodyEnd> {
        self.tx
            .send(result)
            .await
            .map_err(|_| BodyEnd::ConsumerGone)
    }
}

/// Report a frame that outgrew `max_body_bytes` and end this body.
async fn over_cap(
    request: &Arc<Request>,
    tx: &mpsc::Sender<Result<RecordBatch, SaciError>>,
    cap: Option<usize>,
) -> BodyEnd {
    request.metrics.error("size");
    let cap = cap.unwrap_or_default();
    let url = &request.url;
    if tx
        .send(Err(SaciError::generic(format!(
            "{WHAT}: a frame from {url} exceeds max_body_bytes ({cap})"
        ))))
        .await
        .is_err()
    {
        BodyEnd::ConsumerGone
    } else {
        BodyEnd::Reconnect
    }
}

#[async_trait]
impl Source for HttpSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        let Self {
            request,
            transformer,
            declared,
            schema_from,
            schema,
            max_body_bytes,
            engine,
        } = self;

        match engine {
            Engine::Stream(state) => {
                if state.batches.is_none() {
                    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                    state.batches = Some(rx);
                    state.task = Some(tokio::spawn(read_stream(
                        Arc::clone(request),
                        Arc::clone(transformer),
                        Arc::clone(schema),
                        state.window,
                        state.events.clone(),
                        *max_body_bytes,
                        tx,
                    )));
                }
                let rx = state.batches.as_mut().expect("the receiver was just set");
                // `None` means the reader task is gone, which is terminal.
                match rx.recv().await {
                    Some(result) => result.map(Some),
                    None => Ok(None),
                }
            }
            Engine::Document(state) => loop {
                if state.batches.is_none() {
                    let first = state.next_page.is_none();
                    let url = state
                        .next_page
                        .take()
                        .unwrap_or_else(|| request.url.clone());
                    let sent = if first {
                        validators(state)
                    } else {
                        HeaderMap::new()
                    };
                    let page = fetch_page(
                        request,
                        Decode {
                            transformer: Arc::clone(transformer),
                            declared: match schema_from {
                                SchemaFrom::Config => declared.clone(),
                                SchemaFrom::Body => None,
                            },
                            check: *schema_from == SchemaFrom::Body,
                            expected: Arc::clone(schema),
                            cap: *max_body_bytes,
                        },
                        &url,
                        &sent,
                    )
                    .await?;
                    let Some(page) = page else {
                        // 304: the resource has not changed, so this read is
                        // over and the stored validators stand.
                        return Ok(None);
                    };
                    if first {
                        state.estimated_rows = page.estimated_rows;
                        if page.etag.is_some() || page.last_modified.is_some() {
                            state.etag = page.etag;
                            state.last_modified = page.last_modified;
                        }
                    }
                    if state.pagination == Pagination::Link {
                        state.next_page = page.next_page;
                    }
                    state.batches = Some(page.batches);
                }
                let rx = state.batches.as_mut().expect("the receiver was just set");
                match rx.recv().await {
                    Some(Ok(batch)) => return Ok(Some(batch)),
                    Some(Err(e)) => return Err(e),
                    None => {
                        // This page is drained. Another page continues the
                        // same read; otherwise the read is over and dropping
                        // the receiver re-arms the next one, which is what
                        // makes `interval` re-read the resource per tick.
                        state.batches = None;
                        if state.next_page.is_some() {
                            continue;
                        }
                        return Ok(None);
                    }
                }
            },
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        match &self.engine {
            Engine::Document(state) => state.estimated_rows,
            // A stream has no total to estimate.
            Engine::Stream(_) => None,
        }
    }
}

impl Drop for HttpSource {
    fn drop(&mut self) {
        if let Engine::Stream(state) = &mut self.engine
            && let Some(task) = state.task.take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A decoder that refuses one payload and can still flush the rows it
    /// took before it, which is what the stream's contract asks of a format.
    #[derive(Default)]
    struct CleanDecoder {
        rows: Vec<i64>,
    }

    impl saci_transformer::MessageDecoder for CleanDecoder {
        fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
            let text = String::from_utf8_lossy(payload);
            let row: i64 = text
                .parse()
                .map_err(|_| SaciError::generic(format!("stub: '{text}' is not a row")))?;
            self.rows.push(row);
            Ok(())
        }

        fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            if self.rows.is_empty() {
                return Ok(None);
            }
            let taken = std::mem::take(&mut self.rows);
            let schema = stub_schema();
            RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow_array::Int64Array::from(taken)) as arrow_array::ArrayRef],
            )
            .map(Some)
            .map_err(|e| SaciError::generic(e.to_string()))
        }
    }

    struct CleanFormat;

    impl Transformer for CleanFormat {
        fn format(&self) -> &'static str {
            "stub"
        }

        fn open_message_decoder(
            &self,
            _schema: Arc<Schema>,
        ) -> Result<Box<dyn saci_transformer::MessageDecoder>, SaciError> {
            Ok(Box::new(CleanDecoder::default()))
        }
    }

    fn stub_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]))
    }

    fn frames(text: &[&str]) -> Vec<Vec<u8>> {
        text.iter().map(|line| line.as_bytes().to_vec()).collect()
    }

    /// Every row of the window is a row the consumer takes: a frame the
    /// decoder refuses hands over what came before it and reports itself,
    /// rather than dropping a window of up to `batch_size` rows.
    #[tokio::test]
    async fn a_refused_frame_hands_over_the_rows_before_it() {
        let metrics = Instruments::new("source", None);
        let transformer: Arc<dyn Transformer> = Arc::new(CleanFormat);
        let schema = stub_schema();
        let (tx, mut rx) = mpsc::channel(8);
        let window = Window {
            batch_size: 1_000,
            ..Window::default()
        };

        let mut decode = Frames::open(&metrics, &transformer, &schema, &tx, window).expect("open");
        decode
            .push(frames(&["1", "2", "3", "oops", "5", "6"]))
            .await
            .expect("the consumer is alive");
        decode.close().await.expect("the consumer is alive");

        let first = rx.recv().await.expect("a batch").expect("rows 1 to 3");
        assert_eq!(first.num_rows(), 3, "the rows before the bad frame");
        let refused = rx.recv().await.expect("an outcome");
        assert!(refused.is_err(), "the bad frame is reported");
        let last = rx.recv().await.expect("a batch").expect("rows 5 and 6");
        assert_eq!(last.num_rows(), 2, "the rows after the bad frame");
    }

    /// `arrow-json`'s tape keeps the partial record a failed decode left, so
    /// the decoder is replaced: one bad line costs its own window, not every
    /// frame of a body that never ends.
    #[tokio::test]
    async fn a_refused_frame_does_not_end_the_decode() {
        let metrics = Instruments::new("source", None);
        let transformer: Arc<dyn Transformer> =
            Arc::new(saci_transformer_ndjson::NdjsonTransformer::default());
        let schema = stub_schema();
        let (tx, mut rx) = mpsc::channel(8);

        let mut decode =
            Frames::open(&metrics, &transformer, &schema, &tx, Window::default()).expect("open");
        decode
            .push(frames(&[r#"{"id":1}"#, "not json", r#"{"id":2}"#]))
            .await
            .expect("the consumer is alive");
        decode.close().await.expect("the consumer is alive");

        let refused = rx.recv().await.expect("an outcome");
        assert!(refused.is_err(), "the bad line is reported once");
        let batch = rx.recv().await.expect("a batch").expect("the row after it");
        assert_eq!(batch.num_rows(), 1);
        assert!(rx.try_recv().is_err(), "one error, one batch, nothing else");
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                reqwest::header::HeaderName::try_from(*name).expect("name"),
                HeaderValue::from_str(value).expect("value"),
            );
        }
        map
    }

    #[test]
    fn a_relative_next_link_resolves_against_the_page() {
        let map = headers(&[("link", "</data?page=2>; rel=\"next\"")]);
        assert_eq!(
            next_link(&map, "https://host/data?page=1").expect("parse"),
            Some("https://host/data?page=2".to_string())
        );
    }

    #[test]
    fn the_next_relation_is_picked_out_of_a_list() {
        let map = headers(&[(
            "link",
            "<https://host/a>; rel=\"prev\", <https://host/c>; rel=\"last next\"; title=\"x\"",
        )]);
        assert_eq!(
            next_link(&map, "https://host/b").expect("parse"),
            Some("https://host/c".to_string())
        );
    }

    #[test]
    fn an_unquoted_rel_is_read_too() {
        let map = headers(&[("link", "<https://host/c>; rel=next")]);
        assert_eq!(
            next_link(&map, "https://host/b").expect("parse"),
            Some("https://host/c".to_string())
        );
    }

    #[test]
    fn a_response_with_no_next_relation_ends_the_read() {
        let map = headers(&[("link", "<https://host/a>; rel=\"prev\"")]);
        assert_eq!(next_link(&map, "https://host/b").expect("parse"), None);
        assert_eq!(
            next_link(&HeaderMap::new(), "https://host/b").expect("parse"),
            None
        );
    }

    /// Two `Link` fields are as legal as one field with two entries.
    #[test]
    fn repeated_link_fields_are_both_read() {
        let map = headers(&[
            ("link", "<https://host/a>; rel=\"prev\""),
            ("link", "<https://host/c>; rel=\"next\""),
        ]);
        assert_eq!(
            next_link(&map, "https://host/b").expect("parse"),
            Some("https://host/c".to_string())
        );
    }

    #[test]
    fn validators_are_only_sent_when_conditional_is_on() {
        let mut state = DocumentState {
            pagination: Pagination::None,
            conditional: false,
            batches: None,
            next_page: None,
            etag: Some(HeaderValue::from_static("\"v1\"")),
            last_modified: Some(HeaderValue::from_static("Tue, 01 Jan 2030 00:00:00 GMT")),
            estimated_rows: None,
        };
        assert!(validators(&state).is_empty());

        state.conditional = true;
        let sent = validators(&state);
        assert_eq!(sent.get("if-none-match").expect("etag"), "\"v1\"");
        assert!(sent.contains_key("if-modified-since"));
    }
}
