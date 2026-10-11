//! [`OpenTelemetryMetricsSource`]: OTLP/HTTP metrics into a pipeline.
//!
//! Two read strategies sit behind one connector. The push mode binds a
//! listener and answers OTLP/HTTP requests; the pull mode reads a url that
//! answers one body. Both flatten what they read into the Arrow rows
//! [`crate::schema`] defines, so a pipeline sees the same shape
//! whichever one feeds it.
//!
//! The push mode follows the OTLP/HTTP specification for its answers: `200`
//! with an `ExportMetricsServiceResponse` for a body it took, `400` for a body
//! it could not read, `401` for a missing or wrong bearer token, `404` for
//! another path, `405` for another method, `413` over `max_body_bytes`, and
//! `415` for a `content-type` that names neither encoding. A body it took in
//! part answers `200` with `partial_success` naming what it dropped, which is
//! how OTLP tells a sender about data the receiver cannot represent without
//! rejecting the rest of the request. A body it could not read is also handed
//! to the runner as an error, so a source delivering undecodable payloads
//! shows up in a run's counters rather than only in a log.
//!
//! A `stop_at_end` pull, and any read that fails, reach EOF or raise an error
//! the runner retries under its own policy, the same way every other source
//! behaves.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use saci_core::error::SaciError;
use saci_core::io::source::Source;
use subtle::ConstantTimeEq as _;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::{
    AuthConfig, Encoding, OpenTelemetryMetricsSourceConfig, PullMode, PushMode, SourceModeConfig,
};
use crate::schema::{self, RejectedPoints};

/// The connector name every error and response body of this half carries.
const WHAT: &str = "OpenTelemetryMetricsSource";

/// How long a connection has to finish its protocol before hyper sees it.
///
/// A peer that opens a socket and sends nothing would otherwise hold its
/// handler, and the ingest channel with it, for as long as it keeps the socket
/// open: `hyper_util`'s auto listener reads the first bytes with no deadline
/// of its own.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// The HTTP/2 connection preface, which a client sends before its first frame.
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// A listening OTLP/HTTP metrics source, or a url it reads on an interval.
///
/// # Example
///
/// ```rust,no_run
/// use saci_connector_opentelemetry::{
///     OpenTelemetryMetricsSource, OpenTelemetryMetricsSourceConfig,
/// };
///
/// let source = OpenTelemetryMetricsSource::new(
///     OpenTelemetryMetricsSourceConfig {
///         bind: Some("127.0.0.1:0".to_string()),
///         ..Default::default()
///     },
///     None,
/// )
/// .expect("the listener binds");
/// println!("listening on {}", source.local_addr().expect("push mode"));
/// ```
pub struct OpenTelemetryMetricsSource {
    /// The schema every batch carries: the declared one, or the canonical one.
    schema: Arc<Schema>,
    /// The declared schema, which the rows are projected onto.
    target: Option<Arc<Schema>>,
    state: State,
}

enum State {
    Push {
        /// The bound socket, until the first read adopts it into the runtime.
        listener: Option<std::net::TcpListener>,
        local_addr: SocketAddr,
        handler: Arc<PushHandler>,
        rx: mpsc::Receiver<Result<RecordBatch, SaciError>>,
        tasks: Vec<JoinHandle<()>>,
        /// Why the accept loop never started, reported by every read.
        listen_failure: Option<SaciError>,
    },
    Pull(Box<PullState>),
}

/// The pull mode's live state.
struct PullState {
    client: reqwest::Client,
    url: String,
    headers: HeaderMap,
    interval: Duration,
    stop_at_end: bool,
    max_body_bytes: usize,
    queued: VecDeque<RecordBatch>,
    /// Whether a body has been read already, which is what makes the interval
    /// a gap between reads rather than a delay before the first one.
    read_before: bool,
    /// Whether a `stop_at_end` read has happened, after which the next read
    /// reports EOF.
    done: bool,
}

/// What every accepted request is judged and decoded against.
struct PushHandler {
    path: String,
    max_body_bytes: usize,
    bearer: Option<Vec<u8>>,
    target: Option<Arc<Schema>>,
    tx: mpsc::Sender<Result<RecordBatch, SaciError>>,
}

/// One decided response.
struct Answer {
    status: StatusCode,
    body: Vec<u8>,
    /// The `content-type` of the body, or `None` for the `text/plain` one
    /// every refusal carries.
    encoding: Option<Encoding>,
    /// `true` on a `405`, which carries `allow`.
    allow: bool,
}

impl Answer {
    /// A refusal whose body is the reason, as `text/plain`.
    fn refusal(status: StatusCode, body: String, allow: bool) -> Self {
        Self {
            status,
            body: body.into_bytes(),
            encoding: None,
            allow,
        }
    }
}

/// How reading one request body ended.
enum BodyRead {
    Whole(Bytes),
    Over,
    Failed(String),
}

impl OpenTelemetryMetricsSource {
    /// Build the source, binding a push mode's socket and opening a pull
    /// mode's client.
    ///
    /// `target` is the Arrow schema the config declared, which every batch is
    /// built to; `None` emits [`schema::metrics_schema`]. The listener binds
    /// here rather than on the first read, so a port already in use is a
    /// configuration error at load time.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the config names no usable
    /// mode, when a push mode's address cannot be bound, or when a pull mode's
    /// headers are not valid HTTP header pairs.
    pub fn new(
        config: OpenTelemetryMetricsSourceConfig,
        target: Option<Arc<Schema>>,
    ) -> Result<Self, SaciError> {
        let mode = config.resolve_mode(WHAT)?;
        let schema = target
            .as_ref()
            .map_or_else(schema::metrics_schema, Arc::clone);
        let state = match mode {
            SourceModeConfig::Push(push) => {
                let bound = bind_listener(&push, target.clone())?;
                State::Push {
                    listener: Some(bound.listener),
                    local_addr: bound.local_addr,
                    handler: bound.handler,
                    rx: bound.rx,
                    tasks: Vec::new(),
                    listen_failure: None,
                }
            }
            SourceModeConfig::Pull(pull) => State::Pull(Box::new(pull_state(&pull)?)),
        };
        Ok(Self {
            schema,
            target,
            state,
        })
    }

    /// The bound address of a push source, `None` for a pull source.
    ///
    /// An ephemeral port is resolved here, so a test can bind port zero and
    /// read back the port the kernel chose.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        match &self.state {
            State::Push { local_addr, .. } => Some(*local_addr),
            State::Pull(_) => None,
        }
    }

    /// Adopt a push mode's socket into the runtime and spawn its accept loop.
    ///
    /// Idempotent. The accept loop is spawned on the first read rather than in
    /// the constructor, because a constructor runs outside the runtime that
    /// serves the connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the bound socket cannot be adopted
    /// into the runtime. The failure is kept and reported by every later read:
    /// with no accept loop running, no request can arrive and a read would wait
    /// forever.
    fn ensure_listening(&mut self) -> Result<(), SaciError> {
        let State::Push {
            listener,
            handler,
            tasks,
            listen_failure,
            ..
        } = &mut self.state
        else {
            return Ok(());
        };
        if let Some(failure) = listen_failure {
            return Err(failure.clone());
        }
        let Some(std_listener) = listener.take() else {
            return Ok(());
        };
        let listener = tokio::net::TcpListener::from_std(std_listener).map_err(|e| {
            let failure = SaciError::generic(format!(
                "{WHAT}: cannot adopt the listener into the runtime: {e}"
            ));
            *listen_failure = Some(failure.clone());
            failure
        })?;
        tasks.push(tokio::spawn(accept(listener, Arc::clone(handler))));
        Ok(())
    }
}

/// Read one body from the pull mode's url and queue the rows it held.
///
/// # Errors
///
/// Returns [`SaciError::Store`] when the request fails or the url answers a
/// non-2xx status, [`SaciError::Configuration`] when the response names a
/// `content-type` neither encoding matches, and [`SaciError::Generic`] when
/// the body is not an OTLP request or is over `max_body_bytes`.
async fn fetch(pull: &mut PullState, target: Option<&Schema>) -> Result<(), SaciError> {
    let response = pull
        .client
        .get(&pull.url)
        .headers(pull.headers.clone())
        .send()
        .await
        .map_err(|e| SaciError::store(format!("{WHAT}: {} did not answer: {e}", pull.url)))?;
    let status = response.status();
    if !status.is_success() {
        return Err(SaciError::store(format!(
            "{WHAT}: {} answered {status}",
            pull.url
        )));
    }
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let Some(encoding) = schema::encoding_for_media_type(&content_type) else {
        return Err(SaciError::configuration(format!(
            "{WHAT}: {} answered content-type '{content_type}', and this connector reads {} and {}",
            pull.url,
            schema::CONTENT_TYPE_PROTOBUF,
            schema::CONTENT_TYPE_JSON
        )));
    };
    let body = read_capped(response, pull.max_body_bytes).await?;
    let request = schema::decode(encoding, WHAT, &body)?;
    let (batch, conversion) = schema::export_request_to_record_batches(&request, target)?;
    report_rejected(conversion.rejected.as_ref());
    if batch.num_rows() > 0 {
        pull.queued.push_back(batch);
    }
    Ok(())
}

/// Bind the push mode's socket, without adopting it into a runtime.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the address cannot be bound.
fn bind_listener(push: &PushMode, target: Option<Arc<Schema>>) -> Result<PushListener, SaciError> {
    let listener = std::net::TcpListener::bind(&push.bind).map_err(|e| {
        SaciError::configuration(format!("{WHAT}: cannot bind '{}': {e}", push.bind))
    })?;
    listener.set_nonblocking(true).map_err(|e| {
        SaciError::configuration(format!("{WHAT}: set_nonblocking on '{}': {e}", push.bind))
    })?;
    let local_addr = listener.local_addr().map_err(|e| {
        SaciError::configuration(format!("{WHAT}: local_addr on '{}': {e}", push.bind))
    })?;
    let (tx, rx) = mpsc::channel(push.buffer);
    let handler = Arc::new(PushHandler {
        path: push.path.clone(),
        max_body_bytes: push.max_body_bytes,
        bearer: push
            .auth
            .as_ref()
            .map(|auth: &AuthConfig| auth.bearer.clone().into_bytes()),
        target,
        tx,
    });
    Ok(PushListener {
        listener,
        local_addr,
        handler,
        rx,
    })
}

/// What binding a push listener produced.
struct PushListener {
    listener: std::net::TcpListener,
    local_addr: SocketAddr,
    handler: Arc<PushHandler>,
    rx: mpsc::Receiver<Result<RecordBatch, SaciError>>,
}

/// Build the pull mode's client and state.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a header is not a valid pair or
/// the client cannot be built.
fn pull_state(pull: &PullMode) -> Result<PullState, SaciError> {
    crate::install_ring();
    let mut headers = HeaderMap::with_capacity(pull.headers.len());
    for (name, value) in &pull.headers {
        let name = header::HeaderName::try_from(name.as_str()).map_err(|e| {
            SaciError::configuration(format!("{WHAT}: '{name}' is not a valid header name: {e}"))
        })?;
        let value = header::HeaderValue::try_from(value.as_str()).map_err(|e| {
            SaciError::configuration(format!("{WHAT}: header '{name}' has an invalid value: {e}"))
        })?;
        headers.append(name, value);
    }
    let client = reqwest::Client::builder()
        // A pull reads whole bodies, one after another, so the timeout is the
        // whole exchange rather than an idle gap.
        .timeout(Duration::from_millis(30_000))
        .build()
        .map_err(|e| {
            SaciError::configuration(format!("{WHAT}: cannot build an HTTP client: {e}"))
        })?;
    Ok(PullState {
        client,
        url: pull.url.clone(),
        headers,
        interval: Duration::from_millis(pull.interval_ms),
        stop_at_end: pull.stop_at_end,
        max_body_bytes: pull.max_body_bytes,
        queued: VecDeque::new(),
        read_before: false,
        done: false,
    })
}

/// Read a response body, refusing one over `max_bytes`.
///
/// # Errors
///
/// Returns [`SaciError::Store`] when the body stops early, and
/// [`SaciError::Generic`] when it is over the cap.
async fn read_capped(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Bytes, SaciError> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| SaciError::store(format!("{WHAT}: the body stopped early: {e}")))?
    {
        if body.len() + chunk.len() > max_bytes {
            return Err(SaciError::generic(format!(
                "{WHAT}: the body is over max_body_bytes ({max_bytes})"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

/// Report the data points one conversion could not carry.
fn report_rejected(rejected: Option<&RejectedPoints>) {
    let Some(rejected) = rejected else {
        return;
    };
    #[cfg(feature = "tracing")]
    tracing::warn!(
        rejected = rejected.count,
        reason = %rejected.reason,
        "{WHAT}: data points the Arrow view cannot represent were dropped"
    );
    #[cfg(not(feature = "tracing"))]
    let _ = rejected;
}

#[async_trait]
impl Source for OpenTelemetryMetricsSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if matches!(self.state, State::Push { .. }) {
            self.ensure_listening()?;
        }
        match &mut self.state {
            State::Push { rx, .. } => match rx.recv().await {
                Some(Ok(batch)) => Ok(Some(batch)),
                Some(Err(e)) => Err(e),
                // Every sender is gone, which only happens once the handler
                // itself has been dropped, and is terminal.
                None => Ok(None),
            },
            State::Pull(pull) => {
                let target = self.target.as_deref();
                loop {
                    if let Some(batch) = pull.queued.pop_front() {
                        return Ok(Some(batch));
                    }
                    if pull.done {
                        return Ok(None);
                    }
                    if pull.read_before {
                        let interval = pull.interval;
                        tokio::time::sleep(interval).await;
                    }
                    fetch(pull, target).await?;
                    pull.read_before = true;
                    if pull.stop_at_end {
                        pull.done = true;
                    }
                }
            }
        }
    }
}

impl Drop for OpenTelemetryMetricsSource {
    fn drop(&mut self) {
        if let State::Push { tasks, .. } = &mut self.state {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
        // A per-connection task ends on its next `tx.send`, which fails once
        // the receiver is dropped with this struct.
    }
}

/// Accept requests forever, serving each connection on its own task.
async fn accept(listener: tokio::net::TcpListener, handler: Arc<PushHandler>) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "{WHAT}: accept failed");
                #[cfg(not(feature = "tracing"))]
                let _ = _e;
                continue;
            }
        };
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let mut builder =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            // hyper's HTTP/1 header read timeout defaults to 30 seconds but
            // needs a timer to arm: with none, an idle keep-alive connection
            // holds this handler, and the ingest channel with it, until the
            // peer closes the socket.
            builder.http1().timer(hyper_util::rt::TokioTimer::new());
            builder.http2().timer(hyper_util::rt::TokioTimer::new());
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let handler = Arc::clone(&handler);
                async move { Ok::<_, std::convert::Infallible>(serve_hyper(&handler, request).await) }
            });
            if !preface_settled(&stream).await {
                #[cfg(feature = "tracing")]
                tracing::debug!("{WHAT}: dropped a connection that sent no request");
                return;
            }
            let served = builder
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                .await;
            if let Err(_e) = served {
                #[cfg(feature = "tracing")]
                tracing::debug!(error = %_e, "{WHAT}: connection ended");
                #[cfg(not(feature = "tracing"))]
                let _ = _e;
            }
        });
    }
}

/// Whether more bytes could still turn `peeked` into the HTTP/2 preface, which
/// is exactly when the protocol is undecided.
fn undecided_preface(peeked: &[u8]) -> bool {
    peeked.len() < H2_PREFACE.len() && H2_PREFACE.starts_with(peeked)
}

/// Wait until the protocol sniff can decide, or drop the connection.
async fn preface_settled(stream: &tokio::net::TcpStream) -> bool {
    let mut peeked = [0u8; H2_PREFACE.len()];
    let deadline = tokio::time::Instant::now() + HANDSHAKE_TIMEOUT;
    loop {
        match tokio::time::timeout_at(deadline, stream.peek(&mut peeked)).await {
            // Zero bytes is the peer closing, and an error or the deadline is
            // a connection there is no point serving.
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return false,
            Ok(Ok(read)) if !undecided_preface(&peeked[..read]) => return true,
            Ok(Ok(_)) => {}
        }
        if tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(50)))
            .await
            .is_err()
        {
            return false;
        }
    }
}

/// Read one request, judge it, and build its response.
async fn serve_hyper(
    handler: &Arc<PushHandler>,
    request: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();

    if let Some(answer) = handler.reject(&method, &path, request.headers()) {
        return handler.respond(answer);
    }
    if handler.announced_too_large(request.headers()) {
        let answer = handler.too_large();
        return handler.respond(answer);
    }
    let Some(encoding) = schema::encoding_for_media_type(&content_type) else {
        let answer = handler.unsupported_media_type(&content_type);
        return handler.respond(answer);
    };
    match read_body(request.into_body(), handler.max_body_bytes).await {
        BodyRead::Whole(body) => {
            let answer = handler.accept(encoding, &body).await;
            handler.respond(answer)
        }
        BodyRead::Over => {
            let answer = handler.too_large();
            handler.respond(answer)
        }
        BodyRead::Failed(reason) => {
            let answer = Answer::refusal(
                StatusCode::BAD_REQUEST,
                format!("{WHAT}: the request body could not be read: {reason}"),
                false,
            );
            handler.respond(answer)
        }
    }
}

/// Read one request body, refusing one over `max_bytes` without buffering it.
async fn read_body(body: Incoming, max_bytes: usize) -> BodyRead {
    let mut body = body;
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(e) => return BodyRead::Failed(e.to_string()),
        };
        let Some(chunk) = frame.data_ref() else {
            continue;
        };
        if bytes.len() + chunk.len() > max_bytes {
            return BodyRead::Over;
        }
        bytes.extend_from_slice(chunk);
    }
    BodyRead::Whole(Bytes::from(bytes))
}

impl PushHandler {
    /// The response to refuse this request with, or `None` to read its body.
    ///
    /// Checked in order: path, method, token. A `404` before a `405` means an
    /// unknown path never advertises which methods the real one takes.
    fn reject(&self, method: &Method, path: &str, headers: &HeaderMap) -> Option<Answer> {
        if path != self.path {
            return Some(Answer::refusal(
                StatusCode::NOT_FOUND,
                format!("{WHAT}: no such path"),
                false,
            ));
        }
        if method != Method::POST {
            return Some(Answer::refusal(
                StatusCode::METHOD_NOT_ALLOWED,
                format!("{WHAT}: {method} is not accepted here"),
                true,
            ));
        }
        if let Some(expected) = &self.bearer {
            // `ct_eq` on slices of different lengths is already `false`, and a
            // token's length is not the secret.
            let presented = bearer_token(headers);
            let ok = presented.is_some_and(|token| bool::from(token.ct_eq(expected.as_slice())));
            if !ok {
                return Some(Answer::refusal(
                    StatusCode::UNAUTHORIZED,
                    format!("{WHAT}: bearer token missing or wrong"),
                    false,
                ));
            }
        }
        None
    }

    /// Whether the announced `content-length` alone is over the cap, which
    /// refuses the body before any of it is read.
    fn announced_too_large(&self, headers: &HeaderMap) -> bool {
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|announced| announced > self.max_body_bytes as u64)
    }

    /// The `413` an oversized body is answered with.
    fn too_large(&self) -> Answer {
        Answer::refusal(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "{WHAT}: the body is over max_body_bytes ({})",
                self.max_body_bytes
            ),
            false,
        )
    }

    /// The `415` a body of a media type this connector does not read is
    /// answered with.
    fn unsupported_media_type(&self, content_type: &str) -> Answer {
        Answer::refusal(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!(
                "{WHAT}: content-type '{content_type}' is neither {} nor {}",
                schema::CONTENT_TYPE_PROTOBUF,
                schema::CONTENT_TYPE_JSON
            ),
            false,
        )
    }

    /// Decode one whole body, queue every row it held, and answer it.
    ///
    /// The rows go down the channel in one send, so a body is taken whole or
    /// not at all: a retry of a request this source refused duplicates no row.
    async fn accept(&self, encoding: Encoding, body: &Bytes) -> Answer {
        let request = match schema::decode(encoding, WHAT, body) {
            Ok(request) => request,
            Err(e) => {
                // The runner counts a source error, so a sender posting
                // undecodable bodies is visible without reading the logs.
                let _ = self.tx.send(Err(e.clone())).await;
                return Answer::refusal(StatusCode::BAD_REQUEST, e.message(), false);
            }
        };
        let (batch, conversion) =
            match schema::export_request_to_record_batches(&request, self.target.as_deref()) {
                Ok(converted) => converted,
                Err(e) => {
                    let _ = self.tx.send(Err(e.clone())).await;
                    return Answer::refusal(StatusCode::BAD_REQUEST, e.message(), false);
                }
            };
        report_rejected(conversion.rejected.as_ref());
        // A payload that held no row is still taken: OTLP counts a request
        // that carried nothing as accepted, and a body whose metrics were all
        // empty is one.
        if batch.num_rows() > 0 && self.tx.send(Ok(batch)).await.is_err() {
            return Answer::refusal(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("{WHAT}: the source was dropped"),
                false,
            );
        }
        let response = schema::response_for(&conversion);
        match schema::encode_response(encoding, &response) {
            Ok(body) => Answer {
                status: StatusCode::OK,
                body,
                encoding: Some(encoding),
                allow: false,
            },
            Err(e) => Answer::refusal(StatusCode::INTERNAL_SERVER_ERROR, e.message(), false),
        }
    }

    /// Render one [`Answer`] as the response every connection sends.
    fn respond(&self, answer: Answer) -> Response<Full<Bytes>> {
        let mut response = Response::builder().status(answer.status);
        if !answer.body.is_empty() {
            let content_type = match answer.encoding {
                Some(encoding) => schema::media_type(encoding),
                None => "text/plain; charset=utf-8",
            };
            response = response.header(header::CONTENT_TYPE, content_type);
        }
        if answer.allow {
            response = response.header(header::ALLOW, "POST");
        }
        response
            .body(Full::new(Bytes::from(answer.body)))
            .unwrap_or_else(|_| {
                // Every header above is already a validated value, so this is
                // unreachable; answering 500 beats panicking in a connection task.
                let mut fallback = Response::new(Full::new(Bytes::new()));
                *fallback.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                fallback
            })
    }
}

/// The token an `authorization: Bearer <token>` header presents.
///
/// RFC 7235 makes the scheme case-insensitive and allows more than one space
/// before the token, so both are read leniently; the token itself is compared
/// byte for byte by the caller.
fn bearer_token(headers: &HeaderMap) -> Option<&[u8]> {
    let value = headers.get(header::AUTHORIZATION)?.as_bytes();
    let at = value.iter().position(|byte| *byte == b' ')?;
    let (scheme, token) = value.split_at(at);
    scheme
        .eq_ignore_ascii_case(b"Bearer")
        .then(|| token.trim_ascii_start())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handler(push: PushMode) -> Arc<PushHandler> {
        let listener = bind_listener(&push, None).expect("the listener binds");
        listener.handler
    }

    fn push() -> PushMode {
        PushMode {
            bind: "127.0.0.1:0".to_string(),
            path: crate::config::DEFAULT_PATH.to_string(),
            buffer: crate::config::DEFAULT_BUFFER,
            auth: None,
            max_body_bytes: crate::config::DEFAULT_MAX_BODY_BYTES,
        }
    }

    fn authorization(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, value.parse().expect("a value"));
        headers
    }

    fn post() -> Method {
        Method::POST
    }

    #[test]
    fn a_partial_http2_preface_leaves_the_protocol_undecided() {
        assert!(undecided_preface(b""), "nothing has arrived yet");
        assert!(undecided_preface(b"P"));
        assert!(undecided_preface(b"PRI * HTTP/2.0\r\n\r\nSM"));
        assert!(!undecided_preface(b"GET "), "HTTP/1.1 diverges at the G");
        assert!(!undecided_preface(H2_PREFACE), "the preface is complete");
    }

    #[test]
    fn another_path_or_method_is_refused_before_the_body_is_read() {
        let handler = handler(push());
        let refused = handler
            .reject(&post(), "/v1/traces", &HeaderMap::new())
            .expect("another path is refused");
        assert_eq!(refused.status, StatusCode::NOT_FOUND);
        let refused = handler
            .reject(&Method::GET, crate::config::DEFAULT_PATH, &HeaderMap::new())
            .expect("another method is refused");
        assert_eq!(refused.status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(refused.allow, "a 405 names the method it takes");
        assert!(
            handler
                .reject(&post(), crate::config::DEFAULT_PATH, &HeaderMap::new())
                .is_none(),
            "the standard request is read"
        );
    }

    #[test]
    fn a_bearer_token_is_matched_without_regard_to_case_or_spacing() {
        let mut push = push();
        push.auth = Some(AuthConfig {
            bearer: "s3cret".to_string(),
        });
        let handler = handler(push);
        assert!(
            handler
                .reject(&post(), crate::config::DEFAULT_PATH, &HeaderMap::new())
                .is_some_and(|answer| answer.status == StatusCode::UNAUTHORIZED),
            "no token is refused"
        );
        assert!(
            handler
                .reject(
                    &post(),
                    crate::config::DEFAULT_PATH,
                    &authorization("Bearer s3cret")
                )
                .is_none()
        );
        assert!(
            handler
                .reject(
                    &post(),
                    crate::config::DEFAULT_PATH,
                    &authorization("bearer    s3cret")
                )
                .is_none()
        );
        assert!(
            handler
                .reject(
                    &post(),
                    crate::config::DEFAULT_PATH,
                    &authorization("Bearer other")
                )
                .is_some()
        );
        assert!(
            handler
                .reject(
                    &post(),
                    crate::config::DEFAULT_PATH,
                    &authorization("Basic s3cret")
                )
                .is_some(),
            "another scheme is no token"
        );
    }

    #[test]
    fn an_announced_length_over_the_cap_is_refused_before_the_body() {
        let mut push = push();
        push.max_body_bytes = 10;
        let handler = handler(push);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, "11".parse().expect("a value"));
        assert!(handler.announced_too_large(&headers));
        headers.insert(header::CONTENT_LENGTH, "10".parse().expect("a value"));
        assert!(!handler.announced_too_large(&headers));
    }
}
