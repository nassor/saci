//! [`HttpPushSource`]: a listening HTTP [`Source`] for stream mode.
//!
//! The other three connectors in this crate dial out. This one binds, accepts,
//! and yields one or more [`RecordBatch`]es per request body. It never reaches
//! EOF: `next_batch` blocks until the next request arrives, so only the stream
//! runner (`run_mode kind="stream"`) can drive it, and the service config
//! validator rejects every other run mode.
//!
//! ## Protocol
//!
//! One request is one whole document in the configured format, exactly what
//! [`HttpSink`](crate::HttpSink) sends: a csv with its header row, a block of
//! ndjson, one parquet or avro container. Every batch the document decoded to
//! is queued in one send, so it is accepted whole or not at all, and a retry
//! of a request this source refused, with any 4xx or 5xx, duplicates no rows.
//! Ingest is at-least-once past that: hyper polls a request whose client went
//! away to completion, so a client that stops waiting for its `202`, say on a
//! timeout while the channel is full, cannot know whether its document was
//! queued, and its retry queues that document a second time. `buffer`
//! therefore counts queued documents.
//!
//! One listener serves HTTP/1.1 and HTTP/2 on TCP:
//! `hyper_util::server::conn::auto` sniffs the HTTP/2 connection preface, so
//! h2c prior knowledge and HTTP/1.1 share a port, and ALPN picks between `h2`
//! and `http/1.1` once a `tls` block terminates TLS. Both sub-builders carry a
//! tokio timer, which is what arms hyper's 30 second HTTP/1 header read
//! timeout: without one an idle keep-alive connection would hold its handler,
//! and the ingest channel with it, for as long as the peer keeps the socket
//! open. `quic_bind` adds an HTTP/3 endpoint on its own UDP port, advertised
//! to TCP clients as `alt-svc`.
//!
//! ## Answers
//!
//! `202` for a document that was queued, whether or not it held rows. `404`
//! for another path, `405` for a method `methods` does not list, `401` when
//! `auth { bearer }` is set and the request's token is missing or wrong, `413`
//! for a body over `max_body_bytes`, announced or discovered, `400` for a
//! document the format could not read and for a body the transport could not
//! deliver, and `500` only when the spool file could not be written. Every
//! error body is the reason as `text/plain`, and a document the format refused
//! is also sent down the channel so the runner counts it.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use http_body_util::BodyExt as _;
use subtle::ConstantTimeEq as _;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use saci_connector::NodeIdentity;
use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

use crate::client;
use crate::document;
use crate::metrics::Instruments;
use crate::source::SchemaFrom;

/// The connector name every error and metric label of this half carries.
const WHAT: &str = "HttpPushSource";

/// The certificate and key a push listener terminates TLS with.
///
/// Both are required together: a listener with a certificate and no key cannot
/// complete a handshake.
#[derive(Debug, Clone)]
pub struct ServerTls {
    /// The certificate chain to present, PEM.
    pub cert_file: PathBuf,
    /// The private key for `cert_file`, PEM.
    pub key_file: PathBuf,
}

/// Everything [`HttpPushSource::new`] takes past the bind address and the
/// transformer.
#[derive(Debug, Clone)]
pub struct HttpPushOptions {
    /// The schema every document is read against. Required: the graph is
    /// validated before a request can arrive.
    pub declared: Arc<Schema>,
    /// Whether `declared` is handed to the format or checked against what the
    /// body carried.
    pub schema_from: SchemaFrom,
    /// The one path that accepts documents.
    pub path: String,
    /// The methods that accept documents.
    pub methods: Vec<String>,
    /// The largest request body to accept.
    pub max_body_bytes: usize,
    /// Accepted documents that may queue before backpressure reaches the
    /// client. A document is one channel entry however many batches it held.
    pub buffer: usize,
    /// The certificate and key to terminate TLS with, `None` for cleartext.
    pub tls: Option<ServerTls>,
    /// A UDP address to serve HTTP/3 on. Needs `tls`.
    pub quic_bind: Option<String>,
    /// A bearer token every request must carry.
    pub bearer: Option<String>,
    /// The node this connector is, for metric labels.
    pub identity: Option<NodeIdentity>,
}

impl Default for HttpPushOptions {
    fn default() -> Self {
        Self {
            declared: Arc::new(Schema::empty()),
            schema_from: SchemaFrom::default(),
            path: "/".to_string(),
            methods: vec!["POST".to_string()],
            max_body_bytes: 64 * 1024 * 1024,
            buffer: 64,
            tls: None,
            quic_bind: None,
            bearer: None,
            identity: None,
        }
    }
}

/// What every accepted request is judged and decoded against.
struct Handler {
    path: String,
    methods: Vec<Method>,
    /// `methods` joined for the `allow` header of a `405`.
    allow: HeaderValue,
    max_body_bytes: usize,
    bearer: Option<Vec<u8>>,
    declared: Arc<Schema>,
    schema_from: SchemaFrom,
    transformer: Arc<dyn Transformer>,
    /// Every batch of one document, so a document is queued in one send.
    tx: mpsc::Sender<Result<Vec<RecordBatch>, SaciError>>,
    /// `alt-svc` for the QUIC port, when one is served.
    alt_svc: Option<HeaderValue>,
    metrics: Instruments,
}

/// Listening HTTP source: one request in, its whole document out as batches.
///
/// Both sockets are bound in [`new`](Self::new) synchronously, so a bind
/// failure surfaces at config time. The accept loops are spawned on the first
/// [`next_batch`](Source::next_batch) call, which always runs inside a tokio
/// runtime.
///
/// Backpressure comes from the channel: once `buffer` documents are queued, a
/// request in flight waits for its response and the client waits with it. A
/// retry of a request this source refused, with any 4xx or 5xx, duplicates no
/// rows; a retry after the client stopped waiting for its `202` can, because
/// that request is polled to completion and queues its document without the
/// client.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_http::{HttpPushOptions, HttpPushSource};
/// use saci_transformer_ndjson::NdjsonTransformer;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let source = HttpPushSource::new(
///     "127.0.0.1:0",
///     Arc::new(NdjsonTransformer::default()),
///     HttpPushOptions {
///         declared: schema,
///         path: "/orders".to_string(),
///         ..HttpPushOptions::default()
///     },
/// )
/// .unwrap();
/// println!("listening on {}", source.local_addr());
/// ```
pub struct HttpPushSource {
    listener: Option<std::net::TcpListener>,
    local_addr: SocketAddr,
    quic_socket: Option<std::net::UdpSocket>,
    quic_local_addr: Option<SocketAddr>,
    /// ALPN `h2` then `http/1.1`, for the TCP listener.
    tcp_tls: Option<Arc<rustls::ServerConfig>>,
    /// The same certificate with ALPN `h3`, for the QUIC endpoint.
    quic_tls: Option<Arc<rustls::ServerConfig>>,
    handler: Arc<Handler>,
    schema: Arc<Schema>,
    rx: mpsc::Receiver<Result<Vec<RecordBatch>, SaciError>>,
    /// The batches of the document being handed over, one `next_batch` each.
    queued: VecDeque<RecordBatch>,
    /// Why the accept loops never started, reported by every `next_batch`:
    /// with no loop running, nothing will ever arrive on `rx`.
    listen_failure: Option<SaciError>,
    tasks: Vec<JoinHandle<()>>,
}

impl HttpPushSource {
    /// Bind the listener (and the QUIC socket, when one is asked for) and
    /// prepare the ingest channel.
    ///
    /// The transformer's read surface is not probed here: a stream surface is
    /// probed on the first document, the way [`HttpSink`](crate::HttpSink)
    /// probes its writer.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `bind` or `quic_bind` cannot
    /// be bound, when a method name is not a valid HTTP method, when
    /// `quic_bind` is asked for without `tls`, when a `tls` file is missing or
    /// malformed, or when `methods` is empty.
    pub fn new(
        bind: &str,
        transformer: Arc<dyn Transformer>,
        options: HttpPushOptions,
    ) -> Result<Self, SaciError> {
        let HttpPushOptions {
            declared,
            schema_from,
            path,
            methods,
            max_body_bytes,
            buffer,
            tls,
            quic_bind,
            bearer,
            identity,
        } = options;

        if methods.is_empty() {
            return Err(SaciError::configuration(format!(
                "{WHAT} config.methods must name at least one HTTP method"
            )));
        }
        let mut parsed_methods = Vec::with_capacity(methods.len());
        for method in &methods {
            parsed_methods.push(Method::from_bytes(method.as_bytes()).map_err(|e| {
                SaciError::configuration(format!(
                    "{WHAT}: '{method}' is not a valid HTTP method: {e}"
                ))
            })?);
        }
        let allow = HeaderValue::from_str(&methods.join(", ")).map_err(|e| {
            SaciError::configuration(format!("{WHAT}: config.methods is not a header value: {e}"))
        })?;

        if quic_bind.is_some() && tls.is_none() {
            return Err(SaciError::configuration(format!(
                "{WHAT}: quic_bind needs a 'tls' block: HTTP/3 runs over TLS"
            )));
        }

        client::install_ring();
        let (tcp_tls, quic_tls) = match &tls {
            None => (None, None),
            Some(files) => {
                let base = crate::tls::load_server(WHAT, &files.cert_file, &files.key_file)?;
                let mut tcp = base.clone();
                tcp.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
                let mut quic = base;
                quic.alpn_protocols = vec![b"h3".to_vec()];
                (Some(Arc::new(tcp)), Some(Arc::new(quic)))
            }
        };

        let listener = std::net::TcpListener::bind(bind)
            .map_err(|e| SaciError::configuration(format!("{WHAT}: cannot bind '{bind}': {e}")))?;
        listener.set_nonblocking(true).map_err(|e| {
            SaciError::configuration(format!("{WHAT}: set_nonblocking failed: {e}"))
        })?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| SaciError::configuration(format!("{WHAT}: local_addr failed: {e}")))?;

        let (quic_socket, quic_local_addr) = match &quic_bind {
            None => (None, None),
            Some(address) => {
                let socket = std::net::UdpSocket::bind(address).map_err(|e| {
                    SaciError::configuration(format!(
                        "{WHAT}: cannot bind quic_bind '{address}': {e}"
                    ))
                })?;
                let addr = socket.local_addr().map_err(|e| {
                    SaciError::configuration(format!("{WHAT}: quic local_addr failed: {e}"))
                })?;
                (Some(socket), Some(addr))
            }
        };

        let alt_svc = quic_local_addr
            .map(|addr| format!("h3=\":{}\"; ma=86400", addr.port()))
            .and_then(|value| HeaderValue::from_str(&value).ok());

        let (tx, rx) = mpsc::channel(buffer.max(1));

        Ok(Self {
            listener: Some(listener),
            local_addr,
            quic_socket,
            quic_local_addr,
            tcp_tls,
            quic_tls,
            handler: Arc::new(Handler {
                path,
                methods: parsed_methods,
                allow,
                max_body_bytes,
                bearer: bearer.map(String::into_bytes),
                declared: Arc::clone(&declared),
                schema_from,
                transformer,
                tx,
                alt_svc,
                metrics: Instruments::new("push", identity.as_ref()),
            }),
            schema: declared,
            rx,
            queued: VecDeque::new(),
            listen_failure: None,
            tasks: Vec::new(),
        })
    }

    /// The bound TCP address, with any ephemeral port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The bound QUIC address, `None` when `quic_bind` was not set.
    pub fn quic_local_addr(&self) -> Option<SocketAddr> {
        self.quic_local_addr
    }

    /// Adopt both sockets into the runtime and spawn their accept loops.
    /// Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when a bound socket cannot be adopted
    /// into the runtime, or when the QUIC endpoint cannot be started. The
    /// failure is kept and returned again by every later call: with no accept
    /// loop running, no request can arrive and a read would wait forever.
    fn ensure_listening(&mut self) -> Result<(), SaciError> {
        if let Some(failure) = &self.listen_failure {
            return Err(failure.clone());
        }
        if let Some(std_listener) = self.listener.take() {
            match tokio::net::TcpListener::from_std(std_listener) {
                Ok(listener) => {
                    self.tasks.push(tokio::spawn(accept_tcp(
                        listener,
                        self.tcp_tls.clone(),
                        Arc::clone(&self.handler),
                    )));
                }
                Err(e) => {
                    return Err(self.fail(SaciError::generic(format!(
                        "{WHAT}: cannot adopt the listener into the runtime: {e}"
                    ))));
                }
            }
        }
        if let Some(socket) = self.quic_socket.take()
            && let Some(config) = self.quic_tls.clone()
        {
            match quic_endpoint(socket, config) {
                Ok(endpoint) => {
                    self.tasks.push(tokio::spawn(accept_quic(
                        endpoint,
                        Arc::clone(&self.handler),
                    )));
                }
                Err(e) => return Err(self.fail(e)),
            }
        }
        Ok(())
    }

    /// Keep `e` as the failure every later read reports, and hand it back.
    fn fail(&mut self, e: SaciError) -> SaciError {
        self.listen_failure = Some(e.clone());
        e
    }
}

/// Build the QUIC endpoint over the already-bound UDP socket.
///
/// `Endpoint::new` rather than `Endpoint::server` so the socket is the one
/// bound in the constructor: the port a config named is refused at load, not
/// on the first batch.
fn quic_endpoint(
    socket: std::net::UdpSocket,
    config: Arc<rustls::ServerConfig>,
) -> Result<quinn::Endpoint, SaciError> {
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(config).map_err(|e| {
        SaciError::generic(format!("{WHAT}: this TLS config cannot serve QUIC: {e}"))
    })?;
    quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(quinn::ServerConfig::with_crypto(Arc::new(crypto))),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
    .map_err(|e| SaciError::generic(format!("{WHAT}: cannot start a QUIC endpoint: {e}")))
}

/// How long a connection has to get through the phases before HTTP: the TLS
/// handshake, and the protocol sniff on cleartext. The same 30 seconds hyper
/// gives a request head, which is the deadline that takes over once a
/// connection reaches hyper at all.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the cleartext wait re-peeks at a connection that has sent part of
/// the HTTP/2 preface. Socket readiness is level triggered, so bytes already
/// buffered keep the socket readable and there is nothing else to wake on.
const PREFACE_POLL: Duration = Duration::from_millis(50);

/// The HTTP/2 connection preface, which a client sends before its first frame
/// and which `hyper_util`'s auto listener reads to pick a protocol.
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Whether more bytes could still turn `peeked` into the HTTP/2 preface, which
/// is exactly when the protocol is undecided.
fn undecided_preface(peeked: &[u8]) -> bool {
    peeked.len() < H2_PREFACE.len() && H2_PREFACE.starts_with(peeked)
}

/// Wait until the protocol sniff can decide, or drop the connection.
///
/// `hyper_util`'s auto listener reads the first 24 bytes with no deadline of
/// its own, and hyper's header read timeout only starts once that read has
/// picked a protocol, so a peer that opens a socket and stalls, or that writes
/// a prefix of the HTTP/2 preface and stalls, would hold a handler and the
/// ingest sender for as long as it likes.
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
        if tokio::time::timeout_at(deadline, tokio::time::sleep(PREFACE_POLL))
            .await
            .is_err()
        {
            return false;
        }
    }
}

/// Accept TCP connections forever, serving each on its own task.
async fn accept_tcp(
    listener: tokio::net::TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
    handler: Arc<Handler>,
) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "HttpPushSource: accept failed");
                #[cfg(not(feature = "tracing"))]
                let _ = _e;
                continue;
            }
        };
        let tls = tls.clone();
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
            let service =
                hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
                    let handler = Arc::clone(&handler);
                    async move {
                        Ok::<_, std::convert::Infallible>(serve_hyper(&handler, request).await)
                    }
                });
            let served = match tls {
                None => {
                    if !preface_settled(&stream).await {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            "HttpPushSource: dropped a connection that sent no request"
                        );
                        return;
                    }
                    builder
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await
                }
                Some(config) => {
                    let acceptor = tokio_rustls::TlsAcceptor::from(config);
                    let accepted = match tokio::time::timeout(
                        HANDSHAKE_TIMEOUT,
                        acceptor.accept(stream),
                    )
                    .await
                    {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(_e)) => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!(error = %_e, "HttpPushSource: TLS handshake failed");
                            #[cfg(not(feature = "tracing"))]
                            let _ = _e;
                            return;
                        }
                        Err(_) => {
                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                "HttpPushSource: dropped a connection that never finished its \
                                     TLS handshake"
                            );
                            return;
                        }
                    };
                    // ALPN already named the protocol, so there is nothing to
                    // sniff and no reason to wait for the client's first bytes.
                    let builder = if accepted.get_ref().1.alpn_protocol() == Some(b"h2") {
                        builder.http2_only()
                    } else {
                        builder.http1_only()
                    };
                    builder
                        .serve_connection(hyper_util::rt::TokioIo::new(accepted), service)
                        .await
                }
            };
            if let Err(_e) = served {
                #[cfg(feature = "tracing")]
                tracing::debug!(error = %_e, "HttpPushSource: connection ended");
                #[cfg(not(feature = "tracing"))]
                let _ = _e;
            }
        });
    }
}

/// Read one hyper request, judge it, and build its response.
async fn serve_hyper(
    handler: &Arc<Handler>,
    request: http::Request<hyper::body::Incoming>,
) -> Response<http_body_util::Full<Bytes>> {
    let started = Instant::now();
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_string();

    let answer = match handler.reject(&parts.method, &path, &parts.headers) {
        Some(answer) => answer,
        None if handler.announced_too_large(&parts.headers) => handler.too_large(),
        None => {
            // `Limited` stops reading at the cap, so a chunked body that
            // announced no length is refused without buffering the rest of it.
            match http_body_util::Limited::new(body, handler.max_body_bytes)
                .collect()
                .await
            {
                Ok(collected) => handler.accept(collected.to_bytes()).await,
                Err(e) if e.is::<http_body_util::LengthLimitError>() => handler.too_large(),
                Err(e) => handler.unreadable(&e),
            }
        }
    };

    handler
        .metrics
        .request(parts.method.as_str(), answer.status.as_u16());
    handler
        .metrics
        .elapsed(parts.method.as_str(), started.elapsed().as_secs_f64());
    handler.respond(answer)
}

/// Accept QUIC connections forever, serving HTTP/3 on each.
async fn accept_quic(endpoint: quinn::Endpoint, handler: Arc<Handler>) {
    while let Some(incoming) = endpoint.accept().await {
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let connection = match incoming.await {
                Ok(connection) => connection,
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %_e, "HttpPushSource: QUIC handshake failed");
                    #[cfg(not(feature = "tracing"))]
                    let _ = _e;
                    return;
                }
            };
            let mut h3 =
                match h3::server::Connection::new(h3_quinn::Connection::new(connection)).await {
                    Ok(h3) => h3,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %_e, "HttpPushSource: HTTP/3 session failed");
                        #[cfg(not(feature = "tracing"))]
                        let _ = _e;
                        return;
                    }
                };
            loop {
                match h3.accept().await {
                    Ok(Some(resolver)) => {
                        let handler = Arc::clone(&handler);
                        tokio::spawn(async move {
                            serve_h3(&handler, resolver).await;
                        });
                    }
                    Ok(None) => return,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(error = %_e, "HttpPushSource: HTTP/3 connection ended");
                        #[cfg(not(feature = "tracing"))]
                        let _ = _e;
                        return;
                    }
                }
            }
        });
    }
}

/// Read one HTTP/3 request, judge it, and write its response.
async fn serve_h3(
    handler: &Arc<Handler>,
    resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>,
) {
    let started = Instant::now();
    let Ok((request, mut stream)) = resolver.resolve_request().await else {
        return;
    };
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    let answer = match handler.reject(&method, &path, request.headers()) {
        Some(answer) => answer,
        None if handler.announced_too_large(request.headers()) => handler.too_large(),
        None => {
            let mut body = Vec::new();
            let mut over = false;
            loop {
                match stream.recv_data().await {
                    Ok(Some(mut chunk)) => {
                        use bytes::Buf as _;
                        let bytes = chunk.copy_to_bytes(chunk.remaining());
                        if body.len() + bytes.len() > handler.max_body_bytes {
                            over = true;
                            break;
                        }
                        body.extend_from_slice(&bytes);
                    }
                    Ok(None) => break,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %_e, "HttpPushSource: HTTP/3 body read failed");
                        #[cfg(not(feature = "tracing"))]
                        let _ = _e;
                        return;
                    }
                }
            }
            if over {
                handler.too_large()
            } else {
                handler.accept(Bytes::from(body)).await
            }
        }
    };

    handler
        .metrics
        .request(method.as_str(), answer.status.as_u16());
    handler
        .metrics
        .elapsed(method.as_str(), started.elapsed().as_secs_f64());

    let response = handler.respond(answer);
    let (parts, body) = response.into_parts();
    let head = Response::from_parts(parts, ());
    if stream.send_response(head).await.is_err() {
        return;
    }
    let body = body
        .collect()
        .await
        .map(|b| b.to_bytes())
        .unwrap_or_default();
    if !body.is_empty() && stream.send_data(body).await.is_err() {
        return;
    }
    let _ = stream.finish().await;
}

/// One decided response: a status and the plain-text reason.
struct Answer {
    status: StatusCode,
    body: String,
    /// `true` on a `405`, which carries `allow`.
    allow: bool,
}

/// Which side a document that could not be read is the fault of.
enum ReadFailure {
    /// The spool file could not be written: this host's, so a `500`.
    Spool(SaciError),
    /// The format could not read the document: the client's, so a `400`.
    Document(SaciError),
}

/// The token an `authorization: Bearer <token>` header presents.
///
/// RFC 7235 makes the scheme case-insensitive and allows more than one space
/// before the token, so both are read leniently; the token itself is compared
/// byte for byte by the caller.
fn bearer_token(headers: &HeaderMap) -> Option<&[u8]> {
    let value = headers.get(http::header::AUTHORIZATION)?.as_bytes();
    let at = value.iter().position(|b| *b == b' ')?;
    let (scheme, token) = value.split_at(at);
    scheme
        .eq_ignore_ascii_case(b"Bearer")
        .then(|| token.trim_ascii_start())
}

impl Handler {
    /// The response to refuse this request with, or `None` to read its body.
    ///
    /// Checked in order: path, method, token. A `404` before a `405` means an
    /// unknown path never advertises which methods the real one takes.
    fn reject(&self, method: &Method, path: &str, headers: &HeaderMap) -> Option<Answer> {
        if path != self.path {
            return Some(Answer {
                status: StatusCode::NOT_FOUND,
                body: format!("{WHAT}: no such path"),
                allow: false,
            });
        }
        if !self.methods.contains(method) {
            return Some(Answer {
                status: StatusCode::METHOD_NOT_ALLOWED,
                body: format!("{WHAT}: {method} is not accepted here"),
                allow: true,
            });
        }
        if let Some(expected) = &self.bearer {
            // `ct_eq` on slices of different lengths is already `false`, and
            // a token's length is not the secret.
            let ok = bearer_token(headers)
                .is_some_and(|token| bool::from(token.ct_eq(expected.as_slice())));
            if !ok {
                self.metrics.error("auth");
                return Some(Answer {
                    status: StatusCode::UNAUTHORIZED,
                    body: format!("{WHAT}: bearer token missing or wrong"),
                    allow: false,
                });
            }
        }
        None
    }

    /// Whether the announced `content-length` alone is over the cap, which
    /// refuses the body before any of it is read.
    fn announced_too_large(&self, headers: &HeaderMap) -> bool {
        headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|announced| announced > self.max_body_bytes as u64)
    }

    /// The `413` both transports answer an oversized body with.
    fn too_large(&self) -> Answer {
        self.metrics.error("size");
        Answer {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            body: format!(
                "{WHAT}: body exceeds max_body_bytes ({})",
                self.max_body_bytes
            ),
            allow: false,
        }
    }

    /// The `400` for a body the transport never delivered whole. The cap was
    /// never reached and nothing here failed, so it is neither a `413` nor a
    /// `500`.
    fn unreadable(&self, error: &dyn std::fmt::Display) -> Answer {
        self.metrics.error("transport");
        Answer {
            status: StatusCode::BAD_REQUEST,
            body: format!("{WHAT}: the request body could not be read: {error}"),
            allow: false,
        }
    }

    /// Decode one whole document and queue every batch it held.
    ///
    /// The batches go down the channel in one send, so a document is queued
    /// whole or not at all: a retry of a request this source refused
    /// duplicates no rows, and a retry of one whose client stopped waiting
    /// for the `202` can, because the queueing finished without it.
    async fn accept(&self, body: Bytes) -> Answer {
        self.metrics.bytes("in", body.len() as u64);
        let declared = match self.schema_from {
            SchemaFrom::Config => Some(Arc::clone(&self.declared)),
            SchemaFrom::Body => None,
        };
        let check = self.schema_from == SchemaFrom::Body;
        let expected = Arc::clone(&self.declared);
        let transformer = Arc::clone(&self.transformer);

        // Spooling and decoding are blocking: the read surface needs a
        // seekable file, and a parquet footer read is a seek.
        let decoded = tokio::task::spawn_blocking(move || {
            let spool = document::spool_bytes(WHAT, &body).map_err(ReadFailure::Spool)?;
            let mut reader = document::open(
                WHAT,
                "body",
                &transformer,
                spool,
                declared,
                check,
                &expected,
            )
            .map_err(ReadFailure::Document)?;
            let mut batches = Vec::new();
            while let Some(batch) = reader.next_batch().map_err(ReadFailure::Document)? {
                batches.push(batch);
            }
            Ok::<_, ReadFailure>(batches)
        })
        .await;

        let batches = match decoded {
            Err(e) => {
                return Answer {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    body: format!("{WHAT}: the document read task did not finish: {e}"),
                    allow: false,
                };
            }
            // A spool file this host could not write is this host's fault; a
            // document the format could not read is the client's.
            Ok(Err(ReadFailure::Spool(e))) => {
                return Answer {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    body: e.message().to_string(),
                    allow: false,
                };
            }
            Ok(Err(ReadFailure::Document(e))) => {
                self.metrics.error("decode");
                let body = e.message().to_string();
                // The runner counts a source error, so a peer sending
                // undecodable documents is visible without reading the logs.
                let _ = self.tx.send(Err(e)).await;
                return Answer {
                    status: StatusCode::BAD_REQUEST,
                    body,
                    allow: false,
                };
            }
            Ok(Ok(batches)) => batches,
        };

        // Bounded, so a consumer that falls behind makes this request wait
        // for its response and the client wait with it. A document that held
        // no rows queues nothing and is still accepted.
        if !batches.is_empty() && self.tx.send(Ok(batches)).await.is_err() {
            return Answer {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: format!("{WHAT}: the source was dropped"),
                allow: false,
            };
        }
        Answer {
            status: StatusCode::ACCEPTED,
            body: String::new(),
            allow: false,
        }
    }

    /// Render one [`Answer`] as the response both transports send.
    fn respond(&self, answer: Answer) -> Response<http_body_util::Full<Bytes>> {
        let mut response = Response::builder().status(answer.status);
        if !answer.body.is_empty() {
            response = response.header("content-type", "text/plain; charset=utf-8");
        }
        if answer.allow {
            response = response.header(http::header::ALLOW, self.allow.clone());
        }
        if let Some(alt_svc) = &self.alt_svc {
            response = response.header("alt-svc", alt_svc.clone());
        }
        response
            .body(http_body_util::Full::new(Bytes::from(answer.body)))
            .unwrap_or_else(|_| {
                // Every header above is already a validated `HeaderValue`, so
                // this is unreachable; answering `500` beats panicking in a
                // connection task.
                let mut fallback = Response::new(http_body_util::Full::new(Bytes::new()));
                *fallback.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                fallback
            })
    }
}

#[async_trait]
impl Source for HttpPushSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_listening()?;
        loop {
            if let Some(batch) = self.queued.pop_front() {
                return Ok(Some(batch));
            }
            // `None` means every sender is gone, which only happens once the
            // handler itself has been dropped, and is terminal.
            match self.rx.recv().await {
                Some(Ok(batches)) => self.queued.extend(batches),
                Some(Err(e)) => return Err(e),
                None => return Ok(None),
            }
        }
    }
}

impl Drop for HttpPushSource {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
        // Per-connection tasks end on their next `tx.send`, which fails once
        // `rx` is dropped with this struct.
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field};
    use saci_transformer_ndjson::NdjsonTransformer;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn ndjson() -> Arc<dyn Transformer> {
        Arc::new(NdjsonTransformer::default())
    }

    fn options() -> HttpPushOptions {
        HttpPushOptions {
            declared: schema(),
            ..HttpPushOptions::default()
        }
    }

    /// The auto listener cannot pick a protocol until the peeked bytes either
    /// complete the HTTP/2 preface or diverge from it, and a peer that stalls
    /// in between is what the accept loop has to stop waiting on.
    #[test]
    fn a_partial_http2_preface_leaves_the_protocol_undecided() {
        assert!(undecided_preface(b""), "nothing has arrived yet");
        assert!(undecided_preface(b"P"));
        assert!(undecided_preface(b"PRI * HTTP/2.0\r\n\r\nSM"));
    }

    #[test]
    fn a_request_line_or_a_whole_preface_decides_the_protocol() {
        assert!(!undecided_preface(b"GET "), "HTTP/1.1 diverges at the G");
        assert!(!undecided_preface(b"POST /orders HTTP/1.1\r\n"));
        assert!(!undecided_preface(H2_PREFACE), "the preface is complete");
    }

    /// A handler to judge hand-built requests against, taken off a bound
    /// source so every field is the one a request would meet.
    fn handler(options: HttpPushOptions) -> Arc<Handler> {
        let source = HttpPushSource::new("127.0.0.1:0", ndjson(), options).expect("binds");
        Arc::clone(&source.handler)
    }

    fn authorization(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(value).expect("header value"),
        );
        headers
    }

    /// RFC 7235 makes the auth-scheme case-insensitive, so a client spelling
    /// it `bearer` presents the same credentials as one spelling it `Bearer`.
    #[test]
    fn the_bearer_scheme_is_matched_without_regard_to_case() {
        let handler = handler(HttpPushOptions {
            bearer: Some("s3cret".to_string()),
            ..options()
        });
        for value in ["bearer s3cret", "Bearer s3cret", "BEARER s3cret"] {
            assert!(
                handler
                    .reject(&Method::POST, "/", &authorization(value))
                    .is_none(),
                "'{value}' presents the configured token"
            );
        }
    }

    #[test]
    fn a_wrong_or_missing_bearer_token_is_refused() {
        let handler = handler(HttpPushOptions {
            bearer: Some("s3cret".to_string()),
            ..options()
        });
        for headers in [
            authorization("Bearer wrong"),
            authorization("Bearer s3cret-and-more"),
            authorization("Basic s3cret"),
            authorization("s3cret"),
            HeaderMap::new(),
        ] {
            let answer = handler
                .reject(&Method::POST, "/", &headers)
                .expect("refused");
            assert_eq!(answer.status, StatusCode::UNAUTHORIZED);
        }
    }

    /// The token is what is checked, not the whitespace before it: RFC 7235
    /// allows more than one space between the scheme and the credentials.
    #[test]
    fn extra_space_before_the_token_is_accepted() {
        let handler = handler(HttpPushOptions {
            bearer: Some("s3cret".to_string()),
            ..options()
        });
        assert!(
            handler
                .reject(&Method::POST, "/", &authorization("Bearer   s3cret"))
                .is_none()
        );
    }

    /// An announced length over the cap is refused before a byte of the body
    /// is read, which is what `max_body_bytes` promises.
    #[test]
    fn an_announced_length_over_the_cap_is_refused_before_the_body() {
        let handler = handler(HttpPushOptions {
            max_body_bytes: 1_024,
            ..options()
        });
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from(1_025));
        assert!(handler.announced_too_large(&headers));

        headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from(1_024));
        assert!(!handler.announced_too_large(&headers), "exactly the cap");
    }

    /// A chunked body announces no length, so the cap is only reached while
    /// reading it.
    #[test]
    fn a_body_that_announces_nothing_is_not_refused_up_front() {
        let handler = handler(HttpPushOptions {
            max_body_bytes: 1,
            ..options()
        });
        assert!(!handler.announced_too_large(&HeaderMap::new()));
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from_static("many"),
        );
        assert!(
            !handler.announced_too_large(&headers),
            "an unparseable length announces nothing"
        );
    }

    #[test]
    fn new_reports_the_bound_port() {
        let source = HttpPushSource::new("127.0.0.1:0", ndjson(), options()).expect("binds");
        assert_ne!(source.local_addr().port(), 0);
        assert_eq!(source.quic_local_addr(), None, "no quic_bind was asked for");
    }

    #[test]
    fn an_unbindable_address_is_a_configuration_error() {
        let Err(err) = HttpPushSource::new("256.256.256.256:1", ndjson(), options()) else {
            panic!("that is not an address");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("cannot bind"), "got: {err}");
    }

    #[test]
    fn quic_without_tls_is_a_configuration_error() {
        let Err(err) = HttpPushSource::new(
            "127.0.0.1:0",
            ndjson(),
            HttpPushOptions {
                quic_bind: Some("127.0.0.1:0".to_string()),
                ..options()
            },
        ) else {
            panic!("HTTP/3 has no cleartext form");
        };
        assert_eq!(
            err.message(),
            "HttpPushSource: quic_bind needs a 'tls' block: HTTP/3 runs over TLS"
        );
    }

    #[test]
    fn an_invalid_method_is_a_configuration_error_naming_it() {
        let Err(err) = HttpPushSource::new(
            "127.0.0.1:0",
            ndjson(),
            HttpPushOptions {
                methods: vec!["PO ST".to_string()],
                ..options()
            },
        ) else {
            panic!("a space is not legal in a method name");
        };
        assert!(err.message().contains("'PO ST'"), "got: {err}");
    }

    #[test]
    fn no_methods_at_all_is_a_configuration_error() {
        let Err(err) = HttpPushSource::new(
            "127.0.0.1:0",
            ndjson(),
            HttpPushOptions {
                methods: Vec::new(),
                ..options()
            },
        ) else {
            panic!("a listener that accepts no method accepts nothing");
        };
        assert!(err.message().contains("at least one"), "got: {err}");
    }
}
