//! [`SaciSource`]: the receiving half of a service-to-service link.
//!
//! Listens on a bind address and yields one [`RecordBatch`] per batch a
//! [`SaciSink`](crate::SaciSink) in another service pushed to it. It never
//! reaches EOF: `next_batch` blocks until the next batch arrives, so only the
//! stream runner (`run_mode kind="stream"`) can drive it, and the service
//! config validator rejects every other mode.
//!
//! A session opens with the sink's hello, which names the service, workflow
//! and sink node calling and declares the schema every batch will carry. The
//! source answers `accept` or `reject`; a refusal reaches the sink as its own
//! configuration error, because the mismatch is in the two files rather than
//! in the network. Nothing after that answer is negotiated: each following
//! frame carries one batch.
//!
//! A protocol violation closes only that session; the listener stays up and
//! other peers are unaffected. Concurrent sessions are accepted, and ordering
//! holds within a session but not across them.
//!
//! Dropping the source ends the accept loop and every open session: a session
//! task waits on the receive channel alongside the socket, so it returns as
//! soon as the receiver is gone and the peer sees the close on its next
//! write.
//!
//! The wire format is Arrow IPC, so a `saci` node takes no `transformer` key:
//! the peer is another SACI service and both ends already agree on
//! `RecordBatch`es.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use saci_connector::NodeIdentity;
use saci_core::error::SaciError;
use saci_core::io::source::Source;

use crate::metrics::Instruments;
use crate::wire::{self, Frame, PROTOCOL_VERSION};

/// Receiving half of a service-to-service link: one pushed batch in, one
/// `RecordBatch` out.
///
/// The listener socket is bound in [`bind`](Self::bind) synchronously, so bind
/// failures surface at config time. The accept loop is spawned on the first
/// [`next_batch`](Source::next_batch) call, which always runs inside a tokio
/// runtime.
///
/// Backpressure comes from the channel: once `buffer` batches are queued,
/// session tasks stop reading their sockets and TCP flow control pushes back
/// on the sending services.
pub struct SaciSource {
    listener: Option<std::net::TcpListener>,
    local_addr: SocketAddr,
    schema: Arc<Schema>,
    shared: Arc<Shared>,
    rx: mpsc::Receiver<RecordBatch>,
    listener_task: Option<JoinHandle<()>>,
}

/// What every session task needs, built once per source.
struct Shared {
    /// The schema this source declares; a peer announcing different fields is
    /// refused rather than cast.
    schema: Arc<Schema>,
    /// This node's own place, for the log fields and the series labels.
    identity: NodeIdentity,
    tx: mpsc::Sender<RecordBatch>,
    max_frame_bytes: usize,
    /// Base instruments: this source's own labels, no peer. A session that got
    /// as far as a hello records through its own peer-labelled set instead.
    instruments: Instruments,
}

impl SaciSource {
    /// Bind `bind` and prepare the receive channel.
    ///
    /// `buffer` is the number of batches that may queue before backpressure
    /// reaches the sending services. `max_frame_bytes` caps a single frame; a
    /// peer that announces more has its session closed. `identity` is where
    /// this node sits, which the source both logs and labels its series with.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `bind` cannot be bound, or if
    /// the bound socket cannot be put into non-blocking mode or report its
    /// address.
    pub fn bind(
        bind: &str,
        schema: Arc<Schema>,
        buffer: usize,
        max_frame_bytes: usize,
        identity: NodeIdentity,
    ) -> Result<Self, SaciError> {
        let listener = std::net::TcpListener::bind(bind).map_err(|e| {
            SaciError::configuration(format!("SaciSource: cannot bind '{bind}': {e}"))
        })?;
        listener.set_nonblocking(true).map_err(|e| {
            SaciError::configuration(format!("SaciSource: set_nonblocking failed: {e}"))
        })?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| SaciError::configuration(format!("SaciSource: local_addr failed: {e}")))?;

        let (tx, rx) = mpsc::channel(buffer.max(1));
        let instruments = Instruments::source(&identity);

        Ok(Self {
            listener: Some(listener),
            local_addr,
            schema: Arc::clone(&schema),
            shared: Arc::new(Shared {
                schema,
                identity,
                tx,
                max_frame_bytes,
                instruments,
            }),
            rx,
            listener_task: None,
        })
    }

    /// The bound address, with any ephemeral port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Convert the std listener and spawn the accept loop.
    ///
    /// Idempotent in both directions. A second call after a success finds the
    /// accept task and does nothing. A failed adoption consumes the socket
    /// too, so a second call finds neither and reports the failure again,
    /// which is what keeps `next_batch` off a channel nothing feeds.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the bound socket cannot be adopted
    /// into the current runtime.
    fn ensure_listening(&mut self) -> Result<(), SaciError> {
        let Some(std_listener) = self.listener.take() else {
            if self.listener_task.is_some() {
                return Ok(());
            }
            return Err(cannot_adopt("an earlier call consumed the bound socket"));
        };
        let listener =
            TcpListener::from_std(std_listener).map_err(|e| cannot_adopt(&e.to_string()))?;

        let shared = Arc::clone(&self.shared);
        self.listener_task = Some(tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %_e, "SaciSource: accept failed");
                        continue;
                    }
                };
                let shared = Arc::clone(&shared);
                tokio::spawn(async move {
                    serve_session(stream, peer, shared).await;
                });
            }
        }));
        Ok(())
    }
}

/// The one wording for a bound socket the runtime would not adopt.
fn cannot_adopt(reason: &str) -> SaciError {
    SaciError::generic(format!(
        "SaciSource: cannot adopt the listener into the runtime: {reason}"
    ))
}

/// One frame the session read, or the fact that the peer closed cleanly.
enum Incoming {
    /// A frame body, without its length prefix.
    Body(Vec<u8>),
    /// A clean close between frames: the normal disconnect.
    Eof,
}

/// Read one length-prefixed frame body.
///
/// `Err` is a protocol violation, already worded for the log.
async fn read_frame(stream: &mut TcpStream, max_frame_bytes: usize) -> Result<Incoming, String> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(Incoming::Eof),
        Err(e) => return Err(format!("frame header read failed: {e}")),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    // Unlike the raw `tcp` source, a zero-length frame is not a no-op to skip:
    // every frame here carries at least a kind byte, so an empty one means the
    // peer is not speaking this protocol.
    if len == 0 {
        return Err("a frame announced a zero-length body".to_string());
    }
    if len > max_frame_bytes {
        return Err(format!(
            "a frame announced {len} bytes, above the {max_frame_bytes} byte cap"
        ));
    }
    let mut body = vec![0u8; len];
    match stream.read_exact(&mut body).await {
        Ok(_) => Ok(Incoming::Body(body)),
        Err(e) => Err(format!("truncated frame body: {e}")),
    }
}

/// Read one frame, or return when the receiving source is gone.
///
/// `None` means the source was dropped. The session returns at once, so the
/// stream is dropped and the peer learns of it on its next write; a read
/// abandoned part-way through a frame costs nothing, because that frame had
/// nowhere left to go.
async fn read_frame_or_closed(
    stream: &mut TcpStream,
    shared: &Shared,
) -> Option<Result<Incoming, String>> {
    tokio::select! {
        read = read_frame(stream, shared.max_frame_bytes) => Some(read),
        () = shared.tx.closed() => None,
    }
}

/// Write one frame and flush it.
async fn reply(stream: &mut TcpStream, frame: &Frame) -> Result<(), SaciError> {
    let mut buf = Vec::new();
    wire::encode_frame(&mut buf, frame)?;
    stream
        .write_all(&buf)
        .await
        .map_err(|e| SaciError::generic(format!("SaciSource: writing a reply failed: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| SaciError::generic(format!("SaciSource: flushing a reply failed: {e}")))
}

/// Refuse the session with `reason`, recording it against `instruments`.
///
/// `instruments` is the peer-labelled set once a hello parsed, and the
/// source's own base set before that.
async fn refuse(
    stream: &mut TcpStream,
    addr: SocketAddr,
    instruments: &Instruments,
    reason: String,
) {
    instruments.session_rejected();
    #[cfg(feature = "tracing")]
    tracing::warn!(peer = %addr, reason = %reason, "SaciSource: session refused");
    #[cfg(not(feature = "tracing"))]
    let _ = addr;
    // The peer learns why or it does not; either way this session is over.
    let _ = reply(stream, &Frame::Reject { reason }).await;
}

/// Serve one accepted connection: the handshake, then batches until it closes
/// or violates the protocol.
async fn serve_session(mut stream: TcpStream, addr: SocketAddr, shared: Arc<Shared>) {
    let Some((peer, session)) = handshake(&mut stream, addr, &shared).await else {
        return;
    };
    receive(&mut stream, addr, &shared, &peer, &session).await;
}

/// Read the hello, decide, and answer. `Some` means the session is open.
async fn handshake(
    stream: &mut TcpStream,
    addr: SocketAddr,
    shared: &Shared,
) -> Option<(NodeIdentity, Instruments)> {
    let body = match read_frame_or_closed(stream, shared).await {
        Some(Ok(Incoming::Body(body))) => body,
        // A dial that never said hello is a probe, not a peer: nothing to
        // record and nobody to tell.
        Some(Ok(Incoming::Eof)) => return None,
        // The source is gone, so there is nobody left to hand a batch to.
        None => return None,
        Some(Err(_reason)) => {
            shared.instruments.error("frame");
            shared.instruments.session_rejected();
            #[cfg(feature = "tracing")]
            tracing::warn!(
                peer = %addr,
                reason = %_reason,
                "SaciSource: protocol violation in the handshake"
            );
            return None;
        }
    };

    // Peeked before the decode, so a peer speaking a future version is
    // refused by version rather than by whatever its layout decodes to.
    if let Some(version) = wire::hello_version(&body)
        && version != PROTOCOL_VERSION
    {
        refuse(
            stream,
            addr,
            &shared.instruments,
            format!(
                "protocol version {version} is not supported; this source speaks version {PROTOCOL_VERSION}"
            ),
        )
        .await;
        return None;
    }

    let (identity, peer_schema) = match wire::decode_frame(Buffer::from(body)) {
        Ok(Frame::Hello {
            identity, schema, ..
        }) => (identity, schema),
        Ok(other) => {
            let kind = other.kind();
            refuse(
                stream,
                addr,
                &shared.instruments,
                format!("expected a hello frame, got frame kind {kind}"),
            )
            .await;
            return None;
        }
        Err(_e) => {
            shared.instruments.error("frame");
            shared.instruments.session_rejected();
            #[cfg(feature = "tracing")]
            tracing::warn!(peer = %addr, error = %_e, "SaciSource: undecodable handshake frame");
            return None;
        }
    };

    let session = Instruments::session(&shared.identity, &identity);
    if peer_schema.fields() != shared.schema.fields() {
        refuse(
            stream,
            addr,
            &session,
            format!(
                "schema mismatch: the peer declares {peer_schema:?}, this source declares {:?}",
                shared.schema
            ),
        )
        .await;
        return None;
    }

    if let Err(_e) = reply(stream, &Frame::Accept).await {
        #[cfg(feature = "tracing")]
        tracing::warn!(peer = %addr, error = %_e, "SaciSource: cannot accept the session");
        return None;
    }
    session.session_accepted();
    #[cfg(feature = "tracing")]
    tracing::info!(
        peer = %addr,
        peer_service = %identity.service,
        peer_workflow = %identity.workflow,
        peer_sink = %identity.node,
        workflow = %shared.identity.workflow,
        source = %shared.identity.node,
        "SaciSource: session accepted"
    );
    #[cfg(not(feature = "tracing"))]
    let _ = addr;
    Some((identity, session))
}

/// Read data frames until the peer closes or violates the protocol.
async fn receive(
    stream: &mut TcpStream,
    addr: SocketAddr,
    shared: &Shared,
    peer: &NodeIdentity,
    session: &Instruments,
) {
    // One decoder per session: the peer's encoder emits the schema message
    // once, on its first data frame, and the dictionary state that follows is
    // that session's.
    let mut decoder = StreamDecoder::new();
    let mut schema_checked = false;
    #[cfg(not(feature = "tracing"))]
    let _ = (addr, peer);

    loop {
        let body = match read_frame_or_closed(stream, shared).await {
            Some(Ok(Incoming::Body(body))) => body,
            Some(Ok(Incoming::Eof)) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    peer = %addr,
                    peer_service = %peer.service,
                    workflow = %shared.identity.workflow,
                    source = %shared.identity.node,
                    "SaciSource: session closed"
                );
                return;
            }
            // The source was dropped; dropping the stream is what tells the
            // peer, whose next write then fails and redials.
            None => return,
            Some(Err(_reason)) => {
                session.error("frame");
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    peer = %addr,
                    reason = %_reason,
                    "SaciSource: protocol violation, closing the session"
                );
                return;
            }
        };

        let body_len = body.len() as u64;
        let (traceparent, ipc) = match wire::decode_frame(Buffer::from(body)) {
            Ok(Frame::Data { traceparent, ipc }) => (traceparent, ipc),
            Ok(_other) => {
                session.error("frame");
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    peer = %addr,
                    kind = _other.kind(),
                    "SaciSource: expected a data frame, closing the session"
                );
                return;
            }
            Err(_e) => {
                session.error("frame");
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    peer = %addr,
                    error = %_e,
                    "SaciSource: undecodable frame, closing the session"
                );
                return;
            }
        };
        #[cfg(not(feature = "trace-context"))]
        let _ = traceparent;

        #[cfg(feature = "tracing")]
        let span = tracing::debug_span!(
            "peer.receive",
            workflow = %shared.identity.workflow,
            source = %shared.identity.node,
            peer_service = %peer.service,
            peer_workflow = %peer.workflow,
            peer_sink = %peer.node,
            bytes = body_len,
            rows = tracing::field::Empty,
        );
        #[cfg(feature = "trace-context")]
        if let Some(tp) = traceparent.as_deref() {
            // A header the peer's exporter never produced, or one this process
            // has no layer to adopt, costs the batch nothing.
            crate::trace::adopt(&span, tp);
        }

        // The decode runs inside the span and the send outside it, so no guard
        // is ever held across an await.
        let decoded = {
            #[cfg(feature = "tracing")]
            let _guard = span.enter();
            decode_batches(&mut decoder, ipc)
        };
        let batches = match decoded {
            Ok(batches) => batches,
            Err(_e) => {
                session.error("decode");
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    peer = %addr,
                    error = %_e,
                    "SaciSource: cannot decode a batch, closing the session"
                );
                return;
            }
        };

        // The hello's schema is what the session was accepted on; this is the
        // stream's own, read once it has carried its schema message.
        if !schema_checked && let Some(declared) = decoder.schema() {
            schema_checked = true;
            if declared.fields() != shared.schema.fields() {
                session.error("schema");
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    peer = %addr,
                    "SaciSource: the stream declares a schema the hello did not, closing the session"
                );
                return;
            }
        }

        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        session.batch(rows, body_len);
        #[cfg(feature = "tracing")]
        span.record("rows", rows);

        for batch in batches {
            // Bounded send, so TCP carries the backpressure. A send error
            // means the source was dropped and this session has nowhere to go.
            if shared.tx.send(batch).await.is_err() {
                return;
            }
        }
    }
}

/// Feed one frame's IPC bytes to the session decoder and collect what they
/// complete.
fn decode_batches(decoder: &mut StreamDecoder, ipc: Buffer) -> Result<Vec<RecordBatch>, SaciError> {
    let mut buf = ipc;
    let mut out = Vec::new();
    while !buf.is_empty() {
        match decoder.decode(&mut buf) {
            Ok(Some(batch)) => out.push(batch),
            // `Ok(None)` is both a drained buffer and an end-of-stream
            // marker, which leaves the decoder finished. A producer that
            // sends the marker therefore closes its own session on its next
            // frame, where the decoder answers "Unexpected EOS".
            Ok(None) => break,
            Err(e) => return Err(SaciError::generic(format!("SaciSource: {e}"))),
        }
    }
    Ok(out)
}

#[async_trait]
impl Source for SaciSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_listening()?;
        // `None` here means the accept task is gone (it never drops `tx`
        // otherwise), which is terminal for this source.
        Ok(self.rx.recv().await)
    }
}

impl Drop for SaciSource {
    fn drop(&mut self) {
        if let Some(handle) = self.listener_task.take() {
            handle.abort();
        }
        // Every session task also waits on `tx.closed()`, so dropping `rx`
        // with this struct closes the open sessions as well as the accept
        // loop.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use arrow_array::{Int64Array, StringArray};
    use arrow_ipc::writer::StreamEncoder;
    use arrow_schema::{DataType, Field};
    use tokio::time::timeout;

    fn identity() -> NodeIdentity {
        NodeIdentity {
            service: "svc-b".to_string(),
            workflow: "w".to_string(),
            node: "in".to_string(),
        }
    }

    fn peer_identity() -> NodeIdentity {
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).expect("batch")
    }

    /// The one value of a single row batch this module builds.
    fn value_of(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an Int64 column")
            .value(0)
    }

    /// A source already serving its accept loop, and the address to dial.
    fn listening(buffer: usize, max_frame_bytes: usize) -> (SaciSource, SocketAddr) {
        let mut source =
            SaciSource::bind("127.0.0.1:0", schema(), buffer, max_frame_bytes, identity())
                .expect("bind an ephemeral port");
        let addr = source.local_addr();
        source.ensure_listening().expect("adopt the listener");
        (source, addr)
    }

    /// Encode `frame` and write it, flushed.
    async fn send(peer: &mut TcpStream, frame: &Frame) {
        let mut buf = Vec::new();
        wire::encode_frame(&mut buf, frame).expect("encode a frame");
        peer.write_all(&buf).await.expect("write a frame");
        peer.flush().await.expect("flush a frame");
    }

    /// Frame `ipc` as a data frame and write it, flushed.
    async fn send_ipc(peer: &mut TcpStream, ipc: &[u8]) {
        let mut framed = wire::data_header(None, ipc.len()).expect("a data header");
        framed.extend_from_slice(ipc);
        peer.write_all(&framed).await.expect("write a data frame");
        peer.flush().await.expect("flush a data frame");
    }

    /// Arrow IPC stream bytes for `batch`, contiguous the way a frame carries
    /// them.
    fn ipc_bytes(encoder: &mut StreamEncoder, batch: &RecordBatch) -> Vec<u8> {
        encoder
            .encode(batch)
            .expect("encode a batch")
            .iter()
            .flat_map(|b| b.as_slice().to_vec())
            .collect()
    }

    /// Read one frame the source wrote back.
    async fn read_reply(peer: &mut TcpStream) -> Frame {
        let mut len = [0u8; 4];
        peer.read_exact(&mut len).await.expect("read reply length");
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
        peer.read_exact(&mut body).await.expect("read reply body");
        wire::decode_frame(Buffer::from(body)).expect("decode reply")
    }

    /// Dial and declare `schema`, leaving the answer for the caller to read.
    async fn dial_declaring(addr: SocketAddr, schema: Arc<Schema>) -> TcpStream {
        let mut peer = TcpStream::connect(addr).await.expect("dial");
        send(
            &mut peer,
            &Frame::Hello {
                version: PROTOCOL_VERSION,
                identity: peer_identity(),
                schema,
            },
        )
        .await;
        peer
    }

    /// Dial, declare `schema`, and take the accept: an open session.
    async fn dial_with_hello(addr: SocketAddr, schema: Arc<Schema>) -> TcpStream {
        let mut peer = dial_declaring(addr, schema).await;
        assert!(
            matches!(read_reply(&mut peer).await, Frame::Accept),
            "the handshake must be accepted"
        );
        peer
    }

    /// Whether the peer's next read sees the session gone: `Ok(0)` for the
    /// FIN, or an error where the host answered a socket dropped with unread
    /// bytes in it with an RST instead.
    async fn sees_the_close(peer: &mut TcpStream) -> bool {
        let mut byte = [0u8; 1];
        match timeout(Duration::from_secs(2), peer.read(&mut byte)).await {
            Ok(Ok(0)) | Ok(Err(_)) => true,
            Ok(Ok(_)) | Err(_) => false,
        }
    }

    #[test]
    fn binding_port_zero_reports_the_resolved_port() {
        let source = SaciSource::bind("127.0.0.1:0", schema(), 8, 1 << 20, identity())
            .expect("bind an ephemeral port");
        assert_ne!(source.local_addr().port(), 0);
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn an_unbindable_address_is_a_configuration_error() {
        let err = SaciSource::bind("256.256.256.256:1", schema(), 8, 1 << 20, identity())
            .err()
            .expect("bind must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("SaciSource: cannot bind"),
            "got: {err}"
        );
    }

    /// A dial that closes without a hello is a health probe, not a peer, and
    /// the listener must survive it.
    #[tokio::test]
    async fn a_probe_that_says_nothing_leaves_the_listener_up() {
        let (_source, addr) = listening(8, 1 << 20);

        drop(TcpStream::connect(addr).await.expect("dial"));
        // The second dial proves the accept loop is still running.
        drop(dial_with_hello(addr, schema()).await);
    }

    #[tokio::test]
    async fn a_peer_declaring_other_fields_is_refused_by_schema() {
        let (_source, addr) = listening(8, 1 << 20);
        let other = Arc::new(Schema::new(vec![Field::new("other", DataType::Utf8, true)]));

        let mut peer = dial_declaring(addr, other).await;
        match read_reply(&mut peer).await {
            Frame::Reject { reason } => {
                assert!(reason.contains("schema mismatch"), "got: {reason}")
            }
            other => panic!("expected a reject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_future_protocol_version_is_refused_by_version() {
        let (_source, addr) = listening(8, 1 << 20);

        let mut peer = TcpStream::connect(addr).await.expect("dial");
        send(
            &mut peer,
            &Frame::Hello {
                version: PROTOCOL_VERSION + 1,
                identity: peer_identity(),
                schema: schema(),
            },
        )
        .await;
        match read_reply(&mut peer).await {
            Frame::Reject { reason } => assert_eq!(
                reason,
                "protocol version 2 is not supported; this source speaks version 1"
            ),
            other => panic!("expected a reject, got {other:?}"),
        }
    }

    /// An opening frame that is not a hello is refused by kind, so a peer that
    /// starts sending data without a handshake is told what went wrong.
    #[tokio::test]
    async fn an_opening_frame_that_is_not_a_hello_is_refused_by_kind() {
        let (_source, addr) = listening(8, 1 << 20);

        let mut peer = TcpStream::connect(addr).await.expect("dial");
        send(
            &mut peer,
            &Frame::Data {
                traceparent: None,
                ipc: Buffer::from(vec![1u8, 2, 3]),
            },
        )
        .await;
        match read_reply(&mut peer).await {
            Frame::Reject { reason } => {
                assert_eq!(reason, "expected a hello frame, got frame kind 4");
            }
            other => panic!("expected a reject, got {other:?}"),
        }
    }

    /// A frame above the cap is refused at its header, so that session closes
    /// and the next dial still gets one. The cap clears a schema message plus
    /// one record batch message, which is what the second session sends.
    #[tokio::test]
    async fn an_oversized_frame_closes_only_that_session() {
        let (mut source, addr) = listening(8, 4096);

        let mut peer = dial_with_hello(addr, schema()).await;
        send(
            &mut peer,
            &Frame::Data {
                traceparent: None,
                ipc: Buffer::from(vec![0u8; 8192]),
            },
        )
        .await;
        assert!(
            sees_the_close(&mut peer).await,
            "the oversized frame must close the session"
        );

        let mut second = dial_with_hello(addr, schema()).await;
        let mut encoder = StreamEncoder::try_new(&schema()).expect("an encoder");
        let expected = batch(vec![7]);
        send_ipc(&mut second, &ipc_bytes(&mut encoder, &expected)).await;
        let got = timeout(Duration::from_secs(2), source.next_batch())
            .await
            .expect("the batch of the second session must arrive")
            .expect("next_batch");
        assert_eq!(got, Some(expected));
    }

    /// Nothing is negotiated after the accept, so any frame kind other than
    /// data closes that session while the listener keeps serving.
    #[tokio::test]
    async fn a_frame_that_is_not_data_closes_the_session() {
        let (_source, addr) = listening(8, 1 << 20);

        for frame in [
            Frame::Accept,
            Frame::Reject {
                reason: "no".to_string(),
            },
        ] {
            let mut peer = dial_with_hello(addr, schema()).await;
            send(&mut peer, &frame).await;
            assert!(
                sees_the_close(&mut peer).await,
                "frame kind {} must close the session",
                frame.kind()
            );
        }
        drop(dial_with_hello(addr, schema()).await);
    }

    /// A data frame Arrow refuses closes the session and hands nothing on.
    /// The bytes announce a four byte message and then four bytes no
    /// flatbuffer verifier accepts; bytes of `0xff` alone would instead
    /// announce a four gigabyte message the decoder is still waiting for.
    #[tokio::test]
    async fn an_undecodable_message_closes_the_session() {
        let (mut source, addr) = listening(8, 1 << 20);

        let mut peer = dial_with_hello(addr, schema()).await;
        let mut ipc = 4u32.to_le_bytes().to_vec();
        ipc.extend_from_slice(&[0xff; 4]);
        send_ipc(&mut peer, &ipc).await;
        assert!(
            timeout(Duration::from_millis(200), source.next_batch())
                .await
                .is_err(),
            "no batch may reach the consumer"
        );
        assert!(sees_the_close(&mut peer).await, "the session must close");
        drop(dial_with_hello(addr, schema()).await);
    }

    /// The hello's schema is what the session was accepted on; a stream
    /// declaring another one closes the session before any of its batches is
    /// handed on.
    #[tokio::test]
    async fn a_stream_declaring_another_schema_closes_the_session() {
        let (mut source, addr) = listening(8, 1 << 20);

        let mut peer = dial_with_hello(addr, schema()).await;
        let other = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let other_batch = RecordBatch::try_new(
            Arc::clone(&other),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .expect("a batch of the other schema");
        let mut encoder = StreamEncoder::try_new(&other).expect("an encoder");
        send_ipc(&mut peer, &ipc_bytes(&mut encoder, &other_batch)).await;

        assert!(
            timeout(Duration::from_millis(200), source.next_batch())
                .await
                .is_err(),
            "a mismatched stream must deliver nothing"
        );
        assert!(sees_the_close(&mut peer).await, "the session must close");
    }

    /// Frames are transport and messages are format: one batch cut across two
    /// frames is still one batch.
    #[tokio::test]
    async fn a_batch_split_across_two_frames_yields_one_batch() {
        let (mut source, addr) = listening(8, 1 << 20);

        let mut peer = dial_with_hello(addr, schema()).await;
        let mut encoder = StreamEncoder::try_new(&schema()).expect("an encoder");
        let expected = batch(vec![1, 2, 3]);
        let ipc = ipc_bytes(&mut encoder, &expected);
        let cut = ipc.len() / 3;
        send_ipc(&mut peer, &ipc[..cut]).await;
        send_ipc(&mut peer, &ipc[cut..]).await;

        let got = timeout(Duration::from_secs(2), source.next_batch())
            .await
            .expect("the completed batch must arrive")
            .expect("next_batch");
        assert_eq!(got, Some(expected));
        assert!(
            timeout(Duration::from_millis(200), source.next_batch())
                .await
                .is_err(),
            "two frames carried one batch, not two"
        );
    }

    #[tokio::test]
    async fn two_batches_in_one_frame_yield_both_in_order() {
        let (mut source, addr) = listening(8, 1 << 20);

        let mut peer = dial_with_hello(addr, schema()).await;
        let mut encoder = StreamEncoder::try_new(&schema()).expect("an encoder");
        let first = batch(vec![1, 2]);
        let second = batch(vec![3, 4]);
        let mut ipc = ipc_bytes(&mut encoder, &first);
        ipc.extend_from_slice(&ipc_bytes(&mut encoder, &second));
        send_ipc(&mut peer, &ipc).await;

        for expected in [first, second] {
            let got = timeout(Duration::from_secs(2), source.next_batch())
                .await
                .expect("both batches must arrive")
                .expect("next_batch");
            assert_eq!(got, Some(expected));
        }
    }

    /// Five batches written before anything is read, on a source whose
    /// channel holds one: TCP flow control carries the backpressure, and a
    /// consumer that starts late still gets every batch in order.
    #[tokio::test]
    async fn a_slow_consumer_loses_no_batch() {
        let (mut source, addr) = listening(1, 1 << 20);

        let mut peer = dial_with_hello(addr, schema()).await;
        let mut encoder = StreamEncoder::try_new(&schema()).expect("an encoder");
        let sent: Vec<RecordBatch> = (1..=5).map(|v| batch(vec![v])).collect();
        for b in &sent {
            send_ipc(&mut peer, &ipc_bytes(&mut encoder, b)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut got = Vec::new();
        for _ in 0..sent.len() {
            got.push(
                timeout(Duration::from_secs(2), source.next_batch())
                    .await
                    .expect("every batch must arrive")
                    .expect("next_batch")
                    .expect("a batch"),
            );
        }
        assert_eq!(got, sent);
    }

    /// Ordering holds within a session, not across them: two sessions writing
    /// at once deliver every batch, each session's own in order.
    #[tokio::test]
    async fn two_sessions_keep_their_own_order() {
        let (mut source, addr) = listening(8, 1 << 20);

        let mut peers = Vec::new();
        for _ in 0..2 {
            peers.push((
                dial_with_hello(addr, schema()).await,
                StreamEncoder::try_new(&schema()).expect("an encoder"),
            ));
        }
        // Interleaved, so both sessions are writing over the same window.
        for round in 1..=3i64 {
            for (index, (peer, encoder)) in peers.iter_mut().enumerate() {
                let value = if index == 0 { round } else { round * 10 };
                send_ipc(peer, &ipc_bytes(encoder, &batch(vec![value]))).await;
            }
        }

        let mut got = Vec::new();
        for _ in 0..6 {
            let batch = timeout(Duration::from_secs(2), source.next_batch())
                .await
                .expect("all six batches must arrive")
                .expect("next_batch")
                .expect("a batch");
            got.push(value_of(&batch));
        }
        let first: Vec<i64> = got.iter().copied().filter(|v| *v < 10).collect();
        let second: Vec<i64> = got.iter().copied().filter(|v| *v >= 10).collect();
        assert_eq!(first, vec![1, 2, 3]);
        assert_eq!(second, vec![10, 20, 30]);
    }

    /// Dropping the source closes the sessions it accepted, so a peer learns
    /// the link is gone instead of writing into a socket nobody reads.
    #[tokio::test]
    async fn dropping_the_source_closes_an_open_session() {
        let (source, addr) = listening(8, 1 << 20);
        let mut peer = dial_with_hello(addr, schema()).await;

        drop(source);
        assert!(
            sees_the_close(&mut peer).await,
            "the session must close with the source"
        );
    }

    /// A listener the runtime never adopted is an error, not a wait: no std
    /// socket and no accept task is the state a failed `TcpListener::from_std`
    /// leaves, and `next_batch` would otherwise block on a channel nothing
    /// feeds.
    #[tokio::test]
    async fn next_batch_reports_a_listener_that_was_never_adopted() {
        let mut source = SaciSource::bind("127.0.0.1:0", schema(), 8, 1 << 20, identity())
            .expect("bind an ephemeral port");
        source.listener = None;

        let err = timeout(Duration::from_secs(2), source.next_batch())
            .await
            .expect("next_batch must return rather than wait")
            .expect_err("an unadopted listener is an error");
        assert_eq!(err.category(), "generic", "got: {err}");
        assert!(
            err.to_string()
                .contains("cannot adopt the listener into the runtime"),
            "got: {err}"
        );
    }
}
