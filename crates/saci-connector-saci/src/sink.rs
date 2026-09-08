//! [`SaciSink`]: the sending half of a service-to-service link.
//!
//! [`SaciSource`]: crate::SaciSource
//!
//! The source listens and reads frames; this sink dials out and writes them.
//! [`connect`](SaciSink::connect) resolves every candidate address and opens
//! nothing, so `saci-service validate` passes while the peers are down. The
//! session is opened on the first [`write_batch`](Sink::write_batch), which is
//! where an unreachable peer surfaces.
//!
//! ## Failover
//!
//! `connect` takes one address or a list of them. The sink dials them in
//! order and keeps the first that accepts a session, so a set of downstream
//! services acts as failover peers. Two answers are not the same thing:
//!
//! - Unreachable, or an I/O error or timeout during the handshake: this peer
//!   is down, so the next one is tried.
//! - A `reject` frame: the peer is up and says the two configs disagree, most
//!   often on the schema. That verdict is the same at every peer, so it is
//!   returned at once rather than hidden behind the next dial.
//!
//! A write that fails drops the session instead of putting it back, so the
//! runner's retry of that batch redials, possibly onto the next peer, and
//! sends it again. No frame acknowledges a batch, so a batch the socket
//! accepted before the peer went away is reported as written and a lost
//! session does not resend it.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamEncoder;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use saci_connector::NodeIdentity;
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;

use crate::wire::{self, Frame, PROTOCOL_VERSION};

/// A reject reason is a sentence, so a reply body past this is a peer that is
/// not speaking this protocol and counts as a failed dial.
const MAX_REPLY_BYTES: usize = 65_536;

/// Sending half of a service-to-service link: one `RecordBatch` in, one data
/// frame on the wire.
///
/// One session serves the sink until a write on it fails. There is no
/// background reconnect: the next `write_batch` after a failure opens a new
/// session, which is what makes the runner's retry the reconnect loop.
pub struct SaciSink {
    /// `None` until the first batch opens a session, and again after a write
    /// on it failed.
    session: Option<Session>,
    /// Candidate peers in declaration order, each with the address string it
    /// was resolved from so an error names what the file said.
    peers: Vec<(String, SocketAddr)>,
    schema: Arc<Schema>,
    identity: NodeIdentity,
    handshake_timeout: Duration,
}

/// One open session: the socket, and the encoder whose schema message rode out
/// on its first data frame.
struct Session {
    writer: BufWriter<TcpStream>,
    /// Exactly one encoder per session, created at `accept`, so the Arrow IPC
    /// schema message is emitted once and the peer's decoder never sees a
    /// second one.
    encoder: StreamEncoder,
    peer: SocketAddr,
}

impl SaciSink {
    /// Resolve every entry of `connect` and prepare the sink without opening a
    /// socket.
    ///
    /// Each entry is a `host:port` string, and its first resolved address is
    /// that peer for the sink's lifetime. `identity` is what the peer sees as
    /// `peer_service`, `peer_workflow` and `peer_sink`. `handshake_timeout`
    /// bounds the wait for the peer's `accept` or `reject`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `connect` is empty, if an entry
    /// cannot be resolved or resolves to no address at all, or if Arrow
    /// refuses to encode `schema` as an IPC stream.
    pub fn connect(
        connect: &[String],
        schema: Arc<Schema>,
        identity: NodeIdentity,
        handshake_timeout: Duration,
    ) -> Result<Self, SaciError> {
        if connect.is_empty() {
            return Err(SaciError::configuration(
                "SaciSink: 'connect' names no address",
            ));
        }
        let mut peers = Vec::with_capacity(connect.len());
        for entry in connect {
            let addr = entry
                .to_socket_addrs()
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "SaciSink: cannot resolve 'connect' address '{entry}': {e}"
                    ))
                })?
                .next()
                .ok_or_else(|| {
                    SaciError::configuration(format!(
                        "SaciSink: 'connect' address '{entry}' resolved to no address"
                    ))
                })?;
            peers.push((entry.clone(), addr));
        }

        // The call each session makes, so a schema Arrow will not encode is
        // this sink's own configuration error at build time instead of
        // something the first batch discovers.
        new_encoder(&schema)?;

        Ok(Self {
            session: None,
            peers,
            schema,
            identity,
            handshake_timeout,
        })
    }

    /// The peers this sink dials, in the order it tries them.
    pub fn peers(&self) -> Vec<SocketAddr> {
        self.peers.iter().map(|(_, addr)| *addr).collect()
    }

    /// Dial the peers in order and keep the first that accepts.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for a schema Arrow will not encode
    /// and for a peer that answered `reject`, and [`SaciError::Generic`] when
    /// no peer accepted a session.
    async fn open_session(&self) -> Result<Session, SaciError> {
        let mut failures = Vec::with_capacity(self.peers.len());
        for (entry, addr) in &self.peers {
            // One encoder per attempt, so the schema message is still pending
            // for whichever peer accepts.
            let encoder = new_encoder(&self.schema)?;
            match self.try_peer(*addr, encoder).await {
                Ok(session) => {
                    #[cfg(feature = "tracing")]
                    tracing::info!(
                        peer = %addr,
                        workflow = %self.identity.workflow,
                        sink = %self.identity.node,
                        "SaciSink: session opened"
                    );
                    return Ok(session);
                }
                // A refusal is about this pair of configs, and every peer in
                // the list would say the same, so it ends the call here.
                Err(PeerVerdict::Refused(reason)) => {
                    return Err(SaciError::configuration(format!(
                        "SaciSink: peer {addr} refused the session: {reason}"
                    )));
                }
                Err(PeerVerdict::Unreachable(_e)) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        peer = %addr,
                        error = %_e,
                        "SaciSink: peer unreachable, trying the next"
                    );
                    failures.push(format!("{entry} ({addr}): {_e}"));
                }
            }
        }
        Err(SaciError::generic(format!(
            "SaciSink: no peer accepted a session: {}",
            failures.join("; ")
        )))
    }

    /// Dial one peer and open a session carrying `encoder` on it.
    ///
    /// [`PeerVerdict::Unreachable`] is the caller's cue to try the next peer;
    /// [`PeerVerdict::Refused`] ends the whole attempt. The peer answers the
    /// hello, so neither verdict can come from this sink's own schema.
    async fn try_peer(
        &self,
        addr: SocketAddr,
        encoder: StreamEncoder,
    ) -> Result<Session, PeerVerdict> {
        let down = |e: String| PeerVerdict::Unreachable(SaciError::generic(e));
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| down(e.to_string()))?;
        let mut writer = BufWriter::new(stream);

        let mut hello = Vec::new();
        wire::encode_frame(
            &mut hello,
            &Frame::Hello {
                version: PROTOCOL_VERSION,
                identity: self.identity.clone(),
                schema: Arc::clone(&self.schema),
            },
        )
        .map_err(PeerVerdict::Unreachable)?;
        writer
            .write_all(&hello)
            .await
            .map_err(|e| down(format!("writing the hello failed: {e}")))?;
        writer
            .flush()
            .await
            .map_err(|e| down(format!("flushing the hello failed: {e}")))?;

        let reply = tokio::time::timeout(self.handshake_timeout, read_reply(&mut writer))
            .await
            .map_err(|_| {
                down(format!(
                    "no answer within {} ms",
                    self.handshake_timeout.as_millis()
                ))
            })?
            .map_err(PeerVerdict::Unreachable)?;

        match reply {
            Frame::Accept => Ok(Session {
                writer,
                encoder,
                peer: addr,
            }),
            Frame::Reject { reason } => Err(PeerVerdict::Refused(reason)),
            other => Err(down(format!(
                "answered frame kind {} instead of accept or reject",
                other.kind()
            ))),
        }
    }
}

/// One Arrow IPC stream encoder for `schema`, its schema message still
/// pending.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`]: a schema Arrow will not encode is
/// this sink's own configuration, not an answer any peer gave.
fn new_encoder(schema: &Schema) -> Result<StreamEncoder, SaciError> {
    StreamEncoder::try_new(schema).map_err(|e| {
        SaciError::configuration(format!(
            "SaciSink: this schema cannot be encoded as Arrow IPC: {e}"
        ))
    })
}

/// Why one peer did not yield a session.
enum PeerVerdict {
    /// The peer is down or is not answering this protocol. Try the next.
    Unreachable(SaciError),
    /// The peer is up and says the two configs disagree. Every peer in the
    /// list would answer the same, so this ends the attempt.
    Refused(String),
}

/// Read one length-prefixed frame from the peer.
async fn read_reply(writer: &mut BufWriter<TcpStream>) -> Result<Frame, SaciError> {
    let stream = writer.get_mut();
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| SaciError::generic(format!("reading the reply failed: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_REPLY_BYTES {
        return Err(SaciError::generic(format!(
            "answered a {len} byte reply, which is not a reply this protocol has"
        )));
    }
    let mut body = vec![0u8; len];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|e| SaciError::generic(format!("reading the reply body failed: {e}")))?;
    wire::decode_frame(arrow_buffer::Buffer::from(body))
}

/// Write one data frame: the header, then the encoder's buffers as they are.
///
/// The header and the buffers are separate writes rather than one copied
/// buffer, so a batch reaches the socket without being staged again.
async fn write_frame(
    session: &mut Session,
    buffers: &[arrow_buffer::Buffer],
    traceparent: Option<&str>,
) -> Result<(), SaciError> {
    let peer = session.peer;
    let ipc_len: usize = buffers.iter().map(|b| b.len()).sum();
    let header = wire::data_header(traceparent, ipc_len)?;
    session.writer.write_all(&header).await.map_err(|e| {
        SaciError::generic(format!(
            "SaciSink: writing a frame header to {peer} failed: {e}"
        ))
    })?;
    for buffer in buffers {
        session.writer.write_all(buffer).await.map_err(|e| {
            SaciError::generic(format!(
                "SaciSink: writing a {ipc_len} byte frame to {peer} failed: {e}"
            ))
        })?;
    }
    session.writer.flush().await.map_err(|e| {
        SaciError::generic(format!("SaciSink: flushing a frame to {peer} failed: {e}"))
    })
}

#[async_trait]
impl Sink for SaciSink {
    /// Encode `batch` and push it as one data frame, opening a session first
    /// if there is none.
    ///
    /// An empty batch writes nothing: the peer's decoder would count it as a
    /// zero-row batch, and the frame carries no information.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a peer refuses the session,
    /// and [`SaciError::Generic`] when no peer accepts one, when the batch
    /// cannot be encoded, or when a write fails. A failed write leaves the
    /// sink with no session, so the next call redials.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut session = match self.session.take() {
            Some(session) => session,
            None => self.open_session().await?,
        };

        #[cfg(feature = "tracing")]
        let span = tracing::debug_span!(
            "peer.send",
            workflow = %self.identity.workflow,
            sink = %self.identity.node,
            peer = %session.peer,
            rows = batch.num_rows(),
        );
        #[cfg(feature = "trace-context")]
        let traceparent = crate::trace::traceparent_of(&span);
        #[cfg(not(feature = "trace-context"))]
        let traceparent: Option<String> = None;

        let peer = session.peer;
        let send = async {
            let buffers = session.encoder.encode(batch).map_err(|e| {
                SaciError::generic(format!("SaciSink: encoding a batch for {peer} failed: {e}"))
            })?;
            write_frame(&mut session, &buffers, traceparent.as_deref()).await
        };
        // Instrumented per poll, so the span is never held as a guard across
        // an await.
        #[cfg(feature = "tracing")]
        let result = tracing::Instrument::instrument(send, span).await;
        #[cfg(not(feature = "tracing"))]
        let result = send.await;

        match result {
            // A failed write leaves the peer's decoder mid-message, so this
            // session is finished; the runner's retry opens a fresh one,
            // possibly onto the next peer.
            Err(e) => Err(e),
            Ok(()) => {
                self.session = Some(session);
                Ok(())
            }
        }
    }

    /// Flush the session and close the write half.
    ///
    /// The peer sees the close between frames, which its reader treats as a
    /// normal disconnect. A sink that never wrote a batch never dialled, and
    /// finishing it does nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] if the flush or the shutdown fails.
    async fn finish(&mut self) -> Result<(), SaciError> {
        let Some(mut session) = self.session.take() else {
            return Ok(());
        };
        let peer = session.peer;
        session.writer.flush().await.map_err(|e| {
            SaciError::generic(format!(
                "SaciSink: flushing the socket to {peer} failed: {e}"
            ))
        })?;
        session.writer.shutdown().await.map_err(|e| {
            SaciError::generic(format!(
                "SaciSink: shutting down the socket to {peer} failed: {e}"
            ))
        })
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_buffer::Buffer;
    use arrow_schema::{DataType, Field};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    fn identity() -> NodeIdentity {
        NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "w1".to_string(),
            node: "out".to_string(),
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .expect("batch")
    }

    /// What a stand-in peer answers the hello with.
    enum Answer {
        /// Take the connection and write nothing back.
        Silence,
        /// One whole frame.
        Frame(Frame),
        /// A length prefix announcing that many body bytes, and no body.
        Announcing(usize),
    }

    /// The hello body a stand-in peer read, and its still open socket: the
    /// task hands the socket back rather than dropping it, so a frame the sink
    /// writes after the answer still lands.
    type Dialled = (Vec<u8>, TcpStream);

    /// Bind a stand-in peer that takes one dial, reads the hello and answers
    /// `answer`.
    async fn stand_in_peer(answer: Answer) -> (SocketAddr, JoinHandle<Dialled>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a stand-in peer");
        let addr = listener.local_addr().expect("the stand-in peer's address");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept the dial");
            let hello = read_body(&mut stream).await;
            match answer {
                Answer::Silence => std::future::pending().await,
                Answer::Frame(frame) => {
                    let mut out = Vec::new();
                    wire::encode_frame(&mut out, &frame).expect("encode the answer");
                    stream.write_all(&out).await.expect("write the answer");
                    stream.flush().await.expect("flush the answer");
                }
                Answer::Announcing(len) => {
                    let prefix = u32::try_from(len).expect("a prefix that fits");
                    stream
                        .write_all(&prefix.to_be_bytes())
                        .await
                        .expect("write a length prefix");
                    stream.flush().await.expect("flush the prefix");
                }
            }
            (hello, stream)
        });
        (addr, handle)
    }

    /// Read one length-prefixed frame body.
    async fn read_body(stream: &mut TcpStream) -> Vec<u8> {
        let mut len = [0u8; 4];
        stream
            .read_exact(&mut len)
            .await
            .expect("read a frame length");
        let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
        stream
            .read_exact(&mut body)
            .await
            .expect("read a frame body");
        body
    }

    /// The identity a hello body names.
    fn hello_identity(body: Vec<u8>) -> NodeIdentity {
        match wire::decode_frame(Buffer::from(body)).expect("decode the hello") {
            Frame::Hello { identity, .. } => identity,
            other => panic!("expected a hello, got {other:?}"),
        }
    }

    #[test]
    fn connect_resolves_every_peer_in_order() {
        let sink = SaciSink::connect(
            &["127.0.0.1:9701".to_string(), "127.0.0.1:9702".to_string()],
            schema(),
            identity(),
            Duration::from_secs(5),
        )
        .expect("resolve");
        let peers = sink.peers();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].port(), 9701);
        assert_eq!(peers[1].port(), 9702);
    }

    #[test]
    fn an_empty_connect_list_is_a_configuration_error() {
        let err = SaciSink::connect(&[], schema(), identity(), Duration::from_secs(5))
            .err()
            .expect("must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("names no address"), "got: {err}");
    }

    #[test]
    fn an_unresolvable_peer_is_a_configuration_error_naming_it() {
        let err = SaciSink::connect(
            &["no-such-host.invalid:9701".to_string()],
            schema(),
            identity(),
            Duration::from_secs(5),
        )
        .err()
        .expect("must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("no-such-host.invalid:9701"),
            "got: {err}"
        );
    }

    /// Arrow refuses a dictionary whose values are themselves a dictionary, so
    /// such a schema is this sink's own configuration error at build time,
    /// with no peer involved in the verdict.
    #[test]
    fn a_schema_arrow_cannot_encode_is_a_configuration_error() {
        let nested = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8),
            )),
        );
        let err = SaciSink::connect(
            &["127.0.0.1:9701".to_string()],
            Arc::new(Schema::new(vec![Field::new("v", nested, true)])),
            identity(),
            Duration::from_secs(5),
        )
        .err()
        .expect("must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string()
                .contains("SaciSink: this schema cannot be encoded as Arrow IPC"),
            "got: {err}"
        );
    }

    /// An empty batch is not a frame: nothing is dialled for it, so the write
    /// succeeds with every peer down.
    #[tokio::test]
    async fn an_empty_batch_opens_no_session() {
        let mut sink = SaciSink::connect(
            &["127.0.0.1:1".to_string()],
            schema(),
            identity(),
            Duration::from_millis(50),
        )
        .expect("resolve");
        let empty = RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .expect("empty batch");
        sink.write_batch(&empty).await.expect("no frame, no dial");
        sink.finish().await.expect("nothing to finish");
    }

    #[tokio::test]
    async fn every_peer_down_names_all_of_them() {
        let mut sink = SaciSink::connect(
            &["127.0.0.1:1".to_string(), "127.0.0.1:2".to_string()],
            schema(),
            identity(),
            Duration::from_millis(200),
        )
        .expect("resolve");
        let err = sink.write_batch(&batch()).await.expect_err("must fail");
        let text = err.to_string();
        assert!(text.contains("no peer accepted a session"), "got: {text}");
        assert!(text.contains("127.0.0.1:1"), "got: {text}");
        assert!(text.contains("127.0.0.1:2"), "got: {text}");
    }

    /// A peer nothing is listening on is skipped rather than fatal, and the
    /// next entry gets the hello and the batch.
    #[tokio::test]
    async fn an_unreachable_first_peer_fails_over_to_the_next() {
        let (addr, accepted) = stand_in_peer(Answer::Frame(Frame::Accept)).await;

        let mut sink = SaciSink::connect(
            &["127.0.0.1:1".to_string(), addr.to_string()],
            schema(),
            identity(),
            Duration::from_secs(5),
        )
        .expect("resolve");
        sink.write_batch(&batch()).await.expect("write");

        let (hello, mut stream) = accepted.await.expect("peer task");
        assert_eq!(hello_identity(hello), identity());
        assert!(matches!(
            wire::decode_frame(Buffer::from(read_body(&mut stream).await)).expect("decode data"),
            Frame::Data { .. }
        ));
        assert_eq!(sink.peers()[1], addr);
    }

    /// A peer that takes the connection and says nothing is down as far as the
    /// sink is concerned, once the handshake timeout is up.
    #[tokio::test]
    async fn a_peer_that_never_answers_is_unreachable() {
        let (silent, _silent) = stand_in_peer(Answer::Silence).await;
        let (accepting, accepted) = stand_in_peer(Answer::Frame(Frame::Accept)).await;

        let mut sink = SaciSink::connect(
            &[silent.to_string(), accepting.to_string()],
            schema(),
            identity(),
            Duration::from_millis(200),
        )
        .expect("resolve");
        sink.write_batch(&batch())
            .await
            .expect("the second peer accepts");

        let (hello, _stream) = accepted.await.expect("peer task");
        assert_eq!(hello_identity(hello), identity());
    }

    /// Only accept and reject answer a hello: anything else is a peer not
    /// speaking this protocol, which is a failed dial rather than a verdict.
    #[tokio::test]
    async fn a_peer_answering_with_a_data_frame_is_unreachable() {
        let (rude, _rude) = stand_in_peer(Answer::Frame(Frame::Data {
            traceparent: None,
            ipc: Buffer::from(vec![1u8, 2, 3]),
        }))
        .await;
        let (accepting, accepted) = stand_in_peer(Answer::Frame(Frame::Accept)).await;

        let mut sink = SaciSink::connect(
            &[rude.to_string(), accepting.to_string()],
            schema(),
            identity(),
            Duration::from_secs(5),
        )
        .expect("resolve");
        sink.write_batch(&batch())
            .await
            .expect("the second peer accepts");

        let (hello, _stream) = accepted.await.expect("peer task");
        assert_eq!(hello_identity(hello), identity());
    }

    /// A reply body above the cap is a peer not speaking this protocol, so it
    /// is a failed dial and not a refusal: the error is generic, which is what
    /// lets the sink try another peer.
    #[tokio::test]
    async fn a_reply_longer_than_the_cap_is_unreachable() {
        let (addr, _task) = stand_in_peer(Answer::Announcing(MAX_REPLY_BYTES + 1)).await;

        let mut sink = SaciSink::connect(
            &[addr.to_string()],
            schema(),
            identity(),
            Duration::from_secs(5),
        )
        .expect("resolve");
        let err = sink.write_batch(&batch()).await.expect_err("must fail");
        assert_eq!(err.category(), "generic", "got: {err}");
        assert!(
            err.to_string().contains("no peer accepted a session"),
            "got: {err}"
        );
        assert!(
            err.to_string()
                .contains("which is not a reply this protocol has"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn a_refusal_is_returned_rather_than_hidden_behind_the_next_peer() {
        let (addr, _task) = stand_in_peer(Answer::Frame(Frame::Reject {
            reason: "schema mismatch: no".to_string(),
        }))
        .await;

        let mut sink = SaciSink::connect(
            &[addr.to_string(), "127.0.0.1:9999".to_string()],
            schema(),
            identity(),
            Duration::from_secs(5),
        )
        .expect("resolve");
        let err = sink.write_batch(&batch()).await.expect_err("must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("refused the session"),
            "got: {err}"
        );
        assert!(err.to_string().contains("schema mismatch"), "got: {err}");
    }

    /// A failed write drops the session, so the next batch redials and opens
    /// the new session with its own hello.
    #[tokio::test]
    async fn a_failed_write_redials_with_a_new_hello() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a stand-in peer");
        let addr = listener.local_addr().expect("the stand-in peer's address");
        let peer = tokio::spawn(async move {
            let accept = {
                let mut out = Vec::new();
                wire::encode_frame(&mut out, &Frame::Accept).expect("encode accept");
                out
            };
            // The first session is accepted and then dropped mid-session.
            let (mut first, _) = listener.accept().await.expect("accept the first dial");
            read_body(&mut first).await;
            first.write_all(&accept).await.expect("write accept");
            first.flush().await.expect("flush accept");
            drop(first);
            // The redial, whose first frame must be a hello of its own.
            let (mut second, _) = listener.accept().await.expect("accept the redial");
            let hello = read_body(&mut second).await;
            second
                .write_all(&accept)
                .await
                .expect("write the second accept");
            second.flush().await.expect("flush the second accept");
            let data = read_body(&mut second).await;
            (hello, data)
        });

        let mut sink = SaciSink::connect(
            &[addr.to_string()],
            schema(),
            identity(),
            Duration::from_secs(5),
        )
        .expect("resolve");
        // A write after the peer closed may still succeed, because the kernel
        // buffers it, so the failure is waited for rather than assumed.
        let mut failed = false;
        for _ in 0..50 {
            if sink.write_batch(&batch()).await.is_err() {
                failed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(failed, "writing to a closed peer must fail");
        assert!(
            sink.session.is_none(),
            "a failed write must leave no session behind"
        );

        sink.write_batch(&batch())
            .await
            .expect("the redial must open a new session");
        let (hello, data) = peer.await.expect("peer task");
        assert_eq!(hello_identity(hello), identity());
        assert!(matches!(
            wire::decode_frame(Buffer::from(data)).expect("decode data"),
            Frame::Data { .. }
        ));
    }
}
