//! [`TcpSink`]: client-mode TCP [`Sink`], the mirror of [`TcpSource`].
//!
//! [`TcpSource`]: crate::connector::tcp::TcpSource
//!
//! The source listens and reads frames; this sink connects out and writes them.
//! One frame per encoded message: a `u32` big-endian length prefix, then
//! exactly that many payload bytes, the same convention the source's frame
//! reader consumes. The two halves are wire compatible, so a `TcpSink` in one
//! service feeds a `TcpSource` in another when both name the same format.
//!
//! Payload bytes come from the codec's message surface
//! ([`encode_messages`](Codec::encode_messages)), the same surface the
//! Kafka and NATS sinks publish from. Framing is transport and belongs here;
//! what a payload means does not.
//!
//! Whether the format has a message surface at all is not a write failure but
//! a property of the config, and the answer is the same for every batch.
//! [`new`](TcpSink::new) therefore settles it while building and hands
//! the refusal back, the way the source opens a decoder while binding. The
//! question is put differently only because the trait offers nothing else:
//! there is no encoder to open, and `encode_messages` needs a batch, so what
//! is answerable without one is
//! [`message_shape`](Codec::message_shape), whose contract is that
//! `None` means the format has no message surface. That is the same gate the
//! Kafka and NATS sinks already stand on. A sink that deferred the answer
//! would build, dial, and fail its first write with the pipeline already
//! live.

use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use rustls_pki_types::ServerName;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::codec::Codec;
use crate::connector::tcp::TcpSinkConfig;
use crate::error::SaciError;
use crate::io::Sink;

/// Client-mode TCP sink: one encoded message out, one length-prefixed frame on
/// the wire.
///
/// [`new`](Self::new) resolves the address and opens nothing, so
/// `saci-service validate` passes while the peer is down. The socket is dialled
/// on the first [`write_batch`](Sink::write_batch), which is where an
/// unreachable peer surfaces.
///
/// One connection serves the sink's whole lifetime. There is no reconnect: once
/// a dial has succeeded and the peer later goes away, every following write
/// fails on that same socket and the error reaches the runner.
///
/// The socket is buffered, and every `write_batch` flushes it once after its
/// last frame, so a batch's frames reach the kernel in as few writes as the
/// buffer allows and nothing is left behind when the call returns.
enum TcpClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncWrite for TcpClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(&mut **s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(&mut **s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(&mut **s).poll_shutdown(cx),
        }
    }
}

pub struct TcpSink {
    /// `None` until the first batch dials the peer.
    stream: Option<BufWriter<TcpClientStream>>,
    /// Resolved in [`new`](Self::new), so a malformed address is a
    /// config error rather than a first-batch one.
    peer: SocketAddr,
    schema: Arc<Schema>,
    codec: Arc<dyn Codec>,
    tls: Option<(TlsConnector, ServerName<'static>)>,
}

impl TcpSink {
    /// Resolve `config.connect` and prepare the sink without opening a socket.
    ///
    /// `config.connect` is a `host:port` string. Its first resolved address is
    /// the peer for the sink's lifetime. `codec` encodes each batch into the
    /// payloads to frame, and a format that declares no message shape has no
    /// message surface to encode with: that is settled here rather than on the
    /// first batch, because no batch can change the answer.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `codec` declares no
    /// [`message_shape`](Codec::message_shape), if `tls` files or server name
    /// are invalid, if `connect` cannot be parsed or resolved, or if it
    /// resolves to no address at all.
    pub fn new(
        config: TcpSinkConfig,
        schema: Arc<Schema>,
        codec: Arc<dyn Codec>,
    ) -> Result<Self, SaciError> {
        let TcpSinkConfig { connect, tls } = config;
        // The declaration is the whole answer for every batch this sink will
        // ever encode, so asking once here is what makes a format with no
        // message surface a config error the caller is told about, rather than
        // a sink that builds, dials, and fails its first write. There is no
        // encoder to open the way the source opens a decoder: `encode_messages`
        // is the only encode entry point and it needs a batch.
        if codec.message_shape().is_none() {
            return Err(SaciError::configuration(format!(
                "TcpSink: format '{}' has no message codec",
                codec.format()
            )));
        }
        let tls = match tls {
            Some(cfg) => {
                let connector =
                    TlsConnector::from(crate::tls::load_client_config("TcpSink", &cfg, &[])?);
                let name = crate::tls::resolve_server_name(
                    "TcpSink",
                    &connect,
                    cfg.server_name.as_deref(),
                )?;
                Some((connector, name))
            }
            None => None,
        };

        let peer = connect
            .to_socket_addrs()
            .map_err(|e| {
                SaciError::configuration(format!(
                    "TcpSink: cannot resolve 'connect' address '{connect}': {e}"
                ))
            })?
            .next()
            .ok_or_else(|| {
                SaciError::configuration(format!(
                    "TcpSink: 'connect' address '{connect}' resolved to no address"
                ))
            })?;

        Ok(Self {
            stream: None,
            peer,
            schema,
            codec,
            tls,
        })
    }

    /// The peer this sink writes to, with the configured address resolved.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

/// Write one frame per payload: a `u32` big-endian length, then the bytes.
///
/// The header and the payload are two writes into the buffered stream rather
/// than one copied buffer, so a batch's payloads reach the socket without
/// being staged again, and the stream is flushed once after the last frame.
async fn write_frames(
    stream: &mut BufWriter<TcpClientStream>,
    payloads: &[Vec<u8>],
    peer: SocketAddr,
) -> Result<(), SaciError> {
    for payload in payloads {
        let len = u32::try_from(payload.len()).map_err(|_| {
            SaciError::generic(format!(
                "TcpSink: message of {} bytes does not fit the u32 frame length prefix",
                payload.len()
            ))
        })?;
        stream.write_all(&len.to_be_bytes()).await.map_err(|e| {
            SaciError::generic(format!(
                "TcpSink: writing a frame header to {peer} failed: {e}"
            ))
        })?;
        stream.write_all(payload).await.map_err(|e| {
            SaciError::generic(format!(
                "TcpSink: writing a {len} byte frame payload to {peer} failed: {e}"
            ))
        })?;
    }
    stream
        .flush()
        .await
        .map_err(|e| SaciError::generic(format!("TcpSink: flushing frames to {peer} failed: {e}")))
}

#[async_trait]
impl Sink for TcpSink {
    /// Encode `batch` and write one frame per payload, dialling the peer first
    /// if this is the first batch.
    ///
    /// A batch the codec encodes to no payloads writes no frames.
    ///
    /// # Errors
    ///
    /// Returns the codec's error when the batch cannot be encoded, and
    /// [`SaciError::Generic`] when the dial fails, a payload exceeds the `u32`
    /// length prefix, or a socket write fails.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        let payloads = self.codec.encode_messages(batch)?;

        // The connection moves out of `self` for the write and back in after,
        // so the frame loop owns it outright instead of reborrowing an option.
        let mut stream = match self.stream.take() {
            Some(stream) => stream,
            None => {
                let tcp = TcpStream::connect(self.peer).await.map_err(|e| {
                    SaciError::generic(format!("TcpSink: cannot connect to {}: {e}", self.peer))
                })?;
                let inner = match &self.tls {
                    Some((connector, name)) => {
                        let tls = connector.connect(name.clone(), tcp).await.map_err(|e| {
                            SaciError::generic(format!(
                                "TcpSink: TLS handshake with {} failed: {e}",
                                self.peer
                            ))
                        })?;
                        TcpClientStream::Tls(Box::new(tls))
                    }
                    None => TcpClientStream::Plain(tcp),
                };
                BufWriter::new(inner)
            }
        };
        let result = write_frames(&mut stream, &payloads, self.peer).await;
        self.stream = Some(stream);
        result
    }

    /// Flush the socket and close the write half.
    ///
    /// The peer sees the close between frames, which its reader treats as a
    /// normal disconnect. A sink that never wrote a batch never connected, and
    /// finishing it does nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] if the flush or the shutdown fails.
    async fn finish(&mut self) -> Result<(), SaciError> {
        let peer = self.peer;
        let Some(stream) = self.stream.as_mut() else {
            return Ok(());
        };
        stream.flush().await.map_err(|e| {
            SaciError::generic(format!(
                "TcpSink: flushing the socket to {peer} failed: {e}"
            ))
        })?;
        stream.shutdown().await.map_err(|e| {
            SaciError::generic(format!(
                "TcpSink: shutting down the socket to {peer} failed: {e}"
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
    use crate::codec::arrow_ipc::ArrowIpcCodec;
    use crate::codec::{MessageDecoder, MessageShape};
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// Framing is what this crate owns; the payload encoding is the codec's,
    /// and its own tests cover the encode paths.
    fn arrow_ipc() -> Arc<dyn Codec> {
        Arc::new(ArrowIpcCodec::new())
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("build batch")
    }

    /// A format that encodes every batch to no messages at all, which is the
    /// only way to observe "no payloads, no frames" without a real codec that
    /// does it. It declares a shape, so it must carry a whole message surface:
    /// the decoder half is `arrow-ipc`'s, unexercised here but real, because
    /// [`Codec::message_shape`] answers for both directions at once.
    struct NoMessages;

    impl Codec for NoMessages {
        fn format(&self) -> &'static str {
            "no-messages"
        }

        fn open_message_decoder(
            &self,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn MessageDecoder>, SaciError> {
            ArrowIpcCodec::new().open_message_decoder(schema)
        }

        fn encode_messages(&self, _batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
            Ok(Vec::new())
        }

        fn message_shape(&self) -> Option<MessageShape> {
            Some(MessageShape::PerBatch)
        }
    }

    /// A format with no message surface at all: every message method is the
    /// trait's default. Every format SACI registers implements the message
    /// surface, so the gate is exercised against a stand-in.
    struct NoMessageSurface;

    impl Codec for NoMessageSurface {
        fn format(&self) -> &'static str {
            "no-surface"
        }
    }

    /// Read one frame: the `u32` big-endian prefix, then that many bytes.
    async fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await.expect("frame header");
        let len = u32::from_be_bytes(header) as usize;
        let mut payload = vec![0u8; len];
        stream
            .read_exact(&mut payload)
            .await
            .expect("frame payload");
        payload
    }

    /// A sink dialling `connect` with `codec`.
    fn sink_to(connect: &str, codec: Arc<dyn Codec>) -> Result<TcpSink, SaciError> {
        TcpSink::new(TcpSinkConfig::new(connect), test_schema(), codec)
    }

    #[test]
    fn connect_resolves_the_configured_address() {
        let sink = sink_to("127.0.0.1:9500", arrow_ipc()).expect("resolve");
        assert_eq!(sink.peer_addr().port(), 9500);
    }

    #[test]
    fn connect_rejects_an_address_with_no_port() {
        let err = sink_to("127.0.0.1", arrow_ipc())
            .err()
            .expect("resolution must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    /// The refusal is the config's, not the first batch's: nothing about a
    /// batch can change whether the format has a message surface, and the same
    /// misconfiguration on a `tcp` source is refused while building too.
    #[test]
    fn connect_refuses_a_format_with_no_message_codec() {
        let err = sink_to("127.0.0.1:9501", Arc::new(NoMessageSurface))
            .err()
            .expect("a format with no message surface must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("no message codec"), "got: {err}");
        assert!(err.to_string().contains("no-surface"), "got: {err}");
    }

    /// A peer that is down is not a construction error: the dial waits for the
    /// first batch so `saci-service validate` passes without the peer.
    #[test]
    fn connect_opens_no_socket() {
        let sink = sink_to("127.0.0.1:1", arrow_ipc()).expect("resolve");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    /// The framing claim: one `u32` big-endian prefix, exactly that many bytes
    /// after it, and nothing else on the wire for a one-message batch.
    #[tokio::test]
    async fn write_batch_writes_one_length_prefixed_frame_per_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let accept = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            let payload = read_frame(&mut peer).await;
            let mut rest = Vec::new();
            peer.read_to_end(&mut rest).await.expect("read to close");
            (payload.len(), rest.len())
        });

        let mut sink = sink_to(&addr.to_string(), arrow_ipc()).expect("resolve");
        sink.write_batch(&batch(&[1, 2, 3])).await.expect("write");
        sink.finish().await.expect("finish");

        let (payload_len, trailing) = accept.await.expect("reader task");
        assert!(payload_len > 0, "the prefix announced a real payload");
        assert_eq!(trailing, 0, "arrow-ipc emits one message per batch");
    }

    #[tokio::test]
    async fn a_frame_arrives_wire_compatible_with_the_source_decoder() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let accept = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            read_frame(&mut peer).await
        });

        let mut sink = sink_to(&addr.to_string(), arrow_ipc()).expect("resolve");
        sink.write_batch(&batch(&[7, 8])).await.expect("write");
        sink.finish().await.expect("finish");

        let payload = accept.await.expect("reader task");
        let mut decoder = arrow_ipc()
            .open_message_decoder(test_schema())
            .expect("decoder");
        decoder.push(&payload).expect("push");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 2);
        let column = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v is Int64");
        assert_eq!(column.values(), &[7, 8]);
    }

    #[tokio::test]
    async fn a_batch_that_encodes_to_no_messages_writes_no_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let mut sink = sink_to(&addr.to_string(), Arc::new(NoMessages)).expect("resolve");
        sink.write_batch(&batch(&[1])).await.expect("write");
        sink.finish().await.expect("finish");

        let (mut peer, _) = listener.accept().await.expect("the sink still connected");
        let mut buf = [0u8; 4];
        let read = peer.read(&mut buf).await.expect("read");
        assert_eq!(read, 0, "no frames, just the close");
    }

    #[tokio::test]
    async fn finish_without_a_batch_is_ok() {
        let mut sink = sink_to("127.0.0.1:1", arrow_ipc()).expect("resolve");
        sink.finish().await.expect("finishing an unconnected sink");
    }

    #[tokio::test]
    async fn write_batch_reports_an_unreachable_peer() {
        // Bind, capture the port, then drop the listener: nothing is listening
        // on that port, so the dial fails rather than hanging.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        drop(listener);

        let mut sink = sink_to(&addr.to_string(), arrow_ipc()).expect("resolve");
        let err = sink
            .write_batch(&batch(&[1]))
            .await
            .expect_err("the dial must fail");
        assert!(err.to_string().contains("cannot connect"), "got: {err}");
    }

    /// The sink buffers its socket, so a batch's frames must be flushed before
    /// `write_batch` returns: the peer reads the frame with the sink still
    /// open, and `finish` is never called.
    #[tokio::test]
    async fn write_batch_flushes_its_frames_before_returning() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let mut sink = sink_to(&addr.to_string(), arrow_ipc()).expect("resolve");
        sink.write_batch(&batch(&[4, 5, 6])).await.expect("write");

        let (mut peer, _) = listener.accept().await.expect("accept");
        let payload =
            tokio::time::timeout(std::time::Duration::from_secs(5), read_frame(&mut peer))
                .await
                .expect("the frame was left in the sink's buffer");
        assert!(!payload.is_empty());
    }

    /// A payload larger than the write buffer takes the direct-write path; it
    /// must still arrive whole, in order, behind the next frame's header.
    #[tokio::test]
    async fn a_payload_larger_than_the_buffer_arrives_intact() {
        let values: Vec<i64> = (0..50_000).collect();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let accept = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.expect("accept");
            let big = read_frame(&mut peer).await;
            let small = read_frame(&mut peer).await;
            (big, small)
        });

        let mut sink = sink_to(&addr.to_string(), arrow_ipc()).expect("resolve");
        sink.write_batch(&batch(&values)).await.expect("write big");
        sink.write_batch(&batch(&[9])).await.expect("write small");
        sink.finish().await.expect("finish");

        let (big, small) = accept.await.expect("reader task");
        let decode = |payload: &[u8]| {
            let mut decoder = arrow_ipc()
                .open_message_decoder(test_schema())
                .expect("decoder");
            decoder.push(payload).expect("push");
            decoder.flush().expect("flush").expect("one batch")
        };
        let big = decode(&big);
        assert_eq!(big.num_rows(), values.len());
        let column = big
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v is Int64");
        assert_eq!(&column.values()[..], values.as_slice());
        assert_eq!(decode(&small).num_rows(), 1);
    }
}
