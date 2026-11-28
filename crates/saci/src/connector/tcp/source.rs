//! [`TcpSource`]: live TCP [`Source`] for stream mode.
//!
//! Listens on a bind address and yields one [`RecordBatch`] per received frame.
//! It never reaches EOF: `next_batch` blocks until the next frame arrives, so
//! only the stream runner (`run "stream"`) can drive it. The batch
//! loop's [`read_all`](crate::io::read_all) would
//! block forever, and the service config validator rejects that combination.
//!
//! ## Frame format
//!
//! A `u32` big-endian length prefix, then exactly that many payload bytes: the
//! same convention the Raft transport uses
//! (`distributed::consensus::transport::wire`). Framing is transport, so this
//! crate owns it; what the payload bytes mean is the
//! [`Codec`] the host resolved from the
//! source's `codec` key.
//!
//! A frame carrying several batches yields their concatenation, because the
//! decoder is asked for one batch per frame and folds whatever that frame held.
//!
//! A clean connection close between frames is a normal disconnect. A protocol
//! violation (oversized frame, undecodable payload, a payload the declared
//! schema cannot project onto) closes only that connection; the listener stays
//! up and other producers are unaffected. Concurrent connections are accepted;
//! ordering holds within a connection but not across them.
//!
//! Whether the format can decode a message at all is not a protocol violation
//! but a property of the config, and the answer is the same for every
//! connection. [`new`](TcpSource::new) therefore asks the codec
//! for a decoder while building and hands the refusal back, the way
//! `FileSource::new` asks its codec for a reader. A source that
//! deferred that answer would accept every connection, close it, and deliver
//! nothing.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use crate::codec::Codec;
use crate::connector::tcp::TcpSourceConfig;
use crate::error::SaciError;
use crate::io::{CorruptRecord, Source};

/// Live TCP ingestion source: one frame in, one `RecordBatch` out.
///
/// The listener socket is bound in [`new`](Self::new) synchronously, so bind
/// failures surface at config time. The accept loop is spawned on the first
/// [`next_batch`](Source::next_batch) call, which always runs inside a tokio
/// runtime.
///
/// Backpressure comes from the channel: once `buffer` batches are queued,
/// reader tasks stop consuming their sockets and TCP flow control pushes back
/// on the producers.
pub struct TcpSource {
    listener: Option<std::net::TcpListener>,
    local_addr: SocketAddr,
    schema: Arc<Schema>,
    codec: Arc<dyn Codec>,
    tx: mpsc::Sender<RecordBatch>,
    rx: mpsc::Receiver<RecordBatch>,
    max_frame_bytes: usize,
    tls: Option<TlsAcceptor>,
    listener_task: Option<JoinHandle<()>>,
    skip_corrupt: bool,
    corrupt: Arc<Mutex<Vec<CorruptRecord>>>,
}

impl TcpSource {
    /// Bind `config.bind` and prepare the ingest channel.
    ///
    /// `config.buffer` is the number of decoded batches that may queue before
    /// backpressure reaches the producers. `config.max_frame_bytes` caps a
    /// single frame; a producer that announces more has its connection
    /// closed. `codec` decodes each frame's payload; every accepted
    /// connection opens its own decoder from it, and one is opened here to
    /// settle whether this format can decode a message against `schema` at
    /// all.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `buffer` or `max_frame_bytes`
    /// is `0`, if `tls` files are missing or invalid, if `codec` cannot open a
    /// message decoder for `schema`, or if `bind` cannot be bound.
    pub fn new(
        config: TcpSourceConfig,
        schema: Arc<Schema>,
        codec: Arc<dyn Codec>,
    ) -> Result<Self, SaciError> {
        config.validate()?;
        // Opening a decoder reads `schema` and nothing else, so every
        // connection would get this same answer. Asking here is what makes a
        // format with no message decoder a config error the caller is told
        // about, rather than a listener that accepts, closes, and delivers
        // nothing at all.
        codec.open_message_decoder(Arc::clone(&schema))?;

        let TcpSourceConfig {
            bind,
            buffer,
            max_frame_bytes,
            tls,
        } = config;
        let tls = tls
            .as_ref()
            .map(|cfg| crate::tls::load_server_config("TcpSource", cfg, &[]).map(TlsAcceptor::from))
            .transpose()?;
        let listener = std::net::TcpListener::bind(&bind).map_err(|e| {
            SaciError::configuration(format!("TcpSource: cannot bind '{bind}': {e}"))
        })?;
        listener.set_nonblocking(true).map_err(|e| {
            SaciError::configuration(format!("TcpSource: set_nonblocking failed: {e}"))
        })?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| SaciError::configuration(format!("TcpSource: local_addr failed: {e}")))?;

        let (tx, rx) = mpsc::channel(buffer);

        Ok(Self {
            listener: Some(listener),
            local_addr,
            schema,
            codec,
            tx,
            rx,
            max_frame_bytes,
            tls,
            listener_task: None,
            skip_corrupt: false,
            corrupt: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// The bound address, with any ephemeral port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Convert the std listener and spawn the accept loop. Idempotent.
    fn ensure_listening(&mut self) {
        let Some(std_listener) = self.listener.take() else {
            return;
        };
        let listener = match TcpListener::from_std(std_listener) {
            Ok(l) => l,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(error = %_e, "TcpSource: cannot adopt listener into runtime");
                return;
            }
        };

        let tx = self.tx.clone();
        let schema = Arc::clone(&self.schema);
        let codec = Arc::clone(&self.codec);
        let max_frame_bytes = self.max_frame_bytes;
        let tls = self.tls.clone();
        let skip_corrupt = self.skip_corrupt;
        let corrupt = Arc::clone(&self.corrupt);

        self.listener_task = Some(tokio::spawn(async move {
            loop {
                let (stream, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %_e, "TcpSource: accept failed");
                        continue;
                    }
                };

                let tx = tx.clone();
                let schema = Arc::clone(&schema);
                let codec = Arc::clone(&codec);
                let tls = tls.clone();
                let corrupt = Arc::clone(&corrupt);
                tokio::spawn(async move {
                    if let Some(acceptor) = tls {
                        match tokio::time::timeout(Duration::from_secs(30), acceptor.accept(stream))
                            .await
                        {
                            Ok(Ok(accepted)) => {
                                read_connection(
                                    accepted,
                                    tx,
                                    schema,
                                    codec,
                                    max_frame_bytes,
                                    skip_corrupt,
                                    corrupt,
                                )
                                .await;
                            }
                            Ok(Err(_e)) => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(error = %_e, "TcpSource: TLS handshake failed");
                            }
                            Err(_) => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!("TcpSource: TLS handshake timed out");
                            }
                        }
                    } else {
                        read_connection(
                            stream,
                            tx,
                            schema,
                            codec,
                            max_frame_bytes,
                            skip_corrupt,
                            corrupt,
                        )
                        .await;
                    }
                });
            }
        }));
    }
}

/// Read length-prefixed frames from one connection until it closes or violates
/// the protocol.
///
/// One decoder serves the whole connection: `flush` resets it, so a frame's
/// batch never leaks into the next frame's. [`TcpSource::new`] already
/// opened one off the same schema, so the open below fails only for a
/// codec that answers differently twice.
async fn read_connection<S: AsyncRead + Unpin>(
    stream: S,
    tx: mpsc::Sender<RecordBatch>,
    schema: Arc<Schema>,
    codec: Arc<dyn Codec>,
    max_frame_bytes: usize,
    skip_corrupt: bool,
    corrupt: Arc<Mutex<Vec<CorruptRecord>>>,
) {
    let mut decoder = match codec.open_message_decoder(schema) {
        Ok(mut decoder) => {
            decoder.set_skip_corrupt(skip_corrupt);
            decoder
        }
        Err(_e) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_e, "TcpSource: cannot decode this format, closing connection");
            return;
        }
    };

    // One buffered reader and one frame buffer serve the whole connection: a
    // small frame costs the socket a single read instead of two, and the
    // buffer is refilled in place rather than allocated per frame.
    let mut reader = BufReader::new(stream);
    let mut frame: Vec<u8> = Vec::new();

    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            // Clean close between frames: the normal disconnect path.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "TcpSource: frame header read failed");
                return;
            }
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 {
            continue;
        }
        if len > max_frame_bytes {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                frame_bytes = len,
                max_frame_bytes,
                "TcpSource: oversized frame, closing connection"
            );
            return;
        }

        frame.clear();
        frame.resize(len, 0);
        if let Err(_e) = reader.read_exact(&mut frame).await {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_e, "TcpSource: truncated frame payload, closing connection");
            return;
        }

        let decoded = decoder.push(&frame).and_then(|()| decoder.flush());
        let drained = decoder.drain_corrupt();
        if !drained.is_empty()
            && let Ok(mut guard) = corrupt.lock()
        {
            guard.extend(drained);
        }
        let batch = match decoded {
            Ok(Some(batch)) => batch,
            // A frame that decodes to no rows at all is isolated if skip_corrupt is on,
            // otherwise it is a protocol violation.
            Ok(None) => {
                if skip_corrupt {
                    continue;
                }
                #[cfg(feature = "tracing")]
                tracing::warn!("TcpSource: frame decoded no batch");
                return;
            }
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "TcpSource: bad frame, closing connection");
                return;
            }
        };

        // Bounded send, so TCP carries the backpressure. A send error means the
        // source was dropped and this connection has nowhere to go.
        if tx.send(batch).await.is_err() {
            return;
        }
    }
}

#[async_trait]
impl Source for TcpSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn set_skip_corrupt(&mut self, skip: bool) {
        self.skip_corrupt = skip;
    }

    fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
        self.corrupt
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_listening();
        // `None` here means the accept task is gone (it never drops `tx`
        // otherwise), which is terminal for this source.
        Ok(self.rx.recv().await)
    }
}

impl Drop for TcpSource {
    fn drop(&mut self) {
        if let Some(handle) = self.listener_task.take() {
            handle.abort();
        }
        // Per-connection reader tasks exit on their next `tx.send`, which
        // fails once `rx` is dropped with this struct.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::arrow_ipc::ArrowIpcCodec;
    use arrow_schema::{DataType, Field};
    use tokio::net::TcpStream;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// Framing is what this crate owns; the payload encoding is the codec's,
    /// and its own tests cover the decode paths.
    fn arrow_ipc() -> Arc<dyn Codec> {
        Arc::new(ArrowIpcCodec::new())
    }

    #[test]
    fn new_reports_bound_port() {
        let src = TcpSource::new(
            TcpSourceConfig::new("127.0.0.1:0"),
            test_schema(),
            arrow_ipc(),
        )
        .unwrap();
        assert_ne!(src.local_addr().port(), 0);
    }

    #[test]
    fn new_rejects_unbindable_address() {
        let err = TcpSource::new(
            TcpSourceConfig::new("256.256.256.256:1"),
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("bind must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
    }

    /// A stream-only format: every message method keeps the trait's
    /// `unsupported` default. Every format SACI registers implements the message
    /// surface, so the gate is exercised against a stand-in.
    struct StreamOnly;

    impl Codec for StreamOnly {
        fn format(&self) -> &'static str {
            "stream-only"
        }
    }

    #[test]
    fn new_rejects_a_format_with_no_message_decoder() {
        let err = TcpSource::new(
            TcpSourceConfig::new("127.0.0.1:0"),
            test_schema(),
            Arc::new(StreamOnly),
        )
        .err()
        .expect("a format with no message decoder must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.message()
                .contains("does not support decoding discrete messages"),
            "got: {err}"
        );
    }

    #[test]
    fn new_refuses_a_zero_buffer_or_frame_cap() {
        for (buffer, max_frame_bytes, key) in [(0, 1024, "buffer"), (4, 0, "max_frame_bytes")] {
            let config = TcpSourceConfig {
                buffer,
                max_frame_bytes,
                ..TcpSourceConfig::new("127.0.0.1:0")
            };
            let err = TcpSource::new(config, test_schema(), arrow_ipc())
                .err()
                .expect("a zero limit must be refused");
            assert_eq!(err.category(), "configuration", "got: {err}");
            assert!(
                err.to_string()
                    .contains(&format!("'{key}' must be at least 1")),
                "got: {err}"
            );
        }
    }

    /// One frame buffer serves a connection, so a short frame after a long one
    /// must decode from its own bytes alone, and a frame larger than the read
    /// buffer must still arrive whole.
    #[tokio::test]
    async fn frames_of_different_sizes_on_one_connection_decode_independently() {
        use arrow_array::Int64Array;
        use tokio::io::AsyncWriteExt;

        let codec = arrow_ipc();
        let mut src = TcpSource::new(
            TcpSourceConfig::new("127.0.0.1:0"),
            test_schema(),
            Arc::clone(&codec),
        )
        .expect("bind");
        let mut client = TcpStream::connect(src.local_addr())
            .await
            .expect("the listen backlog takes the dial");

        let sizes = [20_000usize, 1, 9_000, 3];
        let mut payloads = Vec::new();
        for (n, size) in sizes.iter().enumerate() {
            let base = (n as i64) * 1_000_000;
            let values: Vec<i64> = (base..base + *size as i64).collect();
            let batch =
                RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(values))])
                    .expect("batch");
            payloads.extend(codec.encode_messages(&batch).expect("encode"));
        }
        // The accept loop starts on the first `next_batch`, so the writes
        // run beside it: written up front they could outgrow the socket
        // buffers with nothing reading yet.
        let writer = tokio::spawn(async move {
            for payload in payloads {
                let len = u32::try_from(payload.len()).expect("a frame that fits");
                client.write_all(&len.to_be_bytes()).await.expect("header");
                client.write_all(&payload).await.expect("payload");
            }
            client.flush().await.expect("flush");
            client
        });

        for (n, size) in sizes.iter().enumerate() {
            let batch = tokio::time::timeout(std::time::Duration::from_secs(5), src.next_batch())
                .await
                .expect("a frame must arrive")
                .expect("next_batch")
                .expect("a batch");
            let base = (n as i64) * 1_000_000;
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("v is Int64");
            assert_eq!(column.len(), *size, "frame {n}");
            assert_eq!(column.value(0), base, "frame {n}");
            assert_eq!(
                column.value(*size - 1),
                base + *size as i64 - 1,
                "frame {n}"
            );
        }
        let _client = writer.await.expect("writer task");
    }
}
