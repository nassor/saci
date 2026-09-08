//! [`TcpIngestSource`]: live TCP [`Source`] for stream mode.
//!
//! Listens on a bind address and yields one [`RecordBatch`] per received frame.
//! It never reaches EOF: `next_batch` blocks until the next frame arrives, so
//! only the stream runner (`run_mode kind="stream"`) can drive it. The batch
//! loop's [`drain_into_dataset`](saci_core::io::source::drain_into_dataset) would
//! block forever, and the service config validator rejects that combination.
//!
//! ## Frame format
//!
//! A `u32` big-endian length prefix, then exactly that many payload bytes: the
//! same convention the Raft transport uses
//! (`distributed::consensus::transport::wire`). Framing is transport, so this
//! crate owns it; what the payload bytes mean is the
//! [`Transformer`] the host resolved from the
//! source's `transformer` key.
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
//! connection. [`new`](TcpIngestSource::new) therefore asks the transformer
//! for a decoder while building and hands the refusal back, the way
//! `FileSource::open` asks its transformer for a reader. A source that
//! deferred that answer would accept every connection, close it, and deliver
//! nothing.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

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
pub struct TcpIngestSource {
    listener: Option<std::net::TcpListener>,
    local_addr: SocketAddr,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    tx: mpsc::Sender<RecordBatch>,
    rx: mpsc::Receiver<RecordBatch>,
    max_frame_bytes: usize,
    listener_task: Option<JoinHandle<()>>,
}

impl TcpIngestSource {
    /// Bind `bind` and prepare the ingest channel.
    ///
    /// `buffer` is the number of decoded batches that may queue before
    /// backpressure reaches the producers. `max_frame_bytes` caps a single
    /// frame; a producer that announces more has its connection closed.
    /// `transformer` decodes each frame's payload; every accepted connection
    /// opens its own decoder from it, and one is opened here to settle whether
    /// this format can decode a message against `schema` at all.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `transformer` cannot open a
    /// message decoder for `schema`, or if `bind` cannot be bound.
    pub fn new(
        bind: &str,
        schema: Arc<Schema>,
        buffer: usize,
        max_frame_bytes: usize,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        // Opening a decoder reads `schema` and nothing else, so every
        // connection would get this same answer. Asking here is what makes a
        // format with no message decoder a config error the caller is told
        // about, rather than a listener that accepts, closes, and delivers
        // nothing at all.
        transformer.open_message_decoder(Arc::clone(&schema))?;

        let listener = std::net::TcpListener::bind(bind).map_err(|e| {
            SaciError::configuration(format!("TcpIngestSource: cannot bind '{bind}': {e}"))
        })?;
        listener.set_nonblocking(true).map_err(|e| {
            SaciError::configuration(format!("TcpIngestSource: set_nonblocking failed: {e}"))
        })?;
        let local_addr = listener.local_addr().map_err(|e| {
            SaciError::configuration(format!("TcpIngestSource: local_addr failed: {e}"))
        })?;

        let (tx, rx) = mpsc::channel(buffer.max(1));

        Ok(Self {
            listener: Some(listener),
            local_addr,
            schema,
            transformer,
            tx,
            rx,
            max_frame_bytes,
            listener_task: None,
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
                tracing::error!(error = %_e, "TcpIngestSource: cannot adopt listener into runtime");
                return;
            }
        };

        let tx = self.tx.clone();
        let schema = Arc::clone(&self.schema);
        let transformer = Arc::clone(&self.transformer);
        let max_frame_bytes = self.max_frame_bytes;

        self.listener_task = Some(tokio::spawn(async move {
            loop {
                let (stream, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %_e, "TcpIngestSource: accept failed");
                        continue;
                    }
                };

                let tx = tx.clone();
                let schema = Arc::clone(&schema);
                let transformer = Arc::clone(&transformer);
                tokio::spawn(async move {
                    read_connection(stream, tx, schema, transformer, max_frame_bytes).await;
                });
            }
        }));
    }
}

/// Read length-prefixed frames from one connection until it closes or violates
/// the protocol.
///
/// One decoder serves the whole connection: `flush` resets it, so a frame's
/// batch never leaks into the next frame's. [`TcpIngestSource::new`] already
/// opened one off the same schema, so the open below fails only for a
/// transformer that answers differently twice.
async fn read_connection(
    mut stream: TcpStream,
    tx: mpsc::Sender<RecordBatch>,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    max_frame_bytes: usize,
) {
    let mut decoder = match transformer.open_message_decoder(schema) {
        Ok(decoder) => decoder,
        Err(_e) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_e, "TcpIngestSource: cannot decode this format, closing connection");
            return;
        }
    };

    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            // Clean close between frames: the normal disconnect path.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "TcpIngestSource: frame header read failed");
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
                "TcpIngestSource: oversized frame, closing connection"
            );
            return;
        }

        let mut payload = vec![0u8; len];
        if let Err(_e) = stream.read_exact(&mut payload).await {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_e, "TcpIngestSource: truncated frame payload, closing connection");
            return;
        }

        let decoded = decoder.push(&payload).and_then(|()| decoder.flush());
        let batch = match decoded {
            Ok(Some(batch)) => batch,
            // A frame that decodes to no rows at all is a protocol violation:
            // the producer sent bytes that carry nothing.
            Ok(None) => {
                #[cfg(feature = "tracing")]
                tracing::warn!("TcpIngestSource: frame decoded no batch");
                return;
            }
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "TcpIngestSource: bad frame, closing connection");
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
impl Source for TcpIngestSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_listening();
        // `None` here means the accept task is gone (it never drops `tx`
        // otherwise), which is terminal for this source.
        Ok(self.rx.recv().await)
    }
}

impl Drop for TcpIngestSource {
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
    use arrow_schema::{DataType, Field};
    use saci_transformer_arrow_ipc::ArrowIpcTransformer;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// Framing is what this crate owns; the payload codec is the transformer's,
    /// and its own tests cover the decode paths.
    fn arrow_ipc() -> Arc<dyn Transformer> {
        Arc::new(ArrowIpcTransformer::new())
    }

    #[test]
    fn new_reports_bound_port() {
        let src = TcpIngestSource::new("127.0.0.1:0", test_schema(), 4, 1024, arrow_ipc()).unwrap();
        assert_ne!(src.local_addr().port(), 0);
    }

    #[test]
    fn new_rejects_unbindable_address() {
        let err = TcpIngestSource::new("256.256.256.256:1", test_schema(), 4, 1024, arrow_ipc())
            .err()
            .expect("bind must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
    }

    /// A stream-only format: every message method keeps the trait's
    /// `unsupported` default. Every format SACI registers implements the message
    /// surface, so the gate is exercised against a stand-in.
    struct StreamOnly;

    impl Transformer for StreamOnly {
        fn format(&self) -> &'static str {
            "stream-only"
        }
    }

    #[test]
    fn new_rejects_a_format_with_no_message_decoder() {
        let err = TcpIngestSource::new("127.0.0.1:0", test_schema(), 4, 1024, Arc::new(StreamOnly))
            .err()
            .expect("a format with no message decoder must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.message()
                .contains("does not support decoding discrete messages"),
            "got: {err}"
        );
    }
}
