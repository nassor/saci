//! [`UdpSource`]: live UDP [`Source`] for stream mode.
//!
//! Binds a datagram socket and yields one [`RecordBatch`] per received
//! datagram. It never reaches EOF: `next_batch` blocks until the next datagram
//! arrives, so only the stream runner (`run_mode kind="stream"`) can drive it.
//! The batch loop's
//! [`drain_into_dataset`](saci_core::io::source::drain_into_dataset) would
//! block forever, and the service config validator rejects that combination.
//!
//! ## Wire rule
//!
//! One datagram carries exactly one payload, with no length prefix: the
//! datagram boundary is the frame. What the payload bytes mean is the
//! [`Transformer`] the host resolved from the source's `transformer` key.
//!
//! A zero-length datagram is a liveness probe. The source answers it with a
//! zero-length datagram to its sender and never decodes it, which is what lets
//! a [`UdpSink`](crate::UdpSink) with `probe kind="echo"` learn that this
//! source is up.
//!
//! A datagram the format cannot decode is dropped with a warning, and so is
//! one that decodes to no rows. There is no connection to close: the next
//! datagram, from that producer or any other, is decoded as if the bad one had
//! never arrived. The decoder is replaced after a failed push, because a
//! decoder's state after one is unspecified.
//!
//! Backpressure is the kernel's. Once `buffer` batches are queued the receive
//! loop stops reading, the socket's receive buffer fills, and the OS drops
//! datagrams. Nothing tells the producer, which is what a datagram transport
//! costs.
//!
//! Whether the format can decode a message at all is not a per-datagram
//! question but a property of the config. [`new`](UdpSource::new) therefore
//! asks the transformer for a decoder while binding and hands the refusal
//! back, the way `TcpIngestSource::new` does.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

use crate::DATAGRAM_BUFFER_BYTES;

/// Live UDP ingestion source: one datagram in, one `RecordBatch` out.
///
/// The socket is bound in [`new`](Self::new) synchronously, so bind failures
/// and a bad multicast group surface at config time. The receive loop is
/// spawned on the first [`next_batch`](Source::next_batch) call, which always
/// runs inside a tokio runtime.
pub struct UdpSource {
    /// Bound in [`new`](Self::new), taken by the first `next_batch`.
    socket: Option<std::net::UdpSocket>,
    local_addr: SocketAddr,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    /// Moved into the receive task with the socket, so `rx` closes exactly
    /// when that task ends.
    tx: Option<mpsc::Sender<RecordBatch>>,
    rx: mpsc::Receiver<RecordBatch>,
    task: Option<JoinHandle<SaciError>>,
    /// The error the receive task ended with. Every later `next_batch`
    /// returns it, so the runner keeps seeing a failure rather than an EOF.
    dead: Option<SaciError>,
}

impl UdpSource {
    /// Bind `bind`, optionally join `multicast_group`, and prepare the batch
    /// channel.
    ///
    /// `buffer` is the number of decoded batches that may queue before the
    /// receive loop stops reading the socket. `transformer` decodes each
    /// datagram; one decoder is opened here to settle whether this format can
    /// decode a message against `schema` at all.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `transformer` cannot open a
    /// message decoder for `schema`, if `bind` cannot be bound, or if
    /// `multicast_group` is not a multicast address, is of a different IP
    /// family than `bind`, or cannot be joined.
    pub fn new(
        bind: &str,
        multicast_group: Option<IpAddr>,
        schema: Arc<Schema>,
        buffer: usize,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        // Opening a decoder reads `schema` and nothing else, so every datagram
        // would get this same answer. Asking here is what makes a format with
        // no message decoder a config error the caller is told about, rather
        // than a socket that receives and delivers nothing at all.
        transformer.open_message_decoder(Arc::clone(&schema))?;

        let socket = std::net::UdpSocket::bind(bind).map_err(|e| {
            SaciError::configuration(format!("UdpSource: cannot bind '{bind}': {e}"))
        })?;
        socket.set_nonblocking(true).map_err(|e| {
            SaciError::configuration(format!("UdpSource: set_nonblocking failed: {e}"))
        })?;
        let local_addr = socket
            .local_addr()
            .map_err(|e| SaciError::configuration(format!("UdpSource: local_addr failed: {e}")))?;

        if let Some(group) = multicast_group {
            join_multicast(&socket, group, local_addr, bind)?;
        }

        let (tx, rx) = mpsc::channel(buffer.max(1));

        Ok(Self {
            socket: Some(socket),
            local_addr,
            schema,
            transformer,
            tx: Some(tx),
            rx,
            task: None,
            dead: None,
        })
    }

    /// The bound address, with any ephemeral port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Adopt the socket into the runtime and spawn the receive loop.
    /// Idempotent.
    fn ensure_receiving(&mut self) -> Result<(), SaciError> {
        // Both fields are set together in `new` and taken together here, so
        // the `else` arm only ever discards a pair of `None`s.
        let (Some(std_socket), Some(tx)) = (self.socket.take(), self.tx.take()) else {
            return Ok(());
        };
        let socket = UdpSocket::from_std(std_socket).map_err(|e| {
            let err = SaciError::generic(format!(
                "UdpSource: cannot adopt the socket into the runtime: {e}"
            ));
            self.dead = Some(err.clone());
            err
        })?;

        let schema = Arc::clone(&self.schema);
        let transformer = Arc::clone(&self.transformer);
        let local_addr = self.local_addr;
        self.task = Some(tokio::spawn(async move {
            receive(socket, tx, schema, transformer, local_addr).await
        }));
        Ok(())
    }
}

/// Join `group` on the interface `bind` chose.
///
/// One group per source. Several receivers on one host would need
/// `SO_REUSEADDR`, which tokio's socket does not expose, so the answer there
/// is one receiver per port per host.
fn join_multicast(
    socket: &std::net::UdpSocket,
    group: IpAddr,
    local_addr: SocketAddr,
    bind: &str,
) -> Result<(), SaciError> {
    if !group.is_multicast() {
        return Err(SaciError::configuration(format!(
            "UdpSource: 'multicast_group' {group} is not a multicast address"
        )));
    }
    let joined = match (group, local_addr.ip()) {
        (IpAddr::V4(g), IpAddr::V4(_)) => socket.join_multicast_v4(&g, &Ipv4Addr::UNSPECIFIED),
        (IpAddr::V6(g), IpAddr::V6(_)) => socket.join_multicast_v6(&g, 0),
        _ => {
            return Err(SaciError::configuration(format!(
                "UdpSource: 'multicast_group' {group} and 'bind' {bind} are not the same IP family"
            )));
        }
    };
    joined.map_err(|e| {
        SaciError::configuration(format!(
            "UdpSource: cannot join multicast group {group}: {e}"
        ))
    })
}

/// Receive datagrams until the socket fails or the source is dropped.
///
/// One decoder serves every producer: each datagram is decoded whole and
/// flushed, so nothing leaks from one into the next. The returned error is
/// what the next [`UdpSource::next_batch`] reports.
async fn receive(
    socket: UdpSocket,
    tx: mpsc::Sender<RecordBatch>,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    local_addr: SocketAddr,
) -> SaciError {
    let mut decoder = match transformer.open_message_decoder(Arc::clone(&schema)) {
        Ok(decoder) => decoder,
        Err(e) => return e,
    };
    let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];

    loop {
        match socket.recv_from(&mut buf).await {
            // The OS relaying that an echo this source sent hit a closed
            // port. It says nothing about this socket.
            Err(e) if crate::probe::is_unreachable_report(&e) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    error = %e,
                    "UdpSource: the OS reported a peer this source echoed to as unreachable"
                );
                // This arm is the loop's only suspension point. A socket that
                // kept re-raising one report would hold the worker it runs on,
                // and on a single-threaded runtime that is the whole service,
                // so the runtime gets a turn either way.
                tokio::task::yield_now().await;
            }
            Err(e) => {
                return SaciError::generic(format!(
                    "UdpSource: receiving on {local_addr} failed: {e}"
                ));
            }
            // A liveness probe: answer it and never decode it.
            Ok((0, from)) => {
                if let Err(_e) = socket.send_to(&[], from).await {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        peer = %from,
                        error = %_e,
                        "UdpSource: echoing a liveness probe failed"
                    );
                }
            }
            Ok((n, _from)) => {
                let decoded = decoder.push(&buf[..n]).and_then(|()| decoder.flush());
                let spent = match decoded {
                    Ok(Some(batch)) => {
                        // Bounded send, so a full channel stalls the receive
                        // loop and the kernel drops what it cannot hold. A
                        // send error means the source was dropped.
                        if tx.send(batch).await.is_err() {
                            return SaciError::generic("UdpSource: dropped");
                        }
                        false
                    }
                    Ok(None) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            peer = %_from,
                            bytes = n,
                            "UdpSource: datagram decoded no batch, dropped"
                        );
                        true
                    }
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            peer = %_from,
                            bytes = n,
                            error = %_e,
                            "UdpSource: undecodable datagram dropped"
                        );
                        true
                    }
                };
                // A decoder's state after a failed push is unspecified, and a
                // decoder that yielded nothing may be holding part of the
                // datagram just dropped. Replacing it in both cases is what
                // makes the datagram boundary the frame for any decoder,
                // rather than only for the formats whose own `flush` refuses
                // a partial record. A fresh one costs nothing.
                if spent {
                    decoder = match transformer.open_message_decoder(Arc::clone(&schema)) {
                        Ok(decoder) => decoder,
                        Err(e) => return e,
                    };
                }
            }
        }
    }
}

#[async_trait]
impl Source for UdpSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Wait for the next datagram's batch.
    ///
    /// Never returns `Ok(None)`: an EOF drops a live source from the stream
    /// runner's rotation for good, so a socket that failed is reported as an
    /// error on every call instead. The runner logs it, backs off, and asks
    /// again, which is what lets a `heal` block rebuild the source and rebind.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] once the receive loop has ended, with
    /// the reason it ended.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if let Some(dead) = &self.dead {
            return Err(dead.clone());
        }
        self.ensure_receiving()?;

        if let Some(batch) = self.rx.recv().await {
            return Ok(Some(batch));
        }

        // The channel closed, so the receive task ended and its error is the
        // reason. The handle is polled through `&mut` and cleared only once it
        // resolves: taking it first loses both the handle and the error if the
        // caller drops this future while the task is still finishing, which
        // the stream runner does on every cancel and on its prime timeout.
        // Nothing awaits between the handle resolving and `dead` being set.
        let err = match self.task.as_mut() {
            Some(handle) => match handle.await {
                Ok(err) => err,
                Err(join) => {
                    SaciError::generic(format!("UdpSource: receive task panicked: {join}"))
                }
            },
            None => SaciError::generic("UdpSource: the receive loop is gone"),
        };
        self.task = None;
        self.dead = Some(err.clone());
        Err(err)
    }
}

impl Drop for UdpSource {
    fn drop(&mut self) {
        if let Some(handle) = self.task.take() {
            // The sender lives inside that future, so aborting it closes the
            // channel too.
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use saci_transformer::{MessageDecoder, MessageShape};
    use saci_transformer_arrow_ipc::ArrowIpcTransformer;
    use saci_transformer_ndjson::NdjsonTransformer;
    use std::time::Duration;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// The datagram rule is what this crate owns; the payload codec is the
    /// transformer's, and its own tests cover the decode paths.
    fn arrow_ipc() -> Arc<dyn Transformer> {
        Arc::new(ArrowIpcTransformer::new())
    }

    /// One message per row, so a batch of three rows is three datagrams.
    fn ndjson() -> Arc<dyn Transformer> {
        Arc::new(NdjsonTransformer::new(64))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("build batch")
    }

    fn encode(transformer: &Arc<dyn Transformer>, values: &[i64]) -> Vec<Vec<u8>> {
        transformer
            .encode_messages(&batch(values))
            .expect("encode messages")
    }

    fn values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v is Int64")
            .values()
            .to_vec()
    }

    /// A plain sender socket, the way a producer that knows nothing about SACI
    /// would write to this source.
    async fn sender() -> UdpSocket {
        UdpSocket::bind("127.0.0.1:0").await.expect("bind sender")
    }

    #[test]
    fn new_reports_bound_port() {
        let src = UdpSource::new("127.0.0.1:0", None, test_schema(), 4, arrow_ipc()).unwrap();
        assert_ne!(src.local_addr().port(), 0);
    }

    #[test]
    fn new_rejects_unbindable_address() {
        let err = UdpSource::new("256.256.256.256:1", None, test_schema(), 4, arrow_ipc())
            .err()
            .expect("bind must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
    }

    /// A stream-only format: every message method keeps the trait's
    /// `unsupported` default. Every format SACI registers implements the
    /// message surface, so the gate is exercised against a stand-in.
    struct StreamOnly;

    impl Transformer for StreamOnly {
        fn format(&self) -> &'static str {
            "stream-only"
        }
    }

    #[test]
    fn new_rejects_a_format_with_no_message_decoder() {
        let err = UdpSource::new("127.0.0.1:0", None, test_schema(), 4, Arc::new(StreamOnly))
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
    fn new_rejects_a_unicast_multicast_group() {
        let group = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let err = UdpSource::new("127.0.0.1:0", Some(group), test_schema(), 4, arrow_ipc())
            .err()
            .expect("a unicast group must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("not a multicast address"),
            "got: {err}"
        );
    }

    #[test]
    fn new_rejects_a_group_of_the_other_family() {
        let group: IpAddr = "ff02::1".parse().expect("parse group");
        let err = UdpSource::new("127.0.0.1:0", Some(group), test_schema(), 4, arrow_ipc())
            .err()
            .expect("a v6 group on a v4 bind must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("same IP family"), "got: {err}");
    }

    #[tokio::test]
    async fn a_datagram_is_one_batch() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 4, arrow_ipc()).expect("bind");
        let addr = source.local_addr();
        let payloads = encode(&arrow_ipc(), &[7, 8]);

        let sender = sender().await;
        sender
            .send_to(&payloads[0], addr)
            .await
            .expect("send datagram");

        let received = source
            .next_batch()
            .await
            .expect("receive")
            .expect("one batch");
        assert_eq!(values(&received), vec![7, 8]);
    }

    /// The datagram boundary is the frame: two datagrams are two batches, in
    /// the order one producer sent them.
    #[tokio::test]
    async fn each_datagram_is_one_batch() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 4, ndjson()).expect("bind");
        let addr = source.local_addr();
        let payloads = encode(&ndjson(), &[1, 2]);
        assert_eq!(payloads.len(), 2, "ndjson emits one message per row");

        let sender = sender().await;
        for payload in &payloads {
            sender.send_to(payload, addr).await.expect("send datagram");
        }

        let first = source.next_batch().await.expect("receive").expect("batch");
        let second = source.next_batch().await.expect("receive").expect("batch");
        assert_eq!(values(&first), vec![1]);
        assert_eq!(values(&second), vec![2]);
    }

    /// The probe contract: a zero-length datagram comes back to its sender and
    /// never reaches the decoder, so data after it still decodes.
    #[tokio::test]
    async fn an_empty_datagram_is_echoed_not_decoded() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 4, arrow_ipc()).expect("bind");
        let addr = source.local_addr();
        let payloads = encode(&arrow_ipc(), &[5]);

        let sender = sender().await;
        sender.send_to(&[], addr).await.expect("send probe");

        // The receive loop starts on the first `next_batch`, and a probe
        // yields no batch, so this call is what drives the echo out and it
        // must time out. `rx.recv` is cancel-safe and the loop keeps running
        // on its own task.
        let polled = tokio::time::timeout(Duration::from_millis(500), source.next_batch()).await;
        assert!(polled.is_err(), "a probe must not decode into a batch");

        let mut buf = [0u8; 16];
        let (n, from) = tokio::time::timeout(Duration::from_secs(5), sender.recv_from(&mut buf))
            .await
            .expect("the echo must arrive")
            .expect("recv_from");
        assert_eq!(n, 0, "the echo is zero-length");
        assert_eq!(from, addr, "the echo comes from the source's socket");

        sender.send_to(&payloads[0], addr).await.expect("send data");
        let batch = source.next_batch().await.expect("receive").expect("batch");
        assert_eq!(values(&batch), vec![5]);
    }

    /// A bad datagram costs itself and nothing else: there is no connection to
    /// close, so the next one decodes.
    #[tokio::test]
    async fn an_undecodable_datagram_is_dropped_and_the_next_one_decodes() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 4, arrow_ipc()).expect("bind");
        let addr = source.local_addr();
        let payloads = encode(&arrow_ipc(), &[9]);

        let sender = sender().await;
        sender.send_to(b"garbage", addr).await.expect("send junk");
        sender
            .send_to(&payloads[0], addr)
            .await
            .expect("send datagram");

        let received = source
            .next_batch()
            .await
            .expect("receive")
            .expect("one batch");
        assert_eq!(values(&received), vec![9]);
    }

    /// Multicast delivery depends on the host's interfaces and routing, so a
    /// join that the OS refuses is an absence rather than a failure. A join
    /// that succeeded and delivers nothing is a failure.
    #[tokio::test]
    async fn a_multicast_group_is_received() {
        let group: IpAddr = "239.255.0.1".parse().expect("parse group");
        let mut source =
            match UdpSource::new("0.0.0.0:0", Some(group), test_schema(), 4, arrow_ipc()) {
                Ok(source) => source,
                Err(e) => {
                    println!("SKIP: multicast unavailable: {e}");
                    return;
                }
            };
        let port = source.local_addr().port();
        let payloads = encode(&arrow_ipc(), &[4]);

        let sender = UdpSocket::bind("0.0.0.0:0").await.expect("bind sender");
        sender
            .send_to(&payloads[0], (group, port))
            .await
            .expect("send to the group");

        let received = tokio::time::timeout(Duration::from_secs(5), source.next_batch())
            .await
            .expect("a joined group must deliver")
            .expect("receive")
            .expect("one batch");
        assert_eq!(values(&received), vec![4]);
    }

    /// The rebuild contract: a heal must be able to rebind the same port
    /// once this source is gone.
    #[tokio::test]
    async fn dropping_the_source_frees_its_port() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 4, arrow_ipc()).expect("bind");
        let addr = source.local_addr();

        // Drives the receive loop's first poll, which must time out: nothing
        // was sent.
        let polled = tokio::time::timeout(Duration::from_millis(200), source.next_batch()).await;
        assert!(
            polled.is_err(),
            "nothing was sent, so the first poll must time out"
        );

        drop(source);

        let mut rebound = false;
        for _ in 0..20 {
            if std::net::UdpSocket::bind(addr).is_ok() {
                rebound = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            rebound,
            "a heal rebuild must be able to rebind the port a dropped source held"
        );
    }

    #[tokio::test]
    async fn buffer_zero_is_clamped_to_one() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 0, arrow_ipc()).expect("bind");
        let addr = source.local_addr();
        let payloads = encode(&arrow_ipc(), &[3]);

        let sender = sender().await;
        sender
            .send_to(&payloads[0], addr)
            .await
            .expect("send datagram");

        let received = source
            .next_batch()
            .await
            .expect("receive")
            .expect("one batch");
        assert_eq!(values(&received), vec![3]);
    }

    /// A blank line decodes to no rows, the same "dropped" outcome as a
    /// decode error: the next real datagram still decodes.
    #[tokio::test]
    async fn a_datagram_that_decodes_to_nothing_is_dropped_and_the_next_one_decodes() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 4, ndjson()).expect("bind");
        let addr = source.local_addr();
        let payloads = encode(&ndjson(), &[6]);

        let sender = sender().await;
        sender.send_to(b"\n", addr).await.expect("send blank line");
        sender
            .send_to(&payloads[0], addr)
            .await
            .expect("send datagram");

        let received = source
            .next_batch()
            .await
            .expect("receive")
            .expect("one batch");
        assert_eq!(values(&received), vec![6]);
    }

    /// One receive loop serves every producer: two sockets each sending once
    /// still both land in the same source's batch stream.
    #[tokio::test]
    async fn several_producers_share_one_source() {
        let mut source =
            UdpSource::new("127.0.0.1:0", None, test_schema(), 4, ndjson()).expect("bind");
        let addr = source.local_addr();

        let first_sender = sender().await;
        let second_sender = sender().await;
        first_sender
            .send_to(&encode(&ndjson(), &[10])[0], addr)
            .await
            .expect("send from the first producer");
        second_sender
            .send_to(&encode(&ndjson(), &[20])[0], addr)
            .await
            .expect("send from the second producer");

        let first = source.next_batch().await.expect("receive").expect("batch");
        let second = source.next_batch().await.expect("receive").expect("batch");
        let mut rows = vec![values(&first)[0], values(&second)[0]];
        rows.sort_unstable();
        assert_eq!(rows, vec![10, 20]);
    }

    /// A test-only decoder that buffers every push across datagrams and only
    /// flushes when its buffer ends in a newline, the way a genuinely
    /// stateful decoder holds a partial record between calls: the shape that
    /// makes decoder residue leaking across datagrams observable.
    struct BufferingDecoder {
        buf: Vec<u8>,
        inner: Box<dyn MessageDecoder>,
    }

    impl MessageDecoder for BufferingDecoder {
        fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
            self.buf.extend_from_slice(payload);
            Ok(())
        }

        fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            if self.buf.last() != Some(&b'\n') {
                return Ok(None);
            }
            self.inner.push(&self.buf)?;
            self.buf.clear();
            self.inner.flush()
        }
    }

    /// A format whose decoder carries a buffer across `push` calls: real
    /// enough to show that the receive loop's decoder replacement covers an
    /// empty flush, not only a failed one.
    struct BufferingTransformer;

    impl Transformer for BufferingTransformer {
        fn format(&self) -> &'static str {
            "buffering-test"
        }

        fn open_message_decoder(
            &self,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn MessageDecoder>, SaciError> {
            Ok(Box::new(BufferingDecoder {
                buf: Vec::new(),
                inner: NdjsonTransformer::new(64).open_message_decoder(schema)?,
            }))
        }

        fn message_shape(&self) -> Option<MessageShape> {
            Some(MessageShape::PerRow)
        }
    }

    /// Without replacing the decoder after an empty flush, a datagram that
    /// completes an earlier partial buffer would leak into the batch stream
    /// ahead of the datagram that really produced it.
    #[tokio::test]
    async fn a_decoder_that_buffers_across_datagrams_is_replaced_after_an_empty_flush() {
        let mut source = UdpSource::new(
            "127.0.0.1:0",
            None,
            test_schema(),
            4,
            Arc::new(BufferingTransformer),
        )
        .expect("bind");
        let addr = source.local_addr();

        let sender = sender().await;
        sender
            .send_to(b"{\"v\":1", addr)
            .await
            .expect("send a datagram with no trailing newline");
        sender
            .send_to(b"}\n", addr)
            .await
            .expect("send the closing brace alone");
        sender
            .send_to(b"{\"v\":3}\n", addr)
            .await
            .expect("send a whole record");

        let received = source
            .next_batch()
            .await
            .expect("receive")
            .expect("one batch");
        assert_eq!(
            values(&received),
            vec![3],
            "a leaked decoder would have produced [1] from the completed residue instead"
        );
    }
}
