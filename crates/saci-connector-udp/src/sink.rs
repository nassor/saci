//! [`UdpSink`]: fire-and-forget UDP [`Sink`], the mirror of [`UdpSource`].
//!
//! [`UdpSource`]: crate::UdpSource
//!
//! The source binds and receives datagrams; this sink sends them. One datagram
//! per encoded message, with no length prefix, so the two halves are wire
//! compatible: a `UdpSink` in one service feeds a `UdpSource` in another when
//! both name the same format.
//!
//! A `send` that returns `Ok` says the kernel took the datagram and nothing
//! else. The peer may be down, its receive buffer full, or a fragment lost on
//! the path, and all three look exactly like delivery. That is why the sink
//! can offer a liveness probe: without one it has no way to tell a listening
//! peer from an absent one.
//!
//! The probe runs on its own task, one round per `interval`, plus one inline
//! round when the sockets open, so the first write is already gated by a real
//! answer. Under `on_failure="refuse"` a peer that failed its last round is
//! skipped, and a batch no peer accepted is a `write_batch` error, which is
//! what the host's `retry`, `heal` and `dlq` blocks act on. Reconnection is the
//! `heal` block and nothing else: a rebuilt sink re-resolves every `connect`
//! address and opens fresh sockets.
//!
//! With several `connect` addresses the sink sends every message to every
//! peer and fails only when the batch reached nobody. A live peer therefore
//! never receives the runner's retry of a batch it already took.
//!
//! Payload bytes come from the transformer's message surface
//! ([`encode_messages`](Transformer::encode_messages)), the same surface the
//! Kafka, NATS and TCP sinks publish from. Whether the format has a message
//! surface at all is a property of the config, so
//! [`connect`](UdpSink::connect) settles it while building rather than on the
//! first batch, the way `TcpSink::connect` does.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::Transformer;

use crate::DATAGRAM_BUFFER_BYTES;
use crate::probe::{self, Liveness, OnFailure, ProbeSettings};

/// The largest payload an IPv4 datagram carries: 65535 bytes minus the 20 byte
/// IP header and the 8 byte UDP header. IPv6 with a jumbogram could carry
/// more, which this ceiling ignores.
pub const MAX_DATAGRAM_BYTES: usize = 65_507;

/// Fire-and-forget UDP sink: one encoded message out, one datagram on the
/// wire.
///
/// [`connect`](Self::connect) resolves every address and opens nothing, so
/// `saci-service validate` passes while every peer is down. The sockets open
/// on the first [`write_batch`](Sink::write_batch).
pub struct UdpSink {
    peers: Vec<Peer>,
    broadcast: bool,
    max_datagram_bytes: usize,
    probe: Option<ProbeSettings>,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    /// `Some` once the sockets opened with a `probe` configured.
    probe_task: Option<JoinHandle<()>>,
    opened: bool,
}

/// One `connect` address and the socket that serves it.
struct Peer {
    addr: SocketAddr,
    /// A connected socket has one peer, which is what makes `send` and the
    /// OS's unreachable reports available. A broadcast or multicast
    /// destination is addressed per datagram instead.
    connected: bool,
    /// `None` until the first batch opens the sockets.
    socket: Option<Arc<UdpSocket>>,
    /// Written by the probe task, read by [`Sink::write_batch`].
    down: Arc<AtomicBool>,
}

/// What one peer's probe round needs: the socket, where it points, and where
/// to record the answer.
#[derive(Clone)]
struct ProbeTarget {
    addr: SocketAddr,
    socket: Arc<UdpSocket>,
    down: Arc<AtomicBool>,
}

impl UdpSink {
    /// Resolve every `connect` address and prepare the sink without opening a
    /// socket.
    ///
    /// Each entry is a `host:port` string, and its first resolved address is
    /// that peer for the sink's lifetime. `broadcast` enables `SO_BROADCAST`
    /// on every socket. `max_datagram_bytes` caps one message, so a batch
    /// holding a larger one is refused whole. `probe` asks each peer from time
    /// to time whether it is still there.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `transformer` declares no
    /// [`message_shape`](Transformer::message_shape), if `connect` is empty or
    /// an address cannot be resolved, if `max_datagram_bytes` is zero or above
    /// [`MAX_DATAGRAM_BYTES`], if `probe` is combined with `broadcast` or a
    /// multicast address, or if a probe interval or timeout is zero.
    pub fn connect(
        connect: &[String],
        broadcast: bool,
        max_datagram_bytes: usize,
        probe: Option<ProbeSettings>,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        // The declaration is the whole answer for every batch this sink will
        // ever encode, so asking once here is what makes a format with no
        // message surface a config error rather than a failed first write.
        if transformer.message_shape().is_none() {
            return Err(SaciError::configuration(format!(
                "UdpSink: format '{}' has no message codec",
                transformer.format()
            )));
        }

        if connect.is_empty() {
            return Err(SaciError::configuration(
                "UdpSink: 'connect' names no address",
            ));
        }

        let mut peers = Vec::with_capacity(connect.len());
        for address in connect {
            let addr = address
                .to_socket_addrs()
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "UdpSink: cannot resolve 'connect' address '{address}': {e}"
                    ))
                })?
                .next()
                .ok_or_else(|| {
                    SaciError::configuration(format!(
                        "UdpSink: 'connect' address '{address}' resolved to no address"
                    ))
                })?;
            peers.push(Peer {
                addr,
                connected: !broadcast && !addr.ip().is_multicast(),
                socket: None,
                down: Arc::new(AtomicBool::new(false)),
            });
        }

        if max_datagram_bytes == 0 || max_datagram_bytes > MAX_DATAGRAM_BYTES {
            return Err(SaciError::configuration(format!(
                "UdpSink: 'max_datagram_bytes' must be between 1 and {MAX_DATAGRAM_BYTES}, got {max_datagram_bytes}"
            )));
        }

        if let Some(settings) = probe {
            // A probe reads one peer's answer off a connected socket, and
            // neither a broadcast nor a multicast destination has one peer.
            if broadcast || peers.iter().any(|p| p.addr.ip().is_multicast()) {
                return Err(SaciError::configuration(
                    "UdpSink: 'probe' needs one answering peer; it cannot be combined with 'broadcast' or a multicast 'connect' address",
                ));
            }
            if settings.interval.is_zero() || settings.timeout.is_zero() {
                return Err(SaciError::configuration(
                    "UdpSink: probe 'interval_ms' and 'timeout_ms' must be at least 1",
                ));
            }
        }

        Ok(Self {
            peers,
            broadcast,
            max_datagram_bytes,
            probe,
            schema,
            transformer,
            probe_task: None,
            opened: false,
        })
    }

    /// The peers this sink sends to, with the configured addresses resolved.
    pub fn peer_addrs(&self) -> Vec<SocketAddr> {
        self.peers.iter().map(|peer| peer.addr).collect()
    }

    /// Open one socket per peer and start the probe. Idempotent.
    ///
    /// The sockets are built into a local list and assigned only once every
    /// one succeeded, so a failure leaves nothing half-open for the next
    /// attempt to trip over.
    async fn ensure_open(&mut self) -> Result<(), SaciError> {
        if self.opened {
            return Ok(());
        }

        let mut sockets = Vec::with_capacity(self.peers.len());
        for peer in &self.peers {
            let local = if peer.addr.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            };
            let socket = UdpSocket::bind(local).await.map_err(|e| {
                SaciError::generic(format!(
                    "UdpSink: cannot open a socket for {}: {e}",
                    peer.addr
                ))
            })?;
            if self.broadcast {
                socket.set_broadcast(true).map_err(|e| {
                    SaciError::generic(format!(
                        "UdpSink: cannot enable broadcast for {}: {e}",
                        peer.addr
                    ))
                })?;
            }
            if peer.connected {
                // On Linux a missing route surfaces here rather than on the
                // first send.
                socket.connect(peer.addr).await.map_err(|e| {
                    SaciError::generic(format!("UdpSink: cannot connect to {}: {e}", peer.addr))
                })?;
            }
            sockets.push(Arc::new(socket));
        }

        let targets: Vec<ProbeTarget> = self
            .peers
            .iter_mut()
            .zip(sockets)
            .map(|(peer, socket)| {
                peer.socket = Some(Arc::clone(&socket));
                ProbeTarget {
                    addr: peer.addr,
                    socket,
                    down: Arc::clone(&peer.down),
                }
            })
            .collect();

        if let Some(settings) = self.probe {
            // One buffer per peer, allocated once and moved into the task: a
            // round probes every peer at the same time, so each needs its own
            // place to receive.
            let mut buffers: Vec<Vec<u8>> = targets
                .iter()
                .map(|_| vec![0u8; DATAGRAM_BUFFER_BYTES])
                .collect();
            probe_round(&targets, settings, &mut buffers).await;
            // Dropping a `JoinHandle` detaches its task rather than cancelling
            // it, and a detached probe would keep the sockets it was given and
            // keep writing the flags the live sink reads.
            self.stop_probe();
            self.probe_task = Some(tokio::spawn(async move {
                // Starting one interval out, because the inline round above is
                // this peer set's answer for the interval that just began.
                let start = tokio::time::Instant::now() + settings.interval;
                let mut rounds = tokio::time::interval_at(start, settings.interval);
                // A round that overran its interval must not be followed by a
                // burst of catch-up rounds against the same peers.
                rounds.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    rounds.tick().await;
                    probe_round(&targets, settings, &mut buffers).await;
                }
            }));
        }

        self.opened = true;
        Ok(())
    }

    /// Stop the probe task. The sockets need no shutdown: every `send` handed
    /// a whole datagram to the kernel.
    fn stop_probe(&mut self) {
        if let Some(task) = self.probe_task.take() {
            task.abort();
        }
    }
}

impl Peer {
    /// Send one datagram per payload, stopping at the first failure.
    ///
    /// A zero-length payload is skipped: an empty datagram is the liveness
    /// probe, so the sink never sends one as data. The remaining payloads of a
    /// batch are not attempted on a peer whose send already failed, because
    /// the batch is the runner's to retry as a whole.
    async fn send_all(&self, payloads: &[Vec<u8>]) -> Result<(), String> {
        let Some(socket) = self.socket.as_ref() else {
            return Err("its socket is not open".to_string());
        };
        for payload in payloads {
            if payload.is_empty() {
                continue;
            }
            let mut sent = self.send_one(socket, payload).await;
            // A connected socket holds one pending unreachable report and
            // hands it to the next operation, which may be this send and may
            // have been raised by a datagram sent while the peer was away.
            // The first failure clears it, so the second send is the one that
            // asks whether the peer is away now. Without this a peer that came
            // back costs one whole batch, and the runner's retry of it
            // duplicates every payload the peer already took.
            if let Err(e) = &sent
                && self.connected
                && probe::is_unreachable_report(e)
            {
                sent = self.send_one(socket, payload).await;
            }
            match sent {
                Ok(n) if n == payload.len() => {}
                Ok(n) => return Err(format!("sent {n} of {} bytes", payload.len())),
                Err(e) => {
                    return Err(format!(
                        "sending a {} byte datagram failed: {e}",
                        payload.len()
                    ));
                }
            }
        }
        Ok(())
    }

    /// One datagram out, addressed the way this peer's socket needs.
    async fn send_one(&self, socket: &UdpSocket, payload: &[u8]) -> std::io::Result<usize> {
        if self.connected {
            socket.send(payload).await
        } else {
            socket.send_to(payload, self.addr).await
        }
    }
}

/// Probe every peer once and record each answer.
///
/// The peers are probed at the same time, so a round costs one `timeout`
/// however many peers there are. Probing them in turn would put `N` timeouts
/// on the first write and stretch the gap between rounds by as much again.
/// `buffers` holds one receive buffer per target, in the same order.
async fn probe_round(targets: &[ProbeTarget], settings: ProbeSettings, buffers: &mut [Vec<u8>]) {
    let rounds = targets
        .iter()
        .zip(buffers.iter_mut())
        .map(|(target, buf)| probe_one(target, settings, buf));
    futures::future::join_all(rounds).await;
}

/// Probe one peer and record its answer.
///
/// Only a transition is logged, so a peer that stays down costs one line
/// rather than one per interval.
async fn probe_one(target: &ProbeTarget, settings: ProbeSettings, buf: &mut [u8]) {
    // The address names the peer in a transition log line and nowhere else, so
    // a build without `tracing` still consumes it.
    #[cfg(not(feature = "tracing"))]
    let _ = &target.addr;
    let was_down = target.down.load(Ordering::Relaxed);
    match probe::probe(&target.socket, settings.kind, settings.timeout, buf).await {
        Liveness::Up => {
            if was_down {
                target.down.store(false, Ordering::Relaxed);
                #[cfg(feature = "tracing")]
                tracing::info!(peer = %target.addr, "UdpSink: peer answers again");
            }
        }
        Liveness::Down(_reason) => {
            if !was_down {
                target.down.store(true, Ordering::Relaxed);
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    peer = %target.addr,
                    reason = %_reason,
                    "UdpSink: peer is down"
                );
            }
        }
    }
}

#[async_trait]
impl Sink for UdpSink {
    /// Encode `batch` and send one datagram per message to every peer,
    /// opening the sockets first if this is the first batch.
    ///
    /// A batch the transformer encodes to no payloads sends nothing and
    /// succeeds, whatever the peers are doing: there is nothing for one to
    /// refuse. A batch holding a message above `max_datagram_bytes` sends
    /// nothing either: the check runs before the first send, so such a batch
    /// is all-or-nothing rather than half delivered.
    ///
    /// # Errors
    ///
    /// Returns the transformer's error when the batch cannot be encoded, and
    /// [`SaciError::Generic`] when a message exceeds `max_datagram_bytes`,
    /// when a socket cannot be opened, or when no peer accepted the batch,
    /// naming every peer and why.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        let payloads = self.transformer.encode_messages(batch)?;

        // Nothing to deliver, so no peer can fail to take it. Reporting a
        // write error here would hand an empty batch to `retry`, `heal` and
        // the dead letter queue.
        if payloads.is_empty() {
            return Ok(());
        }

        if let Some(oversized) = payloads
            .iter()
            .find(|payload| payload.len() > self.max_datagram_bytes)
        {
            return Err(SaciError::generic(format!(
                "UdpSink: a {} byte message exceeds max_datagram_bytes ({}); nothing was sent",
                oversized.len(),
                self.max_datagram_bytes
            )));
        }

        self.ensure_open().await?;

        let refuse = matches!(
            self.probe,
            Some(ProbeSettings {
                on_failure: OnFailure::Refuse,
                ..
            })
        );
        let mut accepted = 0usize;
        let mut refusals = Vec::new();
        for peer in &self.peers {
            if refuse && peer.down.load(Ordering::Relaxed) {
                refusals.push(format!("{}: down at its last liveness probe", peer.addr));
                continue;
            }
            match peer.send_all(&payloads).await {
                Ok(()) => accepted += 1,
                Err(reason) => refusals.push(format!("{}: {reason}", peer.addr)),
            }
        }

        if accepted == 0 {
            return Err(SaciError::generic(format!(
                "UdpSink: no peer accepted the batch: {}",
                refusals.join("; ")
            )));
        }
        // The batch landed somewhere, so the peers that missed it are a
        // warning rather than the write's outcome.
        #[cfg(feature = "tracing")]
        for _reason in &refusals {
            tracing::warn!(reason = %_reason, "UdpSink: a peer did not take the batch");
        }
        Ok(())
    }

    /// Stop the probe and return.
    ///
    /// Nothing is buffered: every `send` handed a whole datagram to the
    /// kernel, so there is nothing left to flush and no close for a peer to
    /// observe. The sink is spent afterwards: the sockets stay open but no
    /// probe updates what the write path reads about them, so a rebuilt
    /// instance is what writes again.
    ///
    /// # Errors
    ///
    /// Never fails.
    async fn finish(&mut self) -> Result<(), SaciError> {
        self.stop_probe();
        Ok(())
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

impl Drop for UdpSink {
    fn drop(&mut self) {
        self.stop_probe();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::ProbeKind;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use saci_transformer::{MessageDecoder, MessageShape};
    use saci_transformer_arrow_ipc::ArrowIpcTransformer;
    use saci_transformer_ndjson::NdjsonTransformer;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::time::Instant;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    /// The datagram rule is what this crate owns; the payload codec is the
    /// transformer's, and its own tests cover the encode paths.
    fn arrow_ipc() -> Arc<dyn Transformer> {
        Arc::new(ArrowIpcTransformer::new())
    }

    /// One message per row, so a three-row batch is three datagrams.
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

    fn addrs(one: SocketAddr) -> Vec<String> {
        vec![one.to_string()]
    }

    fn probe_settings(on_failure: OnFailure) -> ProbeSettings {
        ProbeSettings {
            kind: ProbeKind::Echo,
            interval: Duration::from_millis(100),
            timeout: Duration::from_millis(100),
            on_failure,
        }
    }

    /// A format that encodes every batch to no messages at all, which is the
    /// only way to observe "no payloads, no datagrams" without a real codec
    /// that does it. It declares a shape, so it must carry a whole message
    /// surface: the decoder half is `arrow-ipc`'s, unexercised here but real,
    /// because [`Transformer::message_shape`] answers for both directions at
    /// once.
    struct NoMessages;

    impl Transformer for NoMessages {
        fn format(&self) -> &'static str {
            "no-messages"
        }

        fn open_message_decoder(
            &self,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn MessageDecoder>, SaciError> {
            ArrowIpcTransformer::new().open_message_decoder(schema)
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

    impl Transformer for NoMessageSurface {
        fn format(&self) -> &'static str {
            "no-surface"
        }
    }

    /// A peer socket that counts the data datagrams it received and answers a
    /// zero-length one only while `echo` is set, so a test can take a peer's
    /// liveness away and give it back without rebinding.
    struct TestPeer {
        addr: SocketAddr,
        echo: Arc<AtomicBool>,
        data: Arc<AtomicUsize>,
        probes: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl TestPeer {
        async fn bind(echo: bool) -> Self {
            let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind peer");
            let addr = socket.local_addr().expect("local_addr");
            let echo = Arc::new(AtomicBool::new(echo));
            let data = Arc::new(AtomicUsize::new(0));
            let probes = Arc::new(AtomicUsize::new(0));
            let (flag, counter, probe_counter) =
                (Arc::clone(&echo), Arc::clone(&data), Arc::clone(&probes));
            let task = tokio::spawn(async move {
                let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
                loop {
                    match socket.recv_from(&mut buf).await {
                        Ok((0, from)) => {
                            probe_counter.fetch_add(1, Ordering::Relaxed);
                            if flag.load(Ordering::Relaxed) {
                                let _ = socket.send_to(&[], from).await;
                            }
                        }
                        Ok(_) => {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                        // An unconnected socket on Windows sees a stale ICMP
                        // report as an error on the next receive; it says
                        // nothing about this socket.
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionRefused
                            ) => {}
                        Err(_) => return,
                    }
                }
            });
            Self {
                addr,
                echo,
                data,
                probes,
                task,
            }
        }

        fn received(&self) -> usize {
            self.data.load(Ordering::Relaxed)
        }

        fn probes_received(&self) -> usize {
            self.probes.load(Ordering::Relaxed)
        }
    }

    /// Wait up to a second for `check` to hold, polling every 20 ms.
    async fn within_a_second(mut check: impl FnMut() -> bool) -> bool {
        for _ in 0..50 {
            if check() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    #[test]
    fn connect_resolves_the_configured_addresses() {
        let sink = UdpSink::connect(
            &["127.0.0.1:9701".to_string(), "127.0.0.1:9702".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        assert_eq!(sink.peer_addrs().len(), 2);
    }

    #[test]
    fn connect_rejects_an_address_with_no_port() {
        let err = UdpSink::connect(
            &["127.0.0.1".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("resolution must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    /// The refusal is the config's, not the first batch's: nothing about a
    /// batch can change whether the format has a message surface.
    #[test]
    fn connect_refuses_a_format_with_no_message_codec() {
        let err = UdpSink::connect(
            &["127.0.0.1:9701".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            Arc::new(NoMessageSurface),
        )
        .err()
        .expect("a format with no message surface must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("no message codec"), "got: {err}");
        assert!(err.to_string().contains("no-surface"), "got: {err}");
    }

    /// A peer that is down is not a construction error, so
    /// `saci-service validate` passes without it.
    #[test]
    fn connect_opens_no_socket() {
        let sink = UdpSink::connect(
            &["127.0.0.1:1".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        assert_eq!(sink.schema().fields().len(), 1);
        assert!(sink.peers.iter().all(|peer| peer.socket.is_none()));
    }

    #[test]
    fn connect_rejects_an_oversized_max_datagram_bytes() {
        let err = UdpSink::connect(
            &["127.0.0.1:9701".to_string()],
            false,
            MAX_DATAGRAM_BYTES + 1,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("an oversized cap must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("'max_datagram_bytes'"),
            "got: {err}"
        );
    }

    /// A broadcast socket has no one peer to read an answer from.
    #[test]
    fn connect_refuses_a_probe_with_broadcast() {
        let err = UdpSink::connect(
            &["127.0.0.1:9701".to_string()],
            true,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Refuse)),
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("a probe with broadcast must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'probe'"), "got: {err}");
    }

    #[test]
    fn connect_refuses_a_probe_with_a_multicast_peer() {
        let err = UdpSink::connect(
            &["239.255.0.1:9".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Refuse)),
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("a probe with a multicast peer must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("multicast"), "got: {err}");
    }

    /// The datagram rule: one message, one datagram, and the payload bytes are
    /// the transformer's own.
    #[tokio::test]
    async fn write_batch_sends_one_datagram_per_message() {
        let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            ndjson(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[1, 2, 3])).await.expect("write");
        sink.finish().await.expect("finish");

        let mut decoder = ndjson()
            .open_message_decoder(test_schema())
            .expect("decoder");
        let mut rows = Vec::new();
        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        for _ in 0..3 {
            let (n, _from) =
                tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                    .await
                    .expect("three datagrams must arrive")
                    .expect("recv_from");
            decoder.push(&buf[..n]).expect("push");
            let decoded = decoder.flush().expect("flush").expect("one batch");
            rows.extend(
                decoded
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("v is Int64")
                    .values()
                    .iter()
                    .copied(),
            );
        }
        assert_eq!(rows, vec![1, 2, 3]);
    }

    /// The wire-compatibility claim: what this sink sends is what a `udp`
    /// source's decoder reads.
    #[tokio::test]
    async fn a_datagram_arrives_wire_compatible_with_the_source_decoder() {
        let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[7, 8])).await.expect("write");
        sink.finish().await.expect("finish");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let (n, _from) =
            tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                .await
                .expect("the datagram must arrive")
                .expect("recv_from");

        let mut decoder = arrow_ipc()
            .open_message_decoder(test_schema())
            .expect("decoder");
        decoder.push(&buf[..n]).expect("push");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 2);
        let column = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v is Int64");
        assert_eq!(column.values(), &[7, 8]);
    }

    /// A batch is all-or-nothing: the cap is checked before the first send, so
    /// a batch carrying one oversized message delivers none of it.
    #[tokio::test]
    async fn a_message_over_max_datagram_bytes_is_refused_before_any_send() {
        let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(&addrs(addr), false, 8, None, test_schema(), arrow_ipc())
            .expect("resolve");
        let err = sink
            .write_batch(&batch(&[1, 2, 3]))
            .await
            .expect_err("an oversized message must be refused");
        assert!(
            err.to_string().contains("exceeds max_datagram_bytes"),
            "got: {err}"
        );

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let quiet =
            tokio::time::timeout(Duration::from_millis(200), collector.recv_from(&mut buf)).await;
        assert!(quiet.is_err(), "nothing may reach the peer");
    }

    #[tokio::test]
    async fn a_batch_that_encodes_to_no_messages_sends_nothing() {
        let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            Arc::new(NoMessages),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[1])).await.expect("write");
        sink.finish().await.expect("finish");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let quiet =
            tokio::time::timeout(Duration::from_millis(200), collector.recv_from(&mut buf)).await;
        assert!(quiet.is_err(), "no payloads, no datagrams");
    }

    #[tokio::test]
    async fn finish_without_a_batch_is_ok() {
        let mut sink = UdpSink::connect(
            &["127.0.0.1:1".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.finish().await.expect("finishing an unopened sink");
    }

    #[tokio::test]
    async fn fan_out_reaches_every_peer() {
        let first = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let second = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let connect = vec![
            first.local_addr().expect("addr").to_string(),
            second.local_addr().expect("addr").to_string(),
        ];

        let mut sink = UdpSink::connect(
            &connect,
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[4])).await.expect("write");
        sink.finish().await.expect("finish");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        for collector in [&first, &second] {
            let (n, _from) =
                tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                    .await
                    .expect("each peer must receive the datagram")
                    .expect("recv_from");
            assert!(n > 0);
        }
    }

    /// A fan-out write fails only when the batch reached nobody, so a live
    /// peer never sees the runner's retry of a batch it already took.
    #[tokio::test]
    async fn fan_out_is_ok_while_one_peer_accepts() {
        let live = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let connect = vec![
            live.local_addr().expect("addr").to_string(),
            "127.0.0.1:1".to_string(),
        ];

        let mut sink = UdpSink::connect(
            &connect,
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[6])).await.expect("write");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let (n, _from) = tokio::time::timeout(Duration::from_secs(5), live.recv_from(&mut buf))
            .await
            .expect("the live peer must receive the datagram")
            .expect("recv_from");
        assert!(n > 0);
    }

    /// The whole point of `on_failure="refuse"`: a peer that failed its last
    /// probe gets nothing, the batch that reached nobody is an error the host
    /// can heal on, and a peer that answers again is sent to without a
    /// rebuild.
    #[tokio::test]
    async fn a_refused_probe_fails_the_write_and_a_returning_peer_lifts_it() {
        let peer = TestPeer::bind(false).await;

        let mut sink = UdpSink::connect(
            &addrs(peer.addr),
            false,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Refuse)),
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");

        let err = sink
            .write_batch(&batch(&[1]))
            .await
            .expect_err("a peer that failed its probe must refuse the batch");
        assert!(
            err.to_string().contains("no peer accepted the batch"),
            "got: {err}"
        );
        assert!(
            err.to_string().contains("down at its last liveness probe"),
            "got: {err}"
        );
        assert_eq!(peer.received(), 0, "a refused peer receives no data");

        peer.echo.store(true, Ordering::Relaxed);
        let resumed = within_a_second(|| {
            // The probe task lifts the flag; the write only reads it.
            !sink.peers[0].down.load(Ordering::Relaxed)
        })
        .await;
        assert!(resumed, "a peer that echoes again must be marked up");

        sink.write_batch(&batch(&[2]))
            .await
            .expect("a peer that answers again takes the batch");
        assert!(
            within_a_second(|| peer.received() == 1).await,
            "the returning peer must receive the batch"
        );

        sink.finish().await.expect("finish");
        peer.task.abort();
    }

    /// `on_failure="warn"` keeps the write path blind, which is what a peer
    /// that never echoes needs.
    #[tokio::test]
    async fn warn_keeps_sending_to_a_down_peer() {
        let peer = TestPeer::bind(false).await;

        let mut sink = UdpSink::connect(
            &addrs(peer.addr),
            false,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Warn)),
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");

        sink.write_batch(&batch(&[3]))
            .await
            .expect("warn never refuses a write");
        assert!(
            within_a_second(|| peer.received() == 1).await,
            "a warned peer still receives the data"
        );
        assert!(
            sink.peers[0].down.load(Ordering::Relaxed),
            "the probe still recorded the peer as down"
        );

        sink.finish().await.expect("finish");
        peer.task.abort();
    }

    /// `SO_BROADCAST` is set on every socket and a unicast destination still
    /// works, which is what makes `broadcast` safe to leave on for a peer list
    /// that mixes the two.
    #[tokio::test]
    async fn broadcast_sets_the_socket_option() {
        let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            true,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[5])).await.expect("write");

        let socket = sink.peers[0]
            .socket
            .as_ref()
            .expect("the socket is open after a write");
        assert!(
            socket.broadcast().expect("read SO_BROADCAST"),
            "broadcast must be enabled on the socket"
        );
        assert!(
            !sink.peers[0].connected,
            "a broadcast peer is addressed per datagram"
        );

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let (n, _from) =
            tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                .await
                .expect("the datagram must arrive")
                .expect("recv_from");
        assert!(n > 0);
    }

    #[test]
    fn max_datagram_bytes_at_the_ceiling_is_accepted() {
        UdpSink::connect(
            &["127.0.0.1:9701".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("the ceiling itself must be accepted");
    }

    #[test]
    fn max_datagram_bytes_zero_is_refused() {
        let err = UdpSink::connect(
            &["127.0.0.1:9701".to_string()],
            false,
            0,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("zero must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("'max_datagram_bytes'"),
            "got: {err}"
        );
    }

    #[test]
    fn connect_rejects_an_empty_list() {
        let err = UdpSink::connect(
            &[],
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("an empty connect list must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("names no address"), "got: {err}");
    }

    #[test]
    fn connect_refuses_a_zero_probe_interval() {
        let mut settings = probe_settings(OnFailure::Refuse);
        settings.interval = Duration::ZERO;
        let err = UdpSink::connect(
            &["127.0.0.1:9701".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            Some(settings),
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("a zero interval must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("interval_ms"), "got: {err}");
    }

    #[test]
    fn connect_refuses_a_zero_probe_timeout() {
        let mut settings = probe_settings(OnFailure::Refuse);
        settings.timeout = Duration::ZERO;
        let err = UdpSink::connect(
            &["127.0.0.1:9701".to_string()],
            false,
            MAX_DATAGRAM_BYTES,
            Some(settings),
            test_schema(),
            arrow_ipc(),
        )
        .err()
        .expect("a zero timeout must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("interval_ms"), "got: {err}");
    }

    /// Learns the real payload length first, so the cap can sit exactly on
    /// it: one byte under refuses, and the ceiling itself must not.
    #[tokio::test]
    async fn a_message_exactly_at_max_datagram_bytes_is_sent() {
        let payload_len = arrow_ipc()
            .encode_messages(&batch(&[42]))
            .expect("encode messages")[0]
            .len();

        let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            false,
            payload_len,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[42]))
            .await
            .expect("a message exactly at the cap must be sent");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let (n, _from) =
            tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                .await
                .expect("the datagram must arrive")
                .expect("recv_from");
        assert_eq!(n, payload_len);

        let mut refusing_sink = UdpSink::connect(
            &addrs(addr),
            false,
            payload_len - 1,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        let err = refusing_sink
            .write_batch(&batch(&[42]))
            .await
            .expect_err("one byte over the cap must be refused");
        assert!(
            err.to_string().contains("exceeds max_datagram_bytes"),
            "got: {err}"
        );

        let quiet =
            tokio::time::timeout(Duration::from_millis(200), collector.recv_from(&mut buf)).await;
        assert!(quiet.is_err(), "a refused message must not reach the peer");
    }

    /// The socket opens once, on the first write; a second batch reuses it
    /// rather than churning a fresh one.
    #[tokio::test]
    async fn later_batches_reuse_the_first_batch_s_socket() {
        let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[1])).await.expect("first write");
        sink.write_batch(&batch(&[2])).await.expect("second write");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let (_n, first_from) =
            tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                .await
                .expect("the first datagram must arrive")
                .expect("recv_from");
        let (_n, second_from) =
            tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                .await
                .expect("the second datagram must arrive")
                .expect("recv_from");
        assert_eq!(
            first_from, second_from,
            "the same socket must serve every batch, not a fresh one per write"
        );
    }

    /// `Drop` stops the probe task rather than merely detaching it: no round
    /// runs once the sink is gone.
    #[tokio::test]
    async fn dropping_the_sink_stops_the_probe() {
        let peer = TestPeer::bind(true).await;

        let mut sink = UdpSink::connect(
            &addrs(peer.addr),
            false,
            MAX_DATAGRAM_BYTES,
            Some(ProbeSettings {
                kind: ProbeKind::Echo,
                interval: Duration::from_millis(50),
                timeout: Duration::from_millis(50),
                on_failure: OnFailure::Refuse,
            }),
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[1]))
            .await
            .expect("open the sockets");

        let ran = within_a_second(|| peer.probes_received() >= 3).await;
        assert!(ran, "at least three probe rounds must have run by now");

        drop(sink);

        tokio::time::sleep(Duration::from_millis(150)).await;
        let count_after_drop = peer.probes_received();

        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            peer.probes_received(),
            count_after_drop,
            "no probe round may run after the sink is dropped"
        );

        peer.task.abort();
    }

    /// A batch that reaches nobody names every peer that refused it, not just
    /// the first.
    #[tokio::test]
    async fn fan_out_refusal_names_every_down_peer() {
        let first = TestPeer::bind(false).await;
        let second = TestPeer::bind(false).await;
        let connect = vec![first.addr.to_string(), second.addr.to_string()];

        let mut sink = UdpSink::connect(
            &connect,
            false,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Refuse)),
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");

        let err = sink
            .write_batch(&batch(&[1]))
            .await
            .expect_err("two down peers must refuse the whole batch");
        let message = err.to_string();
        assert!(message.contains(&first.addr.to_string()), "got: {message}");
        assert!(message.contains(&second.addr.to_string()), "got: {message}");
        assert!(
            message.contains("down at its last liveness probe"),
            "got: {message}"
        );

        first.task.abort();
        second.task.abort();
    }

    /// A fan-out write succeeds through the peer that answers, and the down
    /// peer receives nothing.
    #[tokio::test]
    async fn fan_out_delivers_to_the_answering_peer_while_the_other_is_down() {
        let up = TestPeer::bind(true).await;
        let down = TestPeer::bind(false).await;
        let connect = vec![up.addr.to_string(), down.addr.to_string()];

        let mut sink = UdpSink::connect(
            &connect,
            false,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Refuse)),
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");

        sink.write_batch(&batch(&[1]))
            .await
            .expect("the answering peer accepts the batch");
        assert!(
            within_a_second(|| up.received() == 1).await,
            "the up peer must receive the batch"
        );
        assert_eq!(down.received(), 0, "the down peer must receive nothing");

        sink.finish().await.expect("finish");
        up.task.abort();
        down.task.abort();
    }

    /// The inverse of the return-to-life test: a peer that goes silent is
    /// refused as soon as the next probe round notices.
    #[tokio::test]
    async fn a_peer_that_stops_echoing_is_refused_after_its_next_round() {
        let peer = TestPeer::bind(true).await;

        let mut sink = UdpSink::connect(
            &addrs(peer.addr),
            false,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Refuse)),
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");

        sink.write_batch(&batch(&[1]))
            .await
            .expect("an echoing peer accepts the first batch");

        peer.echo.store(false, Ordering::Relaxed);
        let marked_down = within_a_second(|| sink.peers[0].down.load(Ordering::Relaxed)).await;
        assert!(marked_down, "the probe must mark a silenced peer down");

        let err = sink
            .write_batch(&batch(&[2]))
            .await
            .expect_err("a peer marked down must refuse the next write");
        assert!(
            err.to_string().contains("no peer accepted the batch"),
            "got: {err}"
        );

        sink.finish().await.expect("finish");
        peer.task.abort();
    }

    /// A peer resolved to an IPv6 address must get a socket bound to an IPv6
    /// local address, not a v4-mapped one.
    #[tokio::test]
    async fn an_ipv6_peer_gets_an_ipv6_socket() {
        let collector = match UdpSocket::bind("[::1]:0").await {
            Ok(socket) => socket,
            Err(e) => {
                println!("SKIP: IPv6 loopback unavailable: {e}");
                return;
            }
        };
        let addr = collector.local_addr().expect("local_addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[1])).await.expect("write");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let (n, _from) =
            tokio::time::timeout(Duration::from_secs(5), collector.recv_from(&mut buf))
                .await
                .expect("the datagram must arrive")
                .expect("recv_from");
        assert!(n > 0);

        let socket = sink.peers[0]
            .socket
            .as_ref()
            .expect("the socket is open after a write");
        assert!(
            socket.local_addr().expect("local_addr").is_ipv6(),
            "an IPv6 peer must get an IPv6 socket"
        );
    }

    /// A batch that encodes to nothing must never open a socket or run a
    /// probe: `write_batch` returns before either, so a config with every
    /// peer down still writes an empty batch cleanly.
    #[tokio::test]
    async fn an_empty_batch_is_ok_even_when_every_peer_is_down() {
        let first = TestPeer::bind(false).await;
        let second = TestPeer::bind(false).await;
        let connect = vec![first.addr.to_string(), second.addr.to_string()];

        let mut sink = UdpSink::connect(
            &connect,
            false,
            MAX_DATAGRAM_BYTES,
            Some(probe_settings(OnFailure::Refuse)),
            test_schema(),
            Arc::new(NoMessages),
        )
        .expect("resolve");

        sink.write_batch(&batch(&[1]))
            .await
            .expect("an empty-payload batch must not fail even with every peer down");

        assert!(
            sink.peers.iter().all(|p| p.socket.is_none()),
            "no socket may open for a batch that encodes to nothing"
        );
        assert_eq!(first.probes_received(), 0, "no probe round may run either");
        assert_eq!(second.probes_received(), 0, "no probe round may run either");

        first.task.abort();
        second.task.abort();
    }

    /// A probe round must probe every peer concurrently: three peers cost
    /// one timeout, not three, in the inline round `write_batch` waits on.
    #[tokio::test]
    async fn a_probe_round_costs_one_timeout_for_many_peers() {
        let peers = [
            TestPeer::bind(false).await,
            TestPeer::bind(false).await,
            TestPeer::bind(false).await,
        ];
        let connect: Vec<String> = peers.iter().map(|p| p.addr.to_string()).collect();

        let mut sink = UdpSink::connect(
            &connect,
            false,
            MAX_DATAGRAM_BYTES,
            Some(ProbeSettings {
                kind: ProbeKind::Echo,
                interval: Duration::from_secs(5),
                timeout: Duration::from_millis(300),
                on_failure: OnFailure::Refuse,
            }),
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");

        let started = Instant::now();
        let err = sink
            .write_batch(&batch(&[1]))
            .await
            .expect_err("every peer is down, so the batch reaches nobody");
        let elapsed = started.elapsed();
        assert!(
            err.to_string().contains("no peer accepted the batch"),
            "got: {err}"
        );
        assert!(
            elapsed < Duration::from_millis(700),
            "a concurrent round costs one timeout, not one per peer; took {elapsed:?}"
        );

        for peer in peers {
            peer.task.abort();
        }
    }

    /// The stale ICMP report from a batch sent while the peer's old socket
    /// was gone must not fail the write once the peer restarts: `send_all`
    /// retries once on the same report the probe does.
    #[tokio::test]
    async fn a_write_after_the_peer_restarts_is_not_failed_by_the_stale_report() {
        let closed = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let port = closed.local_addr().expect("local_addr").port();
        drop(closed);
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse addr");

        let mut sink = UdpSink::connect(
            &addrs(addr),
            false,
            MAX_DATAGRAM_BYTES,
            None,
            test_schema(),
            arrow_ipc(),
        )
        .expect("resolve");
        sink.write_batch(&batch(&[1]))
            .await
            .expect("the kernel takes the datagram even though nobody is listening");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut collector = None;
        for _ in 0..20 {
            if let Ok(socket) = UdpSocket::bind(addr).await {
                collector = Some(socket);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let collector = collector.expect("must be able to rebind the peer's port");

        sink.write_batch(&batch(&[2]))
            .await
            .expect("a stale report from the earlier send must not fail this write");

        let mut buf = vec![0u8; DATAGRAM_BUFFER_BYTES];
        let (n, _from) =
            tokio::time::timeout(Duration::from_secs(2), collector.recv_from(&mut buf))
                .await
                .expect("exactly one datagram must arrive")
                .expect("recv_from");
        assert!(n > 0);
    }
}
