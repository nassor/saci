//! `saci-connector-udp`: a live UDP [`Source`] and a fire-and-forget UDP
//! [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! UDP delivers nothing. A datagram that leaves the socket is neither
//! acknowledged nor retransmitted, so a peer that is down, a receive buffer
//! that is full and a fragment lost on the path all look exactly like a
//! delivered datagram to the sender. Use this connector where that is the
//! trade you want: metrics, logs, telemetry, anything whose value is in
//! arriving soon rather than arriving at all.
//!
//! [`UdpSource`] binds `bind` and yields one `RecordBatch` per received
//! datagram. [`UdpSink`] sends one datagram per encoded message to every
//! address in `connect`. One datagram carries exactly one payload and no
//! length prefix, because the datagram boundary is the frame, so the two
//! halves are wire compatible: the
//! [`Transformer`](saci_transformer::Transformer) the host resolved from the
//! node's `transformer` key decodes or encodes the bytes.
//!
//! A zero-length datagram is the liveness probe. The source answers one with a
//! zero-length datagram to its sender and never decodes it; the sink never
//! sends an empty payload as data. That is what lets a sink with
//! `probe kind="echo"` learn whether a `udp` source is still there. A
//! collector that is not a `udp` source answers nothing, so `probe
//! kind="icmp"` is the kind for one of those: it reads only the OS's relayed
//! unreachable. Under `on_failure="refuse"` a peer that failed its last round
//! is skipped, and a batch no peer accepted is a write error, which is what
//! the host's `retry`, `heal` and `dlq` blocks act on; `heal` is also the only
//! reconnect, since a rebuilt sink re-resolves every address and opens fresh
//! sockets.
//!
//! The source never reaches EOF, so only the stream runner can drive it. Its
//! `config` table takes `bind`, an optional `buffer` and `multicast_group`,
//! and a `schema_fields` array declaring the Arrow schema. The sink's takes
//! `connect`, an optional `broadcast`, `max_datagram_bytes` and `probe` node,
//! and `schema_fields`, and it runs under any run mode.
//!
//! ```kdl
//! transformer "json" format="ndjson"
//!
//! source "ingest" type="udp" transformer="json" component="Reading" {
//!     config bind="0.0.0.0:9700" buffer=64 {
//!         schema_fields "v" type="Int64" nullable=#false
//!     }
//! }
//!
//! sink "forward" type="udp" transformer="json" component="Reading" {
//!     config connect="collector.internal:9701" max_datagram_bytes=65507 {
//!         probe kind="icmp" interval_ms=5000 timeout_ms=1000 on_failure="refuse"
//!         schema_fields "v" type="Int64" nullable=#false
//!     }
//! }
//! ```

pub mod factory;
pub mod probe;
pub mod sink;
pub mod source;

pub use factory::{UdpSinkFactory, UdpSourceFactory};
pub use probe::{OnFailure, ProbeKind, ProbeSettings};
pub use sink::{MAX_DATAGRAM_BYTES, UdpSink};
pub use source::UdpSource;

/// A buffer this large never truncates a datagram: the UDP length field is 16
/// bits, so no IPv4 or plain IPv6 datagram can announce a longer payload, and
/// an IPv6 jumbogram is the one shape outside that, the same shape
/// [`MAX_DATAGRAM_BYTES`] ignores. Both halves read into one: Windows fails a
/// receive whose buffer is smaller than the datagram, so a short buffer turns
/// an oversized datagram into an error rather than a truncation.
pub(crate) const DATAGRAM_BUFFER_BYTES: usize = u16::MAX as usize;
