//! `saci-connector-saci`: a service-to-service [`Source`] and [`Sink`] for
//! SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! [`SaciSink`] in one standalone service pushes `RecordBatch`es to
//! [`SaciSource`] in another, the way a `ChannelSink` feeds a `ChannelSource`
//! inside one process. The receiving side knows which service, workflow and
//! sink node produced each batch, because a session opens with a hello that
//! names all three, and it puts them in its own series labels and span
//! fields.
//!
//! Arrow IPC is the wire format and there is no `transformer` key: both ends
//! are SACI services that already agree on `RecordBatch`es. [`wire`] owns the
//! framing, and it is public so a foreign producer, or a harness standing in
//! for one half, can speak it.
//!
//! The source is stream-only: it never reaches EOF, so `run_mode
//! kind="stream"` is the one mode that can drive it. The sink runs under every
//! mode, and its `connect` key takes several addresses tried in order, so a
//! set of downstream services acts as failover peers.
//!
//! ```kdl
//! // Service A, pushing out.
//! sink "forward" type="saci" component="Tick" {
//!     config {
//!         connect "b.internal:9700" "b-standby.internal:9700"
//!         handshake_timeout_ms 5000
//!         schema_fields "v" type="Int64" nullable=#false
//!     }
//! }
//!
//! // Service B, receiving.
//! source "ingest" type="saci" component="Tick" {
//!     config {
//!         bind "0.0.0.0:9700"
//!         buffer 64
//!         max_frame_bytes 8388608
//!         schema_fields "v" type="Int64" nullable=#false
//!     }
//! }
//! ```
//!
//! ## Features
//!
//! - `tracing`: a `peer.send` span per written batch and a `peer.receive` span
//!   per received one, both carrying the peer fields.
//! - `trace-context`: carries the sender's span as a W3C `traceparent` and
//!   adopts it on receipt, so the two spans join one trace under OTLP export.
//! - `metrics`: the `saci_peer_source_*` series, labelled with the receiving
//!   node and the peer that fed it.

mod metrics;
#[cfg(feature = "trace-context")]
mod trace;

pub mod factory;
pub mod sink;
pub mod source;
pub mod wire;

pub use factory::{SaciSinkFactory, SaciSourceFactory};
pub use sink::SaciSink;
pub use source::SaciSource;
