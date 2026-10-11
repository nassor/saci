//! `saci-connector-opentelemetry`: an OpenTelemetry metrics [`Source`] and
//! [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! `OpenTelemetryMetricsSource` reads OTLP/HTTP metrics into Arrow rows, either
//! by listening for requests or by reading a url; `OpenTelemetryMetricsSink`
//! posts the rows of a batch back as one OTLP/HTTP request. Both speak
//! Protobuf and JSON, and neither resolves a `transformer`: an OTLP request is
//! the wire format, so the connector owns its own decoding and encoding.
//!
//! # Source modes
//!
//! - `kind="push"` (the default): a listener. `bind` defaults to `127.0.0.1:4318`,
//!   the standard OTLP/HTTP port, and `path` to `/v1/metrics`. The top-level
//!   `bind`, `path`, `auth`, `buffer` and `max_body_bytes` keys are the
//!   shorthand for this mode, so a config that needs nothing else declares no
//!   `mode` block at all.
//! - `kind="pull"`: `url` is read once, or every `interval_ms`. With
//!   `stop_at_end` the source reaches EOF after one read, which is what makes
//!   it drivable by the batch runner; without it the source is live and only
//!   the stream runner drives it.
//!
//! # Rows
//!
//! `schema` describes the Arrow view: one row per OTLP data point, under the
//! ten canonical columns or under the `schema_fields` a config declares.
//! `schema::metrics_schema` is the canonical one, and `schema` documents how a
//! declared schema is filled, which attribute keys are reserved, and what the
//! view does not carry.
//!
//! # Sink exports
//!
//! A `url` naming only a base gains the standard `/v1/metrics` path, so both
//! `http://collector:4318` and `http://collector:4318/v1/metrics` work.
//! `encoding` picks the body, `metric_name` names a batch with no name column,
//! `service_name` becomes the `service.name` resource attribute, and
//! `batch_rows` caps how many rows one export carries.
//!
//! # Cargo features
//!
//! - `tracing`: emit `tracing` events for what a run cannot see otherwise, such
//!   as the data points an Arrow view has no home for.

#![deny(missing_docs)]

pub mod config;
pub mod factory;
pub mod schema;
pub mod sink;
pub mod source;

pub use config::{
    AuthConfig, Encoding, OpenTelemetryMetricsSinkConfig, OpenTelemetryMetricsSourceConfig,
    PullMode, PushMode, SourceModeConfig,
};
pub use factory::{OpenTelemetryMetricsSinkFactory, OpenTelemetryMetricsSourceFactory};
pub use sink::OpenTelemetryMetricsSink;
pub use source::OpenTelemetryMetricsSource;

use std::sync::LazyLock;

/// The workspace's `ring` crypto provider, installed on first force.
///
/// Process global and idempotent: a provider another crate installed first, or
/// a racing install from another thread, is left in place.
static RING_PROVIDER: LazyLock<()> = LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
});

/// Install the `ring` provider, once per process.
///
/// Every rustls-backed client this crate builds goes through here first, so
/// reqwest's "no process-level CryptoProvider" panic, which its
/// `rustls-no-provider` feature leaves reachable, cannot be.
pub(crate) fn install_ring() {
    LazyLock::force(&RING_PROVIDER);
}
