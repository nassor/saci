//! OpenTelemetry instruments for the source half.
//!
//! [`Instruments`] has the same method surface whether or not the `metrics`
//! feature is on, so no call site carries a `#[cfg]`. With the feature off
//! every method is an empty inline body and the type is zero-sized.
//!
//! The provider is process-global: the `saci-service` binary installs one, and
//! this module reaches it through `opentelemetry::global::meter("saci")`. The
//! constructors build every instrument up front, so descriptions appear in
//! `/metrics` as soon as a source is constructed, before the first session.
//!
//! Only the source records. A `saci` sink's throughput is already the host's
//! `saci_sink_*` series, and a second copy of it under another name would
//! disagree with the first the moment a write fails halfway.
//!
//! Every series carries the receiving node's own `workflow` and `source`. A
//! session that got as far as a hello carries its peer's `peer_service`,
//! `peer_workflow` and `peer_sink` too, so a receiver fed by several services
//! reads them apart.

#[cfg(feature = "metrics")]
use saci_connector::NodeIdentity;

/// Source counters, pre-bound to their labels.
#[cfg(feature = "metrics")]
pub(crate) struct Instruments {
    sessions: opentelemetry::metrics::Counter<u64>,
    batches: opentelemetry::metrics::Counter<u64>,
    rows: opentelemetry::metrics::Counter<u64>,
    bytes: opentelemetry::metrics::Counter<u64>,
    errors: opentelemetry::metrics::Counter<u64>,
    attrs: Vec<opentelemetry::KeyValue>,
}

/// Zero-sized stand-in used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
pub(crate) struct Instruments;

#[cfg(feature = "metrics")]
impl Instruments {
    /// The instruments a source shares across sessions, labelled with its own
    /// workflow and node id and nothing else.
    ///
    /// A session refused before its hello parsed records through these, so a
    /// rejection with no identifiable peer is still visible.
    pub(crate) fn source(own: &NodeIdentity) -> Self {
        Self::with_attrs(vec![
            opentelemetry::KeyValue::new("workflow", own.workflow.clone()),
            opentelemetry::KeyValue::new("source", own.node.clone()),
        ])
    }

    /// The instruments one accepted session records through: the source's own
    /// labels plus the peer the hello named.
    pub(crate) fn session(own: &NodeIdentity, peer: &NodeIdentity) -> Self {
        Self::with_attrs(vec![
            opentelemetry::KeyValue::new("workflow", own.workflow.clone()),
            opentelemetry::KeyValue::new("source", own.node.clone()),
            opentelemetry::KeyValue::new("peer_service", peer.service.clone()),
            opentelemetry::KeyValue::new("peer_workflow", peer.workflow.clone()),
            opentelemetry::KeyValue::new("peer_sink", peer.node.clone()),
        ])
    }

    fn with_attrs(attrs: Vec<opentelemetry::KeyValue>) -> Self {
        let meter = opentelemetry::global::meter("saci");
        Self {
            sessions: meter
                .u64_counter("saci_peer_source_sessions_total")
                .with_description("Peer sessions a saci source accepted or rejected, by outcome")
                .build(),
            batches: meter
                .u64_counter("saci_peer_source_batches_total")
                .with_description("Total batches a saci source received from its peers")
                .build(),
            rows: meter
                .u64_counter("saci_peer_source_rows_total")
                .with_description("Total rows a saci source received from its peers")
                .build(),
            bytes: meter
                .u64_counter("saci_peer_source_bytes_total")
                .with_description("Total data frame body bytes a saci source received")
                .build(),
            errors: meter
                .u64_counter("saci_peer_source_errors_total")
                .with_description("Total saci source session errors by kind")
                .build(),
            attrs,
        }
    }

    /// One session the source took.
    pub(crate) fn session_accepted(&self) {
        self.outcome("accepted");
    }

    /// One session the source refused.
    pub(crate) fn session_rejected(&self) {
        self.outcome("rejected");
    }

    fn outcome(&self, outcome: &'static str) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("outcome", outcome));
        self.sessions.add(1, &attrs);
    }

    /// One received batch of `rows`, carried by a data frame of `bytes`.
    pub(crate) fn batch(&self, rows: u64, bytes: u64) {
        self.batches.add(1, &self.attrs);
        self.rows.add(rows, &self.attrs);
        self.bytes.add(bytes, &self.attrs);
    }

    /// One error of `kind`, one of `frame`, `decode` or `schema`.
    pub(crate) fn error(&self, kind: &'static str) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("kind", kind));
        self.errors.add(1, &attrs);
    }
}

#[cfg(not(feature = "metrics"))]
impl Instruments {
    #[inline]
    pub(crate) fn source(_own: &saci_connector::NodeIdentity) -> Self {
        Self
    }
    #[inline]
    pub(crate) fn session(
        _own: &saci_connector::NodeIdentity,
        _peer: &saci_connector::NodeIdentity,
    ) -> Self {
        Self
    }
    #[inline]
    pub(crate) fn session_accepted(&self) {}
    #[inline]
    pub(crate) fn session_rejected(&self) {}
    #[inline]
    pub(crate) fn batch(&self, _rows: u64, _bytes: u64) {}
    #[inline]
    pub(crate) fn error(&self, _kind: &'static str) {}
}
