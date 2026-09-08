//! OpenTelemetry instruments for all three halves of this connector.
//!
//! [`Instruments`] has the same method surface whether or not the `metrics`
//! feature is on, so no call site carries a `#[cfg]`. With the feature off
//! every method is an empty inline body and the type is zero-sized.
//!
//! The provider is process-global: the `saci-service` binary installs one, and
//! this module reaches it through `opentelemetry::global::meter("saci")`. The
//! constructors build every instrument up front, so descriptions appear in
//! `/metrics` as soon as a connector is constructed, before its first request.
//!
//! Every series carries `role`, one of `source`, `sink` or `push`, so the three
//! halves are read apart under one set of names. A connector the host built
//! carries its `workflow` and `node` too; a library caller that constructed one
//! with no [`NodeIdentity`](saci_connector::NodeIdentity) records under `role`
//! alone rather than under empty labels.

/// Request counters and the duration histogram, pre-bound to their labels.
#[cfg(feature = "metrics")]
pub(crate) struct Instruments {
    requests: opentelemetry::metrics::Counter<u64>,
    errors: opentelemetry::metrics::Counter<u64>,
    duration: opentelemetry::metrics::Histogram<f64>,
    bytes: opentelemetry::metrics::Counter<u64>,
    attrs: Vec<opentelemetry::KeyValue>,
}

/// Zero-sized stand-in used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
pub(crate) struct Instruments;

#[cfg(feature = "metrics")]
impl Instruments {
    /// The instruments one connector instance records through.
    ///
    /// `role` is `source`, `sink` or `push`. `identity` adds the node's own
    /// `workflow` and `node` when the host bound one.
    pub(crate) fn new(role: &'static str, identity: Option<&saci_connector::NodeIdentity>) -> Self {
        let mut attrs = vec![opentelemetry::KeyValue::new("role", role)];
        if let Some(identity) = identity {
            attrs.push(opentelemetry::KeyValue::new(
                "workflow",
                identity.workflow.clone(),
            ));
            attrs.push(opentelemetry::KeyValue::new("node", identity.node.clone()));
        }
        let meter = opentelemetry::global::meter("saci");
        Self {
            requests: meter
                .u64_counter("saci_http_requests_total")
                .with_description("HTTP exchanges that produced a response, by method and status")
                .build(),
            errors: meter
                .u64_counter("saci_http_request_errors_total")
                .with_description("HTTP request failures by kind")
                .build(),
            duration: meter
                .f64_histogram("saci_http_request_duration_seconds")
                .with_description("HTTP exchange duration in seconds, by method")
                .with_unit("s")
                .build(),
            bytes: meter
                .u64_counter("saci_http_bytes_total")
                .with_description("HTTP body bytes read or written, by direction")
                .build(),
            attrs,
        }
    }

    /// One exchange that produced a response, whatever the status was.
    pub(crate) fn request(&self, method: &str, status: u16) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("method", method.to_string()));
        attrs.push(opentelemetry::KeyValue::new("status", status.to_string()));
        self.requests.add(1, &attrs);
    }

    /// One failure of `kind`: `transport`, `status`, `decode`, `size` or
    /// `auth`.
    pub(crate) fn error(&self, kind: &'static str) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("kind", kind));
        self.errors.add(1, &attrs);
    }

    /// How long one exchange took, send to last body byte.
    pub(crate) fn elapsed(&self, method: &str, seconds: f64) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("method", method.to_string()));
        self.duration.record(seconds, &attrs);
    }

    /// Body bytes moved: `in` for a body read, `out` for one written.
    pub(crate) fn bytes(&self, direction: &'static str, count: u64) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("direction", direction));
        self.bytes.add(count, &attrs);
    }
}

#[cfg(not(feature = "metrics"))]
impl Instruments {
    #[inline]
    pub(crate) fn new(
        _role: &'static str,
        _identity: Option<&saci_connector::NodeIdentity>,
    ) -> Self {
        Self
    }
    #[inline]
    pub(crate) fn request(&self, _method: &str, _status: u16) {}
    #[inline]
    pub(crate) fn error(&self, _kind: &'static str) {}
    #[inline]
    pub(crate) fn elapsed(&self, _method: &str, _seconds: f64) {}
    #[inline]
    pub(crate) fn bytes(&self, _direction: &'static str, _count: u64) {}
}
