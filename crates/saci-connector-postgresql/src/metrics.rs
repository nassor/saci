//! OpenTelemetry instruments for the connector.
//!
//! [`Instruments`] has the same method surface whether or not the `metrics`
//! feature is on, so no call site carries a `#[cfg]`. With the feature off every
//! method is an empty inline body and the type is zero-sized.
//!
//! The provider is process-global: the `saci-service` binary installs one, and
//! this module reaches it through `opentelemetry::global::meter("saci")`. The
//! constructors build every instrument up front, so descriptions appear in
//! `/metrics` as soon as a connector is constructed, before the first batch.
//!
//! Names follow the repository's `saci_<subsystem>_<thing>_total` / `_seconds`
//! convention.

/// Source and sink counters, histograms and gauges, pre-bound to their labels.
#[cfg(feature = "metrics")]
pub(crate) struct Instruments {
    batches: opentelemetry::metrics::Counter<u64>,
    rows: opentelemetry::metrics::Counter<u64>,
    duration: opentelemetry::metrics::Histogram<f64>,
    errors: opentelemetry::metrics::Counter<u64>,
    skipped: opentelemetry::metrics::Counter<u64>,
    gauge: opentelemetry::metrics::Gauge<u64>,
    attrs: Vec<opentelemetry::KeyValue>,
}

/// Zero-sized stand-in used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
pub(crate) struct Instruments;

#[cfg(feature = "metrics")]
impl Instruments {
    /// Instruments for a source, labelled with its name and mode.
    pub(crate) fn source(name: &str, mode: &'static str) -> Self {
        let meter = opentelemetry::global::meter("saci");
        Self {
            batches: meter
                .u64_counter("saci_postgres_source_batches_total")
                .with_description("Total batches emitted by PostgreSQL sources")
                .build(),
            rows: meter
                .u64_counter("saci_postgres_source_rows_total")
                .with_description("Total rows emitted by PostgreSQL sources")
                .build(),
            duration: meter
                .f64_histogram("saci_postgres_source_query_duration_seconds")
                .with_description("PostgreSQL source query duration in seconds")
                .build(),
            errors: meter
                .u64_counter("saci_postgres_source_errors_total")
                .with_description("Total PostgreSQL source errors by kind")
                .build(),
            skipped: meter
                .u64_counter("saci_postgres_source_skipped_changes_total")
                .with_description("Logical changes skipped because they name another relation")
                .build(),
            gauge: meter
                .u64_gauge("saci_postgres_source_wal_lag_bytes")
                .with_description(
                    "WAL bytes between the slot's confirmed flush LSN and the server's current LSN",
                )
                .build(),
            attrs: vec![
                opentelemetry::KeyValue::new("name", name.to_string()),
                opentelemetry::KeyValue::new("mode", mode),
            ],
        }
    }

    /// Instruments for a sink, labelled with its name.
    pub(crate) fn sink(name: &str) -> Self {
        let meter = opentelemetry::global::meter("saci");
        Self {
            batches: meter
                .u64_counter("saci_postgres_sink_flushes_total")
                .with_description("Total PostgreSQL sink flush transactions")
                .build(),
            rows: meter
                .u64_counter("saci_postgres_sink_rows_total")
                .with_description("Total rows written by PostgreSQL sinks")
                .build(),
            duration: meter
                .f64_histogram("saci_postgres_sink_flush_duration_seconds")
                .with_description("PostgreSQL sink flush duration in seconds")
                .build(),
            errors: meter
                .u64_counter("saci_postgres_sink_errors_total")
                .with_description("Total PostgreSQL sink errors by kind")
                .build(),
            skipped: meter
                .u64_counter("saci_postgres_sink_skipped_rows_total")
                .with_description("Rows the PostgreSQL sink discarded before writing")
                .build(),
            gauge: meter
                .u64_gauge("saci_postgres_sink_pending_rows")
                .with_description("Rows buffered in a PostgreSQL sink but not yet flushed")
                .build(),
            attrs: vec![opentelemetry::KeyValue::new("name", name.to_string())],
        }
    }

    /// One batch of `rows` produced or written.
    pub(crate) fn batch(&self, rows: u64) {
        self.batches.add(1, &self.attrs);
        self.rows.add(rows, &self.attrs);
    }

    /// A query or flush that took `seconds`.
    pub(crate) fn observe(&self, seconds: f64) {
        self.duration.record(seconds, &self.attrs);
    }

    /// One error of `kind`, one of `connect`, `query`, `decode`, `encode`,
    /// `copy`, `offset` or `slot`.
    pub(crate) fn error(&self, kind: &'static str) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("kind", kind));
        self.errors.add(1, &attrs);
    }

    /// `n` rows skipped without being emitted.
    pub(crate) fn skipped(&self, n: u64) {
        self.skipped.add(n, &self.attrs);
    }

    /// Latest value of this connector's gauge.
    pub(crate) fn gauge(&self, v: u64) {
        self.gauge.record(v, &self.attrs);
    }
}

#[cfg(not(feature = "metrics"))]
impl Instruments {
    #[inline]
    pub(crate) fn source(_name: &str, _mode: &'static str) -> Self {
        Self
    }
    #[inline]
    pub(crate) fn sink(_name: &str) -> Self {
        Self
    }
    #[inline]
    pub(crate) fn batch(&self, _rows: u64) {}
    #[inline]
    pub(crate) fn observe(&self, _seconds: f64) {}
    #[inline]
    pub(crate) fn error(&self, _kind: &'static str) {}
    #[inline]
    pub(crate) fn skipped(&self, _n: u64) {}
    #[inline]
    pub(crate) fn gauge(&self, _v: u64) {}
}
