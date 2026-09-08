//! OpenTelemetry instruments for the connector.
//!
//! [`Instruments`] has the same method surface whether or not the `metrics`
//! feature is on, so no call site carries a `#[cfg]`. With the feature off every
//! method is an empty inline body and the type is zero-sized.
//!
//! The provider is process-global: the `saci-service` binary installs one, and
//! this module reaches it through `opentelemetry::global::meter("saci")`. Names
//! follow the repository's `saci_<subsystem>_<thing>_total` convention.

/// Source and sink counters, pre-bound to their labels.
#[cfg(feature = "metrics")]
pub(crate) struct Instruments {
    rows: opentelemetry::metrics::Counter<u64>,
    batches: opentelemetry::metrics::Counter<u64>,
    changes: opentelemetry::metrics::Counter<u64>,
    flushes: opentelemetry::metrics::Counter<u64>,
    attrs: Vec<opentelemetry::KeyValue>,
}

/// Zero-sized stand-in used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
pub(crate) struct Instruments;

#[cfg(feature = "metrics")]
impl Instruments {
    /// Instruments labelled with a source's name and read mode.
    pub(crate) fn source(name: &str, mode: &'static str) -> Self {
        let meter = opentelemetry::global::meter("saci");
        Self {
            rows: meter.u64_counter("saci_turso_source_rows_total").build(),
            batches: meter.u64_counter("saci_turso_source_batches_total").build(),
            changes: meter.u64_counter("saci_turso_source_changes_total").build(),
            flushes: meter.u64_counter("saci_turso_sink_flushes_total").build(),
            attrs: vec![
                opentelemetry::KeyValue::new("node", name.to_string()),
                opentelemetry::KeyValue::new("mode", mode),
            ],
        }
    }

    /// Instruments labelled with a sink's name.
    pub(crate) fn sink(name: &str) -> Self {
        let meter = opentelemetry::global::meter("saci");
        Self {
            rows: meter
                .u64_counter("saci_turso_sink_rows_written_total")
                .build(),
            batches: meter.u64_counter("saci_turso_source_batches_total").build(),
            changes: meter.u64_counter("saci_turso_source_changes_total").build(),
            flushes: meter.u64_counter("saci_turso_sink_flushes_total").build(),
            attrs: vec![opentelemetry::KeyValue::new("node", name.to_string())],
        }
    }

    /// One source batch of `rows` rows.
    pub(crate) fn source_batch(&self, rows: u64) {
        self.rows.add(rows, &self.attrs);
        self.batches.add(1, &self.attrs);
    }

    /// `n` change records decoded by the `cdc` mode.
    pub(crate) fn changes(&self, n: u64) {
        self.changes.add(n, &self.attrs);
    }

    /// One sink flush of `rows` rows.
    pub(crate) fn sink_flush(&self, rows: u64) {
        self.rows.add(rows, &self.attrs);
        self.flushes.add(1, &self.attrs);
    }
}

#[cfg(not(feature = "metrics"))]
impl Instruments {
    /// Instruments labelled with a source's name and read mode.
    #[inline]
    pub(crate) fn source(_name: &str, _mode: &'static str) -> Self {
        Instruments
    }

    /// Instruments labelled with a sink's name.
    #[inline]
    pub(crate) fn sink(_name: &str) -> Self {
        Instruments
    }

    /// One source batch of `rows` rows.
    #[inline]
    pub(crate) fn source_batch(&self, _rows: u64) {}

    /// `n` change records decoded by the `cdc` mode.
    #[inline]
    pub(crate) fn changes(&self, _n: u64) {}

    /// One sink flush of `rows` rows.
    #[inline]
    pub(crate) fn sink_flush(&self, _rows: u64) {}
}
