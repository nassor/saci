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
//! convention. A source records `saci_mariadb_source_batches_total`,
//! `_rows_total`, `_query_duration_seconds`, `_errors_total` and
//! `_skipped_changes_total`, labelled `name` and `mode`; a cursor source (every
//! mode but `cdc_binlog`) adds `_sequence_gaps_total` and
//! `_sequence_held_rows`. A sink records
//! `saci_mariadb_sink_flushes_total`, `_rows_total`, `_flush_duration_seconds`,
//! `_errors_total` and `_pending_rows`, labelled `name`. The method a role has no
//! series for records nothing.

/// Source and sink counters, histograms and gauges, pre-bound to their labels.
#[cfg(feature = "metrics")]
pub(crate) struct Instruments {
    batches: opentelemetry::metrics::Counter<u64>,
    rows: opentelemetry::metrics::Counter<u64>,
    duration: opentelemetry::metrics::Histogram<f64>,
    errors: opentelemetry::metrics::Counter<u64>,
    /// Source only: changes skipped because they name another table.
    skipped: Option<opentelemetry::metrics::Counter<u64>>,
    /// Cursor sources only: holes in an `AUTO_INCREMENT` cursor that were
    /// passed, by outcome.
    gaps: Option<opentelemetry::metrics::Counter<u64>>,
    /// Cursor sources only: committed rows held back behind an open hole at the
    /// last read.
    held: Option<opentelemetry::metrics::Gauge<u64>>,
    /// Sink only: rows buffered and not yet flushed.
    pending: Option<opentelemetry::metrics::Gauge<u64>>,
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
        let cursor = mode != "cdc_binlog";
        Self {
            batches: meter
                .u64_counter("saci_mariadb_source_batches_total")
                .with_description("Total batches emitted by MariaDB and MySQL sources")
                .build(),
            rows: meter
                .u64_counter("saci_mariadb_source_rows_total")
                .with_description("Total rows emitted by MariaDB and MySQL sources")
                .build(),
            duration: meter
                .f64_histogram("saci_mariadb_source_query_duration_seconds")
                .with_description("MariaDB and MySQL source query duration in seconds")
                .build(),
            errors: meter
                .u64_counter("saci_mariadb_source_errors_total")
                .with_description("Total MariaDB and MySQL source errors by kind")
                .build(),
            skipped: Some(
                meter
                    .u64_counter("saci_mariadb_source_skipped_changes_total")
                    .with_description("Binlog changes skipped because they name another table")
                    .build(),
            ),
            gaps: cursor.then(|| {
                meter
                    .u64_counter("saci_mariadb_source_sequence_gaps_total")
                    .with_description(
                        "Holes in an AUTO_INCREMENT cursor passed, by outcome: settled, timeout or cap",
                    )
                    .build()
            }),
            held: cursor.then(|| {
                meter
                    .u64_gauge("saci_mariadb_source_sequence_held_rows")
                    .with_description(
                        "Committed rows held back behind an open hole in an AUTO_INCREMENT cursor",
                    )
                    .build()
            }),
            pending: None,
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
                .u64_counter("saci_mariadb_sink_flushes_total")
                .with_description("Total MariaDB and MySQL sink flush transactions")
                .build(),
            rows: meter
                .u64_counter("saci_mariadb_sink_rows_total")
                .with_description("Total rows written by MariaDB and MySQL sinks")
                .build(),
            duration: meter
                .f64_histogram("saci_mariadb_sink_flush_duration_seconds")
                .with_description("MariaDB and MySQL sink flush duration in seconds")
                .build(),
            errors: meter
                .u64_counter("saci_mariadb_sink_errors_total")
                .with_description("Total MariaDB and MySQL sink errors by kind")
                .build(),
            skipped: None,
            gaps: None,
            held: None,
            pending: Some(
                meter
                    .u64_gauge("saci_mariadb_sink_pending_rows")
                    .with_description(
                        "Rows buffered in a MariaDB or MySQL sink but not yet flushed",
                    )
                    .build(),
            ),
            attrs: vec![opentelemetry::KeyValue::new("name", name.to_string())],
        }
    }

    /// One batch of `rows` produced, or one flush that wrote `rows`.
    pub(crate) fn batch(&self, rows: u64) {
        self.batches.add(1, &self.attrs);
        self.rows.add(rows, &self.attrs);
    }

    /// A query or flush that took `seconds`.
    pub(crate) fn observe(&self, seconds: f64) {
        self.duration.record(seconds, &self.attrs);
    }

    /// One error of `kind`: for a source `connect`, `query`, `decode`,
    /// `offset`, `schema`, `binlog`, `trigger` or `snapshot`; for a sink
    /// `connect`, `encode`, `write` or `commit`.
    pub(crate) fn error(&self, kind: &'static str) {
        let mut attrs = self.attrs.clone();
        attrs.push(opentelemetry::KeyValue::new("kind", kind));
        self.errors.add(1, &attrs);
    }

    /// `n` changes skipped without being emitted. A sink records nothing.
    pub(crate) fn skipped(&self, n: u64) {
        if let Some(skipped) = &self.skipped {
            skipped.add(n, &self.attrs);
        }
    }

    /// `n` holes in an `AUTO_INCREMENT` cursor passed with `outcome`
    /// (`settled`, `timeout` or `cap`). Only a cursor source records it.
    pub(crate) fn gaps(&self, n: u64, outcome: &'static str) {
        if let Some(gaps) = &self.gaps {
            let mut attrs = self.attrs.clone();
            attrs.push(opentelemetry::KeyValue::new("outcome", outcome));
            gaps.add(n, &attrs);
        }
    }

    /// Rows held back behind an open hole at the last read. Only a cursor
    /// source records it.
    pub(crate) fn held_rows(&self, rows: u64) {
        if let Some(held) = &self.held {
            held.record(rows, &self.attrs);
        }
    }

    /// The sink's buffered row count. A source records nothing.
    pub(crate) fn pending(&self, rows: u64) {
        if let Some(pending) = &self.pending {
            pending.record(rows, &self.attrs);
        }
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
    pub(crate) fn pending(&self, _rows: u64) {}
    #[inline]
    pub(crate) fn gaps(&self, _n: u64, _outcome: &'static str) {}
    #[inline]
    pub(crate) fn held_rows(&self, _rows: u64) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_method_is_callable_for_both_roles_whatever_the_features() {
        for instruments in [
            Instruments::source("orders", "polling"),
            Instruments::sink("orders"),
        ] {
            instruments.batch(3);
            instruments.observe(0.25);
            instruments.error("query");
            instruments.skipped(2);
            instruments.pending(5);
            instruments.gaps(1, "timeout");
            instruments.held_rows(4);
        }
    }

    #[cfg(not(feature = "metrics"))]
    #[test]
    fn without_the_metrics_feature_the_instruments_take_no_space() {
        assert_eq!(std::mem::size_of::<Instruments>(), 0);
    }
}
