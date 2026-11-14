//! [`InMemoryMetricExporter`]: the inspector's metric source.
//!
//! A [`PushMetricExporter`] on a [`PeriodicReader`](opentelemetry_sdk::metrics::PeriodicReader)
//! attached to the **same** `SdkMeterProvider` the Prometheus exporter is on.
//! `MeterProviderBuilder::readers` is a plain vector and `with_reader` is
//! additive, so the two readers coexist and each gets its own independent
//! aggregation pipeline: this exporter's temporality choice cannot disturb the
//! Prometheus reader's.
//!
//! Chosen over scraping the in-process `prometheus::Registry`: the SDK path
//! keeps one aggregation source and hands over histogram `sum`/`count` directly,
//! with no exposition-format parsing and no protobuf types.
//!
//! ## The borrow is the whole design constraint
//!
//! `ResourceMetrics`, `ScopeMetrics`, `Metric`, `AggregatedMetrics` and
//! `MetricData<T>` derive only `Debug`, never `Clone`, with `pub(crate)`
//! fields and no public constructor. The `&ResourceMetrics` handed to
//! [`export`](PushMetricExporter::export) is one buffer the `PeriodicReader`
//! allocates once and overwrites every interval on its own thread, so it can be
//! neither stored nor cloned. Everything this exporter keeps is copied out into
//! owned [`SeriesPoint`]s synchronously, inside `export`, before it returns.

use std::time::Duration;

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;

use super::buffer::TimeBoundedBuffer;
use super::record::{MetricSample, Pair, SeriesKind, SeriesPoint, now_unix_ms};

/// Copies each export interval's data points into a [`TimeBoundedBuffer`].
#[derive(Debug, Clone)]
pub struct InMemoryMetricExporter {
    samples: TimeBoundedBuffer<MetricSample>,
}

impl InMemoryMetricExporter {
    /// Build an exporter writing into `samples`.
    pub(crate) fn new(samples: TimeBoundedBuffer<MetricSample>) -> Self {
        Self { samples }
    }
}

/// Attributes as sorted `(key, value)` pairs.
///
/// Sorted so a series' identity does not depend on the SDK's internal attribute
/// ordering: the snapshot builder keys series on `(name, attrs)`.
fn attrs_of<'a>(pairs: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> Vec<Pair> {
    let mut attrs: Vec<Pair> = pairs
        .map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect();
    attrs.sort_by(|a, b| a.0.cmp(&b.0));
    attrs
}

/// Convert every data point of one `MetricData<T>` into [`SeriesPoint`]s.
///
/// Generic over the numeric type because the SDK splits the same four
/// aggregations across `f64`, `u64` and `i64` variants; `to_f64` is the one
/// difference between them.
fn flatten<T, F>(name: &str, data: &MetricData<T>, out: &mut Vec<SeriesPoint>, to_f64: F)
where
    T: Copy,
    F: Fn(T) -> f64,
{
    match data {
        MetricData::Gauge(gauge) => {
            for point in gauge.data_points() {
                out.push(SeriesPoint {
                    name: name.to_string(),
                    kind: SeriesKind::Gauge,
                    attrs: attrs_of(point.attributes()),
                    value: to_f64(point.value()),
                    count: 0,
                });
            }
        }
        MetricData::Sum(sum) => {
            for point in sum.data_points() {
                out.push(SeriesPoint {
                    name: name.to_string(),
                    kind: SeriesKind::Counter,
                    attrs: attrs_of(point.attributes()),
                    value: to_f64(point.value()),
                    count: 0,
                });
            }
        }
        MetricData::Histogram(histogram) => {
            for point in histogram.data_points() {
                out.push(SeriesPoint {
                    name: name.to_string(),
                    kind: SeriesKind::Histogram,
                    attrs: attrs_of(point.attributes()),
                    value: to_f64(point.sum()),
                    count: point.count(),
                });
            }
        }
        MetricData::ExponentialHistogram(histogram) => {
            for point in histogram.data_points() {
                out.push(SeriesPoint {
                    name: name.to_string(),
                    kind: SeriesKind::Histogram,
                    attrs: attrs_of(point.attributes()),
                    value: to_f64(point.sum()),
                    // `ExponentialHistogramDataPoint::count` is `usize`, unlike
                    // the bucketed histogram's `u64`.
                    count: point.count() as u64,
                });
            }
        }
    }
}

impl PushMetricExporter for InMemoryMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut series = Vec::new();
        for scope in metrics.scope_metrics() {
            for metric in scope.metrics() {
                let name = metric.name();
                match metric.data() {
                    AggregatedMetrics::F64(data) => flatten(name, data, &mut series, |v| v),
                    AggregatedMetrics::U64(data) => {
                        flatten(name, data, &mut series, |v| v as f64);
                    }
                    AggregatedMetrics::I64(data) => {
                        flatten(name, data, &mut series, |v| v as f64);
                    }
                }
            }
        }

        self.samples.push(MetricSample {
            at_unix_ms: now_unix_ms(),
            series,
        });
        Ok(())
    }

    /// Nothing is buffered outside the ring buffer, so a flush has nothing to
    /// do.
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    /// Retain what was already captured: the dashboard is still readable while
    /// the process drains, and a later `export` call simply appends.
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    /// Cumulative, matching the Prometheus reader's own choice, so the two
    /// readers on the shared provider never disagree about what a counter
    /// means.
    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

    /// Drive a real `SdkMeterProvider` so the assertion covers the SDK's own
    /// aggregation, not a hand-built `ResourceMetrics` (which is impossible to
    /// construct: its fields are `pub(crate)`).
    #[test]
    fn export_captures_counter_gauge_and_histogram_points() {
        let samples: TimeBoundedBuffer<MetricSample> =
            TimeBoundedBuffer::new(Duration::from_secs(60), 16);
        let exporter = InMemoryMetricExporter::new(samples.clone());
        let reader = PeriodicReader::builder(exporter)
            // Long enough that only the explicit force_flush collects.
            .with_interval(Duration::from_secs(3600))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();

        let meter = provider.meter("inspector-test");
        let counter = meter.u64_counter("test_rows_total").build();
        counter.add(7, &[opentelemetry::KeyValue::new("stage", "enrich")]);
        let gauge = meter.f64_gauge("test_ready").build();
        gauge.record(1.0, &[]);
        let histogram = meter.f64_histogram("test_latency_seconds").build();
        histogram.record(0.25, &[]);
        histogram.record(0.75, &[]);

        provider.force_flush().expect("collect");

        let captured = samples.read_recent();
        assert_eq!(captured.len(), 1, "one export call, one sample");
        let series = &captured[0].series;

        let rows = series
            .iter()
            .find(|s| s.name == "test_rows_total")
            .expect("counter series");
        assert_eq!(rows.kind, SeriesKind::Counter);
        assert!((rows.value - 7.0).abs() < f64::EPSILON);
        assert_eq!(
            rows.attrs,
            vec![("stage".to_string(), "enrich".to_string())]
        );

        let ready = series
            .iter()
            .find(|s| s.name == "test_ready")
            .expect("gauge series");
        assert_eq!(ready.kind, SeriesKind::Gauge);

        let latency = series
            .iter()
            .find(|s| s.name == "test_latency_seconds")
            .expect("histogram series");
        assert_eq!(latency.kind, SeriesKind::Histogram);
        assert_eq!(latency.count, 2);
        assert!((latency.value - 1.0).abs() < 1e-9, "sum: {}", latency.value);
    }

    #[test]
    fn temporality_is_cumulative() {
        let samples: TimeBoundedBuffer<MetricSample> =
            TimeBoundedBuffer::new(Duration::from_secs(60), 4);
        let exporter = InMemoryMetricExporter::new(samples);
        assert_eq!(exporter.temporality(), Temporality::Cumulative);
    }
}
