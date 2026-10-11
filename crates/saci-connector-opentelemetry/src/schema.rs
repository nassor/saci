//! The Arrow view of OTLP metrics, and the conversion in both directions.
//!
//! An OTLP request and a [`RecordBatch`] are two views of one set of data
//! points. [`export_request_to_record_batches`] reads a request into rows and
//! [`record_batch_to_export_request`] writes rows back out, so a source runs
//! the first and a sink the second. The two are inverses for every field the
//! canonical schema carries, including the reserved attribute keys below.
//!
//! # The canonical schema
//!
//! [`metrics_schema`] is the ten-column schema a source emits when its config
//! declares no `schema_fields`. One row is one OTLP data point, so a metric
//! with three data points is three rows and the metric's own name, unit and
//! description repeat on each. [`mod@column`] names the columns.
//!
//! # A declared schema
//!
//! A source whose config declares `schema_fields` emits exactly that schema
//! instead. Each declared column is filled from the canonical column of the
//! same name where one exists, and from the data point's attribute of that
//! name otherwise, which is the shape a sink's own attribute encoding writes.
//! The metric value goes to a column named `value` when the schema declares
//! one, and otherwise to the first declared numeric column no canonical name
//! claims, which mirrors how a sink picks the column it reads a value from.
//! A declared column a request has no value for is null, and a request that
//! leaves a non-nullable declared column without one is refused.
//!
//! # Attributes
//!
//! `attributes` and `resource_attributes` hold a JSON object, one key per OTLP
//! attribute, each in its JSON form: a string, a number, a boolean, an array,
//! an object, or, for a `bytesValue`, the object `{"otel.bytes": "<base64>"}`.
//! A `doubleValue` JSON has no number for travels as the string `"NaN"`,
//! `"Infinity"` or `"-Infinity"`, the three the OTLP JSON mapping defines, and
//! reads back as a double.
//!
//! # Reserved attribute keys
//!
//! Parts of an OTLP data point have no canonical column, and travel in the
//! `attributes` object under keys this connector owns. [`reserved`] names them.
//! A sink consumes the key it knows when it rebuilds a request, so a user
//! attribute must not reuse one: it would be read as the field the key names.
//! A gauge or sum row defines no distribution, so `otel.distribution` on one is
//! an ordinary attribute. Every other key is an ordinary attribute too.
//!
//! # What the view does not carry
//!
//! Exemplars, `Metric.metadata`, both `schema_url` fields and every
//! `dropped_attributes_count` have no column and no reserved key: a data point
//! carries none of them through the Arrow view.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array as _, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::Engine as _;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
    SummaryDataPoint, exponential_histogram_data_point, metric, number_data_point,
    summary_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;
use saci_core::error::SaciError;
use serde_json::{Map, Value as Json};

use crate::config::Encoding;

/// The connector name every error of the request-to-rows direction opens with.
const WHAT: &str = "OpenTelemetryMetricsSource";

/// The connector name every error of the rows-to-request direction opens with.
const SINK_WHAT: &str = "OpenTelemetryMetricsSink";

/// The columns of the canonical schema, and the aliases a sink also reads.
pub mod column {
    /// The metric's name, never null.
    pub const NAME: &str = "name";
    /// A sink reads this column as the metric name when `name` is absent.
    pub const NAME_ALIAS: &str = "metric_name";
    /// The data point's value, never null.
    ///
    /// A gauge's or a sum's own value, a histogram's or an exponential
    /// histogram's sum, and a summary's sum.
    pub const VALUE: &str = "value";
    /// The data point's timestamp in Unix nanoseconds, never null.
    pub const TIMESTAMP: &str = "timestamp";
    /// A sink reads this column as the timestamp when `timestamp` is absent.
    pub const TIMESTAMP_ALIAS: &str = "time_unix_nano";
    /// One of the strings in [`KINDS`][super::KINDS], never null.
    pub const KIND: &str = "kind";
    /// A sink reads this column as the kind when `kind` is absent.
    pub const KIND_ALIAS: &str = "metric_type";
    /// The metric's unit, null when the request carries none.
    pub const UNIT: &str = "unit";
    /// The metric's description, null when the request carries none.
    pub const DESCRIPTION: &str = "description";
    /// The data point's attributes as a JSON object, null when it has none.
    pub const ATTRIBUTES: &str = "attributes";
    /// The resource's attributes as a JSON object, null when it has none.
    pub const RESOURCE_ATTRIBUTES: &str = "resource_attributes";
    /// A histogram's, an exponential histogram's or a summary's count, null
    /// for a gauge and a sum.
    pub const COUNT: &str = "count";
    /// The data point's start timestamp in Unix nanoseconds, null when the
    /// request carries none.
    pub const START_TIMESTAMP: &str = "start_timestamp";
}

/// The attribute keys this connector owns inside the `attributes` object.
///
/// A sink consumes each of these when it rebuilds a request, so a user
/// attribute must not reuse one.
pub mod reserved {
    /// The instrumentation scope, an object with `name`, `version` and an
    /// optional `attributes` object.
    pub const SCOPE: &str = "otel.scope";
    /// `"cumulative"`, `"delta"` or `"unspecified"`: the aggregation
    /// temporality of a sum, a histogram or an exponential histogram.
    pub const TEMPORALITY: &str = "otel.aggregation_temporality";
    /// A boolean: a sum's monotonic flag.
    pub const MONOTONIC: &str = "otel.monotonic";
    /// The buckets of a histogram or an exponential histogram, or the
    /// quantiles of a summary. [`super::distribution`] lists its keys.
    pub const DISTRIBUTION: &str = "otel.distribution";
    /// The key a `bytesValue` attribute is written under, inside an object of
    /// its own, holding base64.
    pub const BYTES: &str = "otel.bytes";
}

/// The keys of the [`reserved::DISTRIBUTION`] object, one set per kind.
pub mod distribution {
    /// Histogram: the upper bound of every bucket but the last, as numbers.
    pub const EXPLICIT_BOUNDS: &str = "explicit_bounds";
    /// Histogram: the count of every bucket, as numbers.
    pub const BUCKET_COUNTS: &str = "bucket_counts";
    /// Histogram and exponential histogram: the smallest value, a number.
    pub const MIN: &str = "min";
    /// Histogram and exponential histogram: the largest value, a number.
    pub const MAX: &str = "max";
    /// Exponential histogram: the base-2 exponent of each bucket, an integer.
    pub const SCALE: &str = "scale";
    /// Exponential histogram: the count of values in the zero bucket.
    pub const ZERO_COUNT: &str = "zero_count";
    /// Exponential histogram: the width of the zero bucket, a number.
    pub const ZERO_THRESHOLD: &str = "zero_threshold";
    /// Exponential histogram: the positive buckets, `{ "offset", "counts" }`.
    pub const POSITIVE: &str = "positive";
    /// Exponential histogram: the negative buckets, `{ "offset", "counts" }`.
    pub const NEGATIVE: &str = "negative";
    /// Summary: the values at the reported quantiles, one
    /// `{ "quantile", "value" }` object each.
    pub const QUANTILES: &str = "quantiles";
    /// A bucket set's index of the first bucket, an integer.
    pub const OFFSET: &str = "offset";
    /// A bucket set's per-bucket counts, as numbers.
    pub const COUNTS: &str = "counts";
    /// A quantile value's quantile, a number.
    pub const QUANTILE: &str = "quantile";
    /// A quantile value's value, a number.
    pub const VALUE: &str = "value";
}

/// Every value the `kind` column takes, in schema order.
pub const KINDS: [&str; 5] = [
    "gauge",
    "sum",
    "histogram",
    "exponential_histogram",
    "summary",
];

/// The `content-type` a protobuf OTLP/HTTP body carries.
pub const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// The `content-type` a JSON OTLP/HTTP body carries.
pub const CONTENT_TYPE_JSON: &str = "application/json";

/// The metric name a sink falls back to when a batch carries none.
pub const DEFAULT_METRIC_NAME: &str = "saci_metric";

/// Which OTLP data variant a row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl MetricKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Sum => "sum",
            Self::Histogram => "histogram",
            Self::ExponentialHistogram => "exponential_histogram",
            Self::Summary => "summary",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "gauge" => Some(Self::Gauge),
            "sum" => Some(Self::Sum),
            "histogram" => Some(Self::Histogram),
            "exponential_histogram" => Some(Self::ExponentialHistogram),
            "summary" => Some(Self::Summary),
            _ => None,
        }
    }
}

/// One data point, in the shape both directions pass around.
///
/// The attributes are the JSON object the `attributes` column holds, so the
/// reserved keys travel with them and each direction reads or writes the ones
/// its side of the pair needs.
struct Row {
    name: String,
    value: f64,
    timestamp: u64,
    kind: MetricKind,
    unit: Option<String>,
    description: Option<String>,
    attributes: Map<String, Json>,
    /// Shared by every data point of one resource: a request holds one per
    /// resource however many points name it.
    resource_attributes: Arc<Map<String, Json>>,
    count: Option<u64>,
    start_timestamp: Option<u64>,
}

/// What one request converted to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsConversion {
    /// How many rows the request converted to.
    pub rows: usize,
    /// The data points the Arrow view has no home for, or `None` when it took
    /// every one. A source reports this as the request's `partial_success`.
    pub rejected: Option<RejectedPoints>,
}

/// The data points of one request that the Arrow view could not represent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedPoints {
    /// How many data points were dropped.
    pub count: u64,
    /// Why, for the request's `partial_success.error_message`.
    pub reason: String,
}

/// The canonical schema: one row per OTLP data point.
pub fn metrics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(column::NAME, DataType::Utf8, false),
        Field::new(column::VALUE, DataType::Float64, false),
        Field::new(column::TIMESTAMP, DataType::UInt64, false),
        Field::new(column::KIND, DataType::Utf8, false),
        Field::new(column::UNIT, DataType::Utf8, true),
        Field::new(column::DESCRIPTION, DataType::Utf8, true),
        Field::new(column::ATTRIBUTES, DataType::Utf8, true),
        Field::new(column::RESOURCE_ATTRIBUTES, DataType::Utf8, true),
        Field::new(column::COUNT, DataType::UInt64, true),
        Field::new(column::START_TIMESTAMP, DataType::UInt64, true),
    ]))
}

/// The response body for one conversion.
///
/// A request the source took whole answers an empty response, which is what
/// OTLP defines for success. A request it took in part answers `partial
/// success` naming what it dropped, so the sender learns what happened instead
/// of retrying it forever.
pub fn response_for(conversion: &MetricsConversion) -> ExportMetricsServiceResponse {
    ExportMetricsServiceResponse {
        partial_success: conversion
            .rejected
            .as_ref()
            .map(|rejected| ExportMetricsPartialSuccess {
                rejected_data_points: rejected.count as i64,
                error_message: rejected.reason.clone(),
            }),
    }
}

// ── OTLP to rows ─────────────────────────────────────────────────────────────

/// Read one OTLP request into one batch, under `target` or [`metrics_schema`].
///
/// The batch holds one row per data point the request carries. A request with
/// no data point yields a batch with the schema and no rows, so a source can
/// hand it on without checking the count.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when a value cannot be read as the type a
/// declared column names, when a declared column's type has no representation
/// here, or when a request leaves a column declared `nullable=#false` without
/// a value.
pub fn export_request_to_record_batches(
    request: &ExportMetricsServiceRequest,
    target: Option<&Schema>,
) -> Result<(RecordBatch, MetricsConversion), SaciError> {
    let (rows, rejected) = rows_from_request(request);
    let schema = target.map_or_else(metrics_schema, |schema| Arc::new(schema.clone()));
    let columns = build_columns(&schema, &rows)?;
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| SaciError::generic(format!("{WHAT}: cannot build a batch: {e}")))?;
    let conversion = MetricsConversion {
        rows: rows.len(),
        rejected,
    };
    Ok((batch, conversion))
}

/// Flatten a request into rows, with what it could not carry.
fn rows_from_request(request: &ExportMetricsServiceRequest) -> (Vec<Row>, Option<RejectedPoints>) {
    let mut rows = Vec::new();
    let mut rejected = 0_u64;
    let mut reasons: Vec<String> = Vec::new();

    for resource_metrics in &request.resource_metrics {
        let resource = Arc::new(attributes_to_json(resource_metrics.resource.as_ref()));
        for scope_metrics in &resource_metrics.scope_metrics {
            let scope = scope_to_json(scope_metrics.scope.as_ref());
            for metric in &scope_metrics.metrics {
                match &metric.data {
                    None => {
                        reasons.push(format!("metric '{}' carries no data", metric_name(metric)))
                    }
                    Some(metric::Data::Gauge(gauge)) => {
                        let (gauge_rows, dropped, missing) = number_rows(
                            &gauge.data_points,
                            metric,
                            MetricKind::Gauge,
                            &scope,
                            &resource,
                        );
                        rows.extend(gauge_rows);
                        rejected += dropped;
                        if missing {
                            reasons.push(missing_number_reason(metric));
                        }
                    }
                    Some(metric::Data::Sum(sum)) => {
                        let (sum_rows, dropped, missing) = number_rows(
                            &sum.data_points,
                            metric,
                            MetricKind::Sum,
                            &scope,
                            &resource,
                        );
                        rows.extend(sum_rows);
                        rejected += dropped;
                        if missing {
                            reasons.push(missing_number_reason(metric));
                        }
                    }
                    Some(metric::Data::Histogram(histogram)) => {
                        for point in &histogram.data_points {
                            let mut attributes = point_attributes(&point.attributes, &scope);
                            attributes.insert(
                                reserved::TEMPORALITY.to_string(),
                                Json::String(
                                    temporality_name(histogram.aggregation_temporality).to_string(),
                                ),
                            );
                            attributes.insert(
                                reserved::DISTRIBUTION.to_string(),
                                histogram_distribution(point),
                            );
                            rows.push(Row {
                                name: metric_name(metric),
                                value: point.sum.unwrap_or(0.0),
                                timestamp: point.time_unix_nano,
                                kind: MetricKind::Histogram,
                                unit: empty_to_none(&metric.unit),
                                description: empty_to_none(&metric.description),
                                attributes,
                                resource_attributes: Arc::clone(&resource),
                                count: Some(point.count),
                                start_timestamp: nonzero(point.start_time_unix_nano),
                            });
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(histogram)) => {
                        for point in &histogram.data_points {
                            let mut attributes = point_attributes(&point.attributes, &scope);
                            attributes.insert(
                                reserved::TEMPORALITY.to_string(),
                                Json::String(
                                    temporality_name(histogram.aggregation_temporality).to_string(),
                                ),
                            );
                            attributes.insert(
                                reserved::DISTRIBUTION.to_string(),
                                exponential_histogram_distribution(point),
                            );
                            rows.push(Row {
                                name: metric_name(metric),
                                value: point.sum.unwrap_or(0.0),
                                timestamp: point.time_unix_nano,
                                kind: MetricKind::ExponentialHistogram,
                                unit: empty_to_none(&metric.unit),
                                description: empty_to_none(&metric.description),
                                attributes,
                                resource_attributes: Arc::clone(&resource),
                                count: Some(point.count),
                                start_timestamp: nonzero(point.start_time_unix_nano),
                            });
                        }
                    }
                    Some(metric::Data::Summary(summary)) => {
                        for point in &summary.data_points {
                            let mut attributes = point_attributes(&point.attributes, &scope);
                            attributes.insert(
                                reserved::DISTRIBUTION.to_string(),
                                summary_distribution(point),
                            );
                            rows.push(Row {
                                name: metric_name(metric),
                                value: point.sum,
                                timestamp: point.time_unix_nano,
                                kind: MetricKind::Summary,
                                unit: empty_to_none(&metric.unit),
                                description: empty_to_none(&metric.description),
                                attributes,
                                resource_attributes: Arc::clone(&resource),
                                count: Some(point.count),
                                start_timestamp: nonzero(point.start_time_unix_nano),
                            });
                        }
                    }
                }
            }
        }
    }

    // A data point with no value is one rejection each; a metric with no data
    // variant is none, because there was no data point to drop.
    let rejected = if reasons.is_empty() {
        None
    } else {
        reasons.sort();
        reasons.dedup();
        Some(RejectedPoints {
            count: rejected,
            reason: reasons.join("; "),
        })
    };
    (rows, rejected)
}

/// One row per number data point, with how many points carried no value.
///
/// The second element counts the points that carried neither an integer nor a
/// double, which is one rejection each; the third says whether any did, so the
/// caller can record the reason once per metric.
fn number_rows(
    points: &[NumberDataPoint],
    metric: &Metric,
    kind: MetricKind,
    scope: &Map<String, Json>,
    resource: &Arc<Map<String, Json>>,
) -> (Vec<Row>, u64, bool) {
    let mut rows = Vec::with_capacity(points.len());
    let mut rejected = 0;
    for point in points {
        let Some(value) = number_value(point) else {
            rejected += 1;
            continue;
        };
        rows.push(Row {
            name: metric_name(metric),
            value,
            timestamp: point.time_unix_nano,
            kind,
            unit: empty_to_none(&metric.unit),
            description: empty_to_none(&metric.description),
            attributes: point_attributes(&point.attributes, scope),
            resource_attributes: Arc::clone(resource),
            count: None,
            start_timestamp: nonzero(point.start_time_unix_nano),
        });
    }
    let missing = rejected > 0;
    (rows, rejected, missing)
}

/// The number a gauge or sum data point carries.
fn number_value(point: &NumberDataPoint) -> Option<f64> {
    match &point.value {
        Some(number_data_point::Value::AsDouble(value)) => Some(*value),
        Some(number_data_point::Value::AsInt(value)) => Some(*value as f64),
        None => None,
    }
}

/// Why a number data point was dropped, naming its metric.
fn missing_number_reason(metric: &Metric) -> String {
    format!(
        "a number data point of metric '{}' carries neither asInt nor asDouble",
        metric_name(metric)
    )
}

/// A metric's name, with the placeholder when it carries none.
fn metric_name(metric: &Metric) -> String {
    if metric.name.is_empty() {
        DEFAULT_METRIC_NAME.to_string()
    } else {
        metric.name.clone()
    }
}

/// The OTLP name of an aggregation temporality.
fn temporality_name(temporality: i32) -> &'static str {
    match AggregationTemporality::try_from(temporality) {
        Ok(AggregationTemporality::Delta) => "delta",
        Ok(AggregationTemporality::Cumulative) => "cumulative",
        Ok(AggregationTemporality::Unspecified) | Err(_) => "unspecified",
    }
}

/// The temporality a name in `otel.aggregation_temporality` stands for.
fn temporality_from_name(name: &str) -> Option<AggregationTemporality> {
    match name {
        "delta" => Some(AggregationTemporality::Delta),
        "cumulative" => Some(AggregationTemporality::Cumulative),
        "unspecified" => Some(AggregationTemporality::Unspecified),
        _ => None,
    }
}

/// An empty string is OTLP's "not set" for unit, description and scope name.
fn empty_to_none(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_string())
}

/// OTLP's fixed64 fields carry no presence, so zero is "not set".
fn nonzero(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

/// The `otel.scope` object for one instrumentation scope, empty when the scope
/// is unknown.
fn scope_to_json(scope: Option<&InstrumentationScope>) -> Map<String, Json> {
    let mut object = Map::new();
    let Some(scope) = scope else {
        return object;
    };
    if !scope.name.is_empty() {
        object.insert("name".to_string(), Json::String(scope.name.clone()));
    }
    if !scope.version.is_empty() {
        object.insert("version".to_string(), Json::String(scope.version.clone()));
    }
    let attributes = attributes_to_json_from(&scope.attributes);
    if !attributes.is_empty() {
        object.insert("attributes".to_string(), Json::Object(attributes));
    }
    object
}

/// A data point's attributes, with the reserved scope key when a scope is
/// known.
fn point_attributes(attributes: &[KeyValue], scope: &Map<String, Json>) -> Map<String, Json> {
    let mut object = attributes_to_json_from(attributes);
    if !scope.is_empty() {
        object.insert(reserved::SCOPE.to_string(), Json::Object(scope.clone()));
    }
    object
}

/// One resource's attribute list as a JSON object.
fn attributes_to_json(resource: Option<&Resource>) -> Map<String, Json> {
    resource.map_or_else(Map::new, |resource| {
        attributes_to_json_from(&resource.attributes)
    })
}

/// An OTLP attribute list as a JSON object.
fn attributes_to_json_from(attributes: &[KeyValue]) -> Map<String, Json> {
    let mut object = Map::with_capacity(attributes.len());
    for attribute in attributes {
        object.insert(attribute.key.clone(), any_value_to_json(&attribute.value));
    }
    object
}

/// One OTLP attribute value in its JSON form.
fn any_value_to_json(value: &Option<AnyValue>) -> Json {
    let inner = value.as_ref().and_then(|value| value.value.as_ref());
    match inner {
        None => Json::Null,
        Some(any_value::Value::StringValue(text)) => Json::String(text.clone()),
        Some(any_value::Value::BoolValue(flag)) => Json::Bool(*flag),
        Some(any_value::Value::IntValue(number)) => Json::Number((*number).into()),
        Some(any_value::Value::DoubleValue(number)) => f64_to_json(*number),
        Some(any_value::Value::ArrayValue(array)) => Json::Array(
            array
                .values
                .iter()
                .map(|value| any_value_to_json(&Some(value.clone())))
                .collect(),
        ),
        Some(any_value::Value::KvlistValue(list)) => {
            let mut object = Map::with_capacity(list.values.len());
            for entry in &list.values {
                object.insert(entry.key.clone(), any_value_to_json(&entry.value));
            }
            Json::Object(object)
        }
        // JSON has no byte string, so one travels as base64 under a key of its
        // own. Only a `kvlistValue` could collide with the object's shape, and
        // a conforming sender does not write that single key.
        Some(any_value::Value::BytesValue(bytes)) => {
            let mut object = Map::with_capacity(1);
            object.insert(
                reserved::BYTES.to_string(),
                Json::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
            );
            Json::Object(object)
        }
        // A string table index, which only the profiling signal defines and
        // which has no metrics-side meaning.
        Some(any_value::Value::StringValueStrindex(_)) => Json::Null,
    }
}

/// A double as JSON, with the three strings the OTLP mapping defines for the
/// values JSON cannot hold.
fn f64_to_json(value: f64) -> Json {
    match serde_json::Number::from_f64(value) {
        Some(number) => Json::Number(number),
        None if value.is_nan() => Json::String("NaN".to_string()),
        None if value.is_sign_positive() => Json::String("Infinity".to_string()),
        None => Json::String("-Infinity".to_string()),
    }
}

/// A JSON double as a number, reading the three strings a non-finite double
/// becomes on the way in.
fn json_f64(value: &Json) -> Option<f64> {
    match value {
        Json::Number(number) => number.as_f64(),
        Json::String(text) if text == "NaN" => Some(f64::NAN),
        Json::String(text) if text == "Infinity" => Some(f64::INFINITY),
        Json::String(text) if text == "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

/// The `otel.distribution` object of one explicit-bucket histogram point.
fn histogram_distribution(point: &HistogramDataPoint) -> Json {
    let mut object = Map::new();
    if !point.explicit_bounds.is_empty() {
        object.insert(
            distribution::EXPLICIT_BOUNDS.to_string(),
            Json::Array(
                point
                    .explicit_bounds
                    .iter()
                    .copied()
                    .map(f64_to_json)
                    .collect(),
            ),
        );
    }
    if !point.bucket_counts.is_empty() {
        object.insert(
            distribution::BUCKET_COUNTS.to_string(),
            counts_to_json(&point.bucket_counts),
        );
    }
    if let Some(min) = point.min {
        object.insert(distribution::MIN.to_string(), f64_to_json(min));
    }
    if let Some(max) = point.max {
        object.insert(distribution::MAX.to_string(), f64_to_json(max));
    }
    Json::Object(object)
}

/// The `otel.distribution` object of one exponential histogram point.
fn exponential_histogram_distribution(point: &ExponentialHistogramDataPoint) -> Json {
    let mut object = Map::new();
    object.insert(
        distribution::SCALE.to_string(),
        Json::Number(point.scale.into()),
    );
    object.insert(
        distribution::ZERO_COUNT.to_string(),
        Json::Number(point.zero_count.into()),
    );
    if point.zero_threshold != 0.0 {
        object.insert(
            distribution::ZERO_THRESHOLD.to_string(),
            f64_to_json(point.zero_threshold),
        );
    }
    if let Some(positive) = &point.positive {
        object.insert(
            distribution::POSITIVE.to_string(),
            buckets_to_json(positive),
        );
    }
    if let Some(negative) = &point.negative {
        object.insert(
            distribution::NEGATIVE.to_string(),
            buckets_to_json(negative),
        );
    }
    if let Some(min) = point.min {
        object.insert(distribution::MIN.to_string(), f64_to_json(min));
    }
    if let Some(max) = point.max {
        object.insert(distribution::MAX.to_string(), f64_to_json(max));
    }
    Json::Object(object)
}

/// One bucket set of an exponential histogram.
fn buckets_to_json(buckets: &exponential_histogram_data_point::Buckets) -> Json {
    let mut object = Map::new();
    object.insert(
        distribution::OFFSET.to_string(),
        Json::Number(buckets.offset.into()),
    );
    object.insert(
        distribution::COUNTS.to_string(),
        counts_to_json(&buckets.bucket_counts),
    );
    Json::Object(object)
}

/// A bucket count list as JSON.
fn counts_to_json(counts: &[u64]) -> Json {
    Json::Array(
        counts
            .iter()
            .map(|count| Json::Number((*count).into()))
            .collect(),
    )
}

/// The `otel.distribution` object of one summary point.
fn summary_distribution(point: &SummaryDataPoint) -> Json {
    let mut object = Map::new();
    if !point.quantile_values.is_empty() {
        object.insert(
            distribution::QUANTILES.to_string(),
            Json::Array(
                point
                    .quantile_values
                    .iter()
                    .map(|value| {
                        let mut entry = Map::with_capacity(2);
                        entry.insert(
                            distribution::QUANTILE.to_string(),
                            f64_to_json(value.quantile),
                        );
                        entry.insert(distribution::VALUE.to_string(), f64_to_json(value.value));
                        Json::Object(entry)
                    })
                    .collect(),
            ),
        );
    }
    Json::Object(object)
}

/// One column's value in one row, before it is read as the column's own type.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    Text(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

/// The value a canonical column holds for one row.
fn canonical_cell(row: &Row, column: &str) -> Option<Cell> {
    Some(match column {
        column::NAME => Cell::Text(row.name.clone()),
        column::VALUE => Cell::F64(row.value),
        column::TIMESTAMP => Cell::U64(row.timestamp),
        column::KIND => Cell::Text(row.kind.as_str().to_string()),
        column::UNIT => optional_text(&row.unit),
        column::DESCRIPTION => optional_text(&row.description),
        column::ATTRIBUTES => object_cell(&row.attributes),
        column::RESOURCE_ATTRIBUTES => object_cell(&row.resource_attributes),
        column::COUNT => row.count.map_or(Cell::Null, Cell::U64),
        column::START_TIMESTAMP => row.start_timestamp.map_or(Cell::Null, Cell::U64),
        _ => return None,
    })
}

/// An optional string as a cell.
fn optional_text(text: &Option<String>) -> Cell {
    match text {
        Some(text) => Cell::Text(text.clone()),
        None => Cell::Null,
    }
}

/// A JSON object as a cell, null when it holds no key.
fn object_cell(object: &Map<String, Json>) -> Cell {
    if object.is_empty() {
        Cell::Null
    } else {
        Cell::Text(Json::Object(object.clone()).to_string())
    }
}

/// The value a declared column holds for one row: the canonical value of that
/// name where one exists, and otherwise the attribute of that name.
fn declared_cell(row: &Row, column: &str) -> Cell {
    if let Some(cell) = canonical_cell(row, column) {
        return cell;
    }
    match row.attributes.get(column) {
        Some(value) => json_to_cell(value),
        None => Cell::Null,
    }
}

/// One JSON attribute value as a cell.
fn json_to_cell(value: &Json) -> Cell {
    match value {
        Json::Null => Cell::Null,
        Json::Bool(flag) => Cell::Bool(*flag),
        Json::String(text) => match non_finite_double(text) {
            Some(number) => Cell::F64(number),
            None => Cell::Text(text.clone()),
        },
        Json::Number(number) => match (number.as_i64(), number.as_u64()) {
            (Some(number), _) => Cell::I64(number),
            (None, Some(number)) => Cell::U64(number),
            (None, None) => Cell::F64(number.as_f64().unwrap_or_default()),
        },
        // An array or an object has no cell of its own. Its JSON text is what
        // a Utf8 column can hold, and every other type refuses it.
        other => Cell::Text(other.to_string()),
    }
}

/// The double one of the three non-finite strings stands for, `None` for any
/// other string.
fn non_finite_double(text: &str) -> Option<f64> {
    match text {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

/// The column a declared schema takes the metric value from: `value` when it
/// declares one, and otherwise the first declared numeric column no canonical
/// name claims.
fn value_column(schema: &Schema) -> Option<usize> {
    if let Ok(index) = schema.index_of(column::VALUE) {
        return Some(index);
    }
    schema
        .fields()
        .iter()
        .position(|field| field.data_type().is_numeric() && !claiming_canonical_name(field.name()))
}

/// Whether a canonical column other than `value` claims this name, which is
/// what keeps a `timestamp` column from becoming the metric value.
fn claiming_canonical_name(name: &str) -> bool {
    matches!(
        name,
        column::NAME
            | column::TIMESTAMP
            | column::KIND
            | column::UNIT
            | column::DESCRIPTION
            | column::ATTRIBUTES
            | column::RESOURCE_ATTRIBUTES
            | column::COUNT
            | column::START_TIMESTAMP
    )
}

/// Build every column of `schema` from `rows`.
fn build_columns(schema: &Schema, rows: &[Row]) -> Result<Vec<ArrayRef>, SaciError> {
    let value = value_column(schema);
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let cells: Vec<Cell> = rows
            .iter()
            .map(|row| {
                if Some(index) == value {
                    Cell::F64(row.value)
                } else {
                    declared_cell(row, field.name())
                }
            })
            .collect();
        columns.push(build_column(field, cells)?);
    }
    Ok(columns)
}

/// Build one column from the cells its rows produced.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when a cell cannot be read as the column's
/// type, when a non-nullable column has a row with no value, or when the
/// column's type has no representation here.
fn build_column(field: &Field, cells: Vec<Cell>) -> Result<ArrayRef, SaciError> {
    let name = field.name().as_str();
    if !field.is_nullable()
        && let Some(index) = cells.iter().position(|cell| *cell == Cell::Null)
    {
        return Err(SaciError::generic(format!(
            "{WHAT}: declared column '{name}' is not nullable, and data point {index} has no value \
             for it"
        )));
    }
    // Every conversion below reports why one cell could not be read, and this
    // names the column it belongs to.
    let fail = |reason: String| {
        SaciError::generic(format!(
            "{WHAT}: declared column '{name}' cannot be filled: {reason}"
        ))
    };

    Ok(match field.data_type() {
        DataType::Utf8 => Arc::new(StringArray::from(
            cells.into_iter().map(cell_text).collect::<Vec<_>>(),
        )),
        DataType::Boolean => Arc::new(BooleanArray::from(converted(cells, cell_bool, &fail)?)),
        DataType::Int8 => Arc::new(Int8Array::from(narrowed(cells, cell_i64, &fail, |v| {
            v as i8
        })?)),
        DataType::Int16 => Arc::new(Int16Array::from(narrowed(cells, cell_i64, &fail, |v| {
            v as i16
        })?)),
        DataType::Int32 => Arc::new(Int32Array::from(narrowed(cells, cell_i64, &fail, |v| {
            v as i32
        })?)),
        DataType::Int64 => Arc::new(Int64Array::from(converted(cells, cell_i64, &fail)?)),
        DataType::UInt8 => Arc::new(UInt8Array::from(narrowed(cells, cell_u64, &fail, |v| {
            v as u8
        })?)),
        DataType::UInt16 => Arc::new(UInt16Array::from(narrowed(cells, cell_u64, &fail, |v| {
            v as u16
        })?)),
        DataType::UInt32 => Arc::new(UInt32Array::from(narrowed(cells, cell_u64, &fail, |v| {
            v as u32
        })?)),
        DataType::UInt64 => Arc::new(UInt64Array::from(converted(cells, cell_u64, &fail)?)),
        DataType::Float32 => Arc::new(Float32Array::from(narrowed(cells, cell_f64, &fail, |v| {
            v as f32
        })?)),
        DataType::Float64 => Arc::new(Float64Array::from(converted(cells, cell_f64, &fail)?)),
        DataType::Timestamp(unit, _) => {
            let values = converted(cells, |cell| cell_timestamp(*unit, cell), &fail)?;
            match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)),
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
            }
        }
        other => {
            return Err(SaciError::generic(format!(
                "{WHAT}: declared column '{name}' has type {other}, which this connector cannot \
                 fill: declare a boolean, an integer, a float, a Utf8 or a Timestamp column"
            )));
        }
    })
}

/// Convert every cell, naming the column when one cannot be read.
fn converted<T>(
    cells: Vec<Cell>,
    convert: impl Fn(Cell) -> Result<Option<T>, String>,
    fail: &impl Fn(String) -> SaciError,
) -> Result<Vec<Option<T>>, SaciError> {
    cells
        .into_iter()
        .map(convert)
        .collect::<Result<Vec<_>, String>>()
        .map_err(fail)
}

/// Convert every cell and narrow the result to a smaller numeric type.
fn narrowed<T, S>(
    cells: Vec<Cell>,
    convert: impl Fn(Cell) -> Result<Option<S>, String>,
    fail: &impl Fn(String) -> SaciError,
    cast: impl Fn(S) -> T,
) -> Result<Vec<Option<T>>, SaciError> {
    Ok(converted(cells, convert, fail)?
        .into_iter()
        .map(|value| value.map(&cast))
        .collect())
}

/// One cell as Utf8, which every cell has a form of.
///
/// Consumes the cell, so a text cell's own string is what the column takes
/// rather than a copy of it.
fn cell_text(cell: Cell) -> Option<String> {
    match cell {
        Cell::Null => None,
        Cell::Text(text) => Some(text),
        Cell::I64(number) => Some(number.to_string()),
        Cell::U64(number) => Some(number.to_string()),
        Cell::F64(number) => Some(number.to_string()),
        Cell::Bool(flag) => Some(flag.to_string()),
    }
}

/// One cell as a boolean.
fn cell_bool(cell: Cell) -> Result<Option<bool>, String> {
    match cell {
        Cell::Null => Ok(None),
        Cell::Bool(flag) => Ok(Some(flag)),
        Cell::Text(text) => text
            .trim()
            .parse::<bool>()
            .map(Some)
            .map_err(|_| format!("'{text}' is not a boolean")),
        other => Err(format!("{other:?} is not a boolean")),
    }
}

/// One cell as an `i64`.
fn cell_i64(cell: Cell) -> Result<Option<i64>, String> {
    match cell {
        Cell::Null => Ok(None),
        Cell::I64(number) => Ok(Some(number)),
        Cell::U64(number) => i64::try_from(number)
            .map(Some)
            .map_err(|_| format!("{number} does not fit an i64")),
        Cell::F64(number) => Ok(Some(number as i64)),
        Cell::Bool(flag) => Ok(Some(i64::from(flag))),
        Cell::Text(text) => text
            .trim()
            .parse::<i64>()
            .map(Some)
            .map_err(|_| format!("'{text}' is not an integer")),
    }
}

/// One cell as a `u64`.
fn cell_u64(cell: Cell) -> Result<Option<u64>, String> {
    match cell {
        Cell::Null => Ok(None),
        Cell::U64(number) => Ok(Some(number)),
        Cell::I64(number) => u64::try_from(number)
            .map(Some)
            .map_err(|_| format!("{number} does not fit a u64")),
        Cell::F64(number) if number >= 0.0 => Ok(Some(number as u64)),
        Cell::F64(number) => Err(format!("{number} does not fit a u64")),
        Cell::Bool(flag) => Ok(Some(u64::from(flag))),
        Cell::Text(text) => text
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("'{text}' is not an unsigned integer")),
    }
}

/// One cell as an `f64`.
fn cell_f64(cell: Cell) -> Result<Option<f64>, String> {
    match cell {
        Cell::Null => Ok(None),
        Cell::F64(number) => Ok(Some(number)),
        Cell::I64(number) => Ok(Some(number as f64)),
        Cell::U64(number) => Ok(Some(number as f64)),
        Cell::Bool(flag) => Ok(Some(f64::from(u8::from(flag)))),
        Cell::Text(text) => text
            .trim()
            .parse::<f64>()
            .map(Some)
            .map_err(|_| format!("'{text}' is not a number")),
    }
}

/// One cell as a timestamp in the unit the column declares.
///
/// Every timestamp this connector writes is Unix nanoseconds, so a coarser
/// column truncates the sub-unit part rather than rounding it.
fn cell_timestamp(unit: TimeUnit, cell: Cell) -> Result<Option<i64>, String> {
    let Some(nanos) = cell_i64(cell)? else {
        return Ok(None);
    };
    let divisor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    Ok(Some(nanos / divisor))
}

// ── Rows to OTLP ─────────────────────────────────────────────────────────────

/// Read one batch into one OTLP request.
///
/// The batch's columns decide the metric. `value`, or the first numeric column
/// when the batch has none, is the data point's value; `name` or `metric_name`
/// is its name, `timestamp` or `time_unix_nano` its timestamp, `kind` or
/// `metric_type` which OTLP data variant it becomes, and `count`,
/// `start_timestamp`, `unit`, `description`, `attributes` and
/// `resource_attributes` the fields of those names. Every remaining column
/// becomes an attribute keyed by the column name, so the columns a batch
/// carries and the attributes a receiver sees are the same set.
///
/// Rows are grouped into one resource per distinct resource attributes, one
/// scope per distinct scope, and one metric per distinct name, kind, unit,
/// description, aggregation temporality and monotonic flag.
///
/// `default_metric_name` fills in for a batch whose names are empty, and
/// `default_service_name` adds the `service.name` resource attribute to every
/// resource that does not carry one.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a column the mapping reads is of
/// the wrong type, and [`SaciError::Generic`] when a `kind` names no OTLP data
/// variant, when a reserved attribute key does not hold what it names, or when
/// a double is not finite: OTLP/JSON has no number for `NaN` or `Infinity`, and
/// this connector does not write a payload one of its two encodings cannot
/// carry.
pub fn record_batch_to_export_request(
    batch: &RecordBatch,
    default_metric_name: Option<&str>,
    default_service_name: Option<&str>,
) -> Result<ExportMetricsServiceRequest, SaciError> {
    let columns = BatchColumns::read(batch)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        rows.push(SinkRow::read(batch, &columns, index)?);
    }
    Ok(rows_into_request(
        rows,
        default_metric_name,
        default_service_name,
    ))
}

/// Where each field of a batch lives.
struct BatchColumns {
    name: Option<usize>,
    timestamp: Option<usize>,
    kind: Option<usize>,
    unit: Option<usize>,
    description: Option<usize>,
    attributes: Option<usize>,
    resource_attributes: Option<usize>,
    count: Option<usize>,
    start_timestamp: Option<usize>,
    /// The column the value comes from: `value`, or the first numeric column.
    value: Option<usize>,
    /// Every column none of the above claims, which becomes an attribute.
    rest: Vec<usize>,
}

impl BatchColumns {
    /// Find every column the mapping reads, by canonical name or alias.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a column the mapping reads is
    /// not the type it must hold.
    fn read(batch: &RecordBatch) -> Result<Self, SaciError> {
        let schema = batch.schema();
        let find = |names: &[&str]| names.iter().find_map(|name| schema.index_of(name).ok());
        let text = |index: Option<usize>, what: &str| -> Result<Option<usize>, SaciError> {
            typed(&schema, index, what, DataType::Utf8)
        };
        let numeric = |index: Option<usize>, what: &str| -> Result<Option<usize>, SaciError> {
            let Some(index) = index else {
                return Ok(None);
            };
            let field = schema.field(index);
            if !field.data_type().is_numeric() {
                return Err(SaciError::configuration(format!(
                    "{SINK_WHAT}: column '{what}' must be numeric, and it is {}",
                    field.data_type()
                )));
            }
            Ok(Some(index))
        };

        let columns = Self {
            name: text(find(&[column::NAME, column::NAME_ALIAS]), column::NAME)?,
            timestamp: numeric(
                find(&[column::TIMESTAMP, column::TIMESTAMP_ALIAS]),
                column::TIMESTAMP,
            )?,
            kind: text(find(&[column::KIND, column::KIND_ALIAS]), column::KIND)?,
            unit: text(find(&[column::UNIT]), column::UNIT)?,
            description: text(find(&[column::DESCRIPTION]), column::DESCRIPTION)?,
            attributes: text(find(&[column::ATTRIBUTES]), column::ATTRIBUTES)?,
            resource_attributes: text(
                find(&[column::RESOURCE_ATTRIBUTES]),
                column::RESOURCE_ATTRIBUTES,
            )?,
            count: numeric(find(&[column::COUNT]), column::COUNT)?,
            start_timestamp: numeric(find(&[column::START_TIMESTAMP]), column::START_TIMESTAMP)?,
            value: None,
            rest: Vec::new(),
        };
        let claimed = [
            columns.name,
            columns.timestamp,
            columns.kind,
            columns.unit,
            columns.description,
            columns.attributes,
            columns.resource_attributes,
            columns.count,
            columns.start_timestamp,
        ];
        let mut rest: Vec<usize> = (0..schema.fields().len())
            .filter(|index| !claimed.contains(&Some(*index)))
            .collect();
        // A column named `value` is the value wherever it sits; otherwise the
        // first numeric column of those left is, so a batch of `id`, `label`
        // and `total` reads `id`, the same column a declared source schema
        // writes the value into.
        let mut value = None;
        if let Some(position) = rest
            .iter()
            .position(|index| schema.field(*index).name() == column::VALUE)
        {
            value = Some(rest.remove(position));
        } else if let Some(position) = rest
            .iter()
            .position(|index| schema.field(*index).data_type().is_numeric())
        {
            value = Some(rest.remove(position));
        }
        Ok(Self {
            value,
            rest,
            ..columns
        })
    }
}

/// Name a column of the wrong type, or pass it through.
fn typed(
    schema: &Schema,
    index: Option<usize>,
    what: &str,
    wanted: DataType,
) -> Result<Option<usize>, SaciError> {
    let Some(index) = index else {
        return Ok(None);
    };
    let field = schema.field(index);
    if field.data_type() != &wanted {
        return Err(SaciError::configuration(format!(
            "{SINK_WHAT}: column '{what}' must be {wanted}, and it is {}",
            field.data_type()
        )));
    }
    Ok(Some(index))
}

/// One batch row, read into the fields the OTLP messages carry.
struct SinkRow {
    name: String,
    value: SinkNumber,
    timestamp: u64,
    kind: MetricKind,
    unit: String,
    description: String,
    attributes: Vec<KeyValue>,
    resource_attributes: Vec<KeyValue>,
    count: u64,
    start_timestamp: u64,
    temporality: AggregationTemporality,
    monotonic: bool,
    distribution: Option<Distribution>,
    scope: Option<InstrumentationScope>,
}

/// A data point's value, kept as the column spelled it.
///
/// An integer column exports `asInt` and a float column `asDouble`, the two
/// OTLP has: reading an integer through an `f64` would round a value wider
/// than 53 bits, and OTLP's own integer form holds 64.
enum SinkNumber {
    Int(i64),
    Double(f64),
}

impl SinkNumber {
    /// Read one cell as a value, refusing a non-finite double.
    ///
    /// A `u64` beyond `i64::MAX` has no `asInt` form and goes as a double,
    /// which is the only loss this direction takes and needs a value past
    /// nine quintillion to reach.
    fn read(what: &str, cell: &Cell) -> Result<Self, SaciError> {
        Ok(match cell {
            Cell::I64(number) => Self::Int(*number),
            Cell::U64(number) => match i64::try_from(*number) {
                Ok(number) => Self::Int(number),
                Err(_) => Self::Double(*number as f64),
            },
            Cell::Bool(flag) => Self::Int(i64::from(*flag)),
            Cell::Null => Self::Double(1.0),
            Cell::Text(text) => match text.trim().parse::<i64>() {
                Ok(number) => Self::Int(number),
                Err(_) => Self::Double(finite(what, f64_of(cell).unwrap_or_default())?),
            },
            Cell::F64(number) => Self::Double(finite(what, *number)?),
        })
    }

    /// The value as a double, for the fields OTLP spells as one.
    fn as_f64(&self) -> f64 {
        match self {
            Self::Int(number) => *number as f64,
            Self::Double(number) => *number,
        }
    }

    /// The value in the form the OTLP messages carry.
    fn to_proto(&self) -> number_data_point::Value {
        match self {
            Self::Int(number) => number_data_point::Value::AsInt(*number),
            Self::Double(number) => number_data_point::Value::AsDouble(*number),
        }
    }
}

/// The buckets or quantiles a histogram, an exponential histogram or a summary
/// row carries.
enum Distribution {
    Histogram {
        explicit_bounds: Vec<f64>,
        bucket_counts: Vec<u64>,
        min: Option<f64>,
        max: Option<f64>,
    },
    Exponential {
        scale: i32,
        zero_count: u64,
        zero_threshold: f64,
        positive: Option<Buckets>,
        negative: Option<Buckets>,
        min: Option<f64>,
        max: Option<f64>,
    },
    Summary {
        quantiles: Vec<(f64, f64)>,
    },
}

/// One bucket set of an exponential histogram.
struct Buckets {
    offset: i32,
    counts: Vec<u64>,
}

impl SinkRow {
    /// Read row `index` of `batch`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] for a kind no OTLP data variant names,
    /// for a reserved attribute key that does not hold what it names, or for a
    /// double that is not finite.
    fn read(batch: &RecordBatch, columns: &BatchColumns, index: usize) -> Result<Self, SaciError> {
        let text = |slot: Option<usize>| slot.and_then(|slot| string_at(batch, slot, index));
        let cell = |slot: Option<usize>| slot.and_then(|slot| cell_at(batch, slot, index));
        let unsigned = |slot: Option<usize>| cell(slot).as_ref().and_then(u64_of);
        let where_ = |what: &str| format!("row {index}'s {what}");

        let kind = match text(columns.kind) {
            Some(kind) => MetricKind::parse(&kind).ok_or_else(|| {
                SaciError::generic(format!(
                    "{SINK_WHAT}: row {index} has kind '{kind}', and the kinds are {}",
                    KINDS.join(", ")
                ))
            })?,
            None => MetricKind::Gauge,
        };
        // A batch with no value column exports every row with a value of one,
        // which keeps a metric whose point is its presence rather than its
        // number valid OTLP.
        let value = match cell(columns.value) {
            None => SinkNumber::Double(1.0),
            Some(cell) => SinkNumber::read(&where_("value"), &cell)?,
        };

        let mut attributes = match text(columns.attributes) {
            Some(json) => parse_object(SINK_WHAT, &where_("attributes"), &json)?,
            None => Map::new(),
        };
        for slot in &columns.rest {
            let column = batch.schema().field(*slot).name().clone();
            if let Some(cell) = cell_at(batch, *slot, index) {
                attributes.insert(column, cell_to_json(&cell));
            }
        }
        let resource_attributes = match text(columns.resource_attributes) {
            Some(json) => parse_object(SINK_WHAT, &where_("resource_attributes"), &json)?,
            None => Map::new(),
        };

        let temporality = match attributes.remove(reserved::TEMPORALITY) {
            Some(Json::String(name)) => temporality_from_name(&name).ok_or_else(|| {
                SaciError::generic(format!(
                    "{SINK_WHAT}: row {index}'s '{}' is '{name}', and the values are cumulative, \
                     delta and unspecified",
                    reserved::TEMPORALITY
                ))
            })?,
            Some(other) => {
                return Err(reserved_type(
                    index,
                    reserved::TEMPORALITY,
                    "a string",
                    &other,
                ));
            }
            None => AggregationTemporality::Cumulative,
        };
        let monotonic = match attributes.remove(reserved::MONOTONIC) {
            Some(Json::Bool(flag)) => flag,
            Some(other) => {
                return Err(reserved_type(
                    index,
                    reserved::MONOTONIC,
                    "a boolean",
                    &other,
                ));
            }
            None => false,
        };
        // A gauge and a sum carry no buckets, so a distribution key on one is
        // an ordinary attribute rather than a field to read.
        let distribution = match kind {
            MetricKind::Histogram | MetricKind::ExponentialHistogram | MetricKind::Summary => {
                match attributes.remove(reserved::DISTRIBUTION) {
                    Some(Json::Null) | None => None,
                    Some(value) => Some(Distribution::read(kind, index, &value)?),
                }
            }
            _ => None,
        };
        let scope = match attributes.remove(reserved::SCOPE) {
            Some(Json::Object(object)) => Some(scope_from_json(index, object)?),
            Some(Json::Null) | None => None,
            Some(other) => {
                return Err(reserved_type(index, reserved::SCOPE, "an object", &other));
            }
        };

        let attributes = json_into_attributes(&attributes);
        let resource_attributes = json_into_attributes(&resource_attributes);
        finite_attributes(&where_("attributes"), &attributes)?;
        finite_attributes(&where_("resource_attributes"), &resource_attributes)?;

        Ok(Self {
            name: text(columns.name).unwrap_or_default(),
            value,
            timestamp: unsigned(columns.timestamp).unwrap_or_else(now_unix_nanos),
            kind,
            unit: text(columns.unit).unwrap_or_default(),
            description: text(columns.description).unwrap_or_default(),
            attributes,
            resource_attributes,
            count: unsigned(columns.count).unwrap_or(0),
            start_timestamp: unsigned(columns.start_timestamp).unwrap_or(0),
            temporality,
            monotonic,
            distribution,
            scope,
        })
    }

    /// This row's metric name, with the fallback for an empty one.
    fn metric_name(&self, default: Option<&str>) -> String {
        if self.name.is_empty() {
            default.unwrap_or(DEFAULT_METRIC_NAME).to_string()
        } else {
            self.name.clone()
        }
    }

    /// This row as the data point of its kind's variant.
    ///
    /// Consumes the row, so its attributes and its distribution move into the
    /// message rather than being copied out of it.
    fn data_point(self) -> DataPoint {
        let Self {
            kind,
            value,
            timestamp,
            start_timestamp,
            count,
            attributes,
            distribution,
            ..
        } = self;
        match kind {
            MetricKind::Gauge | MetricKind::Sum => DataPoint::Number(NumberDataPoint {
                attributes,
                start_time_unix_nano: start_timestamp,
                time_unix_nano: timestamp,
                exemplars: Vec::new(),
                flags: 0,
                value: Some(value.to_proto()),
            }),
            MetricKind::Histogram => {
                let mut point = HistogramDataPoint {
                    attributes,
                    start_time_unix_nano: start_timestamp,
                    time_unix_nano: timestamp,
                    count,
                    sum: Some(value.as_f64()),
                    bucket_counts: Vec::new(),
                    explicit_bounds: Vec::new(),
                    exemplars: Vec::new(),
                    flags: 0,
                    min: None,
                    max: None,
                };
                if let Some(Distribution::Histogram {
                    explicit_bounds,
                    bucket_counts,
                    min,
                    max,
                }) = distribution
                {
                    point.explicit_bounds = explicit_bounds;
                    point.bucket_counts = bucket_counts;
                    point.min = min;
                    point.max = max;
                }
                DataPoint::Histogram(point)
            }
            MetricKind::ExponentialHistogram => {
                let mut point = ExponentialHistogramDataPoint {
                    attributes,
                    start_time_unix_nano: start_timestamp,
                    time_unix_nano: timestamp,
                    count,
                    sum: Some(value.as_f64()),
                    scale: 0,
                    zero_count: 0,
                    positive: None,
                    negative: None,
                    flags: 0,
                    exemplars: Vec::new(),
                    min: None,
                    max: None,
                    zero_threshold: 0.0,
                };
                if let Some(Distribution::Exponential {
                    scale,
                    zero_count,
                    zero_threshold,
                    positive,
                    negative,
                    min,
                    max,
                }) = distribution
                {
                    point.scale = scale;
                    point.zero_count = zero_count;
                    point.zero_threshold = zero_threshold;
                    point.positive = positive.map(|buckets| buckets_into_proto(&buckets));
                    point.negative = negative.map(|buckets| buckets_into_proto(&buckets));
                    point.min = min;
                    point.max = max;
                }
                DataPoint::Exponential(point)
            }
            MetricKind::Summary => {
                let quantiles = match distribution {
                    Some(Distribution::Summary { quantiles }) => quantiles
                        .into_iter()
                        .map(|(quantile, value)| summary_data_point::ValueAtQuantile {
                            quantile,
                            value,
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                DataPoint::Summary(SummaryDataPoint {
                    attributes,
                    start_time_unix_nano: start_timestamp,
                    time_unix_nano: timestamp,
                    count,
                    sum: value.as_f64(),
                    quantile_values: quantiles,
                    flags: 0,
                })
            }
        }
    }
}

impl Distribution {
    /// Read the `otel.distribution` object of a row of `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the object is not an object, when
    /// it holds a key this kind has no field for, or when one of its values is
    /// of the wrong type or not finite.
    fn read(kind: MetricKind, index: usize, value: &Json) -> Result<Self, SaciError> {
        let where_ = format!("row {index}'s '{}'", reserved::DISTRIBUTION);
        let Json::Object(object) = value else {
            return Err(SaciError::generic(format!(
                "{SINK_WHAT}: {where_} must be an object, and it is {value}"
            )));
        };
        let allowed: &[&str] = match kind {
            MetricKind::Histogram => &[
                distribution::EXPLICIT_BOUNDS,
                distribution::BUCKET_COUNTS,
                distribution::MIN,
                distribution::MAX,
            ],
            MetricKind::ExponentialHistogram => &[
                distribution::SCALE,
                distribution::ZERO_COUNT,
                distribution::ZERO_THRESHOLD,
                distribution::POSITIVE,
                distribution::NEGATIVE,
                distribution::MIN,
                distribution::MAX,
            ],
            _ => &[distribution::QUANTILES],
        };
        for key in object.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(SaciError::generic(format!(
                    "{SINK_WHAT}: {where_} of a '{}' row has no key '{key}', and its keys are {}",
                    kind.as_str(),
                    allowed.join(", ")
                )));
            }
        }

        Ok(match kind {
            MetricKind::Histogram => Self::Histogram {
                explicit_bounds: f64_list(&where_, object, distribution::EXPLICIT_BOUNDS)?,
                bucket_counts: u64_list(&where_, object, distribution::BUCKET_COUNTS)?,
                min: optional_f64(&where_, object, distribution::MIN)?,
                max: optional_f64(&where_, object, distribution::MAX)?,
            },
            MetricKind::ExponentialHistogram => Self::Exponential {
                scale: optional_i32(&where_, object, distribution::SCALE)?.unwrap_or(0),
                zero_count: optional_u64(&where_, object, distribution::ZERO_COUNT)?.unwrap_or(0),
                zero_threshold: optional_f64(&where_, object, distribution::ZERO_THRESHOLD)?
                    .unwrap_or(0.0),
                positive: optional_buckets(&where_, object, distribution::POSITIVE)?,
                negative: optional_buckets(&where_, object, distribution::NEGATIVE)?,
                min: optional_f64(&where_, object, distribution::MIN)?,
                max: optional_f64(&where_, object, distribution::MAX)?,
            },
            _ => Self::Summary {
                quantiles: quantile_list(&where_, object)?,
            },
        })
    }
}

/// One bucket set from its `{ "offset", "counts" }` object.
fn optional_buckets(
    what: &str,
    object: &Map<String, Json>,
    key: &str,
) -> Result<Option<Buckets>, SaciError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Json::Object(buckets) = value else {
        return Err(SaciError::generic(format!(
            "{SINK_WHAT}: {what}.{key} must be an object, and it is {value}"
        )));
    };
    for inner in buckets.keys() {
        if inner != distribution::OFFSET && inner != distribution::COUNTS {
            return Err(SaciError::generic(format!(
                "{SINK_WHAT}: {what}.{key} has no key '{inner}', and its keys are {} and {}",
                distribution::OFFSET,
                distribution::COUNTS
            )));
        }
    }
    Ok(Some(Buckets {
        offset: optional_i32(what, buckets, distribution::OFFSET)?.unwrap_or(0),
        counts: u64_list(what, buckets, distribution::COUNTS)?,
    }))
}

/// One bucket set in its OTLP form.
fn buckets_into_proto(buckets: &Buckets) -> exponential_histogram_data_point::Buckets {
    exponential_histogram_data_point::Buckets {
        offset: buckets.offset,
        bucket_counts: buckets.counts.clone(),
    }
}

/// An optional number under `key`.
fn optional_f64(
    what: &str,
    object: &Map<String, Json>,
    key: &str,
) -> Result<Option<f64>, SaciError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    match json_f64(value) {
        Some(number) => Ok(Some(finite(&format!("{what}.{key}"), number)?)),
        None => Err(SaciError::generic(format!(
            "{SINK_WHAT}: {what}.{key} must be a number, and it is {value}"
        ))),
    }
}

/// An optional unsigned integer under `key`.
fn optional_u64(
    what: &str,
    object: &Map<String, Json>,
    key: &str,
) -> Result<Option<u64>, SaciError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    value.as_u64().map(Some).ok_or_else(|| {
        SaciError::generic(format!(
            "{SINK_WHAT}: {what}.{key} must be an unsigned integer, and it is {value}"
        ))
    })
}

/// An optional signed integer under `key`.
fn optional_i32(
    what: &str,
    object: &Map<String, Json>,
    key: &str,
) -> Result<Option<i32>, SaciError> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    value
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .map(Some)
        .ok_or_else(|| {
            SaciError::generic(format!(
                "{SINK_WHAT}: {what}.{key} must be an integer, and it is {value}"
            ))
        })
}

/// The numbers under `key`, empty when the key is absent.
fn f64_list(what: &str, object: &Map<String, Json>, key: &str) -> Result<Vec<f64>, SaciError> {
    let Some(value) = object.get(key) else {
        return Ok(Vec::new());
    };
    let Json::Array(values) = value else {
        return Err(SaciError::generic(format!(
            "{SINK_WHAT}: {what}.{key} must be an array, and it is {value}"
        )));
    };
    let mut numbers = Vec::with_capacity(values.len());
    for value in values {
        match json_f64(value) {
            Some(number) => numbers.push(finite(&format!("{what}.{key}"), number)?),
            None => {
                return Err(SaciError::generic(format!(
                    "{SINK_WHAT}: {what}.{key} must hold numbers, and it holds {value}"
                )));
            }
        }
    }
    Ok(numbers)
}

/// The unsigned integers under `key`, empty when the key is absent.
fn u64_list(what: &str, object: &Map<String, Json>, key: &str) -> Result<Vec<u64>, SaciError> {
    let Some(value) = object.get(key) else {
        return Ok(Vec::new());
    };
    let Json::Array(values) = value else {
        return Err(SaciError::generic(format!(
            "{SINK_WHAT}: {what}.{key} must be an array, and it is {value}"
        )));
    };
    let mut numbers = Vec::with_capacity(values.len());
    for value in values {
        numbers.push(value.as_u64().ok_or_else(|| {
            SaciError::generic(format!(
                "{SINK_WHAT}: {what}.{key} must hold unsigned integers, and it holds {value}"
            ))
        })?);
    }
    Ok(numbers)
}

/// The `{ "quantile", "value" }` objects under
/// [`distribution::QUANTILES`].
fn quantile_list(what: &str, object: &Map<String, Json>) -> Result<Vec<(f64, f64)>, SaciError> {
    let Some(value) = object.get(distribution::QUANTILES) else {
        return Ok(Vec::new());
    };
    let Json::Array(values) = value else {
        return Err(SaciError::generic(format!(
            "{SINK_WHAT}: {what}.{} must be an array, and it is {value}",
            distribution::QUANTILES
        )));
    };
    let mut quantiles = Vec::with_capacity(values.len());
    for value in values {
        let Json::Object(entry) = value else {
            return Err(SaciError::generic(format!(
                "{SINK_WHAT}: {what}.{} must hold objects, and it holds {value}",
                distribution::QUANTILES
            )));
        };
        let quantile = optional_f64(what, entry, distribution::QUANTILE)?.ok_or_else(|| {
            SaciError::generic(format!(
                "{SINK_WHAT}: {what}.{} holds an object with no '{}' key",
                distribution::QUANTILES,
                distribution::QUANTILE
            ))
        })?;
        let number = optional_f64(what, entry, distribution::VALUE)?.ok_or_else(|| {
            SaciError::generic(format!(
                "{SINK_WHAT}: {what}.{} holds an object with no '{}' key",
                distribution::QUANTILES,
                distribution::VALUE
            ))
        })?;
        quantiles.push((quantile, number));
    }
    Ok(quantiles)
}

/// Refuse a double JSON has no number for.
fn finite(what: &str, value: f64) -> Result<f64, SaciError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(SaciError::generic(format!(
            "{SINK_WHAT}: {what} is {value}, and OTLP/JSON has no number for NaN or Infinity"
        )))
    }
}

/// Refuse a non-finite double anywhere in an attribute list.
fn finite_attributes(what: &str, attributes: &[KeyValue]) -> Result<(), SaciError> {
    for attribute in attributes {
        let value = attribute
            .value
            .as_ref()
            .and_then(|value| value.value.as_ref());
        match value {
            Some(any_value::Value::DoubleValue(number)) => {
                finite(&format!("{what}.{}", attribute.key), *number)?;
            }
            Some(any_value::Value::ArrayValue(array)) => {
                for value in &array.values {
                    finite_value(&format!("{what}.{}", attribute.key), value)?;
                }
            }
            Some(any_value::Value::KvlistValue(list)) => {
                finite_attributes(what, &list.values)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Refuse a non-finite double inside one array element.
fn finite_value(what: &str, value: &AnyValue) -> Result<(), SaciError> {
    match value.value.as_ref() {
        Some(any_value::Value::DoubleValue(number)) => finite(what, *number).map(|_| ()),
        Some(any_value::Value::ArrayValue(array)) => array
            .values
            .iter()
            .try_for_each(|value| finite_value(what, value)),
        Some(any_value::Value::KvlistValue(list)) => finite_attributes(what, &list.values),
        _ => Ok(()),
    }
}

/// The refusal a reserved key holding the wrong type gets.
fn reserved_type(index: usize, key: &str, expected: &str, value: &Json) -> SaciError {
    SaciError::generic(format!(
        "{SINK_WHAT}: row {index}'s '{key}' is {value}, and it must be {expected}"
    ))
}

/// Parse an `attributes` or `resource_attributes` column into a JSON object.
fn parse_object(what: &str, where_: &str, json: &str) -> Result<Map<String, Json>, SaciError> {
    match serde_json::from_str::<Json>(json) {
        Ok(Json::Object(object)) => Ok(object),
        Ok(other) => Err(SaciError::generic(format!(
            "{what}: {where_} must be a JSON object, and it is {other}"
        ))),
        Err(e) => Err(SaciError::generic(format!(
            "{what}: {where_} is not JSON: {e}"
        ))),
    }
}

/// One instrumentation scope from its `otel.scope` object.
fn scope_from_json(
    index: usize,
    object: Map<String, Json>,
) -> Result<InstrumentationScope, SaciError> {
    let where_ = format!("row {index}'s '{}'", reserved::SCOPE);
    for key in object.keys() {
        if !matches!(key.as_str(), "name" | "version" | "attributes") {
            return Err(SaciError::generic(format!(
                "{SINK_WHAT}: {where_} has no key '{key}', and its keys are name, version and \
                 attributes"
            )));
        }
    }
    let text = |key: &str| -> Result<String, SaciError> {
        match object.get(key) {
            None | Some(Json::Null) => Ok(String::new()),
            Some(Json::String(text)) => Ok(text.clone()),
            Some(other) => Err(SaciError::generic(format!(
                "{SINK_WHAT}: {where_}.{key} must be a string, and it is {other}"
            ))),
        }
    };
    let attributes = match object.get("attributes") {
        None | Some(Json::Null) => Map::new(),
        Some(Json::Object(attributes)) => attributes.clone(),
        Some(other) => {
            return Err(SaciError::generic(format!(
                "{SINK_WHAT}: {where_}.attributes must be an object, and it is {other}"
            )));
        }
    };
    Ok(InstrumentationScope {
        name: text("name")?,
        version: text("version")?,
        attributes: json_into_attributes(&attributes),
        dropped_attributes_count: 0,
    })
}

/// A JSON object as an OTLP attribute list.
fn json_into_attributes(object: &Map<String, Json>) -> Vec<KeyValue> {
    object
        .iter()
        .map(|(key, value)| KeyValue {
            key: key.clone(),
            value: Some(json_to_any_value(value)),
            key_strindex: 0,
        })
        .collect()
}

/// One JSON value as an OTLP attribute value.
fn json_to_any_value(value: &Json) -> AnyValue {
    let option = match value {
        Json::Null => None,
        Json::Bool(flag) => Some(any_value::Value::BoolValue(*flag)),
        Json::String(text) => match non_finite_double(text) {
            Some(number) => Some(any_value::Value::DoubleValue(number)),
            None => Some(any_value::Value::StringValue(text.clone())),
        },
        Json::Number(number) => match (number.as_i64(), number.as_u64()) {
            (Some(number), _) => Some(any_value::Value::IntValue(number)),
            (None, Some(number)) => Some(any_value::Value::IntValue(number as i64)),
            (None, None) => Some(any_value::Value::DoubleValue(
                number.as_f64().unwrap_or_default(),
            )),
        },
        Json::Array(values) => Some(any_value::Value::ArrayValue(
            opentelemetry_proto::tonic::common::v1::ArrayValue {
                values: values.iter().map(json_to_any_value).collect(),
            },
        )),
        Json::Object(object) => match bytes_from_json(object) {
            Some(bytes) => Some(any_value::Value::BytesValue(bytes)),
            None => Some(any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: json_into_attributes(object),
                },
            )),
        },
    };
    AnyValue { value: option }
}

/// The bytes a `{"otel.bytes": "<base64>"}` object carries, `None` for any
/// other object.
fn bytes_from_json(object: &Map<String, Json>) -> Option<Vec<u8>> {
    if object.len() != 1 {
        return None;
    }
    let text = object.get(reserved::BYTES)?.as_str()?;
    base64::engine::general_purpose::STANDARD.decode(text).ok()
}

/// One cell as the JSON an attribute value takes.
fn cell_to_json(cell: &Cell) -> Json {
    match cell {
        Cell::Null => Json::Null,
        Cell::Text(text) => Json::String(text.clone()),
        Cell::I64(number) => Json::Number((*number).into()),
        Cell::U64(number) => Json::Number((*number).into()),
        Cell::F64(number) => f64_to_json(*number),
        Cell::Bool(flag) => Json::Bool(*flag),
    }
}

/// One cell of a batch, read through the column's own type.
///
/// Every integer is read as the array's own width rather than through an
/// `f64`, which is what keeps a nanosecond timestamp and a 64-bit count exact.
/// A column type the mapping has no form for contributes no cell, and so no
/// attribute, rather than one of the wrong type.
fn cell_at(batch: &RecordBatch, column: usize, row: usize) -> Option<Cell> {
    if batch.column(column).is_null(row) {
        return Some(Cell::Null);
    }
    let data_type = batch.schema().field(column).data_type().clone();
    match data_type {
        DataType::Utf8 => string_at(batch, column, row).map(Cell::Text),
        DataType::Boolean => bool_at(batch, column, row).map(Cell::Bool),
        DataType::Int8 => {
            downcast::<Int8Array>(batch, column).map(|a| Cell::I64(a.value(row) as i64))
        }
        DataType::Int16 => {
            downcast::<Int16Array>(batch, column).map(|a| Cell::I64(a.value(row) as i64))
        }
        DataType::Int32 => {
            downcast::<Int32Array>(batch, column).map(|a| Cell::I64(a.value(row) as i64))
        }
        DataType::Int64 => downcast::<Int64Array>(batch, column).map(|a| Cell::I64(a.value(row))),
        DataType::UInt8 => {
            downcast::<UInt8Array>(batch, column).map(|a| Cell::U64(a.value(row) as u64))
        }
        DataType::UInt16 => {
            downcast::<UInt16Array>(batch, column).map(|a| Cell::U64(a.value(row) as u64))
        }
        DataType::UInt32 => {
            downcast::<UInt32Array>(batch, column).map(|a| Cell::U64(a.value(row) as u64))
        }
        DataType::UInt64 => downcast::<UInt64Array>(batch, column).map(|a| Cell::U64(a.value(row))),
        DataType::Float32 => {
            downcast::<Float32Array>(batch, column).map(|a| Cell::F64(a.value(row) as f64))
        }
        DataType::Float64 => {
            downcast::<Float64Array>(batch, column).map(|a| Cell::F64(a.value(row)))
        }
        DataType::Timestamp(unit, _) => timestamp_at(batch, column, row, unit),
        _ => None,
    }
}

/// One timestamp cell, in Unix nanoseconds whatever the column's unit is.
fn timestamp_at(batch: &RecordBatch, column: usize, row: usize, unit: TimeUnit) -> Option<Cell> {
    let value = match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(batch, column).map(|a| a.value(row)),
        TimeUnit::Millisecond => {
            downcast::<TimestampMillisecondArray>(batch, column).map(|a| a.value(row))
        }
        TimeUnit::Microsecond => {
            downcast::<TimestampMicrosecondArray>(batch, column).map(|a| a.value(row))
        }
        TimeUnit::Nanosecond => {
            downcast::<TimestampNanosecondArray>(batch, column).map(|a| a.value(row))
        }
    }?;
    let factor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    Some(Cell::U64(value.saturating_mul(factor).max(0) as u64))
}

/// The unsigned integer of one cell, read exactly where the cell is one.
fn u64_of(cell: &Cell) -> Option<u64> {
    match cell {
        Cell::U64(number) => Some(*number),
        Cell::I64(number) => u64::try_from(*number).ok(),
        Cell::F64(number) if *number >= 0.0 => Some(*number as u64),
        Cell::Bool(flag) => Some(u64::from(*flag)),
        Cell::Text(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    }
}

/// The double of one cell.
fn f64_of(cell: &Cell) -> Option<f64> {
    match cell {
        Cell::F64(number) => Some(*number),
        Cell::I64(number) => Some(*number as f64),
        Cell::U64(number) => Some(*number as f64),
        Cell::Bool(flag) => Some(f64::from(u8::from(*flag))),
        Cell::Text(text) => text.trim().parse::<f64>().ok(),
        Cell::Null => None,
    }
}

/// The string of one non-null Utf8 cell.
fn string_at(batch: &RecordBatch, column: usize, row: usize) -> Option<String> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .and_then(|array| (!array.is_null(row)).then(|| array.value(row).to_string()))
}

/// The boolean of one non-null Boolean cell.
fn bool_at(batch: &RecordBatch, column: usize, row: usize) -> Option<bool> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .and_then(|array| (!array.is_null(row)).then(|| array.value(row)))
}

/// Downcast one column to its own array type.
fn downcast<T: 'static>(batch: &RecordBatch, column: usize) -> Option<&T> {
    batch.column(column).as_any().downcast_ref::<T>()
}

/// Flatten every row into the request's resource, scope and metric tree.
fn rows_into_request(
    rows: Vec<SinkRow>,
    default_metric_name: Option<&str>,
    default_service_name: Option<&str>,
) -> ExportMetricsServiceRequest {
    let mut resources: Vec<FlatResource> = Vec::new();
    for row in rows {
        let resource_key = attributes_key(&row.resource_attributes);
        let resource = match resources.iter().position(|r| r.key == resource_key) {
            Some(index) => &mut resources[index],
            None => {
                let mut attributes = row.resource_attributes.clone();
                if let Some(service) = default_service_name
                    && !attributes.iter().any(|a| a.key == "service.name")
                {
                    attributes.push(KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(service.to_string())),
                        }),
                        key_strindex: 0,
                    });
                }
                resources.push(FlatResource {
                    key: resource_key,
                    attributes,
                    scopes: Vec::new(),
                });
                let last = resources.len() - 1;
                &mut resources[last]
            }
        };

        let scope_key = scope_key(&row.scope);
        let scope = match resource.scopes.iter().position(|s| s.key == scope_key) {
            Some(index) => &mut resource.scopes[index],
            None => {
                resource.scopes.push(FlatScope {
                    key: scope_key,
                    scope: row.scope.clone(),
                    metrics: Vec::new(),
                });
                let last = resource.scopes.len() - 1;
                &mut resource.scopes[last]
            }
        };

        let name = row.metric_name(default_metric_name);
        let metric_key = format!(
            "{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}",
            name,
            row.kind.as_str(),
            row.unit,
            row.description,
            row.temporality as i32,
            row.monotonic
        );
        let metric = match scope.metrics.iter().position(|m| m.key == metric_key) {
            Some(index) => &mut scope.metrics[index],
            None => {
                scope.metrics.push(FlatMetric {
                    key: metric_key,
                    name,
                    unit: row.unit.clone(),
                    description: row.description.clone(),
                    kind: row.kind,
                    temporality: row.temporality,
                    monotonic: row.monotonic,
                    points: Vec::new(),
                });
                let last = scope.metrics.len() - 1;
                &mut scope.metrics[last]
            }
        };
        metric.points.push(row.data_point());
    }

    ExportMetricsServiceRequest {
        resource_metrics: resources
            .into_iter()
            .map(|resource| ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource.attributes,
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: resource
                    .scopes
                    .into_iter()
                    .map(|scope| ScopeMetrics {
                        scope: scope.scope,
                        metrics: scope
                            .metrics
                            .into_iter()
                            .map(|metric| {
                                let FlatMetric {
                                    name,
                                    unit,
                                    description,
                                    kind,
                                    temporality,
                                    monotonic,
                                    points,
                                    ..
                                } = metric;
                                Metric {
                                    name,
                                    description,
                                    unit,
                                    metadata: Vec::new(),
                                    data: Some(metric_data(kind, temporality, monotonic, points)),
                                }
                            })
                            .collect(),
                        schema_url: String::new(),
                    })
                    .collect(),
                schema_url: String::new(),
            })
            .collect(),
    }
}

/// A stable key for an attribute list, which is what groups resources.
fn attributes_key(attributes: &[KeyValue]) -> String {
    let mut parts: Vec<String> = attributes
        .iter()
        .map(|attribute| format!("{}\u{2}{:?}", attribute.key, attribute.value))
        .collect();
    parts.sort();
    parts.join("\u{1}")
}

/// A stable key for a scope, which is what groups scope metrics.
fn scope_key(scope: &Option<InstrumentationScope>) -> String {
    match scope {
        None => String::new(),
        Some(scope) => format!(
            "{}\u{2}{}\u{2}{:?}",
            scope.name, scope.version, scope.attributes
        ),
    }
}

/// One resource being assembled.
struct FlatResource {
    key: String,
    attributes: Vec<KeyValue>,
    scopes: Vec<FlatScope>,
}

/// One scope being assembled.
struct FlatScope {
    key: String,
    scope: Option<InstrumentationScope>,
    metrics: Vec<FlatMetric>,
}

/// One metric being assembled, with every data point of its kind.
struct FlatMetric {
    key: String,
    name: String,
    unit: String,
    description: String,
    kind: MetricKind,
    temporality: AggregationTemporality,
    monotonic: bool,
    points: Vec<DataPoint>,
}

/// The data variant of one metric, holding every point its kind collected.
fn metric_data(
    kind: MetricKind,
    temporality: AggregationTemporality,
    monotonic: bool,
    points: Vec<DataPoint>,
) -> metric::Data {
    let points = points.into_iter();
    match kind {
        MetricKind::Gauge => metric::Data::Gauge(Gauge {
            data_points: numbers(points),
        }),
        MetricKind::Sum => metric::Data::Sum(Sum {
            data_points: numbers(points),
            aggregation_temporality: temporality as i32,
            is_monotonic: monotonic,
        }),
        MetricKind::Histogram => metric::Data::Histogram(Histogram {
            data_points: histograms(points),
            aggregation_temporality: temporality as i32,
        }),
        MetricKind::ExponentialHistogram => {
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: exponential_histograms(points),
                aggregation_temporality: temporality as i32,
            })
        }
        MetricKind::Summary => metric::Data::Summary(Summary {
            data_points: summaries(points),
        }),
    }
}

/// One data point, before it reaches its metric's variant.
enum DataPoint {
    Number(NumberDataPoint),
    Histogram(HistogramDataPoint),
    Exponential(ExponentialHistogramDataPoint),
    Summary(SummaryDataPoint),
}

/// Take the number points out of a metric's points.
fn numbers(points: impl Iterator<Item = DataPoint>) -> Vec<NumberDataPoint> {
    points
        .filter_map(|point| match point {
            DataPoint::Number(point) => Some(point),
            _ => None,
        })
        .collect()
}

/// Take the histogram points out of a metric's points.
fn histograms(points: impl Iterator<Item = DataPoint>) -> Vec<HistogramDataPoint> {
    points
        .filter_map(|point| match point {
            DataPoint::Histogram(point) => Some(point),
            _ => None,
        })
        .collect()
}

/// Take the exponential histogram points out of a metric's points.
fn exponential_histograms(
    points: impl Iterator<Item = DataPoint>,
) -> Vec<ExponentialHistogramDataPoint> {
    points
        .filter_map(|point| match point {
            DataPoint::Exponential(point) => Some(point),
            _ => None,
        })
        .collect()
}

/// Take the summary points out of a metric's points.
fn summaries(points: impl Iterator<Item = DataPoint>) -> Vec<SummaryDataPoint> {
    points
        .filter_map(|point| match point {
            DataPoint::Summary(point) => Some(point),
            _ => None,
        })
        .collect()
}

// ── The wire encodings ───────────────────────────────────────────────────────

/// The `content-type` one encoding's body carries.
pub fn media_type(encoding: Encoding) -> &'static str {
    match encoding {
        Encoding::Protobuf => CONTENT_TYPE_PROTOBUF,
        Encoding::Json => CONTENT_TYPE_JSON,
    }
}

/// Which encoding a `content-type` names, `None` for a media type this
/// connector does not read.
///
/// The media type is read case-insensitively and its parameters are dropped,
/// so `application/x-protobuf; charset=utf-8` answers protobuf. An OTLP
/// exporter writes one of the two names, and an empty `content-type` is
/// protobuf, which is what the standard makes the default body.
pub fn encoding_for_media_type(content_type: &str) -> Option<Encoding> {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match media_type.as_str() {
        "" | "application/x-protobuf" | "application/protobuf" => Some(Encoding::Protobuf),
        "application/json" => Some(Encoding::Json),
        _ => None,
    }
}

/// Encode one request.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when a double the request holds is not
/// finite, which only the JSON encoding refuses: `serde_json` has no number
/// for `NaN` or `Infinity`.
pub fn encode(
    encoding: Encoding,
    request: &ExportMetricsServiceRequest,
) -> Result<Vec<u8>, SaciError> {
    match encoding {
        Encoding::Protobuf => Ok(request.encode_to_vec()),
        Encoding::Json => serde_json::to_vec(request).map_err(|e| {
            SaciError::generic(format!(
                "{SINK_WHAT}: cannot encode the request as JSON: {e}"
            ))
        }),
    }
}

/// Decode one request body.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] naming `what` when the body is not a request
/// in this encoding.
pub fn decode(
    encoding: Encoding,
    what: &str,
    body: &[u8],
) -> Result<ExportMetricsServiceRequest, SaciError> {
    match encoding {
        Encoding::Protobuf => ExportMetricsServiceRequest::decode(body).map_err(|e| {
            SaciError::generic(format!("{what}: the body is not an OTLP request: {e}"))
        }),
        Encoding::Json => {
            // A body is read through the value tree first, because the OTLP
            // JSON mapping writes every 64-bit integer as a decimal string
            // while the derived deserializer reads a number. An enum is a
            // number in both, so nothing else needs a rewrite, and a body that
            // already wrote numbers decodes unchanged.
            let mut value: Json = serde_json::from_slice(body).map_err(|e| {
                SaciError::generic(format!("{what}: the body is not an OTLP request: {e}"))
            })?;
            normalize_int64_strings(&mut value);
            serde_json::from_value(value).map_err(|e| {
                SaciError::generic(format!("{what}: the body is not an OTLP request: {e}"))
            })
        }
    }
}

/// Encode one response.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the response cannot be written, which
/// no response this connector builds can be.
pub fn encode_response(
    encoding: Encoding,
    response: &ExportMetricsServiceResponse,
) -> Result<Vec<u8>, SaciError> {
    match encoding {
        Encoding::Protobuf => Ok(response.encode_to_vec()),
        Encoding::Json => serde_json::to_vec(response).map_err(|e| {
            SaciError::generic(format!(
                "{SINK_WHAT}: cannot encode the response as JSON: {e}"
            ))
        }),
    }
}

/// The 64-bit fields of the metrics signal, whose JSON form the OTLP mapping
/// defines as a decimal string.
///
/// Every key of the messages this connector reads that holds a `fixed64`,
/// `sfixed64`, `int64`, `uint64` or a length or offset beside them, plus the
/// one such field of the response. `intValue` is absent because the derived
/// deserializer already reads that one as either shape.
const INT64_KEYS: [&str; 11] = [
    "asInt",
    "bucketCounts",
    "count",
    "flags",
    "negativeOffset",
    "positiveOffset",
    "rejectedDataPoints",
    "scale",
    "startTimeUnixNano",
    "timeUnixNano",
    "zeroCount",
];

/// Rewrite the decimal strings the OTLP JSON mapping writes for 64-bit fields
/// into the numbers the derived deserializer reads.
///
/// Only a value under one of [`INT64_KEYS`] is rewritten, and only when it
/// spells an integer, so an attribute whose *value* is a string of digits is
/// left alone: the keys that hold attributes are `key` and `value`, and the
/// string a `stringValue` holds sits under `stringValue`.
fn normalize_int64_strings(value: &mut Json) {
    match value {
        Json::Array(values) => values.iter_mut().for_each(normalize_int64_strings),
        Json::Object(object) => {
            for (key, value) in object.iter_mut() {
                if INT64_KEYS.contains(&key.as_str()) {
                    // A reborrow, so `value` stays usable for the walk below.
                    if let Json::Array(entries) = &mut *value {
                        entries.iter_mut().for_each(as_integer);
                    } else {
                        as_integer(value);
                    }
                }
                normalize_int64_strings(value);
            }
        }
        _ => {}
    }
}

/// One decimal string as the integer it spells, leaving anything else alone.
fn as_integer(value: &mut Json) {
    let Some(text) = value.as_str() else {
        return;
    };
    let parsed = text
        .parse::<i64>()
        .map(|number| Json::Number(number.into()))
        .or_else(|_| {
            text.parse::<u64>()
                .map(|number| Json::Number(number.into()))
        });
    if let Ok(number) = parsed {
        *value = number;
    }
}

/// The current wall clock in Unix nanoseconds, for a batch with no timestamp
/// column.
fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_a_name_and_reads_back_from_it() {
        for kind in [
            MetricKind::Gauge,
            MetricKind::Sum,
            MetricKind::Histogram,
            MetricKind::ExponentialHistogram,
            MetricKind::Summary,
        ] {
            assert_eq!(MetricKind::parse(kind.as_str()), Some(kind));
        }
        let names: Vec<&str> = [
            MetricKind::Gauge,
            MetricKind::Sum,
            MetricKind::Histogram,
            MetricKind::ExponentialHistogram,
            MetricKind::Summary,
        ]
        .iter()
        .map(|kind| kind.as_str())
        .collect();
        assert_eq!(names, KINDS.to_vec());
        assert_eq!(MetricKind::parse("rate"), None);
    }

    #[test]
    fn the_canonical_schema_fixes_every_column_and_its_nullability() {
        let schema = metrics_schema();
        let expected = [
            (column::NAME, DataType::Utf8, false),
            (column::VALUE, DataType::Float64, false),
            (column::TIMESTAMP, DataType::UInt64, false),
            (column::KIND, DataType::Utf8, false),
            (column::UNIT, DataType::Utf8, true),
            (column::DESCRIPTION, DataType::Utf8, true),
            (column::ATTRIBUTES, DataType::Utf8, true),
            (column::RESOURCE_ATTRIBUTES, DataType::Utf8, true),
            (column::COUNT, DataType::UInt64, true),
            (column::START_TIMESTAMP, DataType::UInt64, true),
        ];
        assert_eq!(schema.fields().len(), expected.len());
        for (index, (name, data_type, nullable)) in expected.iter().enumerate() {
            let field = schema.field(index);
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), data_type);
            assert_eq!(field.is_nullable(), *nullable);
        }
    }

    #[test]
    fn a_content_type_picks_the_encoding_it_names() {
        assert_eq!(
            encoding_for_media_type("application/x-protobuf"),
            Some(Encoding::Protobuf)
        );
        assert_eq!(
            encoding_for_media_type("APPLICATION/JSON; charset=utf-8"),
            Some(Encoding::Json)
        );
        assert_eq!(encoding_for_media_type(""), Some(Encoding::Protobuf));
        assert_eq!(encoding_for_media_type("text/plain"), None);
    }

    #[test]
    fn the_value_column_is_value_or_the_first_numeric_column_no_name_claims() {
        let declared = |fields: Vec<Field>| Schema::new(fields);
        let canonical = metrics_schema();
        assert_eq!(
            value_column(&canonical).map(|index| canonical.field(index).name().clone()),
            Some(column::VALUE.to_string())
        );
        let schema = declared(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("total", DataType::Float64, false),
        ]);
        assert_eq!(
            value_column(&schema).map(|index| schema.field(index).name().clone()),
            Some("id".to_string())
        );
        let timestamps_only = declared(vec![
            Field::new(column::TIMESTAMP, DataType::UInt64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        assert_eq!(value_column(&timestamps_only), None);
    }

    #[test]
    fn a_non_finite_attribute_double_is_refused_wherever_it_sits() {
        let nested = KeyValue {
            key: "outer".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::KvlistValue(
                    opentelemetry_proto::tonic::common::v1::KeyValueList {
                        values: vec![KeyValue {
                            key: "inner".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::DoubleValue(f64::NAN)),
                            }),
                            key_strindex: 0,
                        }],
                    },
                )),
            }),
            key_strindex: 0,
        };
        let error = finite_attributes("row 0's attributes", &[nested]).expect_err("NaN is refused");
        assert!(error.to_string().contains("inner"), "{error}");
    }
}
