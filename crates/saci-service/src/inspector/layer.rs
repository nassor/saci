//! [`InspectorLayer`]: one `tracing` layer capturing spans and events.
//!
//! ## Why a layer, not an OTel span exporter
//!
//! [`init_logging`](crate::service::logging::init_logging) already composes
//! `env_filter` → `fmt_layer` → [`SpanMetricsLayer`](crate::service::SpanMetricsLayer)
//! → `otel_layer` onto one `tracing_subscriber::registry()`. Adding a second
//! span pipeline (an OTel `SpanExporter` behind its own tracer provider) would
//! double-instrument the same spans and give the dashboard timings that disagree
//! with the OTLP export. A fifth `.with(...)` on the existing stack shares one
//! source of truth instead.
//!
//! ## Why spans, not only metrics
//!
//! `saci_stage_duration_seconds` is recorded with **no attributes**
//! ([`SpanMetricsLayer`](crate::service::SpanMetricsLayer) deliberately ignores
//! the `pipeline.stage` span's field values), so per-stage and per-system
//! latency exists nowhere else. The inspector recovers it by grouping retained
//! `pipeline.stage` / `system.execute` records on their `stage` / `system`
//! field.
//!
//! Every closed span the subscriber's `EnvFilter` admits and its `Sampler`
//! keeps is captured, whatever its name. Nine exist, and their level decides
//! which of them `log_level` ever lets reach this layer:
//!
//! | span | level | opened by | fields |
//! |---|---|---|---|
//! | `workflow.batch` | `debug` | the runners | `workflow`, `iteration` (`claim` in the distributed runner), `rows` |
//! | `source.drain` | `debug` | the standalone runner | `workflow`, `source`, `component`, `rows` |
//! | `runtime.run` | `debug` | the runners | `workflow`, `processor`, `rows_in`, `rows_out`, plus `runtime` from the distributed runner |
//! | `sink.write` | `debug` | the standalone and stream runners | `workflow`, `sink`, `component`, `rows` |
//! | `processor.batch` | `debug` | `wasm::runner`, `plugin::runtime` | `workflow`, `processor`, `rows_in`, `rows_out`, `systems_run`, `retries`, guest or plugin wall time |
//! | `pipeline.run` | `info` | `saci-core` | `pipeline`, `stages`, `rows` |
//! | `pipeline.stage` | `info` | `saci-core` | `stage`, `systems` |
//! | `system.execute` | `info` | `saci-core` | `system` |
//! | `task_attempt` | `info`, retries only | `saci-core` | `attempt`, `max_attempts` |
//!
//! The default `log_level="error"` materialises none of the nine. The five
//! `debug` spans are the per-item runner tree: one whole tree opens per item,
//! so at `log_level="info"` none of them is materialised either, and what the
//! Traces tab shows there is rooted at `pipeline.run` instead, because
//! `saci-core`'s spans stay at `info` and the outermost surviving span becomes
//! the root. `log_level="debug"` restores the runner tree above it and with it
//! the whole per-item waterfall. Error and warning events raised by the
//! runners carry their own `workflow`, `iteration` and node fields for exactly
//! this reason: they are root events with no span to lean on, an error one
//! survives the error-only default, and the Logs tab is where they are read.
//!
//! The four `saci-core` names are behind its `tracing` feature, and a processor
//! compiles without it, so a WASM stage contributes `processor.batch` and
//! nothing below it.
//!
//! ## Untrusted field content
//!
//! Processor code reaches `tracing` through the WIT `host-io::log` import, so
//! field values are input, not program text. The visitor truncates any single
//! value at [`MAX_FIELD_BYTES`], caps a record at [`MAX_FIELDS`] fields, and
//! appends `("truncated", "true")` when either bound bites, so a record is never
//! silently misleading about what was dropped.

use std::borrow::Cow;
use std::fmt::Debug;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use super::buffer::TimeBoundedBuffer;
use super::record::{LogRecord, Pair, SpanRecord, level_name, now_unix_ms};

/// Largest single field value retained, in bytes. Longer values are truncated
/// on a UTF-8 boundary.
pub const MAX_FIELD_BYTES: usize = 512;

/// Largest number of fields retained per record.
pub const MAX_FIELDS: usize = 32;

/// Per-span state the layer parks in the registry's span extensions.
struct SpanState {
    started: std::time::Instant,
    started_unix_ms: u64,
    fields: Vec<Pair>,
    truncated: bool,
}

/// Collects `tracing` fields into `(name, value)` string pairs under the
/// [`MAX_FIELDS`] / [`MAX_FIELD_BYTES`] bounds.
struct FieldVisitor<'a> {
    fields: &'a mut Vec<Pair>,
    truncated: &'a mut bool,
    /// Filled from the `message` field, which is the event body rather than a
    /// field the UI should list.
    message: Option<String>,
    /// When false, `message` is treated as an ordinary field (spans have no
    /// message body).
    capture_message: bool,
}

impl<'a> FieldVisitor<'a> {
    fn for_span(fields: &'a mut Vec<Pair>, truncated: &'a mut bool) -> Self {
        Self {
            fields,
            truncated,
            message: None,
            capture_message: false,
        }
    }

    fn for_event(fields: &'a mut Vec<Pair>, truncated: &'a mut bool) -> Self {
        Self {
            fields,
            truncated,
            message: None,
            capture_message: true,
        }
    }

    /// Store `value` under `name`, honouring both bounds.
    fn put(&mut self, name: &str, value: String) {
        if self.capture_message && name == "message" {
            self.message = Some(truncate(value, self.truncated));
            return;
        }
        if self.fields.len() >= MAX_FIELDS {
            *self.truncated = true;
            return;
        }
        let value = truncate(value, self.truncated);
        self.fields.push((name.to_string(), value));
    }
}

/// Cut `value` to at most [`MAX_FIELD_BYTES`] bytes on a UTF-8 boundary.
fn truncate(mut value: String, truncated: &mut bool) -> String {
    if value.len() <= MAX_FIELD_BYTES {
        return value;
    }
    let mut end = MAX_FIELD_BYTES;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    *truncated = true;
    value
}

impl Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.put(field.name(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        // Str values go in verbatim: `record_debug` would add quotes and
        // escape sequences the UI would then have to strip.
        self.put(field.name(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field.name(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field.name(), value.to_string());
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.put(field.name(), value.to_string());
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.put(field.name(), value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field.name(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field.name(), value.to_string());
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field.name(), value.to_string());
    }
}

/// Captures closed spans into one buffer and events into another.
///
/// Cheap to clone; both buffers are `Arc`-backed.
#[derive(Debug, Clone)]
pub struct InspectorLayer {
    spans: TimeBoundedBuffer<SpanRecord>,
    logs: TimeBoundedBuffer<LogRecord>,
}

impl InspectorLayer {
    /// Build a layer writing into `spans` and `logs`.
    pub(crate) fn new(
        spans: TimeBoundedBuffer<SpanRecord>,
        logs: TimeBoundedBuffer<LogRecord>,
    ) -> Self {
        Self { spans, logs }
    }
}

/// Walk up from `id` to the outermost span, returning its id.
///
/// A root span is its own trace id, which is what makes a whole `pipeline.run`
/// tree groupable without a second id allocation.
fn trace_root<S>(ctx: &Context<'_, S>, id: &Id) -> u64
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut current = match ctx.span(id) {
        Some(span) => span,
        None => return id.into_u64(),
    };
    while let Some(parent) = current.parent() {
        current = parent;
    }
    current.id().into_u64()
}

impl<S> Layer<S> for InspectorLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut fields = Vec::new();
        let mut truncated = false;
        attrs.record(&mut FieldVisitor::for_span(&mut fields, &mut truncated));
        span.extensions_mut().insert(SpanState {
            started: std::time::Instant::now(),
            started_unix_ms: now_unix_ms(),
            fields,
            truncated,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        let Some(state) = extensions.get_mut::<SpanState>() else {
            return;
        };
        let mut truncated = state.truncated;
        values.record(&mut FieldVisitor::for_span(
            &mut state.fields,
            &mut truncated,
        ));
        state.truncated = truncated;
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(state) = span.extensions_mut().remove::<SpanState>() else {
            return;
        };

        let mut fields = state.fields;
        if state.truncated {
            fields.push(("truncated".to_string(), "true".to_string()));
        }

        let metadata = span.metadata();
        self.spans.push(SpanRecord {
            trace_id: trace_root(&ctx, &id),
            span_id: id.into_u64(),
            parent_id: span.parent().map(|p| p.id().into_u64()),
            name: Cow::Borrowed(metadata.name()),
            target: Cow::Borrowed(metadata.target()),
            started_unix_ms: state.started_unix_ms,
            duration_us: u64::try_from(state.started.elapsed().as_micros()).unwrap_or(u64::MAX),
            fields,
        });
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = Vec::new();
        let mut truncated = false;
        let mut visitor = FieldVisitor::for_event(&mut fields, &mut truncated);
        event.record(&mut visitor);
        let message = visitor.message.take().unwrap_or_default();
        if truncated {
            fields.push(("truncated".to_string(), "true".to_string()));
        }

        let current = ctx.event_span(event);
        let span_id = current.as_ref().map(|span| span.id().into_u64());
        let trace_id = current
            .as_ref()
            .map(|span| trace_root(&ctx, &span.id()))
            .or(span_id);

        let metadata = event.metadata();
        self.logs.push(LogRecord {
            level: Cow::Borrowed(level_name(metadata.level())),
            target: Cow::Borrowed(metadata.target()),
            message,
            at_unix_ms: now_unix_ms(),
            span_id,
            trace_id,
            fields,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tracing_subscriber::prelude::*;

    fn buffers() -> (
        TimeBoundedBuffer<SpanRecord>,
        TimeBoundedBuffer<LogRecord>,
        InspectorLayer,
    ) {
        let spans = TimeBoundedBuffer::new(Duration::from_secs(60), 1024);
        let logs = TimeBoundedBuffer::new(Duration::from_secs(60), 1024);
        let layer = InspectorLayer::new(spans.clone(), logs.clone());
        (spans, logs, layer)
    }

    #[test]
    fn nested_spans_share_a_trace_id_and_link_parent_to_child() {
        let (spans, logs, layer) = buffers();
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let outer = tracing::info_span!("outer");
            let entered = outer.enter();
            let inner = tracing::info_span!("inner");
            let inner_entered = inner.enter();
            tracing::warn!(rows = 5, "late");
            drop(inner_entered);
            drop(inner);
            drop(entered);
            drop(outer);
        });

        let captured = spans.read_recent();
        assert_eq!(captured.len(), 2, "both spans closed: {captured:?}");
        let inner = captured
            .iter()
            .find(|s| s.name == "inner")
            .expect("inner span");
        let outer = captured
            .iter()
            .find(|s| s.name == "outer")
            .expect("outer span");
        assert_eq!(inner.parent_id, Some(outer.span_id));
        assert_eq!(inner.trace_id, outer.trace_id);
        assert_eq!(outer.trace_id, outer.span_id, "root is its own trace");

        let events = logs.read_recent();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, "WARN");
        assert_eq!(events[0].message, "late");
        assert_eq!(events[0].trace_id, Some(outer.span_id));
        assert!(
            events[0]
                .fields
                .iter()
                .any(|(k, v)| k == "rows" && v == "5"),
            "got: {:?}",
            events[0].fields
        );
    }

    #[test]
    fn stage_span_fields_survive_capture() {
        let (spans, _logs, layer) = buffers();
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("pipeline.stage", stage = 1, systems = 2);
            let _entered = span.enter();
        });

        let captured = spans.read_recent();
        let stage = captured
            .iter()
            .find(|s| s.name == "pipeline.stage")
            .expect("stage span");
        assert!(
            stage
                .fields
                .contains(&("stage".to_string(), "1".to_string())),
            "grouping key missing: {:?}",
            stage.fields
        );
        assert!(
            stage
                .fields
                .contains(&("systems".to_string(), "2".to_string()))
        );
    }

    #[test]
    fn oversized_field_values_are_truncated_and_flagged() {
        let (_spans, logs, layer) = buffers();
        let subscriber = tracing_subscriber::registry().with(layer);
        let long = "x".repeat(MAX_FIELD_BYTES * 2);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(blob = %long, "big");
        });

        let events = logs.read_recent();
        let record = &events[0];
        let blob = record
            .fields
            .iter()
            .find(|(k, _)| k == "blob")
            .expect("blob field");
        assert_eq!(blob.1.len(), MAX_FIELD_BYTES);
        assert!(
            record
                .fields
                .iter()
                .any(|(k, v)| k == "truncated" && v == "true")
        );
    }

    #[test]
    fn field_count_is_capped() {
        let (_spans, logs, layer) = buffers();
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            // 40 distinct fields need 40 literal callsite fields; a loop cannot
            // build them, so record them through one span with a wide macro
            // call instead.
            tracing::info!(
                f00 = 0,
                f01 = 1,
                f02 = 2,
                f03 = 3,
                f04 = 4,
                f05 = 5,
                f06 = 6,
                f07 = 7,
                f08 = 8,
                f09 = 9,
                f10 = 10,
                f11 = 11,
                f12 = 12,
                f13 = 13,
                f14 = 14,
                f15 = 15,
                f16 = 16,
                f17 = 17,
                f18 = 18,
                f19 = 19,
                f20 = 20,
                f21 = 21,
                f22 = 22,
                f23 = 23,
                f24 = 24,
                f25 = 25,
                f26 = 26,
                f27 = 27,
                f28 = 28,
                f29 = 29,
                f30 = 30,
                f31 = 31,
                f32 = 32,
                f33 = 33,
                "wide"
            );
        });

        let events = logs.read_recent();
        let record = &events[0];
        // MAX_FIELDS captured values plus the truncation marker.
        assert_eq!(record.fields.len(), MAX_FIELDS + 1);
        assert!(
            record
                .fields
                .iter()
                .any(|(k, v)| k == "truncated" && v == "true")
        );
    }
}
