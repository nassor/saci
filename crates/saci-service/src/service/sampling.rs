//! Subscriber-side sampling for everything that shows a span or an event to a
//! reader: stdout, the `/ui` dashboard and OTLP export.
//!
//! [`init_logging`](crate::service::logging::init_logging) stacks the format
//! layer, the inspector's capture layer and the OTLP layer as one group behind
//! a single [`Sampler`] filter, so the three always agree on which spans and
//! events exist. Metrics are not sampled: `SpanMetricsLayer` sits outside the
//! group.
//!
//! ## What the two ratios cover
//!
//! `observability.error_sample_ratio` governs `Level::ERROR` spans and events;
//! `observability.sample_ratio` governs everything the `log_level` filter
//! admits below ERROR. The two are independent, so an error raised inside a
//! trace `sample_ratio` dropped is still rolled against the error ratio and
//! reaches the Logs tab on its own.
//!
//! ## Decided once per root
//!
//! A root span or a parentless event is rolled; a span or event with a parent
//! follows that parent's verdict, so a kept trace is whole and a dropped trace
//! leaves no orphan behind. The probe is
//! [`Context::current_span`](tracing_subscriber::layer::Context::current_span)
//! (the registry's unfiltered current span) plus
//! [`Context::span`](tracing_subscriber::layer::Context::span) (which applies
//! this filter and answers `None` for a span this filter dropped);
//! `lookup_current` must not be used, because it walks past a disabled
//! ancestor to the nearest enabled one and so would report a dropped root's
//! child as a root.
//!
//! ## The four exceptions
//!
//! [`FLOW_CONTROL_TARGET`], [`WINDOWING_TARGET`], [`HEAL_TARGET`] and
//! [`DLQ_TARGET`] are
//! never sampled and never filtered by level: the adaptive flow controller's
//! lines are how an operator sees admission control working at the error-only
//! default, the windowing lines are how they see a windowed node dropping
//! every arrival it is handed, which otherwise looks exactly like an idle
//! pipeline, the self-healing lines are how they see a connector that had
//! to be replaced, which otherwise looks exactly like one that never failed,
//! and the dead letter lines are how they see a batch a sink refused, which
//! otherwise looks exactly like a batch that was never produced.

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{Event, Level, Metadata, Subscriber, span::Id, subscriber::Interest};
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;

/// Target of the flow-control lines the runners always emit.
///
/// The `EnvFilter` carries a directive enabling it at INFO whatever
/// `log_level` says, and [`Sampler`] never drops it.
pub const FLOW_CONTROL_TARGET: &str = "saci::flow_control";

/// Target of the windowing lines the runners always emit.
///
/// Carried by the same always-on `EnvFilter` directive and the same
/// [`Sampler`] bypass as [`FLOW_CONTROL_TARGET`], for the same reason: a
/// windowed node whose watermark has run ahead of its inbound stream drops
/// every arrival and emits nothing, and no other number on `/metrics` or the
/// dashboard distinguishes that from having no work to do.
pub const WINDOWING_TARGET: &str = "saci::windowing";

/// Target of the connector self-healing lines.
///
/// Carried by the same always-on `EnvFilter` directive and the same
/// [`Sampler`] bypass as [`FLOW_CONTROL_TARGET`], for the same reason: a
/// connector that had to be rebuilt is the one event an operator must see at
/// the error-only default, and no other number distinguishes a healed node
/// from one that never failed.
pub const HEAL_TARGET: &str = "saci::heal";

/// Target of the dead letter queue lines.
///
/// Carried by the same always-on `EnvFilter` directive and the same
/// [`Sampler`] bypass as [`FLOW_CONTROL_TARGET`], for the same reason: a
/// batch a sink refused is data that did not arrive, and at the error-only
/// default the recording and the replay that returns it are the only sight
/// an operator gets of either.
pub const DLQ_TARGET: &str = "saci::dlq";

/// Whether `target` bypasses both ratios and the level filter.
fn never_sampled(target: &str) -> bool {
    target == FLOW_CONTROL_TARGET
        || target == WINDOWING_TARGET
        || target == HEAL_TARGET
        || target == DLQ_TARGET
}

/// One whole unit of the fixed-point accumulator [`Ratio`] adds into.
const ONE: u64 = 1 << 32;

/// Deterministic keep-one-in-N ratio.
///
/// An accumulator in 32.32 fixed point: each roll adds the ratio and keeps the
/// item when the addition crosses the next whole unit. Exact in the long run,
/// lock-free, and reproducible in a test, which no RNG-backed sampler is.
#[derive(Debug)]
struct Ratio {
    /// The ratio scaled to [`ONE`]. `>= ONE` means keep everything.
    step: u64,
    acc: AtomicU64,
}

impl Ratio {
    /// Build a ratio from a fraction in `0.0..=1.0`. A value outside that
    /// range is clamped; `ObservabilityConfig::validate` is what rejects one.
    fn new(ratio: f64) -> Self {
        let scaled = (ratio.clamp(0.0, 1.0) * ONE as f64).round();
        let step = if scaled >= ONE as f64 {
            ONE
        } else {
            scaled as u64
        };
        Self {
            step,
            acc: AtomicU64::new(0),
        }
    }

    /// `true` when this ratio keeps every item, so no accumulator is touched.
    fn is_always(&self) -> bool {
        self.step >= ONE
    }

    /// Roll one item.
    fn roll(&self) -> bool {
        if self.step >= ONE {
            return true;
        }
        if self.step == 0 {
            return false;
        }
        let prev = self.acc.fetch_add(self.step, Ordering::Relaxed);
        (prev & (ONE - 1)) + self.step >= ONE
    }
}

/// What the sampler found above the span or event being decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Parent {
    /// A root: nothing above it, so it is rolled.
    None,
    /// The parent survived sampling, so this follows it in.
    Sampled,
    /// The parent was dropped, so only an error is rolled on its own.
    Unsampled,
}

/// The per-layer filter that samples spans and events once per root.
///
/// Built from `observability.sample_ratio` and
/// `observability.error_sample_ratio`. Both at `1.0` (the default) is a
/// passthrough: every callsite registers as
/// [`Interest::always`](tracing::subscriber::Interest::always) and no
/// accumulator is ever touched.
#[derive(Debug)]
pub struct Sampler {
    errors: Ratio,
    others: Ratio,
}

impl Sampler {
    /// Build a sampler keeping `sample_ratio` of sub-ERROR spans and events and
    /// `error_sample_ratio` of ERROR ones.
    pub fn new(sample_ratio: f64, error_sample_ratio: f64) -> Self {
        Self {
            errors: Ratio::new(error_sample_ratio),
            others: Ratio::new(sample_ratio),
        }
    }

    /// `true` when neither ratio can drop anything.
    fn passthrough(&self) -> bool {
        self.errors.is_always() && self.others.is_always()
    }

    /// Decide one span or event.
    fn decide(&self, meta: &Metadata<'_>, parent: Parent) -> bool {
        if never_sampled(meta.target()) {
            return true;
        }
        let is_error = *meta.level() == Level::ERROR;
        match parent {
            Parent::Sampled => true,
            Parent::Unsampled => is_error && self.errors.roll(),
            Parent::None => {
                if is_error {
                    self.errors.roll()
                } else {
                    self.others.roll()
                }
            }
        }
    }
}

/// Classify `id` as a parent: absent, kept by this filter, or dropped by it.
fn parent_of<S>(cx: &Context<'_, S>, id: Option<&Id>) -> Parent
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    match id {
        None => Parent::None,
        Some(id) => {
            if cx.span(id).is_some() {
                Parent::Sampled
            } else {
                Parent::Unsampled
            }
        }
    }
}

impl<S> Filter<S> for Sampler
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        if never_sampled(meta.target()) || self.passthrough() {
            Interest::always()
        } else {
            Interest::sometimes()
        }
    }

    fn enabled(&self, meta: &Metadata<'_>, cx: &Context<'_, S>) -> bool {
        if self.passthrough() {
            return true;
        }
        // An event's parent may be given explicitly at the callsite, which
        // only `event_enabled` can see, so events are decided there.
        if meta.is_event() {
            return true;
        }
        let parent = parent_of(cx, cx.current_span().id());
        self.decide(meta, parent)
    }

    fn event_enabled(&self, event: &Event<'_>, cx: &Context<'_, S>) -> bool {
        if self.passthrough() {
            return true;
        }
        let parent = if event.is_root() {
            Parent::None
        } else if event.is_contextual() {
            parent_of(cx, cx.current_span().id())
        } else {
            parent_of(cx, event.parent())
        };
        self.decide(event.metadata(), parent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tracing_subscriber::prelude::*;

    use crate::inspector::buffer::TimeBoundedBuffer;
    use crate::inspector::layer::InspectorLayer;
    use crate::inspector::record::{LogRecord, SpanRecord};

    type Buffers = (
        TimeBoundedBuffer<SpanRecord>,
        TimeBoundedBuffer<LogRecord>,
        InspectorLayer,
    );

    fn buffers() -> Buffers {
        let spans = TimeBoundedBuffer::new(Duration::from_secs(60), 1024);
        let logs = TimeBoundedBuffer::new(Duration::from_secs(60), 1024);
        let layer = InspectorLayer::new(spans.clone(), logs.clone());
        (spans, logs, layer)
    }

    /// Run `body` under a registry whose only layer captures into the returned
    /// buffers behind a `Sampler` built from the two ratios.
    fn capture(
        sample_ratio: f64,
        error_sample_ratio: f64,
        body: impl FnOnce(),
    ) -> (Vec<SpanRecord>, Vec<LogRecord>) {
        let (spans, logs, layer) = buffers();
        let subscriber = tracing_subscriber::registry()
            .with(layer.with_filter(Sampler::new(sample_ratio, error_sample_ratio)));
        tracing::subscriber::with_default(subscriber, body);
        (spans.read_recent(), logs.read_recent())
    }

    #[test]
    fn root_errors_are_sampled_by_the_error_ratio() {
        let (_spans, logs) = capture(1.0, 0.5, || {
            for i in 0..10 {
                tracing::error!(i, "boom");
            }
        });
        assert_eq!(logs.len(), 5, "one in two kept: {logs:?}");
    }

    #[test]
    fn sample_ratio_never_touches_errors() {
        let (_spans, logs) = capture(0.0, 1.0, || {
            for i in 0..10 {
                tracing::info!(i, "chatter");
                tracing::error!(i, "boom");
            }
        });
        assert_eq!(logs.len(), 10, "only the errors survive: {logs:?}");
        assert!(
            logs.iter().all(|record| record.level == "ERROR"),
            "got: {logs:?}"
        );
    }

    #[test]
    fn children_follow_their_root() {
        let (spans, logs) = capture(0.5, 1.0, || {
            for i in 0..4 {
                let root = tracing::info_span!("root", i);
                root.in_scope(|| {
                    let child = tracing::info_span!("child", i);
                    let _entered = child.enter();
                });
                tracing::info!(parent: &root, i, "inside");
            }
        });

        let roots: Vec<&SpanRecord> = spans.iter().filter(|s| s.name == "root").collect();
        let children: Vec<&SpanRecord> = spans.iter().filter(|s| s.name == "child").collect();
        assert_eq!(roots.len(), 2, "one root in two kept: {spans:?}");
        assert_eq!(children.len(), 2, "a child follows its root: {spans:?}");
        assert_eq!(logs.len(), 2, "an event follows its root: {logs:?}");

        for child in &children {
            let parent = child.parent_id.expect("a captured child names its parent");
            assert!(
                roots.iter().any(|root| root.span_id == parent),
                "no captured child may name a dropped root: {spans:?}"
            );
        }
        for record in &logs {
            let trace = record.trace_id.expect("a captured event names its trace");
            assert!(
                roots.iter().any(|root| root.span_id == trace),
                "no captured event may name a dropped root: {logs:?}"
            );
        }
    }

    /// A dropped root takes its whole subtree with it.
    ///
    /// This is the behaviour the `Parent::Unsampled` arm depends on, and it
    /// rests on a `tracing-subscriber` detail: a span this filter rejects
    /// still passes `Registry::enabled`, because `FilterMap::any_enabled` is
    /// `bits != u64::MAX` and a filter id is one bit. So the span is created,
    /// `enter()` pushes it, and `Context::span` answers `None` for it here,
    /// which is what tells a dropped parent from an absent one. Only
    /// `Interest::never()` from `callsite_enabled` would suppress creation
    /// outright and turn every child into a fresh root, which is why spans
    /// register as `Interest::sometimes`.
    #[test]
    fn a_dropped_root_drops_its_whole_subtree() {
        let (spans, logs) = capture(0.0, 1.0, || {
            let root = tracing::info_span!("root");
            root.in_scope(|| {
                let child = tracing::info_span!("child");
                child.in_scope(|| {
                    let _grandchild = tracing::info_span!("grandchild").entered();
                    tracing::info!("deep");
                });
            });
        });
        assert!(
            spans.is_empty(),
            "no span of the subtree survives: {spans:?}"
        );
        assert!(
            logs.is_empty(),
            "no event of the subtree survives: {logs:?}"
        );
    }

    /// Metrics are not sampled. `SpanMetricsLayer` sits outside the sampled
    /// group in [`init_logging`](crate::service::logging::init_logging), so a
    /// ratio that empties the traces tab must leave
    /// `saci_stage_duration_seconds` counting every stage.
    #[cfg(feature = "metrics")]
    #[test]
    fn the_stage_histogram_is_not_sampled() {
        use crate::service::span_metrics::SpanMetricsLayer;

        fn stage_count() -> u64 {
            prometheus::TextEncoder::new()
                .encode_to_string(&crate::metrics::test_registry().gather())
                .expect("encode prometheus text")
                .lines()
                .find(|line| line.starts_with("saci_stage_duration_seconds_count"))
                .and_then(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
                .unwrap_or(0)
        }

        let (spans, _logs, layer) = buffers();
        let before = stage_count();
        let subscriber = tracing_subscriber::registry()
            .with(layer.with_filter(Sampler::new(0.0, 0.0)))
            .with(SpanMetricsLayer);
        tracing::subscriber::with_default(subscriber, || {
            for stage in 0..4 {
                let _entered = tracing::info_span!("pipeline.stage", stage, systems = 1).entered();
            }
        });

        assert!(
            spans.read_recent().is_empty(),
            "the sampler dropped every stage span"
        );
        assert_eq!(
            stage_count() - before,
            4,
            "every stage still reached the histogram"
        );
    }

    #[test]
    fn an_error_inside_a_dropped_trace_still_rolls_the_error_ratio() {
        let (spans, logs) = capture(0.0, 1.0, || {
            let root = tracing::info_span!("root");
            root.in_scope(|| tracing::error!("boom"));
        });
        assert!(spans.is_empty(), "the root was dropped: {spans:?}");
        assert_eq!(logs.len(), 1, "the error survived alone: {logs:?}");
        assert_eq!(logs[0].level, "ERROR");
    }

    /// All four never-sampled targets survive a ratio that drops everything,
    /// including an ERROR. `sample_ratio 0.02` is a realistic setting for a
    /// windowed workflow, and a warning delivered one time in fifty is not a
    /// diagnosis.
    #[test]
    fn the_never_sampled_targets_bypass_both_ratios() {
        let (_spans, logs) = capture(0.0, 0.0, || {
            tracing::info!(target: FLOW_CONTROL_TARGET, "x");
            tracing::warn!(target: WINDOWING_TARGET, "w");
            tracing::warn!(target: HEAL_TARGET, "h");
            tracing::warn!(target: DLQ_TARGET, "d");
            tracing::info!("y");
            tracing::error!("z");
        });
        assert_eq!(
            logs.iter()
                .map(|record| (&*record.target, record.message.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (FLOW_CONTROL_TARGET, "x"),
                (WINDOWING_TARGET, "w"),
                (HEAL_TARGET, "h"),
                (DLQ_TARGET, "d")
            ],
            "only the four bypassing targets survive: {logs:?}"
        );
    }
}
