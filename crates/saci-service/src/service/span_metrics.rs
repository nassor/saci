//! Bridge from `saci-core`'s `pipeline.stage` span to the
//! `saci_stage_duration_seconds` histogram.
//!
//! `saci-core` has no metrics dependency and keeps none, and
//! [`RunStats`](saci_core::RunStats) carries no per-stage breakdown, so the
//! histogram is fed host-side from the span lifetime instead.
//!
//! The `EnvFilter` in [`init_logging`](super::logging::init_logging) is a
//! subscriber-wide layer, so a filter that suppresses `saci_core` at span level
//! also stops this histogram.

use std::time::Instant;

use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// The span name `saci-core` opens once per execution stage.
const STAGE_SPAN: &str = "pipeline.stage";

/// Span-local start time, stored in the registry's extensions map.
struct StageStart(Instant);

/// Records `saci_stage_duration_seconds` from the lifetime of each
/// `pipeline.stage` span `saci-core` opens.
///
/// Install it on the subscriber registry alongside the format layer:
///
/// ```rust,no_run
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::SpanMetricsLayer;
/// use tracing_subscriber::prelude::*;
///
/// tracing_subscriber::registry()
///     .with(SpanMetricsLayer)
///     .init();
/// # }
/// ```
pub struct SpanMetricsLayer;

impl<S> tracing_subscriber::Layer<S> for SpanMetricsLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        _attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
        if span.name() != STAGE_SPAN {
            return;
        }
        span.extensions_mut().insert(StageStart(Instant::now()));
    }

    fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(StageStart(start)) = span.extensions_mut().remove::<StageStart>() else {
            return;
        };
        crate::metrics::instruments().stage_duration(start.elapsed().as_secs_f64());
    }
}
