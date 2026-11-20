//! W3C trace-context propagation across a session.
//!
//! The sink reads its send span's OpenTelemetry context into a `traceparent`
//! header field, and the source sets that field as its receive span's remote
//! parent. Both are no-ops without an installed OpenTelemetry layer: `context`
//! then yields an empty context whose span context is invalid, so the sink
//! sends no traceparent and the receive span stays a root carrying the peer
//! fields.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// `00-<trace_id>-<span_id>-<flags>` for `span`'s OpenTelemetry context.
///
/// `None` when no OpenTelemetry layer is installed or the span is disabled,
/// which is the case for every build that exports no traces.
pub(crate) fn traceparent_of(span: &tracing::Span) -> Option<String> {
    let cx = span.context();
    let sc = cx.span().span_context().clone();
    sc.is_valid().then(|| {
        format!(
            "00-{}-{}-{:02x}",
            sc.trace_id(),
            sc.span_id(),
            sc.trace_flags().to_u8()
        )
    })
}

/// Parse `traceparent` and, on success, make it `span`'s remote parent.
///
/// Returns whether the header was adopted. A malformed header is the peer's
/// problem and costs the batch nothing, so the caller ignores a `false`.
pub(crate) fn adopt(span: &tracing::Span, traceparent: &str) -> bool {
    let Some(sc) = parse(traceparent) else {
        return false;
    };
    span.set_parent(opentelemetry::Context::new().with_remote_span_context(sc))
        .is_ok()
}

/// Strict W3C `traceparent` parse: version `00` only, exact field widths.
fn parse(traceparent: &str) -> Option<SpanContext> {
    if traceparent.len() != 55 {
        return None;
    }
    let mut parts = traceparent.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let span_id = parts.next()?;
    let flags = parts.next()?;
    if parts.next().is_some() || version != "00" {
        return None;
    }
    if trace_id.len() != 32 || span_id.len() != 16 || flags.len() != 2 {
        return None;
    }
    let sc = SpanContext::new(
        TraceId::from_hex(trace_id).ok()?,
        SpanId::from_hex(span_id).ok()?,
        TraceFlags::new(u8::from_str_radix(flags, 16).ok()?),
        true,
        TraceState::default(),
    );
    sc.is_valid().then_some(sc)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn a_traceparent_parses_back_to_the_same_ids() {
        let sc = parse(SAMPLE).expect("the sample header is well formed");
        assert_eq!(
            format!(
                "00-{}-{}-{:02x}",
                sc.trace_id(),
                sc.span_id(),
                sc.trace_flags().to_u8()
            ),
            SAMPLE
        );
        assert!(sc.is_remote());
    }

    #[test]
    fn malformed_headers_are_refused() {
        for bad in [
            "",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
            // Version 01 is not this parser's business.
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            // An all-zero trace id is invalid by specification.
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            // An all-zero span id likewise.
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            // Right length, wrong field widths.
            "00-0af7651916cd43dd8448eb211c80319-cb7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-zz",
        ] {
            assert!(parse(bad).is_none(), "must refuse {bad:?}");
        }
    }

    /// With no OpenTelemetry layer installed there is no context to carry, and
    /// a batch still goes out: the sink sends an empty traceparent.
    #[test]
    fn a_span_without_an_otel_layer_carries_no_traceparent() {
        let span = tracing::debug_span!("peer.send");
        assert!(traceparent_of(&span).is_none());
        assert!(!adopt(&span, SAMPLE));
    }

    /// With a real OpenTelemetry layer installed, the header carries the
    /// receive span into the sender's trace. The provider needs no exporter:
    /// the span context is built when the span opens, not when it ends.
    ///
    /// `tracing-opentelemetry` exposes no accessor for a span's parent id, so
    /// the shared trace id is what proves the adoption.
    #[test]
    fn a_traceparent_carries_the_receive_span_into_the_sender_trace() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::layer::SubscriberExt as _;

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));

        tracing::subscriber::with_default(subscriber, || {
            let send = tracing::info_span!("peer.send");
            let tp = traceparent_of(&send).expect("an installed layer yields a traceparent");

            let recv = tracing::info_span!("peer.receive");
            assert!(adopt(&recv, &tp), "the header must be adopted");
            assert_eq!(
                recv.context().span().span_context().trace_id(),
                send.context().span().span_context().trace_id(),
                "the receive span must join the sender's trace"
            );
        });
    }
}
