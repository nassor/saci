//! Logging and span-export initialisation for the SACI service.
//!
//! [`init_logging`] must be called once at service startup, before any
//! `tracing` instrumentation fires.  It selects a log format (Pretty or JSON)
//! based on [`ObservabilityConfig`], wires an [`EnvFilter`] so operators can
//! override the level at runtime via the `RUST_LOG` environment variable,
//! installs the [`SpanMetricsLayer`] that feeds `saci_stage_duration_seconds`,
//! adds the [`Inspector`]'s capture layer unless it is disabled in config, and
//! turns on OTLP/HTTP span export when `otlp_endpoint` is set.
//!
//! ## Format selection
//!
//! | `log_format` | Output |
//! |---|---|
//! | `Pretty` | Human-readable, ANSI colour when stdout is a TTY |
//! | `Json`   | One JSON object per log record for log aggregators |
//!
//! ## Filtering
//!
//! When `RUST_LOG` is not set the filter is
//! `saci=<log_level>,tower_http=<log_level>,error` plus three always-on
//! directives, `saci::flow_control=info`, `saci::windowing=warn` and
//! `saci::heal=warn`.
//! `log_level` defaults to `error`, so a service that is behaving says nothing
//! beyond the flow-control lines and no span is materialised at all.
//!
//! `RUST_LOG` replaces the level directives, never those three:
//! [`EnvFilter::add_directive`] replaces an equal-target directive, and a
//! longer target beats the `saci` prefix, so all three survive `log_level
//! "off"` and `RUST_LOG=off` alike.
//!
//! ## Sampling
//!
//! The format layer, the inspector's capture layer and the OTLP layer are one
//! group behind a single [`Sampler`], so stdout, the `/ui` dashboard and the
//! collector see the same sampled stream. [`SpanMetricsLayer`] sits outside
//! the group: `saci_stage_duration_seconds` is a metric, and metrics are not
//! sampled.
//!
//! ## Span export
//!
//! OTLP carries spans only. Metrics stay on the Prometheus pull endpoint the
//! HTTP control plane serves; no OTLP metrics exporter is installed. The SDK
//! sampler is left at its `AlwaysOn` default, because the layer only ever
//! receives what [`Sampler`] kept. The returned [`TelemetryGuard`] owns the
//! tracer provider, and [`TelemetryGuard::shutdown`] flushes the batch
//! processor before the process exits.

use crate::error::{SaciError, SaciResult};
use crate::inspector::Inspector;
use crate::service::config::{LogFormat, ObservabilityConfig};
use crate::service::sampling::{
    DLQ_TARGET, FLOW_CONTROL_TARGET, HEAL_TARGET, Sampler, WINDOWING_TARGET,
};
use crate::service::span_metrics::SpanMetricsLayer;

use std::io::IsTerminal as _;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

/// The subscriber the format layer is built against: the bare registry plus the
/// `EnvFilter`, which is the same for both log formats.
type FilteredRegistry = tracing_subscriber::layer::Layered<EnvFilter, tracing_subscriber::Registry>;

/// Holds the tracer provider so a final flush is possible.
///
/// Empty when OTLP export is off. Dropping it is not enough once
/// `opentelemetry::global::set_tracer_provider` has stored a clone in a
/// process-lifetime static, so [`shutdown`](Self::shutdown) is load-bearing.
#[derive(Debug)]
pub struct TelemetryGuard {
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Flush and stop span export.
    ///
    /// Logs a failure rather than returning it: the process is already exiting,
    /// and a failed flush must not mask the runner's own exit status.
    pub async fn shutdown(self) {
        let Some(provider) = self.tracer_provider else {
            return;
        };
        // `SdkTracerProvider::shutdown` joins the batch processor's OS thread,
        // so it must not run on a runtime worker.
        let joined = tokio::task::spawn_blocking(move || provider.shutdown()).await;
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "otlp span exporter shutdown failed"),
            Err(e) => tracing::error!(error = %e, "otlp span exporter shutdown task failed"),
        }
    }
}

/// Initialise the global `tracing` subscriber from `config`, and build the
/// in-process inspector when `observability.inspector.enabled` is set.
///
/// Call once per process. A second call returns [`SaciError::Configuration`]
/// because the global subscriber is already installed.
///
/// `node_id` is attached to exported spans as the `saci.node_id` resource
/// attribute, so a collector can tell cluster members apart.
///
/// The returned [`Inspector`] is `None` when capture is disabled, in which case
/// no inspector layer is installed at all and the cost is one `Option` branch.
/// The caller owns the handle: it is what the HTTP router and
/// [`ServiceBuilder`](crate::service::builder::ServiceBuilder) need.
///
/// The `EnvFilter` this installs is subscriber-wide, so a `RUST_LOG` that
/// suppresses `saci_service` also empties the inspector's span buffer. That
/// is the same caveat `saci_stage_duration_seconds` carries.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] if:
/// - `config.sample_ratio` or `config.error_sample_ratio` is outside
///   `0.0..=1.0`.
/// - The OTLP span exporter cannot be built from `config.otlp_endpoint`.
/// - A global subscriber has already been installed.
///
/// # Examples
///
/// ```rust,no_run
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::config::ObservabilityConfig;
/// use saci_service::service::logging::init_logging;
///
/// let cfg = ObservabilityConfig::default(); // Pretty format, error level, no OTLP
/// let (telemetry, inspector) = init_logging(&cfg, 1).expect("logging init");
/// assert!(inspector.is_some()); // the inspector is on by default
/// # }
/// ```
pub fn init_logging(
    config: &ObservabilityConfig,
    node_id: u64,
) -> SaciResult<(TelemetryGuard, Option<Inspector>)> {
    // Reject a bad ratio before building anything.
    config.validate()?;

    let env_filter = env_filter_for(&config.log_level, std::env::var("RUST_LOG").ok().as_deref());

    let (tracer_provider, otel_layer) = match &config.otlp_endpoint {
        None => (None, None),
        Some(endpoint) => {
            use opentelemetry::trace::TracerProvider as _;
            use opentelemetry_otlp::WithExportConfig as _;

            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_endpoint(traces_endpoint(endpoint))
                .build()
                .map_err(|e| SaciError::configuration(format!("otlp span exporter: {e}")))?;
            let resource = opentelemetry_sdk::Resource::builder()
                .with_service_name("saci")
                .with_attribute(opentelemetry::KeyValue::new(
                    "saci.node_id",
                    node_id.to_string(),
                ))
                .build();
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                // No SDK sampler: this layer only ever receives what
                // `Sampler` kept, so a second ratio would compound.
                .build();
            opentelemetry::global::set_tracer_provider(provider.clone());
            let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("saci"));
            (Some(provider), Some(layer))
        }
    };

    // The json and pretty format layers are different types, and
    // `OpenTelemetryLayer<S, T>` is generic over the subscriber it sits on, so
    // a per-format chain would need a second, differently-typed otel layer.
    // Boxing the format layer keeps one chain and one `try_init`.
    let fmt_layer: Box<dyn tracing_subscriber::Layer<FilteredRegistry> + Send + Sync> =
        match config.log_format {
            LogFormat::Json => Box::new(fmt::layer().json()),
            LogFormat::Pretty => {
                let use_ansi = std::io::stdout().is_terminal();
                Box::new(fmt::layer().with_ansi(use_ansi))
            }
        };

    // The inspector is one more layer on this same registry, deliberately: a
    // second span pipeline would double-instrument every span `saci-core` opens.
    let inspector = if config.inspector.enabled {
        Some(Inspector::new(&config.inspector))
    } else {
        None
    };

    // `tracing_subscriber` implements `Layer` for `Option<L>`, so the same
    // group covers both the export-on and export-off cases, and both the
    // inspector-on and inspector-off ones. One `Sampler` filters all three at
    // once, so they never disagree about which spans and events exist;
    // `SpanMetricsLayer` stays outside it because metrics are not sampled.
    let sampled: Box<dyn tracing_subscriber::Layer<FilteredRegistry> + Send + Sync> = Box::new(
        fmt_layer
            .and_then(inspector.as_ref().map(Inspector::layer))
            .and_then(otel_layer)
            .with_filter(Sampler::new(config.sample_ratio, config.error_sample_ratio)),
    );

    let result = tracing_subscriber::registry()
        .with(env_filter)
        .with(sampled)
        .with(SpanMetricsLayer)
        .try_init();

    result.map_err(|e| {
        SaciError::configuration(format!("failed to install tracing subscriber: {e}"))
    })?;

    Ok((TelemetryGuard { tracer_provider }, inspector))
}

/// Build the subscriber-wide filter from `log_level` and an optional
/// `RUST_LOG` directive string.
///
/// `rust_log` replaces the level directives when it parses; an unset or
/// unparsable value falls back to `saci=<log_level>,tower_http=<log_level>,
/// error`. The tail is `error`, not `warn`: the whole dependency tree is
/// error-only by default, and `tower_http` follows `log_level` so HTTP request
/// lines appear only when the operator asks for them.
///
/// The [`FLOW_CONTROL_TARGET`], [`WINDOWING_TARGET`], [`HEAL_TARGET`] and
/// [`DLQ_TARGET`] directives are added
/// last and unconditionally. [`EnvFilter::add_directive`] replaces an
/// equal-target directive, so they win over a `RUST_LOG` that names any of
/// them, and a longer target beats the `saci` prefix directive, so all four
/// pass under `log_level "off"` and `RUST_LOG=off` alike.
///
/// The windowing directive is `warn`, not `info`. Flow control reports a
/// working search, while the windowing lines report one condition only: a
/// windowed node dropping every arrival it is handed, which is a warning
/// wherever it happens. The heal and dead letter directives are `warn` for
/// the same reason, and each carries its recovery line too so that the pair
/// a heal or a replay produces is never split by a level filter.
pub(crate) fn env_filter_for(log_level: &str, rust_log: Option<&str>) -> EnvFilter {
    let base = rust_log
        .and_then(|directives| EnvFilter::try_new(directives).ok())
        .unwrap_or_else(|| {
            EnvFilter::new(format!("saci={log_level},tower_http={log_level},error"))
        });
    base.add_directive(
        format!("{FLOW_CONTROL_TARGET}=info")
            .parse()
            .expect("a static directive parses"),
    )
    .add_directive(
        format!("{WINDOWING_TARGET}=warn")
            .parse()
            .expect("a static directive parses"),
    )
    .add_directive(
        format!("{HEAL_TARGET}=warn")
            .parse()
            .expect("a static directive parses"),
    )
    .add_directive(
        format!("{DLQ_TARGET}=warn")
            .parse()
            .expect("a static directive parses"),
    )
}

/// Turn a collector base URL into the OTLP/HTTP traces URL.
///
/// `opentelemetry-otlp` uses a programmatically supplied endpoint verbatim, so
/// a bare collector root would POST to `/` and every collector would reject it.
/// This applies the same rule the OTLP spec gives for a base endpoint: append
/// `/v1/traces`. An operator who already wrote the full URL gets it unchanged.
fn traces_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if trimmed.ends_with("/v1/traces") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/traces")
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::inspector::buffer::TimeBoundedBuffer;
    use crate::inspector::layer::InspectorLayer;
    use crate::inspector::record::{LogRecord, SpanRecord};
    use crate::service::config::{LogFormat, ObservabilityConfig};
    use std::time::Duration;

    /// Capture the log records `body` emits through `filter`, with no global
    /// subscriber install, so this is safe to run alongside other tests.
    fn records_under(filter: EnvFilter, body: impl FnOnce()) -> Vec<LogRecord> {
        let spans = TimeBoundedBuffer::<SpanRecord>::new(Duration::from_secs(60), 1024);
        let logs = TimeBoundedBuffer::<LogRecord>::new(Duration::from_secs(60), 1024);
        let subscriber = tracing_subscriber::registry()
            .with(filter)
            .with(InspectorLayer::new(spans, logs.clone()));
        tracing::subscriber::with_default(subscriber, body);
        logs.read_recent()
    }

    fn pretty_config() -> ObservabilityConfig {
        ObservabilityConfig {
            log_format: LogFormat::Pretty,
            log_level: "info".to_string(),
            ..ObservabilityConfig::default()
        }
    }

    fn json_config() -> ObservabilityConfig {
        ObservabilityConfig {
            log_format: LogFormat::Json,
            log_level: "debug".to_string(),
            ..ObservabilityConfig::default()
        }
    }

    /// Calling `init_logging` twice returns a Configuration error on the second
    /// call, because the global subscriber is already installed.
    ///
    /// Ignored by default: it races with any other test that installs a
    /// subscriber. Run with `cargo test -- --ignored` to exercise it.
    #[test]
    #[ignore = "installs a global subscriber; must run in isolation"]
    fn test_second_init_returns_error() {
        let cfg = pretty_config();
        // The first call may lose the race to another test's subscriber.
        let _ = init_logging(&cfg, 1);
        let err = init_logging(&cfg, 1).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("subscriber"),
            "error should mention subscriber: {err}"
        );
    }

    /// A ratio outside `0.0..=1.0` is rejected before any provider is built, so
    /// this is safe to run alongside other tests.
    #[test]
    fn test_out_of_range_sample_ratio_is_rejected() {
        let mut cfg = pretty_config();
        cfg.sample_ratio = 1.5;
        let err = init_logging(&cfg, 1).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("sample_ratio"),
            "error should name the key: {err}"
        );
    }

    /// At the default level nothing below ERROR reaches a layer, whatever its
    /// target, except the flow-control and windowing lines.
    #[test]
    fn the_default_filter_admits_errors_flow_and_windowing_lines_only() {
        let logs = records_under(env_filter_for("error", None), || {
            tracing::warn!(target: "saci_service::x", "warned");
            tracing::info!(target: "saci_service::x", "chatter");
            tracing::warn!(target: "hyper::client", "dependency");
            tracing::error!(target: "saci_service::x", "boom");
            tracing::info!(target: FLOW_CONTROL_TARGET, "flow");
            tracing::warn!(target: WINDOWING_TARGET, "window");
            tracing::info!(target: WINDOWING_TARGET, "window chatter");
        });

        let messages: Vec<&str> = logs.iter().map(|record| record.message.as_str()).collect();
        assert_eq!(
            messages,
            vec!["boom", "flow", "window"],
            "the windowing directive is warn, not info, so only the warning passes; got: {logs:?}"
        );
    }

    /// `RUST_LOG` replaces the level directives but neither always-on one.
    #[test]
    fn rust_log_cannot_silence_the_flow_or_windowing_lines() {
        let logs = records_under(env_filter_for("info", Some("off")), || {
            tracing::info!(target: "saci_service::x", "chatter");
            tracing::info!(target: FLOW_CONTROL_TARGET, "flow");
            tracing::warn!(target: WINDOWING_TARGET, "window");
        });

        let messages: Vec<&str> = logs.iter().map(|record| record.message.as_str()).collect();
        assert_eq!(messages, vec!["flow", "window"], "got: {logs:?}");
    }

    #[test]
    fn test_pretty_config_construction() {
        let cfg = pretty_config();
        assert_eq!(cfg.log_format, LogFormat::Pretty);
        assert_eq!(cfg.log_level, "info");
    }

    #[test]
    fn test_json_config_construction() {
        let cfg = json_config();
        assert_eq!(cfg.log_format, LogFormat::Json);
        assert_eq!(cfg.log_level, "debug");
    }

    #[test]
    fn test_default_observability_config_is_pretty_error() {
        let cfg = ObservabilityConfig::default();
        assert_eq!(cfg.log_format, LogFormat::Pretty);
        assert_eq!(cfg.log_level, "error");
        assert!(cfg.otlp_endpoint.is_none());
        assert_eq!(cfg.sample_ratio, 1.0);
        assert_eq!(cfg.error_sample_ratio, 1.0);
    }

    /// A base URL gains the traces path; a full URL is left alone. Getting this
    /// wrong makes the exporter POST to `/`, which collectors reject.
    #[test]
    fn test_traces_endpoint_appends_the_signal_path() {
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318"),
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318/"),
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://collector:4318/otlp"),
            "http://collector:4318/otlp/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318/v1/traces"),
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318/v1/traces/"),
            "http://127.0.0.1:4318/v1/traces"
        );
    }
}
