//! The WIT `run-metrics` record the processor fills every batch must reach
//! Prometheus as the `saci_processor_*` series.
//!
//! Its own test binary, and one test in it, because installing a meter provider
//! is a process-global one-shot: any earlier `run_on` in the same process would
//! bind the instruments to the no-op default provider instead.
//!
//! ```text
//! cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
//! cargo test --test processor_metrics -p saci-service --features wasm,service
//! ```

#![cfg(all(feature = "wasm", feature = "metrics"))]

use std::collections::HashMap;

use prometheus::{Registry, TextEncoder};
use saci_core::runtime::PipelineRuntime;

#[path = "common/smoketest.rs"]
mod smoketest;

use smoketest::{load_runtime, seeded_dataset};

/// Rows in, rows out, and systems run must match what the fixture does: 16
/// `Ping` rows through the smoketest's single system.
#[tokio::test(flavor = "current_thread")]
async fn processor_run_metrics_reach_prometheus() {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .without_counter_suffixes()
        .with_registry(registry.clone())
        .build()
        .expect("build prometheus exporter");
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(exporter)
        .build();
    opentelemetry::global::set_meter_provider(provider);
    saci_service::metrics::init();

    let runtime = load_runtime(HashMap::new());
    let mut dataset = seeded_dataset(&runtime, 16);

    runtime
        .run_on(&mut dataset)
        .await
        .expect("processor run_on success");

    let text = TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encode prometheus text");

    for (series, expected) in [
        ("saci_processor_rows_in_total", 16.0),
        ("saci_processor_rows_out_total", 16.0),
        ("saci_processor_systems_run_total", 1.0),
    ] {
        let value = sample(&text, series)
            .unwrap_or_else(|| panic!("{series} missing from /metrics text:\n{text}"));
        assert_eq!(
            value, expected,
            "{series} should be {expected} after one batch, text was:\n{text}"
        );
    }

    assert_eq!(
        sample(&text, "saci_processor_batch_duration_seconds_count"),
        Some(1.0),
        "the batch histogram should hold exactly one sample, text was:\n{text}"
    );
    assert_eq!(
        sample(&text, "saci_processor_retries_total"),
        Some(0.0),
        "the identity pipeline retries nothing, text was:\n{text}"
    );
}

/// Read the value of the single Prometheus sample whose series is `name`.
///
/// The exporter attaches an `otel_scope_name` label to everything, so the
/// series name is followed by `{...}` rather than a space.
fn sample(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(name))
        .and_then(|rest| rest.rsplit(' ').next())
        .and_then(|v| v.parse::<f64>().ok())
}
