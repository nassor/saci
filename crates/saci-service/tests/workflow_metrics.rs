//! Per-node attribution of the `saci_processor_*` series, end to end through
//! two real WebAssembly processor instances linked in one workflow.
//!
//! This is the arithmetic that motivates per-node attribution: two processors
//! each write their own count into the same process-wide total, so the
//! unattributed series alone cannot answer "how many rows did `first`
//! handle". Two smoketest components give an exact expected split: 4 rows
//! through each of two processors is 8 unattributed and 4 under each
//! processor's own id.
//!
//! Its own test binary because installing a meter provider is a
//! process-global one-shot: any earlier `run_on` in the same process would
//! bind the instruments to the no-op default provider instead.
//!
//! ```bash
//! cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
//! cargo test --test workflow_metrics -p saci-service --features wasm,service
//! ```

#![cfg(all(feature = "wasm", feature = "service"))]

use std::collections::HashMap;

use prometheus::{Registry, TextEncoder};
use saci_core::runtime::PipelineRuntime;

#[path = "common/smoketest.rs"]
mod smoketest;

use smoketest::{load_runtime, seeded_dataset};

/// Two `wasm` nodes pointing at the same real artifact, given distinct
/// identities as `ServiceBuilder` would from their own declared ids. The
/// module paths are only read once, by `load_runtime`; each call loads its
/// own component instance.
#[tokio::test]
async fn two_linked_processors_write_independent_attributed_series() {
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

    let first = load_runtime(HashMap::new()).with_identity("wf".to_string(), "first".to_string());
    let second = load_runtime(HashMap::new()).with_identity("wf".to_string(), "second".to_string());

    let mut first_data = seeded_dataset(&first, 4);
    first.run_on(&mut first_data).await.expect("first run");

    // `first`'s output becomes `second`'s input: `Ping` has no system, so the
    // batch passes through byte-identical, matching a real link with no
    // re-encoding between two processors.
    let ping_batch = first_data
        .batch_for("Ping")
        .expect("first wrote its own Ping component")
        .clone();
    let mut second_data = second.template_dataset();
    second_data
        .append_record_batch("Ping", ping_batch)
        .expect("hand first's output to second");
    second.run_on(&mut second_data).await.expect("second run");

    let text = TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encode prometheus text");

    let lines_for = |text: &str, series: &str| -> Vec<String> {
        text.lines()
            .filter(|line| line.starts_with(&format!("{series}{{")))
            .map(str::to_string)
            .collect()
    };

    // The batch histogram must carry both the unattributed total and one
    // attributed series per processor.
    let counts = lines_for(&text, "saci_processor_rows_out_total");
    assert!(
        counts.iter().any(|line| !line.contains("processor=")),
        "the unattributed series must still exist: {counts:?}"
    );
    assert!(
        counts
            .iter()
            .any(|line| line.contains(r#"processor="first""#)),
        "{counts:?}"
    );
    assert!(
        counts
            .iter()
            .any(|line| line.contains(r#"processor="second""#)),
        "{counts:?}"
    );

    // Each processor's own series must be exactly 4 rows, and the
    // unattributed total exactly 8: each processor's own number must not be
    // the process-wide sum.
    let value_of = |line: &str| -> f64 {
        line.rsplit(' ')
            .next()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or_else(|| panic!("no trailing value in {line}"))
    };
    let unattributed = counts
        .iter()
        .find(|line| !line.contains("processor="))
        .map(|line| value_of(line))
        .expect("unattributed series");
    let first_count = counts
        .iter()
        .find(|line| line.contains(r#"processor="first""#))
        .map(|line| value_of(line))
        .expect("first's own series");
    let second_count = counts
        .iter()
        .find(|line| line.contains(r#"processor="second""#))
        .map(|line| value_of(line))
        .expect("second's own series");
    assert!((first_count - 4.0).abs() < 1e-9, "got {first_count}");
    assert!((second_count - 4.0).abs() < 1e-9, "got {second_count}");
    assert!(
        (unattributed - 8.0).abs() < 1e-9,
        "the unattributed total sums both processors, not either one alone: got {unattributed}"
    );
}
