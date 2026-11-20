//! Per-node attribution of the `saci_processor_*` series, end to end through
//! two real WebAssembly processor instances linked in one workflow.
//!
//! This is the arithmetic that motivates per-node attribution: two processors
//! would otherwise share one total that cannot answer "how many rows did
//! `first` handle". Two smoketest components give an exact expected split: 4
//! rows through each of two processors is 4 under each processor's own id and
//! 8 summed over the `processor` attribute. Every sample is recorded once,
//! under its own node id.
//!
//! Its own test binary because installing a meter provider is a
//! process-global one-shot: any earlier `run_on` in the same process would
//! bind the instruments to the no-op default provider instead.
//!
//! ```bash
//! cargo build --release -p saci-processor-smoketest --target wasm32-wasip3
//! cargo test --test workflow_metrics -p saci --features server,wasm-host,service
//! ```

#![cfg(all(feature = "service", feature = "wasm-host"))]

use std::collections::HashMap;

use prometheus::{Registry, TextEncoder};
use saci::runtime::PipelineRuntime;

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
    saci::metrics::init();

    let first = load_runtime(HashMap::new()).with_identity("wf".to_string(), "first".to_string());
    let second = load_runtime(HashMap::new()).with_identity("wf".to_string(), "second".to_string());

    let mut first_data = seeded_dataset(&first, 4);
    first
        .run_batch(&mut first_data, None)
        .await
        .expect("first run");

    // `first`'s output becomes `second`'s input: `Ping` has no system, so the
    // batch passes through byte-identical, matching a real link with no
    // re-encoding between two processors.
    let ping_batch = first_data
        .batch_named("Ping")
        .expect("first wrote its own Ping component")
        .clone();
    let mut second_data = second.template_dataset();
    second_data
        .append_record_batch("Ping", ping_batch)
        .expect("hand first's output to second");
    second
        .run_batch(&mut second_data, None)
        .await
        .expect("second run");

    let text = TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encode prometheus text");

    let lines_for = |text: &str, series: &str| -> Vec<String> {
        text.lines()
            .filter(|line| line.starts_with(&format!("{series}{{")))
            .map(str::to_string)
            .collect()
    };

    // Every sample names its processor: no series carries an unattributed
    // copy, and each processor has its own.
    let counts = lines_for(&text, "saci_processor_rows_out_total");
    assert!(
        counts.iter().all(|line| line.contains("processor=")),
        "every sample must carry its processor id: {counts:?}"
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

    // Each processor's own series is exactly 4 rows, and the sum over the
    // attribute is 8: each processor's own number is not the process-wide
    // total.
    let value_of = |line: &str| -> f64 {
        line.rsplit(' ')
            .next()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or_else(|| panic!("no trailing value in {line}"))
    };
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
    let total: f64 = counts.iter().map(|line| value_of(line)).sum();
    assert!((first_count - 4.0).abs() < 1e-9, "got {first_count}");
    assert!((second_count - 4.0).abs() < 1e-9, "got {second_count}");
    assert!(
        (total - 8.0).abs() < 1e-9,
        "the sum over the attribute is both processors together: got {total}"
    );
}
