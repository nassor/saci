//! The per-batch numbers a native plugin reports must reach Prometheus as the
//! `saci_processor_*` series, attributed by node id, exactly like the wasm
//! host's `run-metrics`.
//!
//! Its own test binary, and one test in it, because installing a meter provider
//! is a process-global one-shot: `plugin_roundtrip.rs` runs plugin batches in
//! the same process, and whichever test binds the instruments first would win.
//!
//! ```text
//! cargo build -p saci-plugin-smoketest
//! cargo test --test plugin_metrics -p saci-service --features plugin,metrics
//! ```

#![cfg(all(feature = "plugin", feature = "metrics"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use prometheus::{Registry, TextEncoder};
use saci_core::runtime::PipelineRuntime;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::plugin::NativePluginRuntime;
use serde::{Deserialize, Serialize};

use arrow_schema::{DataType, Field, Schema};

/// Host-side mirror of the `Counter` component declared in
/// `crates/saci-plugin-smoketest/src/lib.rs`. Both definitions must agree, which
/// the load-time fingerprint check enforces for the plugin's own two copies and
/// the `seeded_dataset` append enforces for this one.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Counter {
    id: i64,
    seen: i64,
}

impl Component for Counter {
    fn name() -> &'static str {
        "Counter"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("seen", DataType::Int64, false),
        ]))
    }
}

/// Locate the built cdylib.
///
/// `current_exe` is `<target>/<profile>/deps/<test-bin>`, so two `parent()`
/// calls give the profile directory whatever the profile and whatever
/// `CARGO_TARGET_DIR` points at.
fn smoketest_plugin_path() -> PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    let profile_dir = exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("profile directory above deps/");

    profile_dir.join(format!(
        "{}saci_plugin_smoketest{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

fn load_runtime(config: HashMap<String, String>) -> NativePluginRuntime {
    let path = smoketest_plugin_path();
    assert!(
        path.exists(),
        "smoketest plugin not found at {}; run `cargo build -p saci-plugin-smoketest` first",
        path.display()
    );

    NativePluginRuntime::open(&path, config).expect("NativePluginRuntime::open")
}

/// Seed a dataset from the runtime's own template so every component the plugin
/// declares is registered, then fill the data plane with one row per id.
fn seeded_dataset(runtime: &NativePluginRuntime, ids: &[i64]) -> Dataset {
    let mut dataset = runtime.template_dataset();
    let rows: Vec<Counter> = ids.iter().map(|&id| Counter { id, seen: 0 }).collect();
    dataset
        .append::<Counter>(&rows)
        .expect("append Counter rows");
    dataset
}

/// Rows in, rows out, and systems run must match what the fixture does: 3
/// `Counter` rows through the smoketest's single system, attributed under the
/// node id `with_identity` assigned.
#[tokio::test(flavor = "current_thread")]
async fn plugin_run_metrics_reach_prometheus() {
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

    let runtime =
        load_runtime(HashMap::new()).with_identity("wf".to_string(), "proc-1".to_string());
    let mut dataset = seeded_dataset(&runtime, &[10, 20, 30]);

    runtime
        .run_on(&mut dataset)
        .await
        .expect("plugin run_on success");

    let text = TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encode prometheus text");

    // The unattributed values, which every /metrics consumer reads.
    for (series, expected) in [
        ("saci_processor_rows_in_total", 3.0),
        ("saci_processor_rows_out_total", 3.0),
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

    // The same values under processor="proc-1", what the dashboard's latency
    // card and processor edge rates read.
    for (series, expected) in [
        ("saci_processor_rows_in_total", 3.0),
        ("saci_processor_rows_out_total", 3.0),
        ("saci_processor_systems_run_total", 1.0),
    ] {
        let value = attributed(&text, series, "proc-1").unwrap_or_else(|| {
            panic!("{series} attributed to proc-1 missing from /metrics text:\n{text}")
        });
        assert_eq!(
            value, expected,
            "{series} should be {expected} under processor=\"proc-1\" after one batch, text was:\n{text}"
        );
    }

    assert_eq!(
        attributed(
            &text,
            "saci_processor_batch_duration_seconds_count",
            "proc-1"
        ),
        Some(1.0),
        "the attributed histogram should hold exactly one sample, text was:\n{text}"
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

/// Read the value of the single sample of series `name` that also carries
/// `processor="<id>"`.
fn attributed(text: &str, name: &str, id: &str) -> Option<f64> {
    let attr = format!("processor=\"{id}\"");
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(name).filter(|rest| rest.contains(&attr)))
        .and_then(|rest| rest.rsplit(' ').next())
        .and_then(|v| v.parse::<f64>().ok())
}
