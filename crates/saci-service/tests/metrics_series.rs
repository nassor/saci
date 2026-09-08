//! `saci_stage_duration_seconds` is fed by [`SpanMetricsLayer`] from the
//! `pipeline.stage` spans `saci-core` opens, so this exercises both halves at
//! once: the histogram can only have samples if the spans were created and
//! closed.
//!
//! One test in its own binary, because only one meter provider can be installed
//! per process.
//!
//! ```text
//! cargo test --test metrics_series -p saci-service --features service
//! ```

#![cfg(feature = "service")]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use prometheus::{Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use tracing_subscriber::prelude::*;

use saci_service::prelude::*;
use saci_service::service::SpanMetricsLayer;

#[derive(Serialize, Deserialize)]
struct Reading {
    raw: f64,
    scaled: f64,
}

impl Component for Reading {
    fn name() -> &'static str {
        "Reading"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("raw", DataType::Float64, false),
            Field::new("scaled", DataType::Float64, false),
        ]))
    }
}

/// Writes `Reading.scaled`, so `Publish` must run after it.
struct Scale;

#[async_trait]
impl System for Scale {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("scale")
            .read("Reading", "raw")
            .write("Reading", "scaled")
    }

    async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
        Ok(())
    }
}

/// Reads `Reading.scaled`, which `Scale` writes: read-after-write puts these
/// two systems in separate stages.
struct Publish;

#[async_trait]
impl System for Publish {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("publish").read("Reading", "scaled")
    }

    async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
        Ok(())
    }
}

/// A two-stage pipeline run must leave at least two samples in
/// `saci_stage_duration_seconds`.
///
/// Single-threaded so `with_default` covers the whole run: a multi-thread
/// runtime would move the future to a worker where the thread-local subscriber
/// is not installed.
#[tokio::test(flavor = "current_thread")]
async fn stage_spans_feed_the_stage_duration_histogram() {
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

    let pipeline = Pipeline::builder("metrics-series")
        .with::<Reading>()
        .with_system(Scale)
        .with_system(Publish)
        .build();

    let mut dataset = Dataset::new();
    dataset
        .register_component::<Reading>()
        .expect("register Reading");
    dataset
        .append::<Reading>(&[Reading {
            raw: 1.0,
            scaled: 0.0,
        }])
        .expect("append Reading");

    let subscriber = tracing_subscriber::registry().with(SpanMetricsLayer);
    let run = tracing::subscriber::with_default(subscriber, || {
        futures::executor::block_on(pipeline.run_on_with_stats(&mut dataset))
    });
    let stats = run.expect("pipeline run");
    assert_eq!(stats.systems_run, 2, "both systems should have run");
    assert_eq!(
        pipeline.stage_count(),
        Some(2),
        "read-after-write on Reading.scaled must produce two stages"
    );

    let text = TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encode prometheus text");

    let count = text
        .lines()
        .find_map(|l| l.strip_prefix("saci_stage_duration_seconds_count"))
        .and_then(|rest| rest.rsplit(' ').next())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("saci_stage_duration_seconds_count missing from:\n{text}"));

    assert!(
        count >= 2.0,
        "expected at least one sample per stage, got {count} in:\n{text}"
    );
}
