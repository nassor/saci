//! Host and plugin contract tests against the `saci-plugin-smoketest` shared
//! library. Two steps:
//!
//! 1. `cargo build -p saci-plugin-smoketest`
//! 2. `cargo test --test plugin_roundtrip -p saci-service --features plugin`
//!
//! `cargo test -p saci-service` does not build unrelated workspace members, so
//! step 1 is a prerequisite exactly as the wasip2 fixture is for
//! `wasm_roundtrip`. `cargo test --workspace` builds both in one invocation.
//!
//! Four properties are covered:
//!
//! - **Identity.** The manifest is authoritative, so `name()` and
//!   `declared_components()` come straight from what the plugin describes, and
//!   the load fails unless the schemas it ships hash to the fingerprint it
//!   claims.
//! - **Data plane.** `Counter.id` passes through untouched while `Counter.seen`
//!   is written by the plugin, so both directions of the Arrow IPC round-trip
//!   are observed in one batch.
//! - **State across batches.** The plugin's running total lives in a
//!   `ProcessorState` resource, not in the dataset, so the second batch can only
//!   continue from the first when the checkpoint blob crosses the boundary both
//!   ways.
//! - **Config delivery.** `smoketest.multiplier` reaches the plugin through the
//!   `get_config` callback.

#![cfg(feature = "plugin")]

use std::collections::HashMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use saci_core::runtime::PipelineRuntime;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::plugin::NativePluginRuntime;
use serde::{Deserialize, Serialize};

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};

/// Host-side mirror of the `Counter` component declared in
/// `crates/saci-plugin-smoketest/src/lib.rs`.
///
/// Both definitions must share the same name and field shape. The load-time
/// fingerprint check inside `NativePluginRuntime::open` is what enforces that
/// the plugin's own two definitions agree; appending these rows to the
/// plugin's template dataset is what enforces that this mirror agrees too.
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

fn multiplier_config(value: &str) -> HashMap<String, String> {
    HashMap::from([("smoketest.multiplier".to_string(), value.to_string())])
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

fn column(dataset: &Dataset, field: &str) -> Vec<i64> {
    let batch = dataset.batch_for(Counter::name()).expect("Counter batch");
    let array = batch
        .column_by_name(field)
        .unwrap_or_else(|| panic!("Counter.{field} column"));
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("Counter.{field} is an i64 column"));

    values
        .iter()
        .map(|v| v.expect("Counter fields are non-null"))
        .collect()
}

/// The manifest is the only source of identity, and it is read at load, so both
/// of these are populated before the first batch.
#[tokio::test(flavor = "current_thread")]
async fn descriptor_comes_from_the_manifest() {
    let runtime = load_runtime(HashMap::new());

    assert_eq!(runtime.name(), "smoketest-plugin");
    assert_eq!(
        runtime.declared_components(),
        vec!["Counter"],
        "the state component lives in a resource, so the manifest declares only Counter"
    );

    let template = runtime.template_dataset();
    let batch = template
        .batch_for("Counter")
        .expect("Counter registered in the template");
    assert_eq!(batch.num_rows(), 0, "a template carries schemas, not rows");

    let schema = batch.schema();
    let fields: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(fields, vec!["id", "seen"]);

    // `descriptor_info()` is the one generic way a host holding
    // `Box<dyn PipelineRuntime>` can read what a plugin says about itself, so
    // the override must report the manifest rather than the empty default.
    let info = runtime.descriptor_info();
    assert!(
        !info.version.is_empty(),
        "the manifest's version must reach descriptor_info()"
    );
    assert!(
        info.stateful,
        "the smoketest plugin carries state across batches"
    );
    assert_eq!(
        info.schema_fingerprint.len(),
        8,
        "the fingerprint is lowercase 8-character hex: {}",
        info.schema_fingerprint
    );
}

/// One batch, both directions: `id` survives untouched and `seen` carries what
/// the plugin wrote.
#[tokio::test(flavor = "current_thread")]
async fn one_batch_round_trips_and_mutates() {
    let runtime = load_runtime(HashMap::new());
    let mut dataset = seeded_dataset(&runtime, &[10, 20, 30]);

    runtime.run_on(&mut dataset).await.expect("run_on");

    assert_eq!(dataset.rows(), 3, "row count survives the round-trip");
    assert_eq!(
        column(&dataset, "id"),
        vec![10, 20, 30],
        "no system writes id, so it must come back unchanged"
    );
    assert_eq!(
        column(&dataset, "seen"),
        vec![1, 2, 3],
        "a cold start numbers the batch from one"
    );
}

/// Feeding the returned blob back in as `prior` is the only way the plugin's
/// running total survives: it lives in a resource, not in the dataset, and the
/// dataset is rebuilt from IPC bytes on every call. `[4, 5, 6]` is unreachable
/// unless the opaque blob crossed the boundary in both directions.
#[tokio::test(flavor = "current_thread")]
async fn state_survives_across_batches_through_the_checkpoint() {
    let runtime = load_runtime(HashMap::new());

    let mut first = seeded_dataset(&runtime, &[10, 20, 30]);
    let checkpoint = runtime
        .run_on_with_state(&mut first, None)
        .await
        .expect("first batch")
        .expect("a stateful plugin returns a checkpoint");
    assert_eq!(column(&first, "seen"), vec![1, 2, 3]);

    let mut second = seeded_dataset(&runtime, &[40, 50, 60]);
    let next = runtime
        .run_on_with_state(&mut second, Some(&checkpoint))
        .await
        .expect("second batch");

    assert_eq!(column(&second, "seen"), vec![4, 5, 6]);
    assert!(next.is_some(), "the plugin keeps checkpointing");
}

/// Without the blob the plugin starts from scratch on every batch.
#[tokio::test(flavor = "current_thread")]
async fn state_resets_when_the_checkpoint_is_dropped() {
    let runtime = load_runtime(HashMap::new());

    let mut first = seeded_dataset(&runtime, &[10, 20, 30]);
    runtime
        .run_on_with_state(&mut first, None)
        .await
        .expect("first batch");

    let mut second = seeded_dataset(&runtime, &[40, 50, 60]);
    runtime
        .run_on_with_state(&mut second, None)
        .await
        .expect("second batch");

    assert_eq!(column(&second, "seen"), vec![1, 2, 3]);
}

/// `[pipeline.plugin.config]` values reach the plugin through the `get_config`
/// callback.
#[tokio::test(flavor = "current_thread")]
async fn config_reaches_the_plugin() {
    let runtime = load_runtime(multiplier_config("10"));
    let mut dataset = seeded_dataset(&runtime, &[10, 20, 30]);

    runtime.run_on(&mut dataset).await.expect("run_on");

    assert_eq!(column(&dataset, "seen"), vec![10, 20, 30]);
}

/// The plugin measures the batch itself and the host reports what it measured.
#[tokio::test(flavor = "current_thread")]
async fn the_plugin_reports_batch_metrics() {
    let runtime = load_runtime(HashMap::new());
    assert_eq!(runtime.last_batch_metrics(), None, "no batch has run yet");

    let mut dataset = seeded_dataset(&runtime, &[10, 20, 30]);
    runtime.run_on(&mut dataset).await.expect("run_on");

    let metrics = runtime
        .last_batch_metrics()
        .expect("metrics after one batch");
    assert_eq!(metrics.rows_in, 3);
    assert_eq!(metrics.rows_out, 3);
    assert_eq!(metrics.systems_run, 1);
    assert_eq!(metrics.retries, 0);
    assert!(metrics.wall_ns > 0, "wall_ns must be measured");
}

/// A file that is not a shared library fails at load with a message naming it.
#[tokio::test(flavor = "current_thread")]
async fn opening_something_that_is_not_a_library_fails() {
    let mut file = tempfile::Builder::new()
        .prefix("not-a-plugin")
        .suffix(std::env::consts::DLL_SUFFIX)
        .tempfile()
        .expect("temp file");
    file.write_all(b"this is not a shared library")
        .expect("write temp file");
    file.flush().expect("flush temp file");

    let err = NativePluginRuntime::open(file.path(), HashMap::new())
        .expect_err("a text file is not loadable");
    let text = err.to_string();

    assert!(
        text.contains(&file.path().display().to_string()),
        "the error must name the path it tried: {text}"
    );
}
