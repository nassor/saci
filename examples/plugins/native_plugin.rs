//! Drive a SACI workload through a native plugin: a shared library the host
//! opens with `dlopen`, talking the `saci-plugin-abi` C ABI.
//!
//! ```bash
//! cargo build -p saci-plugin-smoketest
//! cargo run -p saci-service --features plugin --example native_plugin
//! ```
//!
//! The fixture registers one `Counter` component with `id` and `seen`, and its
//! single system writes `seen[row] = (total + row + 1) * multiplier`, then adds
//! the batch's row count to `total`. `total` lives in the plugin's checkpoint
//! blob, so batch 2 continues where batch 1 stopped: three rows print
//! `1, 2, 3` and then `4, 5, 6`.
//!
//! Add the `tracing` feature to see the plugin's log lines and metrics; without
//! it the host prints logs through `eprintln!` and drops metrics.

use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use saci_core::runtime::PipelineRuntime;
use saci_service::dataset::Dataset;
use saci_service::plugin::NativePluginRuntime;

/// The component the smoketest fixture registers.
const COMPONENT: &str = "Counter";

/// Rows fed to both batches.
const IDS: [i64; 3] = [1, 2, 3];

/// Install a subscriber so the plugin's `log` and `metric` callbacks reach the
/// terminal. Metrics arrive at TRACE, which no default filter passes, hence the
/// explicit directive. `RUST_LOG` overrides it.
#[cfg(feature = "tracing")]
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,saci_service::plugin=trace"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .without_time()
        .try_init();
}

/// Built without the `tracing` feature: the host uses `eprintln!` for `log` and
/// drops `metric`. Nothing to install.
#[cfg(not(feature = "tracing"))]
fn init_tracing() {}

/// Path of the built smoketest library.
///
/// `current_exe()` is `<target>/<profile>/examples/native_plugin`, so two
/// parents give the profile directory whatever the profile or
/// `CARGO_TARGET_DIR` is. The cdylib lands beside the examples directory under
/// the platform's own library name.
fn smoketest_library() -> Result<PathBuf, Box<dyn Error>> {
    let exe = std::env::current_exe()?;
    let profile_dir = exe
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("cannot find the target profile directory above the running example")?;
    Ok(profile_dir.join(format!(
        "{}saci_plugin_smoketest{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    )))
}

/// Three `Counter` rows: `id` from `IDS`, `seen` zeroed for the plugin to fill.
///
/// Columns are placed by field name, so the batch matches the schema the plugin
/// declared rather than assuming a field order.
fn counter_batch(dataset: &Dataset) -> Result<RecordBatch, Box<dyn Error>> {
    let schema = dataset
        .schemas()
        .get(COMPONENT)
        .ok_or("the plugin's template dataset has no Counter schema")?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let values: Vec<i64> = match field.name().as_str() {
            "id" => IDS.to_vec(),
            _ => vec![0; IDS.len()],
        };
        columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// A three-row `Counter` dataset shaped by the plugin's own template.
fn counter_dataset(runtime: &NativePluginRuntime) -> Result<Dataset, Box<dyn Error>> {
    let mut dataset = runtime.template_dataset();
    let batch = counter_batch(&dataset)?;
    dataset.append_record_batch(COMPONENT, batch)?;
    Ok(dataset)
}

/// Borrow one non-null `Int64` column out of a batch.
fn i64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array, String> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| format!("Counter.{name} missing or not Int64"))
}

fn print_rows(label: &str, dataset: &Dataset) -> Result<(), Box<dyn Error>> {
    let batch = dataset
        .batch_for(COMPONENT)
        .ok_or("the returned dataset has no Counter component")?;
    let id = i64_column(batch, "id")?;
    let seen = i64_column(batch, "seen")?;
    let rows: Vec<String> = (0..batch.num_rows())
        .map(|i| format!("id={} seen={}", id.value(i), seen.value(i)))
        .collect();
    println!("  {label}: {}", rows.join("   "));
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let library = smoketest_library()?;
    if !library.exists() {
        eprintln!(
            "error: the smoketest plugin is not built: {}\n  \
             cargo build -p saci-plugin-smoketest",
            library.display()
        );
        std::process::exit(1);
    }
    println!("library: {}", library.display());

    // `smoketest.multiplier` reaches the plugin through the host vtable's
    // `get_config` callback. 1 is also the default, so the printed values stay
    // the documented ones.
    let config = HashMap::from([("smoketest.multiplier".to_string(), "1".to_string())]);
    let runtime = NativePluginRuntime::open(&library, config)?;
    println!("plugin:  {}", runtime.name());
    println!("declares: {:?}", runtime.declared_components());

    println!("\n── batch 1 (no prior state) ──");
    let mut first = counter_dataset(&runtime)?;
    let checkpoint = runtime
        .run_on_with_state(&mut first, None)
        .await?
        .ok_or("a stateful plugin must return a checkpoint")?;
    print_rows("rows", &first)?;
    println!("  checkpoint: {} bytes", checkpoint.len());

    println!("\n── batch 2 (checkpoint from batch 1 fed back as `prior`) ──");
    let mut second = counter_dataset(&runtime)?;
    let next = runtime
        .run_on_with_state(&mut second, Some(checkpoint.as_slice()))
        .await?
        .ok_or("a stateful plugin must return a checkpoint")?;
    print_rows("rows", &second)?;
    println!("  checkpoint: {} bytes", next.len());

    println!(
        "\nOK: the `seen` values continued across the batch boundary, so the \
         opaque checkpoint crossed the C ABI in both directions."
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();
    run().await
}
