//! Drive one SACI workload through six WebAssembly processors written in six
//! different languages. Every stage is a separate `.wasm` component
//! implementing the same `saci:pipeline@0.3.0` WIT world, loaded through the
//! same host (`WasmPipelineRuntime`).
//!
//! | order | component        | language   | writes                  |
//! |-------|------------------|------------|-------------------------|
//! | 1     | `validate-go`    | Go         | `valid`                 |
//! | 2     | `enrich-py`      | Python     | `usd_amount`, `usd_amount_display` |
//! | 3     | `score-ts`       | TypeScript | `risk_score`, `flagged` |
//! | 4     | `fee-kt`         | Kotlin     | `fee`                   |
//! | 5     | `tier-cs`        | C#         | `review_tier`           |
//! | 6     | `settle-rs`      | Rust       | `settlement` + ledger   |
//!
//! ```bash
//! # build the six components (needs `cargo xtask polyglot` first)
//! cargo xtask polyglot
//!
//! # run the six-language chain
//! cargo run -p saci-service --features wasm,tracing --example polyglot_orders
//! ```
//!
//! Without the `tracing` feature the host prints processor logs through `eprintln!`
//! and drops processor metrics, so use `wasm,tracing` for the run.
//!
//! Every stage now declares its own `Order` in its own language: the schema,
//! the fingerprint and the fixture are no longer generated from one canonical
//! Rust definition. `run` therefore asserts the six stages' reported
//! `schema_fingerprint` values are equal to each other (independently-authored
//! schemas structurally agree) instead of comparing against a Rust-computed
//! canonical value, and exits non-zero on any drift.
//!
//! The `Order` struct here is the driver's own copy of the row type: it is
//! what the host decodes the chain's output with, and its schema must agree
//! field for field with what the six processors report, which the pairwise
//! fingerprint check enforces.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use saci_core::runtime::PipelineRuntime;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::wasm::{WasmEngine, WasmPipelineRuntime};

/// Epoch ticks granted to each processor call: 100 × 100 ms, the same 10 s budget
/// `saci-service`'s own WASM loader uses.
const EPOCH_TICKS: u64 = 100;

/// Absolute tolerance for every float comparison and for the formatted table.
/// `100.0 * 1.10` is not exactly `110.0` in f64.
const EPS: f64 = 1e-6;

/// Install a subscriber so the processors' `host-io::log` and `host-io::metric`
/// calls reach the terminal. Metrics arrive at TRACE, which no default filter
/// passes, hence the explicit directive. `RUST_LOG` overrides it.
#[cfg(feature = "tracing")]
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,saci_service::wasm=trace"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .without_time()
        .try_init();
}

/// Built without the `tracing` feature: the host uses `eprintln!` for `log` and
/// drops `metric`. Nothing to install.
#[cfg(not(feature = "tracing"))]
fn init_tracing() {}

/// Workspace root, derived from this crate's manifest dir so the example
/// behaves identically no matter which directory `cargo run` was invoked from.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/saci-service")
        .to_path_buf()
}

/// Directory holding the built components. `SACI_POLYGLOT_BUILD_DIR`
/// overrides it; the integration test resolves the same way.
fn build_dir() -> PathBuf {
    match std::env::var_os("SACI_POLYGLOT_BUILD_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => workspace_root().join("examples/polyglot/build"),
    }
}

/// One order, in the shape every polyglot stage reads and writes.
///
/// Field order is load-bearing: it feeds the schema fingerprint every stage's
/// descriptor reports and the buffer walk the SDK codecs perform.
/// All six stages declare the same twelve fields in this order, each in its
/// own language's native form.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Order {
    /// Stable row identity. Input only.
    id: i64,
    /// Originating region (`emea` / `apac` / `amer`). Input only, read by the
    /// **Kotlin** stage to pick a fee rate.
    region: String,
    /// ISO currency code of `amount`. Input only.
    currency: String,
    /// Order amount in `currency`. Input only.
    amount: f64,
    /// `amount > min_amount`. Written by the **Go** stage.
    valid: bool,
    /// `amount` converted to USD, or `0.0` when invalid. Written by the
    /// **Python** stage.
    usd_amount: f64,
    /// `usd_amount` formatted for display. Written by the **Python** stage,
    /// the one non-Rust stage with a variable-length writer.
    usd_amount_display: String,
    /// `usd_amount / risk_threshold`. Written by the **TypeScript** stage.
    risk_score: f64,
    /// `risk_score >= 1.0`. Written by the **TypeScript** stage.
    flagged: bool,
    /// `usd_amount` times the region's rate, or `0.0` when invalid. Written by
    /// the **Kotlin** stage.
    fee: f64,
    /// `0` clear, `1` manual review, `2` escalated. Written by the **C#**
    /// stage.
    review_tier: i64,
    /// `REJECTED` / `HOLD` / `REVIEW` / `SETTLED`. Written by the **Rust**
    /// stage.
    settlement: String,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("valid", DataType::Boolean, false),
            Field::new("usd_amount", DataType::Float64, false),
            Field::new("usd_amount_display", DataType::Utf8, false),
            Field::new("risk_score", DataType::Float64, false),
            Field::new("flagged", DataType::Boolean, false),
            Field::new("fee", DataType::Float64, false),
            Field::new("review_tier", DataType::Int64, false),
            Field::new("settlement", DataType::Utf8, false),
        ]))
    }
}

impl Order {
    /// Construct an input row with every derived column zeroed.
    fn seed(id: i64, region: &str, currency: &str, amount: f64) -> Self {
        Self {
            id,
            region: region.to_string(),
            currency: currency.to_string(),
            amount,
            valid: false,
            usd_amount: 0.0,
            usd_amount_display: String::new(),
            risk_score: 0.0,
            flagged: false,
            fee: 0.0,
            review_tier: 0,
            settlement: String::new(),
        }
    }
}

/// The six-row fixture every polyglot verification path uses. The rows are
/// chosen so every branch of every stage fires once: two rows fail validation,
/// one is flagged and escalated, one lands in manual review, and two settle
/// from different regions at different fee rates.
fn fixture_rows() -> Vec<Order> {
    vec![
        Order::seed(1, "emea", "EUR", 100.0),
        Order::seed(2, "emea", "GBP", -5.0),
        Order::seed(3, "apac", "JPY", 1_000_000.0),
        Order::seed(4, "amer", "USD", 60_000.0),
        Order::seed(5, "emea", "EUR", 0.0),
        Order::seed(6, "apac", "USD", 20_000.0),
    ]
}

/// A dataset with only `Order` registered: the seed for both batches.
fn order_dataset() -> Dataset {
    let mut dataset = Dataset::new();
    dataset
        .register_component::<Order>()
        .expect("register Order");
    dataset
}

/// One stage of the chain: which file to load, what to call it, and the config
/// table the host injects through `host-io::get-config`.
struct Stage {
    file: &'static str,
    name: &'static str,
    language: &'static str,
    config: &'static [(&'static str, &'static str)],
}

/// The chain, in execution order: each stage reads columns the previous one
/// wrote.
const STAGES: [Stage; 6] = [
    Stage {
        file: "validate-go.wasm",
        name: "polyglot-validate-go",
        language: "Go",
        config: &[("min_amount", "0")],
    },
    Stage {
        file: "enrich-py.wasm",
        name: "polyglot-enrich-py",
        language: "Python",
        config: &[("fx_eur", "1.10"), ("fx_gbp", "1.30"), ("fx_jpy", "0.0068")],
    },
    Stage {
        file: "score-ts.wasm",
        name: "polyglot-score-ts",
        language: "TypeScript",
        config: &[("risk_threshold", "50000")],
    },
    Stage {
        file: "fee-kt.wasm",
        name: "polyglot-fee-kt",
        language: "Kotlin",
        config: &[
            ("fee_emea", "0.012"),
            ("fee_apac", "0.008"),
            ("fee_amer", "0.010"),
        ],
    },
    Stage {
        file: "tier-cs.wasm",
        name: "polyglot-tier-cs",
        language: "C#",
        config: &[("review_score", "0.2")],
    },
    Stage {
        file: "settle-rs.wasm",
        name: "polyglot-settle-rs",
        language: "Rust",
        config: &[],
    },
];

fn load_stage(engine: &WasmEngine, stage: &Stage) -> Result<WasmPipelineRuntime, String> {
    let path = build_dir().join(stage.file);
    let bytes = fs::read(&path).map_err(|e| {
        format!(
            "{}: {e}\n  run `cargo xtask polyglot` first",
            path.display()
        )
    })?;
    let config: HashMap<String, String> = stage
        .config
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    WasmPipelineRuntime::from_bytes(engine.clone(), stage.name, &bytes, config, EPOCH_TICKS)
        .map_err(|e| format!("{}: {e}", path.display()))
}

fn print_table(dataset: &Dataset) -> Result<(), Box<dyn std::error::Error>> {
    let orders = dataset.view::<Order>()?;
    let id = orders.i64("id")?;
    let region = orders.str("region")?;
    let currency = orders.str("currency")?;
    let amount = orders.f64("amount")?;
    let valid = orders.bool("valid")?;
    let usd = orders.f64("usd_amount")?;
    let display = orders.str("usd_amount_display")?;
    let risk = orders.f64("risk_score")?;
    let flagged = orders.bool("flagged")?;
    let fee = orders.f64("fee")?;
    let tier = orders.i64("review_tier")?;
    let settlement = orders.str("settlement")?;

    println!(
        "  {:>2}  {:<6} {:<4} {:>11}  {:<5} {:>11} {:<14} {:>8}  {:<7} {:>9} {:>4}  {:<9}",
        "id",
        "region",
        "cur",
        "amount",
        "valid",
        "usd_amount",
        "display",
        "risk",
        "flagged",
        "fee",
        "tier",
        "settlement"
    );
    for i in 0..orders.len() {
        println!(
            "  {:>2}  {:<6} {:<4} {:>11.2}  {:<5} {:>11.2} {:<14} {:>8.4}  {:<7} {:>9.2} {:>4}  {:<9}",
            id.value(i),
            region.value(i),
            currency.value(i),
            amount.value(i),
            valid.value(i),
            usd.value(i),
            display.value(i),
            risk.value(i),
            flagged.value(i),
            fee.value(i),
            tier.value(i),
            settlement.value(i),
        );
    }
    Ok(())
}

/// Read the ledger totals out of a `run-result.checkpoint` blob. The blob is a
/// standalone single-component Arrow IPC stream written by `saci-processor`'s
/// `Stateful::capture`, so the host decodes it with the same `Dataset::read_ipc`
/// it uses for the data plane.
fn ledger_totals(blob: &[u8]) -> Result<(i64, f64), Box<dyn std::error::Error>> {
    let state = Dataset::read_ipc(&mut &blob[..])?;
    let batch = state
        .batch_for("Ledger")
        .ok_or("checkpoint blob has no Ledger component")?;
    let count = batch
        .column_by_name("settled_count")
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .map(|a| a.value(0))
        })
        .ok_or("Ledger.settled_count missing or not Int64")?;
    let usd = batch
        .column_by_name("settled_usd")
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .map(|a| a.value(0))
        })
        .ok_or("Ledger.settled_usd missing or not Float64")?;
    Ok((count, usd))
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("build dir:        {}", build_dir().display());

    let engine = WasmEngine::new()?;
    let mut runtimes: Vec<(&Stage, WasmPipelineRuntime)> = Vec::with_capacity(STAGES.len());
    for stage in &STAGES {
        match load_stage(&engine, stage) {
            Ok(runtime) => runtimes.push((stage, runtime)),
            Err(msg) => {
                eprintln!("error: cannot load {} stage: {msg}", stage.language);
                std::process::exit(1);
            }
        }
    }

    println!("── describe() ──");
    let mut drift = false;
    // No canonical fingerprint exists anymore: every stage derives its schema
    // from its own native declaration. What must hold is that all six agree
    // with each other — independently-authored schemas that describe the same
    // twelve-field `Order` produce the same FNV-1a value.
    let fingerprints: Vec<String> = runtimes
        .iter()
        .map(|(_, runtime)| runtime.describe().map(|d| d.schema_fingerprint))
        .collect::<Result<_, _>>()?;
    for ((stage, runtime), fingerprint) in runtimes.iter().zip(&fingerprints) {
        let d = runtime.describe()?;
        let components = runtime.declared_components();
        println!(
            "  {:<10} name={:<22} version={:<7} stateful={:<5} fingerprint={} components={:?}",
            stage.language, d.name, d.version, d.stateful, d.schema_fingerprint, components
        );
        if fingerprint != &fingerprints[0] {
            eprintln!(
                "  FINGERPRINT DRIFT: {} reports {} but {} reports {}",
                stage.name, fingerprint, STAGES[0].name, fingerprints[0]
            );
            drift = true;
        }
        if components != ["Order"] {
            eprintln!(
                "  COMPONENT DRIFT: {} declares {:?}, expected [\"Order\"]",
                stage.name, components
            );
            drift = true;
        }
    }
    if drift {
        eprintln!("\nA stage disagrees with the others on the Order schema.");
        std::process::exit(1);
    }

    let mut dataset = order_dataset();
    dataset.append::<Order>(&fixture_rows())?;

    println!("\n── batch 1 ──");
    let mut checkpoint: Option<Vec<u8>> = None;
    for (stage, runtime) in &runtimes {
        let produced = runtime.run_on_with_state(&mut dataset, None).await?;
        match (&produced, stage.config.is_empty()) {
            (Some(blob), _) => {
                println!(
                    "  {:<10} {} → checkpoint {} bytes",
                    stage.language,
                    stage.name,
                    blob.len()
                );
                checkpoint = produced;
            }
            (None, _) => println!("  {:<10} {} → stateless", stage.language, stage.name),
        }
    }
    print_table(&dataset)?;

    let checkpoint = checkpoint.ok_or("no stage returned a checkpoint")?;
    let (count1, usd1) = ledger_totals(&checkpoint)?;
    println!("  ledger after batch 1: settled_count = {count1}, settled_usd = {usd1:.2}");

    println!("\n── batch 2 (checkpoint from batch 1 fed back as `prior`) ──");
    let mut dataset2 = order_dataset();
    dataset2.append::<Order>(&fixture_rows())?;

    let mut checkpoint2: Option<Vec<u8>> = None;
    for (stage, runtime) in &runtimes {
        // Only the stateful stage gets a `prior`; the others return `None`
        // regardless of what they are handed.
        let prior = if stage.name == "polyglot-settle-rs" {
            Some(checkpoint.as_slice())
        } else {
            None
        };
        let produced = runtime.run_on_with_state(&mut dataset2, prior).await?;
        if produced.is_some() {
            checkpoint2 = produced;
        }
    }
    print_table(&dataset2)?;

    let checkpoint2 = checkpoint2.ok_or("no stage returned a checkpoint on batch 2")?;
    let (count2, usd2) = ledger_totals(&checkpoint2)?;
    println!(
        "  ledger after batch 2: settled_count = {count2}, settled_usd = {usd2:.2} \
         (checkpoint {} bytes)",
        checkpoint2.len()
    );

    if count2 != count1 * 2 || (usd2 - usd1 * 2.0).abs() > EPS {
        eprintln!(
            "error: the stateful stage did not accumulate across the batch boundary \
             (batch 1: {count1}/{usd1:.2}, batch 2: {count2}/{usd2:.2})"
        );
        std::process::exit(1);
    }

    println!(
        "\nOK: six languages, one WIT world. `valid` came from Go, \
         `usd_amount`/`usd_amount_display` from Python, `risk_score`/`flagged` from \
         TypeScript, `fee` from Kotlin, `review_tier` from C#, `settlement` from Rust."
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    run().await
}
