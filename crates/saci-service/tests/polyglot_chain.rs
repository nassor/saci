//! Drives the six-language processor chain in `examples/polyglot/` through the real
//! host (`WasmPipelineRuntime`).
//!
//! Six WebAssembly components (Go, Python, TypeScript, Kotlin, C#, Rust) export
//! the same `saci:pipeline@0.3.0` world, and each writes a different column of
//! the same `Order` component. Running them in order and asserting every stage's
//! exact values pins a codec failure to a named column instead of a byte diff.
//! Five of the six processors share one Arrow codec, carried inside each
//! language's `saci-sdk` package and standard library only; these assertions are
//! the only automated check that they still agree with arrow-rs.
//!
//! # Soft skip
//!
//! Building the components needs Go, Python, Node, Kotlin and .NET toolchains,
//! and the default `test` CI job installs none of the five non-Rust ones. A
//! missing `examples/polyglot/build/` therefore prints `SKIP:` and passes. The
//! dedicated `Polyglot Processors` CI job installs all six toolchains and runs
//! `cargo xtask polyglot` first.
//!
//! ```bash
//! cargo xtask polyglot
//! cargo test -p saci-service --features wasm --test polyglot_chain -- --nocapture
//! ```

#![cfg(feature = "wasm")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use saci_core::runtime::PipelineRuntime;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::wasm::{WasmEngine, WasmPipelineRuntime};

/// Absolute tolerance for float comparisons: `100.0 * 1.10` is not exactly
/// `110.0` in f64, and the value passes through four more processor runtimes after
/// the Python stage computes it.
const EPS: f64 = 1e-6;

/// Rows in the fixture every stage is a pure column rewrite over, so the count
/// is invariant across the chain.
const ROWS: usize = 6;

/// 100 epoch ticks of 100 ms: the same 10 s per-call budget the service loader
/// grants. The Python processor re-initialises CPython on every `run-batch` because
/// the host builds a fresh `Store` per call, so the budget is tighter than it looks.
const EPOCH_TICKS: u64 = 100;

/// `(file, runtime name, config)` for each stage, in execution order.
type StageSpec = (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
);

const STAGES: [StageSpec; 6] = [
    (
        "validate-go.wasm",
        "polyglot-validate-go",
        &[("min_amount", "0")],
    ),
    (
        "enrich-py.wasm",
        "polyglot-enrich-py",
        &[("fx_eur", "1.10"), ("fx_gbp", "1.30"), ("fx_jpy", "0.0068")],
    ),
    (
        "score-ts.wasm",
        "polyglot-score-ts",
        &[("risk_threshold", "50000")],
    ),
    (
        "fee-kt.wasm",
        "polyglot-fee-kt",
        &[
            ("fee_emea", "0.012"),
            ("fee_apac", "0.008"),
            ("fee_amer", "0.010"),
        ],
    ),
    (
        "tier-cs.wasm",
        "polyglot-tier-cs",
        &[("review_score", "0.2")],
    ),
    ("settle-rs.wasm", "polyglot-settle-rs", &[]),
];

/// The one stateful stage. Only it may return a `run-result.checkpoint`.
const STATEFUL_STAGE: &str = "polyglot-settle-rs";

/// Rows that reach `SETTLED` in one pass over the fixture: ids 1 and 3.
const BATCH_SETTLED_COUNT: i64 = 2;

/// Net USD those rows contribute, `usd_amount - fee` summed: id 1 gives
/// `110.0 - 1.32 = 108.68`, id 3 gives `6800.0 - 54.4 = 6745.6`.
const BATCH_SETTLED_USD: f64 = 6_854.28;

/// One order, in the shape every polyglot stage reads and writes. Field order
/// is load-bearing: it feeds the schema fingerprint and the buffer walk the
/// SDK codecs perform. This is the test's own copy of the row type
/// (the shared `saci_polyglot_order` crate no longer exists); the fingerprint
/// agreement check below is what pins it to the six stages' schemas.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Order {
    id: i64,
    region: String,
    currency: String,
    amount: f64,
    valid: bool,
    usd_amount: f64,
    usd_amount_display: String,
    risk_score: f64,
    flagged: bool,
    fee: f64,
    review_tier: i64,
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

/// Resolves the build directory the way the driver example does, so
/// `SACI_POLYGLOT_BUILD_DIR` redirects both at a scratch tree.
fn build_dir() -> PathBuf {
    match std::env::var_os("SACI_POLYGLOT_BUILD_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root above crates/saci-service")
            .join("examples/polyglot/build"),
    }
}

fn order_dataset() -> Dataset {
    let mut dataset = Dataset::new();
    dataset
        .register_component::<Order>()
        .expect("register Order");
    dataset
        .append::<Order>(&fixture_rows())
        .expect("append fixture rows");
    dataset
}

fn load_stages() -> Option<Vec<(&'static str, WasmPipelineRuntime)>> {
    let dir = build_dir();
    let missing: Vec<&str> = STAGES
        .iter()
        .map(|(file, _, _)| *file)
        .filter(|file| !dir.join(file).exists())
        .collect();
    if !missing.is_empty() {
        println!(
            "SKIP: polyglot components not built (run cargo xtask polyglot) — \
             the default CI `test` job installs none of the five non-Rust toolchains — \
             missing {missing:?} under {}",
            dir.display()
        );
        return None;
    }

    let engine = WasmEngine::new().expect("WasmEngine init");
    Some(
        STAGES
            .iter()
            .map(|(file, name, config)| {
                let bytes =
                    std::fs::read(dir.join(file)).unwrap_or_else(|e| panic!("read {file}: {e}"));
                let config: HashMap<String, String> = config
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect();
                let runtime = WasmPipelineRuntime::from_bytes(
                    engine.clone(),
                    *name,
                    &bytes,
                    config,
                    EPOCH_TICKS,
                )
                .unwrap_or_else(|e| panic!("compile {file}: {e}"));
                (*name, runtime)
            })
            .collect(),
    )
}

/// Read the ledger the Rust stage accumulated out of a checkpoint blob.
fn ledger_totals(blob: &[u8]) -> (i64, f64) {
    let state = Dataset::read_ipc(&mut &blob[..]).expect("decode checkpoint blob");
    let batch = state
        .batch_for("Ledger")
        .expect("checkpoint carries a Ledger component");
    let count = batch
        .column_by_name("settled_count")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::Int64Array>())
        .expect("Ledger.settled_count is Int64")
        .value(0);
    let usd = batch
        .column_by_name("settled_usd")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::Float64Array>())
        .expect("Ledger.settled_usd is Float64")
        .value(0);
    (count, usd)
}

fn assert_floats(label: &str, actual: &[f64], expected: &[f64]) {
    assert_eq!(actual.len(), expected.len(), "{label}: row count");
    for (i, (a, e)) in actual.iter().zip(expected).enumerate() {
        assert!(
            (a - e).abs() < EPS,
            "{label}[{i}]: expected {e}, got {a} (tolerance {EPS})"
        );
    }
}

/// Assert every column of the chain's output. `valid` came from Go,
/// `usd_amount`/`usd_amount_display` from Python, `risk_score` and `flagged`
/// from TypeScript, `fee` from Kotlin, `review_tier` from C#, and `settlement`
/// from Rust.
fn assert_chain_output(dataset: &Dataset) {
    let orders = dataset.view::<Order>().expect("Order view");
    assert_eq!(orders.len(), ROWS, "row count survived the chain");

    let ids: Vec<i64> = (0..ROWS)
        .map(|i| orders.i64("id").unwrap().value(i))
        .collect();
    assert_eq!(ids, [1, 2, 3, 4, 5, 6], "id must pass through untouched");

    let regions: Vec<&str> = (0..ROWS)
        .map(|i| orders.str("region").unwrap().value(i))
        .collect();
    assert_eq!(
        regions,
        ["emea", "emea", "apac", "amer", "emea", "apac"],
        "region must pass through untouched"
    );

    let currencies: Vec<&str> = (0..ROWS)
        .map(|i| orders.str("currency").unwrap().value(i))
        .collect();
    assert_eq!(currencies, ["EUR", "GBP", "JPY", "USD", "EUR", "USD"]);

    let amounts: Vec<f64> = (0..ROWS)
        .map(|i| orders.f64("amount").unwrap().value(i))
        .collect();
    assert_floats(
        "amount",
        &amounts,
        &[100.0, -5.0, 1_000_000.0, 60_000.0, 0.0, 20_000.0],
    );

    // Go: valid = amount > min_amount (0). Row 5 has amount == 0, which is not
    // greater than the threshold.
    let valid: Vec<bool> = (0..ROWS)
        .map(|i| orders.bool("valid").unwrap().value(i))
        .collect();
    assert_eq!(
        valid,
        [true, false, true, true, false, true],
        "`valid` is written by the Go processor"
    );

    // Python: usd_amount = valid ? amount * fx(currency) : 0. USD is 1.0.
    let usd: Vec<f64> = (0..ROWS)
        .map(|i| orders.f64("usd_amount").unwrap().value(i))
        .collect();
    assert_floats(
        "usd_amount (Python processor)",
        &usd,
        &[110.0, 0.0, 6_800.0, 60_000.0, 0.0, 20_000.0],
    );

    // Python: usd_amount_display = valid ? "<usd_amount:.2f> USD" : "".
    let display: Vec<&str> = (0..ROWS)
        .map(|i| orders.str("usd_amount_display").unwrap().value(i))
        .collect();
    assert_eq!(
        display,
        [
            "110.00 USD",
            "",
            "6800.00 USD",
            "60000.00 USD",
            "",
            "20000.00 USD",
        ],
        "`usd_amount_display` is written by the Python processor"
    );

    // TypeScript: risk_score = usd_amount / risk_threshold, flagged = risk >= 1.
    let risk: Vec<f64> = (0..ROWS)
        .map(|i| orders.f64("risk_score").unwrap().value(i))
        .collect();
    assert_floats(
        "risk_score (TypeScript processor)",
        &risk,
        &[0.0022, 0.0, 0.136, 1.2, 0.0, 0.4],
    );

    let flagged: Vec<bool> = (0..ROWS)
        .map(|i| orders.bool("flagged").unwrap().value(i))
        .collect();
    assert_eq!(
        flagged,
        [false, false, false, true, false, false],
        "`flagged` is written by the TypeScript processor"
    );

    // Kotlin: fee = valid ? usd_amount * rate(region) : 0. Rates are 0.012
    // emea, 0.008 apac, 0.010 amer.
    let fee: Vec<f64> = (0..ROWS)
        .map(|i| orders.f64("fee").unwrap().value(i))
        .collect();
    assert_floats(
        "fee (Kotlin processor)",
        &fee,
        &[1.32, 0.0, 54.4, 600.0, 0.0, 160.0],
    );

    // C#: review_tier = flagged ? 2 : (risk_score >= review_score (0.2) ? 1 : 0).
    let tier: Vec<i64> = (0..ROWS)
        .map(|i| orders.i64("review_tier").unwrap().value(i))
        .collect();
    assert_eq!(
        tier,
        [0, 0, 0, 2, 0, 1],
        "`review_tier` is written by the C# processor"
    );

    // Rust: !valid -> REJECTED, tier 2 -> HOLD, tier 1 -> REVIEW, else SETTLED.
    let settlement: Vec<&str> = (0..ROWS)
        .map(|i| orders.str("settlement").unwrap().value(i))
        .collect();
    assert_eq!(
        settlement,
        [
            "SETTLED", "REJECTED", "SETTLED", "HOLD", "REJECTED", "REVIEW"
        ],
        "`settlement` is written by the Rust processor"
    );
}

/// The whole chain in one test: the six `describe()` contracts, the twelve
/// output columns, and the ledger accumulating over two batches. Splitting it
/// would recompile and re-instantiate six components per assertion group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn six_language_chain_produces_exact_values() {
    let Some(stages) = load_stages() else {
        return;
    };

    // There is no canonical fingerprint anymore: every stage derives its schema
    // from its own native declaration. What must hold is that all six agree
    // with each other.
    let fingerprints: Vec<String> = stages
        .iter()
        .map(|(name, runtime)| {
            let descriptor = runtime.describe().expect("describe()");
            assert_eq!(
                runtime.declared_components(),
                ["Order"],
                "{name} must declare exactly the Order component"
            );
            assert_eq!(
                descriptor.stateful,
                *name == STATEFUL_STAGE,
                "{name}: only {STATEFUL_STAGE} keeps state across batches"
            );
            descriptor.schema_fingerprint
        })
        .collect();
    for ((name, _), fingerprint) in stages.iter().zip(&fingerprints).skip(1) {
        assert_eq!(
            fingerprint, &fingerprints[0],
            "{name} reports a schema fingerprint that disagrees with the other \
             five stages — the six independently-authored Order schemas must be \
             structurally identical"
        );
    }

    let mut dataset = order_dataset();
    let mut checkpoint: Option<Vec<u8>> = None;
    for (name, runtime) in &stages {
        let produced = runtime
            .run_on_with_state(&mut dataset, None)
            .await
            .unwrap_or_else(|e| panic!("{name} run-batch: {e}"));
        if *name == STATEFUL_STAGE {
            assert!(
                produced.is_some(),
                "{name} is stateful and must return a checkpoint"
            );
            checkpoint = produced;
        } else {
            assert!(
                produced.is_none(),
                "{name} is stateless and must return no checkpoint"
            );
        }
    }
    assert_chain_output(&dataset);

    let checkpoint = checkpoint.expect("the stateful stage returned a checkpoint");
    let (count1, usd1) = ledger_totals(&checkpoint);
    assert_eq!(count1, BATCH_SETTLED_COUNT, "two rows settled in batch 1");
    assert!(
        (usd1 - BATCH_SETTLED_USD).abs() < EPS,
        "batch 1 settled USD: expected {BATCH_SETTLED_USD}, got {usd1}"
    );

    // The host builds a fresh wasmtime Store per call, so the checkpoint blob is
    // the only channel for processor state: doubled totals prove it round-tripped.
    let mut dataset2 = order_dataset();
    let mut checkpoint2: Option<Vec<u8>> = None;
    for (name, runtime) in &stages {
        let prior = if *name == STATEFUL_STAGE {
            Some(checkpoint.as_slice())
        } else {
            None
        };
        let produced = runtime
            .run_on_with_state(&mut dataset2, prior)
            .await
            .unwrap_or_else(|e| panic!("{name} run-batch (2): {e}"));
        if *name == STATEFUL_STAGE {
            checkpoint2 = produced;
        }
    }
    assert_chain_output(&dataset2);

    let (count2, usd2) = ledger_totals(&checkpoint2.expect("batch 2 checkpoint"));
    assert_eq!(
        count2,
        2 * BATCH_SETTLED_COUNT,
        "the ledger accumulated across the batch boundary"
    );
    let expected_usd2 = 2.0 * BATCH_SETTLED_USD;
    assert!(
        (usd2 - expected_usd2).abs() < EPS,
        "batch 2 settled USD: expected {expected_usd2}, got {usd2}"
    );
}
