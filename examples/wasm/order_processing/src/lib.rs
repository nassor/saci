//! Order processing pipeline as a WebAssembly Component Model processor.
//!
//! A 2-stage field-granular DAG (Validate and Enrich in parallel, then Report)
//! running inside a WASM processor via `saci-processor`. It mirrors
//! `examples/native/scheduler_etl.rs`, with two differences the
//! WASM model forces:
//!
//! - No ingest system. Data arrives in the host's `run-batch` Arrow IPC
//!   payload, one batch per partition, from whatever source `saci-service`'s
//!   the config file configures.
//! - `FxRates` lives on the `EnrichSystem` struct rather than on the
//!   `Dataset`. `Dataset::write_ipc` serializes registered components and the
//!   alive bitmap, not the resource map, so a dataset rebuilt from IPC has no
//!   resources and `get_resource::<FxRates>` inside `run_on` would fail. Same
//!   reason for the native `Report` resource: this port prints instead.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p order-processing-wasm --target wasm32-wasip2
//! ```
//!
//! The output component is at:
//!
//! ```text
//! target/wasm32-wasip2/release/order_processing_wasm.wasm
//! ```
//!
//! # Run via saci-service
//!
//! ```bash
//! saci-service serve --config examples/configs/standalone_wasm.kdl
//! ```
//!
//! See `README.md` in this crate for the full instructions.

#![deny(missing_docs)]

// The bindings are generated in place from `crates/saci-processor/wit`. The
// module and the `export_pipeline!` invocation below are gated on
// `target_arch = "wasm32"`: the expansion emits canonical ABI intrinsics and the
// `component-type` custom section, neither of which the host target can link.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../../crates/saci-processor/wit",
        world: "saci-pipeline",
        generate_all,
    });
}

use std::sync::Arc;

use saci_processor::arrow_array::{BooleanArray, Float64Array, RecordBatch};
use saci_processor::arrow_schema::{DataType, Field, Schema};
use saci_processor::prelude::*;

/// A financial transaction in columnar form.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Transaction {
    /// Unique transaction id.
    pub id: u64,
    /// Original amount in `currency`.
    pub amount: f64,
    /// ISO currency code ("USD", "EUR", "GBP", "JPY", "CAD").
    pub currency: String,
    /// Set to `true` by `ValidateSystem` if `amount > 0`.
    pub valid: bool,
    /// Filled by `EnrichSystem` after FX conversion.
    pub usd_amount: f64,
}

impl Component for Transaction {
    fn name() -> &'static str {
        "Transaction"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("valid", DataType::Boolean, false),
            Field::new("usd_amount", DataType::Float64, false),
        ]))
    }
}

impl Transaction {
    const ID: FieldRef<Transaction> = FieldRef::new("id");
    const AMOUNT: FieldRef<Transaction> = FieldRef::new("amount");
    const CURRENCY: FieldRef<Transaction> = FieldRef::new("currency");
    const VALID: FieldRef<Transaction> = FieldRef::new("valid");
    const USD_AMOUNT: FieldRef<Transaction> = FieldRef::new("usd_amount");
}

/// USD-base exchange rates used by `EnrichSystem`.
#[derive(Clone, Copy, Debug)]
struct FxRates {
    eur: f64,
    gbp: f64,
    jpy: f64,
    cad: f64,
}

impl FxRates {
    const DEFAULT: FxRates = FxRates {
        eur: 1.08,
        gbp: 1.27,
        jpy: 0.0067,
        cad: 0.74,
    };

    /// Read the rates from the host's `[pipeline.wasm.config]` table, falling
    /// back to [`DEFAULT`](Self::DEFAULT) per missing key. A present but
    /// unparseable value keeps the default and warns on stderr rather than
    /// trapping the processor mid-`describe`.
    #[cfg(target_arch = "wasm32")]
    fn from_config() -> Self {
        fn rate(key: &str, default: f64) -> f64 {
            match crate::saci_config_parse::<f64>(key) {
                Some(Ok(v)) => v,
                Some(Err(e)) => {
                    eprintln!("[config] {key} is not a valid f64 ({e}); using {default}");
                    default
                }
                None => default,
            }
        }

        Self {
            eur: rate("fx_eur", Self::DEFAULT.eur),
            gbp: rate("fx_gbp", Self::DEFAULT.gbp),
            jpy: rate("fx_jpy", Self::DEFAULT.jpy),
            cad: rate("fx_cad", Self::DEFAULT.cad),
        }
    }

    fn rate_for(&self, currency: &str) -> f64 {
        match currency {
            "USD" => 1.0,
            "EUR" => self.eur,
            "GBP" => self.gbp,
            "JPY" => self.jpy,
            "CAD" => self.cad,
            _ => 1.0,
        }
    }
}

/// Marks each row valid or invalid based on whether `amount > 0`.
///
/// Reads `amount`, writes `valid`. `valid` is disjoint from `usd_amount`,
/// which `EnrichSystem` writes, so the field-granular DAG puts both systems in
/// the same stage.
struct ValidateSystem;

#[saci_processor::prelude::async_trait]
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT)
            .writes(Transaction::VALID)
    }

    async fn run(&self, dataset: &mut Dataset) -> SaciResult<()> {
        let batch = dataset
            .columns::<Transaction>()
            .ok_or_else(|| SaciError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let valid_flags: Vec<bool>;
        {
            let txns = dataset.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            n = txns.len();
            valid_flags = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        }
        let valid_count = valid_flags.iter().filter(|&&v| v).count();

        let new_valid = Arc::new(BooleanArray::from(valid_flags));
        let schema = batch.schema();
        let valid_idx = schema
            .index_of("valid")
            .map_err(|e| SaciError::generic(format!("Transaction.valid missing: {e}")))?;

        let new_columns: Vec<Arc<dyn saci_processor::arrow_array::Array>> =
            (0..schema.fields().len())
                .map(|i| {
                    if i == valid_idx {
                        new_valid.clone() as Arc<dyn saci_processor::arrow_array::Array>
                    } else {
                        batch.column(i).clone()
                    }
                })
                .collect();

        let new_batch = RecordBatch::try_new(schema, new_columns)
            .map_err(|e| SaciError::generic(format!("RecordBatch rebuild error: {e}")))?;

        dataset.replace_batch::<Transaction>(new_batch)?;

        println!(
            "[validate]  {valid_count} valid, {} rejected",
            n - valid_count
        );
        Ok(())
    }
}

/// Converts amounts to USD using `FxRates`.
///
/// Reads `amount` and `currency`, writes `usd_amount`. The rates are a struct
/// field rather than a `Dataset` resource because resources do not survive the
/// host and processor Arrow IPC round-trip; see the crate docs.
struct EnrichSystem {
    rates: FxRates,
}

#[saci_processor::prelude::async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich")
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .writes(Transaction::USD_AMOUNT)
    }

    async fn run(&self, dataset: &mut Dataset) -> SaciResult<()> {
        let batch = dataset
            .columns::<Transaction>()
            .ok_or_else(|| SaciError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let usd_amounts: Vec<f64>;
        {
            let txns = dataset.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            let currency_col = txns.str(Transaction::CURRENCY)?;
            n = txns.len();
            usd_amounts = (0..n)
                .map(|i| {
                    let rate = self.rates.rate_for(currency_col.value(i));
                    amount_col.value(i) * rate
                })
                .collect();
        }

        let new_usd = Arc::new(Float64Array::from(usd_amounts));
        let schema = batch.schema();
        let usd_idx = schema
            .index_of("usd_amount")
            .map_err(|e| SaciError::generic(format!("Transaction.usd_amount missing: {e}")))?;

        let new_columns: Vec<Arc<dyn saci_processor::arrow_array::Array>> =
            (0..schema.fields().len())
                .map(|i| {
                    if i == usd_idx {
                        new_usd.clone() as Arc<dyn saci_processor::arrow_array::Array>
                    } else {
                        batch.column(i).clone()
                    }
                })
                .collect();

        let new_batch = RecordBatch::try_new(schema, new_columns)
            .map_err(|e| SaciError::generic(format!("RecordBatch rebuild error: {e}")))?;

        dataset.replace_batch::<Transaction>(new_batch)?;

        println!("[enrich]    converted {n} rows to USD");
        Ok(())
    }
}

/// Prints a per-row summary plus aggregate totals to stdout.
///
/// Created via `system_fn` for brevity. Reads the four fields the prior stage
/// produced and writes nothing: it is the terminal stage of the per-batch DAG,
/// so the printed report is its only effect.
fn make_report_system() -> impl System {
    system_fn(
        SystemMeta::new("report")
            .reads(Transaction::ID)
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .reads(Transaction::VALID)
            .reads(Transaction::USD_AMOUNT),
        |data| {
            let txns = data.view::<Transaction>()?;
            let n = txns.len();

            let id_col = txns.u64(Transaction::ID)?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            let currency_col = txns.str(Transaction::CURRENCY)?;
            let valid_col = txns.bool(Transaction::VALID)?;
            let usd_col = txns.f64(Transaction::USD_AMOUNT)?;

            let mut valid_count = 0usize;
            let mut total_usd = 0.0f64;

            println!();
            println!("[report]    ── transaction batch ──");
            for i in 0..n {
                let id = id_col.value(i);
                let amount = amount_col.value(i);
                let currency = currency_col.value(i);
                let is_valid = valid_col.value(i);
                let usd = usd_col.value(i);

                if is_valid {
                    valid_count += 1;
                    total_usd += usd;
                    if currency == "USD" {
                        println!("[report]      #{id:<6} {usd:>12.2} USD");
                    } else {
                        println!(
                            "[report]      #{id:<6} {amount:>12.2} {currency} → {usd:>12.2} USD"
                        );
                    }
                } else {
                    println!("[report]      #{id:<6} {amount:>12.2} {currency}  REJECTED");
                }
            }

            let rejected = n - valid_count;
            println!(
                "[report]    {valid_count}/{n} valid, {rejected} rejected, {total_usd:>12.2} USD total"
            );
            Ok(())
        },
    )
}

/// Build the order_processing pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export, and constructed exactly once per component instance.
///
/// FX rates come from the host's `[pipeline.wasm.config]` table via the
/// `host-io` `get-config` import (keys `fx_eur`, `fx_gbp`, `fx_jpy`,
/// `fx_cad`), each falling back to `FxRates::DEFAULT` when absent. They stay
/// a field on `EnrichSystem` because resources do not round-trip through Arrow
/// IPC.
pub fn build() -> Pipeline {
    // `saci_config_parse` is emitted into this crate by `export_pipeline!` and
    // only exists on wasm32, where `crate::bindings` exists.
    #[cfg(target_arch = "wasm32")]
    let rates = FxRates::from_config();
    #[cfg(not(target_arch = "wasm32"))]
    let rates = FxRates::DEFAULT;

    Pipeline::builder("order_processing")
        .with::<Transaction>()
        .with_system(ValidateSystem)
        .with_system(EnrichSystem { rates })
        .with_system(make_report_system())
        .build()
}

#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build);
