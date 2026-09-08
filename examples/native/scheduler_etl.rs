//! Arrow-backed ETL pipeline: four systems over columnar transaction data.
//!
//! Systems declare access at field granularity, so two writers of the same
//! component share a stage when their field sets are disjoint. The scheduler
//! derives this layout from the declarations alone:
//!
//! ```text
//! Stage 0:  [IngestSystem]                  writes every Transaction field
//! Stage 1:  [ValidateSystem, EnrichSystem]  write "valid" and "usd_amount"
//! Stage 2:  [ReportSystem]                  reads both
//! ```
//!
//! ```bash
//! cargo run --example scheduler_etl
//! ```

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use saci_service::SaciError;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::pipeline::Pipeline;
use saci_service::system::{FieldRef, System, SystemMeta, system_fn};
use serde::{Deserialize, Serialize};

/// A financial transaction in columnar form. `amount` is in `currency`;
/// ValidateSystem sets `valid` when the amount is positive, and EnrichSystem
/// fills `usd_amount` from the FX rate.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Transaction {
    id: u64,
    amount: f64,
    currency: String,
    valid: bool,
    usd_amount: f64,
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

/// FX rates used by EnrichSystem, USD as base.
struct FxRates {
    eur: f64,
    gbp: f64,
    jpy: f64,
    cad: f64,
}

impl FxRates {
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

/// Summary written by ReportSystem.
struct Report {
    total_rows: usize,
    valid_count: usize,
    rejected_count: usize,
    total_usd: f64,
}

/// Loads seed transaction data. Writes every field of Transaction, so it lands
/// in stage 0 and every reader follows it.
struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").write_component("Transaction")
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let transactions = vec![
            Transaction {
                id: 1001,
                amount: 1500.00,
                currency: "USD".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1002,
                amount: 2300.50,
                currency: "EUR".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1003,
                amount: 750.00,
                currency: "GBP".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1004,
                amount: -50.00,
                currency: "USD".into(),
                valid: false,
                usd_amount: 0.0,
            }, // negative → rejected
            Transaction {
                id: 1005,
                amount: 5000.00,
                currency: "JPY".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1006,
                amount: 320.75,
                currency: "EUR".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1007,
                amount: 1200.00,
                currency: "GBP".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1008,
                amount: 0.00,
                currency: "USD".into(),
                valid: false,
                usd_amount: 0.0,
            }, // zero → rejected
            Transaction {
                id: 1009,
                amount: 680.00,
                currency: "CAD".into(),
                valid: false,
                usd_amount: 0.0,
            },
        ];

        pipeline.append::<Transaction>(&transactions)?;
        println!("[ingest]    loaded {} transactions", transactions.len());
        Ok(())
    }
}

/// Marks each row valid when its amount is positive. Writes only `valid`, which
/// is disjoint from EnrichSystem's `usd_amount`, so both land in stage 1.
struct ValidateSystem;

#[async_trait]
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT) // decides validity
            .writes(Transaction::VALID) // the only field it touches
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let batch = pipeline
            .columns::<Transaction>()
            .ok_or_else(|| SaciError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let valid_flags: Vec<bool>;
        {
            let txns = pipeline.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            n = txns.len();
            valid_flags = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        }
        let valid_count = valid_flags.iter().filter(|&&v| v).count();

        // Rebuild every column, swapping in the new `valid` array.
        let new_valid = Arc::new(BooleanArray::from(valid_flags));
        let schema = batch.schema();
        let valid_idx = schema.index_of("valid").unwrap();

        let new_columns: Vec<Arc<dyn arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == valid_idx {
                    new_valid.clone() as Arc<dyn arrow_array::Array>
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();

        let new_batch = arrow_array::RecordBatch::try_new(schema, new_columns)
            .map_err(|e| SaciError::generic(format!("RecordBatch rebuild error: {e}")))?;

        pipeline.replace_batch::<Transaction>(new_batch)?;

        println!(
            "[validate]  {} valid, {} rejected",
            valid_count,
            n - valid_count
        );
        Ok(())
    }
}

/// Converts amounts to USD using FX rates. Writes only `usd_amount`, disjoint
/// from ValidateSystem's `valid`, so the two share stage 1.
struct EnrichSystem;

#[async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich")
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .writes(Transaction::USD_AMOUNT)
            .read_resource::<FxRates>()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let rates = pipeline
            .get_resource::<FxRates>()
            .ok_or_else(|| SaciError::generic("FxRates resource not found"))?;

        let batch = pipeline
            .columns::<Transaction>()
            .ok_or_else(|| SaciError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let usd_amounts: Vec<f64>;
        {
            let txns = pipeline.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            let currency_col = txns.str(Transaction::CURRENCY)?;
            n = txns.len();
            usd_amounts = (0..n)
                .map(|i| {
                    let rate = rates.rate_for(currency_col.value(i));
                    amount_col.value(i) * rate
                })
                .collect();
        }

        let new_usd = Arc::new(Float64Array::from(usd_amounts));
        let schema = batch.schema();
        let usd_idx = schema.index_of("usd_amount").unwrap();

        let new_columns: Vec<Arc<dyn arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == usd_idx {
                    new_usd.clone() as Arc<dyn arrow_array::Array>
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();

        let new_batch = arrow_array::RecordBatch::try_new(schema, new_columns)
            .map_err(|e| SaciError::generic(format!("RecordBatch rebuild error: {e}")))?;

        pipeline.replace_batch::<Transaction>(new_batch)?;

        println!("[enrich]    converted {} rows to USD", n);
        Ok(())
    }
}

/// Prints a summary of the validated, enriched rows. Reading `valid` and
/// `usd_amount` puts it after stage 1.
fn make_report_system() -> impl System {
    system_fn(
        SystemMeta::new("report")
            .reads(Transaction::ID)
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .reads(Transaction::VALID)
            .reads(Transaction::USD_AMOUNT)
            .write_resource::<Report>(),
        |data| {
            let n;
            let mut valid_count = 0usize;
            let mut total_usd = 0.0f64;

            println!();
            println!("╔═══════════════════════════════════════════════════════╗");
            println!("║       ARROW ETL PIPELINE — TRANSACTION REPORT        ║");
            println!("╠═══════════════════════════════════════════════════════╣");

            {
                let txns = data.view::<Transaction>()?;
                n = txns.len();

                let id_col = txns.u64(Transaction::ID)?;
                let amount_col = txns.f64(Transaction::AMOUNT)?;
                let currency_col = txns.str(Transaction::CURRENCY)?;
                let valid_col = txns.bool(Transaction::VALID)?;
                let usd_col = txns.f64(Transaction::USD_AMOUNT)?;

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
                            println!(
                                "║  #{:<5}  {:>10.2} USD                                ║",
                                id, usd
                            );
                        } else {
                            println!(
                                "║  #{:<5}  {:>10.2} {} → {:>10.2} USD           ║",
                                id, amount, currency, usd
                            );
                        }
                    } else {
                        println!(
                            "║  #{:<5}  {:>10.2} {}  REJECTED                     ║",
                            id, amount, currency
                        );
                    }
                }
            } // txns borrow released here

            let rejected = n - valid_count;
            println!("╠═══════════════════════════════════════════════════════╣");
            println!(
                "║  Total rows:   {:>4}                                  ║",
                n
            );
            println!(
                "║  Valid:        {:>4}                                  ║",
                valid_count
            );
            println!(
                "║  Rejected:     {:>4}                                  ║",
                rejected
            );
            println!(
                "║  Total USD:    {:>12.2}                         ║",
                total_usd
            );
            if valid_count > 0 {
                println!(
                    "║  Average USD:  {:>12.2}                         ║",
                    total_usd / valid_count as f64
                );
            }
            println!("╚═══════════════════════════════════════════════════════╝");

            data.insert_resource(Report {
                total_rows: n,
                valid_count,
                rejected_count: rejected,
                total_usd,
            });

            Ok(())
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
    let mut pipeline = Pipeline::builder("etl")
        .with::<Transaction>()
        .with_resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
            cad: 0.74,
        })
        .with_system(IngestSystem)
        .with_system(ValidateSystem)
        .with_system(EnrichSystem)
        .with_system(make_report_system())
        .build();

    println!("Starting ETL pipeline...");

    pipeline.run().await?;

    let stages = pipeline.stages().unwrap_or_default();
    println!();
    println!("Stage layout (field-level DAG):");
    for (i, stage) in stages.iter().enumerate() {
        println!("  Stage {i}: {stage:?}");
    }
    println!("  (ValidateSystem and EnrichSystem share stage 1");
    println!("   because they write disjoint fields of Transaction)");

    let report = pipeline
        .data()
        .get_resource::<Report>()
        .ok_or_else(|| SaciError::generic("Report resource missing after pipeline run"))?;
    println!();
    println!(
        "Pipeline complete: {}/{} valid, {} rejected, ${:.2} total USD",
        report.valid_count, report.total_rows, report.rejected_count, report.total_usd
    );

    Ok(())
}
