//! Arrow-backed ETL pipeline using `ParallelSystem` for concurrent stage execution.
//!
//! `ValidateSystem` and `EnrichSystem` implement `ParallelSystem` and write
//! disjoint fields, so the field-level DAG puts them in one stage and runs them
//! concurrently through `join_all`:
//!
//! ```text
//! Stage 0:  [IngestSystem]                  sequential, writes every field
//! Stage 1:  [ValidateSystem, EnrichSystem]  concurrent, "valid", "usd_amount"
//! Stage 2:  [ReportSystem]                  sequential, reads every field
//! ```
//!
//! ```bash
//! cargo run --example scheduler_etl_parallel
//! ```

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use saci_service::SaciError;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::pipeline::Pipeline;
use saci_service::system::{FieldRef, ParallelSystem, System, SystemMeta, WriteSet, system_fn};
use serde::{Deserialize, Serialize};

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

struct Report {
    total_rows: usize,
    valid_count: usize,
    rejected_count: usize,
    total_usd: f64,
}

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
            },
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
            },
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

struct ValidateSystem;

#[async_trait]
impl ParallelSystem for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT)
            .writes(Transaction::VALID)
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let txns = pipeline.view::<Transaction>()?;
        let amount_col = txns.f64(Transaction::AMOUNT)?;
        let n = txns.len();
        let valid_flags: Vec<bool> = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        let valid_count = valid_flags.iter().filter(|&&v| v).count();

        let new_valid: Arc<dyn arrow_array::Array> = Arc::new(BooleanArray::from(valid_flags));

        println!(
            "[validate]  {} valid, {} rejected (parallel)",
            valid_count,
            n - valid_count
        );

        Ok(WriteSet::new().put("Transaction", "valid", new_valid))
    }
}

struct EnrichSystem;

#[async_trait]
impl ParallelSystem for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich")
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .writes(Transaction::USD_AMOUNT)
            .read_resource::<FxRates>()
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, SaciError> {
        let rates = pipeline
            .get_resource::<FxRates>()
            .ok_or_else(|| SaciError::generic("FxRates resource not found"))?;

        let txns = pipeline.view::<Transaction>()?;
        let amount_col = txns.f64(Transaction::AMOUNT)?;
        let currency_col = txns.str(Transaction::CURRENCY)?;
        let n = txns.len();
        let usd_amounts: Vec<f64> = (0..n)
            .map(|i| {
                let rate = rates.rate_for(currency_col.value(i));
                amount_col.value(i) * rate
            })
            .collect();

        let new_usd: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(usd_amounts));

        println!("[enrich]    converted {} rows to USD (parallel)", n);

        Ok(WriteSet::new().put("Transaction", "usd_amount", new_usd))
    }
}

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
            println!("║    ARROW ETL PARALLEL PIPELINE — TRANSACTION REPORT  ║");
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
    let mut pipeline = Pipeline::builder("etl-parallel")
        .with::<Transaction>()
        .with_resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
            cad: 0.74,
        })
        .with_system(IngestSystem)
        .with_parallel_system(ValidateSystem)
        .with_parallel_system(EnrichSystem)
        .with_system(make_report_system())
        .build();

    println!("Starting ETL parallel pipeline...");
    println!("(ValidateSystem and EnrichSystem run concurrently in stage 1)");

    pipeline.run().await?;

    let stages = pipeline.stages().unwrap_or_default();
    println!();
    println!("Stage layout (field-level DAG):");
    for (i, stage) in stages.iter().enumerate() {
        println!("  Stage {i}: {stage:?}");
    }
    println!("  (Stage 1 systems run concurrently — disjoint field writes)");

    let report = pipeline
        .data()
        .get_resource::<Report>()
        .ok_or_else(|| SaciError::generic("Report resource missing"))?;
    println!();
    println!(
        "Pipeline complete: {}/{} valid, {} rejected, ${:.2} total USD",
        report.valid_count, report.total_rows, report.rejected_count, report.total_usd
    );

    Ok(())
}
