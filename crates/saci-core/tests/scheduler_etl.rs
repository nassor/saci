// Runs the full 4-system ETL pipeline and asserts on data correctness.

use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::Float64Array;
use async_trait::async_trait;

use saci_core::SaciError;
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{System, SystemMeta, system_fn};

mod support;
use support::{FxRates, Report, Transaction};

struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").write_component("Transaction")
    }
    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        let transactions = support::seed_transactions();
        pipeline.append::<Transaction>(&transactions)?;
        Ok(())
    }
}

struct ValidateSystem;

#[async_trait]
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT)
            .writes(Transaction::VALID)
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
        Ok(())
    }
}

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
        Ok(())
    }
}

fn make_report_system() -> impl System {
    system_fn(
        SystemMeta::new("report")
            .read_component("Transaction")
            .write_resource::<Report>(),
        |data| {
            let (n, valid_count, total_usd) = {
                let txns = data.view::<Transaction>()?;
                let n = txns.len();
                let valid_col = txns.bool(Transaction::VALID)?;
                let usd_col = txns.f64(Transaction::USD_AMOUNT)?;
                let mut valid_count = 0usize;
                let mut total_usd = 0.0f64;
                for i in 0..n {
                    if valid_col.value(i) {
                        valid_count += 1;
                        total_usd += usd_col.value(i);
                    }
                }
                (n, valid_count, total_usd)
            };
            let rejected_count = n - valid_count;
            data.insert_resource(Report {
                total_rows: n,
                valid_count,
                rejected_count,
                total_usd,
            });
            Ok(())
        },
    )
}

#[tokio::test]
async fn test_scheduler_etl_pipeline_runs_and_produces_correct_report() {
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

    pipeline.run().await.unwrap();

    let report = pipeline
        .data()
        .get_resource::<Report>()
        .expect("Report resource missing after pipeline run");

    // 9 rows total; id 1004 (amount=-50) and id 1008 (amount=0) are rejected.
    assert_eq!(report.total_rows, 9);
    assert_eq!(report.valid_count, 7);
    assert_eq!(report.rejected_count, 2);
    assert!(report.total_usd > 0.0, "total USD should be positive");

    // Validate and enrich touch disjoint fields, so they share a stage.
    let stages = pipeline.stages().unwrap_or_default();
    assert_eq!(
        stages.len(),
        3,
        "expected 3 stages: ingest / validate+enrich / report"
    );
    assert_eq!(
        stages[1].len(),
        2,
        "stage 1 should hold both validate and enrich"
    );
}
