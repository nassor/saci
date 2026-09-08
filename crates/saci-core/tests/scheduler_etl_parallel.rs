// Verifies the ParallelSystem path with disjoint field writes in stage 1.

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array};
use async_trait::async_trait;

use saci_core::SaciError;
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{ParallelSystem, System, SystemMeta, WriteSet, system_fn};

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
        let new_valid: Arc<dyn arrow_array::Array> = Arc::new(BooleanArray::from(valid_flags));
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
        Ok(WriteSet::new().put("Transaction", "usd_amount", new_usd))
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
async fn test_scheduler_etl_parallel_pipeline_runs_and_produces_correct_report() {
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

    pipeline.run().await.unwrap();

    let report = pipeline
        .data()
        .get_resource::<Report>()
        .expect("Report resource missing after pipeline run");

    assert_eq!(report.total_rows, 9);
    assert_eq!(report.valid_count, 7);
    assert_eq!(report.rejected_count, 2);
    assert!(report.total_usd > 0.0);
}

#[tokio::test]
async fn test_scheduler_etl_parallel_stage_layout_has_concurrent_stage() {
    let mut pipeline = Pipeline::builder("etl-parallel-stages")
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

    pipeline.run().await.unwrap();

    let stages = pipeline.stages().unwrap_or_default();
    // Stage 0: ingest; Stage 1: validate+enrich (disjoint writes); Stage 2: report
    assert_eq!(stages.len(), 3);
    assert_eq!(stages[1].len(), 2, "validate and enrich share stage 1");
}
