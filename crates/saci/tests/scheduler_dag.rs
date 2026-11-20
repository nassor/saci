#![cfg(feature = "engine")]
// Cross-pipeline DAG scheduling: order deps, data skip, priority, backpressure.

use std::sync::{Arc, Mutex};

use arrow_array::{Float64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use saci::prelude::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Transaction {
    id: u64,
    amount: f64,
    currency: String,
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
        ]))
    }
}

struct IngestSystem {
    rows: Vec<Transaction>,
}

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").writes_all::<Transaction>()
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        if !data.schemas().contains("Transaction") {
            data.register::<Transaction>()?;
        }
        data.append::<Transaction>(&self.rows)?;
        Ok(())
    }
}

struct EnrichSystem {
    runs: Arc<Mutex<usize>>,
}

#[async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich").reads_all::<Transaction>()
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        *self.runs.lock().unwrap() += 1;
        let batch = data.batch::<Transaction>()?.clone();
        let currency_col = batch
            .column(batch.schema().index_of("currency").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("currency column is StringArray");
        let amount_col = batch
            .column(batch.schema().index_of("amount").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("amount column is Float64Array");

        let usd: Float64Array = (0..batch.num_rows())
            .map(|i| {
                let amount = amount_col.value(i);
                if currency_col.value(i) == "EUR" {
                    amount * 1.1
                } else {
                    amount
                }
            })
            .collect();

        let ws = WriteSet::new().put_named("Transaction", "amount", Arc::new(usd));
        data.apply_write_set(ws)
    }
}

struct ReportSystem {
    total: Arc<Mutex<f64>>,
    runs: Arc<Mutex<usize>>,
}

#[async_trait]
impl System for ReportSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("report").reads_named("Transaction", "amount")
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        *self.runs.lock().unwrap() += 1;
        let batch = data.batch::<Transaction>()?.clone();
        let amount_col = batch
            .column(batch.schema().index_of("amount").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("amount column is Float64Array");
        let sum: f64 = amount_col.iter().flatten().sum();
        *self.total.lock().unwrap() += sum;
        Ok(())
    }
}

struct BumpSystem {
    name: &'static str,
    count: Arc<Mutex<usize>>,
}

#[async_trait]
impl System for BumpSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new(self.name)
    }
    async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
        *self.count.lock().unwrap() += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_order_dependency_chain_runs_sequentially() {
    let total = Arc::new(Mutex::new(0.0f64));
    let enrich_runs = Arc::new(Mutex::new(0usize));
    let report_runs = Arc::new(Mutex::new(0usize));

    let txns = vec![
        Transaction {
            id: 1,
            amount: 100.0,
            currency: "USD".into(),
        },
        Transaction {
            id: 2,
            amount: 50.0,
            currency: "EUR".into(),
        },
    ];

    let mut ingest = Pipeline::new("ingest");
    ingest.data_mut().register::<Transaction>().unwrap();
    let ingest = ingest.system(IngestSystem { rows: txns });

    let mut enrich = Pipeline::new("enrich");
    enrich.data_mut().register::<Transaction>().unwrap();
    let enrich = enrich.system(EnrichSystem {
        runs: Arc::clone(&enrich_runs),
    });

    let mut report = Pipeline::new("report");
    report.data_mut().register::<Transaction>().unwrap();
    let report = report.system(ReportSystem {
        total: Arc::clone(&total),
        runs: Arc::clone(&report_runs),
    });

    let mut sched = Scheduler::new()
        .pipeline(ingest)
        .pipeline_with(enrich, PipelineConfig::new().after("ingest"))
        .pipeline_with(report, PipelineConfig::new().after("enrich"));

    sched.tick().await.unwrap();

    assert_eq!(*enrich_runs.lock().unwrap(), 1);
    assert_eq!(*report_runs.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_data_dependency_skips_report_when_upstream_produces_zero_rows() {
    let total = Arc::new(Mutex::new(0.0f64));
    let report_runs = Arc::new(Mutex::new(0usize));

    let empty_enrich = Pipeline::new("empty_enrich");

    let mut report = Pipeline::new("report_data");
    report.data_mut().register::<Transaction>().unwrap();
    let report = report.system(ReportSystem {
        total: Arc::clone(&total),
        runs: Arc::clone(&report_runs),
    });

    let mut sched = Scheduler::new()
        .pipeline(empty_enrich)
        .pipeline_with(report, PipelineConfig::new().after_data("empty_enrich"));

    sched.tick().await.unwrap();

    assert_eq!(
        *report_runs.lock().unwrap(),
        0,
        "report should be skipped when upstream is empty"
    );
}

#[tokio::test]
async fn test_priority_ordering_runs_both_pipelines() {
    let count_high = Arc::new(Mutex::new(0usize));
    let count_low = Arc::new(Mutex::new(0usize));

    let mut sched = Scheduler::new()
        .pipeline_with(
            Pipeline::new("prio_high").system(BumpSystem {
                name: "prio_high",
                count: Arc::clone(&count_high),
            }),
            PipelineConfig::new().priority(100),
        )
        .pipeline_with(
            Pipeline::new("prio_low").system(BumpSystem {
                name: "prio_low",
                count: Arc::clone(&count_low),
            }),
            PipelineConfig::new().priority(-10),
        );

    sched.tick().await.unwrap();

    assert_eq!(*count_high.lock().unwrap(), 1);
    assert_eq!(*count_low.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_backpressure_predicate_skips_then_runs_after_release() {
    let count = Arc::new(Mutex::new(0usize));
    let paused = Arc::new(Mutex::new(true));
    let paused_clone = Arc::clone(&paused);

    let mut sched = Scheduler::new().pipeline_with(
        Pipeline::new("throttled").system(BumpSystem {
            name: "throttled",
            count: Arc::clone(&count),
        }),
        PipelineConfig::new().pause_when(move |_p| *paused_clone.lock().unwrap()),
    );

    sched.tick().await.unwrap();
    assert_eq!(*count.lock().unwrap(), 0, "should be skipped when paused");

    *paused.lock().unwrap() = false;

    sched.tick().await.unwrap();
    assert_eq!(
        *count.lock().unwrap(),
        1,
        "should run after backpressure released"
    );
}
