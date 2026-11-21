//! Cross-pipeline DAG scheduling with priorities and backpressure.
//!
//! Four demos over the scheduler API. `PipelineConfig::after` makes `enrich`
//! wait for `ingest`. `after_data` skips `report` when `enrich` produces no
//! rows. Priority orders two pipelines within one stage. `pause_when` skips a
//! pipeline until a flag clears.
//!
//! ```bash
//! cargo run --example scheduler_dag
//! ```

use std::sync::{Arc, Mutex};

use arrow_array::{Float64Array, StringArray};
use saci::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Component, Serialize, Deserialize, Clone, Debug)]
struct Transaction {
    id: u64,
    amount: f64,
    currency: String,
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

struct EnrichSystem;

#[async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich").reads_all::<Transaction>()
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        let batch = data
            .batch::<Transaction>()
            .expect("Transaction missing")
            .clone();
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

        // EUR converts to USD at 1.1; other currencies pass through.
        let usd: Float64Array = (0..batch.num_rows())
            .map(|i| {
                let currency = currency_col.value(i);
                let amount = amount_col.value(i);
                if currency == "EUR" {
                    amount * 1.1
                } else {
                    amount
                }
            })
            .collect();

        let ws = WriteSet::new().put(Transaction::AMOUNT, Arc::new(usd));
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
        SystemMeta::new("report").reads(Transaction::AMOUNT)
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        let batch = data
            .batch::<Transaction>()
            .expect("Transaction missing")
            .clone();
        let amount_col = batch
            .column(batch.schema().index_of("amount").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("amount column is Float64Array");
        let sum: f64 = amount_col.iter().flatten().sum();
        *self.total.lock().unwrap() += sum;
        *self.runs.lock().unwrap() += 1;
        println!("  [report] total USD amount this tick: {sum:.2}");
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
        let mut c = self.count.lock().unwrap();
        *c += 1;
        println!("  [{}] tick #{}", self.name, *c);
        Ok(())
    }
}

async fn demo_order_chain() -> SaciResult<()> {
    println!("\n=== Demo 1: Order dependency chain ===");

    let total = Arc::new(Mutex::new(0.0f64));

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

    let ingest = Pipeline::new("ingest")
        .component::<Transaction>()
        .system(IngestSystem { rows: txns });

    let enrich = Pipeline::new("enrich")
        .component::<Transaction>()
        .system(EnrichSystem);

    let report = Pipeline::new("report")
        .component::<Transaction>()
        .system(ReportSystem {
            total: Arc::clone(&total),
            runs: Arc::new(Mutex::new(0)),
        });

    let mut sched = Scheduler::new()
        .pipeline(ingest)
        .pipeline_with(enrich, PipelineConfig::new().after("ingest"))
        .pipeline_with(report, PipelineConfig::new().after("enrich"));

    sched.tick().await?;

    println!("  Accumulated total: {:.2}", *total.lock().unwrap());
    Ok(())
}

async fn demo_data_skip() -> SaciResult<()> {
    println!("\n=== Demo 2: Data dependency — report skipped when enrich produces 0 rows ===");

    let total = Arc::new(Mutex::new(0.0f64));

    // No systems and no rows, so rows_produced is 0.
    let empty_enrich = Pipeline::new("empty_enrich");

    let runs = Arc::new(Mutex::new(0usize));
    let report = Pipeline::new("report_data")
        .component::<Transaction>()
        .system(ReportSystem {
            total: Arc::clone(&total),
            runs: Arc::clone(&runs),
        });

    let mut sched = Scheduler::new()
        .pipeline(empty_enrich)
        .pipeline_with(report, PipelineConfig::new().after_data("empty_enrich"));

    sched.tick().await?;

    let report_runs = *runs.lock().unwrap();
    println!("  report runs: {report_runs} (expected 0 — skipped)");
    assert_eq!(report_runs, 0, "report should have been skipped");
    Ok(())
}

async fn demo_priority() -> SaciResult<()> {
    println!("\n=== Demo 3: Priority ordering (lower number = runs first) ===");

    let count_high = Arc::new(Mutex::new(0usize));
    let count_low = Arc::new(Mutex::new(0usize));

    let mut sched = Scheduler::new()
        .pipeline_with(
            Pipeline::new("prio_high_num").system(BumpSystem {
                name: "prio_high_num",
                count: Arc::clone(&count_high),
            }),
            PipelineConfig::new().priority(100),
        )
        .pipeline_with(
            Pipeline::new("prio_low_num").system(BumpSystem {
                name: "prio_low_num",
                count: Arc::clone(&count_low),
            }),
            PipelineConfig::new().priority(-10),
        );

    sched.tick().await?;
    println!("  Both pipelines ran (prio_low_num first due to lower priority number).");
    assert_eq!(*count_high.lock().unwrap(), 1);
    assert_eq!(*count_low.lock().unwrap(), 1);
    Ok(())
}

async fn demo_backpressure() -> SaciResult<()> {
    println!("\n=== Demo 4: Backpressure predicate ===");

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

    sched.tick().await?;
    assert_eq!(*count.lock().unwrap(), 0, "should be skipped when paused");
    println!("  Tick 1: skipped (backpressure active)");

    *paused.lock().unwrap() = false;

    sched.tick().await?;
    assert_eq!(
        *count.lock().unwrap(),
        1,
        "should run after backpressure released"
    );
    println!("  Tick 2: ran (backpressure released)");

    Ok(())
}

#[tokio::main]
async fn main() -> SaciResult<()> {
    demo_order_chain().await?;
    demo_data_skip().await?;
    demo_priority().await?;
    demo_backpressure().await?;
    println!("\nAll demos passed.");
    Ok(())
}
