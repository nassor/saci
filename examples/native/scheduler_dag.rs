//! Cross-pipeline DAG scheduling with priorities and backpressure.
//!
//! Four demos over the scheduler API. `DependencyKind::Order` makes `enrich`
//! wait for `ingest`. `DependencyKind::Data` skips `report` when `enrich`
//! produces no rows. Priority orders two pipelines within one stage.
//! `BackpressureSpec::Predicate` skips a pipeline until a flag clears.
//! `RunStats` reports rows produced and systems run per pipeline per tick.
//!
//! ```bash
//! cargo run --example scheduler_dag
//! ```

use std::sync::{Arc, Mutex};

use arrow_array::{Float64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use saci_service::prelude::*;
use serde::{Deserialize, Serialize};

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
        SystemMeta::new("ingest").write_component("Transaction")
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        if !data.schemas().contains("Transaction") {
            data.register_component::<Transaction>()?;
        }
        data.append::<Transaction>(&self.rows)?;
        Ok(())
    }
}

struct EnrichSystem;

#[async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich").read_component("Transaction")
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        let batch = data
            .columns::<Transaction>()
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

        let ws = WriteSet::new().put("Transaction", "amount", Arc::new(usd));
        data.apply_write_set(ws)
    }
}

struct ReportSystem {
    total: Arc<Mutex<f64>>,
}

#[async_trait]
impl System for ReportSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("report").read("Transaction", "amount")
    }
    async fn run(&self, data: &mut Dataset) -> SaciResult<()> {
        let batch = data
            .columns::<Transaction>()
            .expect("Transaction missing")
            .clone();
        let amount_col = batch
            .column(batch.schema().index_of("amount").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("amount column is Float64Array");
        let sum: f64 = amount_col.iter().flatten().sum();
        *self.total.lock().unwrap() += sum;
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

    let mut ingest = Pipeline::new("ingest");
    ingest.data.register_component::<Transaction>()?;
    ingest.add_system(IngestSystem { rows: txns });

    let mut enrich = Pipeline::new("enrich");
    enrich.data.register_component::<Transaction>()?;
    enrich.add_system(EnrichSystem);

    let mut report = Pipeline::new("report");
    report.data.register_component::<Transaction>()?;
    report.add_system(ReportSystem {
        total: Arc::clone(&total),
    });

    let mut sched = Scheduler::new();
    sched.add_pipeline(ingest);
    sched.add_pipeline_with_config(
        enrich,
        PipelineConfig::new().after("ingest", DependencyKind::Order),
    );
    sched.add_pipeline_with_config(
        report,
        PipelineConfig::new().after("enrich", DependencyKind::Order),
    );

    sched.tick().await?;

    println!(
        "  ingest stats: {:?}",
        sched.get("ingest").unwrap().last_stats()
    );
    println!(
        "  enrich stats: {:?}",
        sched.get("enrich").unwrap().last_stats()
    );
    println!("  Accumulated total: {:.2}", *total.lock().unwrap());
    Ok(())
}

async fn demo_data_skip() -> SaciResult<()> {
    println!("\n=== Demo 2: Data dependency — report skipped when enrich produces 0 rows ===");

    let total = Arc::new(Mutex::new(0.0f64));

    // No systems and no rows, so rows_produced is 0.
    let empty_enrich = Pipeline::new("empty_enrich");

    let mut report = Pipeline::new("report_data");
    report.data.register_component::<Transaction>()?;
    report.add_system(ReportSystem {
        total: Arc::clone(&total),
    });

    let mut sched = Scheduler::new();
    sched.add_pipeline(empty_enrich);
    sched.add_pipeline_with_config(
        report,
        PipelineConfig::new().after("empty_enrich", DependencyKind::Data),
    );

    sched.tick().await?;

    let report_stats = sched.get("report_data").unwrap().last_stats();
    println!(
        "  report systems_run: {} (expected 0 — skipped)",
        report_stats.systems_run
    );
    assert_eq!(
        report_stats.systems_run, 0,
        "report should have been skipped"
    );
    Ok(())
}

async fn demo_priority() -> SaciResult<()> {
    println!("\n=== Demo 3: Priority ordering (lower number = runs first) ===");

    let count_high = Arc::new(Mutex::new(0usize));
    let count_low = Arc::new(Mutex::new(0usize));

    let mut sched = Scheduler::new();
    sched.add_pipeline_with_config(
        {
            let mut p = Pipeline::new("prio_high_num");
            p.add_system(BumpSystem {
                name: "prio_high_num",
                count: Arc::clone(&count_high),
            });
            p
        },
        PipelineConfig::new().priority(100),
    );
    sched.add_pipeline_with_config(
        {
            let mut p = Pipeline::new("prio_low_num");
            p.add_system(BumpSystem {
                name: "prio_low_num",
                count: Arc::clone(&count_low),
            });
            p
        },
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

    let mut sched = Scheduler::new();
    sched.add_pipeline_with_config(
        {
            let mut p = Pipeline::new("throttled");
            p.add_system(BumpSystem {
                name: "throttled",
                count: Arc::clone(&count),
            });
            p
        },
        PipelineConfig::new().backpressure(BackpressureSpec::Predicate(Box::new(move |_p| {
            *paused_clone.lock().unwrap()
        }))),
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
