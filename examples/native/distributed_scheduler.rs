//! Arrow-IPC distributed scheduler: single-node batch processing with checkpoint.
//!
//! Master batches are registered with [`RedbSharedStore`]. A
//! [`DistributedRunner`] then claims a row-range, runs a [`Pipeline`] over a
//! fresh [`Dataset`], checkpoints the resulting IPC bytes, and acks the claim.
//! Recovery after a crash resumes from the checkpoint.
//!
//! This is single-node mode, no Raft. A production deployment uses a
//! multi-node [`RedbSharedStore`] plus [`ArrowRaftDriver`].
//!
//! ```bash
//! cargo run --example distributed_scheduler --features distributed
//! ```

use std::sync::Arc;

use arrow_array::{Float64Array, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use saci_service::SaciError;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::distributed::RedbSharedStore;
use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
use saci_service::distributed::strategy::CheckpointStrategy;
use saci_service::pipeline::Pipeline;
use saci_service::system::{SystemMeta, system_fn};

/// A sales order with an amount and currency.
#[derive(Debug, Serialize, Deserialize)]
struct Order {
    order_id: u64,
    amount: f64,
    currency: String,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::UInt64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
        ]))
    }
}

fn make_order_ipc() -> Vec<u8> {
    use arrow_array::RecordBatch;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};

    let schema = Order::schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![100.0, 250.0, 75.5])),
            Arc::new(StringArray::from(vec!["USD", "EUR", "GBP"])),
        ],
    )
    .expect("build order batch");

    let options = IpcWriteOptions::default();
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, &schema, options).expect("ipc writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish ipc");
    }
    buf
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
    println!("=== Arrow Distributed Scheduler (single-node) ===\n");

    let db_path = std::env::temp_dir().join(format!(
        "saci_arrow_distributed_example_{}.redb",
        uuid::Uuid::now_v7()
    ));
    let store = RedbSharedStore::single_node(&db_path)?;

    // In production the master batch comes from an ingestion layer such as a
    // Parquet or S3 reader, not a hand-rolled helper.
    let ipc_bytes = make_order_ipc();
    println!(
        "Master batch IPC size: {} bytes ({} rows)",
        ipc_bytes.len(),
        3
    );

    store
        .register_master_batch(0, Order::name().to_string(), 1, ipc_bytes, 3)
        .await?;
    println!("Registered master batch 0 (3 rows)\n");

    let mut pipeline = Pipeline::new("distributed-etl");

    pipeline.add_system(system_fn(
        SystemMeta::new("summarise"),
        |data: &mut Dataset| {
            let row_count = data.rows();
            println!("  [summarise] dataset has {} rows", row_count);
            Ok(())
        },
    ));

    println!("Pipeline: 1 system (stages computed on first run)");

    let config = RunnerConfig {
        // One batch only, so the example terminates.
        max_batches: Some(1),
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        schema_id: 1,
        ..Default::default()
    };

    println!("Instance: {}", config.instance_id);
    println!("Checkpoint strategy: {:?}\n", config.checkpoint_strategy);

    // The dataset factory runs fresh for each claimed batch.
    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(Dataset::new).await?;

    println!("\n=== Result ===");
    println!("Batches processed: {}", processed);
    println!("Checkpoint written: yes (EveryStage)");
    println!("At-least-once guarantee: claim was acked after successful run");

    let _ = std::fs::remove_file(&db_path);

    Ok(())
}
