// DistributedRunner over the in-memory shared-store fixture. Requires the
// `distributed` feature.

#![cfg(feature = "distributed")]

use std::sync::Arc;

use arrow_array::{Float64Array, StringArray, UInt64Array};
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use saci_core::component::Component;
use saci_core::dataset::Dataset;
use saci_core::pipeline::Pipeline;
use saci_core::system::{SystemMeta, system_fn};
use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
use saci_service::distributed::strategy::CheckpointStrategy;

#[path = "common/memory_store.rs"]
mod memory_store;

use memory_store::MemoryStore;

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

fn make_order_ipc(orders: &[(u64, f64, &str)]) -> Vec<u8> {
    use arrow_array::RecordBatch;

    let schema = Order::schema();
    let order_ids: Vec<u64> = orders.iter().map(|(id, _, _)| *id).collect();
    let amounts: Vec<f64> = orders.iter().map(|(_, a, _)| *a).collect();
    let currencies: Vec<&str> = orders.iter().map(|(_, _, c)| *c).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(order_ids)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(StringArray::from(currencies)),
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

#[tokio::test]
async fn test_distributed_runner_processes_single_batch() {
    let store = MemoryStore::new();

    let ipc_bytes = make_order_ipc(&[(1, 100.0, "USD"), (2, 250.0, "EUR"), (3, 75.5, "GBP")]);
    store
        .register_master_batch(0, Order::name().to_string(), 1, ipc_bytes, 3)
        .await
        .unwrap();

    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ran_clone = Arc::clone(&ran);

    let mut pipeline = Pipeline::new("distributed-etl");
    pipeline.add_system(system_fn(
        SystemMeta::new("mark_ran"),
        move |_data: &mut Dataset| {
            ran_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    ));

    let config = RunnerConfig {
        max_batches: Some(1),
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        schema_id: 1,
        ..Default::default()
    };

    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(Dataset::new).await.unwrap();

    assert_eq!(processed, 1, "exactly 1 batch should have been processed");
    assert!(
        ran.load(std::sync::atomic::Ordering::SeqCst),
        "system should have been called"
    );
}

#[tokio::test]
async fn test_distributed_runner_processes_multiple_batches() {
    let store = MemoryStore::new();

    for batch_id in 0..3u64 {
        let ipc_bytes = make_order_ipc(&[
            (batch_id * 10 + 1, 100.0, "USD"),
            (batch_id * 10 + 2, 200.0, "EUR"),
        ]);
        store
            .register_master_batch(batch_id, Order::name().to_string(), 1, ipc_bytes, 2)
            .await
            .unwrap();
    }

    let mut pipeline = Pipeline::new("distributed-multi");
    pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data: &mut Dataset| {
        Ok(())
    }));

    let config = RunnerConfig {
        max_batches: None,
        checkpoint_strategy: CheckpointStrategy::None,
        schema_id: 1,
        ..Default::default()
    };

    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(Dataset::new).await.unwrap();

    assert_eq!(processed, 3, "all 3 batches should have been processed");
}

#[tokio::test]
async fn test_distributed_runner_no_batches_returns_zero() {
    let store = MemoryStore::new();

    let mut pipeline = Pipeline::new("distributed-empty");
    pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data: &mut Dataset| {
        Ok(())
    }));

    let config = RunnerConfig {
        max_batches: None,
        checkpoint_strategy: CheckpointStrategy::None,
        schema_id: 1,
        ..Default::default()
    };

    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(Dataset::new).await.unwrap();

    assert_eq!(processed, 0, "no batches registered, should process 0");
}
