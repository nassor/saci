//! Distributed windowed aggregation with cross-run accumulator persistence.
//!
//! A [`DistributedRunner`] processes two master batches with at-least-once
//! semantics, checkpointing after every stage. [`WindowAccumulator`] is
//! registered in the world factory, so the runner loads and persists partial
//! window aggregates across batch claims: they survive a crash and are merged
//! on recovery. One [`KeyPartition`] covers every key.
//!
//! Production replaces [`RedbSharedStore::single_node`] with a multi-node store
//! backed by [`ArrowRaftDriver`] and sets `num_instances > 1` to shard keys
//! across workers.
//!
//! ```bash
//! cargo run --example distributed_windowed --features "distributed,windows"
//! ```

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use saci_service::SaciError;
use saci_service::System;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::distributed::strategy::CheckpointStrategy;
use saci_service::distributed::{DistributedRunner, KeyPartition, RedbSharedStore, RunnerConfig};
use saci_service::pipeline::Pipeline;
use saci_service::system::{SystemMeta, system_fn};
use saci_service::windows::{
    ReduceAggregate, WindowAccumulator, WindowFunction, WindowResults, WindowSpec,
    WindowedSystemBuilder,
};

#[derive(Debug, Serialize, Deserialize)]
struct SalesEvent {
    /// Unix timestamp in milliseconds.
    timestamp_ms: i64,
    /// Product category name.
    category: String,
    /// Sale amount in USD.
    amount: f64,
}

impl Component for SalesEvent {
    fn name() -> &'static str {
        "SalesEvent"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }
}

fn make_sales_ipc(events: &[(i64, &str, f64)]) -> Vec<u8> {
    use arrow_array::RecordBatch;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};

    let schema = SalesEvent::schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                events.iter().map(|(ts, _, _)| *ts).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                events.iter().map(|(_, cat, _)| *cat).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                events.iter().map(|(_, _, amt)| *amt).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build sales batch");

    let options = IpcWriteOptions::default();
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, &schema, options).expect("ipc writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish");
    }
    buf
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
    println!("=== Distributed Windowed Aggregation (single-node) ===\n");

    let db_path = std::env::temp_dir().join(format!(
        "saci_distributed_windowed_{}.redb",
        uuid::Uuid::now_v7()
    ));
    let store = RedbSharedStore::single_node(&db_path)?;

    // Batch 0 holds events in the first 30-second window, batch 1 the second.
    // Two batches show accumulator state flowing from one run into the next.
    let batch0: Vec<(i64, &str, f64)> = vec![
        (5_000, "Electronics", 299.99),
        (12_000, "Books", 24.95),
        (18_000, "Electronics", 149.50),
        (25_000, "Books", 39.99),
    ];
    let batch1: Vec<(i64, &str, f64)> = vec![
        (31_000, "Electronics", 599.00),
        (45_000, "Electronics", 89.99),
        (50_000, "Books", 14.99),
        (62_000, "Electronics", 199.00),
        (75_000, "Books", 49.99),
    ];

    let ipc0 = make_sales_ipc(&batch0);
    let ipc1 = make_sales_ipc(&batch1);

    store
        .register_master_batch(
            0,
            SalesEvent::name().to_string(),
            1,
            ipc0,
            batch0.len() as u32,
        )
        .await?;
    store
        .register_master_batch(
            1,
            SalesEvent::name().to_string(),
            1,
            ipc1,
            batch1.len() as u32,
        )
        .await?;

    println!(
        "Registered 2 master batches ({} + {} rows)\n",
        batch0.len(),
        batch1.len()
    );

    // WindowResults carries opaque i64 key_hash values. The reverse map comes
    // from running each known category through a one-row Dataset and reading
    // back the hash the windowed system assigned it.
    let known_categories = ["Electronics", "Books"];
    let mut hash_to_category: std::collections::HashMap<i64, &str> = Default::default();

    for cat in known_categories {
        let row = SalesEvent {
            timestamp_ms: 0,
            category: cat.to_string(),
            amount: 1.0,
        };
        let mut mini = Dataset::new();
        mini.register_component::<SalesEvent>()?;
        mini.append::<SalesEvent>(&[row])?;

        let probe = WindowedSystemBuilder::new()
            .source("SalesEvent", "timestamp_ms")
            .keyed_by(&["category"])
            .window(WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "amount",
                aggregate: ReduceAggregate::Sum,
            })
            .build()?;

        probe.run(&mut mini).await?;

        if let Some(wr) = mini.get_resource::<WindowResults>() {
            for batch in &wr.batches {
                if let Ok(kh_idx) = batch.schema().index_of("key_hash") {
                    use arrow_array::cast::AsArray;
                    let col = batch
                        .column(kh_idx)
                        .as_primitive::<arrow_array::types::Int64Type>();
                    if !col.is_empty() {
                        hash_to_category.insert(col.value(0), cat);
                    }
                }
            }
        }
    }
    println!("key_hash map: {:?}\n", hash_to_category);

    // Distributed mode needs a data-free pipeline: it carries the system DAG,
    // and the runner supplies rows per claimed batch.
    let windowed = WindowedSystemBuilder::new()
        .source("SalesEvent", "timestamp_ms")
        .keyed_by(&["category"])
        .window(WindowSpec::Tumbling {
            size_ms: 30_000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "amount",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(5_000)
        .build()?;

    let lookup = Arc::new(hash_to_category);
    let lookup_report = Arc::clone(&lookup);

    let mut pipeline = Pipeline::new("distributed-windowed");
    pipeline.add_system(windowed);
    pipeline.add_system(system_fn(
        SystemMeta::new("report").read_resource::<WindowResults>(),
        move |data: &mut Dataset| {
            let Some(results) = data.get_resource::<WindowResults>() else {
                return Ok(());
            };
            use arrow_array::{Array, cast::AsArray};
            for batch in &results.batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let schema = batch.schema();
                let Ok(wid_idx) = schema.index_of("window_id") else {
                    continue;
                };
                let Ok(kh_idx) = schema.index_of("key_hash") else {
                    continue;
                };
                let sum_idx = schema.fields().len() - 1;

                let wid_col = batch
                    .column(wid_idx)
                    .as_primitive::<arrow_array::types::Int64Type>();
                let kh_col = batch
                    .column(kh_idx)
                    .as_primitive::<arrow_array::types::Int64Type>();
                let sum_col = batch
                    .column(sum_idx)
                    .as_primitive::<arrow_array::types::Float64Type>();

                for row in 0..batch.num_rows() {
                    let wid = wid_col.value(row);
                    let kh = kh_col.value(row);
                    let sum = if sum_col.is_valid(row) {
                        sum_col.value(row)
                    } else {
                        0.0
                    };
                    let cat = lookup_report.get(&kh).copied().unwrap_or("<unknown>");
                    let start_s = wid * 30;
                    println!(
                        "  window {:>3}s–{:>3}s  {:<14}  sum = {:.2}",
                        start_s,
                        start_s + 30,
                        cat,
                        sum
                    );
                }
            }
            Ok(())
        },
    ));

    let config = RunnerConfig {
        // Both master batches, then stop.
        max_batches: Some(2),
        // Checkpointing every stage means recovery replays from the last
        // completed stage instead of the whole batch.
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        schema_id: 1,
        // Single instance, so this worker owns every key partition. A cluster
        // sets instance_ordinal to the worker index and num_instances to the
        // cluster size, and each worker filters its own key slice.
        partition_mask: Some(KeyPartition {
            instance_ordinal: 0,
            num_instances: 1,
        }),
        ..Default::default()
    };

    println!("Instance:             {}", config.instance_id);
    println!("Checkpoint strategy:  {:?}", config.checkpoint_strategy);
    println!("Partition mask:       {:?}\n", config.partition_mask);

    // Called fresh for each claimed batch. SalesEvent lets the runner decode
    // the IPC payload into columnar rows; without WindowAccumulator registered
    // the accumulator load and save are skipped silently.
    let world_factory = || {
        let mut d = Dataset::new();
        d.register_component::<SalesEvent>().unwrap();
        d.register_component::<WindowAccumulator>().unwrap();
        d
    };

    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(world_factory).await?;

    println!("\nBatches processed: {}", processed);
    println!("Accumulator state was checkpointed between batches.");
    println!("Restart the example to observe recovery from the saved checkpoint.");

    let _ = std::fs::remove_file(&db_path);

    Ok(())
}
