//! Windowed aggregation over sales events: tumbling 30-second windows keyed by category.
//!
//! [`WindowedSystemBuilder`] sums the `amount` field of a `SalesEvent`
//! component into 30-second tumbling windows keyed by `category`, and a
//! downstream system prints the totals from [`WindowResults`]. Rows arriving
//! within `allowed_lateness` re-fire their window; later ones go to the side
//! output.
//!
//! ```bash
//! cargo run --example window_aggregation --features windows
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use saci_service::SaciError;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::pipeline::Pipeline;
use saci_service::system::{FieldRef, System, SystemMeta};

use saci_service::windows::{
    ReduceAggregate, WindowFunction, WindowResults, WindowSpec, WindowedSystemBuilder,
};

#[derive(Serialize, Deserialize, Clone, Debug)]
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

impl SalesEvent {
    const CATEGORY: FieldRef<SalesEvent> = FieldRef::new("category");
}

/// Loads seed sales events spanning three 30-second windows across two categories.
struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").write_component("SalesEvent")
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        // The rows span three windows: 0 to 30s, 30 to 60s, 60 to 90s.
        let events = vec![
            SalesEvent {
                timestamp_ms: 5_000,
                category: "Electronics".into(),
                amount: 299.99,
            },
            SalesEvent {
                timestamp_ms: 12_000,
                category: "Books".into(),
                amount: 24.95,
            },
            SalesEvent {
                timestamp_ms: 18_000,
                category: "Electronics".into(),
                amount: 149.50,
            },
            SalesEvent {
                timestamp_ms: 25_000,
                category: "Books".into(),
                amount: 39.99,
            },
            SalesEvent {
                timestamp_ms: 31_000,
                category: "Electronics".into(),
                amount: 599.00,
            },
            SalesEvent {
                timestamp_ms: 45_000,
                category: "Electronics".into(),
                amount: 89.99,
            },
            SalesEvent {
                timestamp_ms: 50_000,
                category: "Books".into(),
                amount: 14.99,
            },
            SalesEvent {
                timestamp_ms: 62_000,
                category: "Electronics".into(),
                amount: 199.00,
            },
            SalesEvent {
                timestamp_ms: 75_000,
                category: "Books".into(),
                amount: 49.99,
            },
        ];
        println!("[ingest]  loaded {} sales events", events.len());
        data.append::<SalesEvent>(&events)?;
        Ok(())
    }
}

/// Maps `key_hash` to `category` so the report system can decode window
/// results. The windowed system derives the same hashes from the `category`
/// field, so the map stays valid for one pipeline run.
struct CategoryLookup(HashMap<i64, String>);

/// Builds the hash-to-category reverse map from the live dataset.
struct BuildLookupSystem;

#[async_trait]
impl System for BuildLookupSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("build_lookup")
            .reads(SalesEvent::CATEGORY)
            .write_resource::<CategoryLookup>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        use saci_service::windows::WindowedSystemBuilder as W;

        // The key-hash helper is crate-private, so each category's hash comes
        // from running an identically configured windowed system over a
        // one-row dataset and reading back its `key_hash` output.
        use saci_service::windows::WindowSpec as WS;
        let known_categories = ["Electronics", "Books"];
        let mut map = HashMap::new();

        for cat in known_categories {
            let row = SalesEvent {
                timestamp_ms: 0,
                category: cat.to_string(),
                amount: 1.0,
            };
            let mut mini = Dataset::new();
            mini.register_component::<SalesEvent>()?;
            mini.append::<SalesEvent>(&[row])?;

            let ws = W::new()
                .source("SalesEvent", "timestamp_ms")
                .keyed_by(&["category"])
                .window(WS::Tumbling {
                    size_ms: 30_000,
                    offset_ms: 0,
                })
                .function(WindowFunction::Reduce {
                    input_field: "amount",
                    aggregate: ReduceAggregate::Sum,
                })
                .build()
                .map_err(|e| SaciError::generic(format!("BuildLookupSystem build: {e}")))?;

            ws.run(&mut mini).await?;

            if let Some(wr) = mini.get_resource::<WindowResults>() {
                for batch in &wr.batches {
                    let kh_idx = batch
                        .schema()
                        .index_of("key_hash")
                        .map_err(|e| SaciError::generic(format!("key_hash column missing: {e}")))?;
                    let kh_col = batch
                        .column(kh_idx)
                        .as_primitive::<arrow_array::types::Int64Type>();
                    if !kh_col.is_empty() {
                        map.insert(kh_col.value(0), cat.to_string());
                    }
                }
            }
        }

        println!("[lookup]  built hash map for {} categories", map.len());
        data.insert_resource(CategoryLookup(map));
        Ok(())
    }
}

/// Reads [`WindowResults`] and prints a table of per-window-per-category totals.
struct ReportSystem;

#[async_trait]
impl System for ReportSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("report")
            .read_resource::<WindowResults>()
            .read_resource::<CategoryLookup>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let results = data
            .get_resource::<WindowResults>()
            .ok_or_else(|| SaciError::generic("WindowResults resource not found"))?;
        let lookup = data
            .get_resource::<CategoryLookup>()
            .ok_or_else(|| SaciError::generic("CategoryLookup resource not found"))?;

        println!();
        println!("╔══════════════════════════════════════════════════════╗");
        println!("║        SALES — 30-SECOND TUMBLING WINDOW TOTALS     ║");
        println!("╠═══════════════════════╦══════════════╦══════════════╣");
        println!("║  Window               ║  Category    ║  Sum (USD)   ║");
        println!("╠═══════════════════════╬══════════════╬══════════════╣");

        for batch in &results.batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let schema = batch.schema();
            let wid_idx = schema
                .index_of("window_id")
                .map_err(|e| SaciError::generic(format!("window_id missing: {e}")))?;
            let kh_idx = schema
                .index_of("key_hash")
                .map_err(|e| SaciError::generic(format!("key_hash missing: {e}")))?;
            // The aggregated column is the last field (after window_id and key_hash).
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

                // Tumbling window ids are floor-division indices, so 30 times
                // the id is the start second.
                let window_start_s = wid * 30;
                let window_end_s = window_start_s + 30;
                let window_label = format!("{window_start_s:>3}s – {window_end_s:>3}s");

                let category = lookup.0.get(&kh).map(|s| s.as_str()).unwrap_or("<unknown>");

                println!(
                    "║  {:<21}  ║  {:<12}  ║  {:>10.2}  ║",
                    window_label, category, sum
                );
            }
        }

        println!("╚═══════════════════════╩══════════════╩══════════════╝");

        if !results.late_batches.is_empty() {
            println!("  Late re-firings: {}", results.late_batches.len());
        }
        if !results.side_output.is_empty() {
            println!(
                "  Dropped (beyond lateness): {} rows",
                results.side_output.total_rows()
            );
        }

        println!();
        println!("Total on-time window groups: {}", results.total_rows());

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
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

    let mut pipeline = Pipeline::builder("window_aggregation")
        .with::<SalesEvent>()
        .with_system(IngestSystem)
        .with_system(BuildLookupSystem)
        .with_system(windowed)
        .with_system(ReportSystem)
        .build();

    println!("Running windowed aggregation pipeline...");
    pipeline.run().await?;

    let stages = pipeline.stages().unwrap_or_default();
    println!();
    println!("Stage layout:");
    for (i, stage) in stages.iter().enumerate() {
        println!("  Stage {i}: {stage:?}");
    }

    Ok(())
}
