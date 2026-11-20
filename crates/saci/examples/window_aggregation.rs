//! Windowed aggregation over sales events: tumbling 30-second windows keyed by category.
//!
//! [`WindowedSystem`] sums the `amount` field of a `SalesEvent`
//! component into 30-second tumbling windows keyed by `category`, and a
//! downstream system prints the totals from [`WindowResults`]. Rows arriving
//! within the allowed lateness re-fire their window; later ones land in
//! [`WindowResults::dropped`].
//!
//! ```bash
//! cargo run --example window_aggregation --features windows
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StringArray, cast::AsArray};
use serde::{Deserialize, Serialize};

use saci::prelude::*;

use saci::windows::{WindowFunction, WindowResults, WindowedSystem, key_hash};

#[derive(Component, Serialize, Deserialize, Clone, Debug)]
struct SalesEvent {
    /// Unix timestamp in milliseconds.
    timestamp_ms: i64,
    /// Product category name.
    category: String,
    /// Sale amount in USD.
    amount: f64,
}

/// Loads seed sales events spanning three 30-second windows across two categories.
struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").writes_all::<SalesEvent>()
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
            .writes_resource::<CategoryLookup>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        // `key_hash` is the function the windowed system keys its groups by,
        // so hashing the known category names yields the same `key_hash`
        // values the window results carry.
        let known_categories = ["Electronics", "Books"];
        let names: ArrayRef = Arc::new(StringArray::from(known_categories.to_vec()));
        let hashes = key_hash(&[&names])?;
        let map: HashMap<i64, String> = known_categories
            .iter()
            .enumerate()
            .map(|(i, cat)| (hashes.value(i), (*cat).to_string()))
            .collect();

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
            .reads_resource::<WindowResults>()
            .reads_resource::<CategoryLookup>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let results = data.resource::<WindowResults>()?;
        let lookup = data.resource::<CategoryLookup>()?;

        println!();
        println!("╔══════════════════════════════════════════════════════╗");
        println!("║        SALES — 30-SECOND TUMBLING WINDOW TOTALS     ║");
        println!("╠═══════════════════════╦══════════════╦══════════════╣");
        println!("║  Window               ║  Category    ║  Sum (USD)   ║");
        println!("╠═══════════════════════╬══════════════╬══════════════╣");

        for batch in &results.on_time {
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

        if !results.late.is_empty() {
            println!("  Late re-firings: {}", results.late.len());
        }
        let dropped_rows: usize = results.dropped.iter().map(|b| b.num_rows()).sum();
        if dropped_rows > 0 {
            println!("  Dropped (beyond lateness): {dropped_rows} rows");
        }

        println!();
        println!("Total on-time window groups: {}", results.total_rows());

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
    let windowed = WindowedSystem::new(
        SalesEvent::TIMESTAMP_MS,
        WindowSpec::tumbling(30_000),
        WindowFunction::sum("amount"),
    )?
    .key_by(["category"])
    .allowed_lateness_ms(5_000);

    let mut pipeline = Pipeline::new("window_aggregation")
        .component::<SalesEvent>()
        .system(IngestSystem)
        .system(BuildLookupSystem)
        .system(windowed)
        .system(ReportSystem);

    println!("Running windowed aggregation pipeline...");
    pipeline.run().await?;

    let stages = pipeline.plan()?.to_vec();
    println!();
    println!("Stage layout:");
    for (i, stage) in stages.iter().enumerate() {
        println!("  Stage {i}: {stage:?}");
    }

    Ok(())
}
