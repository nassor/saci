//! Routing demo as a native plugin.
//!
//! The native counterpart of `examples/branching/wasm`: declares one
//! component, `Order { id, priority }`, and one system, `route_batch`, which
//! reads the batch's first row and inserts a [`RouteDecision`] naming the
//! branch that row selects:
//!
//! - `priority = "high"` → branch `premium`
//! - `priority = "low"`  → branch `standard`
//! - anything else, or an empty batch → no branch (the output is dropped)
//!
//! Branch names differ from the wasm demo's so the two routers' outputs are
//! visibly separate sinks; the workflow in `examples/branching/branching.kdl`
//! carries `branch="premium"` and `branch="standard"` links. Routing is per
//! batch: the first row's priority decides where the whole batch goes, and
//! the stream source hands each NATS message to the plugin as its own batch,
//! so both branches fire as the publisher mixes priorities.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p branching-plugin
//! ```
//!
//! The artifact name is platform specific: `libbranching_plugin.so` on Linux,
//! `libbranching_plugin.dylib` on macOS, `branching_plugin.dll` on Windows.

#![deny(missing_docs)]

use std::sync::Arc;

use saci_plugin::RouteDecision;
use saci_plugin::arrow_array::StringArray;
use saci_plugin::arrow_schema::{DataType, Field, Schema};
use saci_plugin::prelude::*;

/// A row of the demo workload: an id plus the priority that selects the branch.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Order {
    /// Caller-assigned identity.
    pub id: i64,
    /// `"high"` or `"low"`; the first row's value routes the whole batch.
    pub priority: String,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("priority", DataType::Utf8, false),
        ]))
    }
}

/// Insert a [`RouteDecision`] naming the branch the first row selects.
///
/// `high` and `low` map to the `premium` and `standard` branches in
/// `examples/branching/branching.kdl`; an unrecognised priority or an empty
/// batch routes the output nowhere.
fn route_batch(data: &mut Dataset) -> Result<(), SaciError> {
    let batch = data
        .columns::<Order>()
        .ok_or_else(|| SaciError::generic("branching: Order component missing"))?
        .clone();
    let schema = batch.schema();
    let priority_idx = schema
        .index_of("priority")
        .map_err(|e| SaciError::generic(format!("branching: priority field missing: {e}")))?;

    let branch = match batch.num_rows() {
        0 => Vec::new(),
        _ => {
            let priorities = batch
                .column(priority_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SaciError::generic("branching: priority is not a string column"))?;
            match priorities.value(0) {
                "high" => vec!["premium".to_string()],
                "low" => vec!["standard".to_string()],
                _ => Vec::new(),
            }
        }
    };
    data.insert_resource(RouteDecision(branch));
    Ok(())
}

/// Build the branching demo pipeline.
///
/// Called lazily by the `export_plugin!` macro on the first `describe` or
/// `run-batch` call, and constructed exactly once per loaded library.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("branching-plugin");
    pipeline
        .data
        .register_component::<Order>()
        .expect("register Order");
    pipeline.add_system(system_fn(
        SystemMeta::new("route_batch").read("Order", "priority"),
        route_batch,
    ));
    pipeline
}

saci_plugin::export_plugin!(build);
