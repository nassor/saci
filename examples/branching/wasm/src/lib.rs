//! Routing demo as a WebAssembly processor.
//!
//! Declares one component, `Order { id, priority }`, and one system,
//! `route_batch`, which reads the batch's first row and inserts a
//! [`RouteDecision`] naming the branch that row selects:
//!
//! - `priority = "high"` → branch `high`
//! - `priority = "low"`  → branch `low`
//! - anything else, or an empty batch → no branch (the output is dropped)
//!
//! The host delivers this batch's output only to the `link`s whose `branch`
//! the decision names, so `examples/branching/branching.kdl` can send each
//! batch to a different sink. Routing is per batch: the first row's priority
//! decides where the whole batch goes. The demo's stream source makes every
//! NATS message its own batch, and the publisher draws priority 50/50 between
//! `"high"` and `"low"`, so both branches fire as the stream runs. A processor
//! that never inserts a `RouteDecision` keeps the legacy behaviour of
//! multicasting to every downstream link.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p branching-wasm --target wasm32-wasip2
//! ```
//!
//! The output component lands at
//! `target/wasm32-wasip2/release/branching_wasm.wasm`.

#![deny(missing_docs)]

// The bindings are generated in place from `crates/saci-processor/wit`. The
// module and the `export_pipeline!` invocation below are gated on
// `target_arch = "wasm32"`: the expansion emits canonical ABI intrinsics and the
// `component-type` custom section, neither of which the host target can link.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../../crates/saci-processor/wit",
        world: "saci-pipeline",
        generate_all,
    });
}

use std::sync::Arc;

use saci_processor::RouteDecision;
use saci_processor::arrow_array::StringArray;
use saci_processor::arrow_schema::{DataType, Field, Schema};
use saci_processor::prelude::*;

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
/// `high` and `low` map to the same-named branches in
/// `examples/branching/branching.kdl`; an unrecognised priority or an empty
/// batch routes the output nowhere. Reading the data plane is safe here: the
/// host's `run-batch` Arrow IPC payload is a fully materialised dataset, not a
/// stream.
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
                "high" => vec!["high".to_string()],
                "low" => vec!["low".to_string()],
                _ => Vec::new(),
            }
        }
    };
    data.insert_resource(RouteDecision(branch));
    Ok(())
}

/// Build the branching demo pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export, and constructed exactly once per component instance.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("branching");
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

#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build);
