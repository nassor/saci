//! Multi-workflow routing demo as a WebAssembly processor.
//!
//! Declares one component, `Sale { timestamp_ms, symbol, amount }` — the
//! exact schema `examples/windowing/tumbling/wasm` declares, so rows bridge
//! into the windowed processor unchanged — and one system, `route_batch`,
//! which reads the batch's first row and inserts a [`sdk::RouteDecision`] naming
//! the branch that row selects:
//!
//! - `amount >= 100.0` → branch `rush`
//! - otherwise → branch `standard`
//!
//! The host delivers this batch's output only to the `link`s whose `branch`
//! the decision names, so `examples/multi_workflow/multi_workflow.kdl` can
//! send `rush` batches straight to PostgreSQL and `standard` batches into the
//! in-process channel that bridges into the settle workflow. Routing is per
//! batch: the first row's amount decides where the whole batch goes. The
//! demo's stream source makes every NATS message its own batch, and the
//! publisher draws amounts uniformly in 50.0..150.0, so both branches fire as
//! the stream runs. A processor that never inserts a `RouteDecision` keeps
//! the legacy behaviour of multicasting to every downstream link.
//!
//! # Build
//!
//! ```bash
//! cargo build --release -p multi-workflow-router-wasm --target wasm32-wasip3
//! ```
//!
//! The output component lands at
//! `target/wasm32-wasip3/release/multi_workflow_router_wasm.wasm`.

#![deny(missing_docs)]

use std::sync::Arc;

use saci::arrow_array::Float64Array;
use saci::arrow_schema::{DataType, Field, Schema};
use saci::prelude::*;

/// Batches whose first-row amount is at least this land in the `rush` branch.
const RUSH_THRESHOLD: f64 = 100.0;

/// A row of the demo workload: a sale with a simulated event timestamp.
///
/// The exact schema `examples/windowing/tumbling/wasm` declares, so a
/// `standard` batch bridges into the windowed processor with no schema cast.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Sale {
    /// Unix timestamp in milliseconds; the window's event time.
    pub timestamp_ms: i64,
    /// Grouping key, e.g. a stock ticker.
    pub symbol: String,
    /// The value whose size selects the routing branch.
    pub amount: f64,
}

impl Component for Sale {
    fn name() -> &'static str {
        "Sale"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }
}

/// Insert a [`sdk::RouteDecision`] naming the branch the first row selects.
///
/// `rush` and `standard` map to the same-named branches in
/// `examples/multi_workflow/multi_workflow.kdl`; an empty batch routes the
/// output nowhere. Reading the data plane is safe here: the host's
/// `run-batch` Arrow IPC payload is a fully materialised dataset, not a
/// stream.
fn route_batch(data: &mut Dataset) -> Result<(), SaciError> {
    let batch = data.batch::<Sale>()?.clone();
    let schema = batch.schema();
    let amount_idx = schema
        .index_of("amount")
        .map_err(|e| SaciError::generic(format!("multi-workflow: amount field missing: {e}")))?;

    let branch = match batch.num_rows() {
        0 => Vec::new(),
        _ => {
            let amounts = batch
                .column(amount_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    SaciError::generic("multi-workflow: amount is not a float column")
                })?;
            if amounts.value(0) >= RUSH_THRESHOLD {
                vec!["rush".to_string()]
            } else {
                vec!["standard".to_string()]
            }
        }
    };
    data.insert_resource(sdk::RouteDecision::to(branch));
    Ok(())
}

/// Build the multi-workflow routing demo pipeline.
///
/// Called lazily by the `#[processor]` glue on the first call to any WIT
/// export, and constructed exactly once per component instance.
#[processor]
pub fn build() -> Pipeline {
    Pipeline::new("multi-workflow-router")
        .component::<Sale>()
        .system(system_fn(
            SystemMeta::new("route_batch").reads_named("Sale", "amount"),
            route_batch,
        ))
}
