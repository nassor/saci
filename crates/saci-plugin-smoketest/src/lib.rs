//! Minimal native plugin: the host-side load fixture and Arrow IPC round-trip
//! fixture.
//!
//! It declares one data component and one system, and exercises
//! `saci_plugin::export_plugin!` end to end through a real shared library.
//!
//! - `Counter` is the data plane. Its `id` field passes through untouched, so a
//!   byte difference there indicates `arrow-ipc` drift between host and plugin.
//! - `Total` is the cross-batch state, declared via
//!   `export_plugin!(build, state = Total)`. It lives in a `ProcessorState<Total>`
//!   resource, not a registered component, so it never appears in the output
//!   IPC.
//! - `advance` writes `seen` as `(total + row + 1) * multiplier`, then adds the
//!   batch's row count to `total`. Three rows therefore produce `1, 2, 3` on a
//!   cold start and `4, 5, 6` on the next batch when the host threads the
//!   checkpoint back in, which is unreachable unless the blob crossed the
//!   boundary in both directions.
//! - `multiplier` comes from the `smoketest.multiplier` config key, so a host
//!   setting it to `10` observes `10, 20, 30` and proves
//!   `[pipeline.plugin.config]` reached the plugin.
//!
//! Build it with `cargo build -p saci-plugin-smoketest`.

#![deny(missing_docs)]

use std::sync::Arc;

use saci_plugin::arrow_array::{ArrayRef, Int64Array, RecordBatch};
use saci_plugin::arrow_schema::{DataType, Field, Schema};
use saci_plugin::prelude::*;
use saci_plugin::{ProcessorState, RouteDecision};

/// Config key holding the factor applied to every `seen` value.
const MULTIPLIER_KEY: &str = "smoketest.multiplier";

const COUNTER_ID: FieldRef<Counter> = FieldRef::new("id");
const COUNTER_SEEN: FieldRef<Counter> = FieldRef::new("seen");

/// The data plane: one row per counted item.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Counter {
    /// Caller-assigned identity. No system writes it, so it round-trips.
    pub id: i64,
    /// Sequence position this plugin assigned, continuing across batches.
    pub seen: i64,
}

impl Component for Counter {
    fn name() -> &'static str {
        "Counter"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("seen", DataType::Int64, false),
        ]))
    }
}

/// The plugin's state: how many rows it has numbered so far.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Total {
    /// Rows numbered across every batch of this logical partition.
    pub total: i64,
}

impl Component for Total {
    fn name() -> &'static str {
        "Total"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            false,
        )]))
    }
}

/// Numbers every row and advances the running total.
struct AdvanceSystem;

#[saci_plugin::prelude::async_trait]
impl System for AdvanceSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("advance")
            .reads(COUNTER_ID)
            .writes(COUNTER_SEEN)
    }

    async fn run(&self, dataset: &mut Dataset) -> SaciResult<()> {
        // An absent or unparseable key means 1, so a plugin with no config
        // still produces a clean 1, 2, 3.
        let multiplier = match saci_config_parse::<i64>(MULTIPLIER_KEY) {
            Some(Ok(value)) => value,
            Some(Err(e)) => {
                return Err(SaciError::system_execution(format!(
                    "smoketest: {MULTIPLIER_KEY} is not an integer: {e}"
                )));
            }
            None => 1,
        };

        let batch = dataset
            .columns::<Counter>()
            .ok_or_else(|| SaciError::generic("smoketest: Counter batch not found"))?
            .clone();
        let rows = batch.num_rows();

        // The macro installs the state resource before any system runs, so its
        // absence is an SDK bug rather than a recoverable condition.
        let total = dataset
            .get_resource::<ProcessorState<Total>>()
            .ok_or_else(|| SaciError::generic("smoketest: ProcessorState<Total> resource missing"))?
            .rows
            .first()
            .map_or(0, |t| t.total);

        let seen: Vec<i64> = (0..rows)
            .map(|row| (total + row as i64 + 1) * multiplier)
            .collect();

        let schema = batch.schema();
        let seen_idx = schema
            .index_of(COUNTER_SEEN.field)
            .map_err(|e| SaciError::generic(format!("smoketest: seen missing: {e}")))?;
        let new_seen: ArrayRef = Arc::new(Int64Array::from(seen));

        let columns: Vec<ArrayRef> = (0..schema.fields().len())
            .map(|i| {
                if i == seen_idx {
                    new_seen.clone()
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();

        let new_batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| SaciError::generic(format!("smoketest: batch rebuild: {e}")))?;
        dataset.replace_batch::<Counter>(new_batch)?;

        let state = dataset
            .get_resource_mut::<ProcessorState<Total>>()
            .ok_or_else(|| {
                SaciError::generic("smoketest: ProcessorState<Total> resource missing")
            })?;
        let advanced = total + rows as i64;
        match state.rows.first_mut() {
            Some(existing) => existing.total = advanced,
            None => state.rows.push(Total { total: advanced }),
        }

        saci_plugin::host::metric("smoketest.rows", rows as f64);
        saci_plugin::host::info(
            "smoketest",
            &format!("numbered {rows} rows through {advanced}"),
        );
        Ok(())
    }
}

/// Construct the smoketest pipeline.
///
/// Registers only `Counter`. `Total` is state and must not be registered: the
/// IPC format requires every registered component to hold exactly the dataset's
/// row count, while state rows are independent of batch rows.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("smoketest-plugin");
    pipeline
        .data
        .register_component::<Counter>()
        .expect("register Counter");
    pipeline.add_system(AdvanceSystem);

    // The `smoketest.route` config key ("a" or "a,b") installs a
    // `RouteDecision` resource, so the host delivers this batch's output only
    // to the links whose branch it names. Absent = legacy multicast.
    if let Some(route) = saci_config_get(ROUTE_KEY) {
        let branches: Vec<String> = route.split(',').map(str::to_string).collect();
        pipeline.add_system(system_fn(
            SystemMeta::new("route_batch"),
            move |data: &mut Dataset| {
                data.insert_resource(RouteDecision(branches.clone()));
                Ok(())
            },
        ));
    }
    pipeline
}

/// Config key selecting the branch names this fixture's output is delivered to.
const ROUTE_KEY: &str = "smoketest.route";

saci_plugin::export_plugin!(build, state = Total);
