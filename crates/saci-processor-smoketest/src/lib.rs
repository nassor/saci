//! Minimal processor pipeline: a build fixture and Arrow IPC round-trip fixture.
//!
//! It declares one data component and one system, and exercises
//! `#[processor]` against `wit-bindgen`-generated bindings.
//!
//! - `Ping` is the data plane. No system touches it, so `run-batch` is an
//!   identity function and any byte difference in the round-tripped Arrow IPC
//!   indicates `arrow-ipc` drift between host and processor.
//! - `Counter` is the cross-batch state, declared with `.state::<Counter>()`.
//!   It lives in a `sdk::State<Counter>` resource, not a registered component,
//!   so it never appears in the output IPC. `count_batches` increments it once
//!   per `run-batch`, so a host threading `run-result.checkpoint` back in as
//!   the next `prior` observes 1, 2, 3, ….
//! - `build()` reads the `greeting` config key through `sdk::host::config` and
//!   appends it to the pipeline name, so `describe()` proves config reached
//!   the processor.
//!
//! On the host target `#[processor]` emits nothing but `build`, so the crate
//! compiles as an empty cdylib. The WebAssembly build is `cargo build
//! --release -p saci-processor-smoketest --target wasm32-wasip3`.

#![deny(missing_docs)]

use saci::arrow_schema::{DataType, Field, Schema};
use saci::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// A single no-op component so `describe()` has a schema to emit: one `u64`
/// field, with the serde round-trip handled by the default `Component` impl.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Ping {
    /// Monotonic sequence number; exists so the schema has a field.
    pub seq: u64,
}

impl Component for Ping {
    fn name() -> &'static str {
        "Ping"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "seq",
            DataType::UInt64,
            false,
        )]))
    }
}

/// The processor's state: one row holding the number of batches this logical
/// partition has processed.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Counter {
    /// Batches processed, including the current one.
    pub count: u64,
}

impl Component for Counter {
    fn name() -> &'static str {
        "Counter"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }
}

/// Set by `build()` from the `count` config key: `instance` makes `count_batches` count the
/// batches this instance has run instead of threading the count through `prior`.
static COUNT_BY_INSTANCE: AtomicBool = AtomicBool::new(false);

/// Batches this instance has run, kept in the processor's linear memory.
static INSTANCE_BATCHES: AtomicU64 = AtomicU64::new(0);

/// Increment the batch counter, seeding it on the first batch.
///
/// The state resource is installed before any system runs because the
/// pipeline declares `.state::<Counter>()`, so its absence is a bug in the SDK
/// rather than a recoverable condition.
///
/// The `eprintln!` is load-bearing. It is the only thing in this fixture that
/// touches a WASI import at run time (`wasi:cli/stderr`, `wasi:io/streams`),
/// and the host links the synchronous WASI implementation, whose `in_tokio`
/// bridge calls `Handle::block_on`. Without a processor that reaches for WASI, no
/// test covers the host calling the processor from a thread already driving a tokio
/// runtime, which panics and is how `saci-service` runs. The text itself is
/// discarded: the host builds its `WasiCtx` with no `inherit_*`, so only the
/// invocation matters. A real processor logs through `sdk::host::info` instead.
fn count_batches(data: &mut Dataset) -> Result<(), SaciError> {
    let state = data.resource_mut::<sdk::State<Counter>>()?;

    if COUNT_BY_INSTANCE.load(Ordering::Relaxed) {
        // Counts this instance's batches from its own memory, not from `prior`, so a test sees
        // whether the host kept the instance.
        state.rows.clear();
        state.rows.push(Counter {
            count: INSTANCE_BATCHES.fetch_add(1, Ordering::Relaxed) + 1,
        });
    } else {
        match state.rows.first_mut() {
            Some(counter) => counter.count += 1,
            None => state.rows.push(Counter { count: 1 }),
        }
    }

    let count = state.rows.first().map_or(0, |c| c.count);
    eprintln!("smoketest: batch {count}");
    Ok(())
}

/// Construct the smoketest pipeline.
///
/// Registers `Ping` (the identity data plane), declares `Counter` as state
/// and adds the single `count_batches` system. The pipeline name carries the
/// `greeting` config value when the host injected one, which is how the
/// round-trip test observes that the processor's `config` reached it.
#[processor]
pub fn build() -> Pipeline {
    COUNT_BY_INSTANCE.store(
        sdk::host::config("count").as_deref() == Some("instance"),
        Ordering::Relaxed,
    );

    let name = match sdk::host::config("greeting") {
        Some(g) => format!("smoketest-{g}"),
        None => "smoketest".to_string(),
    };

    let mut pipeline = Pipeline::new(name)
        .component::<Ping>()
        .state::<Counter>()
        .system(system_fn(SystemMeta::new("count_batches"), count_batches));
    // The `route` config key ("a" or "a,b") installs a `RouteDecision`
    // resource, so the host delivers this batch's output only to the links
    // whose branch it names. Absent = every link.
    if let Some(route) = sdk::host::config(ROUTE_KEY) {
        let branches: Vec<String> = route.split(',').map(str::to_string).collect();
        pipeline = pipeline.system(system_fn(
            SystemMeta::new("route_batch"),
            move |data: &mut Dataset| {
                data.insert_resource(sdk::RouteDecision::to(branches.iter().cloned()));
                Ok(())
            },
        ));
    }
    pipeline
}

/// Config key selecting the branch names this fixture's output is delivered to.
const ROUTE_KEY: &str = "route";
