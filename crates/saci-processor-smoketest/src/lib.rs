//! Minimal processor pipeline: a build fixture and Arrow IPC round-trip fixture.
//!
//! It declares one data component and one system, and exercises
//! `saci_processor::export_pipeline!` against `wit-bindgen`-generated bindings.
//!
//! - `Ping` is the data plane. No system touches it, so `run-batch` is an
//!   identity function and any byte difference in the round-tripped Arrow IPC
//!   indicates `arrow-ipc` drift between host and processor.
//! - `Counter` is the cross-batch state, declared via
//!   `export_pipeline!(build, state = Counter)`. It lives in a
//!   `ProcessorState<Counter>` resource, not a registered component, so it never
//!   appears in the output IPC. `count_batches` increments it once per
//!   `run-batch`, so a host threading `run-result.checkpoint` back in as the
//!   next `prior` observes 1, 2, 3, ….
//! - `build()` reads the `greeting` config key through the host-io `get-config`
//!   import and appends it to the pipeline name, so `describe()` proves config
//!   reached the processor.
//!
//! On the host target the `export_pipeline!` invocation is gated out, so the
//! crate compiles as an empty cdylib. The WebAssembly build is `cargo build
//! --release -p saci-processor-smoketest --target wasm32-wasip2`.

#![deny(missing_docs)]

// The bindings are generated in place from `crates/saci-processor/wit`. The
// module is gated on wasm32 because the expansion emits the canonical ABI
// intrinsics and the `component-type` custom section, neither of which the host
// target can link; `#[allow(warnings)]` silences bindgen output noise.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings {
    wit_bindgen::generate!({
        path: "../saci-processor/wit",
        world: "saci-pipeline",
        generate_all,
    });
}

use saci_processor::arrow_schema::{DataType, Field, Schema};
use saci_processor::prelude::*;
use saci_processor::{ProcessorState, RouteDecision};
use std::sync::Arc;

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

/// Increment the batch counter, seeding it on the first batch.
///
/// The state resource is installed by the macro before any system runs, so its
/// absence is a bug in the SDK rather than a recoverable condition.
///
/// The `eprintln!` is load-bearing. It is the only thing in this fixture that
/// touches a WASI import at run time (`wasi:cli/stderr`, `wasi:io/streams`),
/// and the host links the synchronous WASI implementation, whose `in_tokio`
/// bridge calls `Handle::block_on`. Without a processor that reaches for WASI, no
/// test covers the host calling the processor from a thread already driving a tokio
/// runtime, which panics and is how `saci-service` runs. The text itself is
/// discarded: the host builds its `WasiCtx` with no `inherit_*`, so only the
/// invocation matters. A real processor logs through `host-io::log` instead.
fn count_batches(data: &mut Dataset) -> Result<(), SaciError> {
    let state = data
        .get_resource_mut::<ProcessorState<Counter>>()
        .ok_or_else(|| SaciError::generic("smoketest: ProcessorState<Counter> resource missing"))?;

    match state.rows.first_mut() {
        Some(counter) => counter.count += 1,
        None => state.rows.push(Counter { count: 1 }),
    }

    let count = state.rows.first().map_or(0, |c| c.count);
    eprintln!("smoketest: batch {count}");
    Ok(())
}

/// Construct the smoketest pipeline.
///
/// Registers `Ping` (the identity data plane) and adds the single
/// `count_batches` system. The pipeline name carries the `greeting` config
/// value when the host injected one, which is how the round-trip test observes
/// that `[pipeline.wasm.config]` reached the processor.
pub fn build() -> Pipeline {
    // `saci_config_get` is emitted by `export_pipeline!` into this crate and
    // only exists on wasm32, where `crate::bindings` exists.
    #[cfg(target_arch = "wasm32")]
    let greeting = saci_config_get("greeting");
    #[cfg(not(target_arch = "wasm32"))]
    let greeting: Option<String> = None;

    let name = match greeting {
        Some(g) => format!("smoketest-{g}"),
        None => "smoketest".to_string(),
    };

    let mut pipeline = Pipeline::new(name);
    pipeline
        .data
        .register_component::<Ping>()
        .expect("register Ping");
    // `Counter` is deliberately NOT registered: state lives in a resource, so
    // it stays out of `describe()` and out of the output IPC.
    pipeline.add_system(system_fn(SystemMeta::new("count_batches"), count_batches));
    // The `route` config key ("a" or "a,b") installs a `RouteDecision`
    // resource, so the host delivers this batch's output only to the links
    // whose branch it names. Absent = legacy multicast.
    #[cfg(target_arch = "wasm32")]
    let route = saci_config_get(ROUTE_KEY);
    #[cfg(not(target_arch = "wasm32"))]
    let route: Option<String> = None;
    if let Some(route) = route {
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
#[cfg(target_arch = "wasm32")]
const ROUTE_KEY: &str = "route";

// The macro references `crate::bindings`, which only exists on wasm32, so the
// invocation is gated out on the host target.
#[cfg(target_arch = "wasm32")]
saci_processor::export_pipeline!(build, state = Counter);
