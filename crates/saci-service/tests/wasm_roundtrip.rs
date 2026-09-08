//! Host↔processor contract tests against the saci-processor-smoketest WebAssembly
//! component. `cargo xtask processor-ipc-roundtrip` runs the two steps:
//!
//! 1. `cargo build --release -p saci-processor-smoketest --target wasm32-wasip2`
//! 2. `cargo test --test wasm_roundtrip -p saci-service --features wasm`
//!
//! `rustc` links the cdylib into a component directly for that target, so the
//! artifact under `target/wasm32-wasip2/release/` is the one the host loads: no
//! preview1 core module and no adapter anywhere in the pipeline.
//!
//! Three properties are covered:
//!
//! - **Arrow IPC byte-exactness.** No system touches `Ping` and the processor keeps
//!   its state in a resource, so the round-tripped IPC bytes must be identical.
//!   Host and processor both take the pinned `arrow-ipc = "=59.3.0"` through
//!   `workspace = true`, so differing bytes mean the pin has drifted on one side.
//! - **Config delivery.** The processor reads the `greeting` key through the
//!   `host-io` `get-config` import and appends it to the pipeline name, so
//!   `describe()` reports whether `[pipeline.wasm.config]` reached the processor.
//! - **State across batches.** The processor is exported with
//!   `export_pipeline!(build, state = Counter)`. The host creates a fresh
//!   wasmtime Store per call, so the counter can only reach 1, 2, 3 by threading
//!   each `run-batch` checkpoint back in as the next `prior`.

#![cfg(feature = "wasm")]

use std::collections::HashMap;

use saci_core::runtime::PipelineRuntime;
use saci_service::component::Component;
use saci_service::dataset::Dataset;

use arrow_array::UInt64Array;

#[path = "common/smoketest.rs"]
mod smoketest;

use smoketest::{Counter, load_runtime, seeded_dataset};

/// Decode a `run-result.checkpoint` blob and read the counter out of it.
fn counter_in_blob(blob: &[u8]) -> Option<u64> {
    let state = Dataset::read_ipc(&mut &blob[..]).expect("decode state blob");
    counter_value(&state)
}

fn counter_value(dataset: &Dataset) -> Option<u64> {
    let batch = dataset.batch_for(Counter::name())?;
    if batch.num_rows() == 0 {
        return None;
    }
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("Counter.count is a u64 column");
    Some(col.value(0))
}

#[tokio::test(flavor = "current_thread")]
async fn smoketest_arrow_ipc_round_trip_is_byte_exact() {
    let runtime = load_runtime(HashMap::new());

    let descriptor = runtime.describe().expect("processor describe");
    assert_eq!(descriptor.name, "smoketest");
    let declared: Vec<&str> = descriptor
        .components
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        declared,
        vec!["Ping"],
        "the state component lives in a resource, so describe() declares only Ping"
    );
    assert!(
        descriptor.stateful,
        "a `state = Counter` processor must declare itself stateful"
    );
    assert!(
        !descriptor.components[0].arrow_schema_ipc.is_empty(),
        "processor must emit non-empty Arrow IPC schema bytes for Ping"
    );

    let mut dataset = seeded_dataset(&runtime, 16);

    let host_fingerprint_hex = format!("{:08x}", dataset.schemas().fingerprint());
    assert_eq!(
        descriptor.schema_fingerprint, host_fingerprint_hex,
        "processor schema fingerprint ({}) must match host fingerprint ({})",
        descriptor.schema_fingerprint, host_fingerprint_hex
    );

    // The trait-level view of the same record. `PipelineRuntimeLoader::load`
    // warms the cache with an eager `describe()`, so this needs no second
    // processor round trip and must agree with the descriptor above.
    let info = saci_core::runtime::PipelineRuntime::descriptor_info(&runtime);
    assert_eq!(info.version, descriptor.version);
    assert_eq!(info.stateful, descriptor.stateful);
    assert_eq!(info.schema_fingerprint, descriptor.schema_fingerprint);

    let mut before: Vec<u8> = Vec::new();
    dataset.write_ipc(&mut before).expect("write_ipc before");
    let before_rows = dataset.rows();

    // No system touches Ping and the counter lives in a resource, so the run is
    // an identity pass over the whole dataset.
    runtime
        .run_on(&mut dataset)
        .await
        .expect("processor run_on success");

    let mut after: Vec<u8> = Vec::new();
    dataset.write_ipc(&mut after).expect("write_ipc after");
    let after_rows = dataset.rows();

    assert_eq!(
        before_rows, after_rows,
        "row count must survive the round-trip exactly"
    );

    assert_eq!(
        before, after,
        "Arrow IPC bytes must be byte-exact across the host↔processor round-trip; \
         if this assertion fails, arrow-ipc has drifted between saci-core and saci-processor"
    );
}

/// `[pipeline.wasm.config]` values reach the processor through the `host-io`
/// `get-config` import, including during `describe()`.
#[tokio::test(flavor = "current_thread")]
async fn processor_reads_host_injected_config() {
    let without = load_runtime(HashMap::new());
    assert_eq!(
        without.describe().expect("describe").name,
        "smoketest",
        "an empty config table means the processor sees no `greeting` key"
    );

    let with = load_runtime(HashMap::from([(
        "greeting".to_string(),
        "hello".to_string(),
    )]));
    assert_eq!(
        with.describe().expect("describe").name,
        "smoketest-hello",
        "the processor must observe the injected `greeting` value"
    );
}

/// Threading `run-result.checkpoint` back in as the next `prior` is the only way
/// processor state survives: the host builds a fresh wasmtime Store, and so fresh
/// processor linear memory, for every call.
#[tokio::test(flavor = "current_thread")]
async fn processor_state_survives_across_batches() {
    let runtime = load_runtime(HashMap::new());

    let mut prior: Option<Vec<u8>> = None;
    for expected in 1..=3u64 {
        let mut dataset = seeded_dataset(&runtime, 4);

        let next = runtime
            .run_on_with_state(&mut dataset, prior.as_deref())
            .await
            .expect("processor run_on_with_state success")
            .expect("a stateful processor must return a checkpoint blob");

        assert_eq!(
            counter_value(&dataset),
            None,
            "batch {expected}: state must not leak into the output dataset"
        );

        assert_eq!(
            counter_in_blob(&next),
            Some(expected),
            "batch {expected}: the checkpoint blob must carry the updated counter"
        );

        prior = Some(next);
    }
}

/// Without the blob the processor starts from scratch on every batch.
#[tokio::test(flavor = "current_thread")]
async fn processor_state_resets_when_prior_is_dropped() {
    let runtime = load_runtime(HashMap::new());

    for _ in 0..3 {
        let mut dataset = seeded_dataset(&runtime, 4);
        let blob = runtime
            .run_on_with_state(&mut dataset, None)
            .await
            .expect("processor run_on_with_state success")
            .expect("a stateful processor must return a checkpoint blob");
        assert_eq!(
            counter_in_blob(&blob),
            Some(1),
            "with no prior, every batch is the processor's first"
        );
    }
}

/// `run_on` must survive being awaited on a **multi-threaded** tokio runtime,
/// which is what `#[tokio::main]` gives `saci-service`.
///
/// The processor is linked against the synchronous WASI implementation
/// (`add_to_linker_sync`), so every WASI import the processor touches funnels into
/// `wasmtime_wasi::runtime::in_tokio` and then `Handle::block_on`. Called from a
/// thread that is already driving a tokio runtime, that panics with "Cannot start
/// a runtime from within a runtime", so `WasmPipelineRuntime::run_on_with_state`
/// hands the call to `spawn_blocking`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_on_works_on_a_multi_thread_runtime() {
    let runtime = load_runtime(HashMap::new());
    let mut dataset = seeded_dataset(&runtime, 3);
    let before = dataset.rows();

    runtime
        .run_on(&mut dataset)
        .await
        .expect("run_on must not panic or fail on a multi-thread runtime");

    assert_eq!(
        dataset.rows(),
        before,
        "the smoketest is an identity pipeline, so the row count must survive"
    );
}

/// Concurrent processor calls one engine's instance pool is sized for.
///
/// Mirrors `POOL_CALLS` in `crates/saci-service/src/wasm/engine.rs`, which is
/// private. The two must move together: lowering the constant without
/// lowering this number turns the test below red, which is the intended
/// signal.
const POOL_CALLS: usize = 128;

/// Rows per concurrent call. Enough that a call is doing real work (encode,
/// instantiate, guest, decode) while the other 127 are being launched, and
/// small enough that 128 live datasets are a few megabytes.
const CONCURRENT_ROWS: usize = 1_024;

/// Every one of the pool's documented concurrent calls must succeed.
///
/// `engine.rs` configures the wasmtime pooling instance allocator for
/// `POOL_CALLS` concurrent calls, at fixed multipliers of core instances,
/// memories and tables per call. Exhausting any of those limits is a hard
/// `PoolConcurrencyLimitError`, with no queueing and no fallback to on-demand
/// allocation. It reaches the caller as
/// `SaciError::SystemExecution("processor trap (instantiate): ...")` and fails
/// the batch. That promise had no test; this is it.
///
/// # Why the full ceiling and not a smaller N
///
/// N concurrent calls consume N times what one call really needs of each
/// pooled resource, so a run at N only proves per-call usage is within
/// `POOL_CALLS / N` times the declared multiplier. The smoketest needs one
/// component instance per call and the pool holds exactly `POOL_CALLS` of
/// them, so **only N = `POOL_CALLS` pins the multipliers to what is
/// declared**: at N = 64 a future component splitting into 16 core instances
/// per call, twice `POOL_CORE_INSTANCES_PER_CALL`, would still pass. Since
/// that is precisely the regression this test exists to catch, N is the whole
/// ceiling. The cost is 128 blocking threads and a few megabytes, and the
/// suite pays it once.
///
/// # What it does not prove
///
/// That the 129th concurrent call fails. Raising N to 256 does produce
/// `processor trap (instantiate): maximum concurrent limit of 128 for
/// component instances reached`, which is how the overlap this test relies on
/// was confirmed, and it also confirms component instances are the resource
/// that binds first. It is not asserted: which call fails depends on how the
/// OS interleaves 256 threads, and a deterministic exhaustion test needs an
/// engine built with a low limit. `WasmEngine::new` takes no configuration and
/// the limits are private constants, deliberately, so there is no public way
/// to build a one-slot engine and no probe method is added here to fake one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pools_documented_concurrency_ceiling_holds() {
    let runtime = load_runtime(HashMap::new());

    // Warm `POOL_CALLS` blocking threads first. `run_batch` hands each call to
    // `spawn_blocking`, and tokio grows that pool one thread per queued task:
    // on a cold pool the early calls would finish and hand their pool slots
    // back while later threads were still being spawned, so the calls would
    // never all be in flight and the test would assert nothing.
    futures::future::join_all((0..POOL_CALLS).map(|_| {
        tokio::task::spawn_blocking(|| std::thread::sleep(std::time::Duration::from_millis(50)))
    }))
    .await;

    let mut datasets: Vec<Dataset> = (0..POOL_CALLS)
        .map(|_| seeded_dataset(&runtime, CONCURRENT_ROWS))
        .collect();

    // One `join_all` poll pass enters all `POOL_CALLS` futures before the
    // executor can complete any of them, so every call has submitted its
    // blocking job, and with the pool warm taken a thread, before the first
    // one returns a store to the pool.
    let results = futures::future::join_all(
        datasets
            .iter_mut()
            .map(|dataset| runtime.run_on_with_state(dataset, None)),
    )
    .await;

    for (i, (result, dataset)) in results.iter().zip(datasets.iter()).enumerate() {
        let state = result.as_ref().unwrap_or_else(|e| {
            panic!(
                "call {i} of {POOL_CALLS} concurrent calls failed: {e}\n\
                 a `PoolConcurrencyLimitError` here means one call now needs more pooled \
                 slots than engine.rs reserves per call, or that POOL_CALLS was lowered \
                 below the number this test drives"
            )
        });
        let blob = state.as_deref().unwrap_or_else(|| {
            panic!("call {i} returned no checkpoint; the smoketest is stateful")
        });
        // Every call started from `prior = None`, so each must have counted
        // exactly one batch. A shared or recycled store leaking state between
        // two simultaneous calls would read 2 here.
        assert_eq!(
            counter_in_blob(blob),
            Some(1),
            "call {i} saw another concurrent call's processor state"
        );
        assert_eq!(
            dataset.rows(),
            CONCURRENT_ROWS,
            "call {i} came back with the wrong row count; the smoketest is an identity"
        );
    }
}
