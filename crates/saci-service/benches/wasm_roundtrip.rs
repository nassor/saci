// WASM processor host-boundary benchmark
//
// Run through the harness, never bare `cargo bench`:
//
//   cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
//   cargo xtask bench wasm_roundtrip --build-only
//   cargo xtask bench wasm_roundtrip
//
// This is the instrument for `crates/saci-service/src/wasm/`: the store and
// instance lifecycle in `engine.rs`, and the Arrow IPC crossings in
// `runner.rs`. A change to either is adjudicated here before and after.
//
// Phases, not one number. A single end-to-end figure hides the interesting
// half: the pooling instance allocator's first measured form took ~5 µs off
// store teardown and put ~5 µs straight back into the guest call, for a net
// delta of 0.02 µs that read as a wash. Four groups, so a change that moves
// cost between phases shows up as two opposite deltas rather than nothing:
//
//   wasm_store_lifecycle  `HostState` + `Store::new` + `pre.instantiate` +
//                         the guest's `describe` export + `drop(store)`,
//                         driven through `describe()` on a freshly built
//                         runtime, the only public surface that forces an
//                         instantiation. Both halves the allocator touches
//                         (setup and teardown) are inside it.
//   wasm_ipc_encode       `Dataset::write_ipc` into the host's input buffer,
//                         with and without the capacity hint `runner.rs`
//                         carries in `last_ipc_len`. The `hint`/`no_hint`
//                         pair is the A/B for that field.
//   wasm_ipc_decode       `Dataset::read_ipc` over the same stream. The
//                         guest returns the smoketest's data plane unchanged,
//                         so its output stream is byte-identical in shape and
//                         size to the input one.
//   wasm_round_trip       `PipelineRuntime::run_on_with_state` end to end,
//                         through `spawn_blocking` on a multi-thread runtime:
//                         production threading, checkpoint threaded back in
//                         as `prior` so the fixture is stateful.
//
// Two phases are deliberately absent, because neither is reachable from
// outside the crate and neither is worth a probe method on
// `WasmPipelineRuntime` to reach:
//
//   - The guest `run-batch` call on its own. `Store`, instance and the
//     `SaciPipeline` bindings are all private, so the only way to call the
//     export is `run_on_with_state`, which is `wasm_round_trip`. What the
//     guest costs is `wasm_round_trip` minus `wasm_ipc_encode`,
//     `wasm_ipc_decode` and `wasm_store_lifecycle`, plus the
//     `spawn_blocking` hop and minus the
//     `describe` export that `wasm_store_lifecycle` includes; that is a
//     reader's subtraction, not a measurement.
//   - `drop(store)` on its own. It is inside `wasm_store_lifecycle` and
//     cannot be separated from instantiation through the public surface.
//
// Row counts: 1, 1 024 and 65 536. Per-call cost is flat to ~1 024 rows, so
// the first two bracket the fixed cost an unamortised stream deployment pays,
// and 65 536 is past the point where the pooled slot reset and the IPC
// capacity hint both start to matter. 131 072 rows is deliberately not
// swept: its IPC stream exceeds `MAX_LOG_ENTRY_BYTES`, so the distributed
// path rejects a batch that size outright.

#![cfg(all(feature = "service", feature = "wasm"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use saci_core::runtime::PipelineRuntime as _;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::wasm::{WasmEngine, WasmPipelineRuntime};
use serde::{Deserialize, Serialize};

// Installs mimalloc so the benchmark uses the same allocator as the shipped
// binary (`saci-service`'s `mimalloc` feature). The IPC buffers this measures
// are the allocator's work as much as arrow-ipc's.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Rows per batch. Every row-dimensioned group is measured at each.
const ROWS: [usize; 3] = [1, 1_024, 65_536];

/// 60 epoch ticks of 100 ms = 6 s per call, the same budget the test fixture
/// grants. A 65 536-row identity batch is three orders of magnitude inside it.
const EPOCH_TICKS: u64 = 60;

/// Host-side mirror of the `Ping` component the smoketest processor declares
/// (`crates/saci-processor-smoketest/src/lib.rs`). `run-batch` is an identity
/// function over it, so a batch's rows survive the round trip unchanged.
#[derive(Serialize, Deserialize, Clone)]
struct Ping {
    seq: u64,
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

/// Where the release build leaves the smoketest artifact.
fn smoketest_wasm_path() -> PathBuf {
    // CARGO_MANIFEST_DIR points at crates/saci-service, so the workspace root
    // is two levels up.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root above crates/saci-service");
    workspace_root
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join("saci_processor_smoketest.wasm")
}

/// The component bytes, or a refusal naming the one command that produces
/// them. Same contract as `tests/common/smoketest.rs`: a missing fixture is a
/// hard stop, never a silently skipped measurement.
fn smoketest_bytes() -> Vec<u8> {
    let path = smoketest_wasm_path();
    assert!(
        path.exists(),
        "smoketest .wasm not found at {}; \
         run `cargo build --release -p saci-processor-smoketest --target wasm32-wasip2` first",
        path.display()
    );
    std::fs::read(&path).unwrap_or_else(|e| panic!("read smoketest wasm: {e}"))
}

/// A runtime over an engine that has already compiled these bytes, so this
/// costs a cache hit and an `Arc` bump rather than a Cranelift compile.
fn load_runtime(engine: &WasmEngine, bytes: &[u8]) -> WasmPipelineRuntime {
    WasmPipelineRuntime::from_bytes(
        engine.clone(),
        "smoketest",
        bytes,
        HashMap::new(),
        EPOCH_TICKS,
    )
    .expect("WasmPipelineRuntime::from_bytes")
}

/// Seed a dataset from the runtime's own template, so every component the
/// processor declares is registered, then fill the data plane with `rows`
/// `Ping` values.
fn seeded_dataset(runtime: &WasmPipelineRuntime, rows: usize) -> Dataset {
    let mut dataset = runtime.template_dataset();
    let pings: Vec<Ping> = (0..rows).map(|i| Ping { seq: i as u64 }).collect();
    dataset.append::<Ping>(&pings).expect("append Ping rows");
    dataset
}

/// The input IPC stream the host would hand the guest for `dataset`.
fn encode(dataset: &Dataset) -> Vec<u8> {
    let mut buf = Vec::new();
    dataset.write_ipc(&mut buf).expect("write_ipc");
    buf
}

/// Store and instance lifecycle: the phase the instance allocator owns.
///
/// `describe()` is measured on a runtime built in `iter_batched`'s untimed
/// setup, because a runtime caches its descriptor after the first call. The
/// timed region is therefore one whole store lifecycle (`HostState`,
/// `Store::new`, `pre.instantiate`, the guest's `describe` export, and the
/// drop) with the descriptor and the runtime handed back so neither drop
/// lands inside it.
fn bench_store_lifecycle(c: &mut Criterion, engine: &WasmEngine, bytes: &[u8]) {
    let mut group = c.benchmark_group("wasm_store_lifecycle");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("instantiate_describe_drop", |b| {
        b.iter_batched(
            || load_runtime(engine, bytes),
            |runtime| {
                let descriptor = runtime.describe().expect("processor describe");
                (runtime, descriptor)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// The host's two crossings of the data batch, one group each.
///
/// `encode` carries both arms of the capacity hint in `runner.rs`: `hint`
/// pre-sizes the buffer from the previous batch's encoded length, as the
/// shipped path does, and `no_hint` grows from `Vec::new()`, as it did before.
fn bench_ipc(c: &mut Criterion, runtime: &WasmPipelineRuntime) {
    let mut encode_group = c.benchmark_group("wasm_ipc_encode");
    encode_group.measurement_time(Duration::from_secs(5));

    for rows in ROWS {
        let dataset = seeded_dataset(runtime, rows);
        let len = encode(&dataset).len();
        encode_group.throughput(Throughput::Elements(rows as u64));

        encode_group.bench_function(format!("{rows}_rows/hint"), |b| {
            b.iter(|| {
                let mut buf: Vec<u8> = Vec::with_capacity(len);
                dataset.write_ipc(&mut buf).expect("write_ipc");
                std::hint::black_box(buf.len())
            });
        });

        encode_group.bench_function(format!("{rows}_rows/no_hint"), |b| {
            b.iter(|| {
                let mut buf: Vec<u8> = Vec::new();
                dataset.write_ipc(&mut buf).expect("write_ipc");
                std::hint::black_box(buf.len())
            });
        });
    }
    encode_group.finish();

    let mut decode_group = c.benchmark_group("wasm_ipc_decode");
    decode_group.measurement_time(Duration::from_secs(5));

    for rows in ROWS {
        let bytes = encode(&seeded_dataset(runtime, rows));
        decode_group.throughput(Throughput::Elements(rows as u64));

        decode_group.bench_function(format!("{rows}_rows"), |b| {
            b.iter(|| {
                // The runner decodes straight out of a slice, not a `Cursor`.
                let mut slice: &[u8] = std::hint::black_box(&bytes);
                let decoded = Dataset::read_ipc(&mut slice).expect("read_ipc");
                std::hint::black_box(decoded.rows())
            });
        });
    }
    decode_group.finish();
}

/// The whole boundary, through the public surface a runner drives.
///
/// The checkpoint is threaded back in as `prior` on every call, so this is a
/// stateful processor paying its own IPC round trip inside the guest, which is
/// what a stream deployment actually runs.
fn bench_round_trip(
    c: &mut Criterion,
    tokio_rt: &tokio::runtime::Runtime,
    runtime: &WasmPipelineRuntime,
) {
    let mut group = c.benchmark_group("wasm_round_trip");
    group.measurement_time(Duration::from_secs(10));

    for rows in ROWS {
        let mut dataset = seeded_dataset(runtime, rows);
        let mut prior: Option<Vec<u8>> = None;
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_function(format!("{rows}_rows"), |b| {
            b.iter(|| {
                let state = tokio_rt
                    .block_on(runtime.run_on_with_state(&mut dataset, prior.as_deref()))
                    .expect("run_on_with_state");
                prior = state;
                std::hint::black_box(dataset.rows())
            });
        });
    }

    group.finish();
}

/// One engine for every group.
///
/// Not four: each engine's pooling allocator reserves ~1 TiB of address space
/// and compiling this component costs ~1.7 s of Cranelift, so a per-group
/// engine would measure the same thing while risking the process's
/// address-space ceiling.
fn bench_wasm_boundary(c: &mut Criterion) {
    let bytes = smoketest_bytes();

    // Multi-thread, because `run_batch` hands the call to `spawn_blocking` and
    // the cross-thread hop is part of what a deployment pays.
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    // `WasmEngine::new` spawns the epoch ticker, so it needs a runtime
    // context. Nothing below holds an enter guard: the measured regions call
    // `block_on` themselves.
    let engine = tokio_rt.block_on(async { WasmEngine::new().expect("WasmEngine init") });

    // Compiles the component once, outside every measured region, and warms
    // this runtime's descriptor cache.
    let runtime = load_runtime(&engine, &bytes);
    let descriptor = runtime.describe().expect("processor describe");

    // The identity contract every group below depends on: the guest returns
    // the data plane unchanged, so one encoded stream describes both
    // crossings and a round trip leaves the row count alone.
    for rows in ROWS {
        let mut dataset = seeded_dataset(&runtime, rows);
        let input = encode(&dataset);
        let state = tokio_rt
            .block_on(runtime.run_on_with_state(&mut dataset, None))
            .expect("run_on_with_state");
        let checkpoint = state.expect("the smoketest is stateful and must return a checkpoint");
        assert_eq!(dataset.rows(), rows, "smoketest run-batch is an identity");
        println!(
            "[wasm_roundtrip] {} rows, input IPC {} B, output IPC {} B, checkpoint {} B",
            rows,
            input.len(),
            encode(&dataset).len(),
            checkpoint.len(),
        );
    }
    println!(
        "[wasm_roundtrip] processor '{}' v{}, stateful={}",
        descriptor.name, descriptor.version, descriptor.stateful
    );

    bench_store_lifecycle(c, &engine, &bytes);
    bench_ipc(c, &runtime);
    bench_round_trip(c, &tokio_rt, &runtime);
}

criterion_group!(benches, bench_wasm_boundary);
criterion_main!(benches);
