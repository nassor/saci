---
name: saci-processors
description: Use when writing, building, hosting, testing or documenting a SACI wasm processor: the saci::sdk module and saci-macros, the saci:pipeline WIT package and its vendored copy, the wasmtime host under crates/saci/src/wasm_host/, the Arrow IPC wire format and conformance corpus under packages/arrow-ipc-conformance/, or the wasm, quickstart and conformance examples.
---

# SACI processors

## SDK and WIT

- `saci::sdk` (`crates/saci/src/sdk.rs` and `sdk/{config,error,host,glue,state}.rs`, plus
  `sdk/plugin_rt.rs` for native plugins): the authoring module both SDKs share. `sdk::State<C>`,
  `sdk::RouteDecision`, `sdk::Config`, `sdk::Error`, `sdk::Result` and `sdk::host`
  (`trace`/`debug`/`info`/`warn`/`error`, `metric`, `config`, `config_parse`, `with_config` for unit
  tests). `sdk/glue.rs` holds the hidden `HostIo` trait, `describe` and `run_batch`, reachable as
  `saci::__private::*`, which is the only path the macro expansions name. `saci::prelude` carries
  the macros (`Component`, `transform`, `fold`, `processor` with feature `processor`, `plugin` with
  feature `plugin`) and `sdk`, so a processor crate depends only on
  `saci = { workspace = true, features = ["processor"] }` (`saci::window` is always compiled, so
  window specs need no extra feature). The
  workspace dependency is
  `default-features = false`; outside this repository it is `saci = { version = "0.1",
  default-features = false, features = ["processor"] }`. The crate also owns the canonical WIT
  package at `crates/saci/wit/pipeline.wit`; `saci-macros` vendors a byte-identical copy at
  `crates/saci-macros/wit/pipeline.wit`, because `cargo package` cannot reach outside a crate's own
  directory.
- `saci-processor-smoketest`: a minimal processor component used as a CI fixture: the arrow-ipc
  drift gate, config delivery, and cross-batch state.
- `saci-macros`: the proc-macro crate, `#[derive(Component)]` (`component.rs`), `#[transform]` and
  `#[fold]` (`system.rs`), `#[processor]` (`processor.rs`) and `#[plugin]` (`plugin.rs`),
  re-exported at the `saci` root, so an authoring crate never depends on it directly.

`crates/saci/wit/pipeline.wit` is the canonical `saci:pipeline@0.4.0` WIT package. The processor
exports two functions:

- `describe()`: synchronous export reporting name, version, component schemas, schema fingerprint,
  stateful flag.
- `run-batch(input, prior)`: `async func` (Component Model Async) taking Arrow IPC input and optional
  prior checkpoint bytes, returning Arrow IPC output plus metrics and an updated state blob.

`#[processor]` takes no arguments. It embeds the WIT, gates the bindings and exports on wasm32,
and installs the host backend (`install_host`) that `sdk::host` calls through. It is what every
guest crate uses: `examples/wasm/{order_processing,settle}`, `examples/quickstart/stages/{validate,settle}`,
`examples/branching`, `examples/windowing`, `examples/multi_workflow`, `examples/integrity` and the
smoketest fixture. A guest crate writes no `mod bindings` and no export macro. The attribute goes on
a `fn build() -> Pipeline`, and the pipeline's name (`Pipeline::new("name")`) is the descriptor name
the host checks. State is declared on the pipeline: `.state::<C>()` makes `run_batch` restore a
`sdk::State<C>` resource on the batch dataset before the systems run (`Pipeline::restore_state`) and
serialize it back into `run-result.checkpoint` afterwards (`Pipeline::capture_state`). State is a
resource rather than a registered component because resources do not round-trip through Arrow IPC,
so processor state never leaks into the output; registering the same type with `.component::<C>()`
is a configuration error. A `sdk::RouteDecision` resource (`RouteDecision::to([..])`, or `none()`) is
the routing channel: `run_batch` takes it after the systems run and reports its branch names in
`run-result.routes`, which the host uses to deliver the output only to the links whose `branch`
names one of them (absent = legacy multicast).

By default the host creates a fresh wasmtime `Store` per call, so `prior`/`checkpoint` is the only
channel by which processor state survives a batch boundary. `WasmPipelineRuntime::with_warm_instances(max_idle)`
keeps instantiated processors between calls (default 0 = off; the service builder never sets it): a
call pops an idle instance or builds one, and puts it back only after a successful `run-batch`, so a
trap or a `run-error` drops the instance. Guest linear memory is then a cache only, safe for what
derives from config alone. An idle instance keeps its pool slot. The smoketest's `count = instance`
config key counts batches in guest memory, which is how `wasm_roundtrip` observes instance reuse.

## Host (`crates/saci/src/wasm_host/`, `wasm-host` feature)

`WasmEngine` owns the wasmtime `Engine` (configured with `wasm_component_model(true)`,
`wasm_component_model_async(true)`, `epoch_interruption(true)`), the epoch ticker, and the compiled
programs. Compiling, linking (`wasmtime_wasi::p3::add_to_linker` + `SaciPipeline::add_to_linker`) and
pre-instantiating a component is synchronous and costs about 1.7 s of one fast core for the 4 MB
smoketest, so `WasmEngine::program` does it once per distinct set of bytes and every later load of
the same module is a `memcmp` plus an `Arc` clone, about 0.1 ms. The engine is shareable:
`ServiceBuilder::wasm_engine` hands one engine to several builders, and the ticker stops with
the last clone rather than outliving it. Its `Config` uses wasmtime's pooling instance allocator,
because the WIT contract puts a fresh `Store` and a fresh instantiation on every batch by default: the pool
recycles pre-reserved slots instead of asking the OS for a linear memory per call, and
`table_keep_resident` stops the recycled tables being decommitted and faulted straight back in.
The `POOL_*` constants in `engine.rs` size it for 128 concurrent processor calls; exceeding that
is a `PoolConcurrencyLimitError` at instantiate, not a queue, and each engine reserves 1 TiB of
address space, which bounds how many engines one process can hold. `WasmPipelineRuntime` implements
`saci::runtime::PipelineRuntime` through its `run_batch(data, prior) -> RuntimeOutput`: it drives `instantiate_async` and `describe` through
`pollster::block_on`, serializes the dataset to Arrow IPC, drives Component Model Async
`call_run_batch` inside `store.run_concurrent` via `pollster::block_on` on `spawn_blocking` (keeping
CPU-bound guest compute off Tokio's worker pool so the epoch ticker cannot starve), and reads the
result back. `bindings.rs` is a `wasmtime::component::bindgen!` pointed at `crates/saci/wit` with
`exports: { default: async }`, so host bindings cannot drift.
`host_impl.rs` implements the `host-io` imports (`log`, `metric`, `get-config`); `metric` records
the `saci_processor_metric` histogram, and `runner.rs` records the five `run-metrics` numbers off
every `run-result`.

`wasm-host` is the feature: the wasmtime host, `WasmEngine`, `WasmPipelineRuntime`, the `bindgen!`
host bindings. It is in the `server` bundle, so only a build without it lacks the host; such a
build still parses a `wasm` node and refuses the file by feature name through
`validate_build_capabilities` (skill `saci-service`).

A config-loaded `wasm` node gets its own declared node id (`ServiceBuilder` calls
`loader.load(&spec.id, &spec)`, forwarded into `WasmPipelineRuntime::from_bytes`), which is what
`RuntimeDescriptorInfo::name` reports. Metric attribution rides
`WasmPipelineRuntime::with_identity(workflow_id, processor_id)`, set by `ServiceBuilder` from the
node's own id, and `HostState.processor_id`, which attributes a `host-io::metric` call.
`host-io::metric` names come from processor code, so distinct names are capped at
`MAX_PROCESSOR_METRIC_NAMES` (256) and further names are dropped after one warning.

`processor.batch` is the `debug` span the wasm host opens under `runtime.run`, and
`WasmPipelineRuntime` re-enters its span inside the `spawn_blocking` closure, which is what puts a
processor's `host-io::log` lines in the trace. Processor code reaches `tracing` through the WIT
`host-io::log` import, so field content is untrusted: values are truncated at `MAX_FIELD_BYTES`
(512), records capped at `MAX_FIELDS` (32), and `("truncated","true")` appended when either bites.

A `wasm` node with no `module` takes its runtime from `ServiceBuilder::runtime(id, ..)`, and
`build_processor_node` **removes** that injected runtime from the builder, so `rebuild_blocker`
reports such a workflow as `restartable: false`.

A host that loads a component from code calls `WasmEngine::new()` (`SaciResult`, needs a tokio
runtime for the epoch ticker) and
`WasmPipelineRuntime::from_bytes(engine, name, bytes, config, timeout: Duration)`; the call limit
is `timeout.as_millis().div_ceil(100).max(1)` epoch ticks of 100 ms. `PipelineRuntimeLoader` holds
one engine and a resolver, defaults to 10 s and takes `with_timeout(Duration)`.

## Building a processor

Each Rust processor generates its WIT bindings in-macro through `#[processor]`'s
`wit_bindgen::generate!` expansion, so nothing
has to be produced on disk before `cargo fmt --all -- --check`. The one build `cargo test` needs
is the smoketest component: `crates/saci/tests/{wasm_roundtrip,processor_metrics,
workflow_branching,workflow_metrics,workflow_dag}.rs` all reach it through the shared
`crates/saci/tests/common/smoketest.rs` fixture and assert the artifact exists before
loading it; `connector_matrix.rs` resolves the same path through `Fixtures::resolve`, which
fails naming the build command. `cargo xtask bench wasm_roundtrip` needs the same artifact and
refuses the same way.

```bash
rustup target add wasm32-wasip3
cargo build --release -p saci-processor-smoketest --target wasm32-wasip3
```

No `cargo-component`: `rustc` links a `wasm32-wasip3` cdylib into a WASI 0.3 Component Model
component itself, so plain `cargo build` writes the finished component to
`target/wasm32-wasip3/release/saci_processor_smoketest.wasm` with no preview1 core module and no
adapter step. `.cargo/config.toml` adds `-C target-feature=+simd128` for that target only, so
every processor's core module ships the SIMD proposal; wasmtime enables it by default.

`saci`'s Arrow crates, serde_arrow, serde, futures, async-trait, fnv and rand are unconditional;
the default `engine` feature adds tokio, rayon and num_cpus, and implies `derive` (the
`#[derive(Component)]`, `#[transform]` and `#[fold]` macros), so a `wasm32-wasip3` processor
build takes `default-features = false, features = ["processor"]`. The `processor` feature targets
WASI 0.3 Component Model Async (requiring `wit-bindgen = { workspace = true }` with
`features = ["async"]`, gated on `cfg(target_arch = "wasm32")`) and exposes `Pipeline`, `System`,
`Component` (the trait), `sdk` and the macros through `saci::prelude` without pulling tokio or
`pollster`. The macro expansions name `::saci::__private::...`, which resolves through the one
direct `saci` dependency.

## Bindings convention

A scope that expands `wasmtime::component::bindgen!`/`wit_bindgen::generate!` for `saci:pipeline`
and then names `saci::` at that same scope (`crates/saci/src/wasm_host/bindings.rs` does exactly
this) must not have an extern crate named `saci` in scope: the macro generates a `mod saci` from
the WIT namespace, and an extern crate of the same name makes every unqualified `saci::`
reference there ambiguous (E0659), not a silent shadow. `wasm_host` sits inside the `saci` crate,
which has no extern crate of its own name. A processor crate does depend on `saci`, so the
`#[processor]` expansion wraps `wit_bindgen::generate!` one level down in its own `mod bindings
{ ... }`: the generated `mod saci` nests inside `bindings` and never shares a scope with a bare
`use saci::...`. The author never writes that module.

## Wire format and conformance

`packages/arrow-ipc-conformance/` holds the Arrow IPC stream conformance corpus. Vectors and
manifest are committed, so consumers (such as `examples/plugins/settle-go/internal/arrowipc`)
need no Rust toolchain. Regenerate after any wire format or `Order` schema change, then commit the
result:

```bash
cargo run -p saci --features conformance --example conformance_vectors -- emit
```

The `conformance` feature exists to enable `arrow-ipc/lz4` for the generator alone, so a normal
build still cannot write a compressed record batch and must reject one.

`arrow-ipc = "=60.1.0"` is exact-pinned workspace-wide. It is the host to processor wire format and
the on-disk checkpoint format. See `crates/saci/PINS.md` before touching it. A bump of that pin
regenerates the corpus above and reruns the Go plugin's `arrowipc` suite (`examples/plugins/settle-go`):
arrow-rs writes a zero-length validity buffer for a column with no nulls, so an independent writer
must match arrow-rs's buffer lengths slot for slot.

The wasm host/processor boundary's instrument is `cargo xtask bench wasm_roundtrip`
(`crates/saci/benches/wasm_roundtrip.rs`), which reports store lifecycle, host IPC encode,
host IPC decode and the full `run_batch` round trip as separate groups, since a change
there routinely moves cost between phases while leaving the total flat.

## Quick Start and xtask commands

`examples/quickstart/` needs two Rust WASI 0.3 components that `cargo xtask quickstart` builds for
`wasm32-wasip3`, copies into the gitignored `examples/quickstart/build/` as `validate.wasm`
(`examples/quickstart/stages/validate`, crate `quickstart-validate-wasm`) and `settle.wasm`
(`examples/quickstart/stages/settle`, crate `quickstart-settle-wasm`), and validates with
`wasm-tools` (`1.246.2`, pinned in `crates/saci/PINS.md`). Nothing in `cargo test` depends on either
artifact.

The xtask commands that build and check a processor:

- `cargo xtask quickstart`: install `wasm32-wasip3` if absent, build both Quick Start processors in
  release mode into `examples/quickstart/build/`, and validate both with `wasm-tools`.
- `cargo xtask check-wasm-processor`: `cargo check` `saci` for `wasm32-wasip3` with
  `--no-default-features --features processor`, the combination that keeps tokio out of the
  build and the one the crate docs hand a processor author. It installs the target first.
- `cargo xtask processor-ipc-roundtrip`: drive one Arrow IPC round trip through a component.

## Examples

- `examples/wasm/order_processing/`: a realistic processor pipeline, built for `wasm32-wasip3`.
- `examples/wasm/settle/`: the standalone settlement processor stage (`settle-wasm`), paired with
  `examples/configs/standalone_settle.kdl`.
- `examples/quickstart/`: the runnable Quick Start, NATS to PostgreSQL through two Rust WASI 0.3
  stages (`validate` and `settle`), one `saci-service` process chaining them, compose file and
  `schema.sql`. Built by `cargo xtask quickstart` into the gitignored `examples/quickstart/build/`.
- `examples/conformance/`: `conformance_vectors.rs`, the `saci` example (feature `conformance`)
  that regenerates `packages/arrow-ipc-conformance/`'s corpus.

## Tests

`crates/saci/tests/{wasm_roundtrip,processor_metrics,workflow_branching,workflow_metrics,workflow_dag}.rs`
reach the smoketest through `tests/common/smoketest.rs`; `connector_matrix.rs` resolves it through
`Fixtures::resolve`. `tests/processor_metrics.rs` holds one test because it installs its own meter
provider (skill `saci-service`, Observability). `wasm_chaos` drives the wasm runtime against the
in-memory store fixture and needs no daemon.

## Changing a processor

1. A WIT change edits `crates/saci/wit/pipeline.wit` and the byte-identical copy at
   `crates/saci-macros/wit/pipeline.wit` (`crates/saci-macros/tests/wit_vendored.rs` checks the
   two match).
2. A wire format or `Order` schema change regenerates `packages/arrow-ipc-conformance/` (command
   above) and commits the result.
3. A new processor runtime kind is added to the connector matrix's runtime dimension (skill
   `saci-connectors`).
4. A change at the host/processor boundary is measured with `cargo xtask bench wasm_roundtrip`
   before and after.
5. A page under `docs/content/service/processors/` (skill `saci-docs`).
6. Update this skill.

## Keep this skill current

Update this file in the same change that: changes the WIT package or its exports; changes
`#[processor]`, `#[plugin]`, `#[derive(Component)]`, `#[transform]`, `#[fold]` or `saci::sdk`; changes
`WasmEngine`, `WasmPipelineRuntime`, `PipelineRuntimeLoader`, `PipelineRuntime::run_batch`, `HostState`, the pooling constants or the `host-io` imports;
changes the smoketest fixture or its build; changes the conformance corpus generator or
`packages/arrow-ipc-conformance/`; adds or removes a processor example; changes
`rebuild_blocker`'s `restartable` rule for a `wasm` node with no `module` (also check skill
`saci-service`'s Service layer section, the canonical copy); changes
`WasmPipelineRuntime::with_warm_instances` or the smoketest's `count` config key; changes
`WasmPipelineRuntime::with_identity` or how a processor's metrics or spans are attributed (also
update skill `saci-service`'s Observability and In-process inspector sections, the canonical copy).
