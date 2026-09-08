---
name: saci-processors
description: Use when writing, building, hosting, testing or documenting a SACI wasm processor: the saci-processor SDK and saci-macros, the saci:pipeline WIT package and its vendored copies, the wasmtime host under crates/saci-service/src/wasm/, the Arrow IPC wire format and conformance corpus, the polyglot SDKs under packages/, or the wasm, polyglot and quickstart examples.
---

# SACI processors

## SDK and WIT

- `saci-processor`: the processor SDK. Re-exports saci-core, the `export_pipeline!` macro, and
  `saci_macros::{Component, transform, fold, processor}` plus `Config`/`Error`/`Result`, so a
  processor crate depends only on `saci-processor`. Owns the canonical WIT package at
  `wit/pipeline.wit`; `saci-service` and `saci-macros` each vendor a byte-identical copy at their
  own `wit/pipeline.wit`, because `cargo package` cannot reach outside a crate's own directory.
- `saci-processor-smoketest`: a minimal processor component used as a CI fixture: the arrow-ipc
  drift gate, config delivery, and cross-batch state.
- `saci-macros`: the proc-macro crate, `#[derive(Component)]`, `#[transform]`, `#[fold]`,
  `#[processor]`, re-exported through `saci-processor`, so a processor crate never depends on it
  directly.

`wit/pipeline.wit` is the canonical `saci:pipeline@0.3.0` WIT package. The processor exports
exactly two functions:

- `describe()`: name, version, component schemas, schema fingerprint, stateful flag.
- `run-batch(input, prior)`: Arrow IPC in, Arrow IPC out, plus metrics and an updated state blob.

The `#[processor]` attribute macro (`saci-macros`, re-exported through `saci-processor`) is the
zero-ceremony path: it embeds the WIT, auto-cfgs the wasm32 target, and emits the log/metric/config
helpers, and is what `examples/polyglot/stages/rust-settle` uses. `export_pipeline!` is the
explicit wiring macro, used by `examples/wasm/order_processing`, `examples/branching`,
`examples/windowing`, `examples/multi_workflow` and the smoketest fixture.
`export_pipeline!(build)` wires a `fn() -> Pipeline` to those exports and emits `saci_config_get` and
`saci_config_parse` into the caller's crate. The WIT bindings are caller-side, so the accessors must
be too. `export_pipeline!(build, state = C)` also installs a `ProcessorState<C>` resource on the
batch dataset before the systems run and serializes it back into `run-result.checkpoint` afterwards.
State is a resource rather than a registered component because resources do not round-trip through
Arrow IPC, so processor state never leaks into the output. A `RouteDecision` resource is the routing
channel: the macro reads it after the systems run and reports its branch names in
`run-result.routes`, which the host uses to deliver the output only to the links whose `branch`
names one of them (absent = legacy multicast).

The host creates a fresh wasmtime `Store` per call, so `prior`/`checkpoint` is the only channel by
which processor state survives a batch boundary.

## Host (`crates/saci-service/src/wasm/`, `wasm` feature)

`WasmEngine` owns the wasmtime `Engine`, the epoch ticker, and the compiled programs. Compiling,
linking and pre-instantiating a component is synchronous and costs about 1.7 s of one fast core for
the 4 MB smoketest, so `WasmEngine::program` does it once per distinct set of bytes and every later
load of the same module is a `memcmp` plus an `Arc` clone, about 0.1 ms. The engine is shareable:
`ServiceBuilder::with_wasm_engine` hands one engine to several builders, and the ticker stops with
the last clone rather than outliving it. Its `Config` uses wasmtime's pooling instance allocator,
because the WIT contract puts a fresh `Store` and a fresh instantiation on every batch: the pool
recycles pre-reserved slots instead of asking the OS for a linear memory per call, and
`table_keep_resident` stops the recycled tables being decommitted and faulted straight back in.
The `POOL_*` constants in `engine.rs` size it for 128 concurrent processor calls; exceeding that
is a `PoolConcurrencyLimitError` at instantiate, not a queue, and each engine reserves 1 TiB of
address space, which bounds how many engines one process can hold. `WasmPipelineRuntime` implements
`saci_core::runtime::PipelineRuntime`: it serializes the dataset to Arrow IPC, calls the processor's
`run-batch` on a fresh `Store`, and reads the result back. `bindings.rs` is 26 lines of
`wasmtime::component::bindgen!` pointed at `../saci-processor/wit`, so host bindings cannot drift.
`host_impl.rs` implements the `host-io` imports (`log`, `metric`, `get-config`); `metric` records
the `saci_processor_metric` histogram, and `runner.rs` records the five `run-metrics` numbers off
every `run-result`.

`wasm` is the feature: the wasmtime host, `WasmEngine`, `WasmPipelineRuntime`, the `bindgen!` host
bindings. It is in `saci-service`'s default bundle, so only a build that trimmed it lacks the
host; such a build still parses a `wasm` node and refuses the file by feature name through
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

A `wasm` node with no `module` takes its runtime from `with_runtime(id, ..)`, and
`build_processor_node` **removes** that injected runtime from the builder, so `rebuild_blocker`
reports such a workflow as `restartable: false`.

## Building a processor

Each Rust processor generates its WIT bindings in-macro via `wit_bindgen::generate!`, so nothing
has to be produced on disk before `cargo fmt --all -- --check`. The one build `cargo test` needs
is the smoketest component: `crates/saci-service/tests/{wasm_roundtrip,processor_metrics,
workflow_branching,workflow_metrics,workflow_dag}.rs` all reach it through the shared
`crates/saci-service/tests/common/smoketest.rs` fixture and assert the artifact exists before
loading it; `connector_matrix.rs` resolves the same path through `Fixtures::resolve`, which
fails naming the build command. `cargo xtask bench wasm_roundtrip` needs the same artifact and
refuses the same way.

```bash
rustup target add wasm32-wasip2
cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
```

No `cargo-component`: `rustc` links a `wasm32-wasip2` cdylib into a Component Model component
itself, so plain `cargo build` writes the finished component to
`target/wasm32-wasip2/release/saci_processor_smoketest.wasm` with no preview1 core module and no
adapter step. `.cargo/config.toml` adds `-C target-feature=+simd128` for that target only, so
every processor's core module ships the SIMD proposal; wasmtime enables it by default.

`saci-core`'s Arrow crates, serde_arrow, serde, futures, async-trait, fnv and rand are
unconditional; the default `runtime` feature adds tokio, rayon and num_cpus, so a wasm32-wasip2
processor build takes `--no-default-features --features processor`. That `saci-core` feature is
`processor`: wasm32-wasip2 target, sequential-only execution driven by the `pollster` sync
executor.

The `saci` facade carries the same path behind one crate name:

- `processor`: `Pipeline`, `System`, `Component` (the trait, not the derive), `Config`, `Error`,
  `Result` and `export_pipeline!`, for `--no-default-features --features processor`: saci-core
  then carries `processor` instead of `engine`'s `runtime`, so the build never needs tokio on
  wasm32-wasip2. Does not carry `#[derive(Component)]`/`#[transform]`/`#[fold]`/`#[processor]`:
  their expansions name `saci-processor` literally, which resolves only as a direct dependency,
  not one reached through this facade; a processor crate using those macros depends on
  `saci-processor` directly.

## Bindings convention

A crate that expands `wasmtime::component::bindgen!`/`wit_bindgen::generate!` for
`saci:pipeline` and then names `saci::` at that same scope (`saci-service`'s
`src/wasm/bindings.rs` does exactly this) must not itself depend on the `saci` crate: the
macro generates a `mod saci` from the WIT namespace, and an extern crate of the same name
makes every unqualified `saci::` reference there ambiguous (E0659), not a silent shadow. A
`wit_bindgen::generate!` wrapped one level down in its own `mod bindings { ... }` (every
processor crate's own pattern) never hits this: the generated `mod saci` nests inside
`bindings`, so it never shares a scope with a bare `use saci::...`.

## Wire format and conformance

`packages/arrow-ipc-conformance/` pins all five Arrow IPC codecs to one answer about which streams
are valid. Vectors and manifest are committed, so a codec's suite needs no Rust toolchain.
Regenerate after any wire format or `Order` schema change, then commit the result:

```bash
cargo run -p saci-service --features conformance --example conformance_vectors -- emit
```

The `conformance` feature exists to enable `arrow-ipc/lz4` for the generator alone, so a normal
build still cannot write a compressed record batch and must reject one.

`arrow-ipc = "=59.3.0"` is exact-pinned workspace-wide. It is the host to processor wire format and
the on-disk checkpoint format. See `crates/saci-processor/PINS.md` before touching it.

The wasm host/processor boundary's instrument is `cargo xtask bench wasm_roundtrip`
(`crates/saci-service/benches/wasm_roundtrip.rs`), which reports store lifecycle, host IPC encode,
host IPC decode and the full `run_on_with_state` round trip as separate groups, since a change
there routinely moves cost between phases while leaving the total flat.

## Polyglot SDKs

`packages/` is an Apache-2.0 subtree, unlike the rest of the repository. One SDK package per
language, released as `saci-sdk`; each SDK carries its Arrow IPC codec internally, so the five
non-Rust polyglot stages consume one package apiece.

- `saci-sdk-go`: module `github.com/nassor/saci/packages/saci-sdk-go`, package `saci`, codec
  subpackage `arrowipc`.
- `saci-sdk-py`: `saci-sdk`, import `saci_sdk`, codec submodule `saci_sdk.arrow_ipc`.
- `saci-sdk-ts`: `@nassor/saci-sdk`, codec module `src/arrow_ipc.ts`.
- `saci-sdk-kt`: `io.github.nassor:saci-sdk-kt`, wasmWasi and jvm targets, codec package
  `io.github.nassor.saci.arrowipc`.
- `saci-sdk-kt-ksp`: `io.github.nassor:saci-sdk-kt-ksp`, the JVM-only KSP export-glue processor.
- `saci-sdk-cs`: `Saci.Sdk`, codec namespace `Saci.ArrowIpc`; `generator/` is its Roslyn source
  generator for the export glue.
- `packages/VERSION`: the one version all five declare. `cargo xtask pack-sdk` asserts every
  manifest matches it.

### Quick Start component prerequisites

`examples/quickstart/` needs two components that `cargo xtask quickstart` produces into the
gitignored `examples/quickstart/build/`: `validate-go.wasm` (the unmodified
`examples/polyglot/stages/go-validate`) and `settle-cs.wasm`
(`examples/quickstart/stages/csharp-settle`). Three toolchains, all pinned in
`examples/polyglot/PINS.md`: `componentize-go` 0.4.1 with Go 1.25.5+, .NET SDK 10, and `wasm-tools`
1.246.2. Nothing in `cargo test` depends on either artifact.

The xtask commands that build and check a processor:

- `cargo xtask polyglot`: build the six polyglot processors.
- `cargo xtask quickstart`: build the two Quick Start processors.
- `cargo xtask pack-sdk`: package the SDKs under `packages/`, asserting every manifest matches
  `packages/VERSION`.
- `cargo xtask check-wasm-processor`: `cargo check` both `saci-core` and the `saci` facade for
  wasm32-wasip2 with `--no-default-features --features processor`, the combination that keeps
  tokio out of the build and the one the facade's crate docs hand a processor author. It installs
  the target first.
- `cargo xtask processor-ipc-roundtrip`: drive one Arrow IPC round trip through a component.

## Examples

- `examples/wasm/order_processing/`: a realistic processor pipeline, built for wasm32-wasip2.
- `examples/polyglot/`: six processor components (Go, Python, TypeScript, Kotlin, C#, Rust)
  against one WIT world. Each of the six declares its own `Order` row type on its language SDK (Go
  struct tags, Python dataclass, TS schema builder, Kotlin data class, C# class, Rust derive).
  `generated/` is regenerated by the `polyglot_schema_emit` example for the Quick Start and plugin
  builds only; the driver asserts the six fingerprints agree pairwise. See `PINS.md` there.
- `examples/quickstart/`: the runnable Quick Start, NATS to PostgreSQL through the reused Go stage
  and a purpose-built C# stage, one `saci-service` process chaining them, compose file and
  `schema.sql`. Built by `cargo xtask quickstart` into the gitignored `build/`.
- `examples/conformance/`: `conformance_vectors.rs`, the `saci-service` example that regenerates
  `packages/arrow-ipc-conformance/`'s corpus.

## Tests

`crates/saci-service/tests/{wasm_roundtrip,processor_metrics,workflow_branching,workflow_metrics,workflow_dag}.rs`
reach the smoketest through `tests/common/smoketest.rs`; `connector_matrix.rs` resolves it through
`Fixtures::resolve`. `tests/processor_metrics.rs` holds one test because it installs its own meter
provider (skill `saci-service`, Observability). `wasm_chaos` drives the wasm runtime against the
in-memory store fixture and needs no daemon.

## Changing a processor

1. A WIT change edits `crates/saci-processor/wit/pipeline.wit` and the byte-identical copies at
   `crates/saci-service/wit/pipeline.wit` and `crates/saci-macros/wit/pipeline.wit`, then every SDK
   under `packages/` and the six polyglot stages.
2. A wire format or `Order` schema change regenerates `packages/arrow-ipc-conformance/` (command
   above) and commits the result.
3. A new processor runtime kind is added to the connector matrix's runtime dimension (skill
   `saci-connectors`).
4. An SDK change bumps `packages/VERSION`; every manifest must match, and `cargo xtask pack-sdk`
   asserts it.
5. A change at the host/processor boundary is measured with `cargo xtask bench wasm_roundtrip`
   before and after.
6. A page under `docs/content/service/processors/` or `processors/build/<language>` (skill
   `saci-docs`).
7. Update this skill.

## Keep this skill current

Update this file in the same change that: changes the WIT package or its exports; changes
`export_pipeline!`, `#[processor]`, `#[derive(Component)]`, `#[transform]` or `#[fold]`; changes
`WasmEngine`, `WasmPipelineRuntime`, `HostState`, the pooling constants or the `host-io` imports;
changes the smoketest fixture or its build; changes the conformance corpus generator; adds, removes
or re-pins an SDK under `packages/`; adds or removes a processor example; changes
`rebuild_blocker`'s `restartable` rule for a `wasm` node with no `module` (also check skill
`saci-service`'s Service layer section, the canonical copy); changes
`WasmPipelineRuntime::with_identity` or how a processor's metrics or spans are attributed (also
update skill `saci-service`'s Observability and In-process inspector sections, the canonical copy).
