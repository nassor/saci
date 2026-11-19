# Changelog

`saci` and `saci-macros` share one version. The release process is in `AGENTS.md`, under
"Releases".

## 0.2.0 - 2026-11-19

### Breaking

- MSRV bumped from `1.99.0` to `1.100.0` (required for `wasm32-wasip3`).
- `wasm-host`: WebAssembly components now require pure WASI 0.3 (`wasmtime_wasi::p3`,
  `wasm_component_model_async(true)`, `saci:pipeline@0.4.0` with `run-batch: async func(...)`).
  WASI 0.2 components (`wasm32-wasip2`, `wasi:*@0.2.x`, `saci:pipeline@0.3.0`) are no longer linked
  or loaded.
- `processor`: `cargo build --target wasm32-wasip3` and `wit-bindgen` with `features = ["async"]`
  are now required; `saci::processor` no longer pulls or re-exports `pollster` and drives
  `pipeline.run_on_with_stats(&mut dataset).await` directly inside `async fn run_batch`.
- Non-Rust WASI 0.2 guest SDK packages (`packages/saci-sdk-{go,py,ts,kt,kt-ksp,cs}`) and the
  `polyglot` examples have been removed because their toolchains do not yet target pure WASI 0.3;
  the native C ABI plugin SDK and Go plugin (`examples/plugins/settle-go/`) remain supported.

### Changed

- The `saci` package carries an empty `[lints]` table. A workspace that depends on saci by path and
  defines `[workspace.lints]` otherwise gets a `missing_lints_inheritance` warning on every build.
- `PipelineSlot::pipeline` returns `&Pipeline` instead of a `MutexGuard`, and `Pipeline` keeps its last-run
  stats behind a mutex instead of a `Cell`, so a pipeline built without `io` (what a processor builds with)
  is `Sync` and sits in a static. The exported async `run-batch` of a processor therefore no longer holds
  a lock across `.await`: a call entering the instance while another was suspended would have trapped on
  it, and clippy warned `await_holding_lock` in every processor crate.

## 0.1.2 - 2026-11-16

### Added

- `wasm-host`: `WasmPipelineRuntime::with_warm_instances(max_idle)` keeps instantiated processors
  between calls, so a processor can cache what it derives from its config.

## 0.1.1 - 2026-11-14

### Added

- Each crate has its own README as its crates.io page, with a dependency line and an example, plus
  keywords and categories. The `saci` README's example runs as a doc test.
- The `saci` package carries nine runnable examples in `examples/`: `first_pipeline`,
  `scheduler_etl`, `scheduler_etl_parallel`, `scheduler_dag`, `scheduler_parquet_etl`,
  `distributed_scheduler`, `window_aggregation`, `distributed_windowed` and `windowed_fan_in`.

### Changed

- `arrow-arith`, `fnv` and `futures` are optional dependencies. `windows` enables `arrow-arith`
  and `fnv`, `distributed` enables `fnv`, and `service` and `connector-udp` enable `futures`, so a
  build with only the default `engine` feature compiles none of them.

## 0.1.0 - 2026-11-14 (yanked)

### Added

- First release of `saci` and `saci-macros`. `saci` holds the columnar engine, the connectors,
  the byte formats, the processor and plugin SDKs and the `saci-service` host, each behind a
  feature; `saci-macros` holds the processor SDK's derive and attribute macros.

Yanked because the package carried no examples and compiled three dependencies that the default
build never used. Use 0.1.1.
