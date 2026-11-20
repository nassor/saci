# Changelog

`saci` and `saci-macros` share one version. The release process is in `AGENTS.md`, under
"Releases".

## Unreleased

### Breaking

- Features: `runtime` and `io` are folded into `engine`; a feature that implied either implies
  `engine`. A new `derive` feature carries `#[derive(Component)]` and is part of `engine`,
  `processor` and `plugin`.
- `#[derive(Component)]` works in every build (`saci::Component`, same name as the trait) and
  declares one `FieldRef` constant per field, named after the field in upper case
  (`Order::USD_AMOUNT`) and pointing at its serde column name. `#[saci(fields = false)]` opts out.
  The derived `schema()` is traced once per process.
- `Dataset`: `register_component` is `register`; `register_raw_component`,
  `register_raw_component_versioned` and `register_named_component` are `register_schema` and
  `register_schema_versioned`, which take any `&str` and return an error instead of panicking.
  Reads return `SaciResult`: `columns::<C>()` is `batch::<C>()`, `batch_for(name)` is
  `batch_named(name)`, `column::<C>("f")` is `column(C::F)` or `column_named("C", "f")`. New
  `resource`, `resource_mut`, `take_resource` and `set_column`. `DatasetBuilder` is removed.
- `SaciError`: `ComponentNotFound` and `EntityNotFound` are replaced by `ComponentNotRegistered` and
  `FieldNotFound`; `is_retryable()` is new and `RetryExhausted` exposes its cause through
  `std::error::Error::source`.
- Retry: `SystemConfig` is removed. `System::retry()` and `ParallelSystem::retry()` return a
  `RetryMode`, which defaults to `RetryMode::None`, and only an error whose `is_retryable()` is true
  is retried. `RetryExhausted` is returned only after at least one retry ran; otherwise the error
  comes back unwrapped. `RetryingSource`/`RetryingSink` and `run_with_retries*` take a `RetryMode`;
  `service::RetryConfig::to_system_config` is `to_retry_mode`.
- `SystemMeta`: `read`/`write` are `reads_named`/`writes_named`, `read_component`/`write_component`
  are `reads_all::<C>()`/`writes_all::<C>()` or `reads_all_named`/`writes_all_named`,
  `read_resource`/`write_resource` are `reads_resource`/`writes_resource`; the fields are private
  (`name()` reads the name). `FieldRef::field` is `FieldRef::name()`.
- `WriteSet`/`SliceWriteSet`: `put` takes a `FieldRef`, `put_named` takes strings,
  `with_resource` takes the value; the fields are private behind `len`, `is_empty`, `get_named`,
  `iter` and `SliceWriteSet::rows`. `ResourceUpdate` is no longer public.
- `ParallelSystem`: `run_slice` returns `SaciResult<SliceWriteSet>` and is used when `sliced()`
  returns `true`; `run` has a default that runs one slice over every row. New `run_sync` and
  `parallel_fn`.
- `Pipeline` is built with a consuming chain from `Pipeline::new`: `component`, `schema`,
  `resource`, `system`, `parallel_system`, `source`, `sink`. `PipelineBuilder`, the public `data`
  field, the `add_*` methods, `run_with_io`, `run_on_with_stats`, `last_stats`, `stages` and
  `stage_count` are removed. `run`, `run_on` and `run_stream` return the call's `RunStats`
  (`elapsed: Duration`, `retries`); `run` drains attached sources and fills attached sinks;
  `plan()` returns the stage plan.
- `Scheduler` is chained: `pipeline`, `pipeline_with`. `PipelineConfig::after(name)`,
  `after_data(name)` and `pause_when(f)` replace `after(name, DependencyKind)` and
  `BackpressureSpec`; a pipeline added after a tick runs on the next one.
- Root exports: connector and transformer types are named by module path
  (`saci::connector::file::FileSource`); `saci::async_trait` is re-exported, so a crate needs no
  `async-trait` dependency; `saci::partition` and `saci::resource` are private.
- Authoring SDK: `saci::processor` and `saci::plugin` are replaced by one `saci::sdk` module
  (`sdk::Config`, `sdk::Error`, `sdk::Result`, `sdk::State`, `sdk::RouteDecision`, `sdk::host`),
  and every macro is reached through `saci::prelude::*`. `export_pipeline!` and `export_plugin!`
  are removed: `#[processor]` exports a WebAssembly component and the new `#[plugin]` a native
  plugin, neither taking arguments; the pipeline's name is the descriptor name. A crate no longer
  writes its own `mod bindings`.
- State is declared on the pipeline with `Pipeline::state::<C>()` (`is_stateful`,
  `restore_state`, `capture_state`), not as a macro argument; `ProcessorState` is `sdk::State`.
- `#[transform]` and `#[fold]` take no arguments: the component and state types come from the
  function's parameters (`fn(row: &mut T)`, `fn(rows: &[T], state: &mut S)`).
- `sdk::host` (`trace`/`debug`/`info`/`warn`/`error`, `metric`, `config`, `config_parse`,
  `with_config` for tests) replaces the generated `saci_config_get`, `saci_config_parse`, `log`
  and `metric` functions and `saci::plugin::host`. `sdk::Config::new()` reads through it and
  fetches each key once.
- `RouteDecision(vec![..])` is `RouteDecision::to([..])` and `RouteDecision::none()`.

### Changed

- `Dataset::append` drops the zero-row registration chunk, so a component read after one append
  needs no concatenation; `clone_empty` keeps the prebuilt empty templates `clear` uses.
- `plugin-host`: the output IPC of a native plugin is decoded in place instead of copied first,
  and the input buffer is presized from the previous batch.

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
