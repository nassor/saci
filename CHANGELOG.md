# Changelog

`saci` and `saci-macros` share one version. The release process is in `AGENTS.md`, under
"Releases".

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
