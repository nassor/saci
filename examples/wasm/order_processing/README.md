# Order Processing: a WASM processor pipeline

This is a realistic processor component for `wasm32-wasip2`. It runs the
`saci-processor` SDK and the `export_pipeline!` macro end to end, with
field-granular DAG scheduling inside the sandbox. The host hands the
processor one Arrow IPC payload per batch and gets one back; everything
between, meaning the DAG build, stage execution and retry handling, is native
Rust running inside the component.

The processor runs a two-stage pipeline over a `Transaction` component:

| Stage | Systems | Writes |
|-------|---------|--------|
| 0 | `ValidateSystem`, `EnrichSystem` | `valid`; `usd_amount`. Disjoint field writes, so the stage runs them in parallel |
| 1 | `ReportSystem` | reads `valid`, `usd_amount` and others; must follow stage 0 |

There is no ingest system: `Transaction` rows arrive in the host's `run-batch`
Arrow IPC payload from a configured source. FX rates come from the `config`
child of the host's `wasm` node, read back through the `host-io` `get-config`
import and parsed with `saci_config_parse`; each missing key falls back to
`FxRates::DEFAULT`.

## Prerequisites

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- `wasm-tools` for the optional component checks

## Build

From the workspace root. The commands in this README run the same on Linux,
macOS and Windows (PowerShell):

```text
cargo build --release -p order-processing-wasm --target wasm32-wasip2
```

No componentizer runs: `rustc` links the `wasm32-wasip2` cdylib into a
Component Model component itself, so plain `cargo build` writes the finished
component to `target/wasm32-wasip2/release/order_processing_wasm.wasm`.

Validate the component (optional):

```text
wasm-tools validate --features component-model target/wasm32-wasip2/release/order_processing_wasm.wasm
```

Inspect the exported world (optional):

```text
wasm-tools component wit target/wasm32-wasip2/release/order_processing_wasm.wasm
```

## Run through saci-service

`examples/configs/standalone_wasm.kdl` runs this component against a five-row
CSV fixture. Its paths are relative to the repository root.

Validate the config:

```text
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_wasm.kdl --strict
```

Run the pipeline:

```text
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_wasm.kdl
```

`run_mode` is `one_shot`, so `serve` processes
`examples/configs/fixtures/order_processing_input.csv` once and writes
`/tmp/saci-order-processing-out.csv` with `valid` and `usd_amount` filled in.
The fixture seeds both columns with `false` and `0.0`: the CSV transformer
turns an empty field into a NULL, the `Transaction` schema is non-nullable,
and one blank cell fails the whole batch.

## How failures surface

The `export_pipeline!` macro converts pipeline failures into the WIT
`run-error` variant. `retryable` releases the claim so the host retries;
`permanent` acks the claim and surfaces the error.

| `SaciError` from `Pipeline::run_on` | WIT `run-error` | Host action |
|-------------------------------------|-----------------|-------------|
| `RetryExhausted`, `SystemExecution` | `retryable(string)` | release claim, retry |
| `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`, `Configuration`, `Scheduler`, `Store`, `Generic` | `permanent(string)` | ack claim, surface |

System authors should return `SaciError::SystemExecution(...)` for recoverable
failures rather than calling `.unwrap()` or `panic!()`. A panic becomes a wasm
trap, which the host catches as `permanent`, and the operator loses the batch
instead of getting a retry.

## Files

| File | What it is |
|------|------------|
| `Cargo.toml` | cdylib, `wit-bindgen` as a wasm32-only dependency |
| `src/lib.rs` | the `wit_bindgen::generate!` bindings, the `Transaction` component, the three systems, `build()` and `export_pipeline!(build)`, all gated on wasm32 |
