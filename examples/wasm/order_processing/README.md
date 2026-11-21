# Order Processing: a WASM processor pipeline

A processor component for `wasm32-wasip3`, written in Rust with the `saci`
crate's `processor` feature and the `#[processor]` attribute. The host hands it
one Arrow IPC payload per batch and takes one back. Inside the component, the
DAG build, stage execution and retries run as native Rust.

The pipeline runs over a `Transaction` component in two stages:

| Stage | Systems | Reads | Writes |
|-------|---------|-------|--------|
| 0 | `ValidateSystem`<br>`EnrichSystem` | `amount`<br>`amount`, `currency` | `valid`<br>`usd_amount` |
| 1 | `ReportSystem` | `id`, `amount`, `currency`, `valid`, `usd_amount` | nothing, it prints a report |

`ValidateSystem` marks a row valid when `amount > 0`. `EnrichSystem` multiplies
`amount` by the currency's USD rate. The two write different fields, so the
scheduler runs them in parallel; `ReportSystem` reads both results and waits for
stage 0.

There is no ingest system. `Transaction` rows arrive in the host's `run-batch`
payload from whatever source the config names. The FX rates come from the
`config` of the host's `wasm` node (`fx_eur`, `fx_gbp`, `fx_jpy`, `fx_cad`).
A missing key falls back to `FxRates::DEFAULT`: EUR 1.08, GBP 1.27, JPY 0.0067,
CAD 0.74. Any other currency converts at 1.0.

## Prerequisites

- Rust with the `wasm32-wasip3` target: `rustup target add wasm32-wasip3`
- `wasm-tools`, only for the optional checks in step 2

Run every command from the repository root. They are the same on Linux, macOS
and Windows (PowerShell) unless a block says otherwise.

## Build and run

1. Build the component. `rustc` links a `wasm32-wasip3` cdylib into a
   Component Model component itself, so plain `cargo build` is the whole
   toolchain:

   ```text
   cargo build --release -p order-processing-wasm --target wasm32-wasip3
   ```

   The result is `target/wasm32-wasip3/release/order_processing_wasm.wasm`.

2. Optional: check the component and print its exported world.

   ```text
   wasm-tools validate --features cm-async target/wasm32-wasip3/release/order_processing_wasm.wasm
   wasm-tools component wit target/wasm32-wasip3/release/order_processing_wasm.wasm
   ```

   `validate` exits 0. The exported world is `saci-pipeline`.

3. Create the output directory. `examples/configs/standalone_wasm.kdl` writes
   to `/tmp/saci-order-processing-out.csv`.

   Linux/macOS: `/tmp` exists, nothing to do.

   Windows (PowerShell), where the path resolves to `C:\tmp` on the current
   drive:

   ```powershell
   New-Item -ItemType Directory -Force C:\tmp
   ```

4. Validate the config. It runs the component against a five-row CSV fixture,
   and its paths are relative to the repository root:

   ```text
   cargo run -p saci --features server,connector-file,codec-csv,wasm-host -- validate --config examples/configs/standalone_wasm.kdl --strict
   ```

   The run ends with `OK: workflow graph validated`, which means the host
   loaded the component and its `describe()` agrees with the source and sink.

5. Run the pipeline:

   ```text
   cargo run -p saci --features server,connector-file,codec-csv,wasm-host -- serve --config examples/configs/standalone_wasm.kdl
   ```

   `run "once"` makes `serve` process
   `examples/configs/fixtures/order_processing_input.csv` once and exits.

6. Read the output.

   Linux/macOS:

   ```bash
   cat /tmp/saci-order-processing-out.csv
   ```

   Windows (PowerShell):

   ```powershell
   Get-Content C:\tmp\saci-order-processing-out.csv
   ```

   The file has the five fixture rows with `valid` and `usd_amount` filled in.
   `valid` is `true` for ids 1, 2, 3 and 5 and `false` for id 4, whose amount
   is `-50.0`. `usd_amount` is `amount` times the rate in the config.

The fixture seeds `valid` with `false` and `usd_amount` with `0.0`. The
processor cannot add columns, and the CSV codec turns an empty field into
a NULL, which the non-nullable `Transaction` schema rejects, so one blank cell
fails the whole batch.

`cargo xtask demo standalone_wasm` does steps 1, 4 and 5 in one command.

## How failures surface

The `#[processor]` attribute converts a `SaciError` from the pipeline into the
WIT `run-error` variant: `SaciError::is_retryable()` selects `retryable`, every
other error is `permanent`:

| `SaciError` | WIT `run-error` |
|-------------|-----------------|
| `RetryExhausted`, `SystemExecution`, `Store` | `retryable(string)` |
| `ComponentNotRegistered`, `FieldNotFound`, `ResourceNotFound`, `Configuration`, `Scheduler`, `Generic` | `permanent(string)` |

The wasm host reports both variants as a failed batch with the message. A panic
becomes a wasm trap, which the host also surfaces as `permanent`, so return
`SaciError::SystemExecution(...)` for a failure a retry can fix instead of
calling `.unwrap()` or `panic!()`. The
[WIT contract](../../../docs/content/service/processors/build/wit-contract.md)
page has the full variant list.

## Files

| File | What it is |
|------|------------|
| `Cargo.toml` | cdylib, `wit-bindgen` as a wasm32-only dependency |
| `src/lib.rs` | the `Transaction` component, the three systems and `build()`, annotated with `#[processor]`; the generated bindings and exports exist only on wasm32 |
