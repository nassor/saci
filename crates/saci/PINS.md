# Toolchain pins, `saci`

The `saci` crate targets the WebAssembly Component Model and WASI 0.3. Host and
processor share an exact-pinned Arrow IPC crate, and the tooling versions below
are validated against the `saci:pipeline@0.4.0` WIT package.

## Required tools

| Tool          | Version | Install |
|---------------|---------|---------|
| `wasmtime`    | 51.0.0  | `cargo install wasmtime-cli --locked --version 51.0.0` |
| `wit-bindgen` | 0.63.0  | workspace dependency (`features = ["async"]`); no separate install |
| `wasm-tools`  | 1.246.2 | `cargo install wasm-tools --locked --version 1.246.2` |
| Rust target   | `wasm32-wasip3` | `rustup target add wasm32-wasip3` |

Native shared-library plugins in Go (`examples/plugins/settle-go/`) use the pins
in [`examples/plugins/settle-go/PINS.md`](../../examples/plugins/settle-go/PINS.md).

## Load-bearing crate pin

`arrow-ipc = "=60.1.0"` is exact-pinned in the workspace (`Cargo.toml`). Both
the host and any processor built against this SDK MUST link against this exact
version, because Arrow IPC is the wire format across the Component Model
boundary. A patch-release drift here can silently corrupt `Dataset` round-trips
between host and processor. Do NOT relax this pin without running the
`wasm_processor` round-trip gate.

## WIT smoke check

Run from the repo root to confirm the WIT parses cleanly:

```text
wasm-tools component wit crates/saci/wit/pipeline.wit > /dev/null
```

Exit code 0 means parsing succeeded. Non-zero means a structural WIT error; diff
against the committed `pipeline.wit` before investigating further.

The `wasm_processor` CI job runs the same check against the built component:

```text
wasm-tools validate --features cm-async target/wasm32-wasip3/release/saci_processor_smoketest.wasm
wasm-tools component wit target/wasm32-wasip3/release/saci_processor_smoketest.wasm
```

The second command must show world `saci:pipeline@0.4.0` exporting
`describe: func()` and `run-batch: async func(...)`, importing `wasi:*@0.3.x`,
and importing zero `wasi:*@0.2.x` interfaces.

`rustc` links a `wasm32-wasip3` cdylib directly into a Component Model
component, so `cargo build --target wasm32-wasip3` writes the finished component
under `target/wasm32-wasip3/` with no adapter step in between.

## SIMD

The workspace `.cargo/config.toml` sets `-C target-feature=+simd128` for
`wasm32-wasip3` only, so every processor's core module carries `simd128` in its
`target_features` custom section. wasmtime enables the SIMD proposal by default,
so no host configuration corresponds to this. Confirm the flag reached the code:

```text
wasm-tools print target/wasm32-wasip3/release/saci_processor_smoketest.wasm \
  | grep -cE '(^|[^a-z0-9_])(v128|i8x16|i16x8|i32x4|i64x2|f32x4|f64x2)\.'
```

The smoketest yields over ten thousand matching vector instruction lines with
the flag and 0 without it. A `custom "target_features"` section is present
either way, so counting instructions is what discriminates, not the section.
An exported `RUSTFLAGS` replaces config-file `rustflags` rather than extending
them, so a wasm build run under `RUSTFLAGS=...` loses the feature.

## Upgrade policy

- **`wasmtime`**: upgrade the host first, verify component loads via the host
  load-time validation suite, then update the pin here. Bumps across majors
  require re-running every integration test that touches
  `wasmtime::component::bindgen!`.
- **`wit-bindgen`**: a minor bump before 1.0 is a breaking change, so bump it
  deliberately. Rebuild every `wasm32-wasip3` processor in the workspace and
  re-run the host suites before keeping the new version. A host build leaves the
  generated bindings behind `cfg(target_arch = "wasm32")`, so it validates none
  of that codegen.
- **`wasm-tools`**: upgrade freely; it is invoked only by CI and local tooling,
  not at runtime.
- **`arrow-ipc`**: DO NOT BUMP without coordination. The pin is load-bearing
  for on-disk checkpoint format stability AND host-processor wire-format
  compatibility. See the workspace `Cargo.toml` comment.

## Compiler and component tooling caveats

- `cargo-component` is not used. `wit_bindgen::generate!` in each processor
  crate covers binding generation with Component Model Async (`async`), and
  plain `cargo build --target wasm32-wasip3` produces the component.
