# Toolchain pins — `saci-processor`

This crate targets the WebAssembly Component Model. Host and processor share an
exact-pinned Arrow IPC crate, and the tooling versions below are what the
team has validated against the `saci:pipeline@0.3.0` WIT package.

## Required tools

| Tool              | Version  | Install                                                       |
| ----------------- | -------- | ------------------------------------------------------------- |
| `wasmtime`        | 48.0.2   | `cargo install wasmtime-cli --locked --version 48.0.2`        |
| `wit-bindgen`     | 0.62.0   | workspace dependency; no separate install                     |
| `wasm-tools`      | 1.246.2  | `cargo install wasm-tools --locked --version 1.246.2`        |
| Rust target       | `wasm32-wasip2` | `rustup target add wasm32-wasip2`                    |

Non-Rust processors need three more componentization toolchains
(`componentize-go`, `componentize-py`, `jco`), pinned in
[`examples/polyglot/PINS.md`](../../examples/polyglot/PINS.md).

## Load-bearing crate pin

`arrow-ipc = "=59.3.0"` is exact-pinned in the workspace (`Cargo.toml`).
Both the host and any processor built against this SDK MUST link against this
exact version, because Arrow IPC is the wire format across the Component
Model boundary. A patch-release drift here can silently corrupt `Dataset`
round-trips between host and processor. Do NOT relax this pin without the
round-trip CI job.

## WIT smoke check

Run from the repo root to confirm the WIT parses cleanly:

```
wasm-tools component wit crates/saci-processor/wit/pipeline.wit > /dev/null
```

Exit code 0 = parse succeeded. Non-zero = structural WIT error; diff against
the committed `pipeline.wit` before investigating further.

The `wasm_processor` CI job runs the same check against the built component:

```
wasm-tools validate --features component-model target/wasm32-wasip2/release/saci_processor_smoketest.wasm
wasm-tools component wit target/wasm32-wasip2/release/saci_processor_smoketest.wasm
```

The second command must show world `saci:pipeline@0.3.0` exporting exactly
`describe` and `run-batch`, importing `wasi:0.2.x`. The exact patch version
tracks whichever toolchain built the component. What matters is the absence of
`wasi:*@0.2.3`, which is what a preview1 adapter imports.

There is no second artifact and no adapter. `rustc` links a `wasm32-wasip2`
cdylib into a Component Model component itself, so `cargo build --target
wasm32-wasip2` writes the finished component under `target/wasm32-wasip2/` and
nothing is ever compiled for `wasm32-wasip1`.

## SIMD

The workspace `.cargo/config.toml` sets `-C target-feature=+simd128` for
`wasm32-wasip2` only, so every processor's core module carries `simd128` in its
`target_features` custom section. wasmtime enables the SIMD proposal by default,
so no host configuration corresponds to this. Confirm the flag reached the code:

```
wasm-tools print target/wasm32-wasip2/release/saci_processor_smoketest.wasm \
  | grep -cE '(^|[^a-z0-9_])(v128|i8x16|i16x8|i32x4|i64x2|f32x4|f64x2)\.'
```

The smoketest yields roughly 13000 matching lines, and 0 when the flag is
absent. A `custom "target_features"` section is present either way, so counting
instructions is what discriminates, not the section. The exact count moves with
the toolchain. An exported `RUSTFLAGS` replaces config-file `rustflags` rather
than extending them, so a wasm build run under `RUSTFLAGS=...` loses the
feature.

## Upgrade policy

- **`wasmtime`**: upgrade the host first, verify component loads via the
  `saci-service` load-time validation suite, then update the pin here. Bumps
  across majors require re-running every integration test that touches
  `wasmtime::component::bindgen!`.
- **`wit-bindgen`**: a minor bump before 1.0 is a breaking change, so bump it
  deliberately. Rebuild every `wasm32-wasip2` processor in the workspace and
  re-run the host suites before keeping the new version. A host build leaves the
  generated bindings behind `cfg(target_arch = "wasm32")`, so it validates none
  of that codegen.
- **`wasm-tools`**: upgrade freely; it is invoked only by CI and local
  tooling, not at runtime.
- **`arrow-ipc`**: DO NOT BUMP without coordination. The pin is load-bearing
  for on-disk checkpoint format stability AND host↔processor wire-format
  compatibility. See the workspace `Cargo.toml` comment.

## Known version caveats (as of 2026-08-26)

- `cargo-component` is not used. Its newest published release, 0.21.1, compiles
  the core module for `wasm32-wasip1` and adapts it into a component, so
  `wasm32-wasip2` rustflags such as `+simd128` never reach the code.
  `wit_bindgen::generate!` in each processor crate covers its binding
  generation, and plain `cargo build` covers its componentization.
