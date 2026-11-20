---
name: saci-plugins
description: Use when writing, building, hosting, testing or documenting a SACI native plugin: the saci::sdk module and #[plugin], the saci::plugin_abi C ABI and its vtables, the NativePluginRuntime host behind the saci crate's plugin-host feature, the saci-plugin-smoketest fixture, or examples/plugins/.
---

# SACI native plugins

## SDK and ABI

- `#[plugin]` (feature `plugin` of the `saci` crate, `crates/saci-macros/src/plugin.rs`): the native
  plugin SDK. It wires a `fn build() -> Pipeline` to the C ABI in `saci::plugin_abi`: the
  `saci_abi_version` and `saci_plugin_v1` exports and the four vtable thunks. The thunks live in
  `crates/saci/src/sdk/plugin_rt.rs`, which enters a `HostScope` over the host's `SaciHostV1`
  callbacks and calls the shared `describe` and `run_batch` of `sdk/glue.rs`. The attribute takes
  no arguments, carries no target `cfg` and is the only export a plugin writes. A plugin crate sets
  `crate-type = ["cdylib"]` and depends on `saci = { workspace = true, features = ["plugin"] }`;
  outside this repository, `saci = { version = "0.1", default-features = false, features =
  ["plugin"] }`. The host is the `plugin-host` feature.
- `saci::plugin_abi` (compiled for `plugin` or `plugin-host`; C header
  `crates/saci/include/saci_plugin.h`): the C ABI itself, the `saci_abi_version` and
  `saci_plugin_v1` exports, plus the `SaciPluginV1` vtable (`describe`, `run_batch`,
  `free_buffer`, `destroy`) and the `SaciHostV1` callback vtable.
- `saci-plugin-smoketest`: a minimal native plugin used as a CI fixture. cdylib only.

`plugin` carries the authoring surface through `use saci::prelude::*;`: `Pipeline`, `System`,
`Component`, `sdk` (`sdk::State`, `sdk::RouteDecision`, `sdk::Config`, `sdk::host`) and the macros,
for a native plugin: a cdylib the host `dlopen`s. State is declared with `.state::<C>()` on the
pipeline, and the pipeline's name is the descriptor name the host checks. `engine`, `processor` and
`plugin` each author a different artifact, a host binary, a wasm component, or a native shared
library. The types they share are one `pub use` in `crates/saci/src/lib.rs`, so enabling several
together is redundant, not ambiguous. The shared module and its files are in skill
`saci-processors` (`crates/saci/src/sdk.rs`, `sdk/{config,error,host,glue,state}.rs`).

## Host

The host is `crates/saci/src/plugin_host/` (`mod.rs`, `loader.rs`, `manifest.rs`, `runtime.rs`,
`host_impl.rs`) plus `crates/saci/src/service/plugin_loader.rs`, the builder-side loader
`ServiceBuilder` calls for a `plugin` node. The `saci` crate's feature gate:

- `plugin-host`: native plugin host, `saci::plugin_host`. `NativePluginRuntime` dlopens a shared
  library exporting the `saci::plugin_abi` C ABI, validates its manifest, and runs each batch
  through its `run_batch` slot.

`plugin-host` is not in the `server` bundle, so a stock `cargo install saci --features server`
binary cannot host a `plugin` node. It still parses one: the config schema is the same in every
build, and `validate_build_capabilities` (skill `saci-service`) refuses the file naming
`--features plugin-host`.

`NativePluginRuntime` maps its validated manifest into `RuntimeDescriptorInfo`, so a `plugin` node
reports the plugin manifest's name as `name()`. Metric attribution rides
`NativePluginRuntime::with_identity`, set by `ServiceBuilder` from the node's own id. The native
plugin ABI's `metric` callback is the counterpart of `host-io::metric` but writes no series;
`NativePluginRuntime` records the five per-batch `saci_processor_*` series exactly as the wasm host
does, and `saci_processor_metric` stays empty for a plugin.

`processor.batch` is the `debug` span the plugin host opens under `runtime.run`.

A `plugin` node with no `library` takes its runtime from `with_runtime(id, ..)`, and
`build_processor_node` **removes** that injected runtime from the builder, so `rebuild_blocker`
reports such a workflow as `restartable: false` and the control plane answers 409 to `start`,
`stop` and `restart`.

## Building

`crates/saci-plugin-smoketest` is a `cdylib`, so `cargo test --workspace` does not build it: a
cdylib-only member has no test target for cargo to reach.
`crates/saci/tests/{plugin_roundtrip,plugin_branching,plugin_metrics}.rs` assert the
artifact exists and `connector_matrix.rs` resolves it through `Fixtures::resolve`, so build it
first:

```bash
cargo build -p saci-plugin-smoketest
```

The artifact name is platform specific: `target/debug/libsaci_plugin_smoketest.so`,
`.dylib` on macOS, `saci_plugin_smoketest.dll` on Windows. Each test resolves it through
`std::env::consts::DLL_PREFIX` and `DLL_SUFFIX`, so it follows whichever profile the test ran under.

`cargo xtask plugins` builds `examples/plugins/settle-go`, the Go cross-language plugin (after
running `examples/plugins/schema_emit.rs` to refresh `examples/plugins/generated/`). The Go plugin
bundles its Arrow IPC wire codec in `examples/plugins/settle-go/internal/arrowipc/`.

## Examples

- `examples/plugins/`: native-plugin proofs, `native_plugin.rs` (a `saci` example, feature
  `plugin-host`, loading the `saci-plugin-smoketest` fixture), `schema_emit.rs` (`plugin_schema_emit`
  example emitting `examples/plugins/generated/`), and `settle-go/` with its internal `arrowipc`
  package (`cargo xtask plugins` builds it).
- `examples/branching/plugin/`: `branching-plugin`, running the same logic as its wasm twin.
- `examples/windowing/tumbling/plugin/`: `windowing-tumbling-plugin`, running the same logic as its
  wasm twin. Skill `saci-service` describes both workflows.

## Tests

`crates/saci/tests/{plugin_roundtrip,plugin_branching,plugin_metrics}.rs` assert the
artifact exists; `connector_matrix.rs` runs the native plugin as one of its three processor
runtimes and resolves the artifact through `Fixtures::resolve`.

## Changing a plugin

1. An ABI change touches `crates/saci/src/plugin_abi.rs` and `crates/saci/include/saci_plugin.h`
   (`saci_abi_version`, `SaciPluginV1`, `SaciHostV1`), `#[plugin]` in
   `crates/saci-macros/src/plugin.rs` and `crates/saci/src/sdk/plugin_rt.rs`, the host's manifest validation in `NativePluginRuntime`,
   `saci-plugin-smoketest`, and `examples/plugins/settle-go`, in one change.
2. A behaviour visible to the matrix (a new runtime kind, a new rejection) updates
   `connector_matrix.rs` (skill `saci-connectors`).
3. A page under `docs/content/service/plugins/` (skill `saci-docs`).
4. Update this skill.

## Keep this skill current

Update this file in the same change that: changes the C ABI, its version or either vtable; changes
`#[plugin]` or `saci::sdk`; changes `NativePluginRuntime`, its manifest validation or its metrics; changes
the smoketest fixture or its build; adds or removes a plugin example; changes `rebuild_blocker`'s
`restartable` rule for a `plugin` node with no `library` (also check skill `saci-service`'s Service
layer section, the canonical copy); changes `NativePluginRuntime::with_identity`, its metric
attribution, or the `host-io::metric`/native ABI metric-callback relationship (also update skill
`saci-service`'s Observability section, the canonical copy).
