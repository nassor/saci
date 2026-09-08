---
name: saci-plugins
description: Use when writing, building, hosting, testing or documenting a SACI native plugin: the saci-plugin SDK and export_plugin!, the saci-plugin-abi C ABI and its vtables, the NativePluginRuntime host behind saci-service's plugin feature, the saci-plugin-smoketest fixture, or examples/plugins/.
---

# SACI native plugins

## SDK and ABI

- `saci-plugin`: the native plugin SDK. Wires a `Pipeline` to the `saci-plugin-abi` C ABI through
  `export_plugin!`. An rlib; the plugin crate is the cdylib, and the host is `saci-service`'s
  `plugin` feature.
- `saci-plugin-abi`: the C ABI itself, the `saci_abi_version` and `saci_plugin_v1` exports, plus
  the `SaciPluginV1` vtable (`describe`, `run_batch`, `free_buffer`, `destroy`) and the
  `SaciHostV1` callback vtable.
- `saci-plugin-smoketest`: a minimal native plugin used as a CI fixture. cdylib only.

The `saci` facade's `plugin` feature carries the authoring surface:

- `plugin`: `Pipeline`, `System`, `Component`, `ProcessorState`, `RouteDecision` and
  `export_plugin!`, for a native plugin: a cdylib the host `dlopen`s.

`engine`, `processor` and `plugin` each enable `dep:saci-core` directly, so every name they
share is one `pub use` in `crates/saci/src/lib.rs`, not three with a precedence rule between
them: the types are identical regardless of which of saci-core's own `runtime`/`processor`
features produced them. The three are still not meant to be enabled together: each authors a
different artifact, a host binary, a wasm component, or a native shared library. Doing so is
redundant, not ambiguous.

## Host

The host is `crates/saci-service/src/plugin/` (`mod.rs`, `loader.rs`, `manifest.rs`, `runtime.rs`,
`host_impl.rs`) plus `crates/saci-service/src/service/plugin_loader.rs`, the builder-side loader
`ServiceBuilder` calls for a `plugin` node. `saci-service`'s feature gate:

- `plugin`: native plugin host. `NativePluginRuntime` dlopens a shared library exporting the
  `saci-plugin-abi` C ABI, validates its manifest, and runs each batch through its `run_batch` slot.

`plugin` is not in `saci-service`'s default bundle, so a stock
`cargo install saci-service` binary cannot host a `plugin` node. It still parses one: the
config schema is the same in every build, and `validate_build_capabilities`
(skill `saci-service`) refuses the file naming `--features plugin`.

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
`crates/saci-service/tests/{plugin_roundtrip,plugin_branching,plugin_metrics}.rs` assert the
artifact exists and `connector_matrix.rs` resolves it through `Fixtures::resolve`, so build it
first:

```bash
cargo build -p saci-plugin-smoketest
```

The artifact name is platform specific: `target/debug/libsaci_plugin_smoketest.so`,
`.dylib` on macOS, `saci_plugin_smoketest.dll` on Windows. Each test resolves it through
`std::env::consts::DLL_PREFIX` and `DLL_SUFFIX`, so it follows whichever profile the test ran under.

`cargo xtask plugins` builds `examples/plugins/settle-go`, the Go cross-language plugin.

## Examples

- `examples/plugins/`: native-plugin proofs, `native_plugin.rs` (a `saci-service` example loading
  the `saci-plugin-smoketest` fixture) plus `settle-go/`, the Go cross-language plugin
  `cargo xtask plugins` builds.
- `examples/branching/plugin/`: `branching-plugin`, running the same logic as its wasm twin.
- `examples/windowing/tumbling/plugin/`: `windowing-tumbling-plugin`, running the same logic as its
  wasm twin. Skill `saci-service` describes both workflows.

## Tests

`crates/saci-service/tests/{plugin_roundtrip,plugin_branching,plugin_metrics}.rs` assert the
artifact exists; `connector_matrix.rs` runs the native plugin as one of its three processor
runtimes and resolves the artifact through `Fixtures::resolve`.

## Changing a plugin

1. An ABI change touches `saci-plugin-abi` (`saci_abi_version`, `SaciPluginV1`, `SaciHostV1`),
   `saci-plugin`'s `export_plugin!`, the host's manifest validation in `NativePluginRuntime`,
   `saci-plugin-smoketest`, and `examples/plugins/settle-go`, in one change.
2. A behaviour visible to the matrix (a new runtime kind, a new rejection) updates
   `connector_matrix.rs` (skill `saci-connectors`).
3. A page under `docs/content/service/plugins/` (skill `saci-docs`).
4. Update this skill.

## Keep this skill current

Update this file in the same change that: changes the C ABI, its version or either vtable; changes
`export_plugin!`; changes `NativePluginRuntime`, its manifest validation or its metrics; changes
the smoketest fixture or its build; adds or removes a plugin example; changes `rebuild_blocker`'s
`restartable` rule for a `plugin` node with no `library` (also check skill `saci-service`'s Service
layer section, the canonical copy); changes `NativePluginRuntime::with_identity`, its metric
attribution, or the `host-io::metric`/native ABI metric-callback relationship (also update skill
`saci-service`'s Observability section, the canonical copy).
