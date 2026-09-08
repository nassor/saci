+++
title = "A Rust plugin"
description = "Build a cdylib exporting the saci-plugin-abi C ABI, point a plugin node at it, and run."
template = "page.html"
weight = 1
aliases = ["/native/plugins/"]
+++
# A Rust plugin

A native plugin is a shared library that `saci-service` loads at runtime, with
`dlopen` on Unix and `LoadLibrary` on Windows. It exports two C symbols.
`saci_abi_version` reports the ABI the library was built against, and
`saci_plugin_v1` fills a host allocated vtable with four function pointers.

The contract behind those pointers is the one a
[WebAssembly processor](@/service/processors/build/_index.md) implements, written in C
instead of WIT.
`describe` runs once at load and reports the plugin name, its version, and the
Arrow schema of every component it declares. `run_batch` runs once per batch over
the same [Arrow IPC wire format](@/library/reference/wire-format.md), and the opaque
checkpoint it returns is the only state the host carries across a batch boundary
for you. The host holds one loaded instance for the node's whole life, so the
library's own memory survives a batch too, and keeping state there is still
wrong: step 2 says why.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 176" role="img" aria-labelledby="plg-title plg-desc">
        <title id="plg-title">The host loads a shared library and drives two calls across a C ABI</title>
        <desc id="plg-desc">
            saci-service opens the shared library, checks the ABI version symbol, then calls
            saci_plugin_v1 to collect a vtable of four function pointers. It calls describe
            once at load to learn the component schemas and the schema fingerprint, then
            run_batch once per batch, passing Arrow IPC input bytes and the prior checkpoint
            and receiving output rows, a new checkpoint and metrics. The plugin calls back
            into the host for log, metric and get_config. The host stores the returned
            checkpoint and replays it as the next batch's prior. The host holds one loaded
            instance for the node's whole life, so the library's own memory survives a batch
            as well; the checkpoint is the state the host persists and replays. Both sides
            share one address space, and no epoch deadline bounds the call.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="40" width="176" height="72" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="40" width="176" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="52" width="176" height="8"/>
            <text class="t-lbl" x="12" y="55">saci-service</text>
            <text class="t-sm" x="12" y="76">dlopen, LoadLibrary</text>
            <text class="t-sm" x="12" y="89">owns the vtable</text>
            <text class="t-sm" x="12" y="102">no epoch deadline</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="456" y="40" width="204" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="456" y="40" width="204" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="456" y="52" width="204" height="8"/>
            <text class="t-lbl" x="468" y="55">your plugin &middot; .so</text>
            <text class="t-sm t-bnd" x="468" y="76">saci_abi_version()</text>
            <text class="t-sm t-bnd" x="468" y="89">saci_plugin_v1()</text>
            <text class="t-sm" x="468" y="102">one address space</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="316" y="44">describe() &rarr; manifest, once at load</text>
            <path class="arw arw-ctl" d="M176 50 H456" marker-end="url(#plg-c)"/>
            <text class="t-sm t-mid" x="316" y="62">run_batch(input, prior)</text>
            <path class="arw arw-data" d="M176 68 H456" marker-end="url(#plg-d)"/>
            <path class="arw arw-data" d="M456 86 H176" marker-end="url(#plg-d)"/>
            <text class="t-sm t-mid" x="316" y="98">rows, checkpoint, metrics</text>
        </g>
        <g class="anim anim-4">
            <text class="t-sm t-ctl t-mid" x="316" y="126">log, metric, get_config</text>
            <path class="arw arw-ctl" d="M456 132 H176" marker-end="url(#plg-c)"/>
            <path class="arw arw-bnd" d="M25 112 V148 H111 V112" marker-end="url(#plg-b)"/>
            <text class="t-sm t-bnd t-mid" x="68" y="164">checkpoint &rarr; prior</text>
        </g>
        <defs>
            <marker id="plg-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="plg-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="plg-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> control plane: describe, and the three host callbacks</span>
        <span class="k-data"><i></i> data plane: Arrow IPC bytes both ways</span>
        <span class="k-boundary"><i></i> the C ABI boundary, and the checkpoint that crosses it</span>
    </div>
    <figcaption class="dgm-cap">
        Every <code>SaciBuffer</code> the plugin writes stays plugin owned. The host copies out
        of it and hands it back to <code>free_buffer</code>, so allocator ownership never
        crosses the boundary in either direction.
    </figcaption>
</div>

## 1. Create a cdylib crate

`saci-plugin` is the Rust SDK. A plugin crate sets `crate-type = ["cdylib"]`
and depends on it; `export_plugin!` writes the two exported symbols, the four
vtable thunks, and the `saci_config_get` and `saci_config_parse` functions into
the crate. Every block below is from `crates/saci-plugin-smoketest/src/lib.rs`,
which CI builds.

```toml,name=The plugin crate manifest
[lib]
crate-type = ["cdylib"]

[dependencies]
saci-plugin = { workspace = true }
serde      = { workspace = true }
```

## 2. Export the pipeline

Hand `export_plugin!` a function that builds a `Pipeline`. The pipeline's
components and systems are the plugin's, written exactly as in a
[native pipeline](@/library/first-pipeline.md).

```rust,name=The build function and the export macro
use saci_plugin::prelude::*;

pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("smoketest-plugin");
    pipeline
        .data
        .register_component::<Counter>()
        .expect("register Counter");
    pipeline.add_system(AdvanceSystem);
    pipeline
}

saci_plugin::export_plugin!(build, state = Total);
```

The optional `state = T` names the one component whose rows survive a batch,
and `export_plugin!(build)` without it declares a stateless plugin. `T` must
not be registered in `build()`. The macro decodes the prior checkpoint into a
`ProcessorState<T>` resource before the pipeline runs and captures it
afterwards, so those rows never appear in the output.

A plugin's process memory does survive between calls, and keeping state there
is still wrong. Consecutive batches of one partition may land on different
processes, and only the checkpoint travels with the claim.

## 3. Build it

```bash,name=Build the plugin
cargo build
```

Runs the same on all three platforms. The artifact name is platform specific:

| Platform | Artifact |
|----------|----------|
| Linux | `target/debug/libsaci_plugin_smoketest.so` |
| macOS | `target/debug/libsaci_plugin_smoketest.dylib` |
| Windows | `target/debug/saci_plugin_smoketest.dll` |

## 4. Write the config node

A `plugin` node in the workflow names the library. The key is `library`, not
`module`; a `link` treats a plugin node exactly like a `wasm` node.

```kdl,name=The plugin node in a service config
workflow "counter" {
    plugin "process_counter" library="${SACI_PLUGIN_LIB:-target/debug/libsaci_plugin_smoketest.so}" {
        // Optional. Hex digest of the library file's bytes, with an optional
        // `sha3-256:` prefix; a mismatch refuses the load.
        // sha3_256="sha3-256:abc123..."
        config "smoketest.multiplier"="10"
    }
}
```

A relative `library` resolves against the directory `saci-service` runs in, and an absolute path
is used as it stands. An unknown key in the node is a parse error.
[Plugins in a workflow](@/service/plugins/_index.md) lists every key.

## 5. Validate and run

Linux/macOS:

```bash,name=Validate the config
cargo run -p saci-service --features connector-file,transformer-csv,plugin -- validate \
  --config examples/configs/standalone_plugin.kdl --strict
```

Windows (PowerShell):

```powershell
$env:SACI_PLUGIN_LIB = "target/debug/saci_plugin_smoketest.dll"
cargo run -p saci-service --features connector-file,transformer-csv,plugin -- validate --config examples/configs/standalone_plugin.kdl --strict
```

Linux/macOS:

```bash,name=Run the service
cargo run -p saci-service --features connector-file,transformer-csv,plugin -- serve \
  --config examples/configs/standalone_plugin.kdl
```

Windows (PowerShell):

```powershell
$env:SACI_PLUGIN_LIB = "target/debug/saci_plugin_smoketest.dll"
cargo run -p saci-service --features connector-file,transformer-csv,plugin -- serve --config examples/configs/standalone_plugin.kdl
```

The config reads `examples/configs/fixtures/counter_input.csv`, runs the
plugin, and writes `/tmp/saci-counter-out.csv` with the `seen` column filled.
`validate` opens no connection, but it does load the plugin library and run
its `describe`, so a broken library fails there rather than at the first
batch.

## Config, logs and metrics

`export_plugin!` writes `saci_config_get` and `saci_config_parse` into your crate. Config values
arrive as strings, and `saci_config_parse` turns one into whatever type you ask for: `None` for
an absent key, `Some(Err(_))` for a value that will not parse.

```rust,name=Reading a config value in a system
let multiplier = match saci_config_parse::<i64>(MULTIPLIER_KEY) {
    Some(Ok(value)) => value,
    Some(Err(e)) => {
        return Err(SaciError::system_execution(format!(
            "smoketest: {MULTIPLIER_KEY} is not an integer: {e}"
        )));
    }
    None => 1,
};
```

A misconfigured value is worth refusing rather than defaulting. An unparseable multiplier
silently becoming 1 is harder to notice than a batch that fails with a message naming the key.

Logs and metrics go out through `saci_plugin::host`, the native counterpart of the imports a
WebAssembly processor calls.

```rust,name=Logs and metrics through the host
saci_plugin::host::metric("smoketest.rows", rows as f64);
saci_plugin::host::info("smoketest", &format!("numbered {rows} rows through {advanced}"));
```

A metric the plugin names goes out as a trace event on the service's own
subscriber, not as a Prometheus series. `saci_processor_metric` belongs to the
WebAssembly path, and the other five `saci_processor_*` names carry the
per-batch numbers, which a plugin reports into exactly the same series a
processor does. Return a `SaciError` from a system rather than panicking.
`export_plugin!` catches a panic and reports it as a permanent failure reading
`panic: <message>`, and a payload that is not a string loses even that.

## Next

- [Plugins in a workflow](@/service/plugins/_index.md): every key of the node that loads this
  library, and what the service prints when it refuses one.
- [Plugins in other languages](@/service/plugins/other-languages.md): the same two symbols, from
  a toolchain other than cargo.
