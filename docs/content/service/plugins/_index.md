+++
title = "Plugins in a workflow"
description = "Declare a plugin node: native speed, native toolchain, no sandbox."
template = "section.html"
sort_by = "weight"
weight = 7
+++

A `plugin` node loads a shared library and runs it over every batch the workflow delivers to it,
in the service's own process. Nothing sandboxes the call, and no deadline bounds it.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 176" role="img" aria-labelledby="pl-title pl-desc">
        <title id="pl-title">A plugin node between a source and a sink, in the service's own process</title>
        <desc id="pl-desc">
            The source trades_in hands a batch to the plugin node settle, which names a shared
            library file. The node's output goes on to the sink trades_out. The plugin box has
            no sandbox frame around it, unlike a wasm node: it runs in the service's own
            process, with the same privileges, and no deadline bounds the call. A checkpoint
            arrow leaves the node and comes back into it as the next batch's prior.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="46" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="58" width="130" height="8"/>
            <text class="t-lbl" x="12" y="61">trades_in</text>
            <text class="t-sm" x="12" y="84">source</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M130 74 H200" marker-end="url(#pl-d)"/>
            <rect class="blk blk-bnd" x="200" y="36" width="210" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="200" y="36" width="210" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="200" y="48" width="210" height="8"/>
            <text class="t-lbl t-bnd" x="212" y="51">settle</text>
            <text class="t-sm" x="212" y="74">plugin node, in process</text>
            <text class="t-sm" x="212" y="94">library=&quot;libsettle.so&quot;</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M410 74 H480" marker-end="url(#pl-d)"/>
            <rect class="blk blk-data" x="480" y="46" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="480" y="46" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="480" y="58" width="130" height="8"/>
            <text class="t-lbl" x="492" y="61">trades_out</text>
            <text class="t-sm" x="492" y="84">sink</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M380 112 V142 H230 V112" marker-end="url(#pl-b)"/>
            <text class="t-sm t-bnd t-mid" x="305" y="158">checkpoint comes back as prior</text>
            <text class="t-sm t-end" x="660" y="158">no sandbox, no deadline</text>
        </g>
        <defs>
            <marker id="pl-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="pl-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the batch, in and out</span>
        <span class="k-boundary"><i></i> the plugin, and the checkpoint that crosses a batch</span>
    </div>
    <figcaption class="dgm-cap">
        A <code>link</code> treats a <code>plugin</code> node exactly like a <code>wasm</code>
        node. The key is <code>library</code> rather than <code>module</code>, and everything
        downstream of the node is written the same way.
    </figcaption>
</div>

## 1. When to choose a plugin

A plugin buys native speed and a native toolchain: real threads, native extensions, and no
componentizer in the build. It reads and writes the same batches a processor does.

It gives up the sandbox. A plugin runs in the service's own process with full host privileges.
It cannot be interrupted, and a memory error in it is a memory error in the service. A wedged
plugin wedges the thread driving it, and a crash in one takes the whole process down.

<div class="note note-warn">
<span class="note-label">A plugin is an operator trusted path</span>

The optional `sha3_256` digest is the only integrity check. Point a `plugin` node at a library
you built, or at one you trust the way you trust the service binary itself.

</div>

Choose a [processor](@/service/processors/_index.md) unless you need what the sandbox costs you.
[How the host runs a processor](@/library/processor-host.md) sets out what each runtime does
around a call.

## 2. Declare a plugin node

A `plugin` node takes an id as its leading argument and a `library` naming the shared library.
A relative `library` resolves against the directory `saci-service` runs in, and an absolute path
is used as it stands.

```kdl,name=A plugin node in a service config
workflow "counter" {
    plugin "process_counter" library="${SACI_PLUGIN_LIB:-target/debug/libsaci_plugin_smoketest.so}" {
        // Optional. Hex digest of the library file's bytes, with an optional
        // `sha3-256:` prefix; a mismatch refuses the load.
        // sha3_256="sha3-256:abc123..."
        config "smoketest.multiplier"="10"
    }

    link from="csv_counters" to="process_counter"
    link from="process_counter" to="csv_out"
}
```

The `config` child holds key-value strings the plugin reads for itself, exactly as a `wasm`
node's does. A `window` block declares event-time geometry the same way too:
[Windowing](@/service/processors/windowing/_index.md) covers its keys.

A shared library's file name is platform specific, which is why the example reads the name from
an environment variable:

| Platform | Artifact |
|---|---|
| Linux | `target/debug/libsaci_plugin_smoketest.so` |
| macOS | `target/debug/libsaci_plugin_smoketest.dylib` |
| Windows | `target/debug/saci_plugin_smoketest.dll` |

## 3. Validate and run

`plugin` is not in the default build. Add `--features plugin` to make the node bind.

Linux/macOS:

```bash,name=Validate a config with a plugin node
cargo build -p saci-plugin-smoketest

cargo run -p saci-service --features connector-file,transformer-csv,plugin -- validate \
  --config examples/configs/standalone_plugin.kdl --strict
```

Windows (PowerShell):

```powershell
cargo build -p saci-plugin-smoketest

$env:SACI_PLUGIN_LIB = "target/debug/saci_plugin_smoketest.dll"
cargo run -p saci-service --features connector-file,transformer-csv,plugin -- validate --config examples/configs/standalone_plugin.kdl --strict
```

```text,name=What validate prints
OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
```

`validate` opens no connection, but it does load the library and run its self description, so a
broken plugin fails here rather than at the first batch.

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

The run reads `examples/configs/fixtures/counter_input.csv`, runs the plugin once, and writes
`/tmp/saci-counter-out.csv` with the `seen` column filled.

## Every key

| Key | Type | Default | What it does |
|---|---|---|---|
| `id` | string | required | the node's leading argument, unique across the workflow |
| `name` | string | the id | display name the dashboard shows instead of the id |
| `library` | path | required | the shared library this node loads |
| `sha3_256` | string | none | expected SHA3-256 of the library file's bytes, with an optional `sha3-256:` prefix |
| `config` | block | empty | key-value strings the plugin reads for itself |
| `window` | block | none | event-time geometry, covered in [Windowing](@/service/processors/windowing/_index.md) |

### config

| Key | Type | Default | What it does |
|---|---|---|---|
| any name | string | none | handed to the plugin unchanged, for it to parse |

An unknown key elsewhere in the node is a parse error. A `config` key is never unknown: the
plugin decides which ones it reads.

## When it refuses to start

| Message | What to change |
|---|---|
| `plugin library '/srv/saci/target/debug/libsaci_plugin_smoketest.so' does not exist` | Build the library, or correct `library`. A relative path is read from the directory the service runs in, and the message shows it resolved to an absolute one. |
| ``cannot load plugin library `target/debug/libsaci_plugin_smoketest.so`: ...`` | The file exists but the system refused to load it. A library built for another platform or another architecture fails here. |
| ``plugin library `libsettle.so` does not export `saci_abi_version`: ...`` | The library is not a SACI plugin, or its two entry points are not exported under those exact names. |
| ``plugin library `libsettle.so`: plugin ABI version 1.3 is incompatible with host ABI version 1.2`` | The major must match and the plugin's minor must be no greater than the host's. Rebuild the plugin against the ABI this binary carries, or run a binary that matches it. |
| `plugin library '/srv/saci/libsettle.so' SHA3-256 mismatch: expected 9f2c..., got 41ab...` | The artifact is not the one the digest pins. Rebuild it, or update `sha3_256`. |
| ``plugin `libsettle.so` declares schema fingerprint 4c1f8a20 but its component schemas hash to 90bb1e77`` | The plugin's embedded schemas and the fingerprint it declares disagree. Rebuild it from one definition. |
| ``plugin manifest field `schema_fingerprint` is `abc`, expected 8 lowercase hex characters`` | The plugin's self description is malformed. Rebuild it with an SDK that fills the field. |

## Next

- [A Rust plugin](@/service/plugins/rust.md): the shortest way to produce one of these
  libraries.
- [Processors in a workflow](@/service/processors/_index.md): the sandboxed node this one trades
  against.
