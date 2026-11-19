+++
title = "Build your own processor"
description = "The pipeline contract is a WASI 0.3 WIT world (`saci:pipeline@0.4.0`). Components targeting `wasm32-wasip3` run inside saci-service with Component Model Async."
template = "section.html"
sort_by = "weight"
weight = 4
aliases = ["/processors/", "/processors/languages/", "/guests/"]
+++
`saci-service` does not know application logic at compile time. It knows one WASI 0.3 WIT world,
`saci:pipeline@0.4.0`, and any component built against that world is a processor.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 170" role="img" aria-labelledby="gd-title gd-desc">
        <title id="gd-title">The shape of a SACI processor: two calls in, one loop back</title>
        <desc id="gd-desc">
            saci-service calls describe on the processor once, at load, to learn its row schemas
            and fingerprint. It then calls async run-batch once per batch, passing Arrow IPC input
            and the prior checkpoint, and gets back a run-result or a run-error. The processor
            exports exactly those two functions and imports host-io for config, metrics and
            logging. Any state the processor keeps must round-trip through the checkpoint the
            host holds and replays as the next call's prior.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="40" width="170" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="40" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="52" width="170" height="8"/>
            <text class="t-lbl" x="12" y="55">saci-service</text>
            <text class="t-sm" x="12" y="76">loads the .wasm</text>
            <text class="t-sm" x="12" y="89">holds the checkpoint</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="450" y="40" width="210" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="450" y="40" width="210" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="450" y="52" width="210" height="8"/>
            <text class="t-lbl" x="462" y="55">your processor &middot; .wasm</text>
            <text class="t-sm t-bnd" x="462" y="76">exports pipeline</text>
            <text class="t-sm t-bnd" x="462" y="89">imports host-io</text>
            <text class="t-sm" x="462" y="102">WASI 0.3 component</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="310" y="44">describe() &rarr; descriptor, once at load</text>
            <path class="arw arw-ctl" d="M170 50 H450" marker-end="url(#gd-c)"/>
            <text class="t-sm t-mid" x="310" y="62">async run-batch(input, prior)</text>
            <path class="arw arw-data" d="M170 68 H450" marker-end="url(#gd-d)"/>
            <path class="arw arw-data" d="M450 86 H170" marker-end="url(#gd-d)"/>
            <text class="t-sm t-mid" x="310" y="98">run-result | run-error</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M25 96 V130 H105 V96" marker-end="url(#gd-b)"/>
            <text class="t-sm t-bnd t-mid" x="65" y="144">checkpoint &rarr; prior</text>
        </g>
        <defs>
            <marker id="gd-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="gd-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="gd-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> describe(), and whatever the processor calls into host-io</span>
        <span class="k-data"><i></i> run-batch: Arrow IPC bytes in, Arrow IPC bytes or an error out</span>
        <span class="k-boundary"><i></i> checkpoint: the one thing that survives a batch</span>
    </div>
    <figcaption class="dgm-cap">
        <code>describe()</code> runs once, at load, which makes the descriptor the easiest part
        to get wrong: a row-type list that disagrees with the workflow is refused before
        anything runs. An SDK derives both the list and the schema from one declaration, so the
        two cannot drift apart.
    </figcaption>
</div>

## 1. What your code provides

A processor is a WebAssembly component: a `.wasm` file built against the
[Component Model](https://component-model.bytecodealliance.org/), targeting pure WASI 0.3 with
Component Model Async. `saci-service` loads whichever `.wasm` your config names using Wasmtime's
WASI 0.3 linker and drives `describe` on load and async `run-batch` per batch.

Your code answers two calls.

`describe` runs once, when the processor loads. It reports the processor's name, its version,
the row types it reads and writes with their columns, one fingerprint over those declarations,
and whether it keeps state.

`run-batch` runs once per batch as a Component Model Async export. Rows go in, rows come back,
and with them an optional state blob the host stores and hands back as the next batch's prior.
A failure comes back as a message, never as a crash.

A processor asks the host for three capabilities through `host-io`: a config value by name, a
metric observation, and a log line. [The wire format](@/library/reference/wire-format.md) specifies
the bytes a processor receives and returns.

## 2. Authoring paths

| Target | Toolchain | When to use | Guide |
|---|---|---|---|
| **Rust WASI 0.3 component** (`wasm32-wasip3`) | `cargo build --release --target wasm32-wasip3` (Rust 1.100+) | Sandboxed, epoch-bounded pipeline stages built directly with `saci`'s `processor` SDK | [A Rust processor](@/service/processors/build/rust.md) |
| **Any WASI 0.3 toolchain** | Language componentizer targeting pure WASI 0.3 + Component Model Async | Custom components compiled directly against `crates/saci/wit/pipeline.wit` (`saci:pipeline@0.4.0`) | [The WIT contract](@/service/processors/build/wit-contract.md) |
| **Native plugin** (Go, C#, Kotlin, C/C++) | C shared library exporting `saci_abi_version` and `saci_plugin_v1` | Non-Rust languages or components needing native threads | [Plugins in a workflow](@/service/plugins/_index.md) |

## 3. Build, validate, declare

Point every component toolchain at the single canonical WIT package:

```text,name=The one canonical WIT package
crates/saci/wit/pipeline.wit
```

You need `wasm-tools`:

```bash,name=Install wasm-tools once
cargo install wasm-tools --locked --version 1.246.2
```

Runs the same on Linux, macOS and Windows (PowerShell). Every processor build verifies its
component with two commands before loading it in a config:

Linux/macOS:

```bash,name=Verify a WASI 0.3 component
wasm-tools validate --features cm-async <component>.wasm
wasm-tools component wit <component>.wasm | grep 'saci:pipeline'
```

Windows (PowerShell):

```powershell
wasm-tools validate --features cm-async <component>.wasm
wasm-tools component wit <component>.wasm | Select-String 'saci:pipeline'
```

The second command must print a world importing `saci:pipeline/host-io@0.4.0` and exporting
`saci:pipeline/pipeline@0.4.0`, with zero `wasi:*@0.2.` imports:

```text,name=Expected wasm-tools output
  import saci:pipeline/host-io@0.4.0;
  import saci:pipeline/types@0.4.0;
  export saci:pipeline/pipeline@0.4.0;
package saci:pipeline@0.4.0 {
```

If it prints anything else, stop. Check that the toolchain targets WASI 0.3 and Component Model
Async rather than WASI 0.2.

A component that verifies is one `wasm` node away from running:

```kdl,name=Declaring the component you just built
wasm "settle" module="target/wasm32-wasip3/release/settle_wasm.wasm" {
    config ledger_tag="prod"
}
```

[Processors in a workflow](@/service/processors/_index.md) covers every key of that node, the
digest you can pin on it, and what the graph check compares it against.

## 4. Logging, config and state from a processor

A processor's `println!` goes nowhere, because the host discards whatever it writes to standard
output or standard error. Its `host-io::log` call is the only log channel out, and those lines
appear in the service's own log and in the dashboard's Logs tab.

Config arrives as strings. A processor asks for a key by name and gets back the string the
`wasm` node's `config` block declared, or nothing. Parsing it, and choosing what an absent key
means, is the processor's own decision.

The state blob is the only thing that survives a batch. A value left in a global or a static
is gone by the next call (unless warm-instance pooling is enabled, which preserves linear memory
across healthy batches as an optimisation rather than a contract). Return state as the checkpoint
instead and the host hands it back as the next batch's prior. What the host does around each call
is described in [how the host runs a processor](@/library/processor-host.md).

## Next

- [A Rust processor](@/service/processors/build/rust.md): build and run the stateful `settle`
  component targeting `wasm32-wasip3`.
- [Processors in a workflow](@/service/processors/_index.md): the node that runs what you built.
