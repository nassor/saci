+++
title = "Build your own processor"
description = "The pipeline contract is a WIT world, not an SDK. Any component that exports describe and run-batch is a SACI pipeline."
template = "section.html"
sort_by = "weight"
weight = 4
aliases = ["/processors/", "/processors/languages/", "/guests/", "/polyglot/", "/polyglot/writing-a-guest/", "/guests/six-languages/", "/guests/four-languages/", "/processors/four-languages/", "/processors/six-languages/"]
+++
`saci-service` does not know Rust. It knows one WIT world, `saci:pipeline@0.3.0`, and anything
that compiles to that shape is a processor.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 170" role="img" aria-labelledby="gd-title gd-desc">
        <title id="gd-title">The shape of a SACI processor: two calls in, one loop back</title>
        <desc id="gd-desc">
            saci-service calls describe on the processor once, at load, to learn its row schemas
            and fingerprint. It then calls run-batch once per batch, passing Arrow IPC input and
            the prior checkpoint, and gets back a run-result or a run-error. The processor
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
            <text class="t-sm" x="462" y="102">any language, WASI 0.2</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="310" y="44">describe() &rarr; descriptor, once at load</text>
            <path class="arw arw-ctl" d="M170 50 H450" marker-end="url(#gd-c)"/>
            <text class="t-sm t-mid" x="310" y="62">run-batch(input, prior)</text>
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
[Component Model](https://component-model.bytecodealliance.org/), targeting WASI 0.2. Rust's
`wasm32-wasip2` target is one way to produce one, and Go, Python, TypeScript, Kotlin and C#
each have their own. `saci-service` loads whichever `.wasm` your config names and never learns
what built it.

Your code answers two calls.

`describe` runs once, when the processor loads. It reports the processor's name, its version,
the row types it reads and writes with their columns, one fingerprint over those declarations,
and whether it keeps state.

`run-batch` runs once per batch. Rows go in, rows come back, and with them an optional state
blob the host stores and hands back as the next batch's prior. A failure comes back as a
message, never as a crash.

A processor also asks the host for three things rather than doing them itself: a config value
by name, a metric observation, and a log line.

Filling those records in by hand is optional. Each language below has a small SDK that reads a
row type declared in that language. It derives the columns, the fingerprint and the description
from the declaration, decodes the batch into row values, runs your transforms, re-encodes, and
turns any failure into a permanent error. Because the declaration is the schema, six
independently written row types report one fingerprint.
[The wire format](@/library/reference/wire-format.md) specifies the bytes a processor receives
and must return.

## 2. Choose a language

| Language | SDK package | Toolchain | Page |
|---|---|---|---|
| Rust | none needed: `saci-processor` carries it | `cargo build --target wasm32-wasip2`, Rust 1.95+ | [A Rust processor](@/service/processors/build/rust.md) |
| Go | `github.com/nassor/saci/packages/saci-sdk-go` | `componentize-go` 0.4.1, Go 1.25.5+ | [A Go processor](@/service/processors/build/go.md) |
| Python | `saci-sdk` | `componentize-py` 0.25.0, Python 3.10+ | [A Python processor](@/service/processors/build/python.md) |
| TypeScript | `@nassor/saci-sdk` | `jco` 1.30.0 and `typescript` 5.9.3, Node 24.12+ | [A TypeScript processor](@/service/processors/build/typescript.md) |
| Kotlin | `io.github.nassor:saci-sdk-kt` with the KSP processor `saci-sdk-kt-ksp` | Kotlin 2.4.0 and Gradle 8.14.4+, JDK 21 | [A Kotlin processor](@/service/processors/build/kotlin.md) |
| C# | `Saci.Sdk` with the generator `Saci.Sdk.Generators` | `componentize-dotnet` on .NET 10 | [A C# processor](@/service/processors/build/csharp.md) |

<div class="note">
<span class="note-label">If your language has no Arrow library</span>

Go, Python, TypeScript, Kotlin and C# have no WASI 0.2 friendly Arrow library today, so each of
those SDKs carries its own reader and its own in-place writers.
[The SDK packages](@/library/reference/sdk-packages.md) lists the coordinates, the shared API and
what those readers refuse. Rust needs none of it, because `saci-processor` handles the encoding
itself.

</div>

Versions above are the ones CI installs. The full pin list, including the toolchain caveats that
cost an hour each, lives in `examples/polyglot/PINS.md`. A seventh language starts from
[the WIT contract](@/service/processors/build/wit-contract.md) instead of an SDK.

## 3. Build, validate, declare

Point every toolchain at the same WIT package. It is the single canonical copy, and vendoring a
second one is how two builds end up on two worlds.

```text,name=The one canonical WIT package
crates/saci-processor/wit/pipeline.wit
```

You need `wasm-tools` once, whatever the language:

```bash,name=Install wasm-tools once
cargo install wasm-tools --locked --version 1.246.2
```

Runs the same on Linux, macOS and Windows (PowerShell).

Every language recipe ends with the same two commands. This is their one home; each language
page links back here from its build step.

Linux/macOS:

```bash,name=The two commands every recipe ends with
wasm-tools validate --features component-model <component>.wasm
wasm-tools component wit <component>.wasm | grep 'saci:pipeline'
```

Windows (PowerShell):

```powershell
wasm-tools validate --features component-model <component>.wasm
wasm-tools component wit <component>.wasm | Select-String 'saci:pipeline'
```

The second command must print a world importing `saci:pipeline/host-io@0.3.0` and exporting
`saci:pipeline/pipeline@0.3.0`:

```text,name=Expected wasm-tools output
  import saci:pipeline/host-io@0.3.0;
  import saci:pipeline/types@0.3.0;
  export saci:pipeline/pipeline@0.3.0;
package saci:pipeline@0.3.0 {
```

If it prints anything else, stop. Nothing past this point works, and the fix is almost always
the bindings step rather than the processor code.

A component that verifies is one `wasm` node away from running:

```kdl,name=Declaring the component you just built
wasm "enrich" module="examples/polyglot/build/enrich-py.wasm" {
    config fx_eur="1.10"
}
```

[Processors in a workflow](@/service/processors/_index.md) covers every key of that node, the
digest you can pin on it, and what the graph check compares it against.

## 4. Six languages, one pipeline

`examples/polyglot/` runs one `Order` row type through six stages in six languages, chained in
one process. Each stage writes one column and reads what an earlier stage wrote, so a wrong
codec in any language produces visibly wrong numbers rather than a silent byte difference.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 250" role="img" aria-labelledby="pg-title pg-desc">
        <title id="pg-title">One Order batch through six WebAssembly processors, one per language</title>
        <desc id="pg-desc">
            Six rows of Order enter validate-go, written in Go, which writes valid. Its
            output feeds enrich-py, written in Python, which writes usd_amount. That feeds
            score-ts, written in TypeScript, which writes risk_score and flagged. The stream
            then wraps to a second row of stages and feeds fee-kt, written in Kotlin, which
            writes fee. That feeds tier-cs, written in C sharp, which writes review_tier.
            That feeds settle-rs, written in Rust, which writes settlement and is the only
            stage whose state survives to the next batch, via a checkpoint that loops back
            into it rather than downstream.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="70" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="70" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="70" height="8"/>
            <text class="t-lbl" x="12" y="51">Order</text>
            <text class="t-sm" x="12" y="72">6 rows</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M70 64 H92" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="92" y="36" width="120" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="92" y="36" width="120" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="92" y="48" width="120" height="8"/>
            <text class="t-lbl" x="104" y="51">validate-go</text>
            <text class="t-sm" x="104" y="72">Go</text>
            <text class="t-sm t-data" x="104" y="85">+ valid</text>
            <path class="arw arw-data" d="M212 64 H234" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="234" y="36" width="130" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="234" y="36" width="130" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="234" y="48" width="130" height="8"/>
            <text class="t-lbl" x="246" y="51">enrich-py</text>
            <text class="t-sm" x="246" y="72">Python</text>
            <text class="t-sm t-data" x="246" y="85">+ usd_amount</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M364 64 H386" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="386" y="36" width="140" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="386" y="36" width="140" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="386" y="48" width="140" height="8"/>
            <text class="t-lbl" x="398" y="51">score-ts</text>
            <text class="t-sm" x="398" y="72">TypeScript</text>
            <text class="t-sm t-data" x="398" y="85">+ risk_score</text>
            <text class="t-sm t-data" x="398" y="98">+ flagged</text>
            <path class="arw arw-data" d="M526 64 H556 V124 H32 V150" marker-end="url(#pg-d)"/>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-bnd" x="0" y="150" width="120" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="150" width="120" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="162" width="120" height="8"/>
            <text class="t-lbl" x="12" y="165">fee-kt</text>
            <text class="t-sm" x="12" y="186">Kotlin</text>
            <text class="t-sm t-data" x="12" y="199">+ fee</text>
            <path class="arw arw-data" d="M120 178 H142" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="142" y="150" width="130" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="142" y="150" width="130" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="142" y="162" width="130" height="8"/>
            <text class="t-lbl" x="154" y="165">tier-cs</text>
            <text class="t-sm" x="154" y="186">C#</text>
            <text class="t-sm t-data" x="154" y="199">+ review_tier</text>
            <path class="arw arw-data" d="M272 178 H294" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="294" y="150" width="130" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="294" y="150" width="130" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="294" y="162" width="130" height="8"/>
            <text class="t-lbl" x="306" y="165">settle-rs</text>
            <text class="t-sm" x="306" y="186">Rust</text>
            <text class="t-sm t-data" x="306" y="199">+ settlement</text>
            <path class="arw arw-bnd" d="M414 206 V228 H334 V206" marker-end="url(#pg-b)"/>
            <text class="t-sm t-bnd t-mid" x="374" y="242">checkpoint</text>
        </g>
        <defs>
            <marker id="pg-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="pg-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> Order rows, and the field each stage writes</span>
        <span class="k-boundary"><i></i> the WebAssembly component boundary</span>
    </div>
    <figcaption class="dgm-cap">
        Every arrow between stages is a length-prefixed Arrow IPC stream. The loop under
        <code>settle-rs</code> is different: a checkpoint never goes downstream. It comes back
        to the same processor as the next call's <code>prior</code>.
    </figcaption>
</div>

| Stage | Language | Toolchain | Writes |
|---|---|---|---|
| `validate-go` | Go | `componentize-go` | `valid` |
| `enrich-py` | Python | `componentize-py` | `usd_amount` |
| `score-ts` | TypeScript | `jco` | `risk_score`, `flagged` |
| `fee-kt` | Kotlin | `wasm-tools` | `fee` |
| `tier-cs` | C# | `componentize-dotnet` | `review_tier` |
| `settle-rs` | Rust | `cargo` | `settlement` |

Only `settle-rs` keeps state. Its ledger survives the batch boundary through the checkpoint the
host replays as its prior. The ledger nets the Kotlin stage's `fee` out of the Python stage's
`usd_amount`, over the rows the C# stage cleared, so one number depends on five languages
agreeing on the same bytes.

Every stage asks the host for something. All six report a metric and log a line, and five also
read config keys. The Python stage reads three FX rates, and the Kotlin stage one rate per
region.

### Run it

```bash,name=Build the six components then drive the chain
cargo xtask polyglot
cargo run -p saci-service --features wasm,tracing --example polyglot_orders
```

Runs the same on Linux, macOS and Windows (PowerShell). The first command needs Go, Python,
Node, a JDK and .NET; the second prints both batches.

The driver prints all six self descriptions first. All six must report the same fingerprint and
declare `["Order"]`. Each stage derives its fingerprint from its own row type, so the driver
compares the six reported values against each other and exits non-zero the moment one
disagrees.

## Logging, config and state from a processor

A processor's `println!` goes nowhere, because the host discards whatever it writes to standard
output or standard error. Its SDK's log call is the only channel out, and those lines appear in
the service's own log and in the dashboard's Logs tab.

Config arrives as strings. A processor asks for a key by name and gets back the string the
`wasm` node's `config` block declared, or nothing. Parsing it, and choosing what an absent key
means, is the processor's own decision.

The state blob is the only thing that survives a batch. A value left in a global or a static
is gone by the next call. Return it as the checkpoint instead and the host hands it back as the
next batch's prior. What the host does around each call is described in
[how the host runs a processor](@/library/processor-host.md).

## Next

- [A Rust processor](@/service/processors/build/rust.md): the shortest recipe, with no
  componentizer in the build.
- [Processors in a workflow](@/service/processors/_index.md): the node that runs what you built.
