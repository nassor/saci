+++
title = "A Kotlin processor"
description = "Kotlin 2.4.0's experimental Component Model support: three annotations a KSP processor turns into the export glue, the two wasm-tools passes Gradle does not run, and the Wasm GC proposals the host has to allow."
template = "page.html"
weight = 5
aliases = ["/processors/kotlin/", "/guests/kotlin/"]
+++
# A Kotlin processor

`fee-kt.wasm` is a WebAssembly component a `wasm` node in your KDL config
loads. It reads the `valid`, `region` and `usd_amount` columns of an `Order`
batch and writes the `fee` column. The stage source is one data class and two
annotated functions. `saci-sdk-kt-ksp` reads the annotations at compile time
and generates the row accessor and the `saci:pipeline/pipeline` export.

Kotlin 2.4.0 carries experimental Component Model support, and two consequences
shape the build. WIT bindings come from JetBrains'
[`wit-bindgen` fork](https://github.com/Kotlin/wit-bindgen), not from Gradle.
Gradle emits a core wasm module, so componentizing it is a separate `wasm-tools`
pass.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 152" role="img" aria-labelledby="kt-title kt-desc">
        <title id="kt-title">One CSV batch through the Kotlin fee stage and back out to a CSV file</title>
        <desc id="kt-desc">
            Six Order rows are read from examples/configs/fixtures/polyglot_orders.csv and
            handed to the wasm node running fee-kt.wasm, whose output is written to
            /tmp/saci-polyglot-out.csv. The Kotlin component reads the valid, region and
            usd_amount columns, multiplies usd_amount by the rate the host injected under the
            key fee_ followed by the row's region, and writes the fee column. The other
            eleven columns pass through unchanged.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="40" width="200" height="68" rx="8"/>
            <rect class="hd hd-data" x="0" y="40" width="200" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="200" height="8"/>
            <text class="t-lbl" x="12" y="55">polyglot_orders.csv</text>
            <text class="t-sm" x="12" y="77">six Order rows</text>
            <text class="t-sm" x="12" y="91">twelve columns</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M200 74 H250" marker-end="url(#kt-d)"/>
            <rect class="blk blk-bnd" x="250" y="40" width="170" height="68" rx="8"/>
            <rect class="hd hd-bnd" x="250" y="40" width="170" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="250" y="52" width="170" height="8"/>
            <text class="t-lbl" x="262" y="55">wasm &middot; fee-kt</text>
            <text class="t-sm t-bnd" x="262" y="77">reads valid, region,</text>
            <text class="t-sm t-bnd" x="262" y="91">usd_amount</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M420 74 H470" marker-end="url(#kt-d)"/>
            <rect class="blk blk-data" x="470" y="40" width="190" height="68" rx="8"/>
            <rect class="hd hd-data" x="470" y="40" width="190" height="20" rx="8"/>
            <rect class="hd hd-data" x="470" y="52" width="190" height="8"/>
            <text class="t-lbl" x="482" y="55">saci-polyglot-out.csv</text>
            <text class="t-sm" x="482" y="77">in /tmp</text>
            <text class="t-sm" x="482" y="91">fee column written</text>
        </g>
        <g class="anim anim-4">
            <text class="t-sm t-mid" x="330" y="132">fee = usd_amount &times; config fee_&lt;region&gt;</text>
        </g>
        <defs>
            <marker id="kt-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane: Arrow IPC bytes in, Arrow IPC bytes out</span>
        <span class="k-boundary"><i></i> the Kotlin component, and the config keys it reads</span>
    </div>
    <figcaption class="dgm-cap">
        The stage writes one column and hands the other eleven back untouched. The rate is
        not in the data: a <code>region</code> string picks the config key, so
        <code>fee_emea</code>, <code>fee_apac</code> and <code>fee_amer</code> are what the
        <code>wasm</code> node has to carry.
    </figcaption>
</div>

## What you need

JDK 21 and Gradle 8.14.4 or newer, verified on Temurin 21.0.12. Kotlin 2.4.0
itself needs no install, because `build.gradle.kts` pins the version and Gradle
fetches the compiler. The KSP plugin is 2.3.11, the release that pairs with
Kotlin 2.4.0. KSP's versioning tracks the Kotlin version from 2.3.0, and there
is no 2.4.x line.

Two published artifacts do the authoring work. `io.github.nassor:saci-sdk-kt` is
the runtime the generated export calls, and it carries the Arrow IPC codec in
`io.github.nassor.saci.arrowipc`, so one dependency resolves both.
`io.github.nassor:saci-sdk-kt-ksp` is the symbol processor that reads your
annotations. Both resolve from `https://nassor.github.io/saci/maven`, the Maven
repository this site serves, alongside
[the other language SDKs](@/library/reference/sdk-packages.md).

The bindings generator is a fork on its own branch, with no released version to
pin:

```bash,name=Install the bindings generator and wasm-tools
cargo install wit-bindgen-cli --git https://github.com/Kotlin/wit-bindgen --branch kotlin
cargo install wasm-tools --locked --version 1.246.2
```
Runs the same on Linux, macOS and Windows (PowerShell).

That puts `wit-bindgen` 0.57.1 on `PATH`. The last build step also needs
`wasi_snapshot_preview1.reactor.wasm`, the WASI preview 1 reactor adapter.
`cargo xtask polyglot` downloads it into `examples/polyglot/generated/` when it
is absent, and `SACI_WASI_ADAPTER` names a copy of your own.

<div class="note note-warn">
<span class="note-label">Your component needs three Wasm proposals</span>

Kotlin/Wasm compiles classes to Wasm GC types, so the finished component uses the
gc, exception-handling and function-references proposals. Whatever loads it has
to allow all three. `saci-service` allows them with no configuration, so this
component runs there unchanged.

</div>

## 1. Create the project

The `wasmWasi` target, one dependency, and the KSP processor on the
configuration Gradle derives from that target:

```kotlin,name=build.gradle.kts
@file:OptIn(ExperimentalWasmDsl::class)

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl

plugins {
    kotlin("multiplatform") version "2.4.0"
    id("com.google.devtools.ksp") version "2.3.11"
}

repositories {
    maven("https://nassor.github.io/saci/maven")
    mavenCentral()
}

kotlin {
    wasmWasi {
        binaries.executable()
        nodejs()
    }

    sourceSets {
        val wasmWasiMain by getting {
            dependencies {
                implementation("io.github.nassor:saci-sdk-kt:0.1.0")
            }
        }
    }
}

dependencies {
    add("kspWasmWasi", "io.github.nassor:saci-sdk-kt-ksp:0.1.0")
}
```

The KSP configuration is `kspWasmWasi`, the name Gradle derives from the target.
A KSP processor runs on the JVM-hosted compiler whatever target it inspects, so
that artifact is an ordinary JVM jar. An in-repo build swaps the repository for
`mavenLocal()`, which is where `gradle publishToMavenLocal` in
`packages/saci-sdk-kt` and then `packages/saci-sdk-kt-ksp` puts both.

One more file decides the artifact's name:

```kotlin,name=settings.gradle.kts
rootProject.name = "fee-kt"
```

Gradle names the core wasm module after the root project, so this line is why
step 5 produces `fee-kt.wasm` rather than something else. Your source goes in
`src/wasmWasiMain/kotlin/impl/`, package `impl`, which step 4 explains.

## 2. Declare the row type

`@SaciComponent` marks the row type of the one component this processor operates
on. It must be a `data class` whose constructor takes every field, because the
generated `decode` calls that constructor:

```kotlin,name=The row type as an annotated data class
@SaciComponent
data class Order(
    val id: Long,
    val region: String,
    val currency: String,
    val amount: Double,
    var valid: Boolean = false,
    var usdAmount: Double = 0.0,
    var usdAmountDisplay: String = "",
    var riskScore: Double = 0.0,
    var flagged: Boolean = false,
    var fee: Double = 0.0,
    var reviewTier: Long = 0,
    var settlement: String = "",
)
```

The declaration is the schema. Property order is wire order, so reordering these
lines is a wire change. A `val` is an input the processor reads; a `var` is an
output it may write. Wire names are the snake_case of the property names, so
`usdAmount` is `usd_amount`.

`Long`, `Double`, `Boolean` and `String` map to `Int64`, `Float64`, `Boolean` and
`Utf8`. A nullable property, or any other type, is a compile error from KSP that
names the property. Two stages agree when they declare the same field names in
the same order, and [the wire format](@/library/reference/wire-format.md)
specifies the algorithm.

## 3. Write the transform

`@SaciTransform` marks a function taking the row type and a `SaciConfig`. It
mutates the row it is handed and returns nothing:

```kotlin,name=The fee transform
@SaciTransform
fun fee(row: Order, config: SaciConfig) {
    row.fee = if (row.valid) row.usdAmount * config.double("fee_${row.region}", 0.0) else 0.0
    if (row.valid) {
        config.metric("fee.charged_rows", 1.0)
        config.metric("fee.total_usd", row.fee)
    }
}
```

The Python stage picks a config key from a `Utf8` column too, `currency`, but
from a fixed table of three. This one builds the key out of the column:
`region` is data, and `fee_` plus that string is the key an operator sets. An
absent or unparseable rate folds into the `0.0` default rather than failing the
batch, so a misconfigured region charges nothing and still shows up in
`fee.charged_rows`.

## 4. Export it

`@SaciProcessor` marks the builder. Its three arguments become
`pipeline-descriptor.name`, `.version` and the log target the runtime's per-batch
summary line goes to:

```kotlin,name=The processor builder
@SaciProcessor("polyglot-fee-kt", "0.1.0", "polyglot::fee_kt")
fun build(): SaciPipeline = SaciPipeline.of(::fee)
```

`SaciPipeline.of` takes the transforms in the order they run, each over every row
of the batch before the next one starts. Add `main()` as an empty function,
because `binaries.executable()` requires an entry point. Nothing ever calls it,
since the host drives the component through the `pipeline` export.

Those four declarations are the whole file. KSP reads the annotations and emits
`impl.OrderCodec`, the typed row accessor, and `impl.PipelineImpl`, the export
object that folds every failure into `run-error::permanent`. It generates that
code because Kotlin/Wasm has no reflection at all. `kotlin-reflect` is JVM only,
so a property name exists in a `wasmWasi` binary only if the build wrote it
there.

Package `impl` is not a choice. `wit-bindgen kotlin --kotlin-imports 'impl.*'`
generates a trampoline that resolves `PipelineImpl.describe()` and
`PipelineImpl.runBatch()` from that package by those exact names, so your
annotated declarations live there too. KSP refuses a processor annotated anywhere
else.

The rest of the WIT mapping, which the generated glue is written against:

| WIT | Kotlin |
|-----|--------|
| `record` | class with `var` fields and a positional constructor |
| `variant` | sealed interface, one class per arm: `Types.RunError.Permanent` |
| `enum` | `enum class`, arms SHOUTY_SNAKE_CASE: `HostIo.LogLevel.INFO` |
| `option<T>` | nullable `T?` |
| `list<u8>` | boxed `List<UByte>` |
| `result<T, E>` | `kotlin.Result<T>`, `E` inside `ComponentException` |
| imported interface | companion object: `HostIo.getConfig(key)` |

`list<u8>` as a boxed `List<UByte>` is the one mapping with a price attached. A
large payload pays a per-element conversion at the boundary, so the SDK moves to
a `ByteArray` on the way in and back with `asUByteArray().asList()` on the way
out.

## 5. Build and validate

Four commands: generate the bindings, compile, attach the world, link the
adapter. `--kotlin-imports` names the package the generator looks for your
implementation in, and the WIT path is relative to a stage sitting four levels
below the repository root.

Linux/macOS:

```bash,name=Generate bindings, compile and componentize
wit-bindgen kotlin --kotlin-imports 'impl.*' ../../../../crates/saci-processor/wit \
  --out-dir src/wasmWasiMain/kotlin/bindings
gradle compileProductionExecutableKotlinWasmWasiOptimize
wasm-tools component embed ../../../../crates/saci-processor/wit \
  build/compileSync/wasmWasi/main/productionExecutable/optimized/fee-kt.wasm \
  -o build/compileSync/wasmWasi/main/productionExecutable/optimized/fee-kt-embedded.wasm
wasm-tools component new \
  build/compileSync/wasmWasi/main/productionExecutable/optimized/fee-kt-embedded.wasm \
  --adapt wasi_snapshot_preview1=../../generated/wasi_snapshot_preview1.reactor.wasm \
  -o fee-kt.wasm
```

Windows (PowerShell):

```powershell
$opt = "build\compileSync\wasmWasi\main\productionExecutable\optimized"
wit-bindgen kotlin --kotlin-imports 'impl.*' ..\..\..\..\crates\saci-processor\wit --out-dir src/wasmWasiMain/kotlin/bindings
gradle compileProductionExecutableKotlinWasmWasiOptimize
wasm-tools component embed ..\..\..\..\crates\saci-processor\wit "$opt\fee-kt.wasm" -o "$opt\fee-kt-embedded.wasm"
wasm-tools component new "$opt\fee-kt-embedded.wasm" --adapt wasi_snapshot_preview1=..\..\generated\wasi_snapshot_preview1.reactor.wasm -o fee-kt.wasm
```

`wit-bindgen` writes `SaciPipeline.kt`, `InternalSaciPipeline.kt` and
`runtime/ComponentSupport.kt` under `bindings/`. Its output is documented as
non-deterministic, so keep that directory out of version control and regenerate
it every build. The result of the four commands is one file in the stage
directory: `fee-kt.wasm`, a component of about 83 KB.

<div class="note note-warn">
<span class="note-label">Gradle stops one step short</span>

`compileProductionExecutableKotlinWasmWasiOptimize` produces a core module with
no WIT metadata and no WASI adapter, so a component host cannot load it. The
last two commands are what make it a component. `embed` attaches the world to
the module, and `new` links the reactor adapter so the processor's
`wasi_snapshot_preview1` imports resolve.

</div>

Before wiring it into a config, run
[the two verify commands](@/service/processors/build/_index.md) from the build
hub against `fee-kt.wasm`. One proves the file is a valid component, and the
other prints the interfaces it imports and exports.

`cargo xtask polyglot` runs every step of this section, including the two SDK
publications and the adapter download, and copies the artifact to
`examples/polyglot/build/fee-kt.wasm`:

```bash,name=Build all six polyglot stages
cargo xtask polyglot
```
Runs the same on Linux, macOS and Windows (PowerShell).

## 6. Run it under saci-service

`examples/configs/standalone_polyglot.kdl` runs one processor between a CSV
source and a CSV sink. It names the Python stage, and two edits point it at this
one: the `wasm` node's `module`, and its `config` keys. The node keeps its name,
because the two `link` lines in the workflow refer to it.

```kdl,name=The wasm node for the Kotlin stage
wasm "enrich" module="examples/polyglot/build/fee-kt.wasm" {
    config fee_emea="0.012" fee_apac="0.008" fee_amer="0.010"
}
```

One key per region in the fixture: `emea`, `apac` and `amer`. A region with no
key charges zero rather than failing the batch, so a typo in a key name is
visible in the output column rather than in an error.

Validate first, then serve. Both run from the repository root, because the paths
in the config are relative to the working directory.

Linux/macOS:

```bash,name=Validate the config, then run it
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- \
  validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- \
  serve --config examples/configs/standalone_polyglot.kdl
```

Windows (PowerShell):

```powershell
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_polyglot.kdl
```

`run_mode kind="one_shot"` means `serve` processes the file once and stops. The
observable result is `/tmp/saci-polyglot-out.csv`, twelve columns wide, with the
`fee` column written by this stage and the other eleven passed through. The
fixture seeds `usd_amount` at `0.0`, because the Python stage fills that column
and is not in this pipeline, so a single-stage run charges real rates against
zero. Chain the Python stage in front of this one for non-zero fees, which is
what the six-stage driver `examples/polyglot/polyglot_orders.rs` does.

## Config, logs and state

`SaciConfig` is everything a transform can reach, and both of its methods exist to
keep the component boundary quiet. `config.double(key, default)` memoises every
key it resolves for the length of the batch, so a per-row lookup costs one
`get-config` call per distinct region. `config.metric(name, value)` accumulates
into a counter the runtime reports once when the batch ends, so the two lines in
step 3 are two host calls per batch however many rows contributed.

Config values arrive as strings, and parsing them is the processor's job.
Distinguishing an absent key from an unparseable one means reading that string
yourself, through the generated `HostIo.getConfig(key)`, which is `null` when the
key is absent.

`println` goes nowhere. The host discards a processor's stdout and stderr, and
in Kotlin it is worse than silent. The `wasmWasi` target reaches the outside
world through WASI preview 1, and those calls trap once the adapter has wrapped
the module. The generated `HostIo.log(HostIo.LogLevel.INFO, target, message)`
is the only channel out, and the SDK runtime already emits one line per batch
to the target you named in `@SaciProcessor`.

The same trap makes `kotlin.time.TimeSource.Monotonic` and `kotlin.random.Random`
unusable, which is why this stage reports `run-metrics.wall-ns` as 0. A Kotlin
processor may call the WIT imports and nothing else. The failure mode is an
opaque wasm trap with no log line, so look here first when a Kotlin processor
dies mid-batch.

This stage is stateless. `SaciPipeline.of(::fee)` returns no checkpoint, and
every row's fee is a function of that row alone. When a processor does need to
remember something, the state blob it returns is the only thing that survives
to the next batch, and it comes back as the next call's prior.

## Next

- [The WIT contract](@/service/processors/build/wit-contract.md): every record
  the descriptor fills in, and what the host checks it against.
- [The build hub](@/service/processors/build/_index.md): the two verify commands,
  and this stage's place in the six-language chain.
