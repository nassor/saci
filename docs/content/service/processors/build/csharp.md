+++
title = "A C# processor"
description = "componentize-dotnet on .NET 10: one dotnet build for a finished component, three attributes a source generator turns into the export, and the experimental NuGet feed the restore needs."
template = "page.html"
weight = 6
aliases = ["/processors/csharp/", "/guests/csharp/"]
+++
# A C# processor

`tier-cs.wasm` is a WASI 0.2 component that reads `flagged` and `risk_score`
from an `Order` row and writes `review_tier`. The steps below build it, run it
under `saci-service` against a CSV fixture, and read the column it filled.

The whole stage is one file, `examples/polyglot/stages/csharp-tier/TierStage.cs`:
a row class, one transform method and one assembly attribute. `Saci.Sdk`'s source
generator writes the export for you, so there is no binding glue to maintain.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 150" role="img" aria-labelledby="cs-title cs-desc">
        <title id="cs-title">One CSV file through the C# processor and out to a second CSV file</title>
        <desc id="cs-desc">
            saci-service reads twelve-column Order rows from examples/configs/fixtures/polyglot_orders.csv,
            hands each batch to the WebAssembly component tier-cs.wasm as Arrow IPC bytes, and writes the
            returned rows to /tmp/saci-polyglot-out.csv. The component reads the flagged and risk_score
            columns and writes the review_tier column. Every other column is forwarded untouched, so the
            output file has the same twelve columns as the input.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="190" height="58" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="190" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="190" height="8"/>
            <text class="t-lbl" x="12" y="51">polyglot_orders.csv</text>
            <text class="t-sm" x="12" y="72">6 Order rows</text>
            <text class="t-sm" x="12" y="86">12 columns</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="245" y="36" width="180" height="58" rx="8"/>
            <rect class="hd hd-bnd" x="245" y="36" width="180" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="245" y="48" width="180" height="8"/>
            <text class="t-lbl" x="257" y="51">wasm tier-cs</text>
            <text class="t-sm t-bnd" x="257" y="72">reads flagged,</text>
            <text class="t-sm t-bnd" x="257" y="86">risk_score</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="470" y="36" width="190" height="58" rx="8"/>
            <rect class="hd hd-data" x="470" y="36" width="190" height="20" rx="8"/>
            <rect class="hd hd-data" x="470" y="48" width="190" height="8"/>
            <text class="t-lbl" x="482" y="51">saci-polyglot-out.csv</text>
            <text class="t-sm" x="482" y="72">review_tier filled,</text>
            <text class="t-sm" x="482" y="86">the rest forwarded</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M190 65 H245" marker-end="url(#cs-arrow)"/>
            <path class="arw arw-data" d="M425 65 H470" marker-end="url(#cs-arrow)"/>
            <text class="t-sm t-mid" x="330" y="118">writes review_tier: 0 clear, 1 review, 2 hold</text>
        </g>
        <defs>
            <marker id="cs-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane: CSV in, Arrow IPC across the boundary, CSV out</span>
        <span class="k-boundary"><i></i> the WebAssembly component you build here</span>
    </div>
    <figcaption class="dgm-cap">
        A processor overwrites columns in place and cannot add one, so
        <code>review_tier</code> is already a column of the input file. The stage decides its
        value; the other eleven columns come out exactly as they went in.
    </figcaption>
</div>

## What you need

The .NET SDK 10, which CI verifies on 10.0.400, from
[the .NET 10 download page](https://dotnet.microsoft.com/download/dotnet/10.0).
There is no `dotnet workload install` step. `componentize-dotnet` is a
Bytecode Alliance layer on top of that SDK, and it arrives through NuGet like
any package.

The authoring SDK is `Saci.Sdk` 0.1.0. It carries the Arrow IPC codec in its
`Saci.ArrowIpc` namespace and packs the `Saci.Sdk.Generators` source generator into
`analyzers/dotnet/cs/`, so one package reference brings both.

```bash,name=Install componentize-dotnet and the SDK
dotnet add package BytecodeAlliance.Componentize.DotNet.Wasm.SDK --version 0.8.0-preview00011
dotnet add package Saci.Sdk --version 0.1.0
```

Runs the same on Linux, macOS and Windows (PowerShell).

The first build downloads wasi-sdk 29.0 into `~/.wasi-sdk/`, about 535 MB over
the wire. Budget for it once per machine. The download URLs are hardcoded to
`x86_64`, so an arm64 machine cannot build this stage.

## 1. Create the project

Two files. The `.csproj` names the component build, and a `nuget.config` names
the feed the AOT backend ships on.

```xml,name=tier-cs.csproj
<PropertyGroup>
  <OutputType>Library</OutputType>
  <TargetFramework>net10.0</TargetFramework>
  <RootNamespace>PolyglotTier</RootNamespace>
  <AssemblyName>tier-cs</AssemblyName>
  <ImplicitUsings>enable</ImplicitUsings>
  <Nullable>enable</Nullable>
  <RuntimeIdentifier>wasi-wasm</RuntimeIdentifier>
  <UseAppHost>false</UseAppHost>
  <PublishTrimmed>true</PublishTrimmed>
  <InvariantGlobalization>true</InvariantGlobalization>
  <SelfContained>true</SelfContained>
  <IlcLlvmVersion>10.0.0-rc.1.26306.1</IlcLlvmVersion>
</PropertyGroup>

<ItemGroup>
  <ProjectReference Include="../../../../packages/saci-sdk-cs/Saci.Sdk.csproj" />
  <ProjectReference Include="../../../../packages/saci-sdk-cs/generator/Saci.Sdk.Generators.csproj"
                    OutputItemType="Analyzer"
                    ReferenceOutputAssembly="false" />
  <PackageReference Include="BytecodeAlliance.Componentize.DotNet.Wasm.SDK" Version="0.8.0-preview00011" />
  <PackageReference Include="runtime.$(NETCoreSdkPortableRuntimeIdentifier).Microsoft.DotNet.ILCompiler.LLVM" Version="$(IlcLlvmVersion)" />
  <Wit Include="../../../../crates/saci-processor/wit" World="saci-pipeline" />
</ItemGroup>
```

`RuntimeIdentifier=wasi-wasm`, `SelfContained` and `PublishTrimmed` are what turn
this project into a wasm component build. Nothing reflects at run time, so the
trimmer needs no root. The generator bakes every field accessor and transform
call into this compilation.

There is no bindings command. The `<Wit>` item points the build at the canonical
WIT package in place, and `dotnet build` generates from it. Vendoring a copy
would desynchronise the moment `pipeline.wit` changes.

The NativeAOT publish is wired in by the ILCompiler targets rather than by
`PublishAot`, and `AllowUnsafeBlocks` comes from the wit-bindgen props the
generated interop needs, so setting either here could only break them. In this
repository the two `ProjectReference` lines resolve the SDK locally; analyzers
do not travel across a `ProjectReference`, which is why the generator is named
separately with `OutputItemType="Analyzer"`.

`IlcLlvmVersion` is named because the LLVM variant of the ILCompiler opts out of
the SDK's implicit host-compiler resolution. Without that second
`PackageReference` the publish fails asking for it by name, for example
"Add a PackageReference for `runtime.win-x64.Microsoft.DotNet.ILCompiler.LLVM`
to allow cross-compilation for wasm".
Deriving the RID lets one project cross-compile from Linux, macOS and Windows.

<div class="note note-warn">
<span class="note-label">A nuget.config is mandatory</span>

`Microsoft.DotNet.ILCompiler.LLVM`, the AOT backend that targets `wasi-wasm`,
ships only on the `dotnet-experimental` feed. Without this file the restore of
`BytecodeAlliance.Componentize.DotNet.Wasm.SDK` resolves and its native
toolchain dependency does not, and the error names the missing package rather
than the missing feed.

```xml,name=nuget.config adds the experimental feed
<configuration>
  <packageSources>
    <clear />
    <add key="dotnet-experimental" value="https://pkgs.dev.azure.com/dnceng/public/_packaging/dotnet-experimental/nuget/v3/index.json" />
    <add key="nuget" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
```

`<clear />` keeps the restore independent of whatever feeds the machine has
configured globally.

</div>

## 2. Declare the row type

`[SaciComponent]` marks one component. The class name is the wire component name,
and its public settable properties are the columns, in declaration order:

```csharp,name=The Order row class
using Saci.Sdk;

namespace PolyglotTier
{
    [SaciComponent]
    public sealed class Order
    {
        public long Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public string Currency { get; set; } = string.Empty;
        public double Amount { get; set; }
        public bool Valid { get; set; }
        public double UsdAmount { get; set; }
        public string UsdAmountDisplay { get; set; } = string.Empty;
        public double RiskScore { get; set; }
        public bool Flagged { get; set; }
        public double Fee { get; set; }
        public long ReviewTier { get; set; }
        public string Settlement { get; set; } = string.Empty;
    }
}
```

The declaration is the schema, and property order is wire order. `long` maps to
Arrow `Int64`, `double` to `Float64`, `bool` to `Boolean` and `string` to `Utf8`.
Those four are the wire format's whole vocabulary, so any other property type is
a compile error naming the property.

Wire names are the snake_case form of the property names, so `UsdAmountDisplay`
addresses `usd_amount_display`, and `[SaciField("name")]` overrides one. The class
needs a public parameterless constructor and settable properties, because the SDK
materialises one instance per row.

Two stages agree when they declare the same field names in the same order, and
[the wire format](@/library/reference/wire-format.md) specifies the algorithm the
SDK uses to derive the value the host compares.

## 3. Write the transform

`[SaciTransform]` marks a static method taking `(TRow row)` or
`(TRow row, SaciConfig config)`. It mutates the row, and the SDK re-encodes every
column afterwards, so a transform may write any field, a string included:

```csharp,name=The tier transform
public static class TierStage
{
    private const string ReviewScoreKey = "review_score";
    private const double ReviewScoreDefault = 0.2;

    // Escalation levels, in the order the Rust stage reads them.
    private const long TierClear = 0;
    private const long TierReview = 1;
    private const long TierHold = 2;

    [SaciTransform]
    public static void Tier(Order row, SaciConfig config)
    {
        double reviewScore = config.GetDouble(ReviewScoreKey, ReviewScoreDefault);
        if (row.Flagged)
        {
            row.ReviewTier = TierHold;
            SaciHost.Count("tier.hold_rows");
        }
        else if (row.RiskScore >= reviewScore)
        {
            row.ReviewTier = TierReview;
            SaciHost.Count("tier.review_rows");
        }
        else
        {
            row.ReviewTier = TierClear;
        }
    }
}
```

A transform sees one row at a time. Transforms run in `Order` order first, then
in source declaration order, and each one runs over the whole batch before the
next starts.

Every column this stage does not write is forwarded untouched, because the host
replaces the whole dataset with what the batch returns.

## 4. Export it

You write no export. Add the assembly attribute and the generator emits the
export class into the same compilation:

```csharp,name=The one line that turns the attributes into an export
[assembly: SaciProcessor("polyglot-tier-cs", "0.1.0", LogTarget = "tier")]
```

Without it the generator emits nothing. Its two arguments become the processor's
reported name and version, and `LogTarget` is the target every log line carries,
defaulting to the name.

The generated code holds a getter and a setter delegate per property and one
delegate per transform, all resolved at compile time. Nothing reads a `Type` or
an attribute at run time, which is what keeps the component correct under
`PublishTrimmed`. A mistake in your declarations is a compile error with a file
and a line, not a failure inside a wasm component.

`result<T, E>` reaches C# as a thrown `WitException<E>` rather than a return
value, and it is the one mapping worth knowing before your first stage. The
rest of the WIT surface you meet in generated names maps like this:

| WIT | C# |
|-----|-----|
| `record` | `struct` with public fields and a positional constructor |
| `variant` | class with a `Tag` byte and `Tags` constants: `ITypesImports.RunError.Permanent(msg)` |
| `enum` | `enum`, arms SHOUTY_SNAKE_CASE: `IHostIoImports.LogLevel.ERROR` |
| `option<T>` | nullable: `byte[]?`, `string?` |
| `list<u8>` | `byte[]` |
| `result<T, E>` | return `T`, throw `WitException<E>` |
| imported interface | static methods on the interface: `IHostIoImports.GetConfig(key)` |

## 5. Build and validate

The one command is `build` rather than `publish`, because componentize-dotnet
hangs its `Publish` target off `Build`. It runs wit-bindgen over the canonical
WIT world, compiles to a core wasm module through NativeAOT LLVM, and wraps that
module into a component with the wasm-tools it ships. The build output is
already a component, so there is no `wasm-tools component new` step.

Linux/macOS:

```bash,name=Build the component
cd examples/polyglot/stages/csharp-tier && dotnet build -c Release --nologo
```

Windows (PowerShell):

```powershell
cd examples\polyglot\stages\csharp-tier; dotnet build -c Release --nologo
```

The finished component lands here:

```text,name=The build output
examples/polyglot/stages/csharp-tier/bin/Release/net10.0/wasi-wasm/publish/tier-cs.wasm
```

From the repository root, one task runs that same build and copies the result to
the path the config below names:

```bash,name=Build the stage through xtask
cargo xtask polyglot --only=csharp
```

Runs the same on Linux, macOS and Windows (PowerShell). It writes
`examples/polyglot/build/tier-cs.wasm`. Confirm the artifact with
[the two verify commands](@/service/processors/build/_index.md) on the build hub.

## 6. Run it under saci-service

`examples/configs/standalone_polyglot.kdl` runs a single processor under the
service. It reads `examples/configs/fixtures/polyglot_orders.csv` and writes
`/tmp/saci-polyglot-out.csv`. As committed it names the Python stage, so change
two things in its `wasm` node: the `module` path and the `config` keys.

```kdl,name=The wasm node for this stage
wasm "enrich" module="examples/polyglot/build/tier-cs.wasm" {
    config review_score="0.0"
}
```

Keep the node name `enrich`, because the workflow's two `link` lines address it.
`review_score` is the only key this stage reads: the risk score at or above which
an unflagged row still earns a look. It has a default of 0.2, so an absent key
runs rather than failing.

Then validate the config and serve it, from the repository root.

Linux/macOS:

```bash,name=Validate, then serve
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

The observable result is the `review_tier` column of
`/tmp/saci-polyglot-out.csv`, the eleventh of twelve. The fixture ships every row
with `flagged` false and `risk_score` 0.0, because the TypeScript stage that
scores them is not in this one-stage pipeline. With `review_score="0.0"` every
row therefore comes back at tier 1:

```text,name=review_tier in /tmp/saci-polyglot-out.csv, middle columns elided
id,...,flagged,fee,review_tier,settlement
1,...,false,0.0,1,PENDING
2,...,false,0.0,1,PENDING
```

Raise the key back to `"0.2"` and the same six rows come back at tier 0, which is
the fastest way to see the config value reach the component.

## Config, logs and state

`SaciConfig` reads the `config` node inside the `wasm` node. `GetString` returns
the trimmed value, or null for an absent or blank key. `GetDouble`, `GetInt64`
and `GetBool` take a fallback for that case instead, and each throws
`SaciProcessorException` for a value that is present and will not parse, because
a misconfiguration is worth failing the batch on. Lookups are memoised, so a
per-row read costs one call across the component boundary per batch.

`SaciHost` is the observability half:

```csharp,name=Reading config, logging and counting
double reviewScore = config.GetDouble("review_score", 0.2);
SaciHost.Log(SaciLogLevel.Info, "tier", $"review_score={reviewScore}");
SaciHost.Metric("tier.review_score", reviewScore);
SaciHost.Count("tier.hold_rows");
```

`Metric` observes once per call. `Count` accumulates into a batch-scoped counter
the SDK reports as one observation when the batch ends. That is what you want
per row, because a per-row `Metric` call would report a batch of six rows as six
observations of one. `SaciHost` is bound only while a batch is running, so a
call from outside one fails loudly instead of writing into a stale channel.

`Console.WriteLine` goes nowhere. The host discards a processor's stdout and
stderr, so `SaciHost.Log` is the only channel out of the component.

This stage is stateless. It ignores the prior state blob and returns none. That
blob is the only thing that survives to the next batch, arriving as the next
call's prior, so a stage that needs to remember anything across batches must put
it there. A stateless stage reports itself as such, and the host then has nothing
to persist.

## Next

- [The WIT contract](@/service/processors/build/wit-contract.md): every record
  the descriptor fills in, and what the host checks it against.
- [The build hub](@/service/processors/build/_index.md): the two verify commands,
  and the same stage in its six-language chain.
