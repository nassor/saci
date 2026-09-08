+++
title = "A Go processor"
description = "componentize-go, a row struct saci-sdk-go reads the schema off, and the export package the bindings generator expects you to write."
template = "page.html"
weight = 2
aliases = ["/processors/go/", "/guests/go/"]
+++
# A Go processor

`validate-go.wasm` is a WASI 0.2 component built from one Go struct and one
transform function. It runs under `saci-service` and fills the `valid` column of
every row it sees. Every block below is from
`examples/polyglot/stages/go-validate/`, which reads `amount` and writes `valid`.

`componentize-go` is the Bytecode Alliance's current Go recommendation. It
builds standard Go rather than TinyGo, whose component tooling page carries a
"not currently being maintained" banner pointing here.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 180" role="img" aria-labelledby="go-fig-title go-fig-desc">
        <title id="go-fig-title">One Go component between a CSV file source and a CSV file sink</title>
        <desc id="go-fig-desc">
            Three boxes left to right. The file source csv_orders reads
            examples/configs/fixtures/polyglot_orders.csv, six Order rows of twelve columns each.
            An arrow carries them into the wasm node running validate-go, which reads the amount
            column and writes the valid column. A second arrow carries the same twelve columns to
            the file sink csv_out, which writes /tmp/saci-polyglot-out.csv. The component adds no
            column and drops no row: it only fills valid in place.
        </desc>
        <text class="t-title" x="0" y="14">One stage under saci-service</text>
        <text class="t-sm" x="0" y="30">standalone_polyglot.kdl, with the Go component in the wasm node</text>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="44" width="180" height="76" rx="8"/>
            <rect class="hd hd-data" x="0" y="44" width="180" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="180" height="8"/>
            <text class="t-lbl" x="12" y="59">csv_orders</text>
            <text class="t-sm" x="12" y="80">polyglot_orders.csv</text>
            <text class="t-sm" x="12" y="93">6 Order rows</text>
            <text class="t-sm" x="12" y="106">12 columns each</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M180 82 H232" marker-end="url(#go-arw-d)"/>
            <rect class="blk blk-bnd" x="232" y="44" width="170" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="232" y="44" width="170" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="232" y="56" width="170" height="8"/>
            <text class="t-lbl t-bnd" x="244" y="59">wasm &middot; validate-go</text>
            <text class="t-sm" x="244" y="80">reads amount</text>
            <text class="t-sm t-data" x="244" y="93">writes valid</text>
            <text class="t-sm" x="244" y="106">min_amount config key</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M402 82 H452" marker-end="url(#go-arw-d)"/>
            <rect class="blk blk-data" x="452" y="44" width="208" height="76" rx="8"/>
            <rect class="hd hd-data" x="452" y="44" width="208" height="20" rx="8"/>
            <rect class="hd hd-data" x="452" y="56" width="208" height="8"/>
            <text class="t-lbl" x="464" y="59">csv_out</text>
            <text class="t-sm" x="464" y="80">/tmp/saci-polyglot-out.csv</text>
            <text class="t-sm t-data" x="464" y="93">valid, one value per row</text>
            <text class="t-sm" x="464" y="106">same 12 columns</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 144 H654"/>
            <text class="t-sm" x="0" y="162">The component fills a column in place. It cannot add one, so all twelve fields</text>
            <text class="t-sm" x="0" y="176">appear in both the source and the sink schema.</text>
        </g>
        <defs>
            <marker id="go-arw-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> Order rows, and the column this stage writes</span>
        <span class="k-boundary"><i></i> the WebAssembly component boundary</span>
    </div>
    <figcaption class="dgm-cap">
        One <code>wasm</code> node, one column. <code>valid</code> is the gate later stages key
        off: the Python stage converts only valid rows and the Kotlin stage charges a fee only on
        valid rows.
    </figcaption>
</div>

## What you need

Go 1.25.5 or newer; CI verifies 1.26.3. `componentize-go` 0.4.1 turns a Go
module into a component. The SDK that reads your row type is
`github.com/nassor/saci/packages/saci-sdk-go`, and it carries the Arrow IPC codec
too, so there is nothing else to resolve.

```bash,name=Install componentize-go
go install github.com/bytecodealliance/componentize-go@v0.4.1
```
Runs the same on Linux, macOS and Windows (PowerShell).

<div class="note note-warn">
<span class="note-label">componentize-go on Windows</span>

`go install` puts a thin wrapper on `PATH` that downloads the real binary on
first use. It asks for `componentize-go-windows-amd64.tar.gz`, while the v0.4.1
release only publishes a `.zip`, so the wrapper 404s. Download the `.zip` from
the release page and put `componentize-go.exe` on `PATH` yourself; overwriting
the wrapper in `%GOPATH%\bin` is fine. Linux and macOS are unaffected.

</div>

## 1. Create the project

`componentize-go bindings` generates the module for you, so the generator runs
first and you patch the result. Global flags come **before** the subcommand.

Linux/macOS:

```bash,name=Generate the bindings, then re-add the SDK
componentize-go -d ../../../../crates/saci-processor/wit -w saci-pipeline bindings --format
go mod edit \
    -require=github.com/nassor/saci/packages/saci-sdk-go@v0.0.0 \
    -replace=github.com/nassor/saci/packages/saci-sdk-go=../../../../packages/saci-sdk-go
```

Windows (PowerShell):

```powershell
componentize-go -d ..\..\..\..\crates\saci-processor\wit -w saci-pipeline bindings --format
go mod edit -require=github.com/nassor/saci/packages/saci-sdk-go@v0.0.0 -replace=github.com/nassor/saci/packages/saci-sdk-go=..\..\..\..\packages\saci-sdk-go
```

`go.mod` now reads like this. The `replace` directive points at this
repository's `packages/`, so the SDK resolves from source rather than from a
release:

```text,name=go.mod after both commands
module wit_component

go 1.25

require (
	github.com/nassor/saci/packages/saci-sdk-go v0.0.0
	go.bytecodealliance.org/pkg v0.2.2
)

replace github.com/nassor/saci/packages/saci-sdk-go => ../../../../packages/saci-sdk-go
```

<div class="note note-warn">
<span class="note-label">componentize-go owns go.mod</span>

`bindings` rewrites `go.mod` to `module wit_component` every time it runs,
so every intra-module import is `wit_component/<pkg>`. Commit `go.mod` with that
module name; `examples/polyglot/stages/go-validate/go.mod` does.

The rewrite is from a fixed template, one `require` and nothing else, so the SDK
dependency is dropped with everything else. That is why the `go mod edit` above
sits between `bindings` and the build: the build never touches the file.

</div>

## 2. Declare the row type

The struct is the schema. Field order is wire order, so a reordering is a wire
change:

```go,name=The row struct the schema comes from
// Order is the chain's row type: these twelve columns in this order are the
// cross-language contract every stage agrees on.
type Order struct {
    ID               int64   `saci:"id"`
    Region           string  `saci:"region"`
    Currency         string  `saci:"currency"`
    Amount           float64 `saci:"amount"`
    Valid            bool    `saci:"valid"`
    UsdAmount        float64 `saci:"usd_amount"`
    UsdAmountDisplay string  `saci:"usd_amount_display"`
    RiskScore        float64 `saci:"risk_score"`
    Flagged          bool    `saci:"flagged"`
    Fee              float64 `saci:"fee"`
    ReviewTier       int64   `saci:"review_tier"`
    Settlement       string  `saci:"settlement"`
}
```

The Go type name is the component name, so this is the host's `Order`. A `saci`
tag names the column; without one the column is the lower snake case of the
field name, which is what `UsdAmountDisplay` already spells. This stage tags
every field anyway, because the names are shared with five other languages and a
Go rename must not silently move a column.

Four field kinds map to the wire format's four types: `int64` to `Int64`,
`float64` to `Float64`, `bool` to `Boolean`, `string` to `Utf8`. Anything else
panics, and so does an embedded or unexported field. Declaring the processor as
a package-level var hits those authoring mistakes when the component is derived
rather than mid-batch.

Two stages agree when they declare the same field names in the same order, and
[the wire format](@/library/reference/wire-format.md) specifies the algorithm the
SDKs hash those names with.

## 3. Write the transform

A transform runs over one row and writes to it through a pointer, so a field
write is a column write. `saci.New` takes the transforms in the order they run,
and `Bind` attaches the host bindings:

```go,name=The validate transform and the bound processor
var stage = saci.New(stageName, stageVersion,
    saci.Transform("validate", func(row *Order, cfg saci.Config) error {
        minAmount, err := cfg.Float64(minAmountKey, minAmountDefault)
        if err != nil {
            return err
        }
        row.Valid = row.Amount > minAmount

        // Counted unconditionally, so a batch with nothing to reject still
        // reports a zero rather than dropping the series.
        invalid := 0.0
        if !row.Valid {
            invalid = 1
        }
        cfg.Count(invalidRowsMetric, invalid)
        return nil
    }),
).Bind(host{})
```

The four names the transform reads are constants in the same file:
`stageName` is `polyglot-validate-go`, `stageVersion` is `0.1.0`,
`minAmountKey` is `min_amount` and `minAmountDefault` is `0.0`.
`invalidRowsMetric` is `validate.invalid_rows`.

Declaring `stage` as a package-level var is what makes `describe` a field
read, because the schema derivation and the descriptor encoding happen once,
at instantiation.

## 4. Export it

`bindings` writes `wit_exports.go` plus one package per WIT interface, and
expects you to supply the export package. It imports
`export_saci_pipeline_pipeline` by that exact path and calls exactly `Describe`
and `RunBatch`. Add `--generate-stubs` to have componentize-go write the two
panicking signatures the first time.

The full file is
`examples/polyglot/stages/go-validate/export_saci_pipeline_pipeline/exports.go`.
Its imports name the SDK and the two generated packages:

```go,name=The export package imports
package export_saci_pipeline_pipeline

import (
    witTypes "go.bytecodealliance.org/pkg/wit/types"

    saci "github.com/nassor/saci/packages/saci-sdk-go"
    hostio "wit_component/saci_pipeline_host_io"
    "wit_component/saci_pipeline_types"
)
```

The SDK cannot import those bindings itself. `componentize-go bindings`
regenerates them into whichever stage module it runs in, always under the module
name `wit_component`, so the import path is not unique across stages. Bridge
them in three methods, which is also where the log target is named:

```go,name=The three host bindings
type host struct{}

func (host) GetConfig(key string) (string, bool) {
    value := hostio.GetConfig(key)
    if !value.IsSome() {
        return "", false
    }
    return value.Some(), true
}

func (host) Log(level saci.LogLevel, message string) {
    hostio.Log(hostio.LogLevel(level), logTarget, message)
}

func (host) Metric(name string, value float64) { hostio.Metric(name, value) }
```

`Describe` copies the SDK's descriptor into the generated records:

```go,name=Describe copies the SDK descriptor
func Describe() saci_pipeline_types.PipelineDescriptor {
    descriptor := stage.Describe()

    components := make([]saci_pipeline_types.ComponentDescriptor, len(descriptor.Components))
    for i, c := range descriptor.Components {
        components[i] = saci_pipeline_types.ComponentDescriptor{
            Name:           c.Name,
            ArrowSchemaIpc: c.ArrowSchemaIPC,
        }
    }
    return saci_pipeline_types.PipelineDescriptor{
        Name:              descriptor.Name,
        Version:           descriptor.Version,
        Components:        components,
        Stateful:          descriptor.Stateful,
        SchemaFingerprint: descriptor.SchemaFingerprint,
    }
}
```

`RunBatch` takes the WIT `option<checkpoint>` as `witTypes.Option[[]uint8]` and
returns `witTypes.Result[RunResult, RunError]`. Every failure the SDK can refuse
comes back as an error, and the stage folds it into one arm:

```go,name=RunBatch folds every failure into one arm
func RunBatch(input []uint8, prior witTypes.Option[[]uint8]) witTypes.Result[saci_pipeline_types.RunResult, saci_pipeline_types.RunError] {
    _ = prior

    outcome, err := stage.RunBatch(input)
    if err != nil {
        message := err.Error()
        hostio.Log(hostio.LogLevelError, logTarget, stageName+": "+message)
        return witTypes.Err[saci_pipeline_types.RunResult, saci_pipeline_types.RunError](
            saci_pipeline_types.MakeRunErrorPermanent(message),
        )
    }

    return witTypes.Ok[saci_pipeline_types.RunResult, saci_pipeline_types.RunError](saci_pipeline_types.RunResult{
        Output:     outcome.Output,
        Checkpoint: witTypes.None[[]uint8](),
        Metrics: saci_pipeline_types.RunMetrics{
            WallNs:     outcome.Metrics.WallNs,
            RowsIn:     outcome.Metrics.RowsIn,
            RowsOut:    outcome.Metrics.RowsOut,
            SystemsRun: outcome.Metrics.SystemsRun,
            Retries:    0,
        },
        Routes: witTypes.None[[]string](),
    })
}
```

`permanent` is the right arm here. The same bytes and the same config would
fail again, so a retry buys nothing. [The WIT contract](@/service/processors/build/wit-contract.md)
lists every record these two functions fill in.

## 5. Build and validate

The build runs from the stage directory, with the same global flags the bindings
step used.

Linux/macOS:

```bash,name=Build the component
componentize-go -d ../../../../crates/saci-processor/wit -w saci-pipeline build -o validate-go.wasm
```

Windows (PowerShell):

```powershell
componentize-go -d ..\..\..\..\crates\saci-processor\wit -w saci-pipeline build -o validate-go.wasm
```

`validate-go.wasm` now sits in the stage directory. `cargo xtask polyglot`
copies it to `examples/polyglot/build/validate-go.wasm`, which is the path a
config names. Finish with [the two verify commands](@/service/processors/build/_index.md)
on the build hub, which prove the artifact is a component and that it exports
`saci:pipeline`.

A transform is an ordinary function and a processor built without `Bind` drops
its logs and metrics, so both test on the host. The SDK's own suite and the
codec's live in one module, so one command covers both.

Linux/macOS:

```bash,name=Run the SDK test suite
cd packages/saci-sdk-go && go test ./...
```

Windows (PowerShell):

```powershell
cd packages\saci-sdk-go; go test ./...
```

<div class="note note-warn">
<span class="note-label"><code>go test ./...</code> does not work in the stage</span>

The generated packages use `//go:wasmimport`, which does not compile for the
host target, so a bare `go test ./...` fails on them inside the stage module.
Host-side tests belong in a module of their own.

</div>

## 6. Run it under saci-service

`examples/configs/standalone_polyglot.kdl` runs a single processor: a
`FileSource` reading `examples/configs/fixtures/polyglot_orders.csv`, one `wasm`
node, and a `FileSink` writing `/tmp/saci-polyglot-out.csv`. It ships naming the
Python component, and two edits point it at this stage.

Swap the `wasm` node's `module` to the Go artifact, and swap its `config` keys to
the one key this stage reads:

```kdl,name=The wasm node, with the Go component in it
wasm "enrich" module="examples/polyglot/build/validate-go.wasm" {
    // The only key this stage reads. Absent means no floor.
    config min_amount="500"
}
```

Leave the node name alone. The two `link` lines join `csv_orders` to it and it
to `csv_out`, so renaming it breaks both. Leave the twelve `schema_fields`
entries in both connectors alone too. They describe the `Order` component
rather than the language, and the component fills a column in place instead of
adding one.

Then build the artifacts and run the service from the repository root.

Linux/macOS:

```bash,name=Validate the config, then run it
cargo xtask polyglot
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- \
    validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- \
    serve --config examples/configs/standalone_polyglot.kdl
```

Windows (PowerShell):

```powershell
cargo xtask polyglot
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_polyglot.kdl
```

`run_mode kind="one_shot"` means `serve` processes the file once and exits.
`/tmp/saci-polyglot-out.csv` then holds the same six rows and the same twelve
columns, with `valid` recomputed against the `min_amount` floor of 500:

```text,name=The amount and valid columns, min_amount = 500
id  amount     valid
1   100.0      false
2   -5.0       false
3   1000000.0  true
4   60000.0    true
5   0.0        false
6   20000.0    true
```

The fixture pre-seeds `valid` as true for every row above zero, so a floor of 500
is what makes this stage's write visible: row 1 flips to `false`. `truncate #true`
on the sink replaces the file each run, so a second run does not append.

## Config, logs and state

`saci.Config` is the whole of the host a transform can reach, and it has two
methods. `Float64(key, fallback)` returns the fallback for an absent or blank
key and an error for a value that will not parse, because a misconfigured floor
defaulting to zero is worse than a refused batch. `Count(name, delta)` adds to a
named counter, and the processor reports one metric observation per counter after
the last transform, so a per-row call costs one addition rather than one host
call. Calling `Count` with a zero delta registers the counter, so a batch that
saw nothing still reports a zero.

A processor's `fmt.Println` goes nowhere. The host discards everything a
component writes to stdout and stderr, so the `Log` binding from step 4 is the
only channel out. `RunBatch` uses it to name the stage before it returns the
error arm. The log target is the string that identifies those lines to the
service, `validate` here.

Nothing in this stage panics. A Go panic inside a component traps the instance,
and the host then reports an opaque trap rather than a reason. Every failure
path folds into the `permanent` error arm instead.

The state blob a processor returns is the only thing that survives to the next
batch, where it comes back as the next call's `prior`. This stage keeps none,
so `Checkpoint` is `witTypes.None`, `prior` is ignored, and the descriptor
reports `stateful: false`. A field on a package-level var is not state, because
nothing carries it forward.

## Next

- [The build hub](@/service/processors/build/_index.md): the two verify commands,
  and the six-language chain this stage opens.
- [A Python processor](@/service/processors/build/python.md): the next stage, which
  reads the `valid` this one wrote.
