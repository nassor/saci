+++
title = "A Go plugin"
description = "A cgo shared library exporting the two plugin symbols."
template = "page.html"
weight = 2
+++
# A Go plugin

Go builds a C shared library, so a Go plugin needs no WebAssembly toolchain.
`examples/plugins/settle-go/` builds into one such library and runs under `saci-service`, writing a
settlement tier into every row of an `Order` batch.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 168" role="img" aria-labelledby="pgo-title pgo-desc">
        <title id="pgo-title">A Go plugin between a CSV source and a CSV sink</title>
        <desc id="pgo-desc">
            The source csv_orders reads polyglot_orders.csv and hands each batch to the plugin
            node settle, which is the shared library built from Go with cgo. The node writes
            the review_tier column and its output goes on to the sink csv_out, which writes
            saci-polyglot-out.csv. The plugin runs in the service's own process: the Go runtime
            starts when the library loads, and no sandbox stands between the two.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="46" width="150" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="58" width="150" height="8"/>
            <text class="t-lbl" x="12" y="61">csv_orders</text>
            <text class="t-sm" x="12" y="84">polyglot_orders.csv</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M150 74 H210" marker-end="url(#pgo-d)"/>
            <rect class="blk blk-bnd" x="210" y="36" width="200" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="36" width="200" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="48" width="200" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="51">settle</text>
            <text class="t-sm" x="222" y="74">c-shared library, cgo</text>
            <text class="t-sm t-data" x="222" y="94">writes review_tier</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M410 74 H470" marker-end="url(#pgo-d)"/>
            <rect class="blk blk-data" x="470" y="46" width="190" height="56" rx="8"/>
            <rect class="hd hd-data" x="470" y="46" width="190" height="20" rx="8"/>
            <rect class="hd hd-data" x="470" y="58" width="190" height="8"/>
            <text class="t-lbl" x="482" y="61">csv_out</text>
            <text class="t-sm" x="482" y="84">saci-polyglot-out.csv</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 134 H654"/>
            <text class="t-sm" x="0" y="152">One Go runtime starts when the library loads, in the service's own process, with no sandbox between them.</text>
        </g>
        <defs>
            <marker id="pgo-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the batch, and the column the plugin writes</span>
        <span class="k-boundary"><i></i> the plugin, loaded into the service's process</span>
    </div>
</div>

## What you need

- **Go 1.25 or newer.** CI verifies 1.26.3.
- **A C compiler cgo accepts.** cgo shells out to whatever `go env CC` names, so the build
  cannot happen without one. Linux: `apt install build-essential`. macOS:
  `xcode-select --install`. Windows: mingw-w64 gcc or clang, because cgo does not support MSVC,
  and a machine with only Visual Studio has no usable compiler here.
- **cargo**, because the build regenerates the `Order` schema constants the plugin compiles in.
- **`saci-sdk-go`**, whose `arrowipc` subpackage reads and mutates batches with nothing but the
  Go standard library. [The SDK packages](@/library/reference/sdk-packages.md) lists its API.

Check the compiler before anything else:

```bash,name=Confirm cgo has a compiler
go env CC
```

Runs the same on Linux, macOS and Windows (PowerShell). An empty answer, or a name that is not
installed, is the failure the last section of this page describes.

## 1. Create the module

A plugin is `package main` built as a shared library, so the module needs nothing but the SDK.
A filesystem `replace` resolves it from this repository, and the SDK has no dependencies of its
own, so there is no `go.sum`.

```go,name=go.mod for a Go plugin
module github.com/nassor/saci/examples/plugins/settle-go

go 1.25

require github.com/nassor/saci/packages/saci-sdk-go v0.0.0

replace github.com/nassor/saci/packages/saci-sdk-go => ../../../packages/saci-sdk-go
```

Nothing rewrites this file, because a cgo build never touches `go.mod`.

## 2. Export the two symbols

`saci-service` looks up exactly two names in the library. `saci_abi_version` reports the ABI the
library was built against, and `saci_plugin_v1` fills a vtable the host allocated with four
function pointers.

The cgo preamble includes `saci_plugin.h`, which ships in this repository and declares every
struct and both entry points. Point `CFLAGS` at the directory holding it;
`examples/plugins/settle-go/main.go` carries the relative path from its own directory.

```go,name=The two exported symbols
//export saci_abi_version
func saci_abi_version() C.uint32_t {
    return C.uint32_t(C.SACI_ABI_VERSION)
}

//export saci_plugin_v1
func saci_plugin_v1(host *C.SaciHostV1, out *C.SaciPluginV1) (status C.SaciStatus) {
    defer func() {
        if r := recover(); r != nil {
            status = statusPermanent
        }
    }()

    if out == nil {
        return statusPermanent
    }

    // One byte of C memory, used only as an identity the host hands back.
    cookie := C.malloc(1)

    instancesMu.Lock()
    instances[uintptr(cookie)] = &instance{host: host}
    instancesMu.Unlock()

    C.saci_shim_fill(out, cookie)
    return statusOK
}
```

Three rules the code above follows, all of them forced by cgo:

- **Every exported body recovers.** A Go panic crossing a C frame aborts the process, and the
  host then has nothing to report.
- **The vtable is filled from C.** cgo cannot take the address of an exported function, so a
  `static inline` shim in the preamble does it. Calls the other way, into the host's `log`,
  `metric` and `get_config` slots, go through shims for the same reason.
- **Per instance state hangs off the cookie.** A `c-shared` library has one Go runtime per
  process, so a package variable would make two loads of the library share one host.

The four vtable functions are `describe`, `run_batch`, `free_buffer` and `destroy`. `describe`
writes the manifest as JSON: the plugin's name, its version, whether it is stateful, the schema
fingerprint, and one entry per component with its schema as base64.

```json,name=The manifest describe writes
{"name":"settle-go","version":"0.1.0","stateful":false,
 "schema_fingerprint":"8c0a76ff","components":[{"name":"Order",
 "arrow_schema_ipc_base64":"/////3gCAAAQ..."}]}
```

The host recomputes that fingerprint from the schema it decoded and refuses the load on a
mismatch, so the two cannot drift apart unnoticed.

## 3. Read and write rows

The codec reads any column and writes fixed-width ones in place: `SetInt64`, `SetFloat64` and
`SetBool`. Writing an `Int64` is one eight byte store, and every other byte of the stream passes
through untouched.

This is why `settle-go` writes `review_tier`, an `Int64`, and not `settlement`. Overwriting a
variable-width string moves every following offset and forces a rewrite of the batch metadata,
which needs a real Arrow writer. `review_tier` carries the same decision as a number: 0 clears,
1 holds for manual review, 2 marks an order with nothing to settle.

```go,name=Reading two columns and writing one
stream, err := arrowipc.Parse(input)
batch, err := stream.Component("Order")
amounts, err := batch.Float64s("amount")
currencies, err := batch.Strings("currency")

for row := range batch.Rows {
    rate := rateFor(currencies[row])       // one host config lookup per currency
    converted := amounts[row] * rate
    tier := int64(0)
    switch {
    case !(converted > 0):
        tier = 2
    case converted >= escalateAbove:
        tier = 1
    }
    if err := batch.SetInt64("review_tier", row, tier); err != nil {
        return err
    }
}
```

The comparison is `converted > 0` rather than `converted <= 0` so that a NaN amount lands on the
unsettleable arm instead of clearing.

Two config keys steer it, and the plugin reads both through the host. `settle.escalate_above`
is the tier 1 threshold, default 10000. `settle.rate_<CURRENCY>` is the multiplier for one
currency, default 1.0, looked up once per distinct currency in the batch. An unparseable value
fails the batch with a message naming the key, rather than silently defaulting.

## 4. Build it

The xtask does the whole build. It regenerates the schema constants, copies them in under
`package main`, and runs the Go build.

```bash,name=Build the Go plugin
cargo xtask plugins --only=go
```

Runs the same on Linux, macOS and Windows (PowerShell). It produces `libsettle_go.so`,
`libsettle_go.dylib` or `settle_go.dll` in `examples/plugins/settle-go/`, and prints the path it
wrote:

```text,name=What the build prints
PASS
go:   examples/plugins/settle-go/libsettle_go.so
```

The Go command underneath is one line, if you would rather run it yourself from the plugin
directory:

Linux/macOS:

```bash,name=The go build the xtask runs
cd examples/plugins/settle-go
CGO_ENABLED=1 go build -buildmode=c-shared -o libsettle_go.so .
```

Windows (PowerShell):

```powershell
cd examples\plugins\settle-go
$env:CGO_ENABLED = "1"
go build -buildmode=c-shared -o settle_go.dll .
```

## 5. Run it under saci-service

The plugin works on the same `Order` rows the polyglot example uses, so
`examples/configs/standalone_polyglot.kdl` runs it with two changes. Its `wasm` node becomes a
`plugin` node naming the library, and the two `link` lines name the new node.

```kdl,name=The polyglot config, with a plugin node instead
plugin "settle" library="examples/plugins/settle-go/libsettle_go.so" {
    config "settle.rate_JPY"="0.0068"
}

link from="csv_orders" to="settle"
link from="settle" to="csv_out"
```

`plugin` is not in the default build, so the feature list gains it:

Linux/macOS:

```bash,name=Validate then serve
cargo run -p saci-service --features connector-file,transformer-csv,plugin -- validate \
  --config examples/configs/standalone_polyglot.kdl --strict

cargo run -p saci-service --features connector-file,transformer-csv,plugin -- serve \
  --config examples/configs/standalone_polyglot.kdl
```

Windows (PowerShell):

```powershell
cargo run -p saci-service --features connector-file,transformer-csv,plugin -- validate --config examples/configs/standalone_polyglot.kdl --strict

cargo run -p saci-service --features connector-file,transformer-csv,plugin -- serve --config examples/configs/standalone_polyglot.kdl
```

The config runs once and writes `/tmp/saci-polyglot-out.csv`. The plugin's own log line reports
what it did, and the `review_tier` column holds its decisions:

```text,name=The plugin's log line
settle-go: 2 of 6 rows cleared to settle
```

Over the six fixture rows, with `settle.rate_JPY` set as above, `review_tier` reads
`0, 2, 0, 1, 2, 1`. The two non-positive amounts are unsettleable. The JPY row converts to 6800
and clears, and the two large USD rows hold for review. Without the rate key the JPY row holds
too, and the line reads `settle-go: 1 of 6 rows cleared to settle` with `review_tier` of
`0, 2, 1, 1, 2, 1`.

## What breaks

**No C compiler.** cgo fails with `cgo: C compiler "gcc" not found`, filed under a
`# runtime/cgo` heading, which reads like a Go toolchain fault rather than a missing dependency.
`cargo xtask plugins` checks `go env CC` first and stops with its own message instead.

**cgo disabled.** With `CGO_ENABLED=0` the implicit build constraint drops `main.go` from the
package, and the build fails with `function main is undeclared in the main package`. The file is
there; the constraint excluded it.

**The Go runtime is not free.** A `c-shared` library starts one Go runtime when it loads,
installs its own signal handlers, and its `GOMAXPROCS` competes with the service's own thread
pools.

## Next

- [Plugins in a workflow](@/service/plugins/_index.md): every key of the node that loads this
  library, and what the service prints when it refuses one.
- [Plugins in other languages](@/service/plugins/other-languages.md): the same two symbols from
  C#, Kotlin or anything else that emits a shared library.
