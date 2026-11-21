# Toolchain pins, settle-go

One plugin, one language, no WebAssembly. `settle-go` is a C shared library the
host loads with dlopen, built from Go with cgo, exporting the ABI in
`crates/saci/include/saci_plugin.h`.

## Required tools

| Tool | Version | Install |
| ---- | ------- | ------- |
| Go | 1.25 floor, verified on 1.26.3 | <https://go.dev/dl/> |
| A C compiler for cgo | any gcc or clang cgo supports | Linux `apt install build-essential`, macOS `xcode-select --install`, Windows mingw-w64 |
| cargo | workspace Rust toolchain | <https://rustup.rs> |

Build with `cargo xtask plugins`, which fails with a distinct exit code per
missing tool: 2 cargo, 3 Go, 4 a C compiler, 5 no artifact. Build this plugin
alone with `--only=go`, which still needs cargo because it regenerates the
schema constants.

cargo is on that list because the plugin compiles in the `Order` schema as
generated constants produced by `examples/plugins/schema_emit.rs`.

## The C compiler is not optional

cgo shells out to the compiler `go env CC` names, so the plugin cannot build
without one. On Windows that compiler must be mingw-w64 gcc or clang; cgo does
not support MSVC, so a machine with only Visual Studio has no usable C
compiler. `cargo xtask plugins` checks `go env CC` before it does any work,
because the raw failure is `cgo: C compiler "gcc" not found` filed under a
`# runtime/cgo` heading, which reads like a Go toolchain fault rather than a
missing dependency.

With cgo disabled the implicit build constraint drops `main.go` from the package
and the build fails with `function main is undeclared in the main package`.

## The codec

The plugin owns `internal/arrowipc`, a pure Go standard-library package that
reads and mutates the SACI wire format with no external dependencies. There is
no `go.sum`.

## Load-bearing crate pin

`arrow-ipc = "=60.2.0"` in the workspace `Cargo.toml` is the host's Arrow IPC
implementation and the byte layout this plugin walks. A patch bump there can
move the buffers the codec resolves by offset. `crates/saci/PINS.md` holds
the upgrade policy. The plugin ABI version is independent of it:
`SACI_ABI_VERSION` is `0x00010000` and covers the struct layout, not the wire
format inside the buffers.

## What this plugin writes, and why

It reads `Order.amount` and `Order.currency` and writes `Order.review_tier`.

`review_tier` is an `Int64`, so writing it is a read of the RecordBatch
flatbuffer plus an eight byte store into the value buffer, and every other byte
of the stream passes through untouched. The codec offers exactly that and
nothing more: `SetInt64`, `SetFloat64` and `SetBool` on fixed width fields.

`settlement` is the disposition this plugin's name suggests, and it is the
schema's only `Utf8` column, so it is the one column the codec refuses to
write. Overwriting a variable width value moves every following offset and
forces a rewrite of the RecordBatch metadata, which needs a real Arrow writer.
`docs/content/library/reference/wire-format.md` covers the framing.
`review_tier` carries the same decision as a fixed width value: 0 clears, 1
holds for manual review, 2 marks an order with nothing to settle.

The plugin declares `stateful: false` and always sets `has_checkpoint` to 0. The
checkpoint path is exercised by `crates/saci-plugin-smoketest`, which keeps a
running total across batches. A Go ledger would add a JSON blob and prove
nothing further about the boundary, because the host treats a checkpoint as
opaque bytes.

Both config keys are read through `SaciHostV1.get_config`, and all three host
callbacks are wired. `settle.escalate_above` is the tier 1 threshold, default
10000. `settle.rate_<CURRENCY>` is the multiplier for one currency, default 1.0,
looked up once per distinct currency in the batch, so a value the host injected
selects the rate from each row's own data.

## cgo hazards

These are the ones that shape the code. Each has a comment at its site in
`main.go`.

### The header's `const` conflicts with cgo's own prototype

cgo copies the preamble into `_cgo_export.h` and then appends its own prototype
for every `//export` function. It has no way to spell `const`, so its
`saci_plugin_v1` takes `SaciHostV1 *` while `saci_plugin.h` declares
`const SaciHostV1 *`. C treats those as conflicting types, and the result is a
hard compile error in a generated file. The preamble renames the header's
prototype out of the way:

```c
#define saci_plugin_v1 saci_plugin_v1_prototype
#include "saci_plugin.h"
#undef saci_plugin_v1
```

`saci_abi_version` needs no such treatment: a `()` declaration and a `(void)`
prototype are compatible in every C mode.

### A file using `//export` may not define anything in its preamble

cgo copies the preamble into more than one C output file, so a function
definition there is a duplicate symbol. The four shims are `static inline`:
internal linkage means each copy is its own, and `inline` means the copy that
goes unused draws no `-Wunused-function` warning.

### cgo cannot reach a C function pointer from Go, in either direction

`C.saci_go_describe` in Go is a callable, not an address, so the vtable is filled
by `saci_shim_fill` in C. Calling the other way has the same limitation, so
`log`, `metric` and `get_config` each go through a shim that null checks the
slot and jumps.

### `C.malloc` never returns nil

cgo routes it through a helper that crashes the process when the C library
reports out of memory, so checking the result is dead code. An out of memory
plugin takes the host with it, which is the trust boundary
`saci_plugin.h` already describes.

### Whether a pointer is a Go pointer depends on how it was allocated

Not on its type. Every buffer the host keeps, and every log string and config
key the host reads, is copied into C memory first, because the collector is free
to move or reclaim Go memory the host still holds. The input is the one place
that borrows: `arrowipc.Parse` copies before the call returns, so the view over
the host's slice never outlives it.

### One Go runtime per process

A `c-shared` library starts one Go runtime at load, whatever the host does with
it. The host vtable therefore hangs off the cookie the host holds as
`SaciPluginV1.instance` rather than a package variable, so two loads of this
library do not share one host. That runtime also installs its own signal
handlers, and its `GOMAXPROCS` competes with the host's tokio and rayon pools.

The fingerprint must equal `examples/plugins/generated/order_fingerprint.txt`,
and the host recomputes it from the decoded schema and refuses a mismatch.

Over the six row `examples/plugins/generated/fixture_input.saci`, whose amounts
are 100 EUR, -5 GBP, 1000000 JPY, 60000 USD, 0 EUR and 20000 USD, one batch with
no config yields `review_tier` of `[0, 2, 1, 1, 2, 1]` and reports 5 blocked
rows. Adding `settle.rate_JPY=0.0068` yields `[0, 2, 0, 1, 2, 1]`: the JPY row
converts to 6800 and clears the 10000 threshold. Setting
`settle.escalate_above=50` yields `[1, 2, 1, 1, 2, 1]`. The output is the same
length as the input, and exactly one byte differs per changed row, which is what
an in place fixed width write looks like.
