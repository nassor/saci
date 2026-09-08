+++
title = "The SDK packages"
description = "saci-sdk for Go, Python, TypeScript, Kotlin and C#: one package per language, the codec inside each, the shared codec API, and what the in-place path refuses."
template = "page.html"
weight = 3
aliases = ["/reference/arrow-ipc-packages/"]
+++
# The SDK packages

A WebAssembly processor has to decode the batch the host hands it. Five of the six
languages in [the polyglot example](@/service/processors/build/_index.md) have no Arrow
library that survives their componentizer, so each language's authoring SDK carries a
standard-library-only codec internally. One package per language resolves both the
authoring API and the wire format. The codec keeps
its original package/module/namespace name inside its SDK.

Each codec is a decoder, a set of in-place setters and a flatbuffer writer,
written against [the wire format](@/library/reference/wire-format.md) and nothing
else. No transitive dependencies in any of the five packages.

## Why five of the six carry their own codec

The non-Rust stages do not link an Arrow library. Each depends on its language's
SDK package, which implements just enough of the IPC format to read the columns a
stage needs and to overwrite fixed-width value bytes in place.

A variable-length output takes the other path. Rewriting a `Utf8` column, dropping
rows, or emitting a component the input did not carry means encoding a RecordBatch
flatbuffer, which each package's writer does. The bytes all five implement are
specified in [the wire format](@/library/reference/wire-format.md).

A standard-library-only codec is a deliberate constraint, not a recommendation: a
stage then depends on one package with no transitive dependencies of its own.
Reach for the real binding first when one survives your componentizer.

## Coordinates

All five SDKs release in lockstep. One wire format, one version. The Kotlin KSP
symbol processor and the C# source generator are build-time companions, not
runtime packages. The C# generator DLL is packed inside the `Saci.Sdk` NuGet
package; the Kotlin processor publishes as its own Maven artifact,
`io.github.nassor:saci-sdk-kt-ksp`, which a Kotlin build adds on its own `ksp`
configuration line.

The coordinate each
language installs, and the toolchain that builds against it, are in the language
table on [Build your own processor](@/service/processors/build/_index.md). That page
also carries the install command for each one.

The Python wheel, the npm tarball, the NuGet package and a tarball of the Maven
repository are assets of the `sdk-v0.1.0` GitHub release. Go resolves through
the module proxy from the `packages/saci-sdk-go/v0.1.0` tag, and Kotlin resolves
from the Maven repository this site serves.

The `sdk-v0.1.0` release has not been cut yet. No tag or release exists on
GitHub, so the Go, Python, TypeScript and C# installs fail until the release is
created. The Kotlin Maven repository is live: this site serves four modules at
0.1.0 today, `saci-sdk-kt` with its `saci-sdk-kt-jvm` and
`saci-sdk-kt-wasm-wasi` target variants, plus `saci-sdk-kt-ksp`.

## Codec API

One shape, five spellings. Parse takes ownership of a mutable copy of the input.
The setters write into it, and the output accessor hands the same bytes back as
`run-result.output`.

| Operation | Go | Python | TypeScript | Kotlin | C# |
|---|---|---|---|---|---|
| Parse | `arrowipc.Parse(b)` | `SaciStream(b)` | `new SaciStream(b)` | `SaciStream.parse(b)` | `new SaciStream(b)` |
| Component lookup | `s.Component(n)` | `s.component(n)` | `s.component(n)` | `s.component(n)` | `s.Component(n)` |
| Row count | `b.Rows` | `b.rows` | `b.rows` | `b.rows` | `b.Rows` |
| Int64 column | `b.Int64s(f)` | `b.int64s(f)` | `b.int64s(f)` | `b.int64s(f)` | `b.Int64s(f)` |
| Float64 column | `b.Float64s(f)` | `b.float64s(f)` | `b.float64s(f)` | `b.float64s(f)` | `b.Float64s(f)` |
| Boolean column | `b.Bools(f)` | `b.bools(f)` | `b.bools(f)` | `b.bools(f)` | `b.Bools(f)` |
| Utf8 column | `b.Strings(f)` | `b.strings(f)` | `b.strings(f)` | `b.strings(f)` | `b.Strings(f)` |
| Int64 setter | `b.SetInt64(f,r,v)` | `b.set_int64(f,r,v)` | `b.setInt64(f,r,v)` | `b.setInt64(f,r,v)` | `b.SetInt64(f,r,v)` |
| Float64 setter | `b.SetFloat64(f,r,v)` | `b.set_float64(f,r,v)` | `b.setFloat64(f,r,v)` | `b.setFloat64(f,r,v)` | `b.SetFloat64(f,r,v)` |
| Boolean setter | `b.SetBool(f,r,v)` | `b.set_bool(f,r,v)` | `b.setBool(f,r,v)` | `b.setBool(f,r,v)` | `b.SetBool(f,r,v)` |
| Output bytes | `s.Buf` | `s.to_bytes()` | `s.toBytes()` | `s.toWit()` | `s.Buffer` |
| Base64 decode | `arrowipc.DecodeBase64(t)` | `decode_base64(t)` | `decodeBase64(t)` | `decodeBase64(t)` | `ArrowIpc.DecodeBase64(t)` |

`decodeBase64` is there because a processor embeds its component's Arrow
schema as a generated base64 constant. It means the processor imports one
package, not two.

Int64 values are the language's widest integer: `bigint` in TypeScript, `Long` in
Kotlin, `long` in C#.

Malformed input is an error, never a trap. Go returns an `error`, Python raises
`ValueError`, TypeScript throws `ArrowIpcError`, Kotlin throws
`ArrowIpcException` and C# throws `ArrowIpcException`. A processor that traps
gives the host an opaque wasm failure instead of the `run-error::permanent`
message it can report.

## What the codecs refuse

The in-place path never writes a flatbuffer, so its setters refuse two things and
the decoder refuses two more:

- **No `Utf8` write in place.** Changing a string resizes the values buffer and
  invalidates the offsets buffer and the RecordBatch flatbuffer that describes
  both, so the setters take fixed-width fields only. The polyglot chain's two
  `Utf8` outputs, `usd_amount_display` and `settlement`, are written by
  re-encoding a whole segment instead.
- **No validity writes.** A non-nullable field carries an all-ones validity
  bitmap from arrow-rs, and an in-place value write never has to touch it, so no
  setter reaches it.
- **No dictionary batches.** A segment holds exactly one Schema message then one
  RecordBatch message. A DictionaryBatch in between is rejected during framing.
- **No compressed bodies.** `RecordBatch.compression` present is an error.

The writer beside it lifts the first two: `arrowipc.NewWriter` in Go,
`SaciStream.write_component` in Python, `SaciStreamWriter` in TypeScript and
Kotlin, `SaciStream.WriteComponent` in C#. It encodes whole segments, so a
processor can write a `Utf8` column, drop rows, or emit a component the input
never carried.

## Conformance corpus

`packages/arrow-ipc-conformance/` pins all five codecs to one answer about
which streams are valid. The `manifest.json` lists the cases; each `vectors/*.saci`
holds one binary stream. A case is `accept` or `reject`: an accept case carries
the components, row count and column values a codec must read back, a reject
case a `reason`. The reason is the contract, because error text is local to
each language; a codec maps each reason to whatever substring its own message
uses. Every SDK suite runs the corpus, so a sixth implementation has an
acceptance suite the day it starts.

The corpus is generated from a real `Dataset::write_ipc` stream, each malformed
vector derived by editing those bytes in place, so no vector is a hand-forged
flatbuffer that could drift from what arrow-rs emits. Regenerate it after any
wire format or `Order` schema change:

```bash,name=Regenerate the conformance corpus
cargo run -p saci-service --features conformance --example conformance_vectors -- emit
```

Two reference rules are host-side, not codec rules, so the corpus deliberately
has no vector for either. They are the `__alive` cross-check on a component's row
count, and 8-byte buffer alignment, which is a property the writer guarantees
rather than a rule a reader enforces.

## Compatibility

The five codecs target the byte layout of `arrow-ipc = "=59.3.0"`, which the SACI
workspace exact-pins as the host to processor wire format, and the
`saci:pipeline@0.3.0` WIT world that carries it. A processor built against these
packages talks to a host built from the same pin.

Version 0.1.0 of all five decodes what
`cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit`
writes to `examples/polyglot/generated/fixture_input.saci`. Each package's test
suite asserts exactly that, column by column, against the JSON the same command
emits.

## License

The `packages/` subtree is Apache-2.0. The engine crates are AGPL-3.0-only.

## Where to go next

- [The wire format](@/library/reference/wire-format.md): what a sixth language
  implements.
- [Build your own processor](@/service/processors/build/_index.md): the chain that
  consumes all five packages, and the toolchain per language.
