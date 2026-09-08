+++
title = "Host to processor wire format"
description = "The exact bytes crossing the Component Model boundary: segment framing, Arrow IPC message framing, the flatbuffer field ids a processor must read, buffer layout per Arrow type, and the schema fingerprint algorithm."
template = "page.html"
weight = 2
aliases = ["/reference/wire-format/", "/polyglot/wire-format/"]
+++
# Host to processor wire format

Read this only if you are writing an Arrow codec by hand. Five languages already
have one, carried inside [`the SDK packages`](@/library/reference/sdk-packages.md).
The implementation on both sides is `crates/saci-core/src/dataset/ipc.rs`, called
identically by the host and by the `export_pipeline!` expansion in the processor.

`run-batch` takes `list<u8>` and answers with a `run-result` whose `output`
field is `list<u8>` again. This page specifies those bytes precisely enough to
implement a processor in a language with no Arrow library. The Go, Python,
TypeScript, Kotlin and C# stages of
[the polyglot example](@/service/processors/build/_index.md) do exactly that.

The numbers here match the stream that
`cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit`
writes to `examples/polyglot/generated/fixture_input.saci`.

---

## Segment framing

The payload is **not** a single Arrow IPC stream. It is a sequence of
length-prefixed streams with a zero terminator:

```text,name=The segment framing grammar
saci_stream := segment* terminator
segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
terminator := u32le 0x00000000
```

One segment per registered component, ordered by component name, then a final
`__alive` segment carrying the liveness bitmap as a single `Boolean` column named
`alive`, then the terminator.

Each `arrow_ipc_stream` is a complete standalone Arrow IPC **stream** as produced
by arrow-rs `StreamWriter` with `IpcWriteOptions::default()`: MetadataVersion V5,
64-byte buffer alignment, no compression. Sixty-four is a multiple of eight, so a
reader that rounds up to 8 still lands on the next message. It contains exactly
one Schema message, one RecordBatch message, and an end-of-stream marker.

Each segment's Schema carries custom metadata:

| key | value |
|-----|-------|
| `__saci_component` | the component name, or `__alive` for the bitmap segment |
| `__saci_schema_version` | decimal `u32`; absent on the `__alive` segment |

A segment with no `__saci_component` key is fatal on the host side. So is a stream
where any component's row count exceeds the `__alive` length (a component may
hold *fewer* rows, a windowing processor's reduced result component), one with
more than one `__alive` segment, and one with no `__alive` segment at all.

## Arrow IPC message framing

Within a segment, each message is:

```text,name=The Arrow IPC message framing
message := 0xFFFFFFFF (u32le continuation)
        ++ u32le metadata_len
        ++ flatbuffer[metadata_len]
        ++ body[bodyLength]
```

- `metadata_len` **already includes** the flatbuffer's padding to an 8-byte
  boundary.
- the body starts at `msg_start + 8 + metadata_len`.
- the next message starts at `body_start + align8(bodyLength)`.
- end-of-stream is `0xFFFFFFFF` followed by `u32le 0`.

`Buffer.offset` values inside a RecordBatch are relative to that message's body
start.

## Reading flatbuffers by hand

Enough of the format to read these two message types:

- The buffer starts with a `uoffset32` pointing at the root table.
- A table at absolute position `t` starts with a **signed** `soffset32`; its
  vtable is at `t - soffset`.
- A vtable is `u16 vtable_len`, `u16 table_len`, then one `u16` per field id. The
  field is **absent** if its offset is `0`, or if its field id is at or beyond
  `vtable_len`.
- A present field's value lives at `t + field_offset`.
- Strings, vectors and sub-tables are referenced by a `uoffset32` stored in that
  slot; the target is at `slot_position + uoffset_value`.
- A string is `u32 byte_len` followed by the bytes.
- A vector is `u32 count` followed by `count` elements: 4 bytes each for offsets
  to tables/strings, `sizeof(struct)` for inline structs.

## Field ids

A union in a flatbuffer schema occupies two vtable slots, a discriminant and a
value, which is where the gaps in this table come from.

| Table | field | id | type / notes |
|-------|-------|----|--------------|
| `Message` | `version` | 0 | i16; V5 is the value `4` |
| `Message` | `header_type` | 1 | u8: 1 = Schema, 2 = DictionaryBatch, 3 = RecordBatch |
| `Message` | `header` | 2 | uoffset to the header table |
| `Message` | `bodyLength` | 3 | i64 |
| `Schema` | `endianness` | 0 | i16, 0 = little |
| `Schema` | `fields` | 1 | vector of `Field` tables |
| `Schema` | `custom_metadata` | 2 | vector of `KeyValue` tables |
| `Field` | `name` | 0 | string |
| `Field` | `nullable` | 1 | bool |
| `Field` | `type_type` | 2 | u8 union discriminant |
| `RecordBatch` | `length` | 0 | i64 row count |
| `RecordBatch` | `nodes` | 1 | vector of inline `FieldNode { i64 length, i64 null_count }`, 16 B each |
| `RecordBatch` | `buffers` | 2 | vector of inline `Buffer { i64 offset, i64 length }`, 16 B each |
| `RecordBatch` | `compression` | 3 | uoffset; **must be absent**; reject the batch if present |
| `KeyValue` | `key` / `value` | 0 / 1 | string / string |

`type_type` values a SACI processor needs: **2 = Int, 3 = FloatingPoint, 5 = Utf8,
6 = Bool**. Cross-check against Arrow's [`Message.fbs`][msg] and
[`Schema.fbs`][sch] if you extend the set.

[msg]: https://github.com/apache/arrow/blob/main/format/Message.fbs
[sch]: https://github.com/apache/arrow/blob/main/format/Schema.fbs

## Buffer slots per Arrow type

Walk the schema's fields in order, accumulating a buffer index. The node index
equals the field index; the buffer index does not, because the slot count varies
by type.

| Arrow type | slots | meaning |
|------------|-------|---------|
| `Int` | 2 | validity, values |
| `FloatingPoint` | 2 | validity, values |
| `Bool` | 2 | validity, values |
| `Utf8` | 3 | validity, i32 offsets, values |

**arrow-rs emits the validity slot even when the field is non-nullable and the
null count is zero**, with a real, non-zero length (`ceil(n/8)` bytes, all bits
set). The slot count is therefore fixed by `type_type`, never inferred from
lengths. The twelve-field `Order` schema,
`Int, Utf8, Utf8, Float, Bool, Float, Utf8, Float, Bool, Float, Int, Utf8`,
consumes exactly 28 buffer slots; after the walk, the accumulated index must
equal the `buffers` vector length, or the batch is malformed.

## Value layouts

| type | layout |
|------|--------|
| `Int64` | 8 bytes LE per row |
| `Float64` | IEEE-754, 8 bytes LE per row |
| `Boolean` | bit-packed LSB-first: row `i` is bit `i & 7` of byte `i >> 3`; `ceil(n/8)` bytes |
| `Utf8` | `n+1` i32 LE offsets into the values buffer; row `i` is `values[offsets[i]..offsets[i+1]]`, UTF-8 |

## What a byte-mutating processor may and may not do

A processor with no Arrow writer can still be a first-class stage, as long as it
never changes any length. The pattern the five non-Rust stages use:

1. split the input into segments,
2. find the segment whose Schema metadata `__saci_component` matches the component
   it cares about,
3. parse that segment's Schema message (field names + `type_type`) and its
   RecordBatch message (`length`, `buffers`),
4. read the columns it needs, then overwrite fixed-width value bytes **in place**
   in the body,
5. return the input byte array, mutated. Every other byte passes through
   untouched: the `__alive` segment, both flatbuffers, the framing.

Writing a variable-length column this way is not possible: a different
string length changes the offsets buffer, the values buffer length, and the
`Buffer` entries in the RecordBatch flatbuffer. A processor that must write
`Utf8` needs a real RecordBatch-message *writer*; the field-id table above
is sufficient to build one.

## Schema fingerprint

`pipeline-descriptor.schema-fingerprint` is a `string`:
`format!("{:08x}", fnv1a32)`, lowercase and zero-padded to 8 characters. FNV-1a
32-bit, offset basis `2166136261`, prime `16777619`, all arithmetic mod 2^32:

```text,name=The fingerprint hash step by step
hash := 2166136261
for each component, sorted by name:
    for each byte of the component name:      hash = (hash XOR byte) * 16777619
    for each of the 4 little-endian bytes of the schema version:
                                              hash = (hash XOR byte) * 16777619
    for each field, in schema order:
        for each byte of the field name:      hash = (hash XOR byte) * 16777619
return lowercase_hex_8(hash)
```

Names, versions and field names only: no types, no nullability. Adding a field
changes it; changing a field's type does not.

Every language's SDK implements this once, deriving the fingerprint at build
or init time from the row type the stage declares, rather than sharing a
generated constant. The driver and the `polyglot_chain` integration test load
all six stages, each declaring the one `Order` component, and compare the
fingerprints they report against each other, failing on any disagreement.

## `component-descriptor.arrow-schema-ipc`

A *schema-only* Arrow IPC stream: a `StreamWriter` opened on the schema and
immediately finished, with no batches. The host parses it with
`StreamReader::schema()` and uses it to build the template dataset that sources
and sinks are validated against. Same per-language derivation as the
fingerprint: each SDK builds it from the row type at build or init time.

## The conformance corpus

A hand implementation has an acceptance suite the day it starts:
`packages/arrow-ipc-conformance/` lists, in `manifest.json`, one binary stream
per `vectors/*.saci` case and what each must do. The five SDK codecs all run it,
so one answer covers which streams are valid. Regenerate it after any wire
format or `Order` schema change:

```bash,name=Regenerate the conformance corpus
cargo run -p saci-service --features conformance --example conformance_vectors -- emit
```

The corpus is processor-side: it deliberately has no vector for the two rules
the host enforces, the `__alive` row-count cross-check and 8-byte buffer
alignment. It is covered with the codecs on
[the SDK packages page](@/library/reference/sdk-packages.md).

## TCP frame

One frame is a `u32` big-endian length prefix followed by exactly that many payload bytes. The
`tcp` sink writes the framing the `tcp` source reads, so the two halves are wire compatible: a
sink in one service feeds a source in another when both name the same format.

Decoding and encoding those bytes belongs to the transformer. On the source the decoder is opened
once per connection, so a frame carrying several batches yields their concatenation. On the sink
one batch becomes one frame per encoded message, which for `arrow-ipc` is one frame per batch.

A clean close between frames is a normal disconnect, and the sink's `finish` closes its write half
that way. On the source a failed frame-header read, an oversized frame, a truncated payload, a
frame that decodes to no batch, and a frame that cannot be projected onto the declared schema each
close that one connection and leave the listener and every other producer running. Each one warns
only when `saci-connector-tcp` carries its `tracing` feature, which `saci-service`'s
`connector-tcp` turns on and a direct dependency does not:
`TcpIngestSource: frame header read failed`,
`TcpIngestSource: oversized frame, closing connection`,
`TcpIngestSource: truncated frame payload, closing connection`, `TcpIngestSource: frame decoded no
batch` and `TcpIngestSource: bad frame, closing connection`. A declared schema projects rather than
checks, so a payload carrying extra columns decodes, and only a payload missing a declared column
or carrying a value that does not fit its declared type arrives inside the last of those, as
`arrow-ipc: casting to the declared schema: ...`.

## SACI session

A `saci` frame is a `u32` big-endian body length followed by that many body bytes, whose first
byte is the frame kind. A `str` below is a `u16` big-endian byte length followed by UTF-8 bytes.
`saci_connector_saci::wire` is public, so a foreign producer speaks this without linking the
host.

| Kind | Body |
|---|---|
| `1` hello | `u8` version (`1`), `str` service, `str` workflow, `str` sink, `u32` schema length, an Arrow IPC Schema message |
| `2` accept | empty |
| `3` reject | `str` reason |
| `4` data | `str` traceparent (length `0` for none), then Arrow IPC stream bytes for one batch |

A session opens with the sink's hello and the source's accept or reject. Every following frame is
a data frame. The sink holds one Arrow IPC stream encoder per session, so the schema message rides
on the first data frame and never repeats; the source holds one stream decoder per session to
match, which is what carries dictionary state across frames.

A refusal reaches the sink as a configuration error, because an unsupported version, a first frame
that is not a hello, and a schema other than the source's are all disagreements between the two
config files rather than transport faults. Everything else, an unreachable peer, an I/O error
mid-handshake, silence past `handshake_timeout_ms`, or a reply that is neither accept nor reject,
is a failed dial and the sink tries the next `connect` address.

After the handshake, a failed frame-header read, a zero-length or oversized frame, a frame that is
not a data frame, and Arrow IPC bytes that will not decode each close that one session and leave
the listener and every other peer running. Each warns only when `saci-connector-saci` carries its
`tracing` feature, which `saci-service`'s `connector-saci` turns on and a direct dependency does
not.

A write that fails mid-frame closes the session the same way, and the runner's retry of that
batch opens a fresh session, possibly to the next peer. There is no acknowledgement frame: a
batch the socket already accepted is not confirmed by the peer, so one lost with the session is
not resent.

The traceparent is a W3C `00-<trace_id>-<span_id>-<flags>`, taken from the sink's `peer.send` span
and adopted as the remote parent of the source's `peer.receive` span. Both spans are
`debug_span!`s, so the sending and receiving services also need `observability.log_level="debug"`
(or an enabling `RUST_LOG`); the default `log_level="error"` opens neither. The traceparent itself
is present only when the sending service has an OpenTelemetry layer installed, which
`observability.otlp_endpoint` is what does; a malformed or absent header leaves the receiving span
a root and costs the batch nothing.

## Avro message prefixes

Single-object encoding, `0xC3 0x01` followed by an 8-byte Rabin fingerprint, is always accepted.
The Confluent prefix, `0x00` followed by a 4-byte big-endian registry id, is accepted only when
the `avro` transformer's `schema_id` option is set. Otherwise the payload is
`avro: payload carries the Confluent prefix; set option 'schema_id' to its registry id`, and
a payload with neither prefix is
`avro: payload is not framed; expected single-object encoding (0xC3 0x01) or the Confluent prefix
(0x00)`.

A decoder binds one fingerprint algorithm for its life, so there is one decoder per framing. A
window mixing both still decodes in arrival order. `schema_id` also selects the framing on the way
out, so setting it makes the encoder emit Confluent framed payloads.

## Error mapping

`run-error` and what the host does with each variant are on
[the WIT contract](@/service/processors/build/wit-contract.md).
