# Arrow IPC conformance corpus

One set of test vectors, five codecs. All five `saci-sdk-*` codecs run this
corpus, so they agree on which streams are valid and which are refused. A sixth
implementation has an acceptance suite the day it starts.

The format itself is specified in
[the wire format reference](https://nassor.github.io/saci/library/reference/wire-format/).

## Layout

| path | what it is |
|---|---|
| `manifest.json` | the case list, generated and committed |
| `vectors/*.saci` | one binary stream per parse-level case |

Both are generated. Regenerate after any change to the wire format or the
canonical `Order` schema:

```bash
cargo run -p saci-service --features conformance --example conformance_vectors -- emit
```

The generator is `examples/conformance/conformance_vectors.rs`. It builds
a real `Dataset::write_ipc` stream and derives each malformed vector by editing
those bytes in place. Nothing here is a hand-forged flatbuffer: a forged stream
can drift from what arrow-rs emits, and then a codec that rejects it looks
correct while failing on real traffic.

`compressed_batch` is the one vector that is re-encoded rather than edited. It
goes back through arrow-rs with LZ4 on, which is what the generator's
`conformance` feature enables. A normal SACI build cannot write a compressed
batch and must refuse one.

## Manifest

```json
{
  "format_version": 1,
  "component": "Order",
  "schema_fingerprint": "f6405a7b",
  "reasons": ["bad_row_count", "..."],
  "cases": [ { "name": "...", "vector": "vectors/x.saci", "expect": "reject", "reason": "..." } ]
}
```

A case is either `"expect": "accept"` or `"expect": "reject"`.

An accept case carries what a codec must read back:

```json
"accept": {
  "components": ["Order", "__alive"],
  "component": "Order",
  "rows": 6,
  "columns": { "amount": { "type": "float64", "values": [1200.5, "..."] } }
}
```

A reject case carries a `reason`. Most reject cases fail while parsing the
vector. The five that carry an `op` parse fine and fail on the operation:

| `op.kind` | fields | meaning |
|---|---|---|
| `component` | `component` | address a component by name |
| `column` | `component`, `field`, `type` | read a column as `type` |
| `set` | `component`, `field`, `type`, `row`, `value` | write one value in place |

## Reason codes

The reason is the contract. Error text is deliberately local to each language,
so a harness maps the reason to whatever substring its own message uses.

| reason | the stream or call is refused because |
|---|---|
| `trailing_bytes` | bytes follow the zero-length terminator |
| `truncated_stream` | the terminator is missing, or a segment runs past the payload |
| `truncated_message` | a message's metadata length runs past its segment |
| `bad_continuation` | a message does not open with `0xFFFFFFFF` |
| `empty_segment` | a segment carries no Schema message at all |
| `first_message_not_schema` | the segment's first message is not a Schema. The rule is positional: message 0 must be the Schema, not merely that a Schema appears somewhere in the segment |
| `second_message_not_record_batch` | the message after the Schema is not a RecordBatch |
| `dictionary_batch` | the second message is a dictionary batch |
| `compressed_batch` | the record batch body declares compression |
| `extra_message` | a segment carries anything past one Schema and one RecordBatch, whether a third message or loose bytes after its end-of-stream marker |
| `bad_row_count` | the declared row count is negative, or above 2^31-1. A Utf8 column addresses its values with i32 offsets, so a wider batch is not describable by the format |
| `nodes_field_mismatch` | the node count does not equal the schema's field count |
| `buffer_overruns_body` | a buffer's declared span leaves the message body |
| `missing_component_key` | a segment's schema has no `__saci_component` metadata |
| `unknown_component` | no segment declares the requested component |
| `unknown_field` | the component declares no such field |
| `type_mismatch` | the field is not the requested type |
| `row_out_of_range` | the row index is past the last row |
| `variable_width_write` | an in-place write to a variable-width column |

## What the corpus deliberately does not cover

Two rules in the reference are not codec rules, and no vector asserts them.

The reference makes a mismatch between a component's row count and the
`__alive` length fatal on the host side. These codecs are processor-side: each
one documents that it never parses `__alive` content, and the host enforces the
rule.

The reference also states 8-byte buffer alignment as a property the arrow-rs
writer guarantees, not a rule a reader enforces. Every codec accepts a
misaligned but in-bounds buffer offset, and a vector for it would be inventing
a requirement.

## Writing a harness

A harness reads `manifest.json`, resolves each `vector` relative to the manifest,
and for each case asserts:

- accept: parsing succeeds, the component list matches, the row count matches,
  and every listed column reads back the listed values.
- reject: the operation throws, the error is the codec's own error type, and
  the message contains the substring the harness maps that reason to.

Asserting the error type matters as much as asserting the throw. A codec that
lets a native `RangeError` or `IndexOutOfBounds` escape has failed the case even
though something was raised, because a caller cannot tell a malformed stream
from a bug in the codec.

Each codec keeps its own reason-to-substring table, so adding a case means
adding one row per language and nothing else.

## License

This subtree is Apache-2.0, per `../LICENSE-APACHE`.
