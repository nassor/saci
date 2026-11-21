+++
title = "Writing a codec"
description = "Implement Codec and register it under a format name."
template = "page.html"
weight = 2
aliases = ["/library/service/transformers/"]
+++
# Writing a codec

A connector moves bytes and a codec gives them meaning. `saci::codec`
holds five traits and two enums. Four of the traits are below, and
`CodecFactory` is under [Writing your own](#writing-your-own). Only
`format` has no default: a codec implements the surfaces it has, inherits
an `unsupported` error for the rest, and declares no message shape. Both readers
and writers are synchronous, because the connector owns the thread and the
async plumbing.

## The traits

```rust,name=Every defaulted method returns unsupported or None
// saci::codec
pub trait Codec: Send + Sync + 'static {
    fn format(&self) -> &'static str;
    fn open_reader(&self, input: ReadInput, schema: Option<Arc<Schema>>)
        -> Result<Box<dyn BatchReader>, SaciError>;      // defaults to `unsupported`
    fn open_writer(&self, output: Box<dyn std::io::Write + Send>, schema: Arc<Schema>)
        -> Result<Box<dyn BatchWriter>, SaciError>;      // defaults to `unsupported`
    fn open_message_decoder(&self, schema: Arc<Schema>)
        -> Result<Box<dyn MessageDecoder>, SaciError>;   // defaults to `unsupported`
    fn encode_messages(&self, batch: &RecordBatch)
        -> Result<Vec<Vec<u8>>, SaciError>;             // defaults to `unsupported`
    fn message_shape(&self) -> Option<MessageShape>;    // defaults to None
    fn media_type(&self) -> Option<&'static str>;       // defaults to None
}

pub trait BatchReader: Send {
    fn schema(&self) -> Arc<Schema>;
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError>;
    fn estimated_rows(&self) -> Option<usize>;          // defaults to None
}

pub trait BatchWriter: Send {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError>;
    fn finish(self: Box<Self>) -> Result<(), SaciError>;
}

pub trait MessageDecoder: Send {
    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError>;
    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError>;
}

pub enum MessageShape { PerRow, PerBatch }
```

Message encoding is `Codec::encode_messages` itself, so there is no encoder
trait to implement. `BatchWriter::finish` consumes the writer, so a format with a
footer cannot be written to afterwards. `MessageDecoder::flush` resets the
decoder, so one decoder serves a whole TCP connection or Kafka consumer.

## The five built-ins

One module per format under `saci::codec`, each behind its own feature and each
registering its factory under the name a `format` key selects.

| `format` | Struct | Module | Feature |
|---|---|---|---|
| `csv` | `CsvCodec` | `saci::codec::csv` | `codec-csv` |
| `ndjson` | `NdjsonCodec` | `saci::codec::ndjson` | `codec-ndjson` |
| `parquet` | `ParquetCodec` | `saci::codec::parquet` | `codec-parquet` |
| `avro` | `AvroCodec` | `saci::codec::avro` | `codec-avro` |
| `arrow-ipc` | `ArrowIpcCodec` | `saci::codec::arrow_ipc` | `codec-arrow-ipc` |

Each is built with `new()` and chained setters, and each factory refuses a key it does not
read with `{format}: unknown option '{key}'`.

```rust,name=Building a codec in Rust
CsvCodec::new().has_headers(false);                       // default: true
NdjsonCodec::new().infer_max(512);                        // default: 1024
AvroCodec::new().compression(AvroCompression::Zstd).schema_id(7);
ParquetCodec::new();                                      // no options
ArrowIpcCodec::new();                                     // no options
```

`open_reader` takes a `ReadInput`: `ReadInput::File` over an open file, or
`ReadInput::Memory` over a document already in memory. `File` and `Vec<u8>` both convert
with `From`. `Codec::media_type()` is the content type of one whole document, which the
HTTP sink sends.

`csv`, `ndjson` and `avro` answer `PerRow` to `message_shape`, one message per
row; `parquet` and `arrow-ipc` answer `PerBatch`, one message per batch. A
message connector refuses its config at build time against a format that answers
neither.

## Writing your own

A format in this repository is a module in `crates/saci/src/codec/<name>.rs` (with
`codec/<name>/codec.rs` for the codec), declared in
`crates/saci/src/codec.rs` and enabled by a `codec-<name>` entry in the `[features]`
table of `crates/saci/Cargo.toml`. A codec outside the repository is a crate that depends
on `saci` with `codec-api`, which brings `SaciError` and the traits, and on nothing
else of SACI: no connector, no host. Implement `Codec` for the codec and
`CodecFactory` for the name a `format` key selects it with.

```rust,name=The factory reads its config table, the codec it builds is shared
// saci::codec
pub trait CodecFactory: Send + Sync + 'static {
    fn format_name(&self) -> &'static str;
    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Codec>, SaciError>;
}

// in a custom binary
let builder = ServiceBuilder::new()
    .codec(ProtobufCodecFactory);
```

`ServiceBuilder::codec(f)` registers it in the builder's `CodecRegistry`, and from then on
`codec "p" format="protobuf"` resolves, with no change to any connector.
Registering a `format_name` that is already taken replaces the earlier factory,
so a custom `csv` can stand in for the built-in one.

## Features

With `service` on, one feature per codec decides what
`ServiceBuilder::new()` registers: `codec-csv`, `codec-ndjson`,
`codec-parquet`, `codec-avro` and `codec-arrow-ipc`. `ServiceBuilder::empty()` registers nothing.
`connector-kafka`, `connector-nats` and `connector-udp` each imply
`codec-ndjson` and `connector-tcp` implies `codec-arrow-ipc`, so
any one of the four is runnable on its own.

```bash,name=connector-file implies no codec, so pick one
cargo build -p saci --bin saci-service --no-default-features --features service,connector-file,codec-csv,wasm-host
```

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
A <code>format</code> the build does not carry fails at load:
<code>codec 'orders_json' names format 'parquet', which no
codec is registered for (registered: csv, ndjson)</code>. The registry
holds exactly what the features compiled in, so the fix is a feature or a
<code>ServiceBuilder::codec</code> call.
</p>
</div>

[Crates and features](@/library/reference/crates.md) lists every feature the
binary can carry.
