+++
title = "Writing a transformer"
description = "Implement Transformer and register it under a format name."
template = "page.html"
weight = 2
+++
# Writing a transformer

A connector moves bytes and a transformer gives them meaning. `saci-transformer`
holds five traits and one enum. Four of them are below, and
`TransformerFactory` is under [Writing your own](#writing-your-own). Only
`format` has no default: a transformer implements the surfaces it has, inherits
an `unsupported` error for the rest, and declares no message shape. Both readers
and writers are synchronous, because the connector owns the thread and the
async plumbing.

## The traits

```rust,name=Every defaulted method returns unsupported or None
// saci-transformer
pub trait Transformer: Send + Sync + 'static {
    fn format(&self) -> &'static str;
    fn open_reader(&self, input: std::fs::File, declared: Option<Arc<Schema>>)
        -> Result<Box<dyn BatchReader>, SaciError>;      // defaults to `unsupported`
    fn open_writer(&self, output: Box<dyn std::io::Write + Send>, schema: Arc<Schema>)
        -> Result<Box<dyn BatchWriter>, SaciError>;      // defaults to `unsupported`
    fn open_message_decoder(&self, schema: Arc<Schema>)
        -> Result<Box<dyn MessageDecoder>, SaciError>;   // defaults to `unsupported`
    fn encode_messages(&self, batch: &RecordBatch)
        -> Result<Vec<Vec<u8>>, SaciError>;             // defaults to `unsupported`
    fn message_shape(&self) -> Option<MessageShape>;    // defaults to None
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

Message encoding is `Transformer::encode_messages` itself, so there is no encoder
trait to implement. `BatchWriter::finish` consumes the writer, so a format with a
footer cannot be written to afterwards. `MessageDecoder::flush` resets the
decoder, so one decoder serves a whole TCP connection or Kafka consumer.

## The five built-ins

One crate per format, each registering its factory under the name a `format` key
selects.

| `format` | Struct | Crate |
|---|---|---|
| `csv` | `CsvTransformer` | `saci-transformer-csv` |
| `ndjson` | `NdjsonTransformer` | `saci-transformer-ndjson` |
| `parquet` | `ParquetTransformer` | `saci-transformer-parquet` |
| `avro` | `AvroTransformer` | `saci-transformer-avro` |
| `arrow-ipc` | `ArrowIpcTransformer` | `saci-transformer-arrow-ipc` |

`csv`, `ndjson` and `avro` answer `PerRow` to `message_shape`, one message per
row; `parquet` and `arrow-ipc` answer `PerBatch`, one message per batch. A
message connector refuses its config at build time against a format that answers
neither.

## Writing your own

An out-of-tree transformer is a crate that depends on `saci-core` for `SaciError`
and `saci-transformer` for the traits, and on nothing else of SACI: no connector,
no host. Implement `Transformer` for the codec and `TransformerFactory` for the
name a `format` key selects it with.

```rust,name=The factory reads options, the transformer it builds is shared
// saci-transformer
pub trait TransformerFactory: Send + Sync + 'static {
    fn format_name(&self) -> &'static str;
    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError>;
}

// in a custom binary
let builder = register_builtin_factories(ServiceBuilder::new())
    .register_transformer(ProtobufTransformerFactory);
```

`ServiceBuilder::register_transformer` installs it, and from then on
`transformer "p" format="protobuf"` resolves, with no change to any connector.
Registering a `format_name` that is already taken replaces the earlier factory,
so a custom `csv` can stand in for the built-in one.

## Features

On `saci-service`, one feature per transformer decides what
`register_builtin_factories` installs: `transformer-csv`, `transformer-ndjson`,
`transformer-parquet`, `transformer-avro` and `transformer-arrow-ipc`.
`connector-kafka` and `connector-nats` each imply `transformer-ndjson` and
`connector-tcp` implies `transformer-arrow-ipc`, so any one of the three is
runnable on its own.

```bash,name=connector-file implies no transformer, so pick one
cargo build --features connector-file,transformer-csv,wasm
```

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
A <code>format</code> the build does not carry fails loudly rather than falling
back: <code>transformer 'orders_json' names format 'parquet', which no
transformer is registered for (registered: csv, ndjson)</code>. The registry
holds exactly what the features compiled in, so the fix is a feature or a
<code>register_transformer</code> call.
</p>
</div>

[Crates and features](@/library/reference/crates.md) lists every feature the
binary can carry.
