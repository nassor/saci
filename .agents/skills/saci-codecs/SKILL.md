---
name: saci-codecs
description: Use when adding, changing, configuring, testing or documenting a SACI byte format (`saci::codec` and every `codec-*` feature of the `saci` crate), a CodecFactory or format registration, a `codec` node in a KDL config, or how a connector reaches its codec.
---

# SACI codecs

## Contract

`saci::codec` (feature `codec-api`) holds the byte-format contract: `Codec`, `BatchReader`,
`BatchWriter`, `MessageDecoder`, `MessageShape`, `CodecFactory`, `CodecRegistry`, `ReadInput` and
the `unsupported` error. The `Codec` trait itself is `codec::contract`.

`Codec::open_reader(&self, input: ReadInput, schema: Option<Arc<Schema>>)` reads a document from
`ReadInput::File(std::fs::File)` or `ReadInput::Memory(Cursor<Vec<u8>>)`; `From<File>` and
`From<Vec<u8>>` build one. A connector that holds a whole document in memory (a redb value, an HTTP
request body) passes `Memory`; one that streams a large document to disk passes the spool file as
`File`. `Codec::media_type()` is the content type of one whole document, `None` unless the codec
overrides it; the HTTP sink advertises it.

`BatchWriter` has two provided methods `FileSink` recovers a failed batch with: `flush` pushes the
writer's buffers to the handle (ndjson, avro, arrow-ipc implement it; csv already flushes per
batch; parquet keeps its row group until `finish`), and `restartable` says a fresh writer can
continue a file that already holds batches (`true` for ndjson and csv without a header, `false`
otherwise). A new writer implementation sets both.

`BatchReader` and `MessageDecoder` both provide `set_skip_corrupt(&mut self, skip: bool)` and
`drain_corrupt(&mut self) -> Vec<CorruptRecord>` (`codec::CorruptRecord`, re-exported from
`io::CorruptRecord`) for source-side bad-record isolation (`on_corrupt="drop"|"dlq"`). Implemented
with per-line/per-payload isolation in `ndjson`, `csv`, and `avro` (`MessageDecoder`).

A connector moves bytes and a codec turns bytes into `RecordBatch`es and back, so transport and
format are separate modules. A byte-carrying source or sink node names a declared `codec`
node's id with `codec="id"`; those are the KDL spellings of the codec declaration and
reference. `ServiceBuilder` resolves that node's `format` against the `CodecRegistry` in
`saci::codec` once per workflow build and hands the result to the connector inside a
`ConnectorContext`, which a connector reads with `ConnectorContext::codec(what)`. No connector
resolves a format itself, and none resolves one implicitly: the declared node's own `format` key is
what the registry resolves. The `service` feature owns the `Registry`; `ServiceBuilder::new()`
fills it through `register_builtin_factories` (`pub(crate)`) with every compiled-in connector and
codec, `ServiceBuilder::empty()` registers nothing, and the chain extends it through
`ServiceBuilder::connector(c)` and `ServiceBuilder::codec(f)` (`Registry::register_codec`).
`Registry::codecs()` returns the `CodecRegistry`, `Registry::codec_count()` its size;
`CodecRegistry` has `len`, `is_empty` and `names`.

## Formats

Each codec is built with `X::new()` and chained setters. Every `CodecFactory::build` rejects a
config key it does not read with `"{format}: unknown option '{key}'"`.

- `codec::arrow_ipc`: `ArrowIpcCodec::new()`, `ArrowIpcCodecFactory`. Stream read and write plus
  `PerBatch` messages, both over the same Arrow IPC stream encapsulation. Takes no options.
- `codec::avro`: `AvroCodec::new().compression(AvroCompression::Zstd).schema_id(7).schema_registry(...)`,
  `AvroCodecFactory`, `SchemaRegistryConfig`, `SchemaRegistryKind::{Confluent, Apicurio, Glue}`,
  `CompatibilityMode`, `RegistryAuth`, `check_avro_compatibility`. Object container files plus
  `PerRow` messages, framed single-object, Confluent (`0x00` + u32 BE), Apicurio (`0x00` + u32/u64
  BE), or AWS Glue (`0x03 0x00` + 16-byte UUID with SigV4 signing). `AvroCompression` is `Deflate`,
  `Snappy` or `Zstd`. Options `compression`, `schema_id`, `registry_url`, `registry_kind`, `subject`,
  `auto_register`, `compatibility`, `apicurio_group`, `apicurio_eight_byte_id`, `glue_registry_name`,
  `glue_schema_name`, `aws_region`, `auth_basic_user`, `auth_basic_password`, `auth_bearer_token`.
  Dynamic writer-schema lookups are cached in memory and decoded batches evolve onto the declared
  consumer schema via `EvolutionPolicy::compatible()`.
- `codec::csv`: `CsvCodec::new().has_headers(false)` (default `true`), `CsvCodecFactory`. Stream
  read and write plus `PerRow` messages, one record per payload. Option `has_headers` governs the
  stream surface only: a message carries no header row in either direction.
- `codec::ndjson`: `NdjsonCodec::new().infer_max(n)` (default 1024), `NdjsonCodecFactory`. Stream
  plus `PerRow` messages. Option `infer_max`.
- `codec::parquet`: `ParquetCodec::new()`, `ParquetCodecFactory`. Stream read and write plus
  `PerBatch` messages, one whole file per payload. The factory takes no options: Snappy
  compression is fixed, and the reader reports `estimated_rows` from row-group metadata. The
  reader opens `ReadInput::Memory` through `bytes::Bytes`.
- `crypto::EncryptedCodec` (`saci::crypto`, feature `crypto`): wraps any `Codec` to encrypt
  whole-stream documents with chunked AEAD framing (`SACS` v1 framing binding the 9-byte stream
  header, chunk index, and finality flag in AAD) and whole-message payloads
  (`SACI` v1 envelope framing) using a `Cipher` (`Algorithm::Aes256Gcm` or `Algorithm::ChaCha20Poly1305`)
  and `KeyProvider` (supporting `key_id`-tagged key rotation). Declared in KDL as a child
  `encryption { enabled #true algorithm="aes-256-gcm" active_key="k1" }` block inside `codec`.

## Features

`crates/saci/Cargo.toml`:

- `codec-arrow-ipc`, `codec-avro`, `codec-csv`, `codec-ndjson`, `codec-parquet`: one per format
  module, each compiling `saci::codec::<name>` and, when `service` is also on, registering its
  factory under its `format` name. `codecs` enables all five. `codec::arrow_ipc` also compiles
  under `service` alone, because the dead letter queue names it directly; its registration still
  needs `codec-arrow-ipc`.
- `codec-api` compiles the contract. `connector-api` compiles the connector contract.

`connector-kafka`, `connector-nats` and `connector-udp` imply `codec-ndjson` and `connector-tcp`
implies `codec-arrow-ipc`, so the connector feature alone is runnable. `connector-file`,
`connector-http`, `connector-redb` and `connector-s3` imply no codec, so the config picks which formats the binary
carries. `connector-peer` implies none either, and like `connector-channel`, `connector-postgresql`,
`connector-mariadb` and `connector-turso` it resolves none: a `peer` node carries `RecordBatch`es
over Arrow IPC natively and takes no `codec` key, so `ConnectorContext::codec` is never
called on it.

`BUILTIN_CODEC_FEATURES` (`crates/saci/src/service/factories.rs`) pairs every format name this
crate carries with the feature that compiles it in, listed unconditionally so a binary built
without a codec can still name the flag that supplies it. `builtin_codec_feature` is the lookup and
`missing_codec_error` the sole producer of the unregistered-format message, called from
`build_codecs`. A tabled format names `--features codec-<name>`; any other format keeps the plain
wording, so an embedder registering their own format is never told a build flag provides it. The
reduced build CI gates, `--no-default-features --features service`, carries no codec at all, which
is where that hint earns its place. `validate` does not partition unresolved formats into warnings
and errors the way it partitions unknown connector types: any unresolved format is a fatal build
error, exit 1 in every mode. `saci-service validate --connectors-only` prints
`OK: every source, sink and codec built`.

`server` carries all five formats; `cargo xtask server --codecs=csv` builds a server with one.

## Tests

The connector matrix (skill `saci-connectors`) runs every registered format against every
connector pair; that skill owns the exact format count and the `FORMATS` list.
`dimensions_cover_the_registry` in `crates/saci/tests/connector_matrix.rs` asserts the registered
codec formats equal that file's `FORMATS` list.

Each codec's `codec.rs` tests `open_reader` over `ReadInput::Memory` and `ReadInput::File` of the same bytes
(`an_in_memory_document_reads_the_same_batches_as_a_file`) and that its factory rejects an unknown option.

`arrow-ipc = "=60.2.0"` is exact-pinned workspace-wide (`AGENTS.md` Conventions); the arrow-ipc
codec shares that pin with the processor wire format, see skill `saci-processors`.

## Adding a codec

1. Module `crates/saci/src/codec/<name>.rs`, with `codec/<name>/codec.rs` and any further
   sub-files. `<name>.rs` declares the private `mod codec;` and re-exports `<Name>Codec`,
   `<Name>CodecFactory`; `crates/saci/src/codec.rs` declares it as
   `#[cfg(feature = "codec-<name>")] pub mod <name>;` (`arrow_ipc` is gated
   `any(feature = "codec-arrow-ipc", feature = "service")`, because the dead letter queue names it
   directly). Write no `use` of an extern crate named like a format module (`parquet`) in
   `codec.rs` itself: the child module shadows it there.
2. Implement `Codec` (`BatchReader`, `BatchWriter`, `MessageDecoder` as the format supports), with
   `<Name>Codec::new()` plus chained setters for each option, and a `CodecFactory` registered under
   the `format` name that errors with `"{format}: unknown option '{key}'"` on any key it does not
   read. Override `media_type` when the format has a registered one.
3. Add a `codec-<name>` feature to the `[features]` table of `crates/saci/Cargo.toml`: it starts
   with `codec-api`, then lists the `dep:` entries the module needs, each an `optional = true`
   dependency. Register the factory in `register_builtin_factories`
   (`crates/saci/src/service/factories.rs`) under `#[cfg(feature = "codec-<name>")]`. Add the
   feature to the `codecs` group and to `server` in the same table, which is what `all` reaches.
   `all_bundle_lists_every_feature` in `crates/saci/tests/feature_bundles.rs` asserts `all` reaches
   every feature the crate declares except `default`, `all` and `conformance`, following `server`,
   `connectors` and `codecs` transitively; `all_bundle_excludes_the_conformance_switch` forbids
   `conformance` in `all`.
4. Add the format to `FORMATS` and the capability table in `crates/saci/tests/common/matrix.rs`,
   and to the `live_slices!` formats list in `crates/saci/tests/connector_matrix.rs`;
   `every_slice_has_a_test` fails until both agree.
5. Add the format name and its feature to `BUILTIN_CODEC_FEATURES`;
   `builtin_codec_table_matches_the_registry` fails until you do.
6. Tests: no test binary is per format. A format is exercised through suites in
   `crates/saci/tests/` named for what they exercise, such as `avro_connector.rs` or
   `file_round_trip.rs`. Each opens with `#![cfg(all(feature = "codec-<name>", ...))]` listing the
   feature of every connector and codec the file names. A Docker-backed binary is excluded from the
   `default` profile in `.config/nextest.toml` as `binary_id(=saci::<file stem>)`. A `#[cfg(test)]`
   item under `crates/saci/src` that names `crate::codec::<name>` carries the same feature.
7. A page under `docs/content/service/formats/` (skill `saci-docs`).
8. Update this skill's `## Formats` and `## Features`.

## Keep this skill current

Update this file in the same change that: adds or removes a codec module or feature; changes
`Codec`, `BatchReader`, `BatchWriter`, `MessageDecoder`, `CodecFactory`, `CodecRegistry` or
`ReadInput`; adds, renames or changes the default of a format option or a codec constructor
setter; changes how `ServiceBuilder` resolves a `codec` node; changes the missing-format
wording or `BUILTIN_CODEC_FEATURES` (also update skill `saci-service`); changes which connector
features imply which codec (also update skill `saci-connectors`); changes the IO layer contract
quoted here and in skill `saci-connectors` (also check that skill; the canonical copy is skill
`saci-service`'s Service layer section).
