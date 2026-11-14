---
name: saci-transformers
description: Use when adding, changing, configuring, testing or documenting a SACI byte format (`saci::transformer` and every `transformer-*` feature of the `saci` crate), a TransformerFactory or format registration, a transformer node in a KDL config, or how a connector reaches its transformer.
---

# SACI transformers

## Contract

`saci::transformer` (feature `transformer-contract`) holds the byte-format contract: `Transformer`,
`BatchReader`, `BatchWriter`, `MessageDecoder`, `TransformerFactory`, `TransformerRegistry`. The
`Transformer` trait itself is `transformer::contract`.

`BatchWriter` has two provided methods `FileSink` recovers a failed batch with: `flush` pushes the
writer's buffers to the handle (ndjson, avro, arrow-ipc implement it; csv already flushes per
batch; parquet keeps its row group until `finish`), and `restartable` says a fresh writer can
continue a file that already holds batches (`true` for ndjson and csv without a header, `false`
otherwise). A new writer implementation sets both.

A connector moves bytes and a transformer turns bytes into `RecordBatch`es and back, so transport
and format are separate modules. A byte-carrying source or sink node names a declared `transformer`
node's id; `ServiceBuilder` resolves that node's `format` against the `TransformerRegistry` in
`saci::transformer` once per workflow build and hands the result to the factory inside a
`ConnectorContext`, so no connector resolves a format itself. No connector resolves a format
implicitly either: a byte-carrying node names a declared `transformer` node's id, and that node's
own `format` key is what the registry resolves. The `service` feature owns the `Registry` and
`register_builtin_factories`, extended through `register_source`, `register_sink`, and
`register_transformer`.

## Formats

- `transformer::arrow_ipc`: `ArrowIpcTransformer`, stream read and write plus `PerBatch`
  messages, both over the same Arrow IPC stream encapsulation.
- `transformer::avro`: `AvroTransformer`, object container files plus `PerRow` messages, framed
  single-object or Confluent. Options `compression`, `schema_id`.
- `transformer::csv`: `CsvTransformer`, stream read and write plus `PerRow` messages, one
  record per payload. Option has_headers governs the stream surface only: a message carries no
  header row in either direction.
- `transformer::ndjson`: `NdjsonTransformer`, stream plus `PerRow` messages. Option
  `infer_max`.
- `transformer::parquet`: `ParquetTransformer`, stream read and write plus `PerBatch` messages,
  one whole file per payload. The factory reads no options: Snappy compression is fixed, and the
  reader reports `estimated_rows` from row-group metadata.

## Features

`crates/saci/Cargo.toml`:

- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet`: one per format module, each compiling `saci::transformer::<name>` and, when
  `service` is also on, registering its factory under its `format` name. `transformers` enables all
  five. `transformer::arrow_ipc` also compiles under `service` alone, because the dead letter queue
  names it directly; its registration still needs `transformer-arrow-ipc`.

`connector-kafka`, `connector-nats` and `connector-udp` imply `transformer-ndjson` and
`connector-tcp` implies `transformer-arrow-ipc`, so the connector feature alone is runnable.
`connector-file`, `connector-http` and `connector-s3` imply no transformer, so the config picks
which formats the binary carries. `connector-saci` implies none either, and like `connector-channel`,
`connector-postgresql`, `connector-mariadb` and `connector-turso` it resolves none: a `saci` node carries
`RecordBatch`es over Arrow IPC natively and takes no `transformer` key, so
`ConnectorContext::transformer` is never called on it.

`BUILTIN_TRANSFORMER_FEATURES` (`crates/saci/src/service/factories.rs`) pairs every
format name this crate carries with the feature that compiles it in, listed unconditionally so a
binary built without a transformer can still name the flag that supplies it.
`builtin_transformer_feature` is the lookup and `missing_transformer_error` the sole producer of
the unregistered-format message, called from `build_transformers`. A tabled format names
`--features transformer-<name>`; any other format keeps the plain wording, so an embedder
registering their own format is never told a build flag provides it. The reduced build CI gates,
`--no-default-features --features service`, carries no transformer at all, which is where that
hint earns its place. `validate` does not partition unresolved formats into warnings and errors
the way it partitions unknown connector types: any unresolved format is a fatal build error, exit
1 in every mode.

`server` carries all five formats.

## Tests

The connector matrix (skill `saci-connectors`) runs every registered format against every
connector pair; that skill owns the exact format count and the `FORMATS` list.
`dimensions_cover_the_registry` in `crates/saci/tests/connector_matrix.rs` asserts the
registered transformer formats equal that file's `FORMATS` list.

`arrow-ipc = "=60.1.0"` is exact-pinned workspace-wide (`AGENTS.md` Conventions); the arrow-ipc
transformer shares that pin with the processor wire format, see skill `saci-processors`.

## Adding a transformer

1. Module `crates/saci/src/transformer/<name>.rs`, with `transformer/<name>/transformer.rs` and any
   further sub-files, declared in `crates/saci/src/transformer.rs` as
   `#[cfg(feature = "transformer-<name>")] pub mod <name>;` (`arrow_ipc` is gated
   `any(feature = "transformer-arrow-ipc", feature = "service")`, because the dead letter queue
   names it directly). Write no `use` of an extern crate named like a format module (`parquet`) in
   `transformer.rs` itself: the child module shadows it there.
2. Implement `Transformer` (`BatchReader`, `BatchWriter`, `MessageDecoder` as the format supports)
   and a `TransformerFactory` registered under the `format` name.
3. Add a `transformer-<name>` feature to the `[features]` table of `crates/saci/Cargo.toml`: it
   starts with `transformer-contract`, then lists the `dep:` entries the module needs, each an
   `optional = true` dependency. Register the factory in `register_builtin_factories`
   (`crates/saci/src/service/factories.rs`) under `#[cfg(feature = "transformer-<name>")]`. Add the
   feature to the `transformers` group and to `server` in the same table, which is what `all`
   reaches. `all_bundle_lists_every_feature` in `crates/saci/tests/feature_bundles.rs` asserts
   `all` reaches every feature the crate declares except `default`, `all` and `conformance`,
   following `server`, `connectors` and `transformers` transitively;
   `all_bundle_excludes_the_conformance_switch` forbids `conformance` in `all`.
4. Add the format to `FORMATS` and the capability table in
   `crates/saci/tests/common/matrix.rs`, and to the `live_slices!` formats list in
   `crates/saci/tests/connector_matrix.rs`; `every_slice_has_a_test` fails until both
   agree.
5. Add the format name and its feature to `BUILTIN_TRANSFORMER_FEATURES`;
   `builtin_transformer_table_matches_the_registry` fails until you do.
6. Tests: no test binary is per format. A format is exercised through suites in
   `crates/saci/tests/` named for what they exercise, such as `avro_connector.rs` or
   `file_round_trip.rs`. Each opens with
   `#![cfg(all(feature = "transformer-<name>", ...))]` listing the feature of every connector and
   transformer the file names. A Docker-backed binary is excluded from the `default` profile in
   `.config/nextest.toml` as `binary_id(=saci::<file stem>)`. A `#[cfg(test)]` item under
   `crates/saci/src` that names `crate::transformer::<name>` carries the same feature.
7. A page under `docs/content/service/formats/` (skill `saci-docs`).
8. Update this skill's `## Formats` and `## Features`.

## Keep this skill current

Update this file in the same change that: adds or removes a transformer module or feature; changes
`Transformer`, `BatchReader`, `BatchWriter`, `MessageDecoder`, `TransformerFactory` or
`TransformerRegistry`; adds, renames or changes the default of a format option; changes how
`ServiceBuilder` resolves a transformer node; changes the missing-format wording or
`BUILTIN_TRANSFORMER_FEATURES` (also update skill `saci-service`); changes which connector
features imply which transformer (also update skill `saci-connectors`); changes the IO layer
contract quoted here and in skill `saci-connectors` (also check that skill; the canonical copy
is skill `saci-service`'s Service layer section).
