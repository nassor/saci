---
name: saci-transformers
description: Use when adding, changing, configuring, testing or documenting a SACI byte format (saci-transformer and every saci-transformer-* crate), a TransformerFactory or format registration, a transformer node in a KDL config, or how a connector reaches its transformer.
---

# SACI transformers

## Contract

`saci-transformer` holds the byte-format contract: `Transformer`, `BatchReader`, `BatchWriter`,
`MessageDecoder`, `TransformerFactory`, `TransformerRegistry`.

A connector moves bytes and a transformer turns bytes into `RecordBatch`es and back, so transport
and format are separate crates. A byte-carrying source or sink node names a declared `transformer`
node's id; `ServiceBuilder` resolves that node's `format` against the `TransformerRegistry` in
`saci-transformer` once per workflow build and hands the result to the factory inside a
`ConnectorContext`, so no connector resolves a format itself. No connector resolves a format
implicitly either: a byte-carrying node names a declared `transformer` node's id, and that node's
own `format` key is what the registry resolves. `saci-service` owns the `Registry` and
`register_builtin_factories`, extended through `register_source`, `register_sink`, and
`register_transformer`.

## Crates

- `saci-transformer-arrow-ipc`: `ArrowIpcTransformer`, stream read and write plus `PerBatch`
  messages, both over the same Arrow IPC stream encapsulation.
- `saci-transformer-avro`: `AvroTransformer`, object container files plus `PerRow` messages, framed
  single-object or Confluent. Options `compression`, `schema_id`.
- `saci-transformer-csv`: `CsvTransformer`, stream read and write plus `PerRow` messages, one
  record per payload. Option has_headers governs the stream surface only: a message carries no
  header row in either direction.
- `saci-transformer-ndjson`: `NdjsonTransformer`, stream plus `PerRow` messages. Option
  `infer_max`.
- `saci-transformer-parquet`: `ParquetTransformer`, stream read and write plus `PerBatch` messages,
  one whole file per payload. The factory reads no options: Snappy compression is fixed, and the
  reader reports `estimated_rows` from row-group metadata.

## Features

`saci-service`:

- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet`: one per transformer crate, registering its factory under its `format`
  name (each implies `service`).

`connector-kafka` and `connector-nats` imply `transformer-ndjson` and `connector-tcp` implies
`transformer-arrow-ipc`, so the connector feature alone is runnable. `connector-file`,
`connector-http` and `connector-s3` imply no transformer, so the config picks which formats the
binary carries. `connector-saci` implies none either, and like `connector-channel`,
`connector-postgresql` and `connector-turso` it resolves none: a `saci` node carries
`RecordBatch`es over Arrow IPC natively and takes no `transformer` key, so
`ConnectorContext::transformer` is never called on it.

`BUILTIN_TRANSFORMER_FEATURES` (`crates/saci-service/src/service/factories.rs`) pairs every
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

`saci`:

- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet` (`transformers` enables all five): one per byte-format crate.

## Tests

The connector matrix (skill `saci-connectors`) runs every registered format against every
connector pair; that skill owns the exact format count and the `FORMATS` list.
`dimensions_cover_the_registry` in `crates/saci-service/tests/connector_matrix.rs` asserts the
registered transformer formats equal that file's `FORMATS` list.

`arrow-ipc = "=59.3.0"` is exact-pinned workspace-wide (`AGENTS.md` Conventions); the arrow-ipc
transformer shares that pin with the processor wire format, see skill `saci-processors`.

## Adding a transformer

1. New crate `crates/saci-transformer-<name>`, workspace-inherited version and deps.
2. Implement `Transformer` (`BatchReader`, `BatchWriter`, `MessageDecoder` as the format supports)
   and a `TransformerFactory` registered under the `format` name.
3. Add a `transformer-<name>` feature to `crates/saci-service/Cargo.toml` registering the factory
   in `register_builtin_factories`, to that crate's `default` and `all` lists, and to the `saci`
   facade's `transformers` group in `crates/saci/Cargo.toml`, which is what the facade's `all`
   reaches. `all_bundle_lists_every_feature` in `crates/saci-service/tests/feature_bundles.rs`
   asserts `all` lists every feature that crate declares except `default`, `all` and
   `conformance`, which `all_bundle_excludes_the_conformance_switch` forbids in `all`;
   `all_bundle_reaches_every_feature` in `crates/saci/tests/feature_bundles.rs` expands the
   facade's `all` through the groups transitively, so it fails until the feature is in
   `transformers`.
4. Add the format to `FORMATS` and the capability table in
   `crates/saci-service/tests/connector_matrix.rs`.
5. Add the format name and its feature to `BUILTIN_TRANSFORMER_FEATURES`;
   `builtin_transformer_table_matches_the_registry` fails until you do.
6. A page under `docs/content/service/formats/` (skill `saci-docs`).
7. Update this skill's `## Crates` and `## Features`.

## Keep this skill current

Update this file in the same change that: adds or removes a transformer crate; changes
`Transformer`, `BatchReader`, `BatchWriter`, `MessageDecoder`, `TransformerFactory` or
`TransformerRegistry`; adds, renames or changes the default of a format option; changes how
`ServiceBuilder` resolves a transformer node; changes the missing-format wording or
`BUILTIN_TRANSFORMER_FEATURES` (also update skill `saci-service`); changes which connector
features imply which transformer (also update skill `saci-connectors`); changes the IO layer
contract quoted here and in skill `saci-connectors` (also check that skill; the canonical copy
is skill `saci-service`'s Service layer section).
