+++
title = "Crates and features"
description = "Every workspace crate and every feature flag, with what each one adds."
template = "page.html"
weight = 1
+++
# Crates and features

Which crate to depend on, and which feature turns each part on. `saci` is the crate to add for
embedding, whose feature flags pick the engine, connectors, byte formats and authoring SDKs a
downstream project needs. The individual crates below remain available directly, for a narrower
build than `saci`'s feature groups express. None are published to crates.io yet, so depend on
them by path inside a clone, or by git outside one.

## Workspace crates

| Crate | What it gives you |
|---|---|
| `saci` | Facade for embedding SACI as a pure-Rust library: the engine, the connectors that need nothing installed or already running, byte formats, and the wasm processor / native plugin authoring SDKs, behind one dependency. `use saci::prelude::*;` is the documented entry point. |
| `saci-core` | The engine: `Dataset`, `Pipeline`, `System`, `Scheduler`, `Component`, plus the `Source` and `Sink` traits and the schema cast helpers. Arrow, serde and futures unconditionally; tokio and rayon only under the default `runtime` feature, so a `wasm32-wasip2` build turns that feature off. |
| `saci-config` | The configuration language: parses KDL into `ConfigValue` (an alias for `serde_json::Value`) plus `ConfigMap`, and exports `from_kdl_str`, `from_kdl_str_with_vars`, `one_or_many`, `substitute_env_vars` and `substitute_vars`. `from_kdl_str_with_vars` substitutes and parses in one call, and a parse failure over substituted text names the variables whose values were inserted, plus any holding a backslash or double quote. Values stay out of the message. |
| `saci-connector` | The factory contract: `SourceFactory`, `SinkFactory`, `ConnectorContext`, the `ChannelBridge` trait, `parse_schema_fields` and `parse_optional_schema_fields`. Sits below `saci-service`, so a connector needs no dependency on the host. |
| `saci-connector-channel` | `ChannelSource`, `ChannelSink`: in-memory mpsc transport. `ChannelRegistry` is the name-keyed bridge pairing a sink in one workflow with a source in another. |
| `saci-connector-datafusion` | `DataFusionSource`. No factory: it needs a live `SessionContext`. |
| `saci-connector-file` | `FileSource`, `FileSink`: local-file IO, format from a transformer. |
| `saci-connector-http` | `HttpSource`, `HttpSink`: one GET spooled through a transformer, and one self-contained document per batch back out. |
| `saci-connector-kafka` | `KafkaSource`, `KafkaSink`: `librdkafka` backed, one topic or several. |
| `saci-connector-nats` | `NatsSource`, `NatsSink`: core subject pub/sub or JetStream, chosen by a mode node's `kind` key. |
| `saci-connector-postgresql` | `PostgresSource`, `PostgresSink`: native client, TLS, and `polling` / `cdc_trigger` / `cdc_logical` read modes. |
| `saci-connector-turso` | `TursoSource`, `TursoSink`: the in-process, SQLite-compatible `turso` engine, embedded in the service's own process or a synced replica of Turso Cloud or sqld. Reads through `polling`, `dump` or `cdc`; writes through `append`, `upsert` or `ignore_conflicts` over a `deferred`, `immediate` or `concurrent` (MVCC) transaction, with optional change capture and page-level encryption. Pins `turso = "=0.7.2"`. |
| `saci-connector-redb` | `RedbSource`, `RedbSink`: an embedded redb key/value file as a transport. One entry per batch, keyed `{key_prefix}{seq:020}{key_suffix}` so key order is insertion order, in the byte format a declared transformer names. |
| `saci-connector-s3` | `S3Source`, `S3Sink`: any S3-compatible endpoint, timestamped object keys, row, byte and age flush thresholds. |
| `saci-connector-tcp` | `TcpIngestSource`, `TcpSink`: live length-prefixed frames, decoded and encoded by a transformer. |
| `saci-connector-saci` | `SaciSource`, `SaciSink`: one SACI service pushing `RecordBatch`es to another over Arrow IPC. A session opens with a hello naming the sending service, workflow and sink node, which the receiving side puts in its own series labels and span fields. |
| `saci-processor` | The processor SDK: re-exports `saci-core`, the `export_pipeline!` macro, the `Component`, `transform`, `fold` and `processor` macros, and `Config`, `Error`, `Result`, `ProcessorState`, `RouteDecision`. Owns the canonical WIT package at `wit/pipeline.wit`. |
| `saci-plugin` | The Rust plugin SDK: re-exports `saci-core` and `export_plugin!`, which wires a `fn() -> Pipeline` into a cdylib exporting the `saci-plugin-abi` C ABI. The host that `dlopen`s that cdylib is `saci-service`'s `plugin` feature, not this crate. |
| `saci-plugin-abi` | The C ABI itself: the two symbols a plugin exports, `saci_abi_version` and `saci_plugin_v1`, the `SaciPluginV1` and `SaciHostV1` vtables they fill, and their layout. |
| `saci-service` | The host: wasmtime runtime, distributed and raft, HTTP control plane, config loading, the factory `Registry`, and the `saci-service` binary. It ships no `Source`, `Sink` or `Transformer` of its own. |
| `saci-transformer` | The byte-format contract: `Transformer`, `BatchReader`, `BatchWriter`, `MessageDecoder`, `TransformerFactory`, `TransformerRegistry`. |
| `saci-transformer-arrow-ipc` | `ArrowIpcTransformer`: stream read and write plus `PerBatch` messages, both over the same Arrow IPC stream encapsulation. |
| `saci-transformer-avro` | `AvroTransformer`: object container files plus `PerRow` messages, framed single-object or Confluent. Options `compression`, `schema_id`. |
| `saci-transformer-csv` | `CsvTransformer`: stream read and write plus `PerRow` messages, one record per payload. Option `has_headers`. |
| `saci-transformer-ndjson` | `NdjsonTransformer`: stream plus `PerRow` messages. Option `infer_max`. |
| `saci-transformer-parquet` | `ParquetTransformer`: stream read and write plus `PerBatch` messages, one whole file per payload. Snappy compression is fixed and the factory reads no options; the reader reports `estimated_rows` from row-group metadata. |
| `saci-inspector-wire` | The inspector's JSON contract: `Topology`, `Snapshot`, `SpanRecord` and friends. serde only, so both the host and the browser compile it. |

## saci features

The **default** bundle is `engine` alone: the columnar engine with no connector, transformer or
authoring SDK pulled in.

- `engine` (**default**): `Dataset`, `Pipeline`, `System`, `Scheduler`, `Component`, the
  `Source`/`Sink` traits (implies `saci-core/io`, which implies `runtime`).
- `windows`: windowed aggregation, forwarding `saci-core/windows` under whichever of `engine`,
  `processor` or `plugin` is enabled.
- `connector-channel`, `connector-file`, `connector-redb`, `connector-http`,
  `connector-tcp`, `connector-saci`, `connector-datafusion` (`connectors` enables all seven): one
  per connector that
  needs nothing
  installed or already running. Never `saci-connector-kafka`, `-nats`, `-postgresql`, `-s3` or
  `-turso`. Each needs a specific broker, database or object store installed and running first, so
  this crate does not carry them.
- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet` (`transformers` enables all five): one per byte format.
- `transformer-contract`: `Transformer`, `BatchReader`, `BatchWriter` and `MessageDecoder`, the
  contract itself with no format behind it. Every `connector-file`, `connector-http`,
  `connector-tcp` and `transformer-*` feature turns it on, so a build rarely names it.
- `processor`: `Pipeline`, `System`, `Component` (the trait, not the derive),
  `Config`, `Error`, `Result`, `ProcessorState`, `RouteDecision` and
  `export_pipeline!`, for a hand-written WebAssembly Component Model processor. Not the
  `#[derive(Component)]`/`#[transform]`/`#[fold]`/`#[processor]` macros, whose expansions name
  `saci-processor` directly, so a processor crate using them depends on `saci-processor`
  instead. A processor targets `wasm32-wasip2` and must not pull tokio in, so build with
  `--no-default-features --features processor`.
- `plugin`: `Pipeline`, `System`, `Component`, `ProcessorState`, `RouteDecision` and
  `export_plugin!`, for a native plugin: a cdylib the host `dlopen`s.
- `all`: every feature this crate carries, `engine`, `windows`, `connectors`, `transformers`,
  `processor` and `plugin`, in one build. `all` reaches `saci-core`'s `runtime` feature through
  `engine`, and tokio cannot target `wasm32-wasip2`; a wasm processor crate still builds with
  `--no-default-features --features processor`.

`engine`, `processor` and `plugin` each enable saci-core directly, so every name they share is
one re-export, not three with a precedence rule between them. The three still author different
artifacts and are not meant to be combined in one build, but doing so is redundant, not
ambiguous.

## saci-core features

- `runtime` (**default**): tokio and rayon stage parallelism. Disable for wasm
  processor builds.
- `processor`: the `wasm32-wasip2` target, sequential-only execution driven by the
  `pollster` sync executor.
- `windows`: windowed aggregation. `WindowedSystem`, watermarks,
  `WindowAccumulator`. The `WindowSpec` geometry enum is outside the feature,
  so a host parses a `window` declaration in every build.
- `io`: the `Source` and `Sink` traits and the schema cast helpers (implies
  `runtime`).
- `distributed`: types shared with the host's distributed layer (implies
  `runtime`).
- `tracing`: `tracing` crate integration. Gates the events and the four nested
  spans `pipeline.run`, `pipeline.stage`, `system.execute` and `task_attempt`,
  the last of which opens only on a retry.

## saci-service features

The **default** bundle is `mimalloc`, `service`, `wasm`, `windows`, `parquet-checkpoint`,
`connector-channel`, `connector-file`, `connector-http`, `connector-tcp`, `connector-saci`,
`connector-redb`, and
every transformer, so `cargo install saci-service` yields a runnable binary with no flags. A
connector ships by default only when nothing has to be installed or already running for it to
work and it needs no extra build toolchain: `connector-channel` is in-process, `connector-file`
reads the local filesystem, `connector-redb` reads and writes one embedded file on local disk,
`connector-http` is an HTTP client with no server of its own, `connector-tcp` is a raw
socket, and `connector-saci`'s peer is another `saci-service`. HTTP, TCP and the peer link speak
the network, and they qualify because none depends on a specific
server product; every connector that needs a database, broker or object store running first is
opt-in instead: `connector-postgresql`, `connector-nats`, `connector-s3`, `connector-kafka` and
`connector-turso`. `connector-kafka` stays opt-in because `librdkafka-sys` builds vendored C and
needs `cmake` plus a C toolchain; `connector-turso` stays opt-in because its synced mode pulls
hyper and rustls; `distributed-raft` and `service-cluster` stay opt-in because a cluster node is
a deliberate deployment choice.

- `connector-channel`, `connector-file`, `connector-http`, `connector-kafka`,
  `connector-nats`, `connector-postgresql`, `connector-redb`, `connector-s3`,
  `connector-saci`, `connector-tcp`, `connector-turso`: one
  per connector crate. Each pulls the crate in and registers its factories in
  `register_builtin_factories` (each implies `service`). `connector-kafka` and
  `connector-nats` imply `transformer-ndjson` and `connector-tcp` implies
  `transformer-arrow-ipc`, so the connector feature alone is runnable.
  `connector-file`, `connector-http`, `connector-redb` and `connector-s3` imply
  no transformer, so the config picks which formats the binary carries.
  `connector-saci` implies none either, and resolves none: Arrow IPC is its
  fixed wire format and a `saci` node takes no `transformer` key.
- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`,
  `transformer-ndjson`, `transformer-parquet`: one per transformer crate,
  registering its factory under its `format` name (each implies `service`).
- `parquet-checkpoint`: `ParquetCheckpointStore`, the archival checkpoint store
  (implies `distributed`).
- `windows`: forwards `saci-core/windows`.
- `tracing`: `tracing` crate integration, here and in `saci-core`. `inspector`
  implies it.
- `metrics`: the OpenTelemetry instruments in `src/metrics.rs`. Stands alone, so a
  library embedder can compile the writers without the HTTP control plane.
- `inspector`: the in-process telemetry: time-bounded ring buffers for spans, log
  events, metric samples and flow-control decisions, read back through the JSON
  API and the `/ui` dashboard. Implies `tracing` and `metrics`; `service` implies
  it.
- `wasm`: the wasmtime host. `WasmEngine`, `WasmPipelineRuntime` and the
  `bindgen!` host bindings.
- `plugin`: the native plugin host. `NativePluginRuntime` dlopens a shared library
  exporting the `saci-plugin-abi` C ABI, validates its manifest, and runs each
  batch through the vtable's `run_batch` slot.
- `distributed`: `PartitionSource`, `CheckpointStore`, `DistributedRunner`,
  `CheckpointStrategy` and `RedbSharedStore`, which serves both traits over a
  local redb file. This is the feature that pulls `redb`.
- `distributed-raft`: the openraft node that replicates the application state:
  `ArrowRedbLogStore`, `ArrowRedbStateMachine`, the driver and the TCP peer
  transport. `RedbSharedStore::multi_node` proposes every mutation through it
  (implies `distributed`).
- `service`: the `saci-service` binary. axum HTTP control plane, KDL config,
  metrics and the standalone runner (implies `saci-core/io`, `distributed`,
  `parquet-checkpoint`, `tracing`, `metrics` and `inspector`). It does **not**
  imply `distributed-raft`.
- `service-cluster`: cluster and raft mode on top of `service` (implies `service`
  and `distributed-raft`). A cluster node keeps its state under `node.data_dir`,
  so a cluster binary needs no other feature.
- `all`: every capability this crate carries, including all five opt-in connectors, the native
  plugin host and a Raft cluster node (`connector-postgresql`, `connector-nats`, `connector-s3`,
  `connector-kafka`, `connector-turso`, `plugin` and `service-cluster`, on top of the default
  bundle). Excludes `conformance`: it turns on `arrow-ipc/lz4` so the corpus generator can write a
  compressed record batch, which the wire format otherwise rejects, so it is the corpus
  generator's own switch rather than a capability of the service.
- `conformance`: the generator behind `packages/arrow-ipc-conformance/`. Turns on
  `arrow-ipc/lz4` so the corpus can write one compressed batch, which every codec
  must reject. Needed only by the `conformance_vectors` example.
- `mimalloc`: the mimalloc global allocator for the binary.

## Which feature adds which node

`service` registers no source, sink or format by itself. Each `connector-*`
feature adds one connector and each `transformer-*` one byte format, and both
imply `service` plus `inspector`. `mode "cluster"` needs `service-cluster` and
nothing else. Neither implies `wasm`, and `plugin` is its own feature.

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
<code>WorkflowSpec</code> carries its <code>wasm</code> and <code>plugin</code>
fields in every build, so a config declaring either node parses whichever hosts
the binary was built with, and the refusal names the missing feature instead of
the key. A <code>wasm</code> node in a
<code>--no-default-features --features service</code> build reports
<code>workflow 'orders': wasm node 'transform' needs the wasmtime processor
host, which this binary was built without, so rebuild or reinstall with
`--features wasm`</code>. <code>plugin</code> is not in the default bundle, so
the stock binary answers the same way on a <code>plugin</code> node. Of the 21
example configs in the repository, 19 declare a <code>wasm</code> node and 3
declare a <code>plugin</code> node.
</p>
</div>

```bash,name=Building a narrower binary
# Single node, WASM pipelines, CSV in and out.
cargo build --release -p saci-service --bin saci-service \
  --no-default-features --features mimalloc,connector-file,transformer-csv,wasm

# Same, plus cluster mode.
cargo build --release -p saci-service --bin saci-service \
  --no-default-features \
  --features mimalloc,service-cluster,connector-file,transformer-csv,wasm
```

Windows (PowerShell):

```powershell
# Single node, WASM pipelines, CSV in and out.
cargo build --release -p saci-service --bin saci-service `
  --no-default-features --features mimalloc,connector-file,transformer-csv,wasm

# Same, plus cluster mode.
cargo build --release -p saci-service --bin saci-service `
  --no-default-features `
  --features mimalloc,service-cluster,connector-file,transformer-csv,wasm
```

The full default build runs the same on Linux, macOS and Windows (PowerShell):

    cargo build --release -p saci-service --bin saci-service

Kafka needs `cmake` and a C toolchain on `PATH`, so add it deliberately:
`cargo install --path crates/saci-service --features connector-kafka`. Cluster
mode is `--features service-cluster`, and the native plugin host is
`--features plugin`.

## Where the traits and helpers live

| What | Items | Crate or feature |
|---|---|---|
| Traits and drains | `Source`, `Sink`, `drain_into_dataset`, `drain_dataset` | `saci-core/io` (implies `runtime`) |
| Cast helpers | `cast_batch`, `CastingSource`, `build_target_schema` | `saci-core/io` |
| Factory contract | `SourceFactory`, `SinkFactory`, `ConnectorContext`, `parse_schema_fields` | `saci-connector` |
| Transformer contract | `Transformer`, `BatchReader`, `BatchWriter`, `MessageDecoder` | `saci-transformer` |
| Config registration | `Registry`, `register_builtin_factories` | `saci-service/service` |
