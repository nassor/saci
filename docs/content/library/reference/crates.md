+++
title = "Crates and features"
description = "The saci crate, the feature that turns each module on, and saci-macros."
template = "page.html"
weight = 1
+++
# Crates and features

SACI publishes two crates. `saci` holds the engine, the connectors, the byte formats, the authoring
SDKs, the hosts and the `saci-service` binary, each behind a feature flag, so a build compiles only
the features it names. `saci-macros` holds the proc macros the processor SDK re-exports, because a
proc macro must be its own crate.

## Crates

| Crate | What it gives you |
|---|---|
| `saci` | Everything below as feature-gated modules. `use saci::prelude::*;` is the documented entry point. The default feature is `engine`. |
| `saci-macros` | `#[derive(Component)]`, `#[transform]`, `#[fold]` and `#[processor]`. A processor crate does not depend on it: `saci::processor` re-exports the macros and their expansions name `::saci::processor::...`. |

The workspace also holds crates that are not published: `saci-processor-smoketest`,
`saci-plugin-smoketest`, the crates under `examples/`, `xtask`, and `saci-service-ui`, the
dashboard, which compiles `crates/saci/src/inspector_wire.rs` into itself by path.

None of this is on crates.io yet, so depend on `saci` by path inside a clone, or by git outside
one.

## Modules and their features

Each module sits under `crates/saci/src/`. A module exists only when its feature is on.

| Module | Feature | What it gives you |
|---|---|---|
| crate root | `engine` (default) | `Dataset`, `Pipeline`, `System`, `Scheduler`, `Component`, plus the `Source` and `Sink` traits and the schema cast helpers. Arrow, serde and futures are always present; tokio and rayon arrive with `runtime`. |
| `saci::config` | `config` | The configuration language: parses KDL into `ConfigValue` (an alias for `serde_json::Value`) plus `ConfigMap`, and exports `from_kdl_str`, `from_kdl_str_with_vars`, `one_or_many`, `substitute_env_vars` and `substitute_vars`. `from_kdl_str_with_vars` substitutes and parses in one call, and a parse failure over substituted text names the variables whose values were inserted, plus any holding a backslash or double quote. Values stay out of the message. |
| `saci::transformer` | `transformer-contract` | The byte-format contract: `Transformer`, `BatchReader`, `BatchWriter`, `MessageDecoder`, `TransformerFactory`, `TransformerRegistry`. The traits live in `transformer::contract`. |
| `saci::transformer::arrow_ipc` | `transformer-arrow-ipc` | `ArrowIpcTransformer`: stream read and write plus `PerBatch` messages, both over the same Arrow IPC stream encapsulation. |
| `saci::transformer::avro` | `transformer-avro` | `AvroTransformer`: object container files plus `PerRow` messages, framed single-object or Confluent. Options `compression`, `schema_id`. |
| `saci::transformer::csv` | `transformer-csv` | `CsvTransformer`: stream read and write plus `PerRow` messages, one record per payload. Option `has_headers`. |
| `saci::transformer::ndjson` | `transformer-ndjson` | `NdjsonTransformer`: stream plus `PerRow` messages. Option `infer_max`. |
| `saci::transformer::parquet` | `transformer-parquet` | `ParquetTransformer`: stream read and write plus `PerBatch` messages, one whole file per payload. Snappy compression is fixed and the factory reads no options; the reader reports `estimated_rows` from row-group metadata. |
| `saci::connector` | `connector-contract` | The factory contract: `SourceFactory`, `SinkFactory`, `ConnectorContext`, `parse_schema_fields` and `parse_optional_schema_fields`. `connector::bridge` holds the `ChannelBridge` trait. A connector needs no dependency on the host. |
| `saci::connector::channel` | `connector-channel` | `ChannelSource`, `ChannelSink`: in-memory mpsc transport. `ChannelRegistry` is the name-keyed bridge pairing a sink in one workflow with a source in another. |
| `saci::connector::clickhouse` | `connector-clickhouse` | `ClickhouseSource`, `ClickhouseSink`: ClickHouse HTTP client with native `ArrowStream` format. Source modes `polling`, `dump` and `query`; sink write mode `append` with optional `truncate_before_first_write`. No transformer: ClickHouse sends and accepts Arrow IPC streams directly. |
| `saci::connector::file` | `connector-file` | `FileSource`, `FileSink`: local-file IO, format from a transformer. |
| `saci::connector::http` | `connector-http` | `HttpSource`, `HttpSink`: one GET spooled through a transformer, and one self-contained document per batch back out. |
| `saci::connector::kafka` | `connector-kafka` | `KafkaSource`, `KafkaSink`: `librdkafka` backed, one topic or several. |
| `saci::connector::nats` | `connector-nats` | `NatsSource`, `NatsSink`: core subject pub/sub or JetStream, chosen by a mode node's `kind` key. |
| `saci::connector::opentelemetry` | `connector-opentelemetry` | `OpenTelemetryMetricsSource`, `OpenTelemetryMetricsSink`: OTLP/HTTP metrics in and out, Protobuf or JSON, one row per data point under the ten-column canonical schema. No transformer: an OTLP request is the wire format, so the connector decodes and encodes it itself. |
| `saci::connector::postgresql` | `connector-postgresql` | `PostgresSource`, `PostgresSink`: native client, TLS, and `polling` / `cdc_trigger` / `cdc_logical` read modes. |
| `saci::connector::mariadb` | `connector-mariadb` | `MariadbSource`, `MariadbSink`: one native client for MariaDB and MySQL servers, TLS, and `polling` / `cdc_trigger` / `cdc_binlog` read modes (binlog by file position, or by GTID on MySQL, with an optional initial snapshot); writes through `append`, `upsert`, `ignore_conflicts` or `apply_changes`. |
| `saci::connector::turso` | `connector-turso` | `TursoSource`, `TursoSink`: the in-process, SQLite-compatible `turso` engine, embedded in the service's own process or a synced replica of Turso Cloud or sqld. Reads through `polling`, `dump` or `cdc`; writes through `append`, `upsert` or `ignore_conflicts` over a `deferred`, `immediate` or `concurrent` (MVCC) transaction, with optional change capture and page-level encryption. Pins `turso = "=0.7.2"`. |
| `saci::connector::redb` | `connector-redb` | `RedbSource`, `RedbSink`: an embedded redb key/value file as a transport. One entry per batch, keyed `{key_prefix}{seq:020}{key_suffix}` so key order is insertion order, in the byte format a declared transformer names. |
| `saci::connector::s3` | `connector-s3` | `S3Source`, `S3Sink`: any S3-compatible endpoint, timestamped object keys, row, byte and age flush thresholds. |
| `saci::connector::tcp` | `connector-tcp` | `TcpIngestSource`, `TcpSink`: live length-prefixed frames, decoded and encoded by a transformer. |
| `saci::connector::udp` | `connector-udp` | `UdpSource`, `UdpSink`: fire-and-forget datagrams, one message each, decoded and encoded by a transformer. The sink's optional `probe` block is what tells it whether a peer is still there. |
| `saci::connector::saci` | `connector-saci` | `SaciSource`, `SaciSink`: one SACI service pushing `RecordBatch`es to another over Arrow IPC. A session opens with a hello naming the sending service, workflow and sink node, which the receiving side puts in its own series labels and span fields. |
| `saci::processor` | `processor` | The processor SDK: the `export_pipeline!` macro, the `Component`, `transform`, `fold` and `processor` macros, and `Config`, `Error`, `Result`, `ProcessorState`, `RouteDecision`. `saci::processor::prelude` is its import set. The canonical WIT package is `crates/saci/wit/pipeline.wit`. |
| `saci::plugin` | `plugin` | The Rust plugin SDK: `export_plugin!` wires a `fn() -> Pipeline` into a cdylib exporting the C ABI in `saci::plugin_abi`. The host that `dlopen`s that cdylib is `plugin-host`, not this module. |
| `saci::plugin_abi` | `plugin` or `plugin-host` | The C ABI itself: the two symbols a plugin exports, `saci_abi_version` and `saci_plugin_v1`, the `SaciPluginV1` and `SaciHostV1` vtables they fill, and their layout. The C header is `crates/saci/include/saci_plugin.h`. |
| `saci::inspector_wire` | `metrics` | The inspector's JSON contract: `Topology`, `Snapshot`, `SpanRecord` and friends. serde only, so both the host and the browser compile it. |
| `saci::wasm_host` | `wasm-host` | The wasmtime host: `WasmEngine`, `WasmPipelineRuntime` and the `bindgen!` host bindings. |
| `saci::plugin_host` | `plugin-host` | The native plugin host: `NativePluginRuntime`. |
| `saci::distributed` | `distributed` | `PartitionSource`, `CheckpointStore`, `DistributedRunner`, `CheckpointStrategy` and `RedbSharedStore`. |
| `saci::inspector` | `inspector` | The in-process telemetry behind the `/ui` dashboard. |
| `saci::metrics` | `metrics` | The OpenTelemetry instruments. |
| `saci::service` | `service` | The host: HTTP control plane, config loading, the factory `Registry`, the runners, and `saci::service::cli`, the `saci-service` command line. It ships no `Source`, `Sink` or `Transformer` of its own. |

## Features

Library default is `engine` alone: the columnar engine with no connector, transformer, host or
authoring SDK.

- `engine` (**default**): `Dataset`, `Pipeline`, `System`, `Scheduler`, `Component`, the
  `Source`/`Sink` traits. Implies `io`, which implies `runtime`.
- `runtime`: tokio and rayon stage parallelism. A `wasm32-wasip2` build leaves it off.
- `io`: the `Source` and `Sink` traits and the schema cast helpers (implies `runtime`).
- `windows`: windowed aggregation. `WindowedSystem`, watermarks, `WindowAccumulator`. The
  `WindowSpec` geometry enum is outside the feature, so a host parses a `window` declaration in
  every build.
- `tracing`: `tracing` crate integration. Gates the events and the four nested spans
  `pipeline.run`, `pipeline.stage`, `system.execute` and `task_attempt`, the last of which opens
  only on a retry.
- `metrics`: the OpenTelemetry instruments in `src/metrics.rs` and the `inspector_wire` types.
  Stands alone, so a library embedder can compile the writers without the HTTP control plane.
- `trace-context`: trace context carried across a `connector-saci` link (implies `tracing`).
- `processor`: the wasm processor SDK. Sequential-only execution driven by the `pollster` sync
  executor. A processor targets `wasm32-wasip2` and must not pull tokio in, so build with
  `--no-default-features --features processor`.
- `plugin`: the native plugin SDK (implies `runtime`).
- `connector-channel`, `connector-clickhouse`, `connector-file`, `connector-http`,
  `connector-kafka`, `connector-mariadb`, `connector-nats`, `connector-opentelemetry`,
  `connector-postgresql`, `connector-redb`, `connector-s3`, `connector-saci`, `connector-tcp`,
  `connector-turso`, `connector-udp`: one per connector module. `connector-kafka`,
  `connector-nats` and `connector-udp` imply `transformer-ndjson`, and `connector-tcp` implies
  `transformer-arrow-ipc`, so the connector feature alone is runnable. `connector-file`,
  `connector-http`, `connector-redb` and `connector-s3` imply no transformer, so the config picks
  which formats the build carries. `connector-saci` implies none either: Arrow IPC is its fixed
  wire format and a `saci` node takes no `transformer` key. TLS is always compiled into
  `connector-postgresql`, `connector-mariadb` and `connector-clickhouse`.
- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet`: one per byte format.
- `connectors`: the seven connectors that need nothing installed or already running:
  `connector-channel`, `connector-file`, `connector-redb`, `connector-http`, `connector-tcp`,
  `connector-udp` and `connector-saci`.
- `transformers`: all five `transformer-*` features.
- `config`, `transformer-contract`, `connector-contract`: the contracts. The features above turn
  them on, so a build rarely names them.
- `inspector`: the in-process telemetry: time-bounded ring buffers for spans, log events, metric
  samples and flow-control decisions, read back through the JSON API and the `/ui` dashboard.
  Implies `tracing` and `metrics`.
- `distributed`: `PartitionSource`, `CheckpointStore`, `DistributedRunner`, `CheckpointStrategy`
  and `RedbSharedStore`, which serves both traits over a local redb file. This is the feature that
  pulls `redb`.
- `distributed-raft`: the openraft node that replicates the application state:
  `ArrowRedbLogStore`, `ArrowRedbStateMachine`, the driver and the TCP peer transport.
  `RedbSharedStore::multi_node` proposes every mutation through it (implies `distributed`).
- `parquet-checkpoint`: `ParquetCheckpointStore`, the archival checkpoint store (implies
  `distributed`).
- `wasm-host`: the wasmtime host, `saci::wasm_host`.
- `plugin-host`: the native plugin host, `saci::plugin_host`. `NativePluginRuntime` dlopens a
  shared library exporting the plugin C ABI, validates its manifest, and runs each batch through
  the vtable's `run_batch` slot.
- `service`: the `saci-service` binary and `saci::service`. axum HTTP control plane, KDL config,
  metrics and the standalone runner (implies `connector-contract`, `distributed`,
  `parquet-checkpoint`, `tracing`, `metrics`, `trace-context` and `inspector`). It does **not**
  imply `distributed-raft`.
- `service-cluster`: cluster and raft mode on top of `service` (implies `service` and
  `distributed-raft`). A cluster node keeps its state under `node.data_dir`, so a cluster binary
  needs no other feature.
- `mimalloc`: the mimalloc global allocator for the binary.
- `server`: what `cargo install saci --features server` builds, a runnable `saci-service` with no
  further flags: `mimalloc`, `service`, `wasm-host`, `windows`, `parquet-checkpoint`, the seven
  `connectors` and all five transformers. A connector joins it only when nothing has to be
  installed or already running for it to work and it needs no extra build toolchain. Every
  connector that needs a database, broker, object store or OTLP endpoint is opt-in instead:
  `connector-postgresql`, `connector-mariadb`, `connector-nats`, `connector-s3`,
  `connector-kafka`, `connector-turso`, `connector-clickhouse` and `connector-opentelemetry`.
  `connector-kafka` needs `cmake` plus a C toolchain because `librdkafka-sys` builds vendored C,
  `connector-turso` pulls hyper and rustls for its synced mode, and `service-cluster` is a
  deliberate deployment choice.
- `all`: every capability: `engine`, `windows`, `processor`, `plugin`, `server`,
  `service-cluster`, `plugin-host`, `connectors`, `transformers` and the eight opt-in
  connectors. Excludes `conformance`.
- `conformance`: the generator behind `packages/arrow-ipc-conformance/`. Turns on
  `arrow-ipc/lz4` so the corpus can write one compressed batch, which every codec must reject.
  Needed only by the `conformance_vectors` example, and kept out of `all` because it is the corpus
  generator's own switch rather than a capability of the service.

`engine`, `processor` and `plugin` share the engine's types, so every name they share is one
re-export, not three with a precedence rule between them. The three author different artifacts and
are not meant to be combined in one build, but doing so is redundant, not ambiguous.

## Which feature adds which node

`service` registers no source, sink or format by itself. With `service` on, each `connector-*`
feature registers one connector's factories and each `transformer-*` one byte format.
Without `service`, those features give the module and nothing else. `mode "cluster"` needs
`service-cluster` and nothing else. Neither `service` nor `connector-*` implies `wasm-host`, and
`plugin-host` is its own feature.

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
`--features wasm-host`</code>. <code>plugin-host</code> is not in <code>server</code>, so
the stock binary answers the same way on a <code>plugin</code> node. Of the 21
example configs in the repository, 19 declare a <code>wasm</code> node and 3
declare a <code>plugin</code> node.
</p>
</div>

```bash,name=Building a narrower binary
# Single node, WASM pipelines, CSV in and out.
cargo build --release -p saci --bin saci-service \
  --no-default-features --features mimalloc,service,connector-file,transformer-csv,wasm-host

# Same, plus cluster mode.
cargo build --release -p saci --bin saci-service \
  --no-default-features \
  --features mimalloc,service-cluster,connector-file,transformer-csv,wasm-host
```

Windows (PowerShell):

```powershell
# Single node, WASM pipelines, CSV in and out.
cargo build --release -p saci --bin saci-service `
  --no-default-features --features mimalloc,service,connector-file,transformer-csv,wasm-host

# Same, plus cluster mode.
cargo build --release -p saci --bin saci-service `
  --no-default-features `
  --features mimalloc,service-cluster,connector-file,transformer-csv,wasm-host
```

From a checkout, `cargo xtask server` builds the same binary from short names. It always turns
on `service`, checks every name against the crate's feature table before cargo starts, and copies
the release binary to `target/server/saci-service` (`--out=PATH` picks another place):

```bash,name=The first build above, through the task runner
cargo xtask server --connectors=file --transformers=csv --processors=wasm --features=mimalloc
```

`--processors` takes `wasm` and `plugin`, `--features` takes any other feature such as
`windows` or `service-cluster`, and `cargo xtask server --list` prints every accepted name.
The command runs the same on Linux, macOS and Windows (PowerShell).

The full `server` build runs the same on Linux, macOS and Windows (PowerShell):

    cargo build --release -p saci --bin saci-service --features server

Kafka needs `cmake` and a C toolchain on `PATH`, so add it deliberately:
`cargo install --path crates/saci --features server,connector-kafka`. Cluster
mode is `--features server,service-cluster`, and the native plugin host is
`--features server,plugin-host`.

## A custom server

A binary crate that depends on `saci` with the server features it needs is a complete server. The
`serve` and `validate` subcommands run exactly as in `saci-service`.

```toml
[dependencies]
saci = { git = "https://github.com/nassor/saci", default-features = false, features = ["service", "connector-file", "transformer-csv"] }
```

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main()
}
```

`saci::service::cli::main_with` takes a closure over the `ServiceBuilder` that `serve` and
`validate` use. It runs after the built-in factories, so a user connector registers beside them:

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main_with(|builder| builder.register_source(MyFactory))
}
```

## Depending on saci

An embedder names the features it uses and nothing else:

```toml
[dependencies]
saci = { git = "https://github.com/nassor/saci", features = ["connector-file", "transformer-csv"] }
```

A wasm processor crate depends on the SDK feature alone, with the default features off, and gets
the derive and attribute macros from `saci::processor`:

```toml
[dependencies]
saci = { git = "https://github.com/nassor/saci", default-features = false, features = ["processor"] }
```

Inside this workspace the dependency is `saci = { workspace = true, features = ["processor"] }`.
Add `windows` for windowed aggregation, or use `plugin` for a native plugin.

## Where the traits and helpers live

| What | Items | Module | Feature |
|---|---|---|---|
| Traits and drains | `Source`, `Sink`, `drain_into_dataset`, `drain_dataset` | crate root | `io` (implies `runtime`) |
| Cast helpers | `cast_batch`, `CastingSource`, `build_target_schema` | crate root | `io` |
| Factory contract | `SourceFactory`, `SinkFactory`, `ConnectorContext`, `parse_schema_fields` | `saci::connector` | `connector-contract` |
| Transformer contract | `Transformer`, `BatchReader`, `BatchWriter`, `MessageDecoder` | `saci::transformer` | `transformer-contract` |
| Config registration | `Registry`, `register_builtin_factories` | `saci::service` | `service` |
