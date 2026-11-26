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
| `saci-macros` | `#[derive(Component)]`, `#[transform]`, `#[fold]`, `#[processor]` and `#[plugin]`. An authoring crate does not depend on it: `saci` re-exports the macros at its root and in `saci::prelude`, and their expansions name `::saci::__private::...`. |

The workspace also holds crates that are not published: `saci-processor-smoketest`,
`saci-plugin-smoketest`, the crates under `examples/`, `xtask`, and `saci-service-ui`, the
dashboard, which compiles `crates/saci/src/inspector_wire.rs` into itself by path.

Depend on `saci` with `saci = "0.1"`, or by path inside a clone of the repository.

## Modules and their features

Each module sits under `crates/saci/src/`. A module exists only when its feature is on.

| Module | Feature | What it gives you |
|---|---|---|
| crate root | `engine` (default) | `Dataset`, `Pipeline`, `System`, `Scheduler`, `Component`, plus the `Source` and `Sink` traits; the rest of the IO surface (`read_all`, `write_all`, `cast_batch`, the retry wrappers) is in `saci::io`. Arrow and serde are always present; tokio and rayon arrive with `engine`, and `futures` with `service` or `connector-udp`. |
| `saci::config` | `config` | The configuration language: parses KDL into `ConfigValue` (an alias for `serde_json::Value`) plus `ConfigMap`, and exports `from_kdl_str`, `from_kdl_str_with_vars`, `one_or_many`, `substitute_env_vars` and `substitute_vars`. `from_kdl_str_with_vars` substitutes and parses in one call, and a parse failure over substituted text names the variables whose values were inserted, plus any holding a backslash or double quote. Values stay out of the message. |
| `saci::codec` | `codec-api` | The byte-format contract: `Codec`, `BatchReader`, `BatchWriter`, `MessageDecoder`, `MessageShape`, `CodecFactory`, `CodecRegistry` and `ReadInput`. The traits live in `codec::contract`. |
| `saci::codec::arrow_ipc` | `codec-arrow-ipc` | `ArrowIpcCodec`: stream read and write plus `PerBatch` messages, both over the same Arrow IPC stream encapsulation. Takes no config keys. |
| `saci::codec::avro` | `codec-avro` | `AvroCodec`: object container files plus `PerRow` messages, framed single-object or Confluent. Config keys `compression` (`AvroCompression`), `schema_id`. |
| `saci::codec::csv` | `codec-csv` | `CsvCodec`: stream read and write plus `PerRow` messages, one record per payload. Config key `has_headers`. |
| `saci::codec::ndjson` | `codec-ndjson` | `NdjsonCodec`: stream plus `PerRow` messages. Config key `infer_max`. |
| `saci::codec::parquet` | `codec-parquet` | `ParquetCodec`: stream read and write plus `PerBatch` messages, one whole file per payload. Snappy compression is fixed and the factory reads no config keys; the reader reports `estimated_rows` from row-group metadata. |
| `saci::connector` | `connector-api` | The connector contract: `Connector`, `Role`, `ConnectorContext`, `parse_fields` and `parse_optional_fields`. `connector::bridge` holds the `ChannelBridge` trait. A connector needs no dependency on the host. |
| `saci::connector::channel` | `connector-channel` | `ChannelSource`, `ChannelSink`: in-memory mpsc transport. `ChannelRegistry` is the name-keyed bridge pairing a sink in one workflow with a source in another. |
| `saci::connector::clickhouse` | `connector-clickhouse` | `ClickhouseSource`, `ClickhouseSink`: ClickHouse HTTP client with native `ArrowStream` format. Source modes `polling`, `dump` and `query`; sink write mode `append` with optional `truncate_before_first_write`. No codec: ClickHouse sends and accepts Arrow IPC streams directly. |
| `saci::connector::file` | `connector-file` | `FileSource`, `FileSink`: local-file IO, format from a codec. |
| `saci::connector::http` | `connector-http` | `HttpSource`, `HttpSink`: one GET spooled through a codec, and one self-contained document per batch back out. |
| `saci::connector::kafka` | `connector-kafka` | `KafkaSource`, `KafkaSink`: `librdkafka` backed, one topic or several. |
| `saci::connector::nats` | `connector-nats` | `NatsSource`, `NatsSink`: core subject pub/sub or JetStream, chosen by a mode node's `kind` key. |
| `saci::connector::otlp` | `connector-otlp` | `OtlpMetricsSource`, `OtlpMetricsSink`: OTLP/HTTP metrics in and out, Protobuf or JSON, one row per data point under the ten-column canonical schema. No codec: an OTLP request is the wire format, so the connector decodes and encodes it itself. |
| `saci::connector::postgresql` | `connector-postgresql` | `PostgresSource`, `PostgresSink`: native client, TLS, and `polling` / `cdc_trigger` / `cdc_logical` read modes. |
| `saci::connector::mariadb` | `connector-mariadb` | `MariadbSource`, `MariadbSink`: one native client for MariaDB and MySQL servers, TLS, and `polling` / `cdc_trigger` / `cdc_binlog` read modes (binlog by file position, or by GTID on MySQL, with an optional initial snapshot); writes through `append`, `upsert`, `ignore_conflicts` or `apply_changes`. |
| `saci::connector::turso` | `connector-turso` | `TursoSource`, `TursoSink`: the in-process, SQLite-compatible `turso` engine, embedded in the service's own process or a synced replica of Turso Cloud or sqld. Reads through `polling`, `dump` or `cdc`; writes through `append`, `upsert` or `ignore_conflicts` over a `deferred`, `immediate` or `concurrent` (MVCC) transaction, with optional change capture and page-level encryption. Pins `turso = "=0.7.2"`. |
| `saci::connector::redb` | `connector-redb` | `RedbSource`, `RedbSink`: an embedded redb key/value file as a transport. One entry per batch, keyed `{key_prefix}{seq:020}{key_suffix}` so key order is insertion order, in the byte format a declared codec names. |
| `saci::connector::s3` | `connector-s3` | `S3Source`, `S3Sink`: any S3-compatible endpoint, timestamped object keys, row, byte and age flush thresholds. |
| `saci::connector::tcp` | `connector-tcp` | `TcpSource`, `TcpSink`: live length-prefixed frames, decoded and encoded by a codec. |
| `saci::connector::udp` | `connector-udp` | `UdpSource`, `UdpSink`: fire-and-forget datagrams, one message each, decoded and encoded by a codec. The sink's optional `probe` block is what tells it whether a peer is still there. |
| `saci::connector::peer` | `connector-peer` | `PeerSource`, `PeerSink`: one SACI service pushing `RecordBatch`es to another over Arrow IPC. A session opens with a hello naming the sending service, workflow and sink node, which the receiving side puts in its own series labels and span fields. |
| `saci::sdk` | always | The authoring module shared by wasm processors and native plugins: `sdk::State`, `sdk::RouteDecision`, `sdk::Config`, `sdk::Error`, `sdk::Result` and `sdk::host` (`config`, `config_parse`, `trace` to `error`, `metric`, `with_config`). `#[transform]` and `#[fold]` need `derive`, `#[processor]` needs `processor` and `#[plugin]` needs `plugin`; all four are in `saci::prelude`. The canonical WIT package is `crates/saci/wit/pipeline.wit`. |
| `saci::plugin_abi` | `plugin` or `plugin-host` | The C ABI itself: the two symbols a plugin exports, `saci_abi_version` and `saci_plugin_v1`, the `SaciPluginV1` and `SaciHostV1` vtables they fill, and their layout. The C header is `crates/saci/include/saci_plugin.h`. |
| `saci::inspector_wire` | `metrics` | The inspector's JSON contract: `Topology`, `Snapshot`, `SpanRecord` and friends. serde only, so both the host and the browser compile it. |
| `saci::wasm_host` | `wasm-host` | The wasmtime host: `WasmEngine`, `WasmPipelineRuntime` and the `bindgen!` host bindings. |
| `saci::plugin_host` | `plugin-host` | The native plugin host: `NativePluginRuntime`. |
| `saci::distributed` | `distributed` | `PartitionSource`, `CheckpointStore`, `DistributedRunner`, `CheckpointStrategy` and `RedbSharedStore`. |
| `saci::inspector` | `inspector` | The in-process telemetry behind the `/ui` dashboard. |
| `saci::metrics` | `metrics` | The OpenTelemetry instruments. |
| `saci::tls` | `tls` | Shared `rustls` (`ring` provider) client and server TLS / mutual-TLS configuration loaders (`ClientTlsConfig`, `ServerTlsConfig`). |
| `saci::crypto` | `crypto` | Whole-payload and field-level AEAD encryption (`Cipher`, `Algorithm`, `KeyMaterial`, `StaticKeyProvider`, `EncryptedCodec`, `EncryptingSink`, `DecryptingSource`). |
| `saci::service` | `service` | The host: HTTP control plane, config loading, the connector `Registry`, the runners, and `saci::service::cli`, the `saci-service` command line. It ships no `Source`, `Sink` or `Codec` of its own. |

## Features

Library default is `engine` alone: the columnar engine with no connector, codec, host or
authoring SDK.

- `engine` (**default**): `Dataset`, `Pipeline`, `System`, `Scheduler`, `Component`, the
  `Source`/`Sink` traits and the schema cast helpers, with tokio and rayon stage parallelism.
  Implies `derive`. A `wasm32-wasip3` build leaves it off.
- `derive`: `#[derive(Component)]`, `#[transform]` and `#[fold]`. It needs no tokio, so a
  `wasm32-wasip3` build carries it.
- `windows`: windowed aggregation. `WindowedSystem`, watermarks, `WindowAccumulator`. The
  `WindowSpec` geometry enum lives in `saci::window`, outside the feature, so a host parses a
  `window` declaration in every build.
- `tracing`: `tracing` crate integration. Gates the events and the four nested spans
  `pipeline.run`, `pipeline.stage`, `system.execute` and `task_attempt`, the last of which opens
  only on a retry.
- `metrics`: the OpenTelemetry instruments in `src/metrics.rs` and the `inspector_wire` types.
  Stands alone, so a library embedder can compile the writers without the HTTP control plane.
- `trace-context`: trace context carried across a `connector-peer` link (implies `tracing`).
- `tls`: shared `rustls` (`ring` provider) client and server TLS/mTLS loaders in `saci::tls`.
- `crypto`: AES-256-GCM and ChaCha20-Poly1305 AEAD envelopes, `EncryptedCodec`, field/payload batch transforms, and `EncryptingSink` / `DecryptingSource` in `saci::crypto` (kept out of `processor`).
- `processor`: the wasm processor SDK targeting WASI 0.3 (`wasm32-wasip3`) and Component Model
  Async. Build with `--no-default-features --features processor` so tokio stays out.
- `plugin`: the native plugin SDK (implies `engine`).
- `connector-channel`, `connector-clickhouse`, `connector-file`, `connector-http`,
  `connector-kafka`, `connector-mariadb`, `connector-nats`, `connector-otlp`,
  `connector-peer`, `connector-postgresql`, `connector-redb`, `connector-s3`, `connector-tcp`,
  `connector-turso`, `connector-udp`: one per connector module. `connector-kafka`,
  `connector-nats` and `connector-udp` imply `codec-ndjson`, and `connector-tcp` implies
  `codec-arrow-ipc`, so the connector feature alone is runnable. `connector-file`,
  `connector-http`, `connector-redb` and `connector-s3` imply no codec, so the config picks
  which formats the build carries. `connector-peer` implies none either: Arrow IPC is its fixed
  wire format and a `peer` node takes no `codec` key. TLS is always compiled into
  `connector-postgresql`, `connector-mariadb` and `connector-clickhouse`.
- `codec-arrow-ipc`, `codec-avro`, `codec-csv`, `codec-ndjson`,
  `codec-parquet`: one per byte format.
- `connectors`: the seven connectors that need nothing installed or already running:
  `connector-channel`, `connector-file`, `connector-redb`, `connector-http`, `connector-tcp`,
  `connector-udp` and `connector-peer`.
- `codecs`: all five `codec-*` features.
- `config`, `codec-api`, `connector-api`: the contracts. The features above turn
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
  metrics and the standalone runner (implies `connector-api`, `distributed`,
  `parquet-checkpoint`, `tracing`, `metrics`, `trace-context` and `inspector`). It does **not**
  imply `distributed-raft`.
- `service-cluster`: cluster and raft mode on top of `service` (implies `service` and
  `distributed-raft`). A cluster node keeps its state under `node.data_dir`, so a cluster binary
  needs no other feature.
- `mimalloc`: the mimalloc global allocator for the binary.
- `server`: what `cargo install saci --features server` builds, a runnable `saci-service` with no
  further flags: `mimalloc`, `service`, `wasm-host`, `windows`, `parquet-checkpoint`, `crypto`,
  the seven `connectors` and all five codecs. A connector joins it only when nothing has to be
  installed or already running for it to work and it needs no extra build toolchain. Every
  connector that needs a database, broker, object store or OTLP endpoint is opt-in instead:
  `connector-postgresql`, `connector-mariadb`, `connector-nats`, `connector-s3`,
  `connector-kafka`, `connector-turso`, `connector-clickhouse` and `connector-otlp`.
  `connector-kafka` needs `cmake` plus a C toolchain because `librdkafka-sys` builds vendored C,
  `connector-turso` pulls hyper and rustls for its synced mode, and `service-cluster` is a
  deliberate deployment choice.
- `all`: every capability: `engine`, `windows`, `processor`, `plugin`, `server`,
  `service-cluster`, `plugin-host`, `connectors`, `codecs` and the eight opt-in
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
feature registers one connector and each `codec-*` one byte format.
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
  --no-default-features --features mimalloc,service,connector-file,codec-csv,wasm-host

# Same, plus cluster mode.
cargo build --release -p saci --bin saci-service \
  --no-default-features \
  --features mimalloc,service-cluster,connector-file,codec-csv,wasm-host
```

Windows (PowerShell):

```powershell
# Single node, WASM pipelines, CSV in and out.
cargo build --release -p saci --bin saci-service `
  --no-default-features --features mimalloc,service,connector-file,codec-csv,wasm-host

# Same, plus cluster mode.
cargo build --release -p saci --bin saci-service `
  --no-default-features `
  --features mimalloc,service-cluster,connector-file,codec-csv,wasm-host
```

From a checkout, `cargo xtask server` builds the same binary from short names. It always turns
on `service`, checks every name against the crate's feature table before cargo starts, and copies
the release binary to `target/server/saci-service` (`--out=PATH` picks another place):

```bash,name=The first build above, through the task runner
cargo xtask server --connectors=file --codecs=csv --processors=wasm --features=mimalloc
```

`--processors` takes `wasm` and `plugin`, `--features` takes any other feature such as
`windows` or `service-cluster`, and `cargo xtask server --list` prints every accepted name.
The command runs the same on Linux, macOS and Windows (PowerShell).

The full `server` build runs the same on Linux, macOS and Windows (PowerShell):

    cargo build --release -p saci --bin saci-service --features server

Kafka needs `cmake` and a C toolchain on `PATH`, so add it deliberately:
`cargo install saci --features server,connector-kafka`. Cluster
mode is `--features server,service-cluster`, and the native plugin host is
`--features server,plugin-host`.

## A custom server

A binary crate that depends on `saci` with the server features it needs is a complete server. The
`serve` and `validate` subcommands run exactly as in `saci-service`.

```toml
[dependencies]
saci = { version = "0.1", default-features = false, features = ["service", "connector-file", "codec-csv"] }
```

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main()
}
```

`saci::service::cli::main_with` takes a closure over the `ServiceBuilder` that `serve` and
`validate` use. It runs after the built-in connectors and codecs, so a user connector registers beside them:

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main_with(|builder| builder.connector(MyConnector))
}
```

## Depending on saci

An embedder names the features it uses and nothing else:

```toml
[dependencies]
saci = { version = "0.1", features = ["connector-file", "codec-csv"] }
```

A wasm processor crate depends on the SDK feature alone, with the default features off, and gets
the derive and attribute macros from `saci::prelude`:

```toml
[dependencies]
saci = { version = "0.1", default-features = false, features = ["processor"] }
```

Inside this workspace the dependency is `saci = { workspace = true, features = ["processor"] }`.
Add `windows` for windowed aggregation, or use `plugin` for a native plugin.

## Where the traits and helpers live

| What | Items | Module | Feature |
|---|---|---|---|
| Traits and helpers | `Source`, `Sink`, `RetryingSource`, `RetryingSink`, `read_all`, `write_all` | `saci::io` | `engine` |
| Cast helper | `cast_batch` | `saci::io` | `engine` |
| Connector contract | `Connector`, `Role`, `ConnectorContext`, `parse_fields` | `saci::connector` | `connector-api` |
| Codec contract | `Codec`, `BatchReader`, `BatchWriter`, `MessageDecoder` | `saci::codec` | `codec-api` |
| Transport TLS | `ClientTlsConfig`, `ServerTlsConfig`, `load_client_config`, `load_server_config` | `saci::tls` | `tls` |
| Payload & field AEAD | `Cipher`, `Algorithm`, `StaticKeyProvider`, `EncryptedCodec`, `EncryptingSink`, `DecryptingSource` | `saci::crypto` | `crypto` |
| Config registration | `Registry`, `ServiceBuilder`, `BuiltWorkflow`, `RunOptions`, `serve` | `saci::service` | `service` |
