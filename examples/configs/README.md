# SACI service example configs

Runnable KDL configurations for the `saci-service` binary: one per connector, plus standalone and
cluster templates. Each file is a `--config` argument to `saci-service validate` or
`saci-service serve`. Run every command from the repository root.

## Features each config needs

Most configs declare a `wasm` node in their `workflow`, and that node needs the `wasm-host`
feature. `service` does not imply it, so a binary built with `--features service` alone refuses
those files:

```text
workflow 'orders': wasm node 'transform' needs the wasmtime processor host, which this binary was built without, so rebuild or reinstall with `--features wasm-host`
```

The file still parses; the refusal names the missing flag. `standalone_plugin.kdl` declares a
`plugin` node instead, which needs `plugin-host`. The `server` feature does not carry it.
`redb.kdl`, `redb_connector.kdl`, `dlq.kdl` and `opentelemetry_metrics.kdl` declare no processor
node.

Each `type="..."` also needs the feature that registers its factory. `server` carries `service`,
`wasm-host` and the seven self-contained connectors (`connector-channel`, `connector-file`,
`connector-http`, `connector-tcp`, `connector-udp`, `connector-saci`, `connector-redb`) with the
five transformers. The table lists what to enable beside it:

| Config | Features |
|--------|----------|
| `standalone.kdl`, `standalone_wasm.kdl`, `standalone_polyglot.kdl` | `connector-file,transformer-csv,wasm-host` |
| `extension_example.kdl` | a custom binary: no feature registers `MongoSource` or `ClickHouseSink`, and `wasm-host` is all `validate` needs to show the warning |
| `standalone_plugin.kdl` | `connector-file,transformer-csv,plugin-host` |
| `postgresql.kdl` | `connector-postgresql,wasm-host` |
| `mariadb.kdl` | `connector-mariadb,wasm-host` |
| `mariadb_trigger.kdl` | `connector-mariadb,wasm-host` |
| `turso.kdl` | `connector-turso,wasm-host` |
| `clickhouse.kdl` | `connector-clickhouse,wasm-host` |
| `s3.kdl` | `connector-s3,transformer-csv,wasm-host` |
| `cluster.kdl` | `service-cluster,wasm-host`; it declares no source, sink or transformer |
| `kafka.kdl` | `connector-kafka,wasm-host` |
| `nats.kdl` | `connector-nats,wasm-host` |
| `tcp.kdl` | `connector-tcp,wasm-host` |
| `udp.kdl` | `connector-udp,wasm-host` |
| `saci.kdl` | `connector-saci,wasm-host` |
| `opentelemetry_metrics.kdl` | `connector-opentelemetry`; it declares no processor node |
| `http.kdl` | `connector-http,transformer-csv,wasm-host` |
| `http_push.kdl` | `connector-http,transformer-ndjson,wasm-host` |
| `redb.kdl` | `connector-file,transformer-csv` |
| `redb_connector.kdl` | `connector-file,connector-redb,transformer-csv` |
| `dlq.kdl` | `server`, which carries `connector-file,connector-http,connector-redb,transformer-csv,transformer-ndjson` |

## Build the binary

Build with `server` plus the features of the config you plan to run:

```text
cargo build -p saci --features server,connector-file,transformer-csv,wasm-host --bin saci-service
```

Runs the same on Linux, macOS and Windows (PowerShell).

`--features all` covers every config here that the stock binary can run: the eight opt-in
connectors, the plugin host and cluster mode. It needs `cmake` and a C toolchain on `PATH` for
Kafka's vendored librdkafka.

## Run the standalone example

`standalone_wasm.kdl` reads `fixtures/order_processing_input.csv`, runs the Rust processor from
`examples/wasm/order_processing/`, and writes `/tmp/saci-order-processing-out.csv`. Its
`run_mode` is `one_shot`, so the process exits after one iteration. The sink opens its file when
the pipeline is built, so the output directory must exist first.

1. Build the processor component. Runs the same on Linux, macOS and Windows (PowerShell):

   ```text
   cargo build --release -p order-processing-wasm --target wasm32-wasip2
   ```

   It writes `target/wasm32-wasip2/release/order_processing_wasm.wasm`, the module the config names.

2. On Windows, create the output directory. `/tmp` in the config resolves to `\tmp` on the current
   drive. Linux and macOS already have `/tmp`.

   ```powershell
   New-Item -ItemType Directory -Force C:\tmp | Out-Null
   ```

3. Validate the config. It has no side effects and exits 0 on success. Runs the same on Linux,
   macOS and Windows (PowerShell):

   ```text
   cargo run -p saci --features server,connector-file,transformer-csv,wasm-host --bin saci-service -- validate --config examples/configs/standalone_wasm.kdl --strict
   ```

4. Run the pipeline. Runs the same on Linux, macOS and Windows (PowerShell):

   ```text
   cargo run -p saci --features server,connector-file,transformer-csv,wasm-host --bin saci-service -- serve --config examples/configs/standalone_wasm.kdl
   ```

5. Read the output. The processor has filled in `valid` and `usd_amount` on every row.

   Linux/macOS:

   ```bash
   cat /tmp/saci-order-processing-out.csv
   ```

   Windows (PowerShell):

   ```powershell
   Get-Content C:\tmp\saci-order-processing-out.csv
   ```

`cargo xtask demo standalone_wasm` runs the same config with a generated `variables` block, and
`cargo xtask validate` checks every runnable config in this repository.

`standalone.kdl` is the same shape with a component named `pipelines/orders.wasm`, a path you
supply. Without that file, `validate --connectors-only` still checks its source, sink and
transformer:

```text
cargo run -p saci --features server,connector-file,transformer-csv,wasm-host --bin saci-service -- validate --config examples/configs/standalone.kdl --connectors-only
```

Runs the same on Linux, macOS and Windows (PowerShell).

## Validate the cluster example

Cluster mode needs `--features service-cluster`. It takes no `store` block: its application state
is the raft-replicated `cluster-app.redb` under `node.data_dir`, and a `store` block there is a
configuration error. The config declares a `wasm` node, so `wasm-host` comes with it.

Linux/macOS:

```bash
SACI_NODE_ID=1 SACI_DATA_DIR=/tmp/saci-node-1 \
cargo run -p saci --features server,service-cluster,wasm-host --bin saci-service -- validate --config examples/configs/cluster.kdl --strict
```

Windows (PowerShell):

```powershell
$env:SACI_NODE_ID = "1"
$env:SACI_DATA_DIR = "C:\tmp\saci-node-1"
cargo run -p saci --features server,service-cluster,wasm-host --bin saci-service -- validate --config examples/configs/cluster.kdl --strict
```

A three-node cluster needs three processes, each with a distinct `SACI_NODE_ID` and
`SACI_DATA_DIR`, and `SACI_BOOTSTRAP=true` on exactly one node during the first bring-up. The
comments in `cluster.kdl` give the commands for each node.

## Run the redb-backed config

`redb.kdl` needs no external service: the store is one local file at `SACI_STATE_DB`, default
`/tmp/saci-state.redb`. With the `store` block present, `serve` saves the raw config file to it
before the pipeline builds, and writes stream-mode source cursors and processor priors back as
items flow. A restarted service resumes from its last save point.

The pipeline reads `fixtures/orders.csv` and writes `/tmp/saci-redb-orders-out.csv`. On Windows,
create `C:\tmp` first as in the standalone steps. Runs the same on Linux, macOS and Windows
(PowerShell):

```text
cargo run -p saci --features server,connector-file,transformer-csv --bin saci-service -- validate --config examples/configs/redb.kdl
cargo run -p saci --features server,connector-file,transformer-csv --bin saci-service -- serve --config examples/configs/redb.kdl
```

## Built-in factories

Each factory lives in a module of the `saci` crate and registers only when its feature and
`service` are both on. `service` alone registers nothing.

A connector moves bytes and a transformer decides what they mean. A config that names a file
therefore needs both: a `source` or `sink` with `type="FileSource"` or `type="FileSink"` from
`connector-file`, and a `transformer "..." format="csv"` node from `transformer-csv` that the
source or sink references by id through its own `transformer="..."` property. A byte-carrying
source or sink always names its `transformer`: there is no implicit default format.

### Sources

| config `type` | Description | Required config keys | Feature |
|---------------|-------------|----------------------|---------|
| `FileSource` | Reads a local file in whatever format its `transformer` names | `path` | `connector-file` |
| `RedbSource` | Reads the entries of one redb table in key order, in whatever format its `transformer` names | `directory`, `schema_fields` | `connector-redb` |
| `HttpSource` | One GET, or a body that never ends, decoded in whatever format its `transformer` names | `url`, plus `schema_fields` where the format needs it | `connector-http` |
| `HttpPushSource` | Documents a client pushes in, one whole document per request, stream mode only | `bind`, `schema_fields` | `connector-http` |
| `PostgresSource` | Polling, outbox or `pgoutput` reads | `name`, `connection`, `mode`, `schema_fields` (optionally `pg_type` per field) | `connector-postgresql` |
| `MariadbSource` | Polling, a trigger changelog (an operator outbox or installed triggers) or row-binlog reads on MariaDB and MySQL, with an optional initial snapshot | `name`, `connection`, `mode`, `schema_fields` | `connector-mariadb` |
| `TursoSource` | Embedded or synced reads: cursor polling, full dump, or the change table | `name`, `connection`, `mode`, `schema_fields` | `connector-turso` |
| `ClickhouseSource` | Polling, dump or custom SELECT query | `name`, `connection`, `mode`, `schema_fields` | `connector-clickhouse` |
| `KafkaSource` | Consumes a Kafka topic | `brokers`, `topic`, `schema_fields` | `connector-kafka` |
| `S3Source` | Lists a prefix once and drains every object in key order | `connection`, `schema_fields` | `connector-s3` |
| `tcp` | Live framed messages off a listener, stream mode only | `bind`, `schema_fields` | `connector-tcp` |
| `udp` | Live datagrams off a bound socket, one message each, stream mode only | `bind`, `schema_fields` | `connector-udp` |
| `saci` | Batches pushed by a `saci` sink in another service, stream mode only | `bind`, `schema_fields` | `connector-saci` |
| `OpenTelemetryMetricsSource` | Listens for OTLP/HTTP metrics, or reads a url, in Protobuf or JSON; listening is stream mode only | `bind`, `schema_fields` for a projection other than the canonical ten columns | `connector-opentelemetry` |
| `ChannelSource` | In-process channel (testing/internal) | `schema_fields` | `connector-channel` |

### Sinks

| config `type` | Description | Required config keys | Feature |
|---------------|-------------|----------------------|---------|
| `FileSink` | Writes a local file in whatever format its `transformer` names | `path`, `schema_fields`, optional `truncate` | `connector-file` |
| `RedbSink` | Stores one redb entry per batch, in whatever format its `transformer` names | `directory`, `schema_fields` | `connector-redb` |
| `HttpSink` | One request per batch, body written by its `transformer` | `url`, `schema_fields`, optional `method` | `connector-http` |
| `PostgresSink` | `COPY FORMAT binary`, optional upsert | `name`, `connection`, `table`, `schema_fields` (optionally `pg_type` per field) | `connector-postgresql` |
| `MariadbSink` | Batched `INSERT` with `append`, `upsert`, `ignore_conflicts` or `apply_changes` | `name`, `connection`, `table`, `schema_fields` | `connector-mariadb` |
| `TursoSink` | Prepared `INSERT` with optional `ON CONFLICT` upsert, `deferred`/`immediate`/`concurrent` transactions, change capture and encryption | `name`, `connection`, `table`, `write_mode`, `schema_fields` | `connector-turso` |
| `ClickhouseSink` | Bulk load via Arrow IPC streaming over HTTP, optional truncate | `name`, `connection`, `table`, `schema_fields` | `connector-clickhouse` |
| `KafkaSink` | Produces to a Kafka topic | `brokers`, `topic`, `schema_fields` | `connector-kafka` |
| `S3Sink` | Accumulates rows and uploads one object per flush | `connection`, `schema_fields` | `connector-s3` |
| `tcp` | Dials a peer and writes one length-prefixed frame per message | `connect`, `schema_fields` | `connector-tcp` |
| `udp` | Sends one datagram per message to every `connect` peer, fire and forget | `connect`, `schema_fields` | `connector-udp` |
| `saci` | Dials a `saci` source in another service, first reachable of `connect` wins | `connect`, `schema_fields` | `connector-saci` |
| `OpenTelemetryMetricsSink` | One OTLP/HTTP export per batch, in Protobuf or JSON | `url`, `schema_fields` for a projection other than the canonical ten columns | `connector-opentelemetry` |
| `ChannelSink` | In-process channel (testing/internal) | `schema_fields` | `connector-channel` |

### Retry

Every `source` and `sink` retries a failed operation with exponential backoff: 4 attempts, a
100 ms base, 2.0x growth, a 30 s cap and 0.1 jitter. An optional `retry` child on a node overrides
the policy, and `max_attempts=1` disables retrying. `standalone.kdl` and `postgresql.kdl` carry
explicit `retry` blocks. The keys are documented in
[`docs/content/service/config/_index.md`](../../docs/content/service/config/_index.md) and
[`workflows.md`](../../docs/content/service/config/workflows.md).

### Flow control

Flow control is on by default for every source. A top-level `flow_control` block or a per-source
override changes its settings. The set of configs shows the keys:

- `clickhouse.kdl`, `kafka.kdl`, `nats.kdl`, `redb.kdl`, `tcp.kdl`, `udp.kdl`,
  `extension_example.kdl`, `../quickstart/quickstart.kdl` and `../branching/branching.kdl` each
  carry a top-level block, tuned per workflow (row bounds, growth step, adjustment epoch or
  `target_latency_ms`).
- `postgresql.kdl`, `mariadb.kdl` and `mariadb_trigger.kdl` carry a top-level block and a
  per-source `flow_control` block. In `postgresql.kdl` the top-level block shortens the
  adjustment epoch from 60 s to 10 s because the workflow redrains every 2 s, and the per-source
  `flow_control { rows 8192 }` pins one source to a fixed admission size.
- `../multi_workflow/multi_workflow.kdl` carries only a per-source override.
- `http.kdl`, `s3.kdl` and the `standalone*` configs run `one_shot`, which builds no controller
  for any source, so they declare no block.
- A stream-mode source builds no controller either, so the stream-mode configs here declare none.
- `cluster.kdl` declares no source, and `ServiceConfig::validate` rejects a `flow_control` block
  in cluster mode, as it does a `store` block.

Every key, the epoch-paced A/B search and the guards that back off at once are in
[`docs/content/service/operate/flow-control.md`](../../docs/content/service/operate/flow-control.md).

### Observability

Every config's `observability` block sets `sample_ratio` and `error_sample_ratio` explicitly. The
`sample_ratio` values run from `0.01` on `../branching/branching.kdl`, where one admitted chunk
multicasts to three downstream nodes, up to `1.0` on the bounded configs, graded by each
workflow's expected span volume. `error_sample_ratio` is `1.0` everywhere, so a lower
`sample_ratio` trims chatter while every failure still shows. Both keys and how they compose with
`log_level` are in
[`docs/content/service/operate/observability.md`](../../docs/content/service/operate/observability.md).

### Transformers

A workflow declares each byte format as its own `transformer "id" format="..."` node, and every
source or sink that moves bytes in that format names it through `transformer="id"`. `options` is
an optional table handed to the format's factory.

| `format` | Stream read | Stream write | Message codec | Schema rule | Feature |
|----------|-------------|--------------|---------------|-------------|---------|
| `csv` | yes | yes | one per row | `schema_fields` required | `transformer-csv` |
| `ndjson` | yes | yes | one per row | inferred when absent on a stream read, required on the message surface | `transformer-ndjson` |
| `parquet` | yes | yes | one per batch | read from the file; `schema_fields` rejected on a source, required on a sink and on the message surface | `transformer-parquet` |
| `avro` | yes | yes | one per row | read from the file; `schema_fields` rejected on a source, required on a sink and on the message surface | `transformer-avro` |
| `arrow-ipc` | yes | yes | one per batch | read from the stream; `schema_fields` rejected on a source, required on a sink and on the message surface | `transformer-arrow-ipc` |

Options:

- `csv` takes `has_headers` (bool, default `#true`, stream surface only; a message is one record
  with no header row).
- `ndjson` takes `infer_max` (integer, default `1024`).
- `avro` takes `compression` (one of `null`, `deflate`, `snappy`, `zstd`, default `null`) and
  `schema_id` (integer, the Confluent registry id).
- `parquet` and `arrow-ipc` take none.

`FileSink` opens its output file as soon as the factory is built, `saci-service validate`
included, so the parent directory must exist before `validate` or `serve`. A missing file is
created and existing bytes are kept: rows land after them. `truncate #true` in the sink's
`config` replaces the file on every build, which the example configs use.

### Components and systems

The config file has no way to declare a component or a system. A processor node is supplied one
of two ways:

- a `wasm` or `plugin` node names a WASM or native plugin component, which reports its components
  through the `describe()` export;
- a custom binary hands `ServiceBuilder::with_runtime` a `Box<dyn PipelineRuntime>` keyed by the
  node's declared id, and the node omits `module` and `library`.

A `systems` or `components` node under `workflow` is a parse error.

### Arrow types for `schema_fields`

`Boolean`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`,
`Float64`, `Utf8`, `LargeUtf8`, `Binary`, `Date32`, `Date64`. Names are case-insensitive. Every
connector accepts this set except three, which read their own vocabulary:

- `PostgresSource` and `PostgresSink`: `timestamp_micros`, `timestamp_micros_utc`, `uuid`, `json`,
  `decimal128` with `precision` and `scale`, `interval_month_day_nano`, `list` with `item`, and an
  optional `pg_type` that pins the server column type. See `crates/saci/src/connector/postgresql.rs`.
- `MariadbSource` and `MariadbSink`: `boolean`, `int8` to `int64`, `uint8` to `uint64`,
  `float32`, `float64`, `decimal128` with `precision` and `scale`, `utf8`, `binary`, `date32`,
  `time64_micros`, `timestamp_micros`, `timestamp_micros_utc`, `json`, and an optional
  `mariadb_type` that asserts the server column type. See `crates/saci/src/connector/mariadb.rs`.
- `TursoSource` and `TursoSink`: `utf8`, `int64`, `float64`, `bool`, `binary`, `decimal128` with
  `precision` and `scale`. See `crates/saci/src/connector/turso.rs`.

## Standalone and cluster mode

| Feature | `mode "standalone"` | `mode "cluster"` |
|---------|---------------------|------------------|
| Feature flag | `service` | `service-cluster` |
| Consensus | None | Raft (openraft), replicating the application state |
| `store` block | Optional | Rejected, validation error if declared |
| `source` nodes allowed | Yes | No, validation error if declared |
| `sink` nodes allowed | Yes | No, validation error if declared |
| `link` nodes allowed | Yes | No, validation error if declared |
| Ingestion mechanism | `Source` trait (file/channel) | `PartitionSource` (distributed pull) |
| Crash recovery | Restart from source | Checkpoint + lease semantics |
| Minimum nodes | 1 | 1 (1-node Raft is valid for testing) |

A cluster workflow declares exactly one processor node (`wasm` or `plugin`) and nothing else. The
distributed runner ingests through `PartitionSource` and checkpoints its output, so there is no
local sink to declare.

Raft replicates cluster state, not a `store` block. Master batches, row-range claims and
checkpoints live in `cluster-app.redb` under the node's `data_dir`, beside `raft-log.redb`,
`bootstrap.lock` and `node-id`.

## Extend saci-service with your own factories

The stock binary is `saci::service::cli::main()`. A binary crate of your own depends on `saci`
with the features it needs:

```toml
[dependencies]
saci = { version = "0.1", default-features = false, features = ["service", "wasm-host", "connector-file", "transformer-csv"] }
```

Its `main` calls `saci::service::cli::main_with`. The closure receives the `ServiceBuilder` that
the `serve` and `validate` subcommands use, after the built-in factories are registered:

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main_with(|builder| {
        builder
            .register_source(MyMongoSourceFactory)
            .register_sink(MyClickHouseSinkFactory)
    })
}
```

Sources, sinks and transformers are the whole factory surface; `register_transformer` adds a byte
format the same way.

`extension_example.kdl` is a commented config for the types such a binary registers. Validate it
with the stock binary to see the unknown-factory warning. Runs the same on Linux, macOS and
Windows (PowerShell):

```text
cargo run -p saci --features server,wasm-host --bin saci-service -- validate --config examples/configs/extension_example.kdl
```

It exits 0 and warns about the unknown types `MongoSource` and `ClickHouseSink`. With `--strict`
it exits 1.

## Files in this directory

| File | Description |
|------|-------------|
| `standalone.kdl` | runnable single-node config using built-in types |
| `cluster.kdl` | runnable cluster template; needs `service-cluster` |
| `standalone_wasm.kdl` | standalone config that loads a WASM processor pipeline |
| `extension_example.kdl` | non-runnable template showing user-defined types |
| `standalone_polyglot.kdl` | runs the Python processor from `examples/polyglot/` |
| `standalone_plugin.kdl` | runs the native plugin fixture |
| `postgresql.kdl` | runnable config driving PostgreSQL at both ends |
| `mariadb.kdl` | runnable config replicating a MariaDB or MySQL table through the binlog with an initial snapshot, needs a server |
| `mariadb_trigger.kdl` | runnable config capturing table changes with installed triggers, needs a server |
| `turso.kdl` | runnable config driving Turso at both ends |
| `clickhouse.kdl` | runnable config driving ClickHouse at both ends, needs a server |
| `kafka.kdl` | runnable config driving Kafka at both ends, needs a broker |
| `nats.kdl` | runnable config driving NATS at both ends, needs a server |
| `s3.kdl` | runnable config driving S3 at both ends, needs a bucket |
| `tcp.kdl` | runnable config driving TCP at both ends, listens and dials |
| `udp.kdl` | runnable config driving UDP at both ends, fire and forget with a liveness probe |
| `saci.kdl` | runnable config linking two SACI services, listens and dials |
| `opentelemetry_metrics.kdl` | runnable config receiving OTLP/HTTP metrics and forwarding them, listens and dials |
| `http.kdl` | runnable config driving HTTP at both ends, needs an endpoint |
| `http_push.kdl` | runnable config accepting pushed HTTP documents, listens and dials |
| `redb.kdl` | standalone config backed by a local redb store |
| `redb_connector.kdl` | runnable config storing rows in a redb file, needs no service |
| `dlq.kdl` | runnable config whose sink fails, so its dead letter queue fills |
| `fixtures/` | the CSV inputs these configs read, plus `dlq_collector.py` |
