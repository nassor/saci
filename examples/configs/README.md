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

Each `connector="..."` also needs the feature that registers it. `server` carries `service`,
`wasm-host` and the seven self-contained connectors (`connector-channel`, `connector-file`,
`connector-http`, `connector-tcp`, `connector-udp`, `connector-peer`, `connector-redb`) with the
five codecs. The table lists what to enable beside it:

| Config | Features |
|--------|----------|
| `standalone.kdl`, `standalone_wasm.kdl`, `standalone_settle.kdl` | `connector-file,codec-csv,wasm-host` |
| `extension_example.kdl` | a custom binary: no feature registers the `mongo` or `clickhouse-enriched` connector, and `wasm-host` is all `validate` needs to show the warning |
| `standalone_plugin.kdl` | `connector-file,codec-csv,plugin-host` |
| `postgresql.kdl` | `connector-postgresql,wasm-host` |
| `mariadb.kdl` | `connector-mariadb,wasm-host` |
| `mariadb_trigger.kdl` | `connector-mariadb,wasm-host` |
| `turso.kdl` | `connector-turso,wasm-host` |
| `clickhouse.kdl` | `connector-clickhouse,wasm-host` |
| `s3.kdl` | `connector-s3,codec-csv,wasm-host` |
| `cluster.kdl` | `service-cluster,wasm-host`; it declares no source, sink or codec |
| `kafka.kdl` | `connector-kafka,wasm-host` |
| `nats.kdl` | `connector-nats,wasm-host` |
| `tcp.kdl` | `connector-tcp,wasm-host` |
| `udp.kdl` | `connector-udp,wasm-host` |
| `saci.kdl` | `connector-peer,wasm-host` |
| `opentelemetry_metrics.kdl` | `connector-otlp`; it declares no processor node |
| `http.kdl` | `connector-http,codec-csv,wasm-host` |
| `http_push.kdl` | `connector-http,codec-ndjson,wasm-host` |
| `redb.kdl` | `connector-file,codec-csv` |
| `redb_connector.kdl` | `connector-file,connector-redb,codec-csv` |
| `dlq.kdl` | `server`, which carries `connector-file,connector-http,connector-redb,codec-csv,codec-ndjson` |

## Build the binary

Build with `server` plus the features of the config you plan to run:

```text
cargo build -p saci --features server,connector-file,codec-csv,wasm-host --bin saci-service
```

Runs the same on Linux, macOS and Windows (PowerShell).

`--features all` covers every config here that the stock binary can run: the eight opt-in
connectors, the plugin host and cluster mode. It needs `cmake` and a C toolchain on `PATH` for
Kafka's vendored librdkafka.

## Run the standalone example

`standalone_wasm.kdl` reads `fixtures/order_processing_input.csv`, runs the Rust processor from
`examples/wasm/order_processing/`, and writes `/tmp/saci-order-processing-out.csv`. Its
`run` node is `"once"`, so the process exits after one iteration. The sink opens its file when
the pipeline is built, so the output directory must exist first.

1. Build the processor component. Runs the same on Linux, macOS and Windows (PowerShell):

   ```text
   cargo build --release -p order-processing-wasm --target wasm32-wasip3
   ```

   It writes `target/wasm32-wasip3/release/order_processing_wasm.wasm`, the module the config names.

2. On Windows, create the output directory. `/tmp` in the config resolves to `\tmp` on the current
   drive. Linux and macOS already have `/tmp`.

   ```powershell
   New-Item -ItemType Directory -Force C:\tmp | Out-Null
   ```

3. Validate the config. It has no side effects and exits 0 on success. Runs the same on Linux,
   macOS and Windows (PowerShell):

   ```text
   cargo run -p saci --features server,connector-file,codec-csv,wasm-host --bin saci-service -- validate --config examples/configs/standalone_wasm.kdl --strict
   ```

4. Run the pipeline. Runs the same on Linux, macOS and Windows (PowerShell):

   ```text
   cargo run -p saci --features server,connector-file,codec-csv,wasm-host --bin saci-service -- serve --config examples/configs/standalone_wasm.kdl
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
codec:

```text
cargo run -p saci --features server,connector-file,codec-csv,wasm-host --bin saci-service -- validate --config examples/configs/standalone.kdl --connectors-only
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
cargo run -p saci --features server,connector-file,codec-csv --bin saci-service -- validate --config examples/configs/redb.kdl
cargo run -p saci --features server,connector-file,codec-csv --bin saci-service -- serve --config examples/configs/redb.kdl
```

## Built-in connectors

Each connector lives in a module of the `saci` crate and registers only when its feature and
`service` are both on. `service` alone registers nothing.

A connector moves bytes and a codec decides what they mean. A config that names a file
therefore needs both: a `source` or `sink` with `connector="file"` from
`connector-file`, and a `codec "..." format="csv"` node from `codec-csv` that the
source or sink references by id through its own `codec="..."` property. A byte-carrying
source or sink always names its `codec`: there is no implicit default format.

### Sources

| `connector` | Description | Required config keys | Feature |
|-------------|-------------|----------------------|---------|
| `file` | Reads a local file in whatever format its `codec` names | `path` | `connector-file` |
| `redb` | Reads the entries of one redb table in key order, in whatever format its `codec` names | `directory`, `field` lines | `connector-redb` |
| `http` | One GET, or a body that never ends, decoded in whatever format its `codec` names | `url`, plus `field` lines where the format needs them | `connector-http` |
| `http-listen` | Documents a client pushes in, one whole document per request, stream mode only | `bind`, `field` lines | `connector-http` |
| `postgresql` | Polling, outbox or `pgoutput` reads | `name`, `connection`, `mode`, `field` lines (optionally `native_type` per field) | `connector-postgresql` |
| `mariadb` | Polling, a trigger changelog (an operator outbox or installed triggers) or row-binlog reads on MariaDB and MySQL, with an optional initial snapshot | `name`, `connection`, `mode`, `field` lines | `connector-mariadb` |
| `turso` | Embedded or synced reads: cursor polling, full dump, or the change table | `name`, `connection`, `mode`, `field` lines | `connector-turso` |
| `clickhouse` | Polling, dump or custom SELECT query | `name`, `connection`, `mode`, `field` lines | `connector-clickhouse` |
| `kafka` | Consumes a Kafka topic | `brokers`, `topic`, `field` lines | `connector-kafka` |
| `s3` | Lists a prefix once and drains every object in key order | `connection`, `field` lines | `connector-s3` |
| `tcp` | Live framed messages off a listener, stream mode only | `bind`, `field` lines | `connector-tcp` |
| `udp` | Live datagrams off a bound socket, one message each, stream mode only | `bind`, `field` lines | `connector-udp` |
| `peer` | Batches pushed by a `peer` sink in another service, stream mode only | `bind`, `field` lines | `connector-peer` |
| `otlp-metrics` | Listens for OTLP/HTTP metrics, or reads a url, in Protobuf or JSON; listening is stream mode only | `bind`, `field` lines for a projection other than the canonical ten columns | `connector-otlp` |
| `channel` | In-process channel (testing/internal) | `field` lines | `connector-channel` |

### Sinks

| `connector` | Description | Required config keys | Feature |
|-------------|-------------|----------------------|---------|
| `file` | Writes a local file in whatever format its `codec` names | `path`, `field` lines, optional `truncate` | `connector-file` |
| `redb` | Stores one redb entry per batch, in whatever format its `codec` names | `directory`, `field` lines | `connector-redb` |
| `http` | One request per batch, body written by its `codec` | `url`, `field` lines, optional `method` | `connector-http` |
| `postgresql` | `COPY FORMAT binary`, optional upsert | `name`, `connection`, `table`, `field` lines (optionally `native_type` per field) | `connector-postgresql` |
| `mariadb` | Batched `INSERT` with `append`, `upsert`, `ignore_conflicts` or `apply_changes` | `name`, `connection`, `table`, `field` lines | `connector-mariadb` |
| `turso` | Prepared `INSERT` with optional `ON CONFLICT` upsert, `deferred`/`immediate`/`concurrent` transactions, change capture and encryption | `name`, `connection`, `table`, `write_mode`, `field` lines | `connector-turso` |
| `clickhouse` | Bulk load via Arrow IPC streaming over HTTP, optional truncate | `name`, `connection`, `table`, `field` lines | `connector-clickhouse` |
| `kafka` | Produces to a Kafka topic | `brokers`, `topic`, `field` lines | `connector-kafka` |
| `s3` | Accumulates rows and uploads one object per flush | `connection`, `field` lines | `connector-s3` |
| `tcp` | Dials a peer and writes one length-prefixed frame per message | `connect`, `field` lines | `connector-tcp` |
| `udp` | Sends one datagram per message to every `connect` peer, fire and forget | `connect`, `field` lines | `connector-udp` |
| `peer` | Dials a `peer` source in another service, first reachable of `connect` wins | `connect`, `field` lines | `connector-peer` |
| `otlp-metrics` | One OTLP/HTTP export per batch, in Protobuf or JSON | `url`, `field` lines for a projection other than the canonical ten columns | `connector-otlp` |
| `channel` | In-process channel (testing/internal) | `field` lines | `connector-channel` |

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
- `http.kdl`, `s3.kdl` and the `standalone*` configs run `once`, which builds no controller
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

### Codecs

A workflow declares each byte format as its own `codec "id" format="..."` node, and every
source or sink that moves bytes in that format names it through `codec="id"`. `config` is
an optional table handed to the format's factory.

| `format` | Stream read | Stream write | Message codec | Schema rule | Feature |
|----------|-------------|--------------|---------------|-------------|---------|
| `csv` | yes | yes | one per row | `field` lines required | `codec-csv` |
| `ndjson` | yes | yes | one per row | inferred when absent on a stream read, required on the message surface | `codec-ndjson` |
| `parquet` | yes | yes | one per batch | read from the file; `field` lines rejected on a source, required on a sink and on the message surface | `codec-parquet` |
| `avro` | yes | yes | one per row | read from the file; `field` lines rejected on a source, required on a sink and on the message surface | `codec-avro` |
| `arrow-ipc` | yes | yes | one per batch | read from the stream; `field` lines rejected on a source, required on a sink and on the message surface | `codec-arrow-ipc` |

Config keys:

- `csv` takes `has_headers` (bool, default `#true`, stream surface only; a message is one record
  with no header row).
- `ndjson` takes `infer_max` (integer, default `1024`).
- `avro` takes `compression` (one of `null`, `deflate`, `snappy`, `zstd`, default `null`) and
  `schema_id` (integer, the Confluent registry id).
- `parquet` and `arrow-ipc` take none.

The `file` sink opens its output file as soon as the factory is built, `saci-service validate`
included, so the parent directory must exist before `validate` or `serve`. A missing file is
created and existing bytes are kept: rows land after them. `truncate #true` in the sink's
`config` replaces the file on every build, which the example configs use.

### Components and systems

The config file has no way to declare a component or a system. A processor node is supplied one
of two ways:

- a `wasm` or `plugin` node names a WASM or native plugin component, which reports its components
  through the `describe()` export;
- a custom binary hands `ServiceBuilder::runtime` a `PipelineRuntime` keyed by the
  node's declared id, and the node omits `path`.

A `systems` or `components` node under `workflow` is a parse error.

### Field types

A `field` line takes `type=` from one lowercase list: `boolean`, `int8`, `int16`, `int32`,
`int64`, `uint8`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `utf8`, `largeutf8`,
`json`, `binary`, `date32`, `date64`, `time64_micros`, `timestamp_micros`,
`timestamp_micros_utc`, `timestamp_millis`, `uuid`, `decimal128` with `precision` and `scale`,
`interval_month_day_nano` and `list` with `item`. Mixed-case names and aliases such as `string`
or `bool` are errors. An optional `native_type` pins the server column type on the database
connectors.

`postgresql`, `mariadb`, `turso` and `clickhouse` refuse a type they do not map and name the field
in the error; each connector page under `docs/content/service/connectors/` lists the types it
maps. For example, `turso` maps `utf8`, `int64`, `float64`, `boolean`, `binary` and `decimal128`
with `precision` and `scale`.

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

## Extend saci-service with your own connectors

The stock binary is `saci::service::cli::main()`. A binary crate of your own depends on `saci`
with the features it needs:

```toml
[dependencies]
saci = { version = "0.1", default-features = false, features = ["service", "wasm-host", "connector-file", "codec-csv"] }
```

Its `main` calls `saci::service::cli::main_with`. The closure receives the `ServiceBuilder` that
the `serve` and `validate` subcommands use, after the built-in connectors and codecs are registered:

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main_with(|builder| {
        builder
            .connector(MyMongoConnector)
            .connector(MyClickHouseConnector)
    })
}
```

Connectors and codecs are the whole extension surface; `ServiceBuilder::codec` adds a byte
format the same way.

`extension_example.kdl` is a commented config for the connectors such a binary registers. Validate it
with the stock binary to see the unknown-connector warning. Runs the same on Linux, macOS and
Windows (PowerShell):

```text
cargo run -p saci --features server,wasm-host --bin saci-service -- validate --config examples/configs/extension_example.kdl
```

It exits 0 and warns about the unknown connectors `mongo` and `clickhouse-enriched`. With `--strict`
it exits 1.

## Files in this directory

| File | Description |
|------|-------------|
| `standalone.kdl` | runnable single-node config using built-in types |
| `cluster.kdl` | runnable cluster template; needs `service-cluster` |
| `standalone_wasm.kdl` | standalone config that loads a WASM processor pipeline |
| `extension_example.kdl` | non-runnable template showing user-defined types |
| `standalone_settle.kdl` | runs the stateful settlement WASM processor from `examples/wasm/settle/` |
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
