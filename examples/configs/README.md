# SACI Service Example Configs

This directory holds runnable KDL configurations for the `saci-service`
binary itself, one per connector plus standalone and cluster templates. Each
is a `--config` argument to `saci-service validate` or `saci-service serve`.

## Required features

Most configs in this directory declare a `wasm` node inside their `workflow`
block, and that node needs the `wasm` feature to run. `service` does not imply
it, so `--features service` alone refuses those files with
``workflow 'orders': wasm node 'transform' needs the wasmtime processor host,
which this binary was built without, so rebuild or reinstall with `--features
wasm` ``. The file still parses: the refusal names the flag rather than
reporting `wasm` as an unknown key. `standalone_plugin.kdl` declares a
`plugin` node instead, which the default bundle does **not** carry and which
is refused the same way, and `redb.kdl`, `redb_connector.kdl` and `dlq.kdl`
declare no processor node at all.
Each `type="..."` also needs the feature that registers its factory. Build and
run with:

| Config | Features |
|--------|----------|
| `standalone.kdl`, `standalone_wasm.kdl`, `standalone_polyglot.kdl` | `connector-file,transformer-csv,wasm` |
| `extension_example.kdl` | a custom binary: no feature registers `MongoSource` or `ClickHouseSink`, and `wasm` is all `validate` needs to show the warning |
| `standalone_plugin.kdl` | `connector-file,transformer-csv,plugin` |
| `postgresql.kdl` | `connector-postgresql,wasm` |
| `turso.kdl` | `connector-turso,wasm` |
| `s3.kdl` | `connector-s3,transformer-csv,wasm` |
| `cluster.kdl` | `service-cluster,wasm`; it declares no source, sink or transformer |
| `kafka.kdl` | `connector-kafka,wasm` |
| `nats.kdl` | `connector-nats,wasm` |
| `tcp.kdl` | `connector-tcp,wasm` |
| `saci.kdl` | `connector-saci,wasm` |
| `http.kdl` | `connector-http,transformer-csv,wasm` |
| `redb.kdl` | `connector-file,transformer-csv` |
| `redb_connector.kdl` | `connector-file,connector-redb,transformer-csv` |
| `dlq.kdl` | the default bundle: `connector-file,connector-http,connector-redb,transformer-csv,transformer-ndjson` |

## Build the binary

The `saci-service` binary needs at least the features of the config you plan to
run, so a build usually carries the same feature list:

```text
cargo build -p saci-service --features connector-file,transformer-csv,wasm --bin saci-service
```

Runs the same on Linux, macOS and Windows (PowerShell).

`--features all` covers every config here the stock binary can run, in one
build: all five opt-in connectors, the plugin host and cluster mode, so it needs
`cmake` and a C toolchain on `PATH` for Kafka's vendored librdkafka.

## How to run the standalone example

```text
# Validate the config. No side effects; exits 0 on success.
cargo run -p saci-service --features connector-file,transformer-csv,wasm --bin saci-service -- validate --config examples/configs/standalone.kdl --strict

# Run the pipeline. Reads fixtures/orders.csv, writes /tmp/saci-standalone-orders-out.csv.
cargo run -p saci-service --features connector-file,transformer-csv,wasm --bin saci-service -- serve --config examples/configs/standalone.kdl
```

Runs the same on Linux, macOS and Windows (PowerShell).

The process exits after one pipeline iteration because `run_mode` sets
`kind="one_shot"`. Check `/tmp/saci-standalone-orders-out.csv` for the output.
The config's wasm node names `pipelines/orders.wasm`, a component that must
exist when the pipeline loads.

## How to validate the cluster example

Cluster mode requires `--features service-cluster`, and takes no `store`
block: its application state is the raft-replicated `cluster-app.redb` under
`node.data_dir`, so a `store` block there is a configuration error. The config
declares a `wasm` node, so the `wasm` feature comes with it.

Linux and macOS:

```text
SACI_NODE_ID=1 SACI_DATA_DIR=/tmp/saci-node-1 \
cargo run -p saci-service --features service-cluster,wasm --bin saci-service -- validate --config examples/configs/cluster.kdl --strict
```

Windows (PowerShell):

```powershell
$env:SACI_NODE_ID = "1"
$env:SACI_DATA_DIR = "C:\tmp\saci-node-1"
cargo run -p saci-service --features service-cluster,wasm --bin saci-service -- validate --config examples/configs/cluster.kdl --strict
```

To run a three-node cluster you need three processes, each with a distinct
`SACI_NODE_ID` and `SACI_DATA_DIR`, with `SACI_BOOTSTRAP=true` on exactly one
node during the first bring-up. See the comments in `cluster.kdl` for the
step-by-step procedure.

## How to run the redb-backed config

`redb.kdl` needs no external service: the store is one local file. With the
`store` block present, `serve` persists the raw config file to it before the
pipeline builds, writes stream-mode source cursors and processor priors back
as items flow, and a restarted service resumes from its last save point.

```text
cargo run -p saci-service --features connector-file,transformer-csv --bin saci-service -- validate --config examples/configs/redb.kdl
cargo run -p saci-service --features connector-file,transformer-csv --bin saci-service -- serve --config examples/configs/redb.kdl
```

Runs the same on Linux, macOS and Windows (PowerShell).

The pipeline reads `examples/configs/fixtures/orders.csv` and writes
`/tmp/saci-redb-orders-out.csv`.

## Built-in factories

Each factory lives in the crate that owns it and reaches the registry only when
its `saci-service` feature is on. `service` alone registers nothing.

A connector moves bytes and a transformer decides what they mean. A config
that names a file therefore needs both: a `source`/`sink` node with
`type="FileSource"`/`type="FileSink"` from `connector-file`, and a declared
`transformer "..." format="csv"` node from `transformer-csv` that the
source/sink references by id through its own `transformer="..."` property.
Kafka, NATS and TCP resolve their byte format the same way and default to
none: a byte-carrying source or sink always names its `transformer`
explicitly, there is no implicit default format.

### Sources

| config `type` | Description | Required config keys | Crate | Feature |
|---------------|-------------|----------------------|-------|---------|
| `FileSource` | Reads a local file in whatever format its `transformer` names | `path` | `saci-connector-file` | `connector-file` |
| `RedbSource` | Reads the entries of one redb table in key order, in whatever format its `transformer` names | `directory`, `schema_fields` | `saci-connector-redb` | `connector-redb` |
| `HttpSource` | One GET, decoded in whatever format its `transformer` names | `url`, plus `schema_fields` where the format needs it | `saci-connector-http` | `connector-http` |
| `PostgresSource` | Polling, outbox or `pgoutput` reads | `name`, `connection`, `mode`, `schema_fields` (optionally `pg_type` per field) | `saci-connector-postgresql` | `connector-postgresql` |
| `TursoSource` | Embedded or synced reads: cursor polling, full dump, or the change table | `name`, `connection`, `mode`, `schema_fields` | `saci-connector-turso` | `connector-turso` |
| `KafkaSource` | Consumes a Kafka topic | `brokers`, `topic`, `schema_fields` | `saci-connector-kafka` | `connector-kafka` |
| `S3Source` | Lists a prefix once and drains every object in key order | `connection`, `schema_fields` | `saci-connector-s3` | `connector-s3` |
| `tcp` | Live framed messages off a listener, stream mode only | `bind`, `schema_fields` | `saci-connector-tcp` | `connector-tcp` |
| `saci` | Batches pushed by a `saci` sink in another service, stream mode only | `bind`, `schema_fields` | `saci-connector-saci` | `connector-saci` |
| `ChannelSource` | In-process channel (testing/internal) | `schema_fields` | `saci-connector-channel` | `connector-channel` |

### Sinks

| config `type` | Description | Required config keys | Crate | Feature |
|---------------|-------------|----------------------|-------|---------|
| `FileSink` | Writes a local file in whatever format its `transformer` names | `path`, `schema_fields`, optional `truncate` | `saci-connector-file` | `connector-file` |
| `RedbSink` | Stores one redb entry per batch, in whatever format its `transformer` names | `directory`, `schema_fields` | `saci-connector-redb` | `connector-redb` |
| `HttpSink` | One request per batch, body written by its `transformer` | `url`, `schema_fields`, optional `method` | `saci-connector-http` | `connector-http` |
| `PostgresSink` | `COPY FORMAT binary`, optional upsert | `name`, `connection`, `table`, `schema_fields` (optionally `pg_type` per field) | `saci-connector-postgresql` | `connector-postgresql` |
| `TursoSink` | Prepared `INSERT` with optional `ON CONFLICT` upsert, `deferred`/`immediate`/`concurrent` transactions, change capture and encryption | `name`, `connection`, `table`, `write_mode`, `schema_fields` | `saci-connector-turso` | `connector-turso` |
| `KafkaSink` | Produces to a Kafka topic | `brokers`, `topic`, `schema_fields` | `saci-connector-kafka` | `connector-kafka` |
| `S3Sink` | Accumulates rows and uploads one object per flush | `connection`, `schema_fields` | `saci-connector-s3` | `connector-s3` |
| `tcp` | Dials a peer and writes one length-prefixed frame per message | `connect`, `schema_fields` | `saci-connector-tcp` | `connector-tcp` |
| `saci` | Dials a `saci` source in another service, first reachable of `connect` wins | `connect`, `schema_fields` | `saci-connector-saci` | `connector-saci` |
| `ChannelSink` | In-process channel (testing/internal) | `schema_fields` | `saci-connector-channel` | `connector-channel` |

### Retry

Every `source` and `sink` retries a failed operation with exponential backoff
by default: 4 attempts, a 100 ms base, 2.0x growth, a 30 s cap and 0.1 jitter.
An optional `retry` child on a `source` or `sink` overrides the policy per
node; `max_attempts=1` disables retrying. `standalone.kdl` and
`postgresql.kdl` carry explicit `retry` blocks, and
`docs/content/service/config/_index.md` and
`docs/content/service/config/workflows.md` document every key.

### Flow control

On by default for every source, at hard-coded defaults: a top-level
`flow_control` block, or a per-source override, changes them.
`postgresql.kdl` carries both: a top-level block that shortens the
adjustment epoch from its 60 s default to 10 s, since this workflow
redrains every 2 s, and a per-source `flow_control { rows 8192 }` pinning
its one source to a fixed admission size instead. `kafka.kdl`, `nats.kdl`,
`redb.kdl`, `tcp.kdl`, `extension_example.kdl`,
`../quickstart/quickstart.kdl` and `../branching/branching.kdl` each carry
their own top-level block, tuned differently per workflow (row bounds,
growth step, adjustment epoch, or `target_latency_ms`), so the set as a
whole covers the available keys.
`../multi_workflow/multi_workflow.kdl` scopes its override to one source
instead: only its "route" workflow's `orders_in` feeds a plain sink chain,
while its "settle" workflow's two sources feed a windowed processor, and a
stream-mode source on that path builds no controller at all regardless of
what a block declares.
`http.kdl`, `s3.kdl`, `standalone.kdl` and its three `standalone_*`
variants run `one_shot`, which builds no controller for any source, so
they declare no `flow_control` block at all; `cluster.kdl` declares no
source to govern and `ServiceConfig::validate` rejects a `flow_control`
block there outright, the same as a `store` block.
`../windowing/`'s three configs each declare two sources feeding windowed
processors under `stream` mode, which also builds no controller regardless of
what a block would declare, so none of them carries one.
`docs/content/service/operate/flow-control.md` documents every key, the
epoch-paced A/B search and the guards that back off immediately, and which
connectors act on the resulting `Source::request_batch_rows` hint.

### Observability

Every config's `observability` block sets `sample_ratio` and
`error_sample_ratio` explicitly rather than taking the implicit `1.0`
default, so the set as a whole shows the full range: from `0.01` on
`../branching/branching.kdl`'s live stream, where one admitted chunk
multicasts to three downstream nodes and reaches five sink destinations,
up to `1.0` on the bounded one-shot tutorials (`standalone.kdl` and its
`standalone_*` variants, plus `http.kdl`), with every other config graded by
its own expected span volume. `error_sample_ratio` stays at `1.0` everywhere,
so a lower `sample_ratio` trims chatter while every failure still shows.
`docs/content/service/operate/observability.md` documents both keys and how
they compose with `log_level`.

### Transformers

A workflow declares each byte format it needs as its own `transformer "id"
format="..."` node, and every source/sink that moves bytes in that format
names it through its own `transformer="id"` property. `options` is an
optional table handed to the format's own factory.

| `format` | Stream read | Stream write | Message codec | Schema rule | Crate | Feature |
|----------|-------------|--------------|---------------|-------------|-------|---------|
| `csv` | yes | yes | one per row | `schema_fields` required | `saci-transformer-csv` | `transformer-csv` |
| `ndjson` | yes | yes | one per row | inferred when absent on a stream read, required on the message surface | `saci-transformer-ndjson` | `transformer-ndjson` |
| `parquet` | yes | yes | one per batch | read from the file; `schema_fields` rejected on a source, required on a sink and on the message surface | `saci-transformer-parquet` | `transformer-parquet` |
| `avro` | yes | yes | one per row | read from the file; `schema_fields` rejected on a source, required on a sink and on the message surface | `saci-transformer-avro` | `transformer-avro` |
| `arrow-ipc` | yes | yes | one per batch | read from the stream; `schema_fields` rejected on a source, required on a sink and on the message surface | `saci-transformer-arrow-ipc` | `transformer-arrow-ipc` |

`options`: `csv` takes `has_headers` (bool, default `#true`, stream surface
only, a message is one record with no header row), `ndjson` takes `infer_max`
(integer, default `1024`), `avro` takes `compression` (string, one of `null`,
`deflate`, `snappy`, `zstd`, default `null`) and `schema_id` (integer, the
Confluent registry id), and the other two take none.

`FileSink` opens the output file as soon as the factory is built,
`saci-service validate` included, so the parent directory must exist before
running `validate` or `serve`. The file is created when it is missing and its
existing bytes are kept: rows land after them. Set `truncate #true` in the
sink's `config` to replace the file on every build instead, which is what the
example configs do.

### Components and systems

There are no component or system factories: the config file has no path for
declaring either. Each processor node in the workflow is supplied one of two
ways:

- a `wasm`/`plugin` node names a WASM or native-plugin processor component,
  which reports its components through the `describe()` export, or
- a custom binary hands `ServiceBuilder::with_runtime` a
  `Box<dyn PipelineRuntime>` keyed by that node's declared id, and the node
  itself omits `module`/`library`.

A `systems` or `components` node under `workflow` is a parse error, not a
silently dropped section.

### Supported Arrow types for `schema_fields`

`Boolean`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`,
`UInt64`, `Float32`, `Float64`, `Utf8`, `LargeUtf8`, `Binary`, `Date32`,
`Date64`. All names are case-insensitive. That is the set every connector but
PostgreSQL and Turso accepts; `PostgresSource`/`PostgresSink` parse `schema_fields`
through their own wider vocabulary (`timestamp_micros`, `timestamp_micros_utc`,
`uuid`, `json`, `decimal128` with `precision`/`scale`, `interval_month_day_nano`,
`list` with `item`, plus an optional `pg_type` to pin the exact server column
type), documented in `crates/saci-connector-postgresql/src/lib.rs`;
`TursoSource`/`TursoSink` use a narrower vocabulary (`utf8`, `int64`, `float64`,
`bool`, `binary`, `decimal128` with `precision`/`scale`), documented in
`crates/saci-connector-turso/src/lib.rs`.

## Standalone vs cluster mode

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

A cluster-mode workflow declares exactly one processor node (`wasm` or
`plugin`) and nothing else: the distributed runner ingests through
`PartitionSource` and checkpoints its output, so there is no local sink to
declare either.

Cluster state is replicated through the node's own raft, not a `store` block.
Master batches, row-range claims and checkpoints live in `cluster-app.redb`
under the node's `data_dir`, alongside `raft-log.redb`, `bootstrap.lock` and
`node-id`.

## How to extend saci-service with user factories

The stock binary calls `register_builtin_factories(ServiceBuilder::new())`. Fork
`src/bin/saci-service/main.rs` (or write your own binary) and add your own
factories before calling `builder.build_all(&config)`:

```rust
use saci_connector::{SinkFactory, SourceFactory};
use saci_service::service::ServiceBuilder;
use saci_service::service::factories::register_builtin_factories;

let builder = register_builtin_factories(ServiceBuilder::new())
    .register_source(MyMongoSourceFactory)
    .register_sink(MyClickHouseSinkFactory);

let built = builder.build_all(&config)?;
```

Sources, sinks and transformers are the whole factory surface;
`register_transformer` adds a byte format the same way. A processor node is
either a `wasm`/`plugin` node naming a module/library, or a
`Box<dyn PipelineRuntime>` passed to `ServiceBuilder::with_runtime` keyed by
that node's declared id, with `module`/`library` omitted on the node itself.

See `extension_example.kdl` for a commented config showing all the types you
would register in a real order-processing service. Validate it to see the
unknown-factory warning behavior:

```text
cargo run -p saci-service --features wasm --bin saci-service -- validate --config examples/configs/extension_example.kdl
```

Runs the same on Linux, macOS and Windows (PowerShell).

This exits 0 and warns about the unknown types (`MongoSource`,
`ClickHouseSink`). With `--strict` it exits 1, because unknown types are
errors in strict mode.

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
| `kafka.kdl` | runnable config driving Kafka at both ends, needs a broker |
| `nats.kdl` | runnable config driving NATS at both ends, needs a server |
| `s3.kdl` | runnable config driving S3 at both ends, needs a bucket |
| `tcp.kdl` | runnable config driving TCP at both ends, listens and dials |
| `saci.kdl` | runnable config linking two SACI services, listens and dials |
| `http.kdl` | runnable config driving HTTP at both ends, needs an endpoint |
| `redb.kdl` | standalone config backed by a local redb store |
| `redb_connector.kdl` | runnable config storing rows in a redb file, needs no service |
| `dlq.kdl` | runnable config whose sink fails, so its dead letter queue fills |
| `fixtures/` | the CSV inputs these configs read, plus `dlq_collector.py` |
