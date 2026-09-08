+++
title = "Writing a connector"
description = "Implement Source, Sink and their factories, and register them."
template = "page.html"
weight = 1
+++
# Writing a connector

A connector is reached either by calling its constructor in Rust and handing the
result to `Pipeline::add_source`, or by naming its `type` string in the service
config and letting the factory build it. The second path needs the connector's
feature on `saci-service`, because `register_builtin_factories` only installs
what is compiled in.

```toml,name=The dependency form is the same for all of them
# In a clone of the repository.
saci-connector-file = { path = "crates/saci-connector-file" }

# Outside one. The crates are not published to crates.io.
saci-connector-file = { git = "https://github.com/nassor/saci" }
```

Every connector depends on `saci-core` with its `io` feature, so the `Source` and
`Sink` traits arrive with it. Nothing here depends on `saci-service`: the factory
contract lives in `saci-connector`, below the host, so a connector compiles
without wasmtime, axum or openraft in the graph. The config keys each node
accepts, and the errors they raise, are on
[Sources and sinks](@/service/connectors/_index.md).

## Writing your own

An out-of-tree connector is a crate that depends on `saci-core` for the traits
and `saci-connector` for `SourceFactory` and `SinkFactory`, and on nothing else
of SACI. Both traits build through
`fn build(&self, config: &ConfigValue, ctx: &ConnectorContext)`, where `ctx`
resolves the node's transformer against the transformer registry so no connector
owns a list of formats. A custom binary then chains `register_source` or
`register_sink` onto `register_builtin_factories`. Registering a `type_name`
that is already taken replaces the earlier factory.

[Embedding saci-service](@/library/service/_index.md) has the factory and the
builder call side by side.

## The built-in constructors

Nine crates, and the constructors are not uniform.

### Channel

```rust,name=saci-connector-channel
use saci_connector_channel::{ChannelSink, ChannelSource};

ChannelSource::new(Arc<Schema>, buffer: usize) -> (mpsc::Sender<RecordBatch>, Self)
ChannelSink::new  (Arc<Schema>, buffer: usize) -> (Self, mpsc::Receiver<RecordBatch>)
```

Each constructor returns the paired endpoint. `ChannelSink`'s `Sink::pending_rows`
reports the rows of every batch the channel holds and the consumer has not
received. The sink sums the row counts of the messages still in flight, so the
number is a row backlog whatever size the batches are.

### File

```rust,name=saci-connector-file
use saci_connector_file::{FileSink, FileSource};

FileSource::open           (&Path, Arc<dyn Transformer>, Option<Arc<Schema>>) -> Result<Self>
FileSource::open_async     (&Path, Arc<dyn Transformer>, Option<Arc<Schema>>) -> Result<Self>
FileSink::create           (&Path, Arc<dyn Transformer>, Arc<Schema>)         -> Result<Self>
FileSink::create_truncating(&Path, Arc<dyn Transformer>, Arc<Schema>)         -> Result<Self>
```

`open_async` moves the open and the metadata read off the executor; `create`
appends, `create_truncating` replaces. `FileSource::estimated_rows` forwards what
the reader reported at open time, so it is `Some` only for a format that counts
rows without reading them.

### HTTP

```rust,name=saci-connector-http
use saci_connector_http::{HttpSink, HttpSource};

HttpSource::new(
    url: &str,
    declared: Option<Arc<Schema>>,
    schema_from: SchemaFrom,
    transformer: Arc<dyn Transformer>,
    headers: Vec<(String, String)>,
    timeout: Duration,
) -> Result<Self>

HttpSink::new(
    url: &str,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    method: &str,
    headers: Vec<(String, String)>,
    timeout: Duration,
) -> Result<Self>
```

The source's schema is the declared one, so it is `Option`; the sink's is not.
Neither makes a request, so a build needs no reachable endpoint.
`HttpSource::estimated_rows` forwards what the reader reported, so it is `None`
until the body has arrived and `Some` after it only for a format that counts rows
without reading them.

### Kafka

```rust,name=saci-connector-kafka
use saci_connector_kafka::{
    KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig,
};

KafkaSource::new(KafkaSourceConfig, Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
KafkaSink::new  (KafkaSinkConfig,   Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
```

Both are synchronous and open no connection. Each one validates the config, asks
the transformer for its message shape, and builds a client. `librdkafka` connects
lazily, so `saci-service validate` stays broker free.

### NATS

```rust,name=saci-connector-nats
use saci_connector_nats::{
    NatsSink, NatsSinkConfig, NatsSource, NatsSourceConfig,
};

NatsSource::new(NatsSourceConfig, Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
NatsSink::new  (NatsSinkConfig,   Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
```

Both are synchronous and open no connection. Each one validates the config and
asks the transformer for its message shape. Connecting, provisioning and creating
the consumer happen on the first `next_batch` or `write_batch`, so
`saci-service validate` stays server free. Every mode struct carries a `Default`
matching the config defaults, so a config built in Rust names only the keys it
changes.

### PostgreSQL

```rust,name=saci-connector-postgresql
use saci_connector_postgresql::{
    PostgresSink, PostgresSinkConfig, PostgresSource, PostgresSourceConfig,
};

PostgresSource::new(PostgresSourceConfig) -> Result<Self>
PostgresSink::new  (PostgresSinkConfig)   -> Result<Self>
```

Both are synchronous and open no connection. Each one validates its config and
then builds a reader or a writer. The DSN is parsed, no socket is opened, so the
first connection happens on the first batch. That is why `saci-service validate`
needs no database.

### S3

```rust,name=saci-connector-s3
use saci_connector_s3::{S3Sink, S3Source};

S3Source::new(S3SourceConfig, Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
S3Sink::new  (S3SinkConfig,   Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
```

Both constructors are synchronous and open no connection; the first request
happens inside the first call.

### TCP

```rust,name=saci-connector-tcp
use saci_connector_tcp::{TcpIngestSource, TcpSink};

TcpIngestSource::new(
    bind: &str,
    schema: Arc<Schema>,
    buffer: usize,
    max_frame_bytes: usize,
    transformer: Arc<dyn Transformer>,
) -> Result<Self>

source.local_addr() -> SocketAddr

TcpSink::connect(
    connect: &str,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
) -> Result<Self>

sink.peer_addr() -> SocketAddr
```

`local_addr` resolves an ephemeral port, `peer_addr` the dialled one. The frame
the two halves agree on is in
[the wire format](@/library/reference/wire-format.md).

### SACI

```rust,name=saci-connector-saci
use saci_connector::NodeIdentity;
use saci_connector_saci::{SaciSink, SaciSource};

SaciSource::bind(
    bind: &str,
    schema: Arc<Schema>,
    buffer: usize,
    max_frame_bytes: usize,
    identity: NodeIdentity,
) -> Result<Self>

source.local_addr() -> SocketAddr

SaciSink::connect(
    connect: &[String],
    schema: Arc<Schema>,
    identity: NodeIdentity,
    handshake_timeout: Duration,
) -> Result<Self>

sink.peers() -> Vec<SocketAddr>
```

Neither half takes a transformer: Arrow IPC is the wire format. `identity` is
the service, workflow and node this instance sits on, which the sink announces
in its hello and the source labels its own series with. Under `saci-service`
the host binds it from the config; a direct caller supplies it.
`SaciSink::connect` resolves every address and dials none, and the first peer
that accepts a session is the one it keeps.

### DataFusion

```rust,name=saci-connector-datafusion
use saci_connector_datafusion::DataFusionSource;

DataFusionSource::from_sql(&SessionContext, &str).await  -> Result<Self>
DataFusionSource::from_stream(SendableRecordBatchStream) -> Self
source.with_estimated_rows(rows: usize)                  -> Self
```

Source only, and it has no factory: it needs a live `SessionContext` that a
config file cannot express. [SQL results as a
source](@/library/service/datafusion.md) is the whole page.
