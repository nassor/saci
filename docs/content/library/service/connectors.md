+++
title = "Writing a connector"
description = "Implement Source, Sink and their factories, and register them."
template = "page.html"
weight = 1
+++
# Writing a connector

A connector is reached either by calling its constructor in Rust and handing the
result to `Pipeline::source` or `Pipeline::sink`, or by naming its `type` string in the service
config and letting the factory build it. The second path needs the connector's
feature on `saci`, because `register_builtin_factories` only installs
what is compiled in, and it registers a `connector-*` feature's factories only when `service` is
also on.

```toml,name=One dependency, one feature per connector
# From crates.io.
saci = { version = "0.1", default-features = false, features = ["connector-file", "codec-csv"] }

# In a clone of the repository.
saci = { path = "crates/saci", default-features = false, features = ["connector-file", "codec-csv"] }
```

Each connector is a module under `saci::connector`, gated by its `connector-<name>` feature, and
every one of them turns on `connector-api`, which implies `engine`, so the `Source` and `Sink`
traits and the factory contract arrive with it. A connector compiles without wasmtime, axum or openraft in
the graph. The config keys each node accepts, and the errors they raise, are on
[Sources and sinks](@/service/connectors/_index.md).

## Writing your own

A connector is a module in `crates/saci/src/connector/<name>.rs` (with `connector/<name>/` for
its submodules), declared in `crates/saci/src/connector.rs` under
`#[cfg(feature = "connector-<name>")]`, and enabled by a `connector-<name>` entry in the
`[features]` table of `crates/saci/Cargo.toml`. Its factories register in
`service/factories.rs`.

A connector outside the repository is a crate that depends on `saci` with `connector-api`
for `SourceFactory` and `SinkFactory`, and on nothing else of SACI. Both traits build through
`fn build(&self, config: &ConfigValue, ctx: &ConnectorContext)`, where `ctx.codec(what)`
resolves the node's codec against the codec registry so no connector
owns a list of formats. A custom binary then chains `register_source` or
`register_sink` onto `register_builtin_factories`, or passes them to
`saci::service::cli::main_with`. Registering a `type_name`
that is already taken replaces the earlier factory.

[Embedding saci-service](@/library/service/_index.md) has the factory and the
builder call side by side.

## The built-in constructors

Ten modules, and the constructors are not uniform.

### Channel

```rust,name=saci::connector::channel
use saci::connector::channel::{ChannelSink, ChannelSource};

ChannelSource::new(Arc<Schema>, buffer: usize) -> (Self, mpsc::Sender<RecordBatch>)
ChannelSink::new  (Arc<Schema>, buffer: usize) -> (Self, mpsc::Receiver<RecordBatch>)

ChannelSource::with_receiver(Arc<Schema>, mpsc::Receiver<RecordBatch>)              -> Self
ChannelSink::with_sender    (Arc<Schema>, buffer: usize, mpsc::Sender<RecordBatch>) -> Self
```

`new` returns the paired endpoint. `with_receiver` and `with_sender` wrap an
endpoint resolved elsewhere, which is how the channel registry builds the two
halves of a bridge.

`ChannelSink`'s `Sink::pending_rows`
reports the rows of every batch the channel holds and the consumer has not
received. The sink sums the row counts of the messages still in flight, so the
number is a row backlog whatever size the batches are.

### File

```rust,name=saci::connector::file
use saci::connector::file::{
    FileSink, FileSinkConfig, FileSource, FileSourceConfig,
};

FileSource::new      (FileSourceConfig, Option<Arc<Schema>>, Arc<dyn Codec>) -> Result<Self>
FileSource::new_async(FileSourceConfig, Option<Arc<Schema>>, Arc<dyn Codec>) -> Result<Self>
FileSink::new        (FileSinkConfig,   Arc<Schema>,         Arc<dyn Codec>) -> Result<Self>
```

`FileSourceConfig::new(path)` names the file. `FileSinkConfig::new(path)` appends to
it, and `truncate: true` replaces it. `new_async` moves the open and the
metadata read off the executor. `FileSource::estimated_rows` forwards what
the reader reported at open time, so it is `Some` only for a format that counts
rows without reading them.

### HTTP

```rust,name=saci::connector::http
use saci::connector::http::{
    HttpListenConfig, HttpListenSource, HttpSink, HttpSinkConfig, HttpSource,
    HttpSourceConfig,
};

HttpSource::new      (HttpSourceConfig, Option<Arc<Schema>>, Arc<dyn Codec>) -> Result<Self>
HttpSink::new        (HttpSinkConfig,   Arc<Schema>,         Arc<dyn Codec>) -> Result<Self>
HttpListenSource::new(HttpListenConfig, Arc<Schema>,         Arc<dyn Codec>) -> Result<Self>
```

`HttpSourceConfig::new(url)`, `HttpSinkConfig::new(url)` and
`HttpListenConfig::new(bind)` take the one required field. The source and the
sink carry the transport in a `client: HttpClientConfig` field: HTTP version,
TLS files, headers and the request timeout. The source's schema is the declared
one, so it is `Option`; the sink's and the listener's are not.
No constructor makes a request, so a build needs no reachable endpoint.
`HttpListenSource::new` binds its listener. `HttpSource::estimated_rows`
forwards what the reader reported, so it is `None` until the body has arrived
and `Some` after it only for a format that counts rows without reading them.

### Kafka

```rust,name=saci::connector::kafka
use saci::connector::kafka::{
    KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig,
};

KafkaSource::new(KafkaSourceConfig, Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
KafkaSink::new  (KafkaSinkConfig,   Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
```

Both are synchronous and open no connection. Each one validates the config, asks
the codec for its message shape, and builds a client. `librdkafka` connects
lazily, so `saci-service validate` stays broker free.

### NATS

```rust,name=saci::connector::nats
use saci::connector::nats::{
    NatsSink, NatsSinkConfig, NatsSource, NatsSourceConfig,
};

NatsSource::new(NatsSourceConfig, Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
NatsSink::new  (NatsSinkConfig,   Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
```

Both are synchronous and open no connection. Each one validates the config and
asks the codec for its message shape. Connecting, provisioning and creating
the consumer happen on the first `next_batch` or `write_batch`, so
`saci-service validate` stays server free. Every mode struct carries a `Default`
matching the config defaults, so a config built in Rust names only the keys it
changes.

### PostgreSQL

```rust,name=saci::connector::postgresql
use saci::connector::postgresql::{
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

```rust,name=saci::connector::s3
use saci::connector::s3::{S3Sink, S3Source};

S3Source::new(S3SourceConfig, Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
S3Sink::new  (S3SinkConfig,   Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
```

Both constructors are synchronous and open no connection; the first request
happens inside the first call.

### TCP

```rust,name=saci::connector::tcp
use saci::connector::tcp::{TcpSink, TcpSinkConfig, TcpSource, TcpSourceConfig};

TcpSource::new(TcpSourceConfig, Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
source.local_addr() -> SocketAddr

TcpSink::new(TcpSinkConfig, Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
sink.peer_addr() -> SocketAddr
```

`TcpSourceConfig::new(bind)` carries `buffer` (default 64) and
`max_frame_bytes` (default 8 MiB); a `0` for either is a configuration error.
`TcpSinkConfig::new(connect)` names the address to dial. `local_addr` resolves
an ephemeral port, `peer_addr` the dialled one. The frame the two halves agree
on is in [the wire format](@/library/reference/wire-format.md).

### UDP

```rust,name=saci::connector::udp
use saci::connector::udp::{
    OnFailure, ProbeConfig, ProbeKind, UdpSink, UdpSinkConfig, UdpSource,
    UdpSourceConfig,
};

UdpSource::new(UdpSourceConfig, Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
source.local_addr() -> SocketAddr

UdpSink::new(UdpSinkConfig, Arc<Schema>, Arc<dyn Codec>) -> Result<Self>
sink.peer_addrs() -> Vec<SocketAddr>
```

`UdpSourceConfig::new(bind)` carries `multicast_group` and `buffer` (default 64,
at least 1). `UdpSinkConfig::new(connect)` takes one address or several and
carries `broadcast`, `max_datagram_bytes` (default 65507, 1 to 65507) and
`probe: Option<ProbeConfig>`. A `ProbeConfig` holds `kind`, `interval_ms`,
`timeout_ms` and `on_failure`; `ProbeConfig::default()` is the bare `probe`
node.

`UdpSource::new` binds the socket and joins the group; `UdpSink::new`
resolves every address and opens nothing. One datagram carries one message and
no prefix, and a zero-length datagram is the sink's liveness probe, which the
source answers and never decodes.

### Peer

```rust,name=saci::connector::peer
use saci::connector::NodeIdentity;
use saci::connector::peer::{PeerSink, PeerSinkConfig, PeerSource, PeerSourceConfig};

PeerSource::new(PeerSourceConfig, Arc<Schema>, NodeIdentity) -> Result<Self>
source.local_addr() -> SocketAddr

PeerSink::new(PeerSinkConfig, Arc<Schema>, NodeIdentity) -> Result<Self>
sink.peers() -> Vec<SocketAddr>
```

Neither half takes a codec: Arrow IPC is the wire format. `identity` is
the service, workflow and node this instance sits on, which the sink announces
in its hello and the source labels its own series with. Under `saci-service`
the host binds it from the config; a direct caller supplies it.
`PeerSourceConfig::new(bind)` carries `buffer` and `max_frame_bytes`.
`PeerSinkConfig::new(connect)` takes one address or several and carries
`handshake_timeout_ms` (default 5000). `PeerSink::new` resolves every address
and dials none, and the first peer that accepts a session is the one it keeps.
