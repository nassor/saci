---
name: saci-connectors
description: Use when adding, changing, configuring, testing or documenting a SACI source or sink connector (saci-connector and every saci-connector-* crate), a SourceFactory or SinkFactory registration, ConnectorContext, a connector's rebuildable/self-healing contract, a connector's KDL config or example, or the connector matrix test.
---

# SACI connectors

## Contract

`saci-connector` holds the factory contract: `SourceFactory`, `SinkFactory`, `ConnectorContext`
(which carries the transformer the host resolved for this node, the optional channel bridge, and
the `NodeIdentity` the host binds to every connector it builds), the `ChannelBridge` trait itself,
`parse_schema_fields` and `parse_optional_schema_fields`. Both
factory traits also carry `rebuildable(&config) -> Result<(), &'static str>`, defaulting to
`Err(REBUILD_UNDECLARED)`: whether the host may build this connector a second time to heal it. The
crate sits below `saci-service`, so a connector needs no dependency on the host. `ConfigValue` is
the value type in the `SourceFactory`/`SinkFactory`/`TransformerFactory` signatures, so every
connector and transformer reaches it through `saci-connector` or `saci-transformer`, which
re-export it.

`NodeIdentity { service, workflow, node }` is where a connector instance sits: the `node.label()`
of the service running it (`node.name`, or the decimal `node.id` when unnamed), the workflow it is
declared in, and its own node id. `ConnectorContext::with_identity` binds it and
`identity(what) -> Result<&NodeIdentity, SaciError>` reads it, mirroring `transformer(what)`: a
connector built with none gets a configuration error naming itself. `Rebuilder` carries it, so a
heal rebuilds with the same identity; `builder.rs`'s `build_source_node`/`build_sink_node` pass
`Some(...)` and `dlq.rs` passes `None`, because a dead-letter store is not a peer-facing node.

`saci-core`'s `io` feature provides the `Source`/`Sink` traits and the schema-cast helpers. A
connector moves bytes and a transformer turns bytes into `RecordBatch`es and back, so transport and
format are separate crates. Connectors: `saci-connector-channel`, `-clickhouse`, `-file`, `-http`, `-kafka`,
`-nats`, `-opentelemetry`, `-postgresql`, `-redb`, `-s3`, `-saci`, `-tcp`, `-turso`, `-udp`. Transformers:
`saci-transformer-arrow-ipc`, `-avro`, `-csv`, `-ndjson`, `-parquet`, each registered under its own
`format` name. A byte-carrying source or sink node names a declared `transformer` node's id;
`ServiceBuilder` resolves that node's `format` against the `TransformerRegistry` in
`saci-transformer` once per workflow build and hands the result to the factory inside a
`ConnectorContext`, so no connector resolves a format itself. `saci-service` owns the `Registry`
and `register_builtin_factories`, extended through `register_source`, `register_sink`, and
`register_transformer`. Pipelines integrate via `drain_into_dataset` and `drain_dataset`.

- `Source::request_batch_rows` is the advisory counterpart `KafkaSource`, `NatsSource` and
  `PostgresSource` act on; every other source is still governed, reshaped with a zero-copy
  `RecordBatch::slice`. The admission policy itself, including the windowed-path exception that
  withholds the hint, is in skill `saci-service`.
- `saci_sink_pending_rows` is the same number the adaptive flow controller reads as congestion,
  taken from `Sink::pending_rows`; a sink that answers `None` records nothing rather than zero.
- `Source::finish` is the source-side counterpart of `Sink::finish`: called once, after the last
  `next_batch`, for a source whose delivery is a commitment. The default does nothing, `Box`,
  `CastingSource`, `RetryingSource` and `HealingSource` all forward it, and both runners call it
  per exit path after the sinks are finished, so a source that consumes what it handed over does
  it only once the downstream write landed. A source dropped without it consumes nothing, which
  is a re-delivery rather than a loss. `RedbSource`'s `consume` mode is the only implementor
  today.
- `build_topology` copies connector options through a per-`type` allowlist keyed on the string
  `SourceFactory::type_name` returns, never a blanket copy of `SourceSpec.config`, which holds DSNs
  and credentials. Keys outside the allowlist are dropped, not masked.

## Self-healing contract

The full healing state machine and its config keys are in skill `saci-service`; this is the part a
connector author implements.

`Rebuilder` is the single connector-construction path: `build_source_node`/`build_sink_node`
build the initial instance through it too, so a heal cannot drift from the first build. It
captures `Arc<Registry>` plus the node's `type_name`, `config`, bound transformer, channel
bridge and `Option<SystemConfig>` (`None` for a stream-mode source, matching
`sources_take_retry_wrapper`).
Which nodes are reached is the factory's answer, not a list here: `builder::resolve_heal`
consults `SourceFactory::rebuildable`/`SinkFactory::rebuildable` for that node's own config.
An `Err` on a node that declares its **own** `heal` block is a load-time configuration error
(`workflow 'w' sink 's': heal is not available for type 'T': <reason>`); the same `Err` under
an inherited policy silently skips the node, because a channel half appears in most
multi-workflow configs and an inherited policy is not a request by name.

The other shape the host cannot build twice is a `ChannelSource` or `ChannelSink`, which
`ChannelRegistry` latches per name and refuses a second time, so `rebuild_blocker` reports the
workflow holding one as `restartable: false`.

## Crates

- `saci-connector-channel`: `ChannelSource`, `ChannelSink`, an in-memory mpsc transport.
  `Sink::pending_rows` reports the row backlog still queued, not the message count.
  `ChannelRegistry` is the name-keyed `ChannelBridge` pairing a sink in one workflow with a source
  in another.
- `saci-connector-file`: `FileSource`, `FileSink`, local-file IO, format from a transformer.
- `saci-connector-http`: `HttpSource`, `HttpSink`, `HttpPushSource`: two halves that dial and one
  that listens, each decoded and encoded by a transformer. `http_version` picks `"auto"`, `"1.1"`,
  `"2"` or `"3"` per node. reqwest carries HTTP/1.1 and HTTP/2, a direct `h3`/`h3-quinn`/`quinn`
  client carries HTTP/3, and `"auto"` never reaches it, because nothing here reads `Alt-Svc` and
  there is no h2c upgrade; `"2"` is prior knowledge, so it is h2c on an http url, and `"3"` needs
  an https one. `tls { ca_file cert_file key_file }` adds a private CA as an extra root and
  presents a client certificate on the two dialling halves. Every rustls configuration is built on
  the `ring` provider, so nothing here reaches `aws-lc-rs`. No request at build.
  - `mode kind` is the source's read shape. `document` is one GET spooled to a temp file and read
    through the transformer's stream surface; it reports EOF at the end of that stream and re-arms,
    so `interval` re-reads the resource on every tick. `lines` reads a body that never ends as
    newline-delimited records, `events` reads one as `text/event-stream` and takes `event` to
    filter to one event name; both go through the message surface, close a batch at `batch_size`
    frames or `poll_timeout_ms`, re-request a body that ended after `reconnect_ms`, require
    `schema_fields`, and are live sources only the stream runner drives, alongside `tcp`, `udp`,
    `saci` and `HttpPushSource`. `max_body_bytes` caps a document body, or one frame in a
    streaming kind.
  - `pagination "link"` follows `Link: <...>; rel="next"` as one read. A relative target resolves
    against the url the response came from, the last hop when the GET was redirected, as RFC
    8288's link context requires, and a next link naming either end of that redirect is refused as
    a page pointing back at itself. `conditional #true` sends the `etag` and `last-modified` the
    last response carried, so an unchanged resource answers `304` and decodes nothing. Both are
    `kind="document"` only, as is `schema_from "body"`.
  - The sink sends one self-contained document per batch and derives its `content-type` from the
    transformer's format (`text/csv`, `application/x-ndjson`,
    `application/vnd.apache.parquet`, `avro/binary`, `application/vnd.apache.arrow.stream`) unless
    `headers` names one. `content_encoding` `"gzip"`, `"br"` or `"zstd"` compresses the whole body
    and declares it, refused next to a `content-encoding` header. Response bodies are decompressed
    transparently, and a `headers` entry for `accept-encoding` is the opt-out.
  - Redirects split by half. The source takes reqwest's own policy, ten hops with a `301`, `302`
    or `303` followed as a `GET`, which is what it sent anyway. The sink follows `307` and `308`
    only, the two that keep the method and the body, and reports every other 3xx as
    `HttpSink: status 302 Found from {url}`: following one would send a bodyless `GET` and report
    the new target's `200` for a batch that never arrived. The HTTP/3 client follows nothing.
  - `HttpPushSource` binds at build the way `tcp` and `udp` do and never reaches EOF. One
    `hyper-util` auto listener serves HTTP/1.1 and h2c on `bind`, both sub-builders carrying a
    `TokioTimer`, which is what arms hyper's 30 second HTTP/1 header read timeout: with no timer
    an idle keep-alive connection holds its handler, and the ingest channel with it, for as long
    as the peer keeps the socket open. A `tls { cert_file key_file }` block terminates TLS with
    ALPN `h2` then `http/1.1`, and `quic_bind` adds HTTP/3 on its own UDP port, advertised as
    `alt-svc` on every TCP response. `ca_file` is refused there: callers are authenticated by
    `auth { bearer }`, the scheme matched without regard to case and the token compared in
    constant time. One request is one whole document on `path` with a method `methods` lists,
    accepted whole or not at all because every batch it decoded to is queued in one send: `202`
    once the document is queued, `404` another path, `405` with `allow`, `401` a missing or wrong
    token, `413` over `max_body_bytes`, announced or discovered, `400` both a document the format
    could not read (also sent down the channel, so the runner counts it) and a body the transport
    could not deliver, `500` only a spool failure. A retry of a request the source refused, any
    4xx or 5xx, duplicates no rows; a retry after the client stopped waiting for the `202`, say
    on a timeout while the channel is full, can, because hyper polls that request to completion
    and queues its document anyway, so ingest is at-least-once. `buffer` counts queued documents
    rather than batches, and is the one lenient key, clamped to at least 1 and defaulting to 64,
    matching `tcp`. A connection has 30 seconds, the same figure hyper gives a request head, to
    finish its TLS handshake or to send bytes that decide the protocol; one that stalls before
    that is dropped, so it holds no handler and no ingest sender. Under `tls` the negotiated ALPN
    picks `http1_only` or `http2_only`, so nothing is sniffed at all.
  - Every optional key of all three halves is refused when it is present with the wrong type,
    `timeout_ms` and the sink's `method` included, and an `auth` table with no `bearer` key names
    that key. `buffer` is the sole lenient exception.
  - Series, on the process-global meter with no registration: `saci_http_requests_total`
    (`role` = `source`, `sink` or `push`, plus `method` and `status`),
    `saci_http_request_errors_total` (`kind` = `transport`, `status`, `decode`, `size` or `auth`),
    `saci_http_request_duration_seconds` and `saci_http_bytes_total` (`direction` = `in` or `out`),
    each also labelled `workflow` and `node` once the host bound a `NodeIdentity`. The crate's two
    features are `metrics`, which emits them, and `tracing`.
- `saci-connector-kafka`: `KafkaSource`, `KafkaSink`, librdkafka-backed, one topic or several.
- `saci-connector-nats`: `NatsSource`, `NatsSink`, core subject pub/sub or JetStream, chosen by a
  mode node's `kind` key.
- `saci-connector-opentelemetry`: `OpenTelemetryMetricsSource`,
  `OpenTelemetryMetricsSink`, OTLP/HTTP metrics in and out, Protobuf or JSON, and no `transformer`:
  an OTLP request is the wire format, so the connector decodes and encodes it itself the way `saci`
  owns Arrow IPC.
  - Rows are the connector's Arrow view of OTLP, documented in its `schema` module.
    `schema::metrics_schema` is the ten-column canonical schema a source with no `schema_fields`
    emits, one row per data point (name, value, timestamp, kind, unit, description, attributes,
    resource_attributes, count, start_timestamp). `schema_fields` replaces it: each declared column
    is filled from the canonical column of the same name, and otherwise from the attribute of that
    name, and the metric value goes to `value` or to the first declared numeric column no canonical
    name claims. A request that leaves a `nullable=#false` column without a value is a 400.
  - Four reserved keys inside the `attributes` object carry what has no column, and a sink consumes
    each: `otel.scope`, `otel.aggregation_temporality`, `otel.monotonic`, `otel.distribution` (a
    histogram's bounds and counts, an exponential histogram's scale, zero count and both bucket
    sets, or a summary's quantiles). A `bytesValue` attribute is `{"otel.bytes": "<base64>"}`, and
    a non-finite double is the string `"NaN"`, `"Infinity"` or `"-Infinity"`. Exemplars,
    `Metric.metadata`, both `schema_url` fields and every `dropped_attributes_count` are not
    carried at all.
  - Source modes: `push` (the default, and what the top-level `bind`/`path`/`auth`/`buffer`/
    `max_body_bytes` shorthand builds) binds `127.0.0.1:4318` and `/v1/metrics` and never reaches
    EOF, so it is live and only the stream runner drives it; `pull` reads `mode.url` once with
    `stop_at_end`, or every `mode.interval_ms`. A shorthand key beside a `mode` block is refused.
    `path` must be absolute, `auth.bearer` must not be empty and `interval_ms` must be at least 1
    unless `stop_at_end` is set; `buffer` is the one lenient key, clamped to at least 1.
  - The listener answers `200` with an `ExportMetricsServiceResponse` for a body it took, and a
    body it took in part carries `partial_success` naming the data points no column holds.
    `401` a missing or wrong bearer token, `404` another path, `405` another method with
    `allow: POST`, `413` over `max_body_bytes`, `415` a `content-type` that is neither
    `application/x-protobuf` nor `application/json` (an empty one means protobuf), and `400` a body
    that is not an OTLP request in its own encoding. A `400` is also sent down the ingest channel,
    so the runner counts it. One body queues every row it converted in one send, so a retry of a
    refused request duplicates no row.
  - Sink keys: `url` (a base url gains `/v1/metrics`), `encoding` `protobuf` or `json`,
    `headers`, `timeout_ms`, `service_name` (the `service.name` resource attribute, unless a row's
    own resource attributes name one), `metric_name` (the fallback name, `saci_metric`),
    `batch_rows`, which splits a larger batch into several exports, and `schema_fields`, which both
    a source and a sink parse into the schema they report to the workflow graph check; the
    conversion itself reads a batch's own columns by name, so a declaration changes no row. Rows are
    grouped into one resource, scope and metric per distinct resource attributes, scope, and metric name, kind,
    unit, description, temporality and monotonic flag; a `sum` row defaults to a cumulative sum
    that may decrease. A non-finite double is refused, and a `u64` beyond `i64::MAX` goes as a
    double. Any status outside 2xx is a `SaciError::Store` naming the collector's own reason.
  - `Source::finish`, `request_batch_rows` and the metrics feature are not used: an export is
    awaited inside `write_batch`, so `Sink::pending_rows` answers `Some(0)`.
- `saci-connector-postgresql`: `PostgresSource`, `PostgresSink`, native (non-JDBC) client, TLS, and
  `polling` / `cdc_trigger` / `cdc_logical` read modes. Every PostgreSQL type is read/write through
  `ROWS` in `src/types.rs`; an unmatched type is canonical text. `schema_fields`'s `pg_type` forces
  and asserts the type; `list` plus `item` cover one-dimensional arrays. `tests/all_types.rs` is
  the Docker round-trip per type family.
- `saci-connector-turso`: `TursoSource`, `TursoSink`, the in-process, SQLite-compatible `turso`
  engine, embedded (`connection.path`) or as a synced replica of Turso Cloud / sqld
  (`connection.remote`, pulled and pushed through `turso::sync`). Source modes `polling` (durable
  cursor), `dump`, and `cdc` (the change table a capture-enabled connection writes); sink write
  modes `append` / `upsert` / `ignore_conflicts` over a `transaction` of `deferred` / `immediate` /
  `concurrent` (MVCC), plus `capture` and page-level `encryption`. Pins `turso = "=0.7.2"` with
  `default-features = false, features = ["sync"]`: the crate's default `mimalloc` feature would
  install a second global allocator.
- `saci-connector-clickhouse`: `ClickhouseSource`, `ClickhouseSink`, ClickHouse HTTP client with
  native `ArrowStream` format. Source modes `polling` (cursor column, optional initial cursor and
  tiebreak column, which resumes inside a repeated cursor value), `dump` (paginated full table
  scan), and `query` (arbitrary SQL statement); sink write mode `append` with optional
  `truncate_before_first_write`, one INSERT per `chunk_rows` rows. No transformer needed:
  ClickHouse sends and accepts Arrow IPC streams directly. A date/time cursor goes out as a
  ClickHouse datetime literal, and a `Date` column is widened from the `UInt16` ArrowStream sends
  to the declared `date32`. The client sets `output_format_arrow_compression_method = none`,
  because ClickHouse compresses an `ArrowStream` result with LZ4 by default and `arrow-ipc/lz4` is
  enabled only under the `conformance` feature, so a normal build refuses a compressed batch by
  design.
- `saci-connector-redb`: `RedbSource`, `RedbSink`, an embedded redb key/value file as a
  transport, `directory`/`file`/`table` naming it. One entry per non-empty batch, keyed
  `{key_prefix}{seq:020}{key_suffix}` so lexicographic key order is insertion order; the sink
  reads the highest matching key back at open and continues the sequence, and refuses a key that
  already exists. Write-safety knobs default on (`durability "immediate"`, `two_phase_commit`,
  `quick_repair`, `compact` at `finish`); `check_integrity` defaults off. The source is
  read-once and requires `schema_fields`, which it always hands the format as a projection
  target.
  - Locking: the sink opens `Builder::open` (read-write, exclusive OS lock) from build to
    `finish`; the source opens `Builder::open_read_only` (`ReadOnlyDatabase`, shared lock) on its
    first batch and drops it at EOF. N sources may read one file at once and need no write
    permission; a sink excludes every other handle.
  - `ReadOnlyDatabase::new` never repairs: a file whose last write left no allocator state table
    is refused with a message naming `check_integrity`. Only a sink with both `quick_repair` and
    `two_phase_commit` off, killed before `finish`, reaches that state, because `quick_repair`
    forces `two_phase_commit` on and `Database::drop` writes the table on a clean close.
    `check_integrity #true` (which opens read-write, so it also needs a writable file) or one
    sink run repairs it.
  - `consume #true` (source only) records every key whose entry was handed over whole and deletes
    them in `Source::finish`, in one read-write transaction taken after both handles are
    released, so the file is a queue instead of a table. A key that left the list any other way
    is never recorded, an instance dropped without `finish` deletes nothing, and a failed delete
    keeps the keys so a later `finish` retries. `finish` on a partial drain is legal and leaves
    the entry still being decoded: dropping the receiver ends that thread, and the database
    `Arc` never leaves the `spawn_blocking` that read the entry, so `finish` holds its last
    handle. This is the mode the
    dead letter queue's redb store runs its source half in.
  - `resume_seq` scans `range(prefix..).rev()` and skips a non-matching key rather than stopping:
    every key above the prefix region sorts ahead of the matching ones in reverse, so stopping
    there would reset the sequence and collide on the next write. `collect_keys` scans forward,
    where `break` on the first non-matching key is both correct and the early exit.
- `saci-connector-s3`: `S3Source`, `S3Sink`, any S3-compatible endpoint, timestamped object keys,
  row/byte/age flush thresholds.
- `saci-connector-tcp`: `TcpIngestSource`, `TcpSink`, live length-prefixed frames, decoded and
  encoded by a transformer. The source listens, the sink dials, both register as `"tcp"`. Framing
  is transport, decoding is format.
- `saci-connector-udp`: `UdpSource`, `UdpSink`, fire-and-forget datagrams, one message each with
  no length prefix, decoded and encoded by a transformer. The source binds, the sink sends, both
  register as `"udp"`.
  - A zero-length datagram is the liveness probe: the source answers one with a zero-length
    datagram to its sender and never decodes it, and the sink never sends an empty payload as
    data. A datagram the format cannot decode, and one that decodes to no rows, is dropped with
    a warning and the decoder is replaced, because there is no connection to close and the
    datagram boundary is the frame for any decoder.
  - The sink's `probe` block (`kind` `echo` or `icmp`, `interval_ms`, `timeout_ms`, `on_failure`
    `refuse` or `warn`) is the only thing that notices a peer. `echo` waits for a datagram back,
    which only another `udp` source sends; `icmp` reads only the OS's relayed unreachable, so
    silence is its one "alive". Under `refuse` a peer that failed its last round is skipped, and
    a batch no peer accepted is a `write_batch` error, which is what `retry`, `heal` and `dlq`
    act on. A batch the format encodes to no messages succeeds whatever the peers are doing. A
    probe needs one answering peer, so it is refused next to `broadcast` or a multicast
    `connect` address.
  - A round drains the socket, then sends, then sends once more if that first send collected an
    unreachable report: each OS holds one report per socket and hands it to the next call, which
    may be a probe rounds after the datagram that earned it. `Peer::send_all` retries a data
    send once for the same reason. Every peer of a round is probed at the same time
    (`futures::future::join_all`, one buffer each), so a round costs one `timeout` however many
    peers there are, and the background loop is a `tokio::time::interval` with
    `MissedTickBehavior::Delay`.
  - `connect` takes one address or a list; every message goes to every peer and the write fails
    only when the batch reached nobody, so a live peer never sees the runner's retry of a batch
    it already took. `max_datagram_bytes` (1 to 65507, the IPv4 payload maximum) is checked
    before the first send, so a batch holding a larger message delivers none of it. The source
    takes `multicast_group`, one group of the `bind` address's family, and one receiver per port
    per host: tokio's socket exposes no address reuse option.
  - Every optional key of both halves is refused when it is present with the wrong type, the way
    `max_datagram_bytes` is: `multicast_group`, `broadcast`, and the probe's `kind`,
    `interval_ms`, `timeout_ms` and `on_failure`. `buffer` is the one lenient key, clamped to at
    least 1 and defaulting to 64, matching `tcp`.
  - Reconnecting is the host's `heal` block and nothing else: a rebuilt sink re-resolves every
    `connect` address and opens fresh sockets.
- `saci-connector-saci`: `SaciSource`, `SaciSink`, one standalone service pushing
  `RecordBatch`es to another. Both register as `"saci"` and neither takes a `transformer`: Arrow
  IPC is the fixed wire format, so `ConnectorContext::transformer` is never called and only
  `identity` is.
  - `wire.rs` is public, because the protocol is: a `u32` big-endian body length, then a kind
    byte. `1` hello (`u8` version, `str` service, `str` workflow, `str` sink, `u32` schema length,
    an Arrow IPC Schema message), `2` accept, `3` reject (`str` reason), `4` data (`str`
    traceparent, length `0` for none, then Arrow IPC stream bytes for one batch). `PROTOCOL_VERSION`
    is 1. `encode_frame`/`decode_frame` do whole frames, `data_header` writes only the prefix so a
    batch's buffers reach the socket unstaged, and `hello_version` peeks the version before the
    rest is decoded.
  - The handshake: the source reads the hello, then refuses an unsupported version, a first frame
    that is not a hello, or a schema whose fields are not its own. Each refusal is a `reject`
    frame the sink turns into a `SaciError::Configuration`, because those are disagreements
    between two config files. A dial that fails, errors, times out past `handshake_timeout_ms`, or
    answers something else is a failed dial instead: the sink tries the next `connect` address,
    and reports `SaciSink: no peer accepted a session:` with every address when none is left.
  - One `StreamEncoder` per session on the sink and one `StreamDecoder` per session on the source,
    so the Arrow IPC schema message is emitted once and the decoder never sees a second one. A
    failed write drops the session rather than keeping it, so the runner's retry redials and
    resends that batch, possibly to the next peer; the peer that saw a partial frame closes that
    session itself. There is no acknowledgement frame, so a batch the socket already accepted is
    not confirmed by the peer and one lost with the session is not resent.
  - Source-side series only, since the host's `saci_sink_*` already cover the sink:
    `saci_peer_source_sessions_total` (`outcome`), `_batches_total`, `_rows_total`, `_bytes_total`
    and `_errors_total` (`kind` = `frame`, `decode`, `schema`), each labelled `workflow` and
    `source` plus `peer_service`/`peer_workflow`/`peer_sink` once a hello parsed.
  - Spans `peer.send` and `peer.receive` carry the same peer fields. Both are `debug_span!`s, the
    level `workflow.batch` opens at, so both also need `observability.log_level="debug"` (or an
    enabling `RUST_LOG`) on their service; the default `log_level="error"` records neither. The
    `trace-context` feature (on under `saci-service`'s `connector-saci`) puts the sender's span
    into the frame as a W3C `traceparent` and adopts it on receipt, which is effective only when
    both services also set `observability.otlp_endpoint`, since that is what installs the
    OpenTelemetry layer.

## Features

`saci-service`:

- `connector-channel`, `connector-clickhouse`, `connector-file`, `connector-http`, `connector-kafka`,
  `connector-nats`, `connector-opentelemetry`, `connector-postgresql`, `connector-redb`,
  `connector-s3`, `connector-saci`, `connector-tcp`, `connector-turso`, `connector-udp`:
  one per connector
  crate. Each pulls the crate in and registers its factories in `register_builtin_factories`
  (each implies `service`). `connector-channel`, `connector-file`, `connector-http`,
  `connector-tcp`, `connector-udp`, `connector-saci` and `connector-redb` are in `default`: an
  in-process channel, local disk, an
  HTTP client and a listening HTTP source, two raw sockets, a peer SACI service, and an embedded
  key/value file on local
  disk. Nothing has to be installed or already running for any of
  them, and none needs an extra build toolchain. HTTP, TCP, UDP and the peer link speak the
  network,
  and they qualify because they are generic primitives tied to no server product. `connector-kafka`,
  `connector-nats`, `connector-postgresql`, `connector-s3`, `connector-turso`, `connector-clickhouse` and
  `connector-opentelemetry` each need a
  specific broker, database, object store or OTLP endpoint running first, so each is opt-in
  instead. `all` turns
  on every one of the fourteen. `connector-kafka`, `connector-nats` and `connector-udp` imply
  `transformer-ndjson` and
  `connector-tcp` implies `transformer-arrow-ipc`, so the connector feature alone is runnable. No
  connector resolves a format implicitly: a byte-carrying node names a declared `transformer`
  node's id, and that node's own `format` key is what the registry resolves. `connector-file`,
  `connector-http`, `connector-redb` and `connector-s3` imply no transformer, so the config picks
  which formats the binary carries; `connector-http` turns on its crate's own `tracing` and
  `metrics` features as well. `connector-saci` implies none and resolves none, and turns on
  its crate's `trace-context` and `metrics` features instead.

`saci`:

- `connector-channel`, `connector-file`, `connector-redb`, `connector-http`, `connector-tcp`,
  `connector-udp`, `connector-saci` (`connectors` enables all seven): one per connector crate that
  needs nothing installed or already running. Deliberately excludes `saci-connector-kafka`,
  `-nats`, `-postgresql`, `-s3`, `-turso`, `-clickhouse` and `-opentelemetry`, each of which
  requires a specific broker, database, object store or OTLP endpoint installed and running first.

## Build prerequisites

`saci-connector-kafka` vendors librdkafka through `librdkafka-sys`'s `cmake-build` feature, so
`cmake` and a C toolchain (MSVC Build Tools on Windows) must be on `PATH` before `cargo build`
reaches it. On POSIX targets that build also needs libcurl's development headers
(`libcurl4-openssl-dev` on Debian/Ubuntu, the equivalent elsewhere; CI installs them in the `test`,
`distributed_chaos`, `wasm_processor` and `polyglot` jobs, every job that compiles a saci-service
test or example target): the `config.h` cmake generates defines `WITH_OAUTHBEARER_OIDC` as `0`
rather than leaving it undefined, and `rdkafka_conf.c` gates its `#include <curl/curl.h>` on
`#ifdef`, so that header is
compiled even though the build is configured with `-DWITH_CURL=0`. Windows builds escape it
because `WITHOUT_WIN32_CONFIG` leaves the macro undefined. No other repository build step needs
any of these.

## Examples and configs

- `examples/connectors/`: one-off connector-crate examples, `postgres_roundtrip.rs` and
  `scheduler_parquet_etl.rs`.
- `examples/configs/`: runnable KDL configs for the `saci-service` binary itself (not
  `[[example]]` targets), one per connector plus standalone and cluster templates. See its own
  `README.md` for the feature-to-config table.

`cargo xtask validate` and `cargo xtask demo` (`xtask/src/examples.rs`) inject a `variables` block
into example configs, so they run with no OS env export. `validate` also parses every
`examples/configs/*.kdl` file through `saci-service validate --connectors-only`, discovered from
the directory rather than a hand-maintained list.

## Tests

Docker-backed tests soft-skip when no daemon is reachable; the `try_start` convention is in
`AGENTS.md`'s Testing section.

`crates/saci-service/tests/connector_matrix.rs` (the `heavy-docker` test group, `ci.yml`'s
`connector_matrix` job) covers 3780 cases, one per `{source connector, sink connector, byte
format, processor runtime}` tuple over 15 source connectors and 14 sink connectors
(`HttpPushSource` registers a source and no sink, so the two dimensions are no longer one list),
6 formats (the five
transformers plus the absence of one, for the six connectors that carry `RecordBatch`es
natively) and 3 processor runtimes (native pipeline, WASM component, native plugin), plus one
maximal workflow declaring every node kind at once. One format applies to both the source and
the sink of a case; the independent source-format x sink-format cross product (22680 cases) is
deliberately not taken, because a mixed pair adds no connector or transformer coverage over the
two paired cases that already cover each half. `full_matrix` is the one test here that starts a
container: nextest gives each test *binary*, not each test, its own process, so it alone starts
exactly one container per external resource (Kafka, NATS, PostgreSQL, S3, ClickHouse) for its whole
run, isolating every case by a unique topic, subject, table, object prefix, file path or
OS-assigned port and running them all concurrently in-process. A rejected case asserts both the
refusal and where it lands: `build` (`ServiceConfig::load` or `ServiceBuilder::build_all`
returns an error containing a predicted fragment) or `run` (the service builds but the runner
reports a non-fatal error and no row reaches the sink). There is deliberately no third site for
a clean run that delivers nothing: that is what a silent bug looks like, so such a combination
gets fixed rather than recorded. A build refusal touches no live resource, so that coverage
holds with no Docker daemon at all; a case whose resource has no reachable container is skipped
individually instead, so Docker-free combinations still run. `full_matrix` is excluded from the
`default` profile and `#[ignore]`d, so `--profile ci` skips it too; it reaches CI only through
the `connector_matrix` job (`cargo nextest run -p saci-service --all-features --profile ci
--test connector_matrix --run-ignored ignored-only`). `--profile ci` is required there: `--test`
selects a cargo build target, not a nextest filter, so it cannot bypass `default`'s exclusion of
`full_matrix` itself. The job builds both the wasm and the native-plugin smoketest fixtures
first, and prints the test's own report (per-case grids, the supported/rejected/skipped/FAILED
totals, the maximal workflow) on a pass as well as a failure. The same file also holds
`dimensions_cover_the_registry`, a Docker-free, non-`#[ignore]`d test comparing the real
factory registry's `source_names`/`sink_names` against `CONNECTORS` through
`Connector::source_type`/`Connector::sink_type`, and its registered transformer `formats`
against `FORMATS`, as sorted name sets rather than counts; it starts no container and runs
under the `default` profile like any other fast test.

`saci-connector-turso::synced_roundtrip` follows the same shape for a remote endpoint rather than a
container: it soft-skips when `SACI_TURSO_URL` or `SACI_TURSO_TOKEN` is unset, when the probe
fails, or when the endpoint does not answer within its 30 s probe budget, and every step past the
seed panics, since
`turso::sync`'s hyper client sets no timeout of its own and a stall past the reachability gate is a
failure, not an absence.

Every Docker-backed test starts its own fresh container (or, for a Raft chaos test, its own
Toxiproxy container plus an N-node cluster with one proxy per directed edge) with OS-assigned host
ports and nanosecond-unique resource names (topics, subjects, streams). For the connector suites
that per-test isolation is the whole story, and it is what makes running them concurrently safe.
`cargo test`'s default behavior runs one test binary at a time to completion before starting the
next, so today's `kafka_roundtrip`/`nats_roundtrip`/`sink`/`source_cursor`/`source_logical`/
`all_types` binaries each get their own turn; nextest schedules every test from every binary into
one global thread pool (`test-threads`, default `num-cpus`), so Docker-backed tests from different
crates run alongside each other instead of one binary finishing before the next starts.

## Adding a connector

1. New crate `crates/saci-connector-<name>` with `version.workspace = true` and
   `{ workspace = true }` sibling deps, registered in the root `[workspace.dependencies]`.
2. Implement `SourceFactory` and/or `SinkFactory` from `saci-connector`, including `type_name` and
   `rebuildable`; read the format from `ConnectorContext`, never resolve one.
3. Add a `connector-<name>` feature to `crates/saci-service/Cargo.toml` that pulls the crate and
   registers the factories in `register_builtin_factories`; imply the transformer feature the
   connector needs to be runnable alone, the way `connector-kafka` implies `transformer-ndjson`.
4. Add the feature to `saci-service`'s `all` list in `crates/saci-service/Cargo.toml`, and to its
   `default` list only when nothing has to be installed or already running for the connector to
   work and it needs no extra build toolchain, the same condition step 5 applies to the `saci`
   facade. In `crates/saci-service/tests/feature_bundles.rs`, `all_bundle_lists_every_feature`
   asserts `all` lists every feature the crate declares except `default`, `all` and `conformance`,
   `all_bundle_excludes_the_conformance_switch` fails when `conformance` sits in `all`, and
   `default_bundle_requires_no_installed_service` fails when a connector that needs an installed
   broker, database or object store sits in `default`.
5. Add the feature to the `saci` facade only when nothing has to be installed or already running
   for the connector to work, and put it in that crate's `connectors` group in
   `crates/saci/Cargo.toml`, which is what `all` reaches. `all_bundle_reaches_every_feature` in
   `crates/saci/tests/feature_bundles.rs` expands `all` through the groups transitively and fails
   until every feature the facade declares is reachable from it.
6. Add the connector's safe option keys to the per-`type` allowlist in
   `crates/saci-service/src/service/topology.rs`. The key is the registered `type_name`, which
   `every_allowlist_key_is_a_connector_type_this_crate_ships` checks against
   `BUILTIN_CONNECTOR_FEATURES` in every build, so a misspelled or Rust-type-named key fails
   there rather than silently rendering no detail.
7. Add an entry to `CONNECTORS`, the capability table and the maximal workflow in
   `crates/saci-service/tests/connector_matrix.rs`; `dimensions_cover_the_registry` fails until
   this is done. A connector or transformer not in the matrix is not considered wired.
8. Docker-backed tests: `tests/common/mod.rs` with `try_start() -> Option<Container>`, and the
   crate's `tests/` added to the `default` profile exclusion in `.config/nextest.toml`.
9. A runnable config under `examples/configs/` and a page under
   `docs/content/service/connectors/` (skill `saci-docs`).
10. Update this skill: its `## Crates` bullet, `## Features`, and any new build prerequisite.

## Keep this skill current

Update this file in the same change that: adds or removes a connector crate; changes
`SourceFactory`, `SinkFactory`, `ConnectorContext`, `ChannelBridge` or `rebuildable`; adds or
renames a connector config key, read mode or write mode; changes a connector's build prerequisite;
changes the connector matrix's dimensions or rejection sites; changes `Source::request_batch_rows`,
`Source::finish` or `Sink::pending_rows` semantics; changes `rebuild_blocker`'s `restartable` rule for a
`ChannelSource`/`ChannelSink` pair (also check skill `saci-service`'s Service layer section, the
canonical copy); changes the IO layer contract a connector relies on to reach its transformer, or
the `Registry`'s `register_source`/`register_sink`/`register_transformer` surface (also check skill
`saci-transformers`); changes which bundle (`default`, `all`) a connector feature belongs to in
`saci-service`, or moves it into or out of the `saci` facade (also check skill `saci-service`);
changes which transformer feature a connector feature implies (also update skill
`saci-transformers`).
