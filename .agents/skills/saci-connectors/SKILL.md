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
format are separate crates. Connectors: `saci-connector-channel`, `-datafusion`, `-file`, `-http`,
`-kafka`, `-nats`, `-postgresql`, `-redb`, `-s3`, `-saci`, `-tcp`, `-turso`. Transformers:
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
- `saci-connector-datafusion`: `DataFusionSource`. No factory: it needs a live `SessionContext`.
- `saci-connector-file`: `FileSource`, `FileSink`, local-file IO, format from a transformer.
- `saci-connector-http`: `HttpSource`, `HttpSink`, one GET spooled through a transformer, and one
  self-contained document per batch back out. No request at build.
- `saci-connector-kafka`: `KafkaSource`, `KafkaSink`, librdkafka-backed, one topic or several.
- `saci-connector-nats`: `NatsSource`, `NatsSink`, core subject pub/sub or JetStream, chosen by a
  mode node's `kind` key.
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

- `connector-channel`, `connector-file`, `connector-http`, `connector-kafka`, `connector-nats`,
  `connector-postgresql`, `connector-redb`, `connector-s3`, `connector-saci`, `connector-tcp`,
  `connector-turso`:
  one per connector
  crate. Each pulls the crate in and registers its factories in `register_builtin_factories`
  (each implies `service`). `connector-channel`, `connector-file`, `connector-http`,
  `connector-tcp`, `connector-saci` and `connector-redb` are in `default`: an in-process channel,
  local disk, an
  HTTP client with no server of its own, a raw socket, a peer SACI service, and an embedded
  key/value file on local
  disk. Nothing has to be installed or already running for any of
  them, and none needs an extra build toolchain. HTTP, TCP and the peer link speak the network,
  and they qualify because they are generic primitives tied to no server product. `connector-kafka`,
  `connector-nats`, `connector-postgresql`, `connector-s3` and `connector-turso` each need a
  specific broker, database or object store running first, so each is opt-in instead. `all` turns
  on every one of the eleven. `connector-kafka` and `connector-nats` imply `transformer-ndjson` and
  `connector-tcp` implies `transformer-arrow-ipc`, so the connector feature alone is runnable. No
  connector resolves a format implicitly: a byte-carrying node names a declared `transformer`
  node's id, and that node's own `format` key is what the registry resolves. `connector-file`,
  `connector-http`, `connector-redb` and `connector-s3` imply no transformer, so the config picks
  which formats the binary carries. `connector-saci` implies none and resolves none, and turns on
  its crate's `trace-context` and `metrics` features instead.

`saci`:

- `connector-channel`, `connector-file`, `connector-redb`, `connector-http`, `connector-tcp`,
  `connector-saci`, `connector-datafusion` (`connectors` enables all seven): one per connector
  crate that needs
  nothing installed or already running. Deliberately excludes `saci-connector-kafka`, `-nats`,
  `-postgresql`, `-s3` and `-turso`, each of which requires a specific broker, database or object
  store installed and running first.

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

- `examples/connectors/`: one-off connector-crate examples, `postgres_roundtrip.rs`,
  `datafusion_interop.rs`, `scheduler_parquet_etl.rs`.
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
`connector_matrix` job) covers 2178 cases, one per `{source connector, sink connector, byte
format, processor runtime}` tuple over 11 connectors on each end, 6 formats (the five
transformers plus the absence of one, for the four connectors that carry `RecordBatch`es
natively) and 3 processor runtimes (native pipeline, WASM component, native plugin), plus one
maximal workflow declaring every node kind at once. One format applies to both the source and
the sink of a case; the independent source-format x sink-format cross product (39204 cases) is
deliberately not taken, because a mixed pair adds no connector or transformer coverage over the
two paired cases that already cover each half. `full_matrix` is the one test here that starts a
container: nextest gives each test *binary*, not each test, its own process, so it alone starts
exactly one container per external resource (Kafka, NATS, PostgreSQL, MinIO/S3) for its whole
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
`dimensions_cover_the_registry`, a Docker-free, non-`#[ignore]`d test asserting the real factory
registry's source count, sink count and registered transformer formats agree exactly with that
file's `CONNECTORS`/`FORMATS` lists; it starts no container and runs under the `default`
profile like any other fast test.

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
