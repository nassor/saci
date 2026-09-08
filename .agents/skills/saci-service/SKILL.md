---
name: saci-service
description: Use when changing, operating, testing or documenting the saci-service host or its dashboard: crates/saci-service/src/service/ (config, builder, runners, lifecycle, flow control, self-healing, HTTP control plane, logging and sampling), src/distributed/ (runner, checkpoints, Raft consensus, redb stores), src/metrics.rs, src/inspector/, the saci-service binary and its CLI, saci-inspector-wire, or the crates/saci-service-ui Leptos dashboard and its committed bundle.
---

# SACI service and dashboard

## Layout

- `saci-service`: the host. wasmtime runtime, distributed/Raft, HTTP control plane, config
  loading, the factory `Registry`, and the `saci-service` binary. It ships no `Source`, `Sink`, or
  `Transformer` of its own.
- `saci-service-ui`: the `/ui` live dashboard. CSR Leptos, wasm32-unknown-unknown only, so **not**
  a workspace member. Its committed bundle lives at `crates/saci-service/assets/ui/`, rebuilt by
  `cargo xtask ui`.
- `saci-inspector-wire`: the inspector's JSON contract, `Topology`, `Snapshot`, `SpanRecord` and
  friends, plus the lifecycle plane's `WorkflowRunState`, `WorkflowStatus` and
  `ServiceLifecycleReport`, and the dead letter queue's `DlqSummary`, `DlqGroup`, `DlqTrigger`,
  `DlqReplayReport` and `DlqReplayRequest`. serde only, so both the host and the browser can
  compile it;
  `saci-service` itself cannot target wasm32-unknown-unknown.

The wasm host (`src/wasm/`) is documented in skill `saci-processors` and the native plugin host in
skill `saci-plugins`; connectors and transformers in their own skills.

## Features

The **default** bundle is `mimalloc`, `service`, `wasm`, `windows`, `parquet-checkpoint`,
`connector-channel`, `connector-file`, `connector-http`, `connector-tcp`, `connector-saci`,
`connector-redb`, and
every transformer,
so `cargo install saci-service` yields a runnable binary with no flags. A connector may be default
only when nothing has to be installed or already running for it to work, and it needs no extra
build toolchain: `connector-channel` is in-process, `connector-file` reads the local filesystem,
`connector-redb` reads and writes one embedded key/value file on local disk,
`connector-http` is an HTTP client with no server of its own (one GET in, one request per batch
out), `connector-tcp` is a raw socket, and `connector-saci`'s peer is another `saci-service`.
HTTP, TCP and the peer link speak the network, and they qualify
because they are generic primitives tied to no server product. Every connector that needs a
specific broker, database or object store running first is opt-in instead: `connector-postgresql`,
`connector-nats`, `connector-s3`, `connector-kafka` and `connector-turso`. `connector-kafka` stays
opt-in because `librdkafka-sys` builds vendored C and needs `cmake` plus a C toolchain;
`connector-turso` stays opt-in because its synced mode pulls hyper and rustls; `distributed-raft`
and `service-cluster` stay opt-in because a cluster node is a deliberate deployment choice.

- `connector-channel`, `connector-file`, `connector-http`, `connector-kafka`, `connector-nats`,
  `connector-postgresql`, `connector-redb`, `connector-s3`, `connector-saci`, `connector-tcp`,
  `connector-turso`:
  one per connector
  crate. Each pulls
  the crate in and registers its factories in `register_builtin_factories` (each implies
  `service`).
  `connector-kafka` and `connector-nats` imply `transformer-ndjson` and `connector-tcp` implies
  `transformer-arrow-ipc`, so the connector feature alone is runnable. No connector resolves a
  format implicitly: a byte-carrying node names a declared `transformer` node's id, and that
  node's own `format` key is what the registry resolves. `connector-file`, `connector-http`,
  `connector-redb` and `connector-s3` imply no transformer, so the config picks which formats the
  binary carries. `connector-saci` implies none and resolves none, since Arrow IPC is its fixed
  wire format; it turns on its crate's `trace-context` and `metrics` features instead.
- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet`: one per transformer crate, registering its factory under its `format`
  name (each implies `service`).
- `parquet-checkpoint`: `ParquetCheckpointStore`, the archival checkpoint store (implies
  `distributed`).
- `windows`: forwards `saci-core/windows`.
- `metrics`: the OpenTelemetry instruments in `src/metrics.rs`. Pulls `dep:opentelemetry` plus
  `dep:saci-inspector-wire`, for the `SOURCE_ATTR`/`PROCESSOR_ATTR`/`SINK_ATTR`/`BRANCH_ATTR`/
  `WORKFLOW_ATTR` attribute keys, and stands alone: a library embedder can compile the writers
  without the HTTP control plane.
- `inspector`: the in-process telemetry in `src/inspector/`. Time-bounded ring buffers for spans,
  log events, metric samples and flow-control decisions: the first three fed by one `tracing`
  layer and one in-memory `PushMetricExporter`, the fourth written directly by the standalone and
  stream runners; read back through the JSON API in `src/service/inspector_api.rs` and the `/ui`
  dashboard. Implies `tracing` and `metrics`, and pulls `dep:saci-inspector-wire`,
  `dep:tracing-subscriber` and `dep:opentelemetry_sdk`. `service` implies it. Not under `service/`,
  so a library embedder can capture without the axum control plane.
- `wasm`: wasmtime host. `WasmEngine`, `WasmPipelineRuntime`, the `bindgen!` host bindings.
- `plugin`: native plugin host. `NativePluginRuntime` dlopens a shared library exporting the
  `saci-plugin-abi` C ABI, validates its manifest, and runs each batch through its `run_batch` slot.
- `distributed`: `PartitionSource`, `CheckpointStore`, `DistributedRunner`, `CheckpointStrategy`,
  plus `RedbSharedStore`, which serves both traits over a local redb file. This is the feature
  that pulls `dep:redb`, so `RedbSharedStore::single_node` is a working store with no consensus
  at all, which is what makes the two `distributed_*` examples runnable without a cluster.
- `distributed-raft`: the openraft node that replicates the application state:
  `ArrowRedbLogStore` (the raft log in its own redb file), `ArrowRedbStateMachine` (the
  application tables in `cluster-app.redb`), the driver, and the request/response TCP peer
  transport. `RedbSharedStore::multi_node` proposes every mutation through it (implies
  `distributed`).
- `service`: the `saci-service` binary. axum HTTP control plane, KDL config through
  `dep:saci-config`, metrics, standalone runner (implies `saci-core/io`, `distributed`,
  `parquet-checkpoint`, `tracing`, `metrics`, `inspector`). Also pulls `opentelemetry-otlp` and
  `tracing-opentelemetry` for OTLP span export, `dep:postcard` for the source cursors
  `RedbStateClient` persists, and `dep:saci-transformer-arrow-ipc`, which the dead letter queue
  names directly to encode a refused batch. `transformer-arrow-ipc` still owns the registration
  that lets a config name the format.
  It does **not** imply `distributed-raft`.
- `service-cluster`: cluster/Raft mode on top of `service` (implies `service`, `distributed-raft`).
  A cluster node keeps its state under `node.data_dir`, so a cluster binary needs no other
  feature: `--features service-cluster`.
- `all`: every capability this crate carries, including all five opt-in connectors, the native
  plugin host and a Raft cluster node (`connector-postgresql`, `connector-nats`, `connector-s3`,
  `connector-kafka`, `connector-turso`, `plugin`, `service-cluster`, on top of the default
  bundle). Excludes `conformance`: that feature turns on `arrow-ipc/lz4` so the corpus generator
  can write a compressed record batch, which the wire format otherwise rejects, so it is the
  corpus generator's own switch rather than a capability of the service.

## Distributed processing (`src/distributed/`)

Multi-instance batch execution with at-least-once semantics. `saci-core`'s `distributed` feature
contributes shared types only; all runner code lives here. The application state, meaning master
batches, row-range claims, checkpoints and instance heartbeats, is replicated by the SACI raft
itself and applied into each node's own redb file.

**`distributed` feature:**
- `PartitionSource`: claims, acks, and releases row-range batches across instances
- `CheckpointStore`: persists Arrow IPC snapshots for crash recovery
- `DistributedRunner` and `RunnerConfig`: holds a `Box<dyn PipelineRuntime>` template. Per claimed
  batch it calls `world_factory()` for a fresh `Dataset`, loads the window accumulator and the
  runtime's opaque state blob, calls `runtime.run_on_with_state(&mut partition_data, prior)`, then
  checkpoints and acks. The template's own data, sources, and sinks are never used.
- `CheckpointStrategy`: `EveryStage`, `EveryNStages`, `None`
- `MAX_LOG_ENTRY_BYTES` (1 MiB, `partition.rs`): caps the Arrow IPC payload of a registered master
  batch, and is the `CheckpointStore::max_checkpoint_bytes` trait default, which
  `RedbSharedStore` does not raise because a checkpoint travels inside a raft log entry
- `accumulator_store` and `processor_state_store`: free functions that park the window accumulator and
  the runtime state blob under reserved `stage_idx` sentinels (`ACCUMULATOR_STAGE_SENTINEL`,
  `PROCESSOR_STATE_STAGE_SENTINEL`)
- `ParquetCheckpointStore`: archival checkpoint store (needs `parquet-checkpoint`)

**`distributed-raft` feature** (`src/distributed/consensus/`), the openraft node and everything it
replicates:
- `types.rs`: `SaciTypeConfig` (`openraft::declare_raft_types!`), `ConsensusCommand` and
  `ConsensusResponse`. Every command carrying wall-clock time carries it as a `now_at_propose`
  field, so the state machine stays deterministic on replay
- `state_machine/`: the redb application tables (`arrow_master_batches`, `arrow_claims`,
  `arrow_claims_by_batch`, `arrow_checkpoints`, `arrow_instances`, `arrow_pending_batches`,
  `arrow_sm_meta`) plus the apply handlers, queries and snapshot IO. Records are JSON-encoded;
  only openraft-native persistence uses postcard
- `storage/`: `ArrowRedbLogStore` (`RaftLogReader` + `RaftLogStorage`, calling
  `IOFlushed::io_completed` only after the redb commit) and `ArrowRedbStateMachine`
  (`RaftStateMachine`, `SnapshotData = Cursor<Vec<u8>>`), plus `validate_store_consistency`
- `ArrowRaftDriver`, `ArrowRaftDriverConfig`, `ArrowRaftDriverHandle`:
  `start(config, log_db_path, app_db_path)` opens both redb files, validates them against each
  other, and spawns the proposal loop, whose `write_command` maps openraft's `ForwardToLeader`
  onto `transport::forward_proposal`. `ArrowRaftDriverConfig::raft_config` builds the openraft
  `Config`: cluster timings, `SnapshotPolicy::LogsSinceLast(snapshot_log_interval)` and
  `replication_lag_threshold` derived as `snapshot_log_interval.saturating_mul(2)`, which holds
  openraft's documented `replication_lag_threshold > LogsSinceLast` relation for any configured
  interval instead of leaving the library default of 5000 below it, with a cluster node filling it
  in through `driver_config` (`src/service/cluster.rs`) from the cluster header. The handle exposes
  `propose`, `app_db`, `metrics`, `initialize`, `spawn_tcp_server` and `shutdown`
- `transport/`: length-prefixed request/response TCP with a `serde_json` body,
  `MAX_FRAME_BYTES` 16 MiB. `TcpNetworkFactory`/`TcpNetwork` implement `RaftNetworkV2` over a
  peer pool with a circuit breaker; `RaftTcpServer` answers `AppendEntries`, `Vote`, snapshot
  chunks and `ProposalForward`
- `RedbSharedStore` (`consensus/store.rs`): `SingleNode` applies commands to the local redb file,
  `MultiNode` proposes through the driver and reads from the state machine's own database. Claim
  leases default to `DEFAULT_LEASE_TTL_MILLIS` (90 s), which only a library caller sees: a cluster
  node builds the store through `cluster_store` (`src/service/cluster.rs`), which chains
  `.with_lease_ttl_millis(cluster.lease_ttl_ms)`, whose config default is 30000 ms. A propose is
  bounded by `CLUSTER_PROPOSE_TIMEOUT` (30 s)
- `RedbStateClient` (`src/service/redb_state.rs`, `service` feature): the standalone/stream
  persistence path. Config bytes, processor priors and source cursors in one local unreplicated
  redb file declared by `store "redb"`. Stream mode persists cursors and priors whenever a store
  is configured; interval/one-shot *persistence* is opt-in via
  `store "redb" { batch_resume #true }`. In-memory prior threading between passes follows the
  same flag, except for a processor node declaring a `window` block, which threads
  unconditionally: a backlog reaches such a node over several passes, and it accumulates
  across passes by definition (`service/standalone.rs`, `service/windowing.rs`)

## Service layer (`src/service/`, `src/bin/saci-service/`)

Requires the `service` feature. KDL-driven config, factory registry, HTTP control plane, and
standalone/cluster runners.

Key types:
- `ServiceConfig` and `ServiceMode`: the config schema (`mode "standalone"` or `mode "cluster"`)
- `StoreConfig`: the top-level `store "redb"` block (`path`, `batch_resume`), the local
  unreplicated store the standalone and stream runners persist through. `mode "cluster"` rejects
  a `store` block, because a cluster node's state is the raft-replicated `cluster-app.redb` under
  `node.data_dir`
- `WorkflowSpec`: one declared workflow (standalone may declare several; cluster mode takes
  exactly one). Declared `transformer`, `source`, `wasm`, `plugin` and `sink` nodes, each with a
  mandatory id and an optional name, plus `link` nodes carrying `from`, `to` and an optional
  `branch`. `#[serde(deny_unknown_fields)]` means a key the service cannot honour is a parse
  error, not a silently dropped section. The `wasm` and `plugin` fields, and the `window`
  block either carries, are declared unconditionally, like `ServiceMode::Cluster`: the schema
  is the same in every build, so a node whose host is compiled out, or a `window` block in a
  build without the windowing engine, is a named feature refusal
  (`validate_build_capabilities`) rather than an unknown key. `WindowConfig` names
  `saci_core::window_spec::WindowSpec`, which lives outside saci-core's `windows` feature for
  exactly that reason; the engine under `saci_core::windows` stays gated and re-exports the
  enum so `saci_core::windows::WindowSpec` still resolves. `nodes()`, `rebuild_blocker`, both
  window checks and `WindowConfig`'s whole parse path read unconditionally for the same
  reason; only `build_processor_node`'s dispatch with its two host-gated builders,
  `BuiltNode::window` and the two host modules stay `#[cfg]`-gated.
  Systems and components cannot be declared in the config
  file. `WorkflowSpec::validate` enforces the load-time graph rules: unique/charset-valid ids,
  every `link` naming a declared node, the graph is acyclic, every source has an outbound link
  and every sink an inbound one, a processor may be fed by any mix of sources and processors
  (the fan-in merge that windowing processors rely on), cluster mode declares exactly one
  processor and no source/sink/link, `run_mode kind="stream"` declares at least one source
  (pulled round-robin), no source outside stream mode is one that never reaches EOF, and every
  `window` block is geometrically sane. Rule 17 and the cluster-mode window refusal in rule 10
  run whether or not this build carries the node's host or the windowing engine, since a bad
  geometry, and a window block cluster mode cannot honour, are defects in the file either way.
  The check that a window's time field is carried by every component delivered to the node is
  a builder-time gate in `src/service/validation.rs`.
- `ServiceBuilder` and `BuiltService`: assembles one `BuiltNode` per declared node, in topological
  order, from config plus registered factories. A `BuiltNode` holds its declared id/name/type_name,
  its component (`None` for a processor), a `BuiltNodeKind::{Source,Processor,Sink}` and its
  `downstream` indices into `BuiltService::nodes`. A `wasm`/`plugin` node with no `module`/`library`
  gets its runtime from `with_runtime(id, ..)`, keyed by that node's declared id. With
  `with_inspector(...)` the build also publishes the `Topology` into that inspector, because the
  builder is the only place that knows every node's concrete kind and detail.
- `ServiceFactory` and `rebuild_blocker` (`src/service/builder.rs`): `build_all` consumes the
  builder, so nothing it produced can be built twice. `into_factory(&config)` keeps both halves
  instead, and `build_all` is now that plus one `ServiceFactory::build` per declared workflow plus
  one `publish_topology`, unchanged in behaviour and signature. The lifecycle plane keeps the
  factory, because `start` and `restart` rebuild one workflow at a time.
  `rebuild_blocker(&WorkflowSpec)` names the two shapes that cannot be built twice, both proven by
  the code: a `wasm` node with no `module` or a `plugin` node with no `library`, whose injected
  runtime `build_processor_node` **removes** from the builder; and a `ChannelSource` or
  `ChannelSink`, which `ChannelRegistry` latches per name and refuses a second time. Such a
  workflow reports `restartable: false` and 409s on `start`, `stop` and `restart`. `stop` is
  included, because stopping something unstartable is a one-way door. `pause`/`resume`
  stay available.
- `Registry` plus `saci_connector::{SourceFactory, SinkFactory}`, re-exported from
  `saci_service::service::registry`: the whole extension surface. `source_names` and `sink_names`
  enumerate what actually registered, the counterpart of `TransformerRegistry::formats`.
- `BUILTIN_CONNECTOR_FEATURES` and `builtin_feature` (`src/service/factories.rs`): the eighteen
  connector type names this crate carries, each paired with the feature that compiles it in
  (`tcp` and `saci` are one entry each, since both halves register under that name). Listed
  unconditionally,
  because a factory compiled out takes its `type_name` with it and the binary would otherwise
  have no way to tell a connector it ships from a name nothing here provides. Read on the
  failure path only, by `missing_factory_error`, which `build_source_node`, `build_sink_node`
  and both `Rebuilder` halves share: a miss on a tabled name names the `--features` flag that
  supplies it, a miss on any other name keeps the plain wording, so a user-defined type is never
  told a flag would provide it. The lookup runs after the registry already missed, so a binary
  registering its own `PostgresSource` never reaches it.
  `builtin_connector_table_matches_the_registry` (unit test, gated on `all`) asserts the table
  equals the registry's key set in both directions, failing on a new connector with no entry and
  on an entry a removed connector left behind. The table is also what `build_topology`'s
  `DETAIL_ALLOWLIST` is pinned against, rather than the registry:
  `every_allowlist_key_is_a_connector_type_this_crate_ships` (`src/service/topology.rs`) fails on
  an allowlist key that names no connector this crate ships, and it runs in every build,
  including one carrying no connector at all. Chained with the test above it still says no
  allowlist key is a name nothing ever registers. The converse is deliberately unasserted:
  `HttpSource` and `HttpSink` are tabled with no allowlisted key, because a URL carries
  credentials.
- `BUILTIN_TRANSFORMER_FEATURES` and `builtin_transformer_feature`
  (`src/service/factories.rs`): the same table for the five format names, `arrow-ipc`, `avro`,
  `csv`, `ndjson`, `parquet`, each paired with its `transformer-*` feature. Kept separate from
  the connector table because a format name and a connector type name are different namespaces;
  both go through one private `tabled_feature`. Read only by `missing_transformer_error`, the
  sole producer of the unregistered-format wording, which `build_transformers` calls: a tabled
  format names the `--features` flag, any other format keeps the plain
  `transformer '<id>' names format '<fmt>', which no transformer is registered for
  (registered: ...)` wording. `builtin_transformer_table_matches_the_registry` (unit test,
  gated on `all`) pins the table against `TransformerRegistry::formats` in both directions.
  This matters most in `--no-default-features --features service`, the reduced build CI
  gates, which carries no transformer at all. The binary's `validate` does not partition
  unresolved formats the way it partitions unknown connector types: a missing format is a
  fatal build error in every mode, so the builder message is the only surface.
- `missing_wasm_host_error`, `missing_plugin_host_error`, `missing_windows_engine_error` and
  `missing_cluster_host_error`
  (`src/service/factories.rs`): the third shape of the same question, for a declaration that
  names a *build capability* rather than a registry key, so there is no lookup to miss and no
  user-supplied alternative. All four go through one private `capability_error`, whose tail
  matches the connector and format hints word for word, and
  `every_host_refusal_names_the_feature_that_supplies_it` pins all four in every build,
  because no build that carries every capability can reach them through the real call path.
  `missing_windows_engine_error` takes the node kind (`wasm` or `plugin`) as well as the ids,
  because a `window` block sits on either. `missing_cluster_host_error` is `pub`: the binary's
  `serve` needs it for the arm that keeps its `ServiceMode` match exhaustive in a
  `service`-only build.
- `validate_build_capabilities(&ServiceConfig)` (`src/service/validation.rs`): the gate that
  answers "will this binary run this config", called by the binary's `validate` (ahead of
  everything, `--connectors-only` included) and by `serve` immediately after the load, before
  logging, the store, the meter provider or the port. It refuses a `wasm` node without
  `wasm`, a `plugin` node without `plugin`, a `window` block on either without `windows`, and
  `mode "cluster"` without `service-cluster`, naming the flag. A node's own host is answered
  before its window block, so a `window` on a `wasm` node in a binary carrying neither names
  `--features wasm` first. The four checks read `cfg!` rather than carrying `#[cfg]` arms, so
  every producer stays compiled and reachable in every build. `ServiceConfig::load`
  deliberately does not call it: a library embedder parsing a config to inspect it is not
  asking whether this binary can run it. `build_connectors_only` does not either: it builds
  connectors, and the binary's gate is what stops `validate --connectors-only` reporting OK on
  a config whose processor this binary cannot host. The embedder-facing half of the window
  check sits in `build_wasm_node` and `build_plugin_node`, not in `build_processor_node`
  beside the two `#[cfg(not(feature = ...))]` host arms: inside the host-gated builder the
  guard is not compiled at all when the host is absent, so the host arm returns first and
  host-before-window holds on the builder path without depending on statement order.
  `test_a_missing_host_is_named_before_a_missing_windowing_engine` (`builder.rs`, gated on the
  absence of both features) fails if the check is hoisted ahead of the dispatch. A spec handed
  straight to `ServiceBuilder` is refused rather than silently stripped of its geometry.
- `build_topology(&ServiceConfig, &[&[BuiltNode]], version)` (`src/service/topology.rs`): what the
  dashboard draws. One `TopoNode` per `BuiltNode`, in the same topological order, and connector
  options copied through a per-`type` allowlist keyed on the string `SourceFactory::type_name`
  returns, never a blanket copy of `SourceSpec.config`, which holds DSNs and credentials. Keys
  outside the allowlist are dropped, not masked. A `TopoEdge` is one declared `link`, by node id
  directly: node ids are exactly the declared config ids, with no synthetic prefixing or chain
  indexing. A `link` never crosses a workflow, so a `ChannelSink`/`ChannelSource` pair sharing one
  channel `name` is reported separately as a `Topology::bridges` entry (`BridgeEdge`), outside
  either workflow's `edges`.
- `validate_workflow_graph(workflow_id, &[BuiltNode])` (`src/service/validation.rs`): the load-time
  schema-agreement gate `ServiceBuilder::build_all` runs on every link (matching components and
  field-for-field identical Arrow schemas at both ends), and `validate_schema_fingerprint`: a
  cluster node refuses to start when the runtime's Arrow schema fingerprint differs from the one
  its persisted checkpoints were written with.
- `run_standalone`, `run_stream` and `run_cluster`: runner entry points. `run_standalone` and
  `run_stream` walk `BuiltService::nodes` directly: each pass fans one admitted chunk of a
  source's batch out to its `downstream` nodes, runs every processor in topological order, and
  stages/writes every sink. `run_stream` requires at least one source node and pulls them
  round-robin, one chunk per item; see the `FlowController` bullet below for what sizes a chunk.
  `run_cluster` validates `node.data_dir`, whose only four files are `bootstrap.lock`,
  `raft-log.redb`, `cluster-app.redb` and `node-id`, starts the `ArrowRaftDriver`, binds its
  peer listener through `spawn_tcp_server` (eagerly, so a taken address fails at startup rather
  than leaving a member no peer can reach), builds a `RedbSharedStore::multi_node` over the
  driver's own application database with the configured `lease_ttl_ms`, waits for raft to
  settle, then re-enters the `DistributedRunner` loop until cancellation: `run` returns on an
  empty work pool, and a node that exited there would drop its vote before an operator
  registered anything. `bootstrap.lock` and `node-id` are written on **every** node once it
  first reports a leader, which is what makes a follower's own restart pass
  `validate_data_dir`. Shutdown aborts the listener, signals the driver and awaits its task,
  because that task is what closes both redb files.
- `RunControl`, `PauseGate`, `PauseHandle`, `LifecycleRegistry`, `WorkflowCommand` and
  `run_supervised` (`src/service/lifecycle.rs`, `service` feature): per-workflow start / stop /
  pause / resume / restart, standalone only. `run_standalone` and `run_stream` take
  `control: impl Into<RunControl>` instead of a bare `CancellationToken`, and
  `From<CancellationToken>` yields a gate that never parks, so an uncontrolled call site is
  unchanged and allocates nothing. There are two pause points, both between passes so staged
  batches, carry-over slices, flow controllers and window trackers all survive a park: the head of
  `standalone.rs`'s loop, just before its cancellation drain, and the head of `stream.rs`'s. Both
  pacing arms also skip their wait while a pause is pending, so an `interval_ms` of a
  minute does not hold a `pause` request at `Pausing` for a minute. The two halves of the pair own
  opposite ends of both `watch` channels, so dropping either closes exactly one: a dropped
  supervisor releases a parked runner rather than stranding it, and a finished runner ends the
  supervisor's wait for an acknowledgement.
  `run_supervised` replaces the direct `run_standalone` call in `serve.rs`: with `control` `None`
  it is exactly one `run_standalone`, and with `Some` it builds, runs, parks, drains and rebuilds
  the workflow in response to commands, sharing one `&RefCell<ServiceFactory>` across every
  supervisor (all polled by the one `join_all` on one task, and the borrow never spans an await).
  A completed runner still returns, so a one-shot service exits as it always did; a failed one
  parks only when it is controlled **and** restartable. `LifecycleRegistry::command` applies the
  transition table and waits up to `LIFECYCLE_ACK_BUDGET` (5 s) for the published state to reach
  the verb's target; `command_all` is that per workflow, concurrently, because ten workflows in
  sequence could take fifty seconds to answer. Desired state is process-local: a restarted
  `saci-service` starts every workflow as configured. See
  `docs/content/service/operate/workflows.md`.
- `FlowController`, `FlowSettings`, `FlowPlan`, `FlowControlConfig` and `FlowAdjustment`
  (`src/service/flow.rs`, `src/service/config.rs`; all nine flow types re-exported from
  `saci_service::service`, along with `FlowOutcome`, `FlowSample`, `FlowMove` and `FlowCause`):
  per-source adaptive admission control `run_standalone` and `run_stream` apply before every
  pass. `run_stream`
  takes a `flow: &FlowPlan` parameter; `run_standalone` resolves its own plan from the
  `&ServiceConfig` it already takes, and passes it on to `run_stream` when `run_mode
  kind="stream"` dispatches there. A direct `run_stream` call has no config to resolve and no
  run mode to read off a `BuiltService`, so it names the policy: `FlowPlan::stream_default()`,
  not `FlowPlan::default()`, which is the batch policy and carries no latency objective.
  Adjustment decisions land at the close of an adjustment epoch (`adjust_interval_ms`, 60000 ms
  default), never per pass: each epoch runs an A/B experiment between the incumbent size and
  one candidate a step away (`growth_factor`, 2.0), alternating arms pass by pass; the winner
  (`improve_threshold`, 0.05, judged on rows/second, needing `min_samples_per_arm`, 4, usable
  samples per arm) becomes the next incumbent. A loss flips the step direction; a step that
  would repeat the incumbent (a bound, or the congestion ceiling) turns around instead of
  proposing a dead experiment, so a controller at `max_rows` still tests smaller and one at
  `min_rows` still tests larger, except `min_rows == max_rows`, which has one admissible size
  and runs none. A search that stops moving rests: `settle_after_epochs` (3) consecutive
  deciding epochs that leave the incumbent alone put the controller into a run of
  incumbent-only epochs, doubling with each further settled epoch and capped at
  `MAX_REST_EPOCHS` (16), so a converged source probes in one epoch per seventeen instead of
  on half of its passes. A rest epoch is a normal-length epoch that never schedules the
  candidate and decides nothing, reported as `FlowAdjustment::Held` like any other
  non-deciding epoch. Resetting the streak: an epoch that moves the incumbent (only an
  experiment epoch can), any guard trip, and an incumbent found breaching
  `target_latency_ms` at a rest epoch's close; the last two also end a rest in progress, the
  guard on the pass that saw it and the objective at the boundary that measured it (checked
  every boundary, so a stream objective stays reactive). An epoch too thin to decide measured
  nothing and leaves the streak alone, and `settle_after_epochs 0` never
  rests. Safety is not paced: a chain error (a processor, a sink, a fan-out append)
  divides every source feeding it; a source's own drain error divides only that source; a
  chunk over `max_chunk_bytes` or a sink backlog growing on two consecutive passes divides too.
  What divides is always the larger of the arm under test and the incumbent, so a candidate
  that steps down and fails cannot discard a size that won a clean epoch. Each division
  abandons the epoch, remembers the failed size as a congestion ceiling (climbs back are
  additive, an eighth of the ceiling), and holds for `backoff_cooldown` (4) passes; `min_rows`
  has nothing left to divide to, so a guard tripping there holds the target and the search
  state instead, as the distinct `FlowAdjustment::HeldAtFloor`, still counted into
  `saci_flow_backoff_total`. `target_latency_ms` defaults per run mode (250 in stream, 0 in
  `continuous`/`interval`, an explicit value wins in either): a breaching candidate cannot win,
  and while the incumbent itself breaches, a smaller candidate wins regardless of throughput,
  so the search descends through ordinary epoch wins, not only a guard trip, until a size meets
  the objective or `min_rows` proves it cannot, at which point the whole rule is abandoned
  until a clean pass at the floor meets it again. `FlowControlConfig::validate` rejects (never
  clamps) an out-of-range key, naming it and, for a source's own block, that workflow and
  source id too.
  Every `FlowAdjustment` but `Held` carries a `FlowMove { from_rows, to_rows, cause }`, and
  `FlowCause` names the evidence: `Experiment` with both arms' rows per second,
  `LatencyObjective` with the breaching mean pass, `PassError`, `ChunkBytes` with the projected
  weight, or `SinkBacklog` with the newest backlog. That is what the runners turn into a
  `saci_inspector_wire::FlowDecision`; `flow.rs` itself stays pure arithmetic and formats no
  string.
  `ServiceConfig::validate` refuses a `flow_control` block in cluster mode outright, the way it
  refuses a `store` block: a cluster workflow declares no source node, so there is no admission
  to govern. `RunMode::OneShot`
  engages no controller, because a single pass must drain every source by definition.
  `flow_control { enabled #false }` restores drain-to-EOF iterations; `flow_control { rows N }`
  pins a fixed size and disables the search entirely. `Source::request_batch_rows` is the
  advisory counterpart `KafkaSource`, `NatsSource` and `PostgresSource` act on; every other
  source is still governed, reshaped with a zero-copy `RecordBatch::slice`. The one exception
  is a source on a path to a windowed node (`windowing::reaches_windowed_node`, a reverse pass
  over `BuiltNode::window` and `downstream`): the credit stops the runner pulling the next
  arrival but never splits the one in hand, and the runner withholds `request_batch_rows`
  from it as well, because a windowed node observes event time at every pass boundary, so
  both a credit-sized slice and a credit-sized fetch would put a throughput measurement in
  its output. Such a connector keeps its configured `batch_size`/`batch_rows`, no boundary is
  ever drawn inside an arrival, and `max_chunk_bytes` does not bound it. A source feeding both
  a windowed and a non-windowed node counts as windowed: both see the same `RecordBatch`. In
  `stream` those two suppressions leave the credit deciding nothing, so `run_stream` builds
  **no controller** at all for such a source (`controllers[i] = None`, `RunMode::OneShot`'s
  shape in `run_standalone`): no samples, no epoch experiment, no `saci_flow_*` series, and one
  `tracing::info!` startup line naming it. The batch runners keep theirs, because `remaining
  == 0` still ends the drain there, so the credit decides how many whole arrivals one pass
  merges in `continuous`/`interval` and its gauges stay meaningful; `flow_control { rows N }`
  or `enabled #false` on every feeding source pins that too.
  `crates/saci-service/tests/windowed_fetch_hint.rs` covers the withheld hint in both runners,
  and `standalone.rs`'s `a_windowed_path_source_publishes_{no_flow_target_in_stream_mode,
  a_flow_target_in_continuous_mode}` pin the two halves of the controller rule. See
  `docs/content/service/operate/flow-control.md`.
- `HealingSource`, `HealingSink`, `HealSettings`, `Rebuilder` and `HealConfig`
  (`src/service/heal.rs`, `src/service/config.rs`): per-node self-healing, on by default.
  `RetryingSource`/`RetryingSink` re-drive the *same* instance; this layer replaces it. After
  `after_failures` (3) consecutive failures the node schedules a rebuild for `base_delay_ms`
  (1000) out, growing by `multiplier` (2.0) to `max_delay_ms` (60000) with `jitter` (0.1), and
  `max_attempts` (0) never gives up. `heal_if_due` runs at the **head** of the next
  `next_batch`/`write_batch` and never sleeps, so a down connector adds nothing to the item
  path and the runner's own pacing is what lets the deadline elapse. It drops the old instance
  **before** calling the factory, so an exclusive resource (a bound listen port, an open file
  handle) is released first, which also means a failed rebuild leaves the node holding nothing
  and every call returns `"<node>: connector is down: ..."` until the next attempt lands. A
  rebuilt instance whose `schema()` differs from the one pinned at construction is discarded and
  counted as a failed heal, because `validate_workflow_graph` checked the graph against the
  pinned one. The state machine is `Healthy` / `Failing{n}` / `Scheduled{attempt,at}` /
  `Probing{attempt}` / `Exhausted`; `Probing` is the half-open probe that stops a flapping peer
  being rebuilt at the base delay forever, because a failure there resumes the backoff instead
  of re-counting `after_failures`. `at` is a `tokio::time::Instant`, so a paused test clock
  controls it.
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
  `ServiceConfig::validate` refuses a `heal` block in cluster mode outright, the way it refuses
  `flow_control`. Log lines ride `sampling::HEAL_TARGET` (`saci::heal`), one of four always-on
  `EnvFilter` directives at `warn` beside `saci::flow_control`, `saci::windowing` and
  `saci::dlq`, and are
  never sampled. `Healer` also carries a `recovered: Arc<AtomicBool>`, latched in `success()`'s
  `Probing` arm and read through `HealingSink::recovered_flag`, which `build_sink_node` puts on
  `BuiltNode::heal_recovered`: the dead letter queue swaps it to replay the moment a sink comes
  back rather than waiting out its own backoff. See
  `docs/content/service/operate/self-healing.md`.
- `DeadLetterQueue`, `DlqShared`, `DlqRegistry`, `ReplayFilter`, `DlqError` and `DlqBlock`
  (`src/service/dlq.rs`, `src/service/config.rs`): the per-workflow store for a batch a sink
  refused. Opt-in through a `dlq` block, which parses in three KDL shapes (`dlq`, `dlq "redb"`,
  `dlq "redb" { ... }`) because `DlqConfig`'s hand-written `Deserialize` accepts the scalar form
  the derived one would refuse. `DLQ_STORES` is the table of three stores, each a connector pair
  plus an `exclusive` and a `confirms_end` flag: `redb` (the default, exclusive, end confirmed,
  `RedbSink`/`RedbSource`), `nats` (end confirmed, JetStream only, refused on core NATS because
  it is at-most-once) and `kafka`, whose `stop_at_end` drain also ends on an elapsed
  `poll_timeout_ms` window, so its end of stream confirms nothing and a replay of it never sets
  `known`.
  One letter is one row of `envelope_schema()`, eight columns derived from `ENVELOPE_FIELDS` so
  the Arrow schema and the `schema_fields` both halves are built with cannot drift; the refused
  batch rides in `payload` as one Arrow IPC stream, the one format that round-trips a `Binary`
  column and schema metadata. `half_config` is the merge: the block's shared keys, that half's
  own block over them, the injected defaults where a key is still absent, then `schema_fields`
  unconditionally. `reason_of` unwraps `RetryExhausted` recursively, so the grouping key does
  not carry the varying attempt count.
  `record` runs inside `write_staged` (both runners) and never fails outward: a store that
  cannot take a letter counts `saci_dlq_letters_lost_total` and says so, because nothing is
  buffered in memory. `at_head`/`at_tail` are the two replay points a `replay` key selects, both
  inline between passes, so `record` and `replay` never overlap. `maybe_replay` runs on
  `Startup` (the first pass), `Heal` (any sink's recovered flag), `Schedule`
  (`HealSettings::default().delay_for(auto_attempt)`, so one second doubling to a minute),
  `Manual` and `Purge`; a cancelled token, and a store this process has read through and left
  empty, skip every trigger but a request, so a shutdown never opens a store and a drained
  queue costs no exclusive open per pass. A replay that errored schedules the next one whether
  or not it retained anything, because a queue that learnt nothing about its store has no other
  trigger left. An exclusive store finishes its sink half before the drain, so the source half's
  `Source::finish` delete has to run **before** `retained` is written back and the sink half
  rebuilt; that rebuild ignores the record half's backoff, which describes a store the source
  half has just read through. Every source half consumes a window at the head of the next fetch,
  so `drain` cuts `retained` back to the start of the window it was reading on an early exit
  (cancellation, a row `Letter::from_row` refuses) but not on a `next_batch` error, where that
  next fetch has already run: the brokers commit or ack there and `RedbSource` pops the finished
  key in the same call, before the open that can fail. `rewrite` applies the same retirement to
  the record half, which carries `RetryConfig::default()`: the first write-back the store refuses
  ends the loop, drops the instance, schedules its backoff and counts every letter from there on
  lost, rather than paying four attempts over 700 ms per letter. A crash
  inside a replay loses letters a working store would have kept: redb loses the ones that failed
  again in the window between the delete and the write-back, and a broker loses the same window.
  Within one drain, a sink that refused once is retired for the rest of it: every later letter
  for it is retained unattempted, so a thousand letters against a dead sink do not pay that
  sink's retry policy a thousand times inline in the pass. A payload that will not decode
  against its sink's current schema is written back with the decode failure as its `reason`
  rather than ending the drain, which would leave its entry at the head of the store blocking
  every letter behind it. A `Purge` discards each fetched window without decoding it, so a row
  `Letter::from_row` refuses never blocks emptying the store; a row the source half's own
  transformer refuses still fails in `next_batch`, before any trigger is consulted.
  Both halves are built in `DeadLetterQueue::build`, the source one built and dropped again, so
  a key either half refuses is a load-time error rather than something the first replay finds.
  Log lines ride `sampling::DLQ_TARGET` (`saci::dlq`) at `warn`. `DlqRegistry::from_config` is
  the shared half `serve.rs` builds before the workflows and `ServiceBuilder::with_dlq_registry`
  hands to every build, so the control plane and the runner read one summary. A library embedder
  with no registry gets a private `DlqShared`. See
  `docs/content/service/operate/dead-letter-queue.md`.
- HTTP control plane: `/health`, `/ready`, `/metrics`, `/status` (axum-backed), plus the inspector's
  `/api/*` and `/ui` when `observability.inspector.enabled` is set, plus `GET /api/workflows`,
  `GET /api/workflows/{id}`, `POST /api/workflows/{id}/{start,stop,pause,resume,restart}` and
  `POST /api/service/{start,stop,pause,resume,restart}` (`src/service/lifecycle_api.rs`) when
  `ServiceState::lifecycle` is `Some`, which `serve.rs` sets in standalone mode under
  `http { control #true }` (the default). Every one of those groups is
  **merged** rather than gated inside a handler, so a disabled inspector or a service with no
  lifecycle registry 404s instead of answering 403. A per-workflow `POST` answers `200` once the
  transition settled, `202` while it is still in progress, `404`/`409`/`503` for the three
  refusals, each a `{"error": ...}` body. A `/api/service/` verb answers a
  `saci_inspector_wire::ServiceLifecycleReport` (`applied`, `refused`, `settled`) and reserves
  `409` for the case where not one workflow accepted it: a verb some workflows cannot take still
  moves the rest, which is the whole point of one operator action over a per-workflow script.
  `/api/service/` is a separate path rather than a reserved id, because `pause` is a legal
  declared workflow id. `/status`'s standalone entries carry `state` when the registry is present.
- Dead letter routes (`src/service/dlq_api.rs`): `GET /api/dlq` and
  `GET /api/workflows/{id}/dlq` merged when `ServiceState::dlq` is `Some`, which `serve.rs` sets
  from a non-empty `DlqRegistry`; `POST /api/workflows/{id}/dlq/{replay,purge}` merged only
  alongside the lifecycle registry, because a replay runs inside a live runner. `replay` takes an
  optional `DlqReplayRequest` body (`sink`, `reason`), and an absent body is `{}`. A `POST`
  answers `200` with a `DlqReplayReport` once the replay ran, `202` with the current `DlqSummary`
  after `DLQ_ACK_BUDGET` (5 s) while it is still queued, which is the normal answer for an idle
  workflow, `404` for a workflow with no queue, `409` for one that is not `Running` or already
  has a request waiting, and `503` when the runner dropped its end.
- `init_logging(&ObservabilityConfig, node_id) -> (TelemetryGuard, Option<Inspector>)`: installs the
  subscriber and, when `observability.otlp_endpoint` is set, the OTLP span exporter (with no SDK
  sampler: the layer only receives what `Sampler` kept). The `Inspector`
  is `None` when capture is disabled, in which case no capture layer is installed at all. The caller
  **must** `telemetry.shutdown().await` before exit; dropping the guard does not flush, because
  `set_tracer_provider` keeps a clone in a process-lifetime static.
  `env_filter_for(log_level, rust_log)` is what builds the filter:
  `saci=<log_level>,tower_http=<log_level>,error` when `RUST_LOG` is unset or unparsable, plus the
  four always-on directives `add_directive` appends last, `saci::flow_control=info`,
  `saci::windowing=warn`, `saci::heal=warn` and `saci::dlq=warn`, which a `RUST_LOG` cannot replace (a longer
  target beats the `saci`
  prefix, so the lines pass under `log_level "off"` and `RUST_LOG=off` alike).
- `Sampler`, `FLOW_CONTROL_TARGET`, `WINDOWING_TARGET`, `HEAL_TARGET` and `DLQ_TARGET`
  (`src/service/sampling.rs`): the per-layer
  `tracing_subscriber::layer::Filter` the format, inspector and OTLP layers share, so stdout, the
  dashboard and the collector see one sampled stream; `SpanMetricsLayer` sits outside it because
  metrics are not sampled. `observability.error_sample_ratio` governs `Level::ERROR`,
  `observability.sample_ratio` everything below it, and both default to `1.0`, which is a
  passthrough (`Interest::always` on every callsite, no accumulator touched). `Ratio` is a
  deterministic 32.32 fixed-point accumulator, not an RNG, so a ratio is exact in the long run and
  reproducible in a test. The decision is made once per root: `Context::current_span` reads the
  registry's unfiltered current span and `Context::span` answers `None` for one this filter
  dropped, which is what distinguishes a kept parent from a dropped one; `lookup_current` must not
  be used, because it walks past a disabled ancestor. A span with an explicit `parent:` is
  invisible to `Filter::enabled`, which is why the runners build their per-item children inside
  `batch_span.in_scope(...)`. `never_sampled` bypasses both ratios for `FLOW_CONTROL_TARGET`
  (`"saci::flow_control"`), `WINDOWING_TARGET` (`"saci::windowing"`), `HEAL_TARGET`
  (`"saci::heal"`) and `DLQ_TARGET` (`"saci::dlq"`).
- Flow-control log lines (`standalone.rs`'s `log_flow_start`, `record_flow_decision`,
  `log_flow_finish`, both runners): `flow control starting` per governed source at controller
  construction, `flow control decision` per episode, `flow control finished` per governed source at
  runner exit, all INFO on `FLOW_CONTROL_TARGET`, so neither `log_level` nor a ratio can silence
  them. `FlowEpisode` carries the `adjustments` and `backoffs` totals the finish line reports; the
  decision line and the inspector record share one formatted `reason`.
- `SpanMetricsLayer`: turns each `pipeline.stage` span into a `saci_stage_duration_seconds` sample.

CLI subcommands: `serve`, `validate`, `status`, `cluster init`, `cluster join`, `cluster leave`,
`cluster status`. `--config`/`-c` (env `SACI_CONFIG`) defaults to `saci.kdl`, so `saci-service serve`
needs no flags; a missing file surfaces as
`error: Configuration error: reading config file saci.kdl: <os error>` with exit code 1.

`validate` sorts a declared type the registry does not hold into two kinds. A name in
`BUILTIN_CONNECTOR_FEATURES` is a connector this crate ships whose feature is off, which no
serve-time registration can supply: `ERROR:` on stderr naming the feature, and exit 1 in every
mode, `--strict` included and irrelevant to it. Any other name may be a user-defined factory
registered at serve time, so it stays `WARNING:` plus the `NOTE:` line and exit 0, and `--strict`
is what promotes it to exit 1. `--connectors-only` follows the same split.

`serve`'s shutdown is two stages sharing one deadline, `SHUTDOWN_BUDGET` (30 s), taken the moment
either the signal or a runner exit fires (`bin/saci-service/commands/serve.rs`). The runner future
is `pin!`ed rather than moved into the signal `select!`, so the signal arm goes on awaiting it
under `timeout_at(deadline, ..)`: `wait_for_signal` has already cancelled the root token every
runner polls, so that await is what runs each runner's own drain, `flush_and_finish_all` plus
`log_flow_finish`. Dropping it, which a bare `select!` arm does, abandons both. The task drain
(`ShutdownCoordinator`) then gets whatever is left of the deadline for the HTTP and watchdog
handles. Overrunning at either stage is `tracing::error!` plus `std::process::exit(1)`, so the
clean-stop line is never printed for a stop that was not a drain. That line,
`saci-service stopped cleanly`, is a `println!`, not a `tracing::info!`, because three docs pages
call it the proof a stop was a drain and the default `log_level="error"` would swallow an event;
it pairs with the `saci-service listening on <addr>` banner, a `println!` for the same reason.

## Observability (`src/metrics.rs`, `src/service/span_metrics.rs`)

Thirty-two series, twenty-six service and six `saci_processor_*`, each with a real writer. The
full table of series to writers lives in `src/metrics.rs`'s module doc.

Attribution is additive: nineteen of the thirty-two are recorded twice, and eighteen of those
pair an unattributed form with an attributed one. `saci_processor_metric` is the exception, because
both its writes carry `metric=` and only the second adds `processor=`, so it has no
attribute-free form. `saci_rows_processed_total`,
`saci_source_batches_drained_total` and `saci_flow_backoff_total` are each recorded once with no
attributes (the process-wide total every `/metrics` consumer reads) and once more
under `saci_inspector_wire::SOURCE_ATTR` (`source="<id>"`) naming the source node that produced
them. `saci_sink_batches_written_total` and `saci_sink_rows_written_total` do the same under
`SINK_ATTR` (`sink="<id>"`), the other five `saci_processor_*` series do the same under
`PROCESSOR_ATTR` (`processor="<id>"`), and `saci_workflow_runs_total` and
`saci_workflow_errors_total` do the same under `WORKFLOW_ATTR` (`workflow="<id>"`), the one key
that names a workflow rather than a node. The unattributed value is the sum across every node or
workflow, so a query that adds both forms double counts. `saci_processor_metric` carries both
keys when attributed, sorted by key: `[("metric", <name>), ("processor", <id>)]`.
`saci_processor_rows_out_total` takes a third attributed form, `processor="<id>", branch="<name>"`
(`BRANCH_ATTR`), written by `Instruments::processor_branch_rows` per labelled outbound edge: the
same instrument, no new series, and one more form that must not be summed with the others.
`saci_flow_target_rows` and `saci_flow_throughput_rows_per_second` are attributed-only gauges under
`SOURCE_ATTR`, `saci_sink_pending_rows` is one under `SINK_ATTR`, and
`saci_window_watermark_seconds` plus `saci_window_late_arrivals_total` are two under
`PROCESSOR_ATTR`, and `saci_dlq_letters` is one under `WORKFLOW_ATTR`: no unattributed form,
because a target, a rate, a backlog, a watermark, the
arrivals one watermark rejects, or a queue depth each belong to exactly one node or workflow.
`saci_connector_heals_total` and `saci_connector_heal_failures_total` are the other additive
pair: each is recorded unattributed and once more under `SOURCE_ATTR` or `SINK_ATTR` depending on
which half the healed node is, written by `HealingSource`/`HealingSink` in `src/service/heal.rs`
per rebuild of a failing connector. A rebuild attempt lands in exactly one of the two, so their
sum is every attempt and their ratio is whether the connector is coming back.
`saci_dlq_letters_recorded_total`, `saci_dlq_rows_recorded_total`,
`saci_dlq_letters_replayed_total` and `saci_dlq_letters_lost_total` are four more additive
counters under `SINK_ATTR`, written by `DeadLetterQueue` in `src/service/dlq.rs` per batch a
sink refused and per letter a replay delivered or the store dropped.

`saci_window_late_arrivals_total` is the one series that names a condition rather than a
quantity of work: `WindowTracker::advance_from` classifies each arrival against
`watermark - allowed_lateness_ms` and counts every one whose newest timestamp falls below it,
which is an arrival the node's windowing logic drops whole. The counter takes every one; the
log takes one line per run, on the `saci::windowing` target
(`crate::service::sampling::WINDOWING_TARGET`), enabled at `warn` by an always-on `EnvFilter`
directive and bypassing both sample ratios, like `saci::flow_control`. A run must be
`BEYOND_LATENESS_RUN_TO_REPORT` (4) consecutive unusable arrivals before it reports, and any
usable arrival clears it: one lagging fan-in peer alternates with the leading peer's usable
arrivals, so ordinary skew is counted but never reported. The condition is what a dead sink
downstream of a windowed node looks like: a producer restarted with rewound event time, a
replay, or one far-future timestamp, with every other number reading healthy. It clears itself
once event time climbs back past the watermark, so the blackout lasts about as long as the
previous run of the stream did. See
`docs/content/service/processors/windowing/_index.md`'s "When the sinks go quiet".

A sink's rows counter is what makes a sink's throughput readable: a batch is whatever row count
the upstream handed over, so `saci_sink_batches_written_total` alone moves with the admission
target rather than with the data. `saci_sink_pending_rows` is the same number the adaptive flow
controller reads as congestion, taken from `Sink::pending_rows`; a sink that answers `None`
records nothing rather than zero.

`Instruments` copies the dual-impl shape of `crates/saci-connector-postgresql/src/metrics.rs`: one
real struct under `metrics`, one zero-sized no-op struct without it, identical method surface.
Instruments are process-global, because `ServiceConfig::validate` enforces node ids unique across
every declared workflow, so there is no per-workflow handle to thread through
`WasmPipelineRuntime`, `HostState` or `DistributedRunner`. What those types carry
is the declared id instead: `WasmPipelineRuntime::with_identity(workflow_id, processor_id)` and
`NativePluginRuntime::with_identity`, set by `ServiceBuilder` from the node's own id, and
`HostState.processor_id`, which attributes a `host-io::metric` call.

Four constraints that are not visible from a call site:

- Instruments bind to whichever meter provider is installed when they are first built, so
  `saci_service::metrics::init()` must run **after** `opentelemetry::global::set_meter_provider`.
- The Prometheus exporter is built with `without_counter_suffixes()`. Instrument names already end
  in `_total`; without it the endpoint exports `saci_workflow_runs_total_total`.
- Installing a meter provider is a process-global one-shot, and many `saci-service` lib tests write
  metrics. A lib test that asserts on a series reads `crate::metrics::test_registry()` rather than
  installing its own; an integration test that needs its own provider gets its own test binary,
  which is why `tests/metrics_series.rs` and `tests/processor_metrics.rs` hold one test each.
- `saci_stage_duration_seconds` is derived host-side from the `pipeline.stage` span a native
  `saci_core::Pipeline` opens per system, because `saci-core` carries no metrics dependency and
  `RunStats` has no per-system breakdown. The `EnvFilter` is subscriber-wide, so a filter
  suppressing `saci_core` spans also stops that histogram.

`host-io::metric` names come from processor code, so distinct names are capped at
`MAX_PROCESSOR_METRIC_NAMES` (256) and further names are dropped after one warning. The native
plugin ABI's `metric` callback is the counterpart of `host-io::metric` but writes no series;
`NativePluginRuntime` records the five per-batch `saci_processor_*` series exactly as the wasm
host does, and `saci_processor_metric` stays empty for a plugin.

## In-process inspector (`src/inspector/`, `inspector` feature)

Everything the `/api/*` endpoints and the `/ui` dashboard read, captured and served without a
collector, a scraper or external storage. Nothing leaves the process.

- `TimeBoundedBuffer<T>` (`buffer.rs`): a `RwLock<VecDeque>` bounded by **both** a TTL and a hard
  entry cap, drained on every `push` inside the write lock the pusher already holds. Capacity
  evictions are counted and surfaced as `buffers.dropped`; TTL expiry is not, because that is the
  buffer working as configured. A poisoned lock is recovered with `PoisonError::into_inner`, never
  unwrapped: a panicking consumer must not disable telemetry.
- `InspectorLayer` (`layer.rs`): one `tracing_subscriber::Layer` capturing spans **and** events in a
  single pass, installed by `init_logging` on the existing registry. A second span pipeline would
  double-instrument every span `saci-core` opens. Processor code reaches `tracing` through the WIT
  `host-io::log` import, so field content is untrusted: values are truncated at `MAX_FIELD_BYTES`
  (512), records capped at `MAX_FIELDS` (32), and `("truncated","true")` appended when either bites.
- `InMemoryMetricExporter` (`metric_exporter.rs`): a `PushMetricExporter` on a second
  `PeriodicReader` attached to the **same** `SdkMeterProvider` as the Prometheus exporter, at
  `Temporality::Cumulative`. `ResourceMetrics` derives only `Debug` and is a single buffer the reader
  overwrites every interval, so `export` must copy out owned `SeriesPoint`s synchronously before
  returning: it can neither store nor clone its argument.
- `Inspector` (`mod.rs`): the handle. Unlike `metrics::Instruments` it is **not** process-global,
  because the router needs one and tests need isolated instances, so it is cloned (every field is an `Arc`)
  and threaded explicitly. Its module doc carries the full series-to-dashboard-element table. Four
  buffers: spans, log events, metric samples, and flow-control decisions.
- `Snapshot::flow_decisions` is the one part of a snapshot the execution path writes rather than
  the capture layer. `FlowController::observe` returns a `FlowAdjustment` carrying a `FlowMove`
  (`from_rows`, `to_rows`, `FlowCause`) on every outcome but `Held`, and the runners map it to a
  `saci_inspector_wire::FlowDecision` through `standalone::record_flow_decision`: the runner stamps
  the time, names the workflow and source, and phrases the cause, because `flow.rs` reads no clock
  and knows no node ids.
  **The buffer carries episodes, not trips.** Safety is unpaced, so a guard under sustained
  pressure divides the target to the same size for the same cause on pass after pass, and at
  `min_rows` it divides nothing and sets no cooldown either, so it reports on every pass. Each
  runner therefore holds one `standalone::FlowEpisode` per source, the last recorded
  `(FlowDecisionKind, to_rows, mem::discriminant(FlowCause))`, and drops a decision matching
  it; a new record opens when the target lands elsewhere, the cause changes, or another kind
  intervenes. `Held` is not an adjustment and ends nothing: the backlog trend rule zeroes its
  streak on any non-rise pass, so a plateauing backlog alternates `Held` with a trip.
  `saci_flow_backoff_total` counts every trip, so the trip count is never lost. The `reason` is
  formatted once per episode and shared by the `flow control decision` log line and the record, so
  a pass that held or a decision repeating the episode allocates nothing. The cap is
  `max_flow_decisions` (1000), two orders of magnitude below the span and log caps because one
  episode is one record. `snapshot` filters the buffer by the same window cutoff the series
  histories use, so a marker and the point it annotates share one axis; the order is
  `TimeBoundedBuffer` push order, not a sort on `at_unix_ms`.
- `record.rs` re-exports the `saci-inspector-wire` shapes rather than redefining them, so the buffers
  hold exactly what `/api/*` serves. `trace_id`/`span_id` are `tracing`'s own `span::Id` values, not
  W3C trace ids.
- Edge rates come from whichever end of the edge measures what crossed it. An edge between two
  processors is rated from the upstream processor's `PROCESSOR_ATTR`-attributed
  `saci_processor_rows_out_total`; a processor-to-sink edge is rated from the sink's own
  `SINK_ATTR`-attributed `saci_sink_rows_written_total` and nothing else, because rows-out is the
  row count of the processor's whole output dataset while a sink node takes one component (a
  windowing processor returning its input alongside its reduced component would rate the edge at
  its input rate beside a sink card at zero). `saci_sink_batches_written_total` rates nothing: a
  batch is whatever row count the upstream handed over, and `Instruments::sink_write` records both
  counters on the same write, so a sink can never hold batches without rows. A labelled edge reads
  only its own branch series, with no sink fallback. A source-to-processor edge reads the source's
  own `SOURCE_ATTR`-attributed `saci_rows_processed_total`. An edge whose end has not sampled is
  omitted rather than reported as zero, so `Snapshot::edges` is a lookup by `(from, to)`, not a
  fixed one-entry-per-edge list.
- No new metric series. The inspector reads the existing thirty-two and adds `Snapshot::span_stats`,
  per-system p50/p95/max derived from retained spans, because `saci_stage_duration_seconds` is
  recorded with no attributes and per-system latency exists nowhere else for a native pipeline.
  Empty for a wasm-hosted processor, whose spans open inside the guest. A wasm processor's own
  per-batch latency is the `PROCESSOR_ATTR`-attributed `saci_processor_batch_duration_seconds`
  instead.
- Host-side spans are what fills the traces tab, and their level is what decides whether it has
  anything in it. The five runner spans are **`debug`**: `workflow.batch` (the root each runner
  iteration or claim opens), `source.drain` per source, `runtime.run` per processor, `sink.write`
  per sink, and `processor.batch` from the WASM and plugin hosts. The four `saci-core` names are
  **`info`**: `pipeline.run`, `pipeline.stage`, `system.execute`, plus `task_attempt`, which opens
  **only on a retry** so a clean run produces none. One whole `debug` tree opens per item, so the
  default `log_level="error"` materialises no span at all, `log_level="info"` leaves the traces tab
  showing `pipeline.run`-rooted traces only, and `log_level="debug"` restores the per-item
  waterfall, at roughly 4.6 µs/item against 7.4 µs/item on the reference machine. Because the
  runner spans may not exist, every runner error and warning names its own `workflow`, `iteration`
  and node field rather than relying on a parent span. `layer.rs`'s module doc carries the
  authoritative name/level/fields table.
- `runtime.run` is the contextual parent of whatever the runtime opens:
  `pipeline.run` for a native `Pipeline`, `processor.batch` for a WASM processor or a native plugin.
  Each runner creates its per-item children inside `batch_span.in_scope(...)` rather than passing
  `parent:`, because `Filter::enabled` sees only a contextual parent and `service/sampling.rs` is
  what follows it; `in_scope` is synchronous, so no guard is held across an await, which would
  otherwise adopt every span tokio polls on that thread meanwhile. Events keep their
  `parent: &batch_span` form: `Filter::event_enabled` resolves an explicit parent.
  `WasmPipelineRuntime` is the exception in reverse: it re-enters its span inside the
  `spawn_blocking` closure, which is what puts a processor's `host-io::log` lines in the trace.
  `span_stats` is unaffected: it still groups only `pipeline.stage` and `system.execute`, the spans
  a native `saci_core::Pipeline` opens.
- The `EnvFilter` is subscriber-wide, so a `RUST_LOG` suppressing `saci_service` empties the span
  buffer and the traces tab, and suppressing `saci_core` also empties `span_stats`, the
  same caveat `saci_stage_duration_seconds` carries. `observability.sample_ratio` below 1.0 drops
  whole traces from this buffer too: the capture layer sits in the same sampled group as the
  format and OTLP layers.

## Dashboard (`saci-service-ui`)

`crates/saci-service-ui` is **excluded** from the workspace (`[workspace] exclude`), so neither
`cargo fmt --all` nor `cargo clippy --all-targets` reaches it. `cargo xtask ui` runs both of
its gates itself; run them directly when changing that crate without rebuilding the bundle:

```bash
cargo fmt --manifest-path crates/saci-service-ui/Cargo.toml -- --check
cargo clippy --manifest-path crates/saci-service-ui/Cargo.toml --target wasm32-unknown-unknown -- -D warnings
```

`crates/saci-service/assets/ui/{index.html,app.js,app_bg.wasm,app.css}` is the one committed home
for the dashboard bundle: `cargo xtask ui` writes the three generated files straight there, next
to the hand-written `index.html`, and `include_str!`/`include_bytes!` in
`crates/saci-service/src/service/inspector_api.rs` embeds them from there. It lives under
`saci-service`'s own directory rather than `saci-service-ui`'s (which carries no committed bundle of
its own) because `cargo package`/`publish` never includes files outside the package being packaged;
an `include_str!` reaching into `saci-service-ui` (itself excluded from the workspace) would drop out
of a published `saci-service` tarball. Committed, so `cargo build -p saci-service` needs no wasm
toolchain. Regenerating it does:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.127 --locked   # must equal the resolved crate version
cargo xtask ui
```

The CLI version must match what `crates/saci-service-ui/Cargo.lock` resolves for the `wasm-bindgen`
crate exactly; a mismatch is a hard runtime panic in the browser, not a warning, so the task reads
the version from the lock file and refuses to run on a mismatch. It also downloads the Tailwind
v4.3.3 standalone binary into the gitignored `crates/saci-service-ui/.tools/`, so there is no node and
no `package.json` anywhere in this repository. `assets/ui/index.html` is hand-written and never
generated.

## Examples

- `examples/native/`: single-file saci-core/saci-service tutorials and feature demos,
  `first_pipeline`, `scheduler_etl(_parallel|_dag)`, `window_aggregation`,
  `distributed_scheduler`, `distributed_windowed`, `windowed_fan_in`, `stream_latency`. The two
  `distributed_*` demos run on a local redb store, so they need no external service. Declared as
  `[[example]]` targets of the `saci-service` crate, except `first_pipeline`, an `[[example]]` of
  `saci` itself: it backs the embedding tutorial, so it depends on the same crate the tutorial
  tells the reader to add.
- `examples/distributed_fulfillment/`: a 3-node SACI Raft cluster demo, field-granular DAG
  scheduling, checkpointing into the raft-replicated `cluster-app.redb`, Docker Compose deployment
  of the three nodes. One `[[example]]` target of `saci-service`.
- `examples/branching/`: one long-running stream workflow (`branching.kdl`) demonstrating every
  fan-out split: a core-subject `NatsSource` multicasting to a mirror sink and two routing
  processors, the branching-wasm component processor and the branching-plugin native plugin, each
  delivering per message to branch-labelled `FileSink`s. The `branching_publish` `saci-service`
  example feeds the stream. Demo processors in `wasm/` and `plugin/`; see its `README.md`.
- `examples/windowing/`: Beam-style windowing, one runnable workflow per window kind,
  `tumbling/tumbling.kdl`, `sliding/sliding.kdl` and `session/session.kdl`. Each fans the same two
  NATS sources into one windowed processor writing closed-window totals to its own PostgreSQL
  table, on its own http bind (8080, 8081, 8082); tumbling declares two, a wasm processor and a
  native plugin running the same logic (duplicated like the branching pair), so its two tables
  agree row for row. Four processor crates: windowing-tumbling-wasm, windowing-tumbling-plugin,
  windowing-sliding-wasm, windowing-session-wasm, in each mode's own `wasm/` and `plugin/`.
  `docker-compose.yml`, `schema.sql` (four tables) and the `windowed_publish` `saci-service`
  example are shared by all three; the sources are too, so one mode runs at a time.
  `windowed_publish`'s `--gap-every`/`--gap-ms` put a silence in every symbol's stream at the same
  instant, so a session closes at each burst boundary rather than wherever the random symbol draw
  happens to leave a `gap_ms`-wide hole; the other two modes leave both at 0. See its `README.md`.
- `examples/multi_workflow/`: two workflows in one config, bridged in process. A routing wasm
  processor splits a core-subject `NatsSource` into a PostgreSQL sink and a `ChannelSink`, and the
  second workflow's `ChannelSource` feeds the tumbling windowing example's processor unchanged. No
  link crosses a workflow; the shared channel name does. The `multi_workflow_publish`
  `saci-service` example (an `[[example]]` target, `required-features connector-nats`) feeds both
  NATS subjects. Router component in `wasm/`; see its `README.md`.
- `examples/integrity/`: end-to-end proof that every published row is processed correctly, across
  two `saci-service` processes because `run_mode` is a whole-config property. `integrity.kdl` runs
  the ingest and settle workflows in stream mode (a Kafka NDJSON source and a CSV `FileSource` fan
  into the integrity-classify-wasm processor, which routes each batch to an ndjson `HttpSink`, a
  csv `HttpSink` and a `ChannelSink`; the channel plus a JetStream NDJSON source fan into the
  windowed integrity-aggregate-wasm processor and an arrow-ipc `HttpSink`), while
  `integrity_audit.kdl` runs the audit workflow in interval mode, because every PostgreSQL read
  mode reports EOF once caught up and the stream runner retires such a source for good. That one
  reads `public.order_audit` through a `cdc_logical` replication slot into integrity-audit-wasm and
  a parquet `HttpSink`. All four sinks POST to one endpoint served by `integrity_check`, the
  `saci-service` example that is publisher, verifier and lifecycle driver at once: it recomputes
  every derived field and FNV-1a checksum from what it published and exits non-zero on any
  mismatch. Both stream-half workflows hold a Channel node, so the control plane answers 409 to
  stop/start/restart and only pause/resume; audit holds none and restarts cleanly.
  `integrity_check` runs BEFORE both services and refuses with exit 2 while either control plane
  answers `/health`: its startup reset deletes the Kafka topic, the consumer group, the JetStream
  messages, the audit table and the replication slot, and a logical slot cannot capture WAL written
  before it existed. It creates `saci_integrity_slot` itself, with the statement `slot_autocreate`
  uses, before publishing, so no audit change is ever committed with nothing capturing.
  `integrity.kdl` declares no `store "redb"` block: stream mode threads every processor's
  checkpoint blob in memory anyway, and persisting it would restore aggregate's watermark into a
  run whose clock restarts at a fixed epoch, making every arrival late and leaving `/sink/window`
  empty. Processors in `wasm/{classify,aggregate,audit}/`; see its `README.md`.

## Tests

Profiles, the Docker soft-skip convention and the `heavy-docker` group are in `AGENTS.md`'s Testing
section. Service suites under `crates/saci-service/tests/`: the in-memory store fixture
`tests/common/memory_store.rs` backs `distributed_scheduler`, `runner_chaos`, `checkpoint_chaos`,
`wasm_chaos` and `distributed_processor_state`; `redb_store` drives a local redb file;
`transport_chaos`, `raft_consensus_chaos`, `distributed_harness_smoke` and
`distributed_integration_chaos` are the Raft chaos binaries; `kafka_service` and `nats_service`
each hold two Docker-backed tests, a source to sink round trip and the dead letter store's
record, replay and consume cycle, while
`postgres_service` and `s3_service` each hold one; `metrics_series.rs` holds one
test because a meter provider is a process-global one-shot; `windowed_fetch_hint.rs` covers the
withheld `request_batch_rows` hint in both runners; `feature_bundles.rs` parses this crate's own
`Cargo.toml` and holds the three bundle invariants, that `all` lists every feature the crate
declares except `default`, `all` and `conformance`, that `conformance` (the corpus generator's
`arrow-ipc/lz4` switch, which relaxes a refusal instead of adding a capability) stays out of
`all`, and that `default` carries no connector that needs an installed broker, database or object
store, so it needs no Docker and no running service. Lib unit tests that assert on a series read
`crate::metrics::test_registry()`.

## Keep this skill current

Update this file in the same change that: adds, removes or re-implies a `saci-service` feature;
adds or renames a config block or key (`ServiceConfig`, `WorkflowSpec`, `StoreConfig`,
`FlowControlConfig`, `HealConfig`, `DlqBlock`, `ObservabilityConfig`, the cluster header); changes a runner, the
lifecycle transition table, flow control's search or guards, the healing state machine, the dead
letter queue's store table, envelope or replay triggers, or a
control-plane route or status code; adds, removes or re-attributes a metric series; changes a span
name or level; changes an inspector buffer, cap or snapshot shape; changes `saci-inspector-wire`;
changes the dashboard bundle's build or its committed location; adds or removes a service example.

This file is the canonical copy of `rebuild_blocker`/`restartable` semantics,
`Source::request_batch_rows`, `Sink::pending_rows`, `WasmPipelineRuntime::with_identity`,
`NativePluginRuntime::with_identity` and the plugin metric-callback relationship; a change to any
of these also updates the quoting fragment in skill `saci-connectors`, `saci-processors` or
`saci-plugins` (each names the exact clause in its own Keep this skill current section).
