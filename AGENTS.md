# AGENTS.md

Guidance for coding agents working in this repository.

## Project

SACI is a distributed batch processing engine for Rust built on Apache Arrow. Pipelines compile to
WebAssembly components; a host binary loads them at runtime. Edition 2024, MSRV 1.99.0.

Two crates publish: `saci`, which carries the engine, every connector and byte format, both
authoring SDKs and the `saci-service` host as feature-gated modules, and `saci-macros`, the proc
macros. The repository root is a **virtual manifest**: there is no root `src/`. Every command
that names a target needs `-p <crate>`, almost always `-p saci`.

## Skills

`.agents/skills/<name>/SKILL.md` holds the reference for one area. Read the matching skill
before working in that area; this file keeps the workspace map, the commands, the testing
profiles and the engine reference. "skill `X`" below means that path. `technical-writer`,
`rust-best-practices`, `rust-performance` and `ponytail` are not in this repository: they are
user-level skills under `~/.agents/skills/`, shared by every project.

| Skill | Read before |
|---|---|
| `saci-connectors` | touching `crates/saci/src/connector.rs` or anything under `crates/saci/src/connector/`, a `SourceFactory`/`SinkFactory` registration, `ConnectorContext`, a connector config under `examples/configs/`, `connector_matrix.rs` or `xtask/src/matrix.rs` |
| `saci-transformers` | touching `crates/saci/src/transformer.rs` or anything under `crates/saci/src/transformer/`, a `format` registration, or a transformer option |
| `saci-processors` | touching `crates/saci/src/processor.rs` or `processor/`, `saci-macros`, any `wit/pipeline.wit`, `crates/saci/src/wasm_host/`, `packages/`, or `examples/{wasm,polyglot,quickstart,conformance}/` |
| `saci-plugins` | touching `crates/saci/src/{plugin.rs,plugin/,plugin_abi.rs}`, `saci-plugin-smoketest`, `crates/saci/src/plugin_host/`, `crates/saci/src/service/plugin_loader.rs`, or `examples/plugins/` |
| `saci-service` | touching `crates/saci/src/{service,distributed,inspector,bin}/`, `src/metrics.rs`, `src/inspector_wire.rs`, `crates/saci-service-ui`, or `examples/{native,branching,windowing,multi_workflow,integrity,distributed_fulfillment}/` |
| `technical-writer`, then `saci-docs` | writing or editing any `//`, `///` or `//!` comment, anything under `docs/`, `README.md`, a skill, or any other prose: the first holds the writing rules, the second the SACI surfaces, site layout, diagram classes and docs build commands |
| `rust-best-practices` | the finishing review of every changed `.rs` file |
| `rust-performance` | a change on a hot path or to a build profile, allocator or dependency setting |
| `ponytail` | designing or writing any change: the smallest change that fully solves the task |

The five `saci-*` area skills are documentation of record. A change to how a connector,
transformer, processor, plugin, or the service and its dashboard works (a new or removed crate,
a renamed type, a changed trait method, a new config key, a new metric series, a changed build
step, fixture or example) updates the matching skill in the same change; each skill's last
section, "Keep this skill current", lists its triggers. A skill that lags the code is a defect
of that change, to be fixed before the change is finished.

## Commands

```bash
cargo build                                                  # Build (workspace default members)
cargo nextest run --workspace --all-features                 # Fast suite: skips Docker/chaos tests, run this constantly
cargo nextest run --workspace --all-features --profile ci --run-ignored all  # Full suite, what CI runs; last step of a plan
cargo xtask matrix                                           # Connector matrix: shared containers, one nextest process per slice; see Testing below
cargo test --workspace --all-features --doc                  # Doc tests (nextest does not run these)
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps  # Rustdoc lints; public items of workspace members only, no #[cfg(test)] modules, no saci-service-ui
cargo fmt --all -- --check                                   # Check formatting
cargo clippy --all-targets --all-features -- -D warnings     # Lint (warnings are errors)
cargo clippy -p saci --no-default-features --features service --all-targets -- -D warnings  # Same gate on the narrowest runnable service; catches an item that is dead only when a feature is off
cargo nextest run -p saci --no-default-features --features service,connector-channel --lib  # Runs that build plus the channel connector its unit tests drive; the only place the cfg(not(feature = ...)) tests needing neither processor host execute
cargo nextest run -p saci --no-default-features --features service,wasm-host,plugin-host,connector-channel --lib  # Both hosts, no windowing engine; the only place the three window-block refusal tests execute
cargo nextest run --workspace --lib                          # The library default: the engine alone
cargo nextest run -p saci --features server --lib            # The `server` bundle; `ci.yml`'s "Test, server bundle" step says what only this configuration reaches
cargo xtask check-wasm-processor                             # saci still builds for wasm32-wasip2 with --features processor, without tokio
cargo xtask bench tpch_q6                                    # Benchmarks, always via the harness
cargo check --examples --all-features                        # Verify examples compile; --all-features keeps connector-gated examples compiled
cargo build -p saci --features server --bin saci-service     # The server binary
cargo xtask server --connectors=file --transformers=csv --processors=wasm  # A server binary with only the named parts
cargo run -p saci --example scheduler_etl                    # Run an example
cargo run -p saci --example first_pipeline                   # The native tutorial's example
cargo run -p saci --example distributed_scheduler --features distributed  # No external service
cargo run -p saci --example scheduler_etl_parallel
cargo audit                                                  # Security audit (needs cargo-audit)
cargo xtask polyglot                                         # Build the six polyglot processors
cargo xtask quickstart                                       # Build the two Quick Start processors
cargo xtask ui                                               # Rebuild the /ui dashboard bundle
cargo xtask validate                                         # Validate example configs: build+run the registry, parse-check the rest
cargo xtask demo <name>                                      # Build and run an example pipeline
cargo xtask --help                                           # Every task the runner carries
```

`cargo nextest run` needs `cargo install cargo-nextest --locked --version 0.9.143` first; see
"Testing" below for what the two profiles cover and why the fast one is the default day-to-day
command.

### Prerequisites by area

Two fixtures must exist before `cargo nextest run` reaches `crates/saci/tests/`:

```bash
rustup target add wasm32-wasip2
cargo build --release -p saci-processor-smoketest --target wasm32-wasip2   # skill saci-processors
cargo build -p saci-plugin-smoketest                                      # skill saci-plugins
```

Which tests need each and why a plain `cargo build` yields a finished component are in those
two skills. Kafka's cmake, C toolchain and libcurl needs: skill `saci-connectors`. The dashboard
bundle and the excluded `saci-service-ui` crate's own fmt and clippy gates: skill
`saci-service`. Quick Start components and the Arrow IPC conformance corpus: skill
`saci-processors`.

### Testing

Tests run through [`cargo-nextest`](https://nexte.st), not bare `cargo test`, via
`.config/nextest.toml`. `cargo install cargo-nextest --locked --version 0.9.143` installs it. Two
nextest profiles cover most of the workspace, and a third, `matrix`, runs the connector matrix
alone; which to reach for depends on why you're running tests:

- `cargo nextest run --workspace --all-features` (the `default` profile): the everyday command.
  It compiles the same `--all-features` closure as always, but skips every test that needs a
  Docker daemon: the testcontainers-backed connector suites (the `kafka_*`, `mariadb_*`,
  `nats_*`, `postgresql_*`, `s3_*` and `clickhouse_*` binaries in `crates/saci/tests/`), the two
  whole Raft chaos binaries (`transport_chaos`, `distributed_harness_smoke`), the Docker-gated
  cluster tests in `raft_consensus_chaos` (its `unit` and `idempotency` modules are Docker-free
  and stay), the two Docker-backed tests each in `kafka_service` and `nats_service` (a source to
  sink round trip and the dead letter store's record, replay and consume cycle), one apiece in
  `postgres_service` and `s3_service`, and two in `mariadb_service`. The one non-Docker
  exclusion is `saci::turso_synced_roundtrip`, which drives a synced replica against whatever
  remote endpoint `SACI_TURSO_URL`/`SACI_TURSO_TOKEN` name and clears a fixed table there. Run this
  constantly; it should stay fast regardless of how large the Docker-backed suites grow. The
  embedded store suite (`redb_store`) and the other distributed suites (`distributed_scheduler`,
  `runner_chaos`, `checkpoint_chaos`, `wasm_chaos`, `distributed_processor_state`) need no daemon
  either: the first drives a local redb file, the rest the in-memory store fixture in
  `tests/common/memory_store.rs`.
- `cargo nextest run --workspace --all-features --profile ci --run-ignored all` (the `ci`
  profile): the full suite, including everything `default` skips and any `#[ignore]`d test (today,
  `saci`'s lib unit test `test_second_init_returns_error` (a global-tracing-subscriber
  race), `distributed_integration_chaos.rs`'s `full_stack_chaos_monkey_60s`, a ~70-100s five-node
  Raft-cluster-behind-Toxiproxy chaos run asserting that every node converges on the same applied
  index under combined latency, bandwidth, reset and partition faults, and that isolating the
  leader halfway through the window costs it the office: a random single-edge fault only starves a
  follower of heartbeats when it happens to land on an edge out of the leader, so the election that
  proves liveness is forced by cutting every link of the current leader rather than left to the
  dice (the term-advance assertion was ~10% flaky on a shared runner before that)). `ci`'s
  `default-filter` drops only the connector matrix's `live::` slices, which need the containers
  `cargo xtask matrix` starts. This is the full suite to run as the last verification step of a
  plan, not something to reach for on every edit. `ci.yml` splits it into three: the `test` job
  runs `--profile ci` without `--run-ignored`, a separate `distributed_chaos` job runs only
  `full_stack_chaos_monkey_60s` (`--test distributed_integration_chaos --run-ignored
  ignored-only`), and the `connector_matrix` jobs run the matrix, so that none of them sits on
  `test`'s critical path. Regenerating or reordering the Docker/chaos exclusion list itself lives
  only in `.config/nextest.toml`; nothing here restates it, so that file is the one place to
  update when a test moves between the profiles.
- `cargo xtask matrix` (the `matrix` profile, `ci.yml`'s `connector_matrix_build` and
  `connector_matrix` jobs): `crates/saci/tests/connector_matrix.rs` holds one `#[ignore]`d
  test per {source, processor, format} slice, `live::<source>::<processor>::<format>`, each
  running that source against every sink, plus `live::maximal::workflow`. nextest runs each in its
  own process, so the slices run in parallel; the task builds both processor fixtures, starts one
  container apiece for Kafka, NATS, PostgreSQL, RustFS, ClickHouse, MariaDB and MySQL, names them in
  `SACI_MATRIX_*` variables every slice reads, runs `cargo nextest run --profile matrix
  --run-ignored all` plus any argument given (`-E 'test(/^live::kafka::/)'` for one source), and
  removes the containers. CI compiles the binary once into a nextest archive and runs one job per
  source from it (`--archive-file`). The same file's Docker-free tests run under every profile.
  Dimensions, rejection sites and isolation: skill `saci-connectors`.

`cargo test --workspace --all-features --doc` remains a separate, always-bare-`cargo-test` step in
both this file and `ci.yml`: nextest does not run doctests.

Five commands cover the feature configurations. Four of them widen: `--workspace --lib` is the
library default, the engine alone; `-p saci --no-default-features --features
service,connector-channel --lib` is the narrowest runnable service plus the channel connector its
unit tests drive, registering no other connector and no transformer; `-p saci --features server
--lib` is the `server` bundle, which registers only the `BUILTIN_CONNECTOR_FEATURES` entries
whose feature is on; and `--workspace --all-features` registers every one of them. The fifth,
`-p saci --no-default-features --features service,wasm-host,plugin-host,connector-channel --lib`,
is off that line entirely: both processor hosts with no `windows`, the only configuration where a
`window` block sits on a node the build can host and finds no engine, which is what the three
window-block refusal tests need. A unit test that drives a connector or a byte format through a
module other than its own is gated on that feature, so each rung compiles. Current scale at the
four `--lib` rungs is 415, 801, 1338 and 890 tests. Every count here is what nextest runs under
the `default` profile, which `cargo nextest list` reports too; the profile's excluded tests are
in neither.

Docker-backed tests soft-skip rather than fail when no daemon is reachable: each connector's
`crates/saci/tests/common/<connector>.rs`, included as that suite's `common` module, exposes a
`try_start() -> Option<Container>` that starts a
`testcontainers::GenericImage`, catches the error, prints `SKIP: ... unavailable: {e}`, and returns
`None`; every such `#[tokio::test]` opens with `let Some(x) = common::try_start().await else {
return; };`. This is what lets the `ci` profile run unconditionally on a runner that may or may not
have Docker, and it needs no nextest-side accommodation.
The MariaDB connector's suites run every case against a MariaDB and a MySQL server: `SACI_MARIADB_TEST_MARIADB_IMAGE`
(default `mariadb:11.8`) and `SACI_MARIADB_TEST_MYSQL_IMAGE` (default `mysql:8.4`) pick the server images, and
`SACI_MARIADB_TEST_ONLY=mariadb` or `mysql` runs one flavour only (CI's `mariadb_servers` job sweeps older and newer servers).
The Raft chaos harness sharpens that rule: only the container step soft-skips.
`RaftClusterHarness::try_start` returns `None` when the Docker daemon cannot supply the Toxiproxy
container, and every step after it panics: host-port resolution, per-edge proxy creation, node
startup, and the `await_listening` check that a node really accepted on its reserved port. That is
what stops a green run from hiding a broken harness.

Every Docker-backed connector test starts its own container with OS-assigned ports and
nanosecond-unique resource names, which is what makes nextest's cross-binary concurrency safe
(skill `saci-connectors`).

A test that captures a log line emitted by production code keeps a second `tracing::Dispatch`
alive for its own duration. `tracing` caches each callsite's interest process-wide on its first
hit anywhere, so while at most one dispatcher has ever been registered, a sibling test driving the
same production path with no subscriber of its own latches the line off for the rest of the
binary. nextest gives each test its own process and never sees this; a bare `cargo test --lib` run
does. Reaching the callsite first does not help: `MAX_LEVEL` starts at `OFF` and rises only when a
dispatcher registers, so an `info!` before the subscriber is installed emits nothing and primes
nothing. The second live dispatcher takes away `tracing_core`'s `JustOne` shortcut, so a foreign
first hit folds over the registered list, sees the test's subscriber and yields `sometimes`
instead of `never`.
`flow_control_lines_are_emitted_at_log_level_off` in
`crates/saci/src/service/standalone.rs` is the instance, and the comment on its
`_keepalive` binding carries the reasoning in full. A test whose callsites sit in its own body,
like the filter tests in `crates/saci/src/service/logging.rs`, needs none of this.

The Raft chaos suites are the exception, and the one place a runner feature does the work. The
`heavy-docker` test group caps `max-threads = 1` over `transport_chaos`, `raft_consensus_chaos`,
`distributed_harness_smoke` and `distributed_integration_chaos`, so those four take the machine
one test at a time. Unlike a connector test, which needs only its own container, each Raft chaos
binary contends for the Docker daemon, for host ports, and for enough CPU to run 3 to 5 raft
nodes while asserting on election and log-convergence deadlines. The group is declared once and
applied through an override in **both** profiles: `distributed_integration_chaos` is in it even
though `default` never reaches that binary, because `ci` does. Adding another heavy-Docker binary
to the group means adding it to both `filter` expressions. The connector matrix's slices are
outside the group on purpose: they start no container of their own, so they run in parallel
against the ones `cargo xtask matrix` started.

## Workspace layout

```
crates/
├── saci/                     # The one library crate, every part behind a feature. `src/` root:
│                             # the columnar engine (Dataset, Pipeline, System, Scheduler,
│                             # Component, io/). Reference below. Modules beside it:
│                             #   config        KDL into ConfigValue (serde_json::Value) and
│                             #                 ConfigMap; from_kdl_str, from_kdl_str_with_vars,
│                             #                 one_or_many, substitute_env_vars, substitute_vars.
│                             #                 A parse failure after substitution names the
│                             #                 variables inserted, never a value.
│                             #   connector     SourceFactory, SinkFactory, ConnectorContext,
│                             #                 NodeIdentity, bridge::ChannelBridge, and one child
│                             #                 module per connector. skill saci-connectors
│                             #   transformer   Transformer, BatchReader, BatchWriter,
│                             #                 MessageDecoder, TransformerRegistry, and one child
│                             #                 module per byte format. skill saci-transformers
│                             #   processor     Processor SDK; `wit/` holds the canonical WIT
│                             #                 package. skill saci-processors
│                             #   plugin, plugin_abi
│                             #                 Native plugin SDK and its C ABI; `include/` holds
│                             #                 the C header. skill saci-plugins
│                             #   service, distributed, inspector, inspector_wire, metrics,
│                             #   wasm_host, plugin_host, bin/saci-service
│                             #                 The host. skill saci-service, saci-processors,
│                             #                 saci-plugins
├── saci-macros/              # Proc macros behind the processor SDK's derives. skill saci-processors
├── saci-processor-smoketest/ # Processor component used as a CI fixture. skill saci-processors
├── saci-plugin-smoketest/    # Native plugin used as a CI fixture, cdylib only.
│                             # skill saci-plugins
└── saci-service-ui/          # The /ui dashboard: CSR Leptos, NOT a workspace member. Compiles
                              # crates/saci/src/inspector_wire.rs in by `#[path]`. skill saci-service
examples/
├── native/                   # Engine and service tutorials and demos. skill saci-service
├── branching/, windowing/, multi_workflow/, integrity/, distributed_fulfillment/
│                             # End-to-end service workflows. skill saci-service
├── connectors/, configs/     # Connector examples and runnable KDL configs, including `dlq.kdl`
│                             # and its `fixtures/dlq_collector.py`. skill saci-connectors
├── wasm/, polyglot/, quickstart/, conformance/
│                             # Processor examples and the corpus generator. skill saci-processors
└── plugins/                  # Native plugin proofs. skill saci-plugins
packages/                     # Apache-2.0 subtree: one SDK per language plus the Arrow IPC
                              # conformance corpus. skill saci-processors
docs/                         # Zola site in two areas, service and library. skill saci-docs
xtask/                        # The task runner behind `cargo xtask <command>`: quickstart,
                              # polyglot, plugins, ui, bench, pack-sdk, validate, demo, matrix, server,
                              # check-wasm-processor, processor-ipc-roundtrip. One module per
                              # command, zero dependencies, so it drives Go, .NET, npm, Gradle
                              # and wasm-tools identically on Windows, Linux and macOS. Exit
                              # codes are documented per module and CI reads them.
                              # validate and demo (examples.rs) inject a `variables` block
                              # into example configs, so they run with no OS env export.
                              # validate also parses every examples/configs/*.kdl file
                              # through saci-service validate --connectors-only, discovered
                              # from the directory rather than a hand-maintained list.
```

## Feature flags

`crates/saci/Cargo.toml` declares every feature; a library user compiles only what they name.
The **default** is `engine` alone.

Engine:

- `engine` (**default**): the columnar engine plus the `Source`/`Sink` traits (implies `io`).
- `runtime`: tokio and rayon stage parallelism. Cannot target wasm32-wasip2.
- `io`: the `Source`/`Sink` traits and the schema cast helpers (implies `runtime`).
- `windows`: windowed aggregation. `WindowedSystem`, watermarks, `WindowAccumulator`. The
  `WindowSpec` geometry enum sits outside the feature, in `window_spec.rs`, so a host parses a
  `window` declaration in every build; `saci::windows` re-exports it.
- `tracing`: `tracing` crate integration across the engine, the connectors and the host. Gates
  the events and the three nested spans `pipeline/execution.rs` opens (`pipeline.run`,
  `pipeline.stage`, `system.execute`).
- `metrics`: the OpenTelemetry instruments of the host and the connectors.

Authoring SDKs, each for a different artifact:

- `processor`: the wasm32-wasip2 processor SDK in `saci::processor`, sequential execution driven
  by the `pollster` executor, the `#[derive(Component)]`/`#[transform]`/`#[fold]`/`#[processor]`
  macros and `export_pipeline!`. A processor crate depends on `saci` with
  `default-features = false, features = ["processor"]`, so tokio stays out.
- `plugin`: the native plugin SDK in `saci::plugin` and `export_plugin!`, for a cdylib the host
  `dlopen`s.

Connectors and formats: `connector-<name>` compiles `saci::connector::<name>`,
`transformer-<format>` compiles `saci::transformer::<format>`. `connectors` enables the seven
that need nothing installed or already running (`channel`, `file`, `redb`, `http`, `tcp`, `udp`,
`saci`); `transformers` enables all five formats. `connector-kafka`, `connector-nats` and
`connector-udp` imply `transformer-ndjson`, `connector-tcp` implies `transformer-arrow-ipc`.
TLS is always compiled into the PostgreSQL, MariaDB and ClickHouse connectors.
`connector-kafka` needs `cmake` and a C toolchain for vendored librdkafka.

Host: `service` (KDL config, Registry, runners, HTTP control plane, the `saci-service` binary),
`wasm-host` and `plugin-host` (the two processor hosts, `saci::wasm_host` and
`saci::plugin_host`), `distributed`, `distributed-raft`, `service-cluster`, `inspector`,
`parquet-checkpoint`, `trace-context`, `mimalloc`. A `connector-*` feature registers its
factories only when `service` is on too. The full table and what each implies is in skill
`saci-service`.

Bundles: `server` is what `cargo install saci --features server` builds: `service`, `wasm-host`,
`windows`, `parquet-checkpoint`, `mimalloc`, the seven self-contained connectors and every
format. `all` turns on every capability, including the eight opt-in connectors, the plugin
host and a Raft cluster node, but excludes `conformance`, the corpus generator's own switch.
`crates/saci/tests/feature_bundles.rs` holds both bundles to those rules.

A custom server is a binary depending on `saci` with the features it wants and a `main` that
calls `saci::service::cli::main()`, or `main_with` to register its own factories.

### Columnar engine (the `saci` crate root)

- **`Component` trait** (`src/component.rs`): any type providing `name() -> &'static str` and an
  Arrow `Schema`. Rows serialize via `serde_arrow`.
- **`Dataset`** (`src/dataset.rs` plus the ten-file `src/dataset/` submodule): Arrow-backed
  columnar container. Each registered component holds a `Vec<RecordBatch>` of appended chunks, and
  a read (`batch_for`, `column`, `view`) goes through `get_or_build_merged`, which concatenates
  them once into a `merged_cache` entry; `chunks.rs` owns that merge. A component may hold fewer
  rows than the dataset's row count (a windowing processor's reduced result component), never more.
  Holds a `SchemaRegistry`, a `ResourceMap`, and an alive bitmap. Supports batch `append`, soft
  delete (`mark_dead`), compaction, and IPC round-trip. Builder: `DatasetBuilder`. Canonical path:
  `saci::dataset::Dataset`.
- **`Row`** (`src/row.rs`): stable row index (`u32`). Invalidated by `compact`.
- **`Resource`** (`src/resource.rs`): boxed Rust singleton stored in `Dataset`, keyed by `TypeId`.
  Not columnar, and **not** serialized by `write_ipc`. The processor SDK relies on that to keep
  cross-batch state out of the data plane.
- **`Source`/`Sink` traits** (`src/io/`, `io` feature): `Source::finish`, like `Sink::finish`, is
  called once after the last `next_batch`, and defaults to nothing. It is where a source whose
  delivery is a commitment makes it durable, so a caller that dropped the source instead sees the
  same data again; every wrapper (`Box`, `CastingSource`, `RetryingSource`, `HealingSource`)
  forwards it, and the service's runners call it per exit path after the sinks are finished.
- **`System` trait** (`src/system.rs`): `meta()` declares field-level read/write access,
  `async fn run(&self, data: &mut Dataset)` does the work, `run_sync` is an optional sync fast path.
  Written as a struct impl or via the `system_fn` closure helper.
- **`Pipeline`** (`src/pipeline.rs` plus `src/pipeline/`): self-contained workload
  `{ name, data: Dataset, systems, DAG stages, sources, sinks }`. Builds a conflict graph from
  `SystemMeta`, topologically sorts it into stages, and runs them with per-system retry. Builder:
  `PipelineBuilder`.
- **`Scheduler`** (`src/scheduler.rs`): multi-pipeline orchestrator over `Vec<Pipeline>`. `tick()`
  runs every pipeline once, walking a dependency DAG built from `PipelineConfig`. Reachable from
  library code only: a `ServiceConfig` declares `workflow` nodes, and the runners walk
  `BuiltService::nodes` themselves, so the config-driven binary never builds one.

#### Dataset API

```rust
let mut dataset = Dataset::new();
dataset.register_component::<Price>()?;          // must precede append
dataset.append::<Price>(&rows)?;                 // returns Range<Row>
let col = dataset.column::<Price>("value");      // -> Option<ArrayRef>
dataset.mark_dead(row);                          // soft delete
dataset.compact();                               // filter dead rows
dataset.write_ipc(&mut buf)?;                    // serialize
let dataset2 = Dataset::read_ipc(&mut &buf[..])?;
```

#### Pipeline API

```rust
// Inline construction
let mut pipeline = Pipeline::new("etl");
pipeline.register_component::<Price>()?;         // forwards to self.data
pipeline.append::<Price>(&rows)?;
pipeline.add_system(EnrichPrice);
pipeline.run().await?;                           // validate + DAG + retry

// Builder pattern
let pipeline = Pipeline::builder("etl")
    .with::<Price>()
    .with_resource(TaxRate(0.1))
    .with_system(EnrichPrice)
    .build();
```

`run_on(&self, data: &mut Dataset)` is the escape hatch for hosts that own their own dataset. It
executes the system DAG against an external dataset without touching the template pipeline's data,
sources, or sinks. `run_on_with_stats` is the same call returning the per-call `RunStats`, which is
how the processor SDK fills the WIT `run-metrics` record.

**`PipelineRuntime`** (`src/runtime.rs`, `runtime` feature) is the host-side seam a swappable
backend implements: `name`, `run_on`, `run_on_with_state`, `declared_components`,
`descriptor_info`, `template_dataset`. `descriptor_info() -> RuntimeDescriptorInfo { name, version,
stateful, schema_fingerprint }` has a default empty body and is the one generic way a host holding
`Box<dyn PipelineRuntime>` can read what an out-of-process runtime says about **itself**:
`WasmPipelineRuntime` maps its cached `describe()` record and `NativePluginRuntime` its validated
manifest, while `Pipeline` keeps the default because a native pipeline has no self-description
beyond `name()`. `RuntimeDescriptorInfo::name` is not `name()`: the latter is whatever the host
passed at construction, and what that is differs per runtime kind. A config-loaded `wasm` node gets
its own declared node id (`ServiceBuilder` calls `loader.load(&spec.id, &spec)`, forwarded into
`WasmPipelineRuntime::from_bytes`), a `plugin` node gets the plugin manifest's name, and a
`with_runtime` native pipeline gets the pipeline's own name.

#### System & SystemMeta (`src/system.rs`)

`SystemMeta` declares data access at field granularity via `(component_name, field_name)` pairs. The
pipeline uses this to build a conflict graph and group non-conflicting systems into one stage.

```rust
SystemMeta::new("enrich")
    .read("Order", "id")
    .write("Order", "total")
    .read_component("Price")       // expands to all fields of Price
    .read_resource::<TaxRate>();
```

Conflict rules (B registered after A):
1. Write-after-read: A writes F, B reads F, so B depends on A
2. Read-after-write: A reads F, B writes F, so B depends on A
3. Write-write: A writes F, B writes F, so B depends on A
4. Resource conflicts remain TypeId-level

System trait signatures:
- `async fn run(&self, data: &mut Dataset) -> SaciResult<()>` for exclusive access
- `async fn run(&self, data: &Dataset) -> SaciResult<WriteSet>` for `ParallelSystem`, a read-only
  pass

#### Retry (`src/retry.rs`)

`RetryMode`: `None`, `Fixed`, or `ExponentialBackoff` (default: 3 retries, 100 ms base, 2.0x
multiplier, 30 s cap, 0.1 jitter). `SystemConfig` wraps a `RetryMode` and is returned by
`System::config()`. Every system run goes through one of two drivers that share the attempt-counting
core: `run_with_retries` (async, `tokio::time::sleep`) and `run_with_retries_blocking`
(`std::thread::sleep`, for the rayon/`spawn_blocking` stage path).

#### Error types (`src/error.rs`)

`SaciError` variants: `SystemExecution`, `ComponentNotFound`, `EntityNotFound`, `ResourceNotFound`,
`Store`, `Scheduler`, `Configuration`, `RetryExhausted`, `Generic`. With the `distributed` feature:
`Distributed`, `LeaseExpired`. Alias: `SaciResult<T>`. `PartialEq`/`Eq` are derived.

#### Windowed aggregation (`src/windows/`, `windows` feature)

`WindowedSystem` and `WindowedSystemBuilder` assign rows to tumbling, sliding, or session windows,
track watermarks, aggregate per key, and publish results as a `WindowResults` resource.
`WindowAccumulator` is the component that carries open-window state across batches; the host
persists it through `CheckpointStore`.

### Host (`service` and the modules beside it)

The host is documented in four skills. `saci-connectors` and `saci-transformers` carry the IO
layer: a connector moves bytes, a transformer turns bytes into `RecordBatch`es and back, and
`ServiceBuilder` resolves a node's `transformer` id against the `TransformerRegistry` so no
connector resolves a format itself. `saci-processors` carries the wasm host (`src/wasm_host/`)
and the processor SDK; `saci-plugins` carries the native plugin host and SDK. `saci-service`
carries the rest: the distributed runner and Raft consensus (`src/distributed/`); config,
builder, runners, lifecycle, flow control, self-healing, HTTP control plane, the CLI, logging
and sampling (`src/service/`, `src/bin/saci-service/`); the metric series (`src/metrics.rs`); and
the in-process inspector (`src/inspector/`) with the `/ui` dashboard (`crates/saci-service-ui`).

## Conventions

- All async traits use `#[async_trait]`; `PipelineRuntime` uses `#[async_trait(?Send)]`.
- Tracing instrumentation is behind `#[cfg(feature = "tracing")]`, and every such site keeps a
  `#[cfg(not(feature = "tracing"))]` fallback so the value is still consumed.
- Metric call sites carry no `#[cfg]`: `crate::metrics::Instruments` has an identical, `#[inline]`,
  empty method surface when the `metrics` feature is off. Do not wrap a metric call in
  `#[cfg(feature = "metrics")]`.
- `saci::prelude` is the documented entry point for a downstream embedder: the engine types plus
  whatever the enabled `processor`, `connector-*` and `transformer-*` features add.
  `saci::processor::prelude` and `saci::plugin::prelude` are the narrower preludes of the two
  authoring SDKs.
- An example that needs nothing outside the `saci` crate (no processor component, plugin, config
  file or external service) lives in `crates/saci/examples/`, so the published package carries
  it. Every other runnable example lives under the top-level `examples/` directory, grouped by
  topic (`examples/native/`,
  `examples/connectors/`, `examples/plugins/`, `examples/conformance/`, `examples/configs/`, plus
  the end-to-end demo directories `examples/quickstart/`, `examples/polyglot/`,
  `examples/distributed_fulfillment/`, `examples/wasm/`, `examples/branching/`,
  `examples/windowing/`), and its `[[example]]` target in `crates/saci/Cargo.toml` sets a `path`
  into it; `cargo package` leaves those out, since it cannot reach outside the crate. Either way
  the `[[example]]` target is what makes `cargo run -p saci --example <name>` work and lets
  `required-features` gate it.
- Tests live in `#[cfg(test)]` modules within each source file; integration tests are in each crate's
  `tests/`, run through `cargo nextest`. See "Testing" for the fast/full profile split, the
  Docker soft-skip convention, and why testcontainers-backed tests are safe to run in parallel.
- Every new source connector, sink connector, transformer, and processor runtime is added to
  `crates/saci/tests/connector_matrix.rs`'s dimension lists and its `live_slices!` lists
  in the same change that introduces it; `dimensions_cover_the_registry` and
  `every_slice_has_a_test` fail on an omission. Skill `saci-connectors` has the checklist.
- Benchmarks use Criterion in each crate's `benches/`. Run them through `cargo xtask bench`, never
  bare `cargo bench`. The harness fixes `RUSTFLAGS`, compiles as a separate step so criterion does
  not share the machine with rustc, and takes the benchmark binary from cargo's own `Executable`
  line. Cargo's metadata hash encodes profile, features, and flags, so binaries built under
  different configurations coexist in `target/release/deps/` and picking the wrong one silently
  re-measures a stale build. Published figures are taken unpinned; `--affinity` is for A/B
  comparison only.
- Code quality is a finishing check, not optional: at the end of every task that touches
  `**/*.rs`, walk skill `rust-best-practices`'s review checklist against every
  changed file (ownership/error-handling intent, no unproven `unwrap`/`expect`/`panic!`,
  Clippy-clean, public-item docs, and test coverage of the new behavior and its error paths), in
  addition to the `cargo fmt`/`cargo clippy`/`cargo test` gates already required above.
- Performance is a finishing check, not optional: `cargo clippy --all-targets --all-features -- -D
  warnings` (already required above) covers Clippy's Perf lint group on every task. A change that
  touches a hot path (`Dataset`/`System`/`Pipeline` execution in the engine, Arrow IPC ser/de at
  the wasm host/processor boundary, or the `distributed` checkpoint and redb store paths)
  or a build profile/allocator/dependency setting also needs
  skill `rust-performance`'s triage workflow, validated with
  `cargo xtask bench <name>` before/after. The wasm host/processor boundary's instrument is
  `cargo xtask bench wasm_roundtrip` (`crates/saci/benches/wasm_roundtrip.rs`), which
  reports store lifecycle, host IPC encode, host IPC decode and the full `run_on_with_state`
  round trip as separate groups, since a change there routinely moves cost between phases while
  leaving the total flat.
- `arrow-ipc = "=60.1.0"` is exact-pinned workspace-wide. It is the host to processor wire format and the
  on-disk checkpoint format. See `crates/saci/PINS.md` before touching it.
- Every workspace member takes `version.workspace = true`, and every dependency on a sibling
  crate is `{ workspace = true }` against the entry in the root `[workspace.dependencies]`. One
  crate version therefore covers the whole workspace, declared in `[workspace.package] version`
  with the matching requirement in that table. An inherited dependency may add only `features`
  and `optional`; `default-features` is not a legal key there, so `saci`'s
  `default-features = false` (its default is `engine`, which cannot target wasm32-wasip2) sits
  in the workspace entry and a member asks for the feature it needs, `features = ["processor"]`
  or `features = ["plugin"]`. `crates/saci-service-ui` is excluded from the workspace,
  so it inherits nothing and repeats every field itself.
- Documentation, comments and prose: read skill `technical-writer`, then skill `saci-docs`, before
  writing or editing any `//`, `///` or `//!` comment, anything under `docs/`, `README.md`, or a
  skill, and walk both "Before you finish" lists over the diff. Current state only, no
  optimization history or task references; no mermaid, ASCII art or tables as diagrams, use SVG;
  no AI writing patterns.
