# AGENTS.md

Guidance for coding agents working in this repository.

## Project

SACI is a distributed batch processing engine for Rust built on Apache Arrow. Pipelines compile to
WebAssembly components; a host binary loads them at runtime. Edition 2024, MSRV 1.95.0.

The repository root is a **virtual manifest**: there is no root `src/`. Every command that names a
target needs `-p <crate>`.

## Skills

`.agents/skills/<name>/SKILL.md` holds the reference for one area. Read the matching skill
before working in that area; this file keeps the workspace map, the commands, the testing
profiles and the `saci-core` engine reference. "skill `X`" below means that path.

| Skill | Read before |
|---|---|
| `saci-connectors` | touching `saci-connector` or any `saci-connector-*` crate, a `SourceFactory`/`SinkFactory` registration, `ConnectorContext`, a connector config under `examples/configs/`, or `connector_matrix.rs` |
| `saci-transformers` | touching `saci-transformer` or any `saci-transformer-*` crate, a `format` registration, or a transformer option |
| `saci-processors` | touching `saci-processor`, `saci-macros`, any `wit/pipeline.wit`, `crates/saci-service/src/wasm/`, `packages/`, or `examples/{wasm,polyglot,quickstart,conformance}/` |
| `saci-plugins` | touching `saci-plugin`, `saci-plugin-abi`, `saci-plugin-smoketest`, `crates/saci-service/src/plugin/`, `crates/saci-service/src/service/plugin_loader.rs`, or `examples/plugins/` |
| `saci-service` | touching `crates/saci-service/src/{service,distributed,inspector,bin}/`, `src/metrics.rs`, `saci-inspector-wire`, `crates/saci-service-ui`, or `examples/{native,branching,windowing,multi_workflow,integrity,distributed_fulfillment}/` |
| `saci-docs` | writing or editing any `//`, `///` or `//!` comment, anything under `docs/`, `README.md`, a skill, or any other prose |
| `rust-best-practices` | the finishing review of every changed `.rs` file |
| `rust-performance` | a change on a hot path or to a build profile, allocator or dependency setting |

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
cargo nextest run -p saci-service --all-features --profile ci --test connector_matrix --run-ignored ignored-only  # Connector matrix; --profile ci required since --test can't bypass default's exclusion, see Testing below
cargo test --workspace --all-features --doc                  # Doc tests (nextest does not run these)
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps  # Rustdoc lints; public items of workspace members only, no #[cfg(test)] modules, no saci-service-ui
cargo fmt --all -- --check                                   # Check formatting
cargo clippy --all-targets --all-features -- -D warnings     # Lint (warnings are errors)
cargo clippy -p saci-service --no-default-features --features service --all-targets -- -D warnings  # Same gate on the narrowest runnable service; catches an item that is dead only when a feature is off
cargo nextest run -p saci-service --no-default-features --features service --lib  # Runs that same build; the only place the cfg(not(feature = ...)) tests needing neither processor host execute
cargo nextest run -p saci-service --no-default-features --features service,wasm,plugin --lib  # Both hosts, no windowing engine; the only place the three window-block refusal tests execute
cargo nextest run --workspace --lib                          # The default bundle; `ci.yml`'s "Test, default bundle" step says what only this configuration reaches
cargo xtask check-wasm-processor                             # saci-core and the saci facade still build for wasm32-wasip2 without tokio
cargo xtask bench tpch_q6                                    # Benchmarks, always via the harness
cargo check --examples --all-features                        # Verify examples compile; --all-features keeps connector-gated examples compiled
cargo run -p saci-service --example scheduler_etl            # Run an example
cargo run -p saci --example first_pipeline                   # The native tutorial's example
cargo run -p saci-service --example distributed_scheduler --features distributed  # No external service
cargo run -p saci-service --example scheduler_etl_parallel
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

Two fixtures must exist before `cargo nextest run` reaches `crates/saci-service/tests/`:

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
nextest profiles cover most of the workspace, plus one suite that neither runs by default; which
to reach for depends on why you're running tests:

- `cargo nextest run --workspace --all-features` (the `default` profile): the everyday command.
  It compiles the same `--all-features` closure as always, but skips every test that needs a
  Docker daemon: the testcontainers-backed connector suites
  (`saci-connector-kafka`/`-nats`/`-postgresql`/`-s3`'s `tests/`), the two whole Raft chaos
  binaries (`transport_chaos`, `distributed_harness_smoke`), the Docker-gated cluster tests in
  `raft_consensus_chaos` (its `unit` and `idempotency` modules are Docker-free and stay), the two
  Docker-backed tests each in `saci-service`'s `kafka_service` and `nats_service` (a source to
  sink round trip and the dead letter store's record, replay and consume cycle) and one apiece in
  its `postgres_service` and `s3_service`. The one non-Docker exclusion is
  `saci-connector-turso::synced_roundtrip`, which drives a synced replica against whatever remote
  endpoint `SACI_TURSO_URL`/`SACI_TURSO_TOKEN` name and clears a fixed table there. Run this
  constantly; it should stay fast regardless of how large the Docker-backed suites grow. The
  embedded store suite (`redb_store`) and the other distributed suites (`distributed_scheduler`,
  `runner_chaos`, `checkpoint_chaos`, `wasm_chaos`, `distributed_processor_state`) need no daemon
  either: the first drives a local redb file, the rest the in-memory store fixture in
  `tests/common/memory_store.rs`.
- `cargo nextest run --workspace --all-features --profile ci --run-ignored all` (the `ci`
  profile): the full suite, including everything `default` skips and any `#[ignore]`d test (today,
  `saci-service`'s lib unit test `test_second_init_returns_error` (a global-tracing-subscriber
  race), `distributed_integration_chaos.rs`'s `full_stack_chaos_monkey_60s`, a ~70-100s five-node
  Raft-cluster-behind-Toxiproxy chaos run asserting that every node converges on the same applied
  index under combined latency, bandwidth, reset and partition faults, and that isolating the
  leader halfway through the window costs it the office: a random single-edge fault only starves a
  follower of heartbeats when it happens to land on an edge out of the leader, so the election that
  proves liveness is forced by cutting every link of the current leader rather than left to the
  dice (the term-advance assertion was ~10% flaky on a shared runner before that),
  plus `connector_matrix.rs`'s `full_matrix`: `ci`'s `default-filter = "all()"` carries none of
  `default`'s exclusions, so this command also pays for the matrix's four containers).
  This is the full suite to run as the last verification step of a plan, not something to reach
  for on every edit. `ci.yml` splits it into three: the `test` job runs `--profile ci` without
  `--run-ignored`, a separate `distributed_chaos` job runs only `full_stack_chaos_monkey_60s`
  (`--test distributed_integration_chaos --run-ignored ignored-only`), and a separate
  `connector_matrix` job runs only `full_matrix`, so that neither test ever sits on `test`'s
  critical path. Regenerating or reordering the Docker/chaos exclusion list itself lives only in
  `.config/nextest.toml`; nothing here restates it, so that file is the one place to update when a
  test moves between the two profiles.
- `crates/saci-service/tests/connector_matrix.rs` (the `heavy-docker` test group, `ci.yml`'s
  `connector_matrix` job): one `full_matrix` test over every {source, sink, format, processor
  runtime} tuple, `#[ignore]`d and excluded from `default`, reached only through the command above
  with `--profile ci`, because `--test` selects a build target and cannot bypass `default`'s
  exclusion; plus the Docker-free `dimensions_cover_the_registry`. Dimensions, rejection sites and
  isolation: skill `saci-connectors`.

`cargo test --workspace --all-features --doc` remains a separate, always-bare-`cargo-test` step in
both this file and `ci.yml`: nextest does not run doctests.

Four commands cover the feature configurations. Three of them widen:
`-p saci-service --no-default-features --features service --lib` carries no connector and no
transformer, `--workspace --lib` is the default bundle, which registers only the
`BUILTIN_CONNECTOR_FEATURES` entries whose feature is on, and `--workspace --all-features`
registers every one of them. The fourth,
`-p saci-service --no-default-features --features service,wasm,plugin --lib`, is off that line
entirely: both processor hosts with no `windows`, the only configuration where a `window` block
sits on a node the build can host and finds no engine, which is what the three window-block
refusal tests need. A workspace default build is not each crate at its own defaults:
cargo unifies features across one resolve, so `saci-service`'s default bundle turns on
`saci-core`'s `io`, `windows`, `distributed`, `processor` and `tracing`.
`saci-core` runs 213 tests standalone and 297 inside a workspace build, and `saci-service` is the
only crate whose optional features nothing else in the workspace enables, so it carries the whole
feature-dependent surface the middle rung exists for. The other members come along because that
one resolve is cheaper than a per-package one: `-p` resolves features for one crate alone, while a
`--workspace` resolve takes the union every member asks for and reuses most of the
`--all-features` artifacts. The timings compare the resolve strategies, measured as compile time
on top of identical cold `--all-features` bases: `--workspace --lib` 24.7s, `--workspace` over
all targets 35.0s, `-p saci-service --lib` 56.2s. Current scale at those three rungs is 1645,
1856 and 553 tests. Every count here is what nextest runs under the `default` profile, which
`cargo nextest list` reports too; the profile's excluded tests are in neither.

Docker-backed tests soft-skip rather than fail when no daemon is reachable: each crate's
`tests/common/mod.rs` exposes a `try_start() -> Option<Container>` that starts a
`testcontainers::GenericImage`, catches the error, prints `SKIP: ... unavailable: {e}`, and returns
`None`; every such `#[tokio::test]` opens with `let Some(x) = common::try_start().await else {
return; };`. This is what lets the `ci` profile run unconditionally on a runner that may or may not
have Docker, and it needs no nextest-side accommodation.
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
`crates/saci-service/src/service/standalone.rs` is the instance, and the comment on its
`_keepalive` binding carries the reasoning in full. A test whose callsites sit in its own body,
like the filter tests in `crates/saci-service/src/service/logging.rs`, needs none of this.

The Raft chaos suites and the connector matrix are the exception, and the one place a runner
feature does the work. The `heavy-docker` test group caps `max-threads = 1` over
`transport_chaos`, `raft_consensus_chaos`, `distributed_harness_smoke`,
`distributed_integration_chaos` and `connector_matrix`, so those five take the machine one test at
a time. Unlike a connector test, which needs only its own container, each Raft chaos binary
contends for the Docker daemon, for host ports, and for enough CPU to run 3 to 5 raft nodes while
asserting on election and log-convergence deadlines; `connector_matrix` holds four containers
(Kafka, NATS, PostgreSQL, MinIO/S3) at once for its whole run instead. The group is declared once
and applied through an override in **both** profiles: `distributed_integration_chaos` and
`connector_matrix` are in it even though `default` never reaches either binary, because `ci` does.
Adding another heavy-Docker binary to the group means adding it to both `filter` expressions.

## Workspace layout

```
crates/
├── saci/                     # Facade crate for embedding SACI as a pure-Rust library;
│                             # `use saci::prelude::*;` is the entry point. Feature groups below.
├── saci-core/                # Engine primitives: Dataset, Pipeline, System, Scheduler, Component,
│                             # the Source/Sink traits and the schema cast helpers. Reference below.
├── saci-config/              # The configuration language: KDL into ConfigValue (serde_json::Value)
│                             # and ConfigMap; from_kdl_str, from_kdl_str_with_vars, one_or_many,
│                             # substitute_env_vars, substitute_vars. A parse failure after
│                             # substitution names the variables inserted, never a value.
├── saci-connector/           # SourceFactory, SinkFactory, ConnectorContext, NodeIdentity,
│                             # ChannelBridge, rebuildable. skill saci-connectors
├── saci-connector-{channel,datafusion,file,http,kafka,nats,postgresql,redb,s3,saci,tcp,turso}/
│                             # One crate per connector. skill saci-connectors
├── saci-transformer/         # Transformer, BatchReader, BatchWriter, MessageDecoder,
│                             # TransformerFactory, TransformerRegistry. skill saci-transformers
├── saci-transformer-{arrow-ipc,avro,csv,ndjson,parquet}/
│                             # One crate per byte format. skill saci-transformers
├── saci-processor/           # Processor SDK; owns the canonical WIT package. skill saci-processors
├── saci-processor-smoketest/ # Processor component used as a CI fixture. skill saci-processors
├── saci-macros/              # Proc macros behind saci-processor's derives. skill saci-processors
├── saci-plugin/              # Native plugin SDK, export_plugin!. skill saci-plugins
├── saci-plugin-abi/          # The C ABI: saci_abi_version, saci_plugin_v1, both vtables.
│                             # skill saci-plugins
├── saci-plugin-smoketest/    # Native plugin used as a CI fixture, cdylib only.
│                             # skill saci-plugins
├── saci-service/             # Host: wasmtime runtime, distributed/Raft, HTTP control plane, config
│                             # loading, the factory Registry, the binary. skill saci-service
├── saci-service-ui/          # The /ui dashboard: CSR Leptos, NOT a workspace member.
│                             # skill saci-service
└── saci-inspector-wire/      # The inspector's JSON contract, serde only. skill saci-service
examples/
├── native/                   # saci-core/saci-service tutorials and demos. skill saci-service
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
                              # polyglot, plugins, ui, bench, pack-sdk, validate, demo,
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

### `saci-core`

- `runtime` (**default**): tokio and rayon stage parallelism. Disable for wasm processor builds.
- `processor`: wasm32-wasip2 target, sequential-only execution driven by the `pollster` sync executor.
- `windows`: windowed aggregation. `WindowedSystem`, watermarks, `WindowAccumulator`. The
  `WindowSpec` geometry enum sits outside the feature, in `window_spec.rs`, so a host parses a
  `window` declaration in every build; `saci_core::windows` re-exports it.
- `io`: the `Source`/`Sink` traits and the schema cast helpers (implies `runtime`).
- `distributed`: types shared with the host's distributed layer (implies `runtime`).
- `tracing`: `tracing` crate integration. Gates the events and the three nested spans
  `pipeline/execution.rs` opens (`pipeline.run`, `pipeline.stage`, `system.execute`).

### `saci-service`

The **default** bundle is `mimalloc`, `service`, `wasm`, `windows`, `parquet-checkpoint`,
`connector-channel`, `connector-file`, `connector-http`, `connector-tcp`, `connector-saci`,
`connector-redb`, and
every transformer,
so `cargo install saci-service` yields a runnable binary with no flags. A connector ships by
default only when nothing has to be installed or already running for it to work; every connector
that needs a specific broker, database or object store first is opt-in instead:
`connector-postgresql`, `connector-nats`, `connector-s3`, `connector-kafka` and
`connector-turso`. `connector-kafka` also needs `cmake` and a C toolchain for vendored
librdkafka; `connector-turso`'s synced mode pulls hyper and rustls. `distributed-raft` and
`service-cluster` are opt-in too, because a cluster node is a deliberate deployment choice. `all`
turns on every capability the crate carries, including all five opt-in connectors, the native
plugin host and a Raft cluster node, but excludes `conformance`, the corpus generator's own
switch. The full table, `connector-*`, `transformer-*`, `wasm`, `plugin`, `distributed`,
`distributed-raft`, `service`, `service-cluster`, `metrics`, `inspector`, `parquet-checkpoint`,
`windows`, `all`, and what each implies, is in skill `saci-service`.

### `saci`

A facade over the columnar engine and every connector, transformer, and authoring crate that needs
nothing installed or already running, for a downstream crate that wants one dependency
instead of several path entries. `saci::prelude` is the documented entry point; `saci_core::prelude`,
`saci_processor::prelude` and `saci_plugin::prelude` remain for a crate depending on those directly.
The **default** bundle is `engine` alone.

- `engine` (**default**): the columnar engine (implies `saci-core/io`, which implies `runtime`).
- `windows`: forwards `saci-core/windows`, effective under whichever of `engine`, `processor` or
  `plugin` is enabled, since all three enable `dep:saci-core` directly.
- `connector-channel`, `connector-file`, `connector-redb`, `connector-http`, `connector-tcp`,
  `connector-saci`, `connector-datafusion` (`connectors` enables all seven): one per connector
  crate that needs
  nothing installed or already running. Deliberately excludes `saci-connector-kafka`, `-nats`,
  `-postgresql`, `-s3` and `-turso`, each of which requires a specific broker, database or object
  store installed and running first.
- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet` (`transformers` enables all five): one per byte-format crate.
- `processor`: `Pipeline`, `System`, `Component` (the trait, not the derive), `Config`, `Error`,
  `Result` and `export_pipeline!`, for `--no-default-features --features processor`: saci-core
  then carries `processor` instead of `engine`'s `runtime`, so the build never needs tokio on
  wasm32-wasip2. Does not carry `#[derive(Component)]`/`#[transform]`/`#[fold]`/`#[processor]`:
  their expansions name `saci-processor` literally, which resolves only as a direct dependency,
  not one reached through this facade; a processor crate using those macros depends on
  `saci-processor` directly.
- `plugin`: `Pipeline`, `System`, `Component`, `ProcessorState`, `RouteDecision` and
  `export_plugin!`, for a native plugin: a cdylib the host `dlopen`s.
- `all`: every feature this crate carries, `engine`, `windows`, `connectors`, `transformers`,
  `processor` and `plugin`, in one build. `all` reaches `saci-core`'s `runtime` feature through
  `engine`, and tokio cannot target `wasm32-wasip2`; a wasm processor crate still builds with
  `--no-default-features --features processor`.

`engine`, `processor` and `plugin` each enable `dep:saci-core` directly, so every name they
share is one `pub use` in `crates/saci/src/lib.rs`, not three with a precedence rule between
them: the types are identical regardless of which of saci-core's own `runtime`/`processor`
features produced them. The three are still not meant to be enabled together: each authors a
different artifact, a host binary, a wasm component, or a native shared library. Doing so is
redundant, not ambiguous.

### `saci-core`, columnar engine

- **`Component` trait** (`src/component.rs`): any type providing `name() -> &'static str` and an
  Arrow `Schema`. Rows serialize via `serde_arrow`.
- **`Dataset`** (`src/dataset.rs` plus the ten-file `src/dataset/` submodule): Arrow-backed
  columnar container. Each registered component holds a `Vec<RecordBatch>` of appended chunks, and
  a read (`batch_for`, `column`, `view`) goes through `get_or_build_merged`, which concatenates
  them once into a `merged_cache` entry; `chunks.rs` owns that merge. A component may hold fewer
  rows than the dataset's row count (a windowing processor's reduced result component), never more.
  Holds a `SchemaRegistry`, a `ResourceMap`, and an alive bitmap. Supports batch `append`, soft
  delete (`mark_dead`), compaction, and IPC round-trip. Builder: `DatasetBuilder`. Canonical path:
  `saci_core::dataset::Dataset`.
- **`Row`** (`src/row.rs`): stable row index (`u32`). Invalidated by `compact`.
- **`Resource`** (`src/resource.rs`): boxed Rust singleton stored in `Dataset`, keyed by `TypeId`.
  Not columnar, and **not** serialized by `write_ipc`. The processor SDK relies on that to keep
  cross-batch state out of the data plane.
- **`Source`/`Sink` traits** (`src/io/`, `io` feature): `Source::finish`, like `Sink::finish`, is
  called once after the last `next_batch`, and defaults to nothing. It is where a source whose
  delivery is a commitment makes it durable, so a caller that dropped the source instead sees the
  same data again; every wrapper (`Box`, `CastingSource`, `RetryingSource`, `HealingSource`)
  forwards it, and `saci-service`'s runners call it per exit path after the sinks are finished.
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

### `saci-service`, host

The host is documented in four skills. `saci-connectors` and `saci-transformers` carry the IO
layer: a connector moves bytes, a transformer turns bytes into `RecordBatch`es and back, and
`ServiceBuilder` resolves a node's `transformer` id against the `TransformerRegistry` so no
connector resolves a format itself. `saci-processors` carries the wasm host (`src/wasm/`) and
the processor SDK; `saci-plugins` carries the native plugin host and SDK. `saci-service`
carries the rest: the distributed runner and Raft consensus (`src/distributed/`); config,
builder, runners, lifecycle, flow control, self-healing, HTTP control plane, logging and
sampling (`src/service/`, `src/bin/saci-service/`); the metric series (`src/metrics.rs`); and
the in-process inspector (`src/inspector/`) with the `/ui` dashboard (`crates/saci-service-ui`).

## Conventions

- All async traits use `#[async_trait]`; `PipelineRuntime` uses `#[async_trait(?Send)]`.
- Tracing instrumentation is behind `#[cfg(feature = "tracing")]`, and every such site keeps a
  `#[cfg(not(feature = "tracing"))]` fallback so the value is still consumed.
- Metric call sites carry no `#[cfg]`: `crate::metrics::Instruments` has an identical, `#[inline]`,
  empty method surface when the `metrics` feature is off. Do not wrap a metric call in
  `#[cfg(feature = "metrics")]`.
- `saci_core::prelude`, `saci_processor::prelude`, `saci_plugin::prelude` and `saci_service::prelude`
  each re-export that crate's own public API. `saci::prelude` is the documented entry point for a
  downstream embedder: it re-exports whichever of those the enabled feature groups pull in, plus
  the enabled `connector-*`/`transformer-*` crates, behind one crate name.
- Every runnable example lives under the top-level `examples/` directory, grouped by topic
  (`examples/native/`, `examples/connectors/`, `examples/plugins/`, `examples/conformance/`,
  `examples/configs/`, plus the end-to-end demo directories `examples/quickstart/`,
  `examples/polyglot/`, `examples/distributed_fulfillment/`, `examples/wasm/`,
  `examples/branching/`, `examples/windowing/`). A crate's `Cargo.toml` still declares the
  `[[example]]` target, which is
  what makes `cargo run -p <crate> --example <name>` work and lets `required-features` gate it,
  but its `path` always points into `examples/...` rather than a `<crate>/examples/` subtree, so
  the example's source never spreads across crates. Add a new example under the topic directory it
  belongs to (or a new one), never back under a crate's own directory.
- Tests live in `#[cfg(test)]` modules within each source file; integration tests are in each crate's
  `tests/`, run through `cargo nextest`. See "Testing" for the fast/full profile split, the
  Docker soft-skip convention, and why testcontainers-backed tests are safe to run in parallel.
- Every new source connector, sink connector, transformer, and processor runtime is added to
  `crates/saci-service/tests/connector_matrix.rs`'s dimension lists in the same change that
  introduces it; `dimensions_cover_the_registry` fails on an omission. Skill `saci-connectors` has
  the checklist.
- Benchmarks use Criterion in each crate's `benches/`. Run them through `cargo xtask bench`, never
  bare `cargo bench`. The harness fixes `RUSTFLAGS`, compiles as a separate step so criterion does
  not share the machine with rustc, and takes the benchmark binary from cargo's own `Executable`
  line. Cargo's metadata hash encodes profile, features, and flags, so binaries built under
  different configurations coexist in `target/release/deps/` and picking the wrong one silently
  re-measures a stale build. Published figures are taken unpinned; `--affinity` is for A/B
  comparison only.
- Code quality is a finishing check, not optional: at the end of every task that touches
  `**/*.rs`, walk `.agents/skills/rust-best-practices/SKILL.md`'s review checklist against every
  changed file (ownership/error-handling intent, no unproven `unwrap`/`expect`/`panic!`,
  Clippy-clean, public-item docs, and test coverage of the new behavior and its error paths), in
  addition to the `cargo fmt`/`cargo clippy`/`cargo test` gates already required above.
- Performance is a finishing check, not optional: `cargo clippy --all-targets --all-features -- -D
  warnings` (already required above) covers Clippy's Perf lint group on every task. A change that
  touches a hot path (`Dataset`/`System`/`Pipeline` execution in `saci-core`, Arrow IPC ser/de at the
  `saci-service` wasm host/processor boundary, or the `distributed` checkpoint and redb store paths)
  or a build profile/allocator/dependency setting also needs
  `.agents/skills/rust-performance/SKILL.md`'s triage workflow, validated with
  `cargo xtask bench <name>` before/after. The wasm host/processor boundary's instrument is
  `cargo xtask bench wasm_roundtrip` (`crates/saci-service/benches/wasm_roundtrip.rs`), which
  reports store lifecycle, host IPC encode, host IPC decode and the full `run_on_with_state`
  round trip as separate groups, since a change there routinely moves cost between phases while
  leaving the total flat.
- `arrow-ipc = "=59.3.0"` is exact-pinned workspace-wide. It is the host to processor wire format and the
  on-disk checkpoint format. See `crates/saci-processor/PINS.md` before touching it.
- Every workspace member takes `version.workspace = true`, and every dependency on a sibling
  crate is `{ workspace = true }` against the entry in the root `[workspace.dependencies]`. One
  crate version therefore covers the whole workspace, declared in `[workspace.package] version`
  with the matching requirement in that table. An inherited dependency may add only `features`
  and `optional`; `default-features` is not a legal key there, so `saci-core`'s
  `default-features = false` (its default is `runtime`, which cannot target wasm32-wasip2) sits
  in the workspace entry and a member that wants the host runtime asks for `features =
  ["runtime"]` or `features = ["io"]`. `crates/saci-service-ui` is excluded from the workspace,
  so it inherits nothing and repeats every field itself.
- Documentation, comments and prose: read `.agents/skills/saci-docs/SKILL.md` before writing or
  editing any `//`, `///` or `//!` comment, anything under `docs/`, `README.md`, or a skill, and
  walk its "Before you finish" list over the diff. Current state only, no optimization history or
  task references; no mermaid, ASCII art or tables as diagrams, use SVG; no AI writing patterns.
