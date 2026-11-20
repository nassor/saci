# Changelog

`saci` and `saci-macros` share one version. The release process is in `AGENTS.md`, under
"Releases".

## Unreleased

### Breaking

- Features: `runtime` and `io` are folded into `engine`; a feature that implied either implies
  `engine`. A new `derive` feature carries `#[derive(Component)]` and is part of `engine`,
  `processor` and `plugin`.
- `#[derive(Component)]` works in every build (`saci::Component`, same name as the trait) and
  declares one `FieldRef` constant per field, named after the field in upper case
  (`Order::USD_AMOUNT`) and pointing at its serde column name. `#[saci(fields = false)]` opts out.
  The derived `schema()` is traced once per process.
- `Dataset`: `register_component` is `register`; `register_raw_component`,
  `register_raw_component_versioned` and `register_named_component` are `register_schema` and
  `register_schema_versioned`, which take any `&str` and return an error instead of panicking.
  Reads return `SaciResult`: `columns::<C>()` is `batch::<C>()`, `batch_for(name)` is
  `batch_named(name)`, `column::<C>("f")` is `column(C::F)` or `column_named("C", "f")`. New
  `resource`, `resource_mut`, `take_resource` and `set_column`. `DatasetBuilder` is removed.
- `SaciError`: `ComponentNotFound` and `EntityNotFound` are replaced by `ComponentNotRegistered` and
  `FieldNotFound`; `is_retryable()` is new and `RetryExhausted` exposes its cause through
  `std::error::Error::source`.
- Retry: `SystemConfig` is removed. `System::retry()` and `ParallelSystem::retry()` return a
  `RetryMode`, which defaults to `RetryMode::None`, and only an error whose `is_retryable()` is true
  is retried. `RetryExhausted` is returned only after at least one retry ran; otherwise the error
  comes back unwrapped. `RetryingSource`/`RetryingSink` and `run_with_retries*` take a `RetryMode`;
  `service::RetryConfig::to_system_config` is `to_retry_mode`.
- `SystemMeta`: `read`/`write` are `reads_named`/`writes_named`, `read_component`/`write_component`
  are `reads_all::<C>()`/`writes_all::<C>()` or `reads_all_named`/`writes_all_named`,
  `read_resource`/`write_resource` are `reads_resource`/`writes_resource`; the fields are private
  (`name()` reads the name). `FieldRef::field` is `FieldRef::name()`.
- `WriteSet`/`SliceWriteSet`: `put` takes a `FieldRef`, `put_named` takes strings,
  `with_resource` takes the value; the fields are private behind `len`, `is_empty`, `get_named`,
  `iter` and `SliceWriteSet::rows`. `ResourceUpdate` is no longer public.
- `ParallelSystem`: `run_slice` returns `SaciResult<SliceWriteSet>` and is used when `sliced()`
  returns `true`; `run` has a default that runs one slice over every row. New `run_sync` and
  `parallel_fn`.
- `Pipeline` is built with a consuming chain from `Pipeline::new`: `component`, `schema`,
  `resource`, `system`, `parallel_system`, `source`, `sink`. `PipelineBuilder`, the public `data`
  field, the `add_*` methods, `run_with_io`, `run_on_with_stats`, `last_stats`, `stages` and
  `stage_count` are removed. `run`, `run_on` and `run_stream` return the call's `RunStats`
  (`elapsed: Duration`, `retries`); `run` drains attached sources and fills attached sinks;
  `plan()` returns the stage plan.
- `Scheduler` is chained: `pipeline`, `pipeline_with`. `PipelineConfig::after(name)`,
  `after_data(name)` and `pause_when(f)` replace `after(name, DependencyKind)` and
  `BackpressureSpec`; a pipeline added after a tick runs on the next one.
- Root exports: connector and transformer types are named by module path
  (`saci::connector::file::FileSource`); `saci::async_trait` is re-exported, so a crate needs no
  `async-trait` dependency; `saci::partition` and `saci::resource` are private.
- Authoring SDK: `saci::processor` and `saci::plugin` are replaced by one `saci::sdk` module
  (`sdk::Config`, `sdk::Error`, `sdk::Result`, `sdk::State`, `sdk::RouteDecision`, `sdk::host`),
  and every macro is reached through `saci::prelude::*`. `export_pipeline!` and `export_plugin!`
  are removed: `#[processor]` exports a WebAssembly component and the new `#[plugin]` a native
  plugin, neither taking arguments; the pipeline's name is the descriptor name. A crate no longer
  writes its own `mod bindings`.
- State is declared on the pipeline with `Pipeline::state::<C>()` (`is_stateful`,
  `restore_state`, `capture_state`), not as a macro argument; `ProcessorState` is `sdk::State`.
- `#[transform]` and `#[fold]` take no arguments: the component and state types come from the
  function's parameters (`fn(row: &mut T)`, `fn(rows: &[T], state: &mut S)`).
- `sdk::host` (`trace`/`debug`/`info`/`warn`/`error`, `metric`, `config`, `config_parse`,
  `with_config` for tests) replaces the generated `saci_config_get`, `saci_config_parse`, `log`
  and `metric` functions and `saci::plugin::host`. `sdk::Config::new()` reads through it and
  fetches each key once.
- `RouteDecision(vec![..])` is `RouteDecision::to([..])` and `RouteDecision::none()`.
- `saci::window_spec` is `saci::window`, the one path of `WindowSpec` (also in the prelude), with
  `WindowSpec::tumbling`, `sliding`, `session`, `with_offset` and `kind`; `validate` returns
  `SaciResult`. `assign_tumbling` and `assign_sliding` are the free functions
  `window::tumbling_id` and `window::sliding_ids` (an iterator).
- `windows`: `WindowedSystemBuilder` is removed. `WindowedSystem::new(T::TIME, spec, function)`
  or `new_named(component, time_field, spec, function)` validates the geometry at construction;
  `key_by` and `allowed_lateness_ms` chain onto it. `WindowFunction::sum`, `min`, `max`, `count`,
  `mean` and `process` build the function. `WindowResults` fields are `on_time`, `late` and
  `dropped`; `SideOutput` and `DroppedLate` are removed. `saci::windows` exports a flat list, with
  `key_hash` for the key hash; an empty run publishes the same result schema as a non-empty one.
- `saci::transformer` is `saci::codec`: `Codec`, `CodecFactory`, `CodecRegistry` (`len`, `names`,
  `is_empty`), and `CsvCodec`, `NdjsonCodec`, `AvroCodec`, `ParquetCodec`, `ArrowIpcCodec` with
  their `*CodecFactory`. Features `transformer-*`, `transformers`, `transformer-contract` and
  `connector-contract` are `codec-*`, `codecs`, `codec-api` and `connector-api`.
  `ConnectorContext::codec` and `channels`, `ServiceBuilder::codec`, `Registry::register_codec`,
  `codecs` and `codec_count`, and `service::config::CodecSpec` follow;
  `ServiceBuilder::register_transformer_mut` is removed. KDL keys are unchanged.
- `codec`: every codec is built with `new()` and chained setters (`CsvCodec::new().has_headers`,
  `NdjsonCodec::new().infer_max`, `AvroCodec::new().compression(AvroCompression).schema_id`).
  A codec factory refuses an option it does not read. `Codec::open_reader` takes a `ReadInput`
  (`File` or `Memory`), and `Codec::media_type` names the HTTP content type.
- `io`: `saci::io::{Source, Sink, RetryingSource, RetryingSink, cast_batch, read_all, write_all}`
  is the one path. `drain_into_dataset` and `drain_dataset` are `read_all(source, data, comp)` and
  `write_all(sink, data, comp)`; `CastingSource` and `build_target_schema` are removed;
  `cast_batch` takes the target as `&Arc<Schema>`.
- Connectors: `TcpIngestSource` is `TcpSource`; `HttpPushSource` and `HttpPushOptions` are
  `HttpListenSource` and `HttpListenConfig`; `HttpSourceOptions`, `HttpSinkOptions` and
  `HttpClientOptions` are `*Config`; `http::Window` is `http::BatchWindow`. `connector::saci`
  (feature `connector-saci`, `SaciSource`, `SaciSink`) is `connector::peer` (`connector-peer`,
  `PeerSource`, `PeerSink`); `connector::opentelemetry` (`connector-opentelemetry`,
  `OpenTelemetryMetrics*`, `SourceModeConfig`) is `connector::otlp` (`connector-otlp`,
  `OtlpMetrics*`, `SourceMode`); `udp::ProbeSettings` is `udp::ProbeConfig`;
  `ClickhouseConnectionConfig` and `S3ConnectionConfig` are `ConnectionConfig` in their module.
  KDL type names are unchanged.
- Connectors: every constructor is `X::new(config, ..)`, a byte connector's taking
  `(config, schema, codec)`: `FileSource::new`/`new_async`, `FileSink::new` (a `truncate` config
  field), `TcpSink::new`, `UdpSink::new`, `PeerSource::new`, `PeerSink::new`, `RedbSink::new`,
  `HttpSource::new`, `HttpSink::new`, `HttpListenSource::new`. `ChannelSource::new` returns
  `(Self, Sender)`; `from_receiver` and `from_sender` are `with_receiver` and `with_sender`.
- Connectors: the file, tcp, udp, peer and channel factories deserialize a config struct and
  refuse an unknown key; a `0` buffer or size is a configuration error instead of being clamped
  (a channel `buffer 0` no longer panics). The HTTP factory refuses a key its role does not read.
- `service`: `BuiltService`, `ServiceFactory`, `StandaloneStats` and `NodeRunStats` are
  `BuiltWorkflow`, `WorkflowFactory`, `WorkflowStats` and `NodeStats`;
  `ServiceBuilder::build_all` is `build`. `ServiceBuilder::new()` registers every compiled-in
  connector and codec (`register_builtin_factories` is no longer public) and
  `ServiceBuilder::empty()` registers none. `with_runtime`, `with_inspector`, `with_dlq_registry`,
  `with_channel_bridge` and `with_wasm_engine` are `runtime` (taking `impl PipelineRuntime`),
  `inspector`, `dlq_registry`, `channels` and `wasm_engine`; `register_source_mut` and
  `register_sink_mut` are removed.
- `PipelineRuntime::run_batch(data, prior)` replaces `run_on`, `run_on_with_state` and
  `run_on_with_state_and_routes`. A native `Pipeline` handed to `ServiceBuilder::runtime`
  carries its declared state and honours a `RouteDecision`, as a wasm or plugin processor does.
- `service`: `ServiceConfig::from_kdl(text)` and `FromStr` parse a config held in memory.
- `wasm-host`: `WasmEngine::new()` returns `SaciResult`; `WasmPipelineRuntime::from_bytes` takes
  its deadline as a `Duration` and `PipelineRuntimeLoader::with_epoch_deadline(ticks)` is
  `with_timeout(Duration)`.
- `service`: the `/status` body loses its `cluster` field; `ClusterProbe`,
  `ClusterProbeSnapshot`, `ClusterStatusBody` and `MemberEntry` are removed.
- `service`: `BuiltWorkflow::run(options)` is the one way to run a workflow; it carries its run
  mode, flow plan and resume flag from the config it was built from. `RunOptions::new(control)`
  takes `live_stats` and `state`, and a `CancellationToken` or `RunControl` converts into it.
  `run_standalone` and `run_stream` are no longer public; `run_supervised` takes the factory
  only. `BuiltWorkflow::builder(id)` assembles a workflow by hand (`source`, `processor`,
  `sink`, `link`, `link_branch`, `run_mode`, `flow`, ...); `BuiltWorkflow` and `BuiltNode` are
  `#[non_exhaustive]`. `NodeStats::id` is `Arc<str>` and `kind` is `&'static str`.
- `service`: every module except `cli` is private; `saci::service::Item` is the one path.
- Connectors: one `Connector` trait (`name`, `roles`, `source`, `sink`, `rebuild`, `live`,
  `detail_keys`) replaces `SourceFactory` and `SinkFactory`; each connector registers once under
  a lowercase name (`channel`, `file`, `redb`, `http`, `http-listen`, `tcp`, `udp`, `peer`,
  `kafka`, `nats`, `s3`, `postgresql`, `mariadb`, `turso`, `clickhouse`, `otlp-metrics`).
  `Registry::register`, `connector`, `names` and `len` replace the per-role lookups;
  `ServiceBuilder::connector` replaces `register_source` and `register_sink`.
- Config: a source or sink names its connector with `connector="file"` (was `type="FileSource"`)
  and its codec with `codec="id"`; the `transformer "id" format=.. { options .. }` node is
  `codec "id" format=.. { config .. }`.
- Config: `run_mode kind=".."` is `run "once"`, `run "continuous"`, `run "stream"` or
  `run "interval" every_ms=N` (`RunMode::Once`, `RunMode::Interval { every_ms }`); cluster keys
  live in one `cluster { }` block; `http disabled=#true` is `http enabled=#false`; `wasm`/`plugin`
  take `path=`; a window's `key_field` is `key_fields`; the inspector takes `retention_ms` and
  `sample_interval_ms`. An unknown top-level key, or `mode "standalone"` with a `cluster` block,
  is an error.
- Config: `schema_fields` is `field` with one `FieldSpec`/`FieldType` for every connector
  (lowercase type names only; `pg_type` and `mariadb_type` are `native_type`), and a workflow
  can declare `schema "Component" { field .. }` once for every node of that component.
  `batch_size` on a kafka or nats source and an http `mode` is `batch_rows`; `schema_from
  "body"` and `"object"` are `"payload"`.
- CLI: `serve` takes `--bind` (replaces `--port`/`SACI_HTTP_PORT`), `--log-format`,
  `--log-level` and `--otlp-endpoint`; `status` and `cluster status` take `--url` (replaces
  `--addr`/`SACI_ADDR`) or read the bind address from `--config`; `cluster join` and
  `cluster leave` are removed. `GET /api/snapshot` takes `window_ms` (was `window_secs`).

### Added

- `service::serve(config, builder, ServeOptions)` runs the full service (stores, HTTP plane,
  runners, shutdown budget) from library code and returns a `ServeExit`; the `saci-service
  serve` command calls it.

### Changed

- `Dataset::append` drops the zero-row registration chunk, so a component read after one append
  needs no concatenation; `clone_empty` keeps the prebuilt empty templates `clear` uses.
- `plugin-host`: the output IPC of a native plugin is decoded in place instead of copied first,
  and the input buffer is presized from the previous batch.
- `windows`: sliding windows expand rows in one pre-sized pass, session windows reuse the key hash
  instead of hashing twice, and a `Reduce` aggregate gathers only its input column instead of
  sorting the whole batch.
- `connector-redb` and the HTTP listen source decode a value or request body from memory instead
  of spooling it to a temporary file; `cast_batch` returns its input when the schemas match.
- `connector-tcp`: the source reads frames through a buffered reader into one reused buffer, and
  the sink buffers writes and flushes once per batch; the channel source compares schemas by
  pointer first.
- `service`: the standalone and stream runners copy per-node stats only when they publish them,
  and weigh a carried batch only for a source under flow control.

## 0.2.0 - 2026-11-19

### Breaking

- MSRV bumped from `1.99.0` to `1.100.0` (required for `wasm32-wasip3`).
- `wasm-host`: WebAssembly components now require pure WASI 0.3 (`wasmtime_wasi::p3`,
  `wasm_component_model_async(true)`, `saci:pipeline@0.4.0` with `run-batch: async func(...)`).
  WASI 0.2 components (`wasm32-wasip2`, `wasi:*@0.2.x`, `saci:pipeline@0.3.0`) are no longer linked
  or loaded.
- `processor`: `cargo build --target wasm32-wasip3` and `wit-bindgen` with `features = ["async"]`
  are now required; `saci::processor` no longer pulls or re-exports `pollster` and drives
  `pipeline.run_on_with_stats(&mut dataset).await` directly inside `async fn run_batch`.
- Non-Rust WASI 0.2 guest SDK packages (`packages/saci-sdk-{go,py,ts,kt,kt-ksp,cs}`) and the
  `polyglot` examples have been removed because their toolchains do not yet target pure WASI 0.3;
  the native C ABI plugin SDK and Go plugin (`examples/plugins/settle-go/`) remain supported.

### Changed

- The `saci` package carries an empty `[lints]` table. A workspace that depends on saci by path and
  defines `[workspace.lints]` otherwise gets a `missing_lints_inheritance` warning on every build.
- `PipelineSlot::pipeline` returns `&Pipeline` instead of a `MutexGuard`, and `Pipeline` keeps its last-run
  stats behind a mutex instead of a `Cell`, so a pipeline built without `io` (what a processor builds with)
  is `Sync` and sits in a static. The exported async `run-batch` of a processor therefore no longer holds
  a lock across `.await`: a call entering the instance while another was suspended would have trapped on
  it, and clippy warned `await_holding_lock` in every processor crate.

## 0.1.2 - 2026-11-16

### Added

- `wasm-host`: `WasmPipelineRuntime::with_warm_instances(max_idle)` keeps instantiated processors
  between calls, so a processor can cache what it derives from its config.

## 0.1.1 - 2026-11-14

### Added

- Each crate has its own README as its crates.io page, with a dependency line and an example, plus
  keywords and categories. The `saci` README's example runs as a doc test.
- The `saci` package carries nine runnable examples in `examples/`: `first_pipeline`,
  `scheduler_etl`, `scheduler_etl_parallel`, `scheduler_dag`, `scheduler_parquet_etl`,
  `distributed_scheduler`, `window_aggregation`, `distributed_windowed` and `windowed_fan_in`.

### Changed

- `arrow-arith`, `fnv` and `futures` are optional dependencies. `windows` enables `arrow-arith`
  and `fnv`, `distributed` enables `fnv`, and `service` and `connector-udp` enable `futures`, so a
  build with only the default `engine` feature compiles none of them.

## 0.1.0 - 2026-11-14 (yanked)

### Added

- First release of `saci` and `saci-macros`. `saci` holds the columnar engine, the connectors,
  the byte formats, the processor and plugin SDKs and the `saci-service` host, each behind a
  feature; `saci-macros` holds the processor SDK's derive and attribute macros.

Yanked because the package carried no examples and compiled three dependencies that the default
build never used. Use 0.1.1.
