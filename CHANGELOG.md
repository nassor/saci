# Changelog

`saci` and `saci-macros` share one version. The release process is in `AGENTS.md`, under
"Releases".

## Unreleased

### Changed

- Dependencies: `wasmtime` and `wasmtime-wasi` 51, `redb` 5, `rdkafka` 0.40 (bundled librdkafka
  2.16.0), `async-nats` 0.53, `turso` 0.8.4, `openraft` 0.10.1, plus the compatible patch
  releases. redb files written by earlier releases open unchanged: no saci table stores a type
  whose encoding redb 5 changed. `mysql_common` stays on 0.37, the line `mysql_async` 0.37.1
  depends on.

### Fixed

- `connector::nats`: a JetStream `NatsSource` whose `next_batch` was cancelled mid-window keeps
  its open pull request and drains it on the next call. Before, the dropped request stayed open on
  the server, and a message published before it expired went to an inbox nobody read and came
  back only after `ack_wait`.

## 0.3.0 - 2026-11-21

### Migrating from 0.2

What changed and why is in [Breaking](#breaking); this section is the lookup from the 0.2
spelling to the new one. No 0.2 name compiles any more: every row is a break.

#### Rust

| 0.2 | Now |
| --- | --- |
| `Dataset::register_component::<C>()` | `register::<C>()` |
| `register_raw_component`, `register_raw_component_versioned`, `register_named_component` | `register_schema`, `register_schema_versioned`; return `Result` |
| `Dataset::columns::<C>()` → `Option` | `batch::<C>()` → `SaciResult` |
| `Dataset::batch_for(name)` → `Option` | `batch_named(name)` → `SaciResult` |
| `Dataset::column::<C>("f")` → `Option` | `column(C::F)` or `column_named("C", "f")` → `SaciResult` |
| `DatasetBuilder`, `PipelineBuilder`, `Pipeline::add_*`, `run_with_io`, `run_on_with_stats` | `Pipeline::new().component().schema().system().source().sink()` |
| `Scheduler::add_pipeline*`, `PipelineConfig::after(name, kind)`, `BackpressureSpec` | `Scheduler::pipeline`, `pipeline_with`; `after`, `after_data`, `pause_when` |
| `SystemMeta::read(c, f)`, `write(c, f)` | `reads_named`, `writes_named` |
| `SystemMeta::read_component(c)`, `write_component(c)` | `reads_all_named(c)`, `writes_all_named(c)` (or `reads_all::<C>()`) |
| `SystemMeta::read_resource::<R>()`, `write_resource::<R>()` | `reads_resource::<R>()`, `writes_resource::<R>()` |
| `FieldRef::field` (public field) | `FieldRef::name()` |
| `SystemConfig`, `System::retry() -> SystemConfig` | `RetryMode` |
| `saci::window_spec::WindowSpec` | `saci::window::WindowSpec` |
| `WindowSpec::assign_tumbling(ts, size, offset)` | `window::tumbling_id(ts, size, offset)` |
| `WindowSpec::assign_sliding(..)` → `Vec<i64>` | `window::sliding_ids(..)` → iterator |
| `WindowSpec::validate` → `Result<(), String>` | `validate` → `SaciResult<()>` |
| `WindowedSystemBuilder`, `SideOutput`, `DroppedLate` | `WindowedSystem::new(T::TIME, spec, function)`; `WindowResults::{on_time, late, dropped}` |
| `io::drain_into_dataset(src, data, comp)`, `io::drain_dataset(data, comp, sink)` | `io::read_all(src, data, comp)`, `io::write_all(sink, data, comp)` |
| `saci::transformer::*`, `Transformer`, `TransformerFactory`, `TransformerRegistry`, `*Transformer` | `saci::codec::*`, `Codec`, `CodecFactory`, `CodecRegistry`, `*Codec` |
| `saci::processor`, `saci::plugin`, `export_pipeline!`, `export_plugin!` | `saci::sdk`, `#[processor]`, `#[plugin]` |
| `saci::ProcessorState<C>`, `sdk::ProcessorState<C>` | `sdk::State<C>` |
| `RouteDecision(vec![..])` | `RouteDecision::to([..])`, `RouteDecision::none()` |
| `connector::tcp::TcpIngestSource` | `TcpSource`, built with `TcpSource::new(config, schema, codec)` |
| `connector::http::{HttpPushSource, HttpPushOptions, Window}` | `HttpListenSource`, `HttpListenConfig`, `BatchWindow` |
| `connector::http::HttpClientOptions` | `HttpClientConfig` |
| `connector::http::{HttpSourceOptions, HttpSinkOptions}` | `HttpSourceConfig`, `HttpSinkConfig` |
| `connector::saci::{SaciSource, SaciSink}` | `connector::peer::{PeerSource, PeerSink}` |
| `connector::opentelemetry::*` | `connector::otlp::*` (`OtlpMetrics*`, `SourceMode`) |
| `connector::udp::ProbeSettings` | `ProbeConfig` |
| `clickhouse::ClickhouseConnectionConfig` | `clickhouse::ConnectionConfig` |
| `s3::S3ConnectionConfig` | `s3::ConnectionConfig` |
| `ChannelSource::from_receiver`, `ChannelSink::from_sender` | `with_receiver`, `with_sender` |
| `ChannelSource::new` → `(Sender, Self)` | `new` → `(Self, Sender)` |
| `ConnectorContext::channel_bridge()` | `channels()` |
| `ConnectorContext::transformer(what)` | `codec(what)` |
| `connector::schema::parse_schema_fields` → `Arc<Schema>`, `parse_optional_schema_fields` | `parse_fields` → `Vec<FieldSpec>`, `parse_optional_fields`; `schema_of(&fields)` builds the schema |
| `service::BuiltService`, `ServiceBuilder::build_all` | `BuiltWorkflow`, `build` |
| `service::ServiceFactory` | `WorkflowFactory` |
| `service::StandaloneStats` | `WorkflowStats` (its `nodes` are `NodeStats`) |
| `service::NodeRunStats` (`id: String`, `kind: String`) | `NodeStats` (`id: Arc<str>`, `kind: &'static str`) |
| `ServiceBuilder::with_dlq_registry`, `with_inspector`, `with_channel_bridge`, `with_wasm_engine` | `dlq_registry`, `inspector`, `channels`, `wasm_engine` |
| `ServiceBuilder::with_runtime(id, Box<dyn PipelineRuntime>)` | `runtime(id, impl PipelineRuntime)` |
| `ServiceBuilder::register_source`, `register_sink`, `register_transformer`, `*_mut` | `connector(c)`, `codec(f)` |
| `service::config::TransformerSpec` (`options`) | `CodecSpec` (`config`) |
| `RetryConfig::to_system_config` | `to_retry_mode` |
| `PipelineRuntime::run_on*` | `run_batch(data, prior)` |
| `WasmEngine::new()`, `PipelineRuntimeLoader::with_epoch_deadline(ticks)` | `new() -> SaciResult`, `with_timeout(Duration)` |
| `service::run_standalone`, `run_stream` | `BuiltWorkflow::run(options)` |

#### Cargo features

The 0.2 feature names are gone; a build that names one fails to resolve it.

| 0.2 | Now |
| --- | --- |
| `runtime`, `io` | `engine` |
| `transformers` | `codecs` |
| `transformer-arrow-ipc`, `-avro`, `-csv`, `-ndjson`, `-parquet` | `codec-arrow-ipc`, `-avro`, `-csv`, `-ndjson`, `-parquet` |
| `transformer-contract` | `codec-api` |
| `connector-contract` | `connector-api` |
| `connector-saci` | `connector-peer` |
| `connector-opentelemetry` | `connector-otlp` |

#### KDL

| 0.2 | Now |
| --- | --- |
| `source "id" type="FileSource"`, `sink "id" type="FileSink"` | `source "id" connector="file"`, `sink "id" connector="file"` |
| `transformer "id" format=".." { options .. }`, `transformer="id"` on a node | `codec "id" format=".." { config .. }`, `codec="id"` |
| `wasm "id" module=".."`, `plugin "id" library=".."` | `wasm "id" path=".."`, `plugin "id" path=".."` |
| `run_mode kind="one_shot"` | `run "once"` |
| `run_mode kind="continuous"`, `kind="stream"` | `run "continuous"`, `run "stream"` |
| `run_mode kind="interval" interval_ms=N` | `run "interval" every_ms=N` |
| `lease_ttl_ms`, `election_timeout_ms`, `bootstrap`, `peer ..` at top level | the same nodes inside one `cluster { }` block |
| `http disabled=#true` | `http enabled=#false` |
| `window .. { key_field "a" }` | `window .. { key_fields "a" }` |
| `schema_fields "id" type="Int64"` | `field "id" type="int64"`; `pg_type` and `mariadb_type` are `native_type` |
| a schema repeated on every node of a component | one `schema "Component" { field .. }` in the workflow |
| `batch_size` on a kafka or nats source, http `mode` | `batch_rows` |
| `schema_from "body"`, `"object"` | `schema_from "payload"` |

An unknown top-level key, or `mode "standalone"` beside a `cluster` block, is an error.

Before:

```kdl
run_mode kind="interval" interval_ms=5000

workflow "orders" {
    transformer "csv_fmt" format="csv" {
        options has_headers=#true
    }
    source "in" type="FileSource" component="Order" transformer="csv_fmt" {
        config {
            path "orders.csv"
            schema_fields "id" type="Int64" nullable=#false
            schema_fields "total" type="Float64"
        }
    }
    wasm "settle" module="settle.wasm" {
        window kind="tumbling" size_ms=30000 time_field="ts" {
            key_field "region"
        }
    }
    sink "out" type="FileSink" component="Order" transformer="csv_fmt" {
        config { path "out.csv" }
    }
    link from="in" to="settle"
    link from="settle" to="out"
}
```

After:

```kdl
run "interval" every_ms=5000

workflow "orders" {
    codec "csv_fmt" format="csv" {
        config has_headers=#true
    }
    schema "Order" {
        field "id" type="int64" nullable=#false
        field "total" type="float64"
    }
    source "in" connector="file" component="Order" codec="csv_fmt" {
        config { path "orders.csv" }
    }
    wasm "settle" path="settle.wasm" {
        window kind="tumbling" size_ms=30000 time_field="ts" {
            key_fields "region"
        }
    }
    sink "out" connector="file" component="Order" codec="csv_fmt" {
        config { path "out.csv" }
    }
    link from="in" to="settle"
    link from="settle" to="out"
}
```

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
- `metrics`: series renamed: `saci_workflow_runs_total` → `saci_workflow_passes_total`,
  `saci_workflow_errors_total` → `saci_workflow_pass_errors_total`, `saci_rows_processed_total`
  → `saci_source_rows_total`, `saci_source_batches_drained_total` → `saci_source_batches_total`,
  `saci_sink_batches_written_total` → `saci_sink_batches_total`, `saci_sink_rows_written_total`
  → `saci_sink_rows_total`, `saci_liveness_counter` → `saci_watchdog_ticks_total`,
  `saci_dlq_letters` → `saci_dlq_letters_pending`, `saci_processor_metric` →
  `saci_processor_custom`.
- `metrics`: each event is recorded once, with its node or workflow attribute; the unattributed
  duplicate sample is gone. A process-wide total is `sum()` over the attribute.

### Added

- `SystemMeta::read_fields`, `write_fields`, `read_components`, `write_components`, `read_resources` and
  `write_resources` read back what a system declared.
- `service::serve(config, builder, ServeOptions)` runs the full service (stores, HTTP plane,
  runners, shutdown budget) from library code and returns a `ServeExit`; the `saci-service
  serve` command calls it.
- `config::to_kdl_string` and `config::to_kdl` write a value tree or any `Serialize` type as KDL, the
  inverse of `from_kdl_str`; `ServiceConfig::to_kdl` writes a whole service config. A program that
  generates its config builds the typed specs and lets saci spell the keywords. Default-valued
  fields are left out, a `dlq` with only a store writes as `dlq "redb"`, and the `config` feature
  turns on `serde_json/preserve_order` so output keeps declaration order.
- Constructors for the config specs: `ServiceConfig::standalone`, `NodeConfig::new`,
  `WorkflowSpec::new`, `SourceSpec::new`, `SinkSpec::new`, `WasmSpec::new`, `PluginSpec::new`,
  `CodecSpec::new`, `LinkSpec::new`, `SchemaSpec::new`, `WindowConfig::new`.
- `ServiceConfig::from_kdl` names the replacement when a config uses a 0.2 spelling (`type=`,
  `transformer`, `run_mode`, `schema_fields`, `module`, `key_field`, the old connector type names).
- `FieldSpec` is `Serialize`; `FieldSpec::new`, `from_arrow` and `from_schema` declare existing
  Arrow fields.
- `runtime::StatelessRuntime`: implement `run(&mut Dataset)` and get `PipelineRuntime` through a
  blanket impl, for runtimes that carry no state and pick no route.
- `service::Registry::names_for(Role)`; every built-in connector module exports its `NAME`
  (`connector::nats::NAME == "nats"`); `saci::codec` re-exports each codec type
  (`saci::codec::NdjsonCodec`).
- `Dataset::with::<C>()` and `with_schema(name, schema)`: consuming, chainable registration.

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
- `metrics`: the runners, the distributed runner, the healing wrapper, the dead letter queue and
  the processor hosts bind one metric handle per node, so recording a sample allocates no
  attribute.

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
