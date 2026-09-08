+++
title = "Embedding saci-service"
description = "Assemble the service yourself: register your own factories and runtimes, build every workflow, and drive a runner from your own main."
template = "section.html"
sort_by = "weight"
weight = 12
+++
`saci-service` is a library before it is a binary. `ServiceBuilder` turns a
`ServiceConfig` into one `BuiltService` per declared workflow, and a runner
drives it. Everything the stock binary does, your own `main` can do with two
more calls: your factories and your runtimes.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 170" role="img" aria-labelledby="emb-title emb-desc">
        <title id="emb-title">From a loaded config to a running workflow, in your own binary</title>
        <desc id="emb-desc">
            ServiceConfig::load reads the KDL file. ServiceBuilder takes it, together with the
            source, sink and transformer factories you register and any runtime you hand it
            through with_runtime or with_wasm_engine, and build_all returns one BuiltService per
            declared workflow. Each BuiltService is passed to run_standalone, which owns the
            drain loop until its cancellation token fires.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="70" width="140" height="56" rx="8"/>
            <text class="t-lbl" x="12" y="92">ServiceConfig</text>
            <text class="t-sm" x="12" y="110">load("saci.kdl")</text>
        </g>
        <g class="anim anim-2">
            <text class="t-sm t-ctl" x="176" y="22">register_source, register_sink, register_transformer</text>
            <text class="t-sm t-ctl" x="176" y="40">with_wasm_engine, with_runtime</text>
            <path class="arw arw-ctl" d="M266 48 V64" marker-end="url(#emb-c)"/>
            <path class="arw arw-ctl" d="M140 98 H170" marker-end="url(#emb-c)"/>
            <rect class="blk blk-ctl" x="176" y="70" width="180" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="176" y="70" width="180" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="176" y="82" width="180" height="8"/>
            <text class="t-lbl" x="188" y="85">ServiceBuilder</text>
            <text class="t-sm" x="188" y="112">build_all(&amp;config)</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M356 98 H380" marker-end="url(#emb-d)"/>
            <rect class="blk blk-data" x="386" y="70" width="130" height="56" rx="8"/>
            <text class="t-lbl" x="398" y="92">BuiltService</text>
            <text class="t-sm" x="398" y="110">one per workflow</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M516 98 H530" marker-end="url(#emb-d)"/>
            <rect class="blk blk-data" x="536" y="70" width="124" height="56" rx="8"/>
            <text class="t-lbl" x="548" y="92">run_standalone</text>
            <text class="t-sm" x="548" y="110">until cancelled</text>
        </g>
        <defs>
            <marker id="emb-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="emb-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> config and assembly</span>
        <span class="k-data"><i></i> the built graph and the loop that drives it</span>
    </div>
</div>

## Your own sources, sinks, transformers, and runtime

A `type` string in the config is a key into a registry of factories. To add your
own, implement one trait and register it before `build_all()`. The factory receives
that node's `config` plus a `ConnectorContext` carrying the transformer the host
resolved, and returns a boxed `Source` or `Sink`.

```rust,name=A sink factory for your own connector
use saci_connector::{ConfigValue, ConnectorContext, SinkFactory};
use saci_core::SaciError;
use saci_core::io::sink::Sink;

struct ClickHouseSinkFactory;

impl SinkFactory for ClickHouseSinkFactory {
    // This is the string the config writes as `type="ClickHouseSink"`.
    fn type_name(&self) -> &'static str { "ClickHouseSink" }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let url = config
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SaciError::configuration("ClickHouseSink needs a 'url'"))?;
        // The transformer is whichever declared `transformer` id this sink
        // named. Naming the connector puts it in the error a sink that named
        // none gets.
        let transformer = ctx.transformer("ClickHouseSink")?;
        Ok(Box::new(ClickHouseSink::connect(url, transformer)?))
    }
}
```

Then assemble the service yourself. `register_builtin_factories` adds the
connectors and transformers whose features are on, plus a default
`ChannelRegistry` bridge when `connector-channel` is one of them, and nothing
else. `register_source`, `register_sink` and `register_transformer` chain your
own on, where registering the same name twice replaces the earlier factory.

```rust,name=Assembling the service yourself
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::{ServiceBuilder, run_standalone};
use tokio_util::sync::CancellationToken;

let config = ServiceConfig::load("service.kdl")?;

// `build_all()` constructs every declared node and validates every link end to
// end before it returns, one `BuiltService` per declared workflow.
let mut built = register_builtin_factories(ServiceBuilder::new())
    .register_source(MongoSourceFactory)
    .register_sink(ClickHouseSinkFactory)
    .register_transformer(ProtobufTransformerFactory)
    .build_all(&config)?;

// Each workflow gets its own runner. `None` opts out of publishing live stats
// to /status.
let stats =
    run_standalone(built.remove(0), &config, CancellationToken::new(), None, None).await?;
```

For a native Rust processor, declare its `wasm` node with no `module` and hand
the builder a runtime keyed by that node's id. Any `Box<dyn PipelineRuntime>`
works, and a `Pipeline` is one.

```rust,name=Handing the builder a native runtime
let built = ServiceBuilder::new()
    .with_runtime("enrich", Box::new(my_pipeline))
    .register_sink(ClickHouseSinkFactory)
    .build_all(&config)?;
```

A node naming a `module` or a `library` loads that artifact and never looks at
`with_runtime`. A node naming neither takes the runtime registered under its own
id, and `build_all()` errors naming the node when nothing is registered for it.

The stock binary stops at the first type it cannot resolve, so rerun `validate`
after each registration:

```bash,name=What the stock binary says about a custom type
saci-service validate --config service.kdl

WARNING: no sink factory registered for type 'ClickHouseSink' (required by sink 'orders_out')
NOTE: 1 unknown type(s) above are not in the built-in registry. They may be
user-defined types registered at serve time. Use --strict to treat these as errors.
```

In your own binary `build_all()` is the check: it constructs every declared
node, so an unregistered `type` is an error there rather than a warning.

## What a runtime says about itself

A host holding a `Box<dyn PipelineRuntime>` has seven methods: `name()`,
`run_on()`, `run_on_with_state()`, `run_on_with_state_and_routes()`,
`template_dataset()`, `declared_components()` and `descriptor_info()`. The two
state-carrying calls are what thread a processor's checkpoint blob and its
[branch routes](@/service/processors/branching.md) through a pass.

`name()` is whatever the host handed the runtime at load: the node id for a
`wasm` node, the plugin's own manifest name for a `plugin`, and the pipeline's
name for a runtime registered through `with_runtime`. `descriptor_info()` is the
identity the runtime declares for itself: `name`, `version`, `stateful` and
`schema_fingerprint`, where `name` is a component's `describe()` name or a
plugin's manifest name. [The dashboard](@/service/operate/dashboard.md) prints those four
fields on every processor node, which is how you confirm the artifact running is
the one you built.

## Hosting a component or a plugin from code

`with_runtime` covers a runtime you built in Rust. A `.wasm` file or a shared
library is loaded through its own constructor and handed over the same way.

```rust,name=Loading a component and a plugin yourself
use std::collections::HashMap;
use std::path::Path;
use saci_service::plugin::NativePluginRuntime;
use saci_service::service::ServiceBuilder;
use saci_service::wasm::{WasmEngine, WasmPipelineRuntime};

// One engine per process is enough: it owns the wasmtime Engine, the epoch
// ticker and the compiled programs.
let engine = WasmEngine::new()?;

let component = WasmPipelineRuntime::from_bytes(
    engine.clone(),
    "enrich",                       // the runtime's name
    &std::fs::read("enrich.wasm")?, // the component bytes
    HashMap::from([("rate".to_string(), "0.2".to_string())]),
    100,                            // epoch_deadline_ticks: 100 ticks of 100 ms
)?;

let plugin = NativePluginRuntime::open(
    Path::new("target/release/libsettle.so"),
    HashMap::new(),
)?;

let built = ServiceBuilder::new()
    .with_wasm_engine(engine)
    .with_runtime("enrich", Box::new(component))
    .with_runtime("settle", Box::new(plugin))
    .build_all(&config)?;
```

`WasmEngine::new` returns `wasmtime::Result<WasmEngine>` and spawns the epoch
ticker, a tokio task that fires every 100 ms, so it panics when called outside a
tokio runtime. The ticker stops with the last clone of the engine.

`ServiceBuilder::with_wasm_engine` hands one engine to several builders, so the
compile cost is paid once for a set of bytes however many workflows load it.
`NativePluginRuntime::open` checks the ABI version, parses the manifest and
recomputes the schema fingerprint from the decoded component schemas, all
inside the constructor, so a value it returns is a plugin whose schemas the
host has already verified. The optional `sha3_256` digest is not its business:
the config loader checks that before calling `open`. [How the host runs a
processor](@/library/processor-host.md) is what happens around each call.

## Running a workflow from code

`run_standalone` takes the config it was built from, so it resolves its own run
mode and its own flow-control policy, and dispatches to `run_stream` when
`run_mode kind="stream"` asks for it.

```rust,name=The two runner entry points
run_standalone(
    built: BuiltService,
    config: &ServiceConfig,
    control: impl Into<RunControl>,
    live_stats: Option<Arc<RwLock<StandaloneStats>>>,
    state: Option<Arc<RedbStateClient>>,
) -> Result<StandaloneStats, SaciError>

run_stream(
    built: BuiltService,
    control: impl Into<RunControl>,
    live_stats: Option<Arc<RwLock<StandaloneStats>>>,
    state: Option<Arc<RedbStateClient>>,
    flow: &FlowPlan,
) -> Result<StandaloneStats, SaciError>
```

A direct `run_stream` call has no config to resolve and no run mode to read off a
`BuiltService`, so it names the policy itself: pass `FlowPlan::stream_default()`.
`FlowPlan::default()` is the batch policy and carries no latency objective.
`run_stream` also requires at least one source node and returns a configuration
error when none is declared. [How flow control
decides](@/library/flow-control.md) is what the plan drives.

`control` is the cancellation token plus the pause gate the runner parks on
between passes. A bare `CancellationToken` converts into one whose gate never
parks, which is what the assembly example higher up this page passes.

`live_stats` is the handle `/status` reads; `None` opts out of publishing.
`state` is the `RedbStateClient` over the `store "redb"` file, and `None` keeps
source cursors and processor priors in loop memory for the life of the process.
