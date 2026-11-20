+++
title = "Embedding saci-service"
description = "Assemble the service yourself: register your own factories and runtimes, build every workflow, and drive a runner from your own main."
template = "section.html"
sort_by = "weight"
weight = 12
+++
The service is a library before it is a binary: `saci::service` with the `service` feature on.
`ServiceBuilder` turns a `ServiceConfig` into one `BuiltWorkflow` per declared workflow, and
`BuiltWorkflow::run` drives it. Everything the stock binary does, your own `main` can do with two
more calls: your factories and your runtimes.

For a server that only adds factories, skip the assembly below. A binary crate whose `main` is
`saci::service::cli::main_with(|builder| builder.register_source(MyFactory))` runs the stock
`serve` and `validate` subcommands with your factory registered after the built-in ones.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 170" role="img" aria-labelledby="emb-title emb-desc">
        <title id="emb-title">From a loaded config to a running workflow, in your own binary</title>
        <desc id="emb-desc">
            ServiceConfig::load reads the KDL file. ServiceBuilder takes it, together with the
            source, sink and codec factories you register and any runtime you hand it
            through runtime or wasm_engine, and build returns one BuiltWorkflow per
            declared workflow. Each BuiltWorkflow is driven by run, which owns the
            drain loop until its cancellation token fires.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="70" width="140" height="56" rx="8"/>
            <text class="t-lbl" x="12" y="92">ServiceConfig</text>
            <text class="t-sm" x="12" y="110">load("saci.kdl")</text>
        </g>
        <g class="anim anim-2">
            <text class="t-sm t-ctl" x="176" y="22">register_source, register_sink, codec</text>
            <text class="t-sm t-ctl" x="176" y="40">wasm_engine, runtime</text>
            <path class="arw arw-ctl" d="M266 48 V64" marker-end="url(#emb-c)"/>
            <path class="arw arw-ctl" d="M140 98 H170" marker-end="url(#emb-c)"/>
            <rect class="blk blk-ctl" x="176" y="70" width="180" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="176" y="70" width="180" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="176" y="82" width="180" height="8"/>
            <text class="t-lbl" x="188" y="85">ServiceBuilder</text>
            <text class="t-sm" x="188" y="112">build(&amp;config)</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M356 98 H380" marker-end="url(#emb-d)"/>
            <rect class="blk blk-data" x="386" y="70" width="130" height="56" rx="8"/>
            <text class="t-lbl" x="398" y="92">BuiltWorkflow</text>
            <text class="t-sm" x="398" y="110">one per workflow</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M516 98 H530" marker-end="url(#emb-d)"/>
            <rect class="blk blk-data" x="536" y="70" width="124" height="56" rx="8"/>
            <text class="t-lbl" x="548" y="92">run(cancel)</text>
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

## Your own sources, sinks, codecs, and runtime

A `type` string in the config is a key into a registry of factories. To add your
own, implement one trait and register it before `build()`. The factory receives
that node's `config` plus a `ConnectorContext` carrying the codec the host
resolved, and returns a boxed `Source` or `Sink`.

```rust,name=A sink factory for your own connector
use saci::connector::{ConfigValue, ConnectorContext, SinkFactory};
use saci::SaciError;
use saci::io::Sink;

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
        // The codec is whichever declared `transformer` id this sink
        // named. Naming the connector puts it in the error a sink that named
        // none gets.
        let codec = ctx.codec("ClickHouseSink")?;
        Ok(Box::new(ClickHouseSink::connect(url, codec)?))
    }
}
```

Then assemble the service yourself. `ServiceBuilder::new()` registers the
connectors and codecs whose features are on, plus a default
`ChannelRegistry` bridge when `connector-channel` is one of them, and nothing
else; `ServiceBuilder::empty()` registers nothing. `register_source`,
`register_sink` and `codec` chain your own on, where registering the same name
twice replaces the earlier factory.

```rust,name=Assembling the service yourself
use saci::service::{ServiceBuilder, ServiceConfig};
use tokio_util::sync::CancellationToken;

let config = ServiceConfig::load("service.kdl")?;

// `build()` constructs every declared node and validates every link end to
// end before it returns, one `BuiltWorkflow` per declared workflow.
let mut built = ServiceBuilder::new()
    .register_source(MongoSourceFactory)
    .register_sink(ClickHouseSinkFactory)
    .codec(ProtobufCodecFactory)
    .build(&config)?;

// A bare CancellationToken runs the workflow with no pause gate, no live stats
// and no store.
let stats = built.remove(0).run(CancellationToken::new()).await?;
```

`ServiceConfig::load` reads a KDL file. `ServiceConfig::from_kdl` parses KDL text you
already hold, with the same variable substitution and validation, and `ServiceConfig`
implements `FromStr` through it.

For a native Rust processor, declare its `wasm` node with no `module` and hand
the builder a runtime keyed by that node's id. Any `PipelineRuntime` works, and a
`Pipeline` is one: its `run_batch` restores and captures the state declared with
`Pipeline::state` and delivers the `sdk::RouteDecision` a system leaves behind.

```rust,name=Handing the builder a native runtime
let built = ServiceBuilder::new()
    .runtime("enrich", my_pipeline)
    .register_sink(ClickHouseSinkFactory)
    .build(&config)?;
```

A node naming a `module` or a `library` loads that artifact and never looks at
`runtime`. A node naming neither takes the runtime registered under its own
id, and `build()` errors naming the node when nothing is registered for it.

The stock binary stops at the first type it cannot resolve, so rerun `validate`
after each registration:

```bash,name=What the stock binary says about a custom type
saci-service validate --config service.kdl

WARNING: no sink factory registered for type 'ClickHouseSink' (required by sink 'orders_out')
NOTE: 1 unknown type(s) above are not in the built-in registry. They may be
user-defined types registered at serve time. Use --strict to treat these as errors.
```

In your own binary `build()` is the check: it constructs every declared
node, so an unregistered `type` is an error there rather than a warning.

## What a runtime says about itself

A host holding a `PipelineRuntime` has five methods: `name()`, `run_batch()`,
`template_dataset()`, `declared_components()` and `descriptor_info()`.
`run_batch(data, prior)` is the one call that runs a pass. `prior` is the state blob the
runtime returned last time, or `None` on the first call, and the call returns a
`RuntimeOutput { state, routes }`: the next blob and the optional
[branch routes](@/service/processors/branching.md). A runtime that keeps its state in the
dataset ignores `prior` and returns `RuntimeOutput::default()`.

`name()` is whatever the host handed the runtime at load: the node id for a
`wasm` node, the plugin's own manifest name for a `plugin`, and the pipeline's
name for a runtime registered through `runtime`. `descriptor_info()` is the
identity the runtime declares for itself: `name`, `version`, `stateful` and
`schema_fingerprint`, where `name` is a component's `describe()` name or a
plugin's manifest name. [The dashboard](@/service/operate/dashboard.md) prints those four
fields on every processor node, which is how you confirm the artifact running is
the one you built.

## Hosting a component or a plugin from code

`runtime` covers a runtime you built in Rust. A `.wasm` file or a shared
library is loaded through its own constructor and handed over the same way.

```rust,name=Loading a component and a plugin yourself
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use saci::plugin_host::NativePluginRuntime;
use saci::service::ServiceBuilder;
use saci::wasm_host::{WasmEngine, WasmPipelineRuntime};

// One engine per process is enough: it owns the wasmtime Engine, the epoch
// ticker and the compiled programs.
let engine = WasmEngine::new()?;

let component = WasmPipelineRuntime::from_bytes(
    engine.clone(),
    "enrich",                       // the runtime's name
    &std::fs::read("enrich.wasm")?, // the component bytes
    HashMap::from([("rate".to_string(), "0.2".to_string())]),
    Duration::from_secs(10),        // the longest one call may run
)?;

let plugin = NativePluginRuntime::open(
    Path::new("target/release/libsettle.so"),
    HashMap::new(),
)?;

let built = ServiceBuilder::new()
    .wasm_engine(engine)
    .runtime("enrich", component)
    .runtime("settle", plugin)
    .build(&config)?;
```

`WasmEngine::new` returns `SaciResult<WasmEngine>` and spawns the epoch
ticker, a tokio task that fires every 100 ms, so it panics when called outside a
tokio runtime. The ticker stops with the last clone of the engine. The `timeout` that
`from_bytes` takes becomes an epoch deadline of that many 100 ms ticks, rounded up;
`PipelineRuntimeLoader::with_timeout` sets the same limit for config-loaded nodes and
defaults to 10 seconds.

`ServiceBuilder::wasm_engine` hands one engine to several builders, so the
compile cost is paid once for a set of bytes however many workflows load it.
`NativePluginRuntime::open` checks the ABI version, parses the manifest and
recomputes the schema fingerprint from the decoded component schemas, all
inside the constructor, so a value it returns is a plugin whose schemas the
host has already verified. The optional `sha3_256` digest is not its business:
the config loader checks that before calling `open`. [How the host runs a
processor](@/library/processor-host.md) is what happens around each call.

## Running a workflow from code

`BuiltWorkflow::run` drives one workflow until its run mode finishes it or its options
cancel it. The workflow carries its run mode, its flow-control policy and whether batch
runs resume from the store, all fixed when it was built, so `run` takes nothing but its
options. A `stream` run mode runs the stream runner and every other mode runs the
standalone runner.

```rust,name=Running a built workflow
impl BuiltWorkflow {
    pub async fn run(self, options: impl Into<RunOptions>) -> SaciResult<WorkflowStats>;
}

let options = RunOptions::new(cancel)
    .live_stats(live.clone())
    .state(store.clone());
let stats = workflow.run(options).await?;
```

`RunOptions` is what the runner reads besides the workflow. A `CancellationToken` or a
`RunControl` converts into one, so an uncontrolled run is `workflow.run(cancel)`.

`RunOptions::new(control)` <small>`control: impl Into<RunControl>`</small>
:   the cancellation token plus the pause gate the runner parks on between passes. A bare
    `CancellationToken` converts into a gate that never parks.

`.live_stats(stats)` <small>`Arc<RwLock<WorkflowStats>>`</small>
:   the handle `/status` reads; leaving it unset opts out of publishing.

`.state(client)` <small>`Arc<RedbStateClient>`</small>
:   the client over the `store "redb"` file. Without it source cursors and processor
    priors stay in loop memory for the life of the process.

A stream workflow requires at least one source node, and `run` returns a configuration
error when none is declared. [How flow control decides](@/library/flow-control.md) is
what the workflow's `FlowPlan` drives.

## Assembling a workflow by hand

`BuiltWorkflow::builder` builds a workflow without a config file: a native pipeline
between a channel source and sink in a test, or a graph your program computes. Declare
nodes in flow order and link them.

```rust,name=A workflow without a config
use saci::service::{BuiltWorkflow, RunMode};

let workflow = BuiltWorkflow::builder("orders")
    .source("in", "Order", source)
    .processor("enrich", my_pipeline)
    .sink("out", "Order", sink)
    .link("in", "enrich")
    .link("enrich", "out")
    .run_mode(RunMode::Stream)
    .build()?;

let stats = workflow.run(cancel).await?;
```

`source`, `processor` and `sink` take a node id and, for a source or sink, the component
it carries. `link_branch(from, to, branch)` links on a named branch,
`window(processor_id, window)` attaches a `window` declaration, `name` sets the display
name, `inspector` records flow-control decisions into an inspector and `dlq` gives the
workflow a dead letter queue.

`run_mode` defaults to `RunMode::Continuous`. `flow` sets the `FlowPlan`; without one a
stream workflow gets `FlowPlan::stream_default()` and every other run mode the batch
policy, which carries no latency objective. `batch_resume` defaults to off.

`build()` returns a configuration error for a duplicate node id, a link or window naming
an undeclared node, a window on a node that is not a processor, a link from a node to itself, and a link whose target was declared before its origin. It does not run
`validate_workflow_graph`, and a source or sink added this way is not wrapped in a retry
or heal wrapper.

## Serving from code

`serve` is what `saci-service serve` runs once it has parsed its flags: it initialises
logging and the meter provider, opens the store, builds every workflow, binds the HTTP
control plane and runs the workflows until a shutdown signal arrives. The builder you pass
carries your factories and runtimes; `serve` adds the dead letter queue registry and the
inspector to it.

```rust,name=Your own serve
use saci::service::{ServeExit, ServeOptions, ServiceBuilder, ServiceConfig, serve};

let config = ServiceConfig::load("service.kdl")?;
let builder = ServiceBuilder::new().register_sink(ClickHouseSinkFactory);

let exit = serve(
    config,
    builder,
    ServeOptions {
        config_path: Some("service.kdl".into()),
        announce: true,
        ..ServeOptions::default()
    },
)
.await?;

if exit == ServeExit::ShutdownBudgetExceeded {
    std::process::exit(1);
}
```

`config_path` <small>`Option<PathBuf>`</small>
:   the config file; with a `store` block its raw bytes are stored beside the redb state.
    `None` stores nothing.

`announce` <small>`bool`</small>
:   print the three stdout lines `serve` owns: the listening line, the dashboard or
    endpoints line and `saci-service stopped cleanly`.

`shutdown` <small>`Option<CancellationToken>`</small>
:   stops the service when cancelled, as SIGINT and SIGTERM do. `None` leaves the signals
    as the only way to stop it.

`ServeExit::Clean` means every runner, the HTTP server and the watchdog drained inside the
30 second shutdown budget. `ServeExit::ShutdownBudgetExceeded` means the budget ran out.
`serve` installs the process-wide tracing subscriber and meter provider, so it runs at
most once per process.
