+++
title = "What the loader validates"
description = "The graph rules and schema gates ServiceBuilder applies before anything runs."
template = "page.html"
weight = 4
+++
# What the loader validates

`ServiceBuilder::build_all` is the gate. It parses the document, constructs every
declared node in topological order, and checks the graph end to end before it
returns a `BuiltService`. A binary that assembles the service itself gets every
refusal below as an `Err` from that one call.

## The load-time graph rules

`WorkflowSpec::validate` enforces, in order, one refusal per violation:

- The workflow declares at least one source, processor or sink node.
- Every id (workflow and node) matches the id charset and length bound.
- No id is declared twice, across transformers, sources, processors and sinks.
- Every `source.transformer` and `sink.transformer` names a declared
  `transformer` id.
- Every `link.from` and `link.to` names a declared source, processor or sink id,
  and no link names a node twice (`from == to`). A transformer id is not a
  graph node.
- No `(from, to)` pair is declared twice.
- Nothing links into a source; nothing links out of a sink.
- The graph is acyclic.
- Every source has an outbound link; every sink has an inbound one.
- Cluster mode declares exactly one processor and no source, sink or link, and
  no `window` block.
- `run_mode kind="stream"` declares at least one source.
- Outside stream mode, no source is live: a `tcp` source always is, and a
  `NatsSource` or `KafkaSource` is live unless its config sets `stop_at_end
  #true` (`KafkaSource` also accepts `compacted #true`, which always reaches
  EOF).
- Every `retry` block is valid.
- Every link `branch` matches the id charset and is at most 64 bytes.
- A labelled link starts at a processor.
- A node labels every outbound link or none.
- Every `window` block is geometrically sane, names a non-empty `time_field`,
  and has a non-negative `allowed_lateness_ms`.

Across workflows, `ServiceConfig::validate` adds:

- Cluster mode declares exactly one workflow.
- Workflow ids are unique.
- Every declared id, transformer ids included, is unique across all workflows.
  The metric attribution keys are bare node ids, so an overlap would double
  count.
- Every `ChannelSource` name is paired with exactly one `ChannelSink` of the same
  name, and vice versa.

## What is not a key

`workflow` holds no `systems` node and no `components` node, and `wasm` takes no
`watch` property: nothing in the service builds a `System` from a type name, so
those keys are parse errors.

`WorkflowSpec`, `TransformerSpec`, `SourceSpec`, `SinkSpec`, `WasmSpec`,
`PluginSpec`, `RetryConfig`, `HealConfig` and `FlowControlConfig` carry
`#[serde(deny_unknown_fields)]`, and the `window` block rejects unknown keys
through its own hand-written reader, so a typo inside `workflow` fails the
parse. `ServiceConfig`, `NodeConfig`, `ObservabilityConfig`, `HttpConfig`,
`StandaloneConfig`, `ClusterConfig` and `LinkSpec` do not: an unrecognised key
at the top level, under `observability`, or on a `link` node is accepted and
ignored.

## What it refuses to start on

Four checks, in a fixed order, each one a refusal rather than a warning. Nothing
about your component is touched until gate 2.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 452" role="img" aria-labelledby="svc-g-title svc-g-desc">
        <title id="svc-g-title">The four load-time gates a saci-service start must pass</title>
        <desc id="svc-g-desc">
            Four gates run in order. First the config file is read, environment placeholders are
            substituted, and the document is parsed strictly and cross-validated, which is where
            the graph rules on ids and links are enforced. Second
            the WASM module is read, digest-checked, compiled and instantiated, which is
            where wasmtime matches the WIT world. Third every declared link is checked end
            to end: the components at its two ends must match and their Arrow fields must be
            identical. Fourth, in cluster mode only, the processor's Arrow schema fingerprint
            is compared with the fingerprint recorded in this node's persisted checkpoints.
        </desc>
        <text class="t-title" x="0" y="14">Load order</text>
        <text class="t-sm" x="0" y="30">each gate is a refusal to start, not a warning</text>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="44" width="430" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="44" width="430" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="58" width="430" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="59">1 &nbsp;ServiceConfig::load</text>
            <text class="t-sm" x="12" y="80">read the file, substitute ${VAR}, parse the KDL strictly</text>
            <text class="t-sm" x="12" y="94">then validate(): data_dir, peer ids, store block, bind addr</text>
            <text class="t-sm" x="12" y="108">unique node ids, link endpoints, no link cycle</text>
            <text class="t-lbl t-ctl" x="448" y="59">rejects</text>
            <text class="t-sm" x="448" y="80">a link into a source</text>
            <text class="t-sm" x="448" y="94">a duplicate node id</text>
            <text class="t-sm" x="448" y="108">a store block in cluster mode</text>
        </g>
        <path class="arw arw-bnd" d="M215 120 V132" marker-end="url(#svc-b)"/>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="0" y="136" width="430" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="136" width="430" height="22" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="150" width="430" height="8"/>
            <text class="t-lbl t-bnd" x="12" y="151">2 &nbsp;PipelineRuntimeLoader::load</text>
            <text class="t-sm" x="12" y="172">read the module bytes, check the optional sha3_256</text>
            <text class="t-sm" x="12" y="186">compile, then instantiate against saci:pipeline@0.3.0</text>
            <text class="t-sm" x="12" y="200">describe() is called once here, not at the first batch</text>
            <text class="t-lbl t-bnd" x="448" y="151">rejects</text>
            <text class="t-sm" x="448" y="172">a digest mismatch</text>
            <text class="t-sm" x="448" y="186">a missing import</text>
            <text class="t-sm" x="448" y="200">a trap in describe()</text>
        </g>
        <path class="arw arw-ctl" d="M215 212 V224" marker-end="url(#svc-c)"/>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="0" y="228" width="430" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="228" width="430" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="242" width="430" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="243">3 &nbsp;validate_workflow_graph</text>
            <text class="t-sm" x="12" y="264">every link end to end: the components at its two</text>
            <text class="t-sm" x="12" y="278">ends must match and their Arrow fields be identical.</text>
            <text class="t-sm" x="12" y="292">Runs inside ServiceBuilder::build_all, before it returns</text>
            <text class="t-lbl t-ctl" x="448" y="243">rejects</text>
            <text class="t-sm" x="448" y="264">sink 'orders_out' reads</text>
            <text class="t-sm" x="448" y="278">'Order', which the</text>
            <text class="t-sm" x="448" y="292">processor never declares</text>
        </g>
        <path class="arw arw-data" d="M215 304 V316" marker-end="url(#svc-d)"/>
        <g class="anim anim-4">
            <rect class="blk blk-data" x="0" y="320" width="430" height="76" rx="8"/>
            <rect class="hd hd-data" x="0" y="320" width="430" height="22" rx="8"/>
            <rect class="hd hd-data" x="0" y="334" width="430" height="8"/>
            <text class="t-lbl t-data" x="12" y="335">4 &nbsp;validate_schema_fingerprint</text>
            <text class="t-sm" x="12" y="356">the processor's Arrow schema fingerprint against the one</text>
            <text class="t-sm" x="12" y="370">written into this node's persisted checkpoints</text>
            <text class="t-sm" x="12" y="384">cluster mode only: inside run_cluster, once raft settles</text>
            <text class="t-lbl t-data" x="448" y="335">rejects</text>
            <text class="t-sm" x="448" y="356">a schema change laid</text>
            <text class="t-sm" x="448" y="370">on top of checkpoints</text>
            <text class="t-sm" x="448" y="384">of the older shape</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 420 H654"/>
            <text class="t-sm" x="0" y="440">Gates 1 to 3 also run under <tspan class="t-ctl">saci-service validate</tspan>. Only <tspan class="t-ctl">serve</tspan> reaches gate 4.</text>
        </g>
        <defs>
            <marker id="svc-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="svc-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
            <marker id="svc-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> state already persisted</span>
        <span class="k-control"><i></i> config and host checks</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
    </div>
    <figcaption class="dgm-cap">
        Gate 3 is the only one that compares two things you wrote: a component name in
        your config against a component name in your Rust. It is also the only gate that
        <b>silently passes</b> when a runtime declares nothing. An empty component list
        opts that link out of the comparison rather than failing it.
    </figcaption>
</div>

Gate 2 as drawn is the WebAssembly path. `PipelineRuntimeLoader::load` exists
only under the `wasm` feature; a `plugin` node reaches the same gate through
`load_plugin_runtime`, which resolves a path rather than bytes, checks the
optional `sha3_256` on the file, and hands off to `NativePluginRuntime::open`.
That constructor calls the plugin's `describe` once, so a manifest error, an ABI
version mismatch or a schema fingerprint disagreement still surfaces here rather
than at the first batch.

Gate 3 is `validate_workflow_graph`, which `build_all` runs on every link before
it returns. Gate 4 is `validate_schema_fingerprint(runtime, persisted)`, in
cluster mode only, after raft settles. It compares
`runtime.template_dataset().schemas().fingerprint()` against the `u32` recorded
beside this node's checkpoints, and a node with no persisted state passes.

```text,name=What a mismatch prints
schema fingerprint mismatch: the pipeline declares 0000dead but this node's
persisted checkpoints were written with 0000beef. The deployed pipeline's
component schemas changed. Either restore the previous pipeline or clear
node.data_dir before starting with the new schema.
```

`saci-service validate` runs gates 1 to 3 and exits, so a stale `component` name
is caught without moving any data. Only `serve` reaches gate 4.

[Embedding saci-service](@/library/service/_index.md) is the builder call these
gates hang off, and [the config file](@/service/config/_index.md) is the document
they read.
