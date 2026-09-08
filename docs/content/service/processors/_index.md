+++
title = "Processors in a workflow"
description = "Declare a processor node, pass it configuration, and keep state across batches."
template = "section.html"
sort_by = "weight"
weight = 6
+++

A `wasm` node names a compiled processor and runs it over every batch the workflow delivers to
it. You can declare one, hand it configuration, chain two of them, keep its state across batches,
and read its counters.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 226" role="img" aria-labelledby="pn-title pn-desc">
        <title id="pn-title">A processor node between a source and a sink, with its config and its checkpoint</title>
        <desc id="pn-desc">
            The source orders_in hands a batch to the wasm node enrich, which names a module
            file. The config block above enrich supplies key-value strings the processor reads
            for itself. The node's output goes on to the sink orders_out. Below enrich, a
            checkpoint arrow leaves the node and comes back into it as the next batch's prior,
            which is the only state that crosses a batch boundary.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="72" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="72" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="84" width="130" height="8"/>
            <text class="t-lbl" x="12" y="87">orders_in</text>
            <text class="t-sm" x="12" y="110">source</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M130 100 H200" marker-end="url(#pn-d)"/>
            <rect class="blk blk-bnd" x="200" y="62" width="200" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="200" y="62" width="200" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="200" y="74" width="200" height="8"/>
            <text class="t-lbl t-bnd" x="212" y="77">enrich</text>
            <text class="t-sm" x="212" y="100">wasm node</text>
            <text class="t-sm" x="212" y="120">module=&quot;enrich.wasm&quot;</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="230" y="0" width="150" height="36" rx="8"/>
            <text class="t-lbl" x="242" y="23">config</text>
            <text class="t-sm t-ctl t-end" x="368" y="23">strings</text>
            <path class="arw arw-ctl" d="M305 36 V62" marker-end="url(#pn-c)"/>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M400 100 H470" marker-end="url(#pn-d)"/>
            <rect class="blk blk-data" x="470" y="72" width="140" height="56" rx="8"/>
            <rect class="hd hd-data" x="470" y="72" width="140" height="20" rx="8"/>
            <rect class="hd hd-data" x="470" y="84" width="140" height="8"/>
            <text class="t-lbl" x="482" y="87">orders_out</text>
            <text class="t-sm" x="482" y="110">sink</text>
            <path class="arw arw-bnd" d="M370 138 V178 H230 V138" marker-end="url(#pn-b)"/>
            <text class="t-sm t-bnd t-mid" x="300" y="194">checkpoint comes back as prior</text>
        </g>
        <defs>
            <marker id="pn-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="pn-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="pn-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the batch, in and out</span>
        <span class="k-boundary"><i></i> the processor, and the checkpoint that crosses a batch</span>
        <span class="k-control"><i></i> the configuration you write for it</span>
    </div>
</div>

## 1. Declare a wasm node

A `wasm` node takes an id as its leading argument and a `module` naming the `.wasm` file. A
relative `module` resolves against the directory `saci-service` runs in, and an absolute path is
used as it stands.

```kdl,name=A processor between a source and a sink
workflow "orders" {
    wasm "enrich" module="pipelines/enrich.wasm"

    link from="orders_in" to="enrich"
    link from="enrich" to="orders_out"
}
```

Add `sha3_256` to pin the artifact. The value is the hex SHA3-256 of the module file's bytes,
with an optional `sha3-256:` prefix, and a mismatch refuses the load instead of running an
unexpected build.

```kdl,name=The same node with its digest pinned
wasm "enrich" module="pipelines/enrich.wasm" sha3_256="sha3-256:9f2c...c4"
```

`saci-service validate --config saci.kdl` reads the module, runs its self description, and checks
the graph around it:

```text,name=What validate prints for a graph it accepts
OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
```

## 2. Pass it configuration

A `config` child holds key-value pairs the processor reads for itself. Every value arrives as a
string, and the processor parses it: an FX rate is `"1.10"`, a threshold is `"250"`, a flag is
whatever spelling the processor documents.

```kdl,name=Configuration the processor parses itself
wasm "enrich" module="examples/polyglot/build/enrich-py.wasm" {
    config fx_eur="1.10" fx_gbp="1.30" fx_jpy="0.0068"
}
```

The processor asks for a key by name and gets back the string or nothing. A key it never asks
for is inert. Handling a missing key is the processor's job, so a processor that needs a value
states its default in its own documentation.

## 3. What it receives and returns

A processor declares the components it works on by name, and the node's inbound link must
deliver every one of them: the upstream source's `component`, or the upstream processor's own
declared list. Its downstream reads the same names, so a sink linked to it declares a
`component` the processor produces.

Columns are the other half of the agreement. The `schema_fields` a source or sink declares must
match the fields the processor declares for that component, name for name, type for type and
nullable for nullable, or the load fails naming the link.

## 4. Chain processors

Two processors linked in sequence run against the same in-memory batch: the host forwards Arrow
buffers from one to the next instead of going back through a byte format. Each `wasm` call still
encodes that batch to Arrow IPC on the way in and reads the result back on the way out.

```kdl,name=Two processors in sequence
link from="orders_in" to="enrich"
link from="enrich" to="settle"
link from="settle" to="orders_out"
```

The upstream must declare every component the downstream declares. A disagreement fails the
build with an error naming the link, so `validate` catches it before anything runs.

## 5. Keep state across batches

A processor returns an opaque checkpoint with its output, and the host hands that blob straight
back as the next call's prior. Nothing else survives: a value a processor leaves in a global is
gone by the following batch.

Where the blob lives depends on the run mode. Stream mode writes each node's prior into the
`store "redb"` file as items flow, so a restart resumes from it, and no extra key asks for that.
An `interval` or `one_shot` run carries the prior forward only with `batch_resume #true` in that
block. A node with a `window` block is the exception: its accumulator is threaded from pass to
pass in memory whatever the store says, and `batch_resume #true` is what also persists it.

```kdl,name=Persisting processor state in stream mode
run_mode kind="stream"

store "redb" {
    path "/var/lib/saci/state.redb"
}
```

[Run modes and persistence](@/service/config/run-modes.md) covers both keys and what a restart
resumes in each mode.

## 6. Watch it

Six `saci_processor_*` series carry a `processor` attribute holding the node's id, so one node's
throughput and latency are one selector away:

| Series | Type | What it reports |
|---|---|---|
| `saci_processor_rows_in_total` | counter | rows handed to the node |
| `saci_processor_rows_out_total` | counter | rows it returned, which also rates its outbound edge |
| `saci_processor_batch_duration_seconds` | histogram | how long one batch took |
| `saci_processor_systems_run_total` | counter | steps the processor ran |
| `saci_processor_retries_total` | counter | retries it reported |
| `saci_processor_metric` | histogram | a metric the processor named itself, labelled `metric` |

[The live dashboard](@/service/operate/dashboard.md) draws the same numbers per node and
prints what the processor says about itself: its name, its version, whether it is stateful,
and its schema fingerprint. Read those four fields to confirm the artifact running is the one
you built. The host applies a sandbox and a deadline around each call, both described in
[how the host runs a processor](@/library/processor-host.md).

## Every key

| Key | Type | Default | What it does |
|---|---|---|---|
| `id` | string | required | the node's leading argument, unique across the workflow |
| `name` | string | the id | display name the dashboard shows instead of the id |
| `module` | path | required | the `.wasm` component this node runs |
| `sha3_256` | string | none | expected SHA3-256 of the module bytes, with an optional `sha3-256:` prefix |
| `config` | block | empty | key-value strings the processor reads for itself |
| `window` | block | none | event-time geometry, covered in [Windowing](@/service/processors/windowing/_index.md) |

### config

| Key | Type | Default | What it does |
|---|---|---|---|
| any name | string | none | handed to the processor unchanged, for it to parse |

Every value is a string. An unknown key elsewhere in the node is a parse error, but a `config`
key is never unknown: the processor decides which ones it reads.

## When it refuses to start

| Message | What to change |
|---|---|
| `reading wasm module 'pipelines/enrich.wasm': The system cannot find the path specified.` | Build the component, or correct `module`. A relative path is read from the directory the service runs in. |
| `wasm module SHA3-256 mismatch: expected 9f2c..., got 41ab...` | The artifact is not the one the digest pins. Rebuild it, or update `sha3_256`. |
| `workflow 'orders': link 'enrich' -> 'settle': processor 'settle' declares component 'Order', which upstream processor 'enrich' does not; a processor-to-processor link must deliver every component the downstream processor declares` | Give the upstream processor that component, or link the downstream to a node that produces it. |
| `workflow 'orders': link 'enrich' -> 'orders_out': component 'Order' schema differs between processor 'enrich' and sink 'orders_out'` | Align the sink's `schema_fields` with the fields the processor declares for that component. |
| `schema fingerprint mismatch: the pipeline declares 4c1f8a20 but this node's persisted checkpoints were written with 90bb1e77. The deployed pipeline's component schemas changed. Either restore the previous pipeline or clear node.data_dir before starting with the new schema.` | Cluster mode only, where the runner opens `node.data_dir`. The stored state describes a different row shape: restore the previous artifact, or clear the node's data directory. |

## Next

- [Branching](@/service/processors/branching.md): one processor, several sinks by decision.
- [Build your own processor](@/service/processors/build/_index.md): the same node, from an
  artifact you compiled.
