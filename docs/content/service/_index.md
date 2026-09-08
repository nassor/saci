+++
title = "What the service does"
description = "One KDL file names the sources, formats, processors and sinks; saci-service runs them until you stop it."
template = "section.html"
sort_by = "weight"
aliases = ["/quickstart/"]
+++
One KDL file names the sources rows come from, the formats those rows are
encoded in, the processors that transform them, the sinks they go to, and the
links between all of them. `saci-service` reads that one file, loads
everything it names, and runs the workflow until you stop it. Every rejection
happens before the first row moves.
[The config file](@/service/config/_index.md) is the whole surface.

<dl class="page-facts">
<dt>In one line</dt>
<dd>A KDL config plus a <code>.wasm</code> component becomes a <strong>long-running process with health checks</strong></dd>
<dt>You need</dt>
<dd>the <code>saci-service</code> binary and a config file</dd>
<dt>Read this if</dt>
<dd>You have a pipeline that works and now need it to run unattended, behind a readiness probe, on a schedule</dd>
</dl>

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 200" role="img" aria-labelledby="svc-what-t svc-what-d">
        <title id="svc-what-t">One config file names a source, a format, a processor and a sink, and saci-service runs them</title>
        <desc id="svc-what-d">
            A file or a topic on the left feeds a source node. The source names a
            declared transformer, the byte format it decodes. The source links to a
            wasm processor node drawn on the WebAssembly boundary, and that processor
            links to a sink node. The sink writes a file or a table on the right. The
            source, the transformer, the processor and the sink all live inside one
            saci-service process, drawn as a control-plane frame around them.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="76" width="96" height="52" rx="8"/>
            <text class="t-lbl" x="12" y="98">file</text>
            <text class="t-sm" x="12" y="116">or topic</text>
            <path class="arw arw-data" d="M96 102 H128" marker-end="url(#svc-what-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="120" y="30" width="404" height="150" rx="8"/>
            <rect class="hd hd-ctl" x="120" y="30" width="404" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="120" y="44" width="404" height="8"/>
            <text class="t-lbl t-ctl" x="132" y="45">saci-service &middot; saci.kdl</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="132" y="76" width="106" height="52" rx="8"/>
            <text class="t-lbl" x="142" y="98">source</text>
            <text class="t-sm" x="142" y="116">orders_in</text>
            <path class="arw arw-data" d="M238 102 H254" marker-end="url(#svc-what-a)"/>
            <rect class="blk blk-bnd" x="258" y="76" width="106" height="52" rx="8"/>
            <text class="t-lbl t-bnd" x="268" y="98">wasm</text>
            <text class="t-sm" x="268" y="116">enrich</text>
            <path class="arw arw-data" d="M364 102 H380" marker-end="url(#svc-what-a)"/>
            <rect class="blk blk-data" x="384" y="76" width="106" height="52" rx="8"/>
            <text class="t-lbl" x="394" y="98">sink</text>
            <text class="t-sm" x="394" y="116">orders_out</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="132" y="144" width="238" height="26" rx="6"/>
            <text class="t-sm t-ctl" x="142" y="161">transformer &quot;csv_fmt&quot; format=&quot;csv&quot;</text>
            <text class="t-sm" x="384" y="161">two links wire the three nodes</text>
            <path class="arw arw-data" d="M490 102 H544" marker-end="url(#svc-what-a)"/>
            <rect class="blk blk-data" x="548" y="76" width="112" height="52" rx="8"/>
            <text class="t-lbl" x="560" y="98">file</text>
            <text class="t-sm" x="560" y="116">or table</text>
        </g>
        <defs>
            <marker id="svc-what-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
        <span class="k-control"><i></i> the config and the process</span>
    </div>
    <figcaption class="dgm-cap">
        Declaration order wires nothing. Only a <code>link</code> connects two
        nodes, which is why the same three nodes can be rearranged without
        touching them.
    </figcaption>
</div>

## The vocabulary

Nine words cover the whole config file.

- **workflow** is one graph of nodes, and one file may declare several:
  [Workflows and links](@/service/config/workflows.md).
- **source** reads rows into the workflow, from a file, an HTTP endpoint, a
  topic, a table, a socket or another workflow:
  [Sources and sinks](@/service/connectors/_index.md).
- **transformer** is a declared byte format that a source decodes with and a
  sink encodes with: [Formats](@/service/formats/_index.md).
- **processor** is a WebAssembly component that takes rows in and hands rows
  back: [Processors in a workflow](@/service/processors/_index.md).
- **plugin** is the same contract in a native shared library, for when the
  sandbox costs more than it is worth: [Plugins in a workflow](@/service/plugins/_index.md).
- **sink** writes the rows that reach it out of the process:
  [Sources and sinks](@/service/connectors/_index.md).
- **link** is one edge, `from` one node `to` another:
  [Workflows and links](@/service/config/workflows.md).
- **branch** is a named link a processor chooses per batch:
  [Branching](@/service/processors/branching.md).
- **window** is event-time geometry on a processor node, so it aggregates over
  time rather than per batch: [Windowing](@/service/processors/windowing/_index.md).

To link the engine into your own binary instead of running a config file,
[Embedding SACI](@/library/_index.md) is the same pipeline written as Rust
code.

## Where to go

Install the binary, then run a real workflow end to end. It takes about fifteen
minutes and needs no Docker.

## Next

- [Install saci-service](@/service/install.md) puts the binary on your PATH.
- [The config file](@/service/config/_index.md) is every top-level key, and the
  command that proves a file before you deploy it.
