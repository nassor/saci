+++
title = "Embedding SACI"
description = "Link the engine into your own Rust binary: your main, your loop, no wasmtime and no service."
template = "section.html"
sort_by = "weight"
aliases = ["/native/"]
+++
Your crate depends on `saci-core` or `saci-service` as a library, you own `main`,
and you call `pipeline.run().await`. No `.wasm` file is involved, and neither
wasmtime nor the `saci-service` binary runs. The other way to use SACI is
[the service](@/service/_index.md): one binary that reads a KDL config and runs
the workflow it declares.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 120" role="img" aria-labelledby="nat-title nat-desc">
        <title id="nat-title">A native pipeline runs entirely inside your own binary</title>
        <desc id="nat-desc">
            Your binary links the engine, builds a Pipeline that owns its stage plan and its
            per-system retry, and writes rows out to stdout, Parquet or CSV. Nothing crosses
            a component boundary and no separate host process is involved.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="176" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="176" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="176" height="8"/>
            <text class="t-lbl" x="12" y="49">your binary</text>
            <text class="t-sm" x="12" y="70">cargo run</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M176 60 H222" marker-end="url(#nat-d)"/>
            <rect class="blk blk-data" x="228" y="34" width="204" height="52" rx="8"/>
            <text class="t-lbl" x="240" y="56">Pipeline</text>
            <text class="t-sm" x="240" y="74">stages + retry</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M432 60 H478" marker-end="url(#nat-d)"/>
            <rect class="blk" x="484" y="34" width="176" height="52" rx="8"/>
            <text class="t-lbl" x="496" y="56">rows out</text>
            <text class="t-sm" x="496" y="74">stdout, Parquet, CSV</text>
        </g>
        <defs>
            <marker id="nat-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane, all of it in one process</span>
    </div>
    <figcaption class="dgm-cap">
        One process, one address space. The <code>Pipeline</code> derives its own stage plan
        from the field declarations and retries what fails, exactly as it does inside a
        WebAssembly processor.
    </figcaption>
</div>

## Which mode do you want

Reach for native when:

- The transform ships with the binary and they version together.
- You want a debugger and a profiler on the same process as the pipeline.
- You need Rust types and `Resource` singletons that never cross an IPC
  boundary.

Reach for a [WebAssembly processor](@/service/processors/build/_index.md) when:

- You want to change the pipeline without rebuilding the host.
- You need the sandbox: no filesystem, no network, no clock unless the host
  grants it.
- The pipeline is not Rust.

Reach for a [native plugin](@/service/plugins/rust.md) when the transform must load
at runtime like a processor, and the sandbox is what stands in the way. It is
a shared library the service opens with `dlopen`: native threads and native
extensions, and none of the isolation.

## The idea in one diagram

You never write a stage list. Each system declares the fields it touches, and SACI
derives what can run together.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 300" role="img" aria-labelledby="idea-title idea-desc">
        <title id="idea-title">From field declarations to a parallel execution plan</title>
        <desc id="idea-desc">
            Three systems declare reads and writes over the five fields of a Transaction
            component. Validate writes the valid field and Enrich writes usd_amount. Those
            writes are disjoint, so the two share one parallel stage. Report reads both
            fields, so it waits for a second stage.
        </desc>
        <g class="anim anim-1">
            <text class="t-title" x="0" y="14">Transaction</text>
            <text class="t-sm" x="0" y="30">one RecordBatch, five columns</text>
            <rect class="blk blk-data" x="0" y="42" width="150" height="150" rx="8"/>
            <rect class="hd hd-data" x="0" y="42" width="150" height="22" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="150" height="8"/>
            <text class="t-sm t-data" x="12" y="57">FIELD</text>
            <rect class="row-x" x="8" y="70" width="134" height="20" rx="3"/>
            <text class="t-lbl" x="16" y="84">id</text>
            <rect class="row-r" x="8" y="94" width="134" height="20" rx="3"/>
            <text class="t-lbl" x="16" y="108">amount</text>
            <rect class="row-r" x="8" y="118" width="134" height="20" rx="3"/>
            <text class="t-lbl" x="16" y="132">currency</text>
            <rect class="row-w" x="8" y="142" width="134" height="20" rx="3"/>
            <text class="t-lbl" x="16" y="156">valid</text>
            <rect class="row-w" x="8" y="166" width="134" height="20" rx="3"/>
            <text class="t-lbl" x="16" y="180">usd_amount</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-ctl" d="M150 104 H206" marker-end="url(#i-c)"/>
            <path class="arw arw-data" d="M150 152 H206" marker-end="url(#i-d)"/>
            <path class="arw arw-data" d="M150 176 H206" marker-end="url(#i-d)"/>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl" x="212" y="30">STAGE 1: nothing orders these two</text>
            <rect class="blk blk-ctl" x="212" y="42" width="178" height="62" rx="8"/>
            <text class="t-lbl" x="226" y="64">ValidateSystem</text>
            <text class="t-sm t-ctl" x="226" y="80">reads  amount</text>
            <text class="t-sm t-data" x="226" y="94">writes valid</text>
            <rect class="blk blk-ctl" x="212" y="116" width="178" height="76" rx="8"/>
            <text class="t-lbl" x="226" y="138">EnrichSystem</text>
            <text class="t-sm t-ctl" x="226" y="154">reads  amount, currency</text>
            <text class="t-sm t-data" x="226" y="168">writes usd_amount</text>
            <text class="t-sm" x="226" y="184">no shared write &rarr; no conflict</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-ctl" d="M390 73 H420 V116 H452" marker-end="url(#i-c)"/>
            <path class="arw arw-ctl" d="M390 154 H420 V132 H452" marker-end="url(#i-c)"/>
            <text class="t-sm t-ctl" x="458" y="100">STAGE 2</text>
            <rect class="blk blk-ctl" x="458" y="108" width="196" height="62" rx="8"/>
            <text class="t-lbl" x="472" y="130">ReportSystem</text>
            <text class="t-sm t-ctl" x="472" y="146">reads  valid, usd_amount</text>
            <text class="t-sm" x="472" y="160">waits: both are its inputs</text>
        </g>
        <g class="anim anim-4">
            <rect class="row-r" x="0" y="230" width="14" height="14" rx="3"/>
            <text class="t-sm" x="22" y="241">a system reads this column</text>
            <rect class="row-w" x="216" y="230" width="14" height="14" rx="3"/>
            <text class="t-sm" x="238" y="241">a system writes this column</text>
            <rect class="row-x" x="446" y="230" width="14" height="14" rx="3"/>
            <text class="t-sm" x="468" y="241">untouched</text>
            <path class="ln" d="M0 262 H654"/>
            <text class="t-sm" x="0" y="282">You wrote three <tspan class="t-ctl">meta()</tspan> methods. SACI wrote the plan.</text>
        </g>
        <defs>
            <marker id="i-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="i-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <figcaption class="dgm-cap">
        <b>Validate</b> and <b>Enrich</b> write different columns, so SACI puts them in one
        stage. <b>Report</b> reads what they wrote, so it lands in the next stage. Declare
        the first two as <code>ParallelSystem</code> and a batch big enough to pay for the
        fan-out runs them concurrently; leave them as plain <code>System</code> and it runs
        them in sequence. Either way the order cannot matter. Add a fourth system and the
        plan re-derives itself.
    </figcaption>
</div>

## The concepts, in order

Each one builds on the previous, so nothing is forward-referenced. All of them
apply to a processor too, because a processor runs the same `Pipeline` DAG inside
the component.

1. [Dataset & Components](@/library/dataset.md): the columnar container. One Arrow
   `RecordBatch` per registered `Component`, all sharing a row count.
2. [Systems](@/library/systems.md): one transform, plus the `meta()` that names which
   columns it reads and writes.
3. [Pipeline](@/library/pipeline.md): a Dataset and its Systems. Turns declarations into
   stages, and retries what fails.
4. [Scheduler](@/library/scheduler.md): several independent Pipelines in one process, with
   dependency edges between them.
5. [Sources & Sinks](@/library/io.md): the two traits that move Arrow rows in and out of a
   Dataset, and the cast helpers between the file's schema and the component's.
6. [Windowed aggregation](@/library/windowing.md): tumbling, sliding and session windows,
   watermarks, and per-key aggregates published as a resource.
7. [Distributed processing](@/library/distributed.md): the same Pipeline against claimed row
   ranges across nodes, with claims and checkpoints replicated by raft.

## Which crates

`saci-core` is the engine. `saci-service` adds the runners, the factory registry
and the wasm and plugin hosts, and re-exports `saci-core`, so
`saci_service::pipeline::Pipeline` and `saci_core::pipeline::Pipeline` are the same
type.

```toml,name=Two dependency shapes
# The engine alone: no wasmtime, no HTTP.
saci-core = { git = "https://github.com/nassor/saci", features = ["io"] }

# The engine plus the runners, the registry and the hosts.
saci-service = { git = "https://github.com/nassor/saci" }
```

Add `saci-connector-file` plus a `saci-transformer-*` crate for each format you
read or write. [Crates and features](@/library/reference/crates.md) lists every
crate and every feature flag.

<div class="note">
<span class="note-label">Depending on SACI</span>

The crates are **not published to crates.io**. Inside a clone of the repository,
use a path dependency; outside one, point cargo at the repository as above.

</div>
