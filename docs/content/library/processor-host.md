+++
title = "How the host runs a processor"
description = "What the host does around every run-batch call, for a component and for a plugin."
template = "page.html"
weight = 11
+++
# How the host runs a processor

A processor is a `Box<dyn PipelineRuntime>` like any other, but the two that
cross a boundary, a WebAssembly component and a native plugin, carry a host
around them. This page is what that host does on either side of one `run-batch`
call.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 190" role="img" aria-labelledby="ph-title ph-desc">
        <title id="ph-title">One run-batch call, from the runner to the processor and back</title>
        <desc id="ph-desc">
            The runner serializes the dataset to Arrow IPC and calls run-batch on a blocking
            thread, passing the prior checkpoint alongside the bytes. The processor sits behind
            the WebAssembly boundary and answers with Arrow IPC bytes, per-batch metrics and an
            optional checkpoint. The runner keeps that checkpoint and hands it back as the next
            call's prior, which is the only state that survives the call.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="52" width="170" height="64" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="52" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="64" width="170" height="8"/>
            <text class="t-lbl" x="12" y="67">runner</text>
            <text class="t-sm" x="12" y="88">blocking thread</text>
            <text class="t-sm" x="12" y="104">fresh Store per call</text>
        </g>
        <g class="anim anim-2">
            <text class="t-sm t-mid" x="310" y="52">Arrow IPC bytes + prior</text>
            <path class="arw arw-data" d="M170 62 H430" marker-end="url(#ph-d)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="436" y="40" width="224" height="88" rx="8"/>
            <rect class="hd hd-bnd" x="436" y="40" width="224" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="436" y="52" width="224" height="8"/>
            <text class="t-lbl" x="448" y="55">your processor</text>
            <text class="t-sm t-bnd" x="448" y="76">run-batch</text>
            <text class="t-sm" x="448" y="92">host-io: log, metric, get-config</text>
            <text class="t-sm" x="448" y="108">epoch deadline bounds the call</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M430 100 H170" marker-end="url(#ph-d)"/>
            <text class="t-sm t-mid" x="310" y="116">Arrow IPC bytes, metrics, checkpoint</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M28 116 V152 H140 V116" marker-end="url(#ph-b)"/>
            <text class="t-sm t-bnd t-mid" x="84" y="170">checkpoint &rarr; prior</text>
        </g>
        <defs>
            <marker id="ph-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="ph-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> the runner and its thread</span>
        <span class="k-data"><i></i> Arrow IPC in both directions</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary and the blob that crosses it twice</span>
    </div>
</div>

## A fresh Store per call

The host builds a new wasmtime `Store` for every `run-batch` call. Nothing a
processor keeps in a global or a struct field survives to the next call, only
what it puts in `checkpoint`. The host persists that blob verbatim and hands it
back as the next call's `prior`.

A wasmtime epoch deadline bounds the call. `PipelineRuntimeLoader` sets 100
ticks of 100 ms, so a component that will not return is interrupted after ten
seconds instead of wedging the runner.

The call runs on a blocking thread rather than the async worker, and only Arrow
IPC bytes cross onto that thread and back. `WasmPipelineRuntime` serializes the
dataset, calls the export, and reads the result back into the dataset the runner
holds.

## Compiling happens once

`WasmEngine` owns the wasmtime `Engine`, the epoch ticker and the compiled
programs. Compiling, linking and pre-instantiating a component is synchronous
and costs about 1.7 s of one fast core for the 4 MB smoketest, so
`WasmEngine::program` does it once per distinct set of bytes. Every later load of
the same module is a `memcmp` plus an `Arc` clone, about 0.1 ms.

The engine is shareable. `ServiceBuilder::with_wasm_engine` hands one engine to
several builders, and the ticker stops with the last clone rather than outliving
it.

## What host-io costs the host

`crates/saci-service/src/wasm/host_impl.rs` implements the three imports a
processor gets:

- `log` bridges to `tracing`, one macro per level, tagged with the pipeline name
  and the processor's `target`. Without the `tracing` feature it falls back to
  stderr.
- `metric` routes to the exporter the service layer owns, recording the
  `saci_processor_metric` histogram under the name the processor passed. Names are
  processor-chosen, so distinct names are capped at `MAX_PROCESSOR_METRIC_NAMES`,
  256, and anything past that is dropped after one warning.
- `get-config` is a lookup in the `wasm` node's `config` keys, cloned per call.
  Values are strings and the processor parses numerics itself.

Nothing else is granted: no filesystem, no network, no clock, no
environment. WASI imports are linked because transitive dependencies need them,
not as a capability grant, which is why the host builds its `WasiCtx` with no
`inherit_*` calls.

## What the result records

`WasmPipelineRuntime` reads all four fields of a `run-result`. The five
`run-metrics` numbers become the `saci_processor_batch_duration_seconds`,
`saci_processor_rows_in_total`, `saci_processor_rows_out_total`,
`saci_processor_systems_run_total` and `saci_processor_retries_total` series, which
with `saci_processor_metric` are the six `saci_processor_*` series on `/metrics`.

`routes` decides delivery. The host reads it after the systems run and delivers
the output only to the links whose `branch` names one of those values; absent, it
multicasts to every downstream link.

Both `run-error::retryable` and `run-error::permanent` map to
`SaciError::SystemExecution`, so the runner releases the claim and returns the
error either way. `schema-mismatch` is reserved for a future load-time check and must
never come out of `run-batch`. A trap surfaces as `permanent`, and the batch is
lost, so a structured error is strictly better. [The WIT
contract](@/service/processors/build/wit-contract.md) has the records these fields
belong to.

## A plugin, loaded instead

`load_plugin_runtime` reads the library file and checks its `sha3_256` digest when the
node pins one, then hands the path to `NativePluginRuntime::open`, which fixes the rest of
the order: check `saci_abi_version` against the host's own, call `describe`, decode every
component schema, then recompute the schema fingerprint from what was decoded. A
fingerprint that disagrees with the manifest fails the load, because it means the
plugin's embedded schema constants have drifted from what it declares. Both steps run
before the service takes the runtime, so a running service never holds a plugin whose
schemas it has not verified.

Per batch, the host maps `SACI_STATUS_RETRYABLE` and `SACI_STATUS_PERMANENT` onto
the same `SaciError::SystemExecution` path a component's error takes. The host
catches a panic crossing the boundary and reports it as permanent.

There is no sandbox and no epoch deadline. A plugin runs in-process with full
host privileges, it cannot be interrupted, and a memory error in it is a memory
error in the host. The ABI's `metric` callback writes no series of its own, and
neither does the host on a plugin's behalf. A plugin gets the five
`saci_processor_*` series its per-batch metrics carry, and
`saci_processor_metric` stays empty, because only a component's
`host-io::metric` writes it.

[Plugins in a workflow](@/service/plugins/_index.md) is the node that names one,
and [the wire format](@/library/reference/wire-format.md) specifies the bytes
`run-batch` receives and returns.
