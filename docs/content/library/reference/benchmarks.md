+++
title = "Benchmarks"
description = "Where columnar processing pays off and where it does not: TPC-H Q6 and Q1, batch versus stream item sizing, stage cost, slice parallelism, Arrow IPC versus postcard, and a DataFusion comparison."
template = "page.html"
weight = 4
aliases = ["/benchmarks/"]
+++

# Benchmarks

Every performance claim on this site is traceable to a run you can reproduce.
These are those runs, reported whole, including the ones where SACI loses.

The short version: **SACI wins when the schema is wide, when the work is a
filter-and-reduce over columns, and when checkpoints are being encoded or
decoded in bulk. It loses on group-by aggregation, it loses at one row per
checkpoint, and stream mode always costs throughput.** Q6 beats a hand-written
scalar loop at *both* schema widths, and beats DataFusion on the same query. Q1
is 2.18× slower than a scalar loop, which is what its shape implies.

## How these were produced

The repository has a harness. It fixes the compiler flags, compiles as a
separate step, extracts the exact benchmark binary from cargo's output and runs
it directly. That is what makes these numbers comparable run to run:

```bash,name=The harness commands behind these numbers
# Columnar pipeline benchmarks
cargo xtask bench tpch_q6
cargo xtask bench tpch_q1
cargo xtask bench parallelism_compute
cargo xtask bench ipc_checkpoint

# Batch versus stream, and the stage cost curve
cargo xtask bench batch_vs_stream

# SQL comparison
cargo xtask bench vs_datafusion_q6

# Service-level per-item latency, native and WASM
cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
  cargo run --release -p saci-service --features service,wasm --example stream_latency
```

The `cargo xtask bench` commands above run the same on Linux, macOS and
Windows. On Windows the `stream_latency` command sets its flags through the
environment instead:

Windows (PowerShell):

```powershell
cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
$env:RUSTFLAGS = "-C target-cpu=native -C opt-level=3 -C codegen-units=1"
cargo run --release -p saci-service --features service,wasm --example stream_latency
```

The harness knows each benchmark's package and features, so do not pass your
own. `batch_vs_stream` needs `saci-core`'s `io` feature and `vs_datafusion_q6` needs none,
while `tpch_q6`, `tpch_q1`, `parallelism_compute` and `ipc_checkpoint` are built
with no extra features. Cargo's metadata hash encodes the feature set, so a
different `--features` list produces a *different binary*.

Recorded on **2026-08-22**, on an **AMD Ryzen 9 9950X3D** (16 cores, 32 logical
CPUs, two L3 domains covering logical CPUs 0 to 15 and 16 to 31, DDR5) running
**Windows 11**, Rust 1.98.0. Built with `RUSTFLAGS="-C target-cpu=native -C
opt-level=3 -C codegen-units=1"` and `[profile.bench] lto = "thin"`.

Criterion sample size 10 (20 for `item_size`), 1 000 000 rows and `seed=42` unless stated
otherwise. Every figure is criterion's reported point estimate, the middle value
of the three it prints. **Published figures are taken unpinned**, because
unpinned is what a deployment gets; the harness's `--affinity` flag is for A/B
work, not for this page.

**These numbers assume mimalloc**, which the `saci-service` binary and every
benchmark binary install as the global allocator. The library never installs an
allocator, so an embedder on Windows should expect worse pipeline figures
without it. Switching allocator is worth 2.3 to 2.6× on this suite. Do not reach
for `MIMALLOC_PURGE_DELAY=0` to bound RSS, which costs 3.85× on IPC encode.

`FilterStage`, `ComputeStage`, `AggregateStage`, `RevenueStage` and `TaxStage`
are systems defined inside the benchmark files themselves, not library types.

## Batch versus stream: what item size costs

The two processing modes differ in exactly one way: batch mode makes one
pipeline invocation over N rows, stream mode makes N/k invocations over k rows
each. The systems, the DAG and the stage plan are identical. This benchmark
holds the total at 100 000 rows of Q6-shaped work (a filter-and-compute
`ParallelSystem` plus a sequential accumulator) and sweeps k through
`Pipeline::run_stream`, so item size is the only variable. Source:
`crates/saci-core/benches/batch_vs_stream.rs`.

<!-- fig:item-size -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 376" role="img" aria-labelledby="bs-t bs-d">
        <title id="bs-t">Total wall time and per-invocation cost for 100 000 rows, swept by item size</title>
        <desc id="bs-d">
            Both panels are logarithmic, one gridline per ten-fold step. Top, total wall time for
            the same 100 000 rows: one batch of 100 000 takes 266.3 µs, ten of 10 000 take 274.2
            µs (1.03×), a hundred of 1 000 take 356.3 µs (1.34×), a thousand of 100 take 1.139 ms
            (4.3×), ten thousand of 10 take 8.736 ms (32.8×) and 100 000 single-row items take
            86.44 ms, 324.6× the single batch. Bottom, the same runs divided by their invocation
            count: 266.3 µs, 27.42 µs, 3.563 µs, 1.139 µs, 874 ns and 864 ns. The bottom three
            bars are nearly the same length. That plateau is the fixed cost of one invocation.
        </desc>
        <text class="t-ax t-end" x="78" y="11">ITEM SIZE k</text>
        <text class="t-ax t-end" x="150" y="11">CALLS</text>
        <text class="t-ax" x="164" y="11">TOTAL WALL TIME · LOG SCALE</text>
        <text class="t-ax t-end" x="660" y="11">VS ONE BATCH</text>
        <path class="grid" d="M164 18 V162"/>
        <text class="t-ax t-mid" x="164" y="176">100 µs</text>
        <path class="grid" d="M292 18 V162"/>
        <text class="t-ax t-mid" x="292" y="176">1 ms</text>
        <path class="grid" d="M420 18 V162"/>
        <text class="t-ax t-mid" x="420" y="176">10 ms</text>
        <path class="grid" d="M548 18 V162"/>
        <text class="t-ax t-mid" x="548" y="176">100 ms</text>
        <text class="t-lbl t-end" x="78" y="35">100 000</text>
        <text class="t-sm t-end" x="150" y="35">1</text>
        <rect class="bar bar-data" x="164" y="24" width="54.4" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="35">266.3 µs</text>
        <text class="t-sm t-end" x="660" y="35">1.0×</text>
        <text class="t-lbl t-end" x="78" y="59">10 000</text>
        <text class="t-sm t-end" x="150" y="59">10</text>
        <rect class="bar bar-data" x="164" y="48" width="56.1" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="59">274.2 µs</text>
        <text class="t-sm t-end" x="660" y="59">1.03×</text>
        <text class="t-lbl t-end" x="78" y="83">1 000</text>
        <text class="t-sm t-end" x="150" y="83">100</text>
        <rect class="bar bar-data" x="164" y="72" width="70.6" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="83">356.3 µs</text>
        <text class="t-sm t-end" x="660" y="83">1.34×</text>
        <text class="t-lbl t-end" x="78" y="107">100</text>
        <text class="t-sm t-end" x="150" y="107">1 000</text>
        <rect class="bar bar-data" x="164" y="96" width="135.2" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="107">1.139 ms</text>
        <text class="t-sm t-end" x="660" y="107">4.3×</text>
        <text class="t-lbl t-end" x="78" y="131">10</text>
        <text class="t-sm t-end" x="150" y="131">10 000</text>
        <rect class="bar bar-data" x="164" y="120" width="248.5" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="131">8.736 ms</text>
        <text class="t-sm t-end" x="660" y="131">32.8×</text>
        <text class="t-lbl t-end" x="78" y="155">1</text>
        <text class="t-sm t-end" x="150" y="155">100 000</text>
        <rect class="bar bar-data" x="164" y="144" width="375.9" height="14" rx="2"/>
        <text class="t-num t-data t-end" x="604" y="155">86.44 ms</text>
        <text class="t-sm t-end" x="660" y="155">324.6×</text>
        <text class="t-ax t-end" x="78" y="203">ITEM SIZE k</text>
        <text class="t-ax t-end" x="150" y="203">CALLS</text>
        <text class="t-ax" x="164" y="203">COST PER INVOCATION · LOG SCALE</text>
        <path class="grid" d="M202.5 210 V354"/>
        <text class="t-ax t-mid" x="202.5" y="368">1 µs</text>
        <path class="grid" d="M330.5 210 V354"/>
        <text class="t-ax t-mid" x="330.5" y="368">10 µs</text>
        <path class="grid" d="M458.5 210 V354"/>
        <text class="t-ax t-mid" x="458.5" y="368">100 µs</text>
        <text class="t-lbl t-end" x="78" y="227">100 000</text>
        <text class="t-sm t-end" x="150" y="227">1</text>
        <rect class="bar bar-data-2" x="164" y="216" width="349" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="227">266.3 µs</text>
        <text class="t-lbl t-end" x="78" y="251">10 000</text>
        <text class="t-sm t-end" x="150" y="251">10</text>
        <rect class="bar bar-data-2" x="164" y="240" width="222.6" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="251">27.42 µs</text>
        <text class="t-lbl t-end" x="78" y="275">1 000</text>
        <text class="t-sm t-end" x="150" y="275">100</text>
        <rect class="bar bar-data-2" x="164" y="264" width="109.2" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="275">3.563 µs</text>
        <text class="t-lbl t-end" x="78" y="299">100</text>
        <text class="t-sm t-end" x="150" y="299">1 000</text>
        <rect class="bar bar-data-2" x="164" y="288" width="45.8" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="299">1.139 µs</text>
        <text class="t-lbl t-end" x="78" y="323">10</text>
        <text class="t-sm t-end" x="150" y="323">10 000</text>
        <rect class="bar bar-data-2" x="164" y="312" width="31" height="14" rx="2"/>
        <text class="t-num t-end" x="604" y="323">874 ns</text>
        <text class="t-lbl t-end" x="78" y="347">1</text>
        <text class="t-sm t-end" x="150" y="347">100 000</text>
        <rect class="bar bar-data-2" x="164" y="336" width="30.4" height="14" rx="2"/>
        <text class="t-num t-data t-end" x="604" y="347">864 ns</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> total wall time for the whole 100 000 rows</span>
        <span class="k-data-2"><i></i> the same run, divided by its invocation count</span>
    </div>
    <figcaption class="dgm-cap">
        Both panels plot the same six runs. The upper one is what the wall clock says; the
        lower one divides it by the invocation count, which is where the fixed cost of an
        invocation stops hiding. The bottom three bars are the same length because below about
        a hundred rows an invocation costs what it costs whatever it carries.
    </figcaption>
</div>
<!-- /fig:item-size -->

Read the lower panel. From k=1 to k=10 it is flat at about 0.87 µs, and still
only 1.14 µs at k=100. That is the fixed cost of one invocation: clear the
dataset, append, walk the stages, apply the write set, update stats. Below about
a hundred rows it is *all* you are paying, because the row work vanishes into
it.

The tradeoff is explicit: **processing 100 000 rows one at a time costs 325× the
wall time of processing them in a single batch.** A pipeline handed 100 000 rows
should never run in stream mode; a pipeline that must answer one item
immediately has nothing to amortise anyway.

The floor is measurable on its own. An empty-DAG pipeline, one system that does
nothing, over 10 000 single-row items runs in 2.468 ms: **247 ns per item** of
framework overhead. Of the 864 ns a real item costs, roughly a quarter is the
runner and the rest is two systems doing Arrow array construction and write-set
application on a single row. Arrow's per-array fixed costs, not the runner, are
what make a one-row item cost anything.

For scale, a scalar row loop over the same 100 000 rows takes 153.5 µs.
`Pipeline::run`, the batch entry point with no IO, takes 269.6 µs, within 1.2% of
the equivalent single-invocation `run_stream` measurement of 266.3 µs.

### Service-level latency

The library numbers above exclude the service runner, the source and the sink.
The `stream_latency` example measures the whole path a real deployment takes,
timed from the producer: send one single-row batch, wait for the transformed row
to arrive at the sink.

<!-- fig:latency -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 236" role="img" aria-labelledby="lat-t lat-d">
        <title id="lat-t">Per-item round trip latency, native path against a WebAssembly processor</title>
        <desc id="lat-d">
            Logarithmic, one gridline per ten-fold step. Native source to systems to sink, over 10
            000 items: mean 1.0 µs, p50 1 µs, p99 2 µs, max 6 µs. The same single-row round trip
            through a WebAssembly processor calling run_on_with_state, over 1 000 items: mean
            179.4 µs, p50 159 µs, p99 420 µs, max 678 µs. Every WASM bar sits roughly two
            gridlines, two orders of magnitude, to the right of its native counterpart.
        </desc>
        <text class="t-ax" x="0" y="11">ROUND TRIP, PRODUCER TO SINK · LOG SCALE</text>
        <path class="grid" d="M149.2 20 V214"/>
        <text class="t-ax t-mid" x="149.2" y="228">1 µs</text>
        <path class="grid" d="M286.1 20 V214"/>
        <text class="t-ax t-mid" x="286.1" y="228">10 µs</text>
        <path class="grid" d="M423.1 20 V214"/>
        <text class="t-ax t-mid" x="423.1" y="228">100 µs</text>
        <path class="grid" d="M560 20 V214"/>
        <text class="t-ax t-mid" x="560" y="228">1 ms</text>
        <text class="t-lbl" x="0" y="34">native · source → systems → sink · n = 10 000</text>
        <text class="t-sm t-end" x="96" y="51">mean</text>
        <rect class="bar bar-data" x="108" y="42" width="41.2" height="9" rx="2"/>
        <text class="t-num" x="155.2" y="51">1.0 µs</text>
        <text class="t-sm t-end" x="96" y="66">p50</text>
        <rect class="bar bar-data" x="108" y="57" width="41.2" height="9" rx="2"/>
        <text class="t-num" x="155.2" y="66">1 µs</text>
        <text class="t-sm t-end" x="96" y="81">p99</text>
        <rect class="bar bar-data" x="108" y="72" width="82.4" height="9" rx="2"/>
        <text class="t-num t-data" x="196.4" y="81">2 µs</text>
        <text class="t-sm t-end" x="96" y="96">max</text>
        <rect class="bar bar-data" x="108" y="87" width="147.8" height="9" rx="2"/>
        <text class="t-num" x="261.8" y="96">6 µs</text>
        <text class="t-lbl" x="0" y="142">WASM processor · run_on_with_state · n = 1 000</text>
        <text class="t-sm t-end" x="96" y="159">mean</text>
        <rect class="bar bar-bnd" x="108" y="150" width="349.8" height="9" rx="2"/>
        <text class="t-num" x="463.8" y="159">179.4 µs</text>
        <text class="t-sm t-end" x="96" y="174">p50</text>
        <rect class="bar bar-bnd" x="108" y="165" width="342.6" height="9" rx="2"/>
        <text class="t-num" x="456.6" y="174">159 µs</text>
        <text class="t-sm t-end" x="96" y="189">p99</text>
        <rect class="bar bar-bnd" x="108" y="180" width="400.4" height="9" rx="2"/>
        <text class="t-num t-bnd" x="514.4" y="189">420 µs</text>
        <text class="t-sm t-end" x="96" y="204">max</text>
        <rect class="bar bar-bnd" x="108" y="195" width="428.9" height="9" rx="2"/>
        <text class="t-num" x="542.9" y="204">678 µs</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> native, in-process</span>
        <span class="k-boundary"><i></i> across the WebAssembly boundary</span>
    </div>
    <figcaption class="dgm-cap">
        Timed from the producer: send one single-row batch, wait for the transformed row to
        arrive at the sink. Sample counts differ: 10 000 native, 1 000 through the processor.
        The WASM tail is the thinly sampled half of the chart.
    </figcaption>
</div>
<!-- /fig:latency -->

Native stream mode is a **2 µs p99** round trip end to end, at microsecond
timer granularity. The WASM boundary costs two orders of magnitude more: a fresh
wasmtime instance per call, plus Arrow IPC in and out. Linking and instantiation
planning are hoisted to load time via `InstancePre`, so what remains is
instantiation, instance teardown and the IPC round trip. The IPC section
below shows that half is substantial at one row.

The `Store` lifecycle is one number, not two. `wasm_roundtrip`'s
`wasm_store_lifecycle/instantiate_describe_drop` times `Store` construction, its
`WasiCtx`, instantiation, the guest `describe` call and the drop together,
because the bindings are private and the drop cannot be separated through the
public surface. What the instance costs is its linear memory and tables, which
the pooling allocator recycles instead of returning them to the OS.

Those figures are the engine, not the deployment. `stream_latency` installs no
`tracing` subscriber, so every span on the path costs a no-op dispatch. The
`saci-service` binary installs one, and at this scale that subscriber is the larger
share of per-item cost. Measured through the service's own subscriber, per-item
stream latency is about 4.6 µs at `log_level="info"`, about 7.4 µs at
`log_level="debug"`, and about 1.9 µs compiled without the `tracing` feature. The
step from `info` to `debug` is the five per-item runner spans becoming real. That
is a different measurement path from the round trip charted above, so read it as
the service's own overhead rather than a number to subtract from the 2 µs p99.

At n=1000 the WASM tail is thinly sampled, so treat the 420 µs p99 as
indicative, not settled.

This example is a plain binary rather than a statistical harness, so run-to-run
spread is worth stating. A second build without the flags above, plain `cargo
run --release`, measured native mean 1.1 µs at the same 1 µs p50, and WASM mean
171.4 µs with a 409 µs p99. Native p50 and mean are stable; everything in the
tail moves by more than the compiler flags do. The native `max` is a warm-up
artefact: 205 µs on that run against 6 µs here, on identical code.

## Stage cost: what the dispatch threshold selects

A stage holding several non-conflicting `ParallelSystem`s can either run them
inline, one after another, or dispatch one `spawn_blocking` each. SACI gates that
choice on row count with `STAGE_INLINE_THRESHOLD`, which is 100 000. This
benchmark sweeps a two-system stage across row counts. Neither system implements
`run_slice`, so slice parallelism never applies and stage dispatch is isolated.
Source: `crates/saci-core/benches/batch_vs_stream.rs`, the `stage_dispatch`
group.

<!-- fig:stage-cost -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 283" role="img" aria-labelledby="sc-t sc-d">
        <title id="sc-t">Two-system stage cost by row count, either side of the inline dispatch threshold</title>
        <desc id="sc-d">
            Left, total time on a logarithmic scale; right, the same runs divided by row count, on
            a linear scale from zero to 17 nanoseconds. 256 rows inline, 4.178 µs, 16.3 ns per
            row. 512 inline, 4.470 µs, 8.7 ns. 1 024 inline, 5.441 µs, 5.3 ns. 4 096 inline, 11.02
            µs, 2.7 ns. 16 384 inline, 33.82 µs, 2.1 ns. 65 536 inline, 198.4 µs, 3.0 ns. 131 072
            dispatched, 403.8 µs, 3.1 ns. 262 144 dispatched, 813.7 µs, 3.1 ns. 1 048 576
            dispatched, 3.219 ms, 3.1 ns. The per-row bars collapse from 16.3 ns to about 3 ns and
            then stay there, straight through the inline-to-dispatched transition.
        </desc>
        <text class="t-ax t-end" x="78" y="13">ROWS</text>
        <text class="t-ax" x="92" y="13">TOTAL TIME · LOG SCALE</text>
        <text class="t-ax" x="372" y="13">PER ROW · LINEAR</text>
        <path class="grid" d="M126.8 38 V261"/>
        <text class="t-ax t-mid" x="126.8" y="275">10 µs</text>
        <path class="grid" d="M193.4 38 V261"/>
        <text class="t-ax t-mid" x="193.4" y="275">100 µs</text>
        <path class="grid" d="M259.9 38 V261"/>
        <text class="t-ax t-mid" x="259.9" y="275">1 ms</text>
        <path class="ax" d="M372 38 V261"/>
        <path class="grid" d="M372 38 V261"/>
        <text class="t-ax t-mid" x="372" y="275">0</text>
        <path class="grid" d="M438.5 38 V261"/>
        <text class="t-ax t-mid" x="438.5" y="275">5 ns</text>
        <path class="grid" d="M504.9 38 V261"/>
        <text class="t-ax t-mid" x="504.9" y="275">10 ns</text>
        <path class="grid" d="M571.4 38 V261"/>
        <text class="t-ax t-mid" x="571.4" y="275">15 ns</text>
        <text class="t-lbl t-end" x="78" y="56">256</text>
        <rect class="bar bar-data" x="92" y="46" width="9.6" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="56">4.178 µs</text>
        <rect class="bar bar-data" x="372" y="46" width="216.7" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="56">16.3 ns</text>
        <text class="t-lbl t-end" x="78" y="78">512</text>
        <rect class="bar bar-data" x="92" y="68" width="11.5" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="78">4.470 µs</text>
        <rect class="bar bar-data" x="372" y="68" width="115.7" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="78">8.7 ns</text>
        <text class="t-lbl t-end" x="78" y="100">1 024</text>
        <rect class="bar bar-data" x="92" y="90" width="17.2" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="100">5.441 µs</text>
        <rect class="bar bar-data" x="372" y="90" width="70.5" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="100">5.3 ns</text>
        <text class="t-lbl t-end" x="78" y="122">4 096</text>
        <rect class="bar bar-data" x="92" y="112" width="37.6" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="122">11.02 µs</text>
        <rect class="bar bar-data" x="372" y="112" width="35.9" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="122">2.7 ns</text>
        <text class="t-lbl t-end" x="78" y="144">16 384</text>
        <rect class="bar bar-data" x="92" y="134" width="70" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="144">33.82 µs</text>
        <rect class="bar bar-data" x="372" y="134" width="27.9" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="144">2.1 ns</text>
        <text class="t-lbl t-end" x="78" y="166">65 536</text>
        <rect class="bar bar-data" x="92" y="156" width="121.2" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="166">198.4 µs</text>
        <rect class="bar bar-data" x="372" y="156" width="39.9" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="166">3.0 ns</text>
        <text class="t-lbl t-end" x="78" y="210">131 072</text>
        <rect class="bar bar-ctl" x="92" y="200" width="141.7" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="210">403.8 µs</text>
        <rect class="bar bar-ctl" x="372" y="200" width="41.2" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="210">3.1 ns</text>
        <text class="t-lbl t-end" x="78" y="232">262 144</text>
        <rect class="bar bar-ctl" x="92" y="222" width="162" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="232">813.7 µs</text>
        <rect class="bar bar-ctl" x="372" y="222" width="41.2" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="232">3.1 ns</text>
        <text class="t-lbl t-end" x="78" y="254">1 048 576</text>
        <rect class="bar bar-ctl" x="92" y="244" width="201.7" height="13" rx="2"/>
        <text class="t-num t-end" x="358" y="254">3.219 ms</text>
        <rect class="bar bar-ctl" x="372" y="244" width="41.2" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="254">3.1 ns</text>
        <text class="t-ax t-ctl" x="92" y="192">STAGE_INLINE_THRESHOLD · 100 000 ROWS</text>
        <path class="mark" d="M336 188 H598"/>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> stage ran inline, one system after the other</span>
        <span class="k-control"><i></i> stage dispatched one spawn_blocking per system</span>
    </div>
    <figcaption class="dgm-cap">
        Each row runs whichever path the threshold selects at that size, so this is the cost
        curve a deployment gets, <b>not an A/B</b>. No size here is measured both ways, so the
        chart cannot locate the crossover. What it shows is that the transition leaves no step
        in the per-row curve.
    </figcaption>
</div>
<!-- /fig:stage-cost -->

The fixed cost of an invocation dominates below ~1 000 rows (16.3 ns/row at 256
rows against 2.1 ns/row at 16 384). Per-row cost is flat at about 3 ns from
65 536 rows upward, straight through the inline-to-dispatched transition.
Whatever dispatch costs at 131 072 rows, it is not visible as a step in this
curve.

## TPC-H Q1: aggregation

Aggregation over a 12-column lineitem batch with `GROUP BY (returnflag,
linestatus)`. Source: `crates/saci-core/benches/tpch_q1.rs`.

<!-- fig:q1 -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 249" role="img" aria-labelledby="q1-t q1-d">
        <title id="q1-t">TPC-H Q1: a scalar loop against the SACI pipeline, and where the pipeline time goes</title>
        <desc id="q1-d">
            Linear scale, zero to 3 milliseconds. The scalar baseline, one pass over a Vec of row
            structs, takes 1.287 ms. The SACI pipeline takes 2.806 ms, 2.18× slower. Decomposed:
            setup 212.7 µs, filter 35.5 µs, compute 303.9 µs, aggregate 1.474 ms, and roughly 0.78
            ms that no measured stage accounts for. The aggregate bar alone is longer than the
            entire scalar baseline.
        </desc>
        <text class="t-ax" x="0" y="11">GROUP BY (returnflag, linestatus) OVER 12 COLUMNS, 1M ROWS</text>
        <path class="ax" d="M200 18 V227"/>
        <path class="grid" d="M200 18 V227"/>
        <text class="t-ax t-mid" x="200" y="241">0</text>
        <path class="grid" d="M320 18 V227"/>
        <text class="t-ax t-mid" x="320" y="241">1 ms</text>
        <path class="grid" d="M440 18 V227"/>
        <text class="t-ax t-mid" x="440" y="241">2 ms</text>
        <path class="grid" d="M560 18 V227"/>
        <text class="t-ax t-mid" x="560" y="241">3 ms</text>
        <text class="t-lbl" x="0" y="36">scalar baseline</text>
        <text class="t-sm" x="0" y="53">one pass over a Vec of rows</text>
        <rect class="bar bar-ctl" x="200" y="24" width="154.4" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="36">1.287 ms</text>
        <text class="t-sm t-end" x="655" y="53">1.0×</text>
        <text class="t-lbl" x="0" y="70">SACI pipeline</text>
        <text class="t-sm" x="0" y="87">includes pipeline construction</text>
        <rect class="bar bar-data" x="200" y="58" width="336.7" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="70">2.806 ms</text>
        <text class="t-sm t-data t-end" x="655" y="87">2.18× slower</text>
        <text class="t-ax" x="0" y="112">WHERE THE 2.806 ms GOES</text>
        <text class="t-lbl" x="0" y="132.5">setup</text>
        <rect class="bar bar-data-3" x="200" y="122" width="25.5" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="132.5">212.7 µs</text>
        <text class="t-lbl" x="0" y="152.5">filter</text>
        <rect class="bar bar-data-2" x="200" y="142" width="4.3" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="152.5">35.5 µs</text>
        <text class="t-lbl" x="0" y="172.5">compute</text>
        <rect class="bar bar-data-2" x="200" y="162" width="36.5" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="172.5">303.9 µs</text>
        <text class="t-lbl" x="0" y="192.5">aggregate</text>
        <rect class="bar bar-data" x="200" y="182" width="176.9" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="192.5">1.474 ms</text>
        <text class="t-lbl" x="0" y="212.5">unattributed</text>
        <text class="t-sm" x="0" y="228">pipeline machinery</text>
        <rect class="bar bar" x="200" y="202" width="93.6" height="13" rx="2"/>
        <text class="t-num t-end" x="655" y="212.5">≈ 0.78 ms</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> hand-written scalar loop</span>
        <span class="k-data"><i></i> SACI stage</span>
        <span class="k-mute"><i></i> time no measured stage accounts for</span>
    </div>
    <figcaption class="dgm-cap">
        The four measured stages sum to 2.026 ms against a measured 2.806 ms, so the grey bar
        is pipeline machinery no stage accounts for. That is a much larger share than Q6 pays,
        on a benchmark that rebuilds its <code>Pipeline</code> every iteration.
    </figcaption>
</div>
<!-- /fig:q1 -->

Q1 is the wrong shape for columnar processing: the `GROUP BY` has six groups,
and the scalar row loop already fuses filter, compute and accumulate into one
pass. Both sides index a six-slot array arithmetically from
`(l_returnflag, l_linestatus)`, so neither pays for hashing.

Grouping is a per-row scatter into accumulator slots, and the scatter is
data-dependent, so the loop cannot vectorise. Expressed over columns it is three
passes over the seven `Lineitem` columns the query reads, plus the two derived
ones, where the scalar version fuses everything into one. That is the aggregate
bar, longer on its own than the entire scalar baseline.

## TPC-H Q6: filter and sum, narrow versus wide

Sum of revenue behind three compound predicates. Run twice: once on the
12-column schema, once on a 30-column schema where 18 columns are never read.
Source: `crates/saci-core/benches/tpch_q6.rs`.

<!-- fig:q6 -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 204" role="img" aria-labelledby="q6-t q6-d">
        <title id="q6-t">TPC-H Q6 on a 12-column and a 30-column schema, scalar loop against SACI</title>
        <desc id="q6-d">
            Linear scale, zero to 13 milliseconds. On the narrow 12-column schema the scalar loop
            takes 2.096 ms and SACI 910.2 µs, 2.30× faster. On the wide 30-column schema, where 18
            columns are never read, the scalar loop takes 12.41 ms, 5.92× its own narrow figure,
            while SACI takes 916.9 µs, 13.54× faster than the wide scalar loop and within 0.7% of
            its own narrow figure. The two SACI bars are the same length; the two scalar bars are
            not.
        </desc>
        <path class="ax" d="M210 12 V182"/>
        <path class="grid" d="M210 12 V182"/>
        <text class="t-ax t-mid" x="210" y="196">0</text>
        <path class="grid" d="M261.5 12 V182"/>
        <text class="t-ax t-mid" x="261.5" y="196">2 ms</text>
        <path class="grid" d="M313.1 12 V182"/>
        <text class="t-ax t-mid" x="313.1" y="196">4 ms</text>
        <path class="grid" d="M364.6 12 V182"/>
        <text class="t-ax t-mid" x="364.6" y="196">6 ms</text>
        <path class="grid" d="M416.2 12 V182"/>
        <text class="t-ax t-mid" x="416.2" y="196">8 ms</text>
        <path class="grid" d="M467.7 12 V182"/>
        <text class="t-ax t-mid" x="467.7" y="196">10 ms</text>
        <path class="grid" d="M519.2 12 V182"/>
        <text class="t-ax t-mid" x="519.2" y="196">12 ms</text>
        <text class="t-ax" x="0" y="14">NARROW · 12-COLUMN SCHEMA</text>
        <text class="t-lbl" x="0" y="34">scalar</text>
        <text class="t-sm" x="0" y="51">12 columns, all touched</text>
        <rect class="bar bar-ctl" x="210" y="22" width="54" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="34">2.096 ms</text>
        <text class="t-sm t-end" x="655" y="51">1.0×</text>
        <text class="t-lbl" x="0" y="68">SACI</text>
        <text class="t-sm" x="0" y="85">reads 4 of the 12 columns</text>
        <rect class="bar bar-data" x="210" y="56" width="23.5" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="68">910.2 µs</text>
        <text class="t-sm t-data t-end" x="655" y="85">2.30× faster</text>
        <text class="t-ax" x="0" y="104">WIDE · 30-COLUMN SCHEMA</text>
        <text class="t-lbl" x="0" y="124">scalar</text>
        <text class="t-sm" x="0" y="141">all 30 pulled per row</text>
        <rect class="bar bar-ctl" x="210" y="112" width="319.8" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="124">12.41 ms</text>
        <text class="t-sm t-ctl t-end" x="655" y="141">5.92× slower</text>
        <text class="t-lbl" x="0" y="158">SACI</text>
        <text class="t-sm" x="0" y="175">reads 4 columns of 30</text>
        <rect class="bar bar-data" x="210" y="146" width="23.6" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="158">916.9 µs</text>
        <text class="t-sm t-data t-end" x="655" y="175">13.54× faster</text>
        <path class="mark" d="M233.6 64 V108"/>
        <path class="mark" d="M233.6 132 V154"/>
        <text class="t-sm t-data" x="240.6" y="157">flat to 0.7%</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> hand-written scalar row loop</span>
        <span class="k-data"><i></i> SACI pipeline</span>
    </div>
    <figcaption class="dgm-cap">
        A row-oriented pass pulls every field into cache whether the query reads it or not, so
        the scalar bar grows with the schema. <b>The two SACI bars are the same length</b>
        because the pipeline reads the four columns the predicates name and never touches the
        other 26.
    </figcaption>
</div>
<!-- /fig:q6 -->

Q6 is a win on both halves. A
bit-packed single-pass filter and vectorised compute and aggregate stages carry
the narrow case to 2.30× faster than the scalar loop.

The scalar baseline degrades with column count: 2.096 ms → 12.41 ms for 2.5× the
columns, because a row-oriented pass pulls every field into cache whether it is
read or not. SACI goes 910.2 µs → 916.9 µs over the same change, **flat to
within 0.7%**, because it reads the four columns the predicates name and never
touches the other 26.

Schema width is a multiplier, not a crossover. SACI wins the narrow case on the
strength of the operators themselves, and the advantage grows with every column
added that the query does not read.

Where the 910.2 µs goes: the three stage bodies measure 165.6 µs (filter),
154.8 µs (compute) and 12.43 µs (aggregate), summing to 332.9 µs, which matches
`narrow_direct_systems_warm` at 340.9 µs. Add the 108 µs `narrow_saci_setup_only`
and a full pipeline should land near 449 µs, which is what
`narrow_direct_systems` measures. The same work through `Pipeline::run` does
not: `narrow_saci` is 910.2 µs. See the open items below.

## Slice parallelism

SHA3-256 over 128-byte blobs, CPU-bound. 1M rows, 128 MB of input, 32 logical
CPUs. Source: `crates/saci-core/benches/parallelism_compute.rs`.

<!-- fig:slices -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 142" role="img" aria-labelledby="sp-t sp-d">
        <title id="sp-t">Slice parallelism on a CPU-bound hash, against the same work on one thread</title>
        <desc id="sp-d">
            Linear scale, zero to 420 milliseconds. Sequential, a plain System on one thread:
            399.8 ms. The same work as a ParallelSystem with run_slice: 40.56 ms, a 9.86× speedup
            on 32 logical CPUs. With the slice threshold raised above the row count the executor
            falls back to the whole-dataset path and the time returns to 399.0 ms, confirming the
            gate.
        </desc>
        <text class="t-ax" x="0" y="11">SHA3-256 OVER 1M 128-BYTE BLOBS · 128 MB IN</text>
        <path class="ax" d="M190 18 V120"/>
        <path class="grid" d="M190 18 V120"/>
        <text class="t-ax t-mid" x="190" y="134">0</text>
        <path class="grid" d="M278.1 18 V120"/>
        <text class="t-ax t-mid" x="278.1" y="134">100 ms</text>
        <path class="grid" d="M366.2 18 V120"/>
        <text class="t-ax t-mid" x="366.2" y="134">200 ms</text>
        <path class="grid" d="M454.3 18 V120"/>
        <text class="t-ax t-mid" x="454.3" y="134">300 ms</text>
        <path class="grid" d="M542.4 18 V120"/>
        <text class="t-ax t-mid" x="542.4" y="134">400 ms</text>
        <text class="t-lbl" x="0" y="36">sequential</text>
        <text class="t-sm" x="0" y="53">plain System, one thread</text>
        <rect class="bar bar-ctl" x="190" y="24" width="352.2" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="36">399.8 ms</text>
        <text class="t-sm t-end" x="655" y="53">1.0×</text>
        <text class="t-lbl" x="0" y="70">slice-parallel</text>
        <text class="t-sm" x="0" y="87">ParallelSystem with run_slice</text>
        <rect class="bar bar-data" x="190" y="58" width="35.7" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="70">40.56 ms</text>
        <text class="t-sm t-data t-end" x="655" y="87">9.86× on 32 logical CPUs</text>
        <text class="t-lbl" x="0" y="104">threshold raised</text>
        <text class="t-sm" x="0" y="121">slices gated off</text>
        <rect class="bar bar-ctl-2" x="190" y="92" width="351.5" height="16" rx="2"/>
        <text class="t-num t-end" x="655" y="104">399.0 ms</text>
        <text class="t-sm t-end" x="655" y="121">≈ 1.0×</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> one thread</span>
        <span class="k-data"><i></i> fanned out across rayon</span>
        <span class="k-ctl-2"><i></i> fallback path, slices gated off</span>
    </div>
    <figcaption class="dgm-cap">
        Same system, same rows, same bytes: the only difference between the first two bars is
        whether <code>run_slice</code> exists. The third re-runs the parallel configuration
        with the slice threshold raised above the row count, which is why it lands back on the
        sequential bar.
    </figcaption>
</div>
<!-- /fig:slices -->

9.86× on 32 logical CPUs (16 physical) is 30.8% efficiency against logical
count, or 61.6% against physical. SHA3-256 has internal data dependencies and
SMT siblings do not double throughput, so linear was never available. This is
the most stable instrument in the suite: 0.4% standard deviation on the
sequential and single-thread rows, 1.6% on the parallel one.

Two things carry that scaling. The first is chunking at 4 chunks per CPU rather
than one, which gives rayon something to steal when SMT siblings and the two L3
domains retire chunks at different rates. The second is a read-mostly lock on the
merged-batch cache, since `run_slice` calls `batch_for` once per chunk and an
exclusive lock there serialises the entire fan-out.

The third bar is about correctness rather than speed. With the slice threshold
raised above the row count, the executor falls back to the whole-dataset path
and the time returns to sequential, so the threshold gate does what it claims.

## Arrow IPC versus postcard

Checkpoint encode and decode across 10 columns: three `i64`, three `f64`, two
strings with 100 distinct values, one `bool`, one `Option<f64>`. Source:
`crates/saci-core/benches/ipc_checkpoint.rs`. The 1-row and 1 000-row sizes
matter because stream mode round-trips one checkpoint *per item*.

<!-- fig:ipc -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 344" role="img" aria-labelledby="ipc-t ipc-d">
        <title id="ipc-t">Arrow IPC against postcard, encode and decode, one row to a million</title>
        <desc id="ipc-d">
            Both panels are logarithmic, one gridline per ten-fold step. Encode, 1 row: IPC 4.160
            µs, postcard 73.5 ns. 1 000 rows: IPC 5.770 µs, postcard 31.70 µs. 10 000: IPC 31.08
            µs, postcard 337.9 µs. 100 000: IPC 436.9 µs, postcard 3.807 ms. 1 000 000: IPC 6.961
            ms, postcard 40.73 ms. Decode, 1 row: IPC 4.801 µs, postcard 40.7 ns. 1 000: IPC 10.25
            µs, postcard 40.02 µs. 10 000: IPC 64.44 µs, postcard 457.9 µs. 100 000: IPC 865.1 µs,
            postcard 5.115 ms. 1 000 000: IPC 8.915 ms, postcard 54.21 ms. The IPC bars barely
            lengthen from 1 row to 1 000; the postcard bars lengthen with every row.
        </desc>
        <text class="t-ax t-end" x="74" y="10">ROWS</text>
        <text class="t-ax" x="86" y="10">ENCODE · LOG SCALE</text>
        <path class="grid" d="M131.3 14 V147"/>
        <text class="t-ax t-mid" x="131.3" y="161">100 ns</text>
        <path class="grid" d="M196.1 14 V147"/>
        <text class="t-ax t-mid" x="196.1" y="161">1 µs</text>
        <path class="grid" d="M260.9 14 V147"/>
        <text class="t-ax t-mid" x="260.9" y="161">10 µs</text>
        <path class="grid" d="M325.6 14 V147"/>
        <text class="t-ax t-mid" x="325.6" y="161">100 µs</text>
        <path class="grid" d="M390.4 14 V147"/>
        <text class="t-ax t-mid" x="390.4" y="161">1 ms</text>
        <path class="grid" d="M455.2 14 V147"/>
        <text class="t-ax t-mid" x="455.2" y="161">10 ms</text>
        <text class="t-lbl t-end" x="74" y="32">1 row</text>
        <rect class="bar bar-data" x="86" y="20" width="150.2" height="8" rx="2"/>
        <text class="t-num" x="242.2" y="27">4.160 µs</text>
        <rect class="bar bar-ctl" x="86" y="31" width="36.6" height="8" rx="2"/>
        <text class="t-num t-ctl" x="128.6" y="38">73.5 ns</text>
        <text class="t-lbl t-end" x="74" y="58">1 000</text>
        <rect class="bar bar-data" x="86" y="46" width="159.4" height="8" rx="2"/>
        <text class="t-num t-data" x="251.4" y="53">5.770 µs</text>
        <rect class="bar bar-ctl" x="86" y="57" width="207.3" height="8" rx="2"/>
        <text class="t-num" x="299.3" y="64">31.70 µs</text>
        <text class="t-lbl t-end" x="74" y="84">10 000</text>
        <rect class="bar bar-data" x="86" y="72" width="206.8" height="8" rx="2"/>
        <text class="t-num t-data" x="298.8" y="79">31.08 µs</text>
        <rect class="bar bar-ctl" x="86" y="83" width="273.9" height="8" rx="2"/>
        <text class="t-num" x="365.9" y="90">337.9 µs</text>
        <text class="t-lbl t-end" x="74" y="110">100 000</text>
        <rect class="bar bar-data" x="86" y="98" width="281.1" height="8" rx="2"/>
        <text class="t-num t-data" x="373.1" y="105">436.9 µs</text>
        <rect class="bar bar-ctl" x="86" y="109" width="342" height="8" rx="2"/>
        <text class="t-num" x="434" y="116">3.807 ms</text>
        <text class="t-lbl t-end" x="74" y="136">1 000 000</text>
        <rect class="bar bar-data" x="86" y="124" width="359" height="8" rx="2"/>
        <text class="t-num t-data" x="451" y="131">6.961 ms</text>
        <rect class="bar bar-ctl" x="86" y="135" width="408.7" height="8" rx="2"/>
        <text class="t-num" x="500.7" y="142">40.73 ms</text>
        <text class="t-ax t-end" x="74" y="185">ROWS</text>
        <text class="t-ax" x="86" y="185">DECODE · LOG SCALE</text>
        <path class="grid" d="M131.3 189 V322"/>
        <text class="t-ax t-mid" x="131.3" y="336">100 ns</text>
        <path class="grid" d="M196.1 189 V322"/>
        <text class="t-ax t-mid" x="196.1" y="336">1 µs</text>
        <path class="grid" d="M260.9 189 V322"/>
        <text class="t-ax t-mid" x="260.9" y="336">10 µs</text>
        <path class="grid" d="M325.6 189 V322"/>
        <text class="t-ax t-mid" x="325.6" y="336">100 µs</text>
        <path class="grid" d="M390.4 189 V322"/>
        <text class="t-ax t-mid" x="390.4" y="336">1 ms</text>
        <path class="grid" d="M455.2 189 V322"/>
        <text class="t-ax t-mid" x="455.2" y="336">10 ms</text>
        <text class="t-lbl t-end" x="74" y="207">1 row</text>
        <rect class="bar bar-data" x="86" y="195" width="154.2" height="8" rx="2"/>
        <text class="t-num" x="246.2" y="202">4.801 µs</text>
        <rect class="bar bar-ctl" x="86" y="206" width="20" height="8" rx="2"/>
        <text class="t-num t-ctl" x="112" y="213">40.7 ns</text>
        <text class="t-lbl t-end" x="74" y="233">1 000</text>
        <rect class="bar bar-data" x="86" y="221" width="175.6" height="8" rx="2"/>
        <text class="t-num t-data" x="267.6" y="228">10.25 µs</text>
        <rect class="bar bar-ctl" x="86" y="232" width="213.9" height="8" rx="2"/>
        <text class="t-num" x="305.9" y="239">40.02 µs</text>
        <text class="t-lbl t-end" x="74" y="259">10 000</text>
        <rect class="bar bar-data" x="86" y="247" width="227.3" height="8" rx="2"/>
        <text class="t-num t-data" x="319.3" y="254">64.44 µs</text>
        <rect class="bar bar-ctl" x="86" y="258" width="282.5" height="8" rx="2"/>
        <text class="t-num" x="374.5" y="265">457.9 µs</text>
        <text class="t-lbl t-end" x="74" y="285">100 000</text>
        <rect class="bar bar-data" x="86" y="273" width="300.4" height="8" rx="2"/>
        <text class="t-num t-data" x="392.4" y="280">865.1 µs</text>
        <rect class="bar bar-ctl" x="86" y="284" width="350.4" height="8" rx="2"/>
        <text class="t-num" x="442.4" y="291">5.115 ms</text>
        <text class="t-lbl t-end" x="74" y="311">1 000 000</text>
        <rect class="bar bar-data" x="86" y="299" width="366" height="8" rx="2"/>
        <text class="t-num t-data" x="458" y="306">8.915 ms</text>
        <rect class="bar bar-ctl" x="86" y="310" width="416.8" height="8" rx="2"/>
        <text class="t-num" x="508.8" y="317">54.21 ms</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> Arrow IPC</span>
        <span class="k-control"><i></i> postcard</span>
    </div>
    <figcaption class="dgm-cap">
        Each pair is one size measured both ways, Arrow IPC above and postcard below. The pair
        worth staring at is the top one in each panel against the one under it. <b>IPC barely
        moves from 1 row to 1 000</b>, which is the shape of a fixed cost, while postcard's
        bar grows with every row it is given.
    </figcaption>
</div>
<!-- /fig:ipc -->

**At one row, postcard is 57× faster to encode and 118× faster to decode.**
Arrow IPC writes a schema header, per-column framing and an alive bitmap. That
fixed cost is about 3 KB and 4 µs regardless of payload, so at one row it is the
entire measurement. postcard writes a handful of bytes.

The crossover is early. By 1 000 rows IPC wins in both directions, 5.5× on
encode and 3.9× on decode. It holds from there: **5.85× on encode and 6.08×
on decode at a million rows.** Decode is the figure that matters for recovery,
since a node that dies mid-batch re-reads its last checkpoint before it can do
any useful work.

Every bar here comes from one build with thin LTO enabled, which optimises the
whole bench unit graph, `serde` and `postcard` included. Comparisons within this
chart are sound; do not compare a bar here against a figure from another build.

For stream mode, a WASM processor that returns a checkpoint pays about 9 µs of
IPC round trip per item on top of everything else. That is a large part of why
the WASM p99 above sits at 420 µs while the native path is at 2 µs. A processor
that carries no state should return `None` rather than an empty dataset.

## Against DataFusion

The same Q6 revenue sum, narrow schema, run through DataFusion 55 as SQL over a
`MemTable`. Source: `crates/saci-connector-datafusion/benches/vs_datafusion_q6.rs`.

<!-- fig:datafusion -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 224" role="img" aria-labelledby="df-t df-d">
        <title id="df-t">SACI against DataFusion 55 on the same Q6, whole and decomposed</title>
        <desc id="df-d">
            Linear scale, zero to 1.35 milliseconds. The SACI pipeline runs Q6 in 456.7 µs, of
            which 108.4 µs is per-iteration setup. DataFusion answers the same query as SQL over a
            MemTable in 1.272 ms end to end; its physical plan execution alone is 696.6 µs, parse,
            optimise and physical planning 370.8 µs, and session setup 17.98 µs. The dashed line
            marks the SACI figure: it falls short of DataFusion's execution-only bar, which is the
            comparison worth quoting.
        </desc>
        <text class="t-ax" x="0" y="11">Q6 REVENUE SUM, 12-COLUMN SCHEMA, 1M ROWS · LINEAR</text>
        <path class="ax" d="M200 20 V202"/>
        <path class="grid" d="M200 20 V202"/>
        <text class="t-ax t-mid" x="200" y="216">0</text>
        <path class="grid" d="M263.9 20 V202"/>
        <text class="t-ax t-mid" x="263.9" y="216">250 µs</text>
        <path class="grid" d="M327.8 20 V202"/>
        <text class="t-ax t-mid" x="327.8" y="216">500 µs</text>
        <path class="grid" d="M391.7 20 V202"/>
        <text class="t-ax t-mid" x="391.7" y="216">750 µs</text>
        <path class="grid" d="M455.6 20 V202"/>
        <text class="t-ax t-mid" x="455.6" y="216">1 ms</text>
        <path class="grid" d="M519.4 20 V202"/>
        <text class="t-ax t-mid" x="519.4" y="216">1.25 ms</text>
        <text class="t-lbl" x="0" y="37">SACI pipeline</text>
        <text class="t-sm" x="0" y="53">3 stages + per-iteration setup</text>
        <rect class="bar bar-data-3" x="200" y="26" width="27.7" height="14" rx="2"/>
        <rect class="bar bar-data" x="227.7" y="26" width="89" height="14" rx="2"/>
        <text class="t-num t-end" x="655" y="37">456.7 µs</text>
        <text class="t-lbl" x="0" y="67">SACI, setup only</text>
        <text class="t-sm" x="0" y="83">paid on every iteration</text>
        <rect class="bar bar-data-3" x="200" y="56" width="27.7" height="14" rx="2"/>
        <text class="t-num t-end" x="655" y="67">108.4 µs</text>
        <text class="t-lbl" x="0" y="97">DataFusion, SQL</text>
        <text class="t-sm" x="0" y="113">end to end, session → execute</text>
        <rect class="bar bar-ctl" x="200" y="86" width="325.1" height="14" rx="2"/>
        <text class="t-num t-end" x="655" y="97">1.272 ms</text>
        <text class="t-lbl" x="0" y="127">DataFusion</text>
        <text class="t-sm" x="0" y="143">physical plan execution alone</text>
        <rect class="bar bar-ctl-2" x="200" y="116" width="178" height="14" rx="2"/>
        <text class="t-num t-end" x="655" y="127">696.6 µs</text>
        <text class="t-lbl" x="0" y="157">DataFusion</text>
        <text class="t-sm" x="0" y="173">parse + optimise + planning</text>
        <rect class="bar bar-ctl-2" x="200" y="146" width="94.8" height="14" rx="2"/>
        <text class="t-num t-end" x="655" y="157">370.8 µs</text>
        <text class="t-lbl" x="0" y="187">DataFusion</text>
        <text class="t-sm" x="0" y="203">session setup</text>
        <rect class="bar bar-ctl-3" x="200" y="176" width="4.6" height="14" rx="2"/>
        <text class="t-num t-end" x="655" y="187">17.98 µs</text>
        <path class="mark" d="M316.7 78 V202"/>
        <text class="t-ax t-ctl" x="321.7" y="74">SACI, 456.7 µs</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> SACI</span>
        <span class="k-data-2"><i></i> SACI per-iteration setup, inside the bar above</span>
        <span class="k-control"><i></i> DataFusion end to end</span>
        <span class="k-ctl-2"><i></i> a measured part of that run</span>
    </div>
    <figcaption class="dgm-cap">
        DataFusion's three lower bars are <b>separate measurements, not a partition of the top
        one</b>. Registration is not timed on its own and they do not sum to the end-to-end
        figure, so they are drawn as their own bars rather than stacked. The SACI bar is
        stacked, because its 108.4 µs of setup is measured inside the 456.7 µs above it.
    </figcaption>
</div>
<!-- /fig:datafusion -->

**Read the decomposition, not the headline.** SACI is 2.79× faster than
DataFusion end to end, but that comparison charges DataFusion for session
construction, table registration and planning. A real deployment pays that work
once and then amortises it over every batch.

The comparison worth quoting is SACI against `datafusion_sql_execute_only`:
**456.7 µs against 696.6 µs, so SACI is 1.53× faster on execution**. SACI carries
its own 108.4 µs of per-iteration setup inside that number, and DataFusion's
execute-only figure does not.

Read that result narrowly. Q6 is one aggregate behind three predicates on one
table: no joins, no subqueries, no cardinality estimation, nothing an optimiser
can be clever about. DataFusion is a mature vectorised engine with a cost-based
optimiser and compiled expression evaluation, and on a query where planning
earns its keep it will win. SACI has no query planner and no optimiser; it runs
the imperative Rust you wrote, in an order derived from your field declarations.
Beating it on the simplest possible query is not the same as beating it at SQL.

## Summary

<!-- fig:summary -->
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 322" role="img" aria-labelledby="sum-t sum-d">
        <title id="sum-t">Every headline result, as a ratio against the thing it was measured against</title>
        <desc id="sum-d">
            Bars run right for faster than the baseline and left for slower, on a logarithmic
            scale with gridlines at ten and a hundred times either side of parity. Faster: Q6 on a
            30-column schema 13.54× (12.41 ms to 916.9 µs); slice parallelism 9.86× on 32 CPUs
            (399.8 ms to 40.56 ms); checkpoint decode at a million rows 6.08× (54.21 ms to 8.915
            ms); checkpoint encode 5.85× (40.73 ms to 6.961 ms); Q6 on a 12-column schema 2.30×
            (2.096 ms to 910.2 µs); Q6 as SQL, execution only, 1.53× (696.6 µs to 456.7 µs).
            Slower: Q1 aggregation 2.18× (1.287 ms to 2.806 ms); checkpoint decode of a single row
            118× (40.7 ns to 4.801 µs); 100 000 rows one at a time against one batch, 324.6× the
            wall time (266.3 µs to 86.44 ms).
        </desc>
        <text class="t-ax" x="0" y="12">BASELINE → SACI</text>
        <text class="t-ax t-mid" x="362.5" y="12">SLOWER  ·  LOG  ·  FASTER</text>
        <path class="grid" d="M298.1 22 V300"/>
        <text class="t-ax t-mid" x="298.1" y="314">10×</text>
        <path class="grid" d="M426.9 22 V300"/>
        <text class="t-ax t-mid" x="426.9" y="314">10×</text>
        <path class="grid" d="M233.7 22 V300"/>
        <text class="t-ax t-mid" x="233.7" y="314">100×</text>
        <path class="grid" d="M491.3 22 V300"/>
        <text class="t-ax t-mid" x="491.3" y="314">100×</text>
        <path class="grid grid-0" d="M362.5 22 V300"/>
        <text class="t-ax t-mid" x="362.5" y="314">1×</text>
        <text class="t-lbl" x="0" y="45">Q6, 30-column schema</text>
        <text class="t-sm" x="0" y="61">12.41 ms → 916.9 µs</text>
        <rect class="bar bar-data" x="362.5" y="34" width="72.9" height="14" rx="2"/>
        <text class="t-num t-end t-data" x="655" y="45">13.54× faster</text>
        <text class="t-lbl" x="0" y="75">Slice parallelism</text>
        <text class="t-sm" x="0" y="91">399.8 ms → 40.56 ms</text>
        <rect class="bar bar-data" x="362.5" y="64" width="64" height="14" rx="2"/>
        <text class="t-num t-end t-data" x="655" y="75">9.86× on 32 CPUs</text>
        <text class="t-lbl" x="0" y="105">Checkpoint decode, 1M rows</text>
        <text class="t-sm" x="0" y="121">54.21 ms → 8.915 ms</text>
        <rect class="bar bar-data" x="362.5" y="94" width="50.5" height="14" rx="2"/>
        <text class="t-num t-end t-data" x="655" y="105">6.08× faster</text>
        <text class="t-lbl" x="0" y="135">Checkpoint encode, 1M rows</text>
        <text class="t-sm" x="0" y="151">40.73 ms → 6.961 ms</text>
        <rect class="bar bar-data" x="362.5" y="124" width="49.4" height="14" rx="2"/>
        <text class="t-num t-end t-data" x="655" y="135">5.85× faster</text>
        <text class="t-lbl" x="0" y="165">Q6, 12-column schema</text>
        <text class="t-sm" x="0" y="181">2.096 ms → 910.2 µs</text>
        <rect class="bar bar-data" x="362.5" y="154" width="23.3" height="14" rx="2"/>
        <text class="t-num t-end t-data" x="655" y="165">2.30× faster</text>
        <text class="t-lbl" x="0" y="195">Q6 as SQL, execute only</text>
        <text class="t-sm" x="0" y="211">696.6 µs → 456.7 µs</text>
        <rect class="bar bar-data" x="362.5" y="184" width="11.9" height="14" rx="2"/>
        <text class="t-num t-end t-data" x="655" y="195">1.53× faster</text>
        <text class="t-lbl" x="0" y="225">Q1, aggregation</text>
        <text class="t-sm" x="0" y="241">1.287 ms → 2.806 ms</text>
        <rect class="bar bar-ctl" x="340.7" y="214" width="21.8" height="14" rx="2"/>
        <text class="t-num t-end t-ctl" x="655" y="225">2.18× slower</text>
        <text class="t-lbl" x="0" y="255">Checkpoint decode, 1 row</text>
        <text class="t-sm" x="0" y="271">40.7 ns → 4.801 µs</text>
        <rect class="bar bar-ctl" x="229" y="244" width="133.5" height="14" rx="2"/>
        <text class="t-num t-end t-ctl" x="655" y="255">118× slower</text>
        <text class="t-lbl" x="0" y="285">Stream vs batch, 100k rows</text>
        <text class="t-sm" x="0" y="301">266.3 µs → 86.44 ms</text>
        <rect class="bar bar-ctl" x="200.7" y="274" width="161.8" height="14" rx="2"/>
        <text class="t-num t-end t-ctl" x="655" y="285">324.6× wall time</text>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> SACI ahead</span>
        <span class="k-control"><i></i> SACI behind</span>
    </div>
    <figcaption class="dgm-cap">
        Three published results have no baseline to divide by, so they are not on the chart.
        They are Q6's column-width scaling (5.92× cost for 30 columns against flat to 0.7%),
        the native stream p99 of <b>2 µs per item</b>, and the framework floor of <b>247 ns
        per item</b>.
    </figcaption>
</div>
<!-- /fig:summary -->

Read top to bottom, that is a fair summary of the engine. Wide schemas,
filter-and-reduce operators, per-item latency and bulk checkpoint recovery are
where the design pays for itself. Group-by aggregation and one-row checkpoints
are where it does not.

## Standing caveats

- **The full Q6 pipeline costs twice what its parts do.** Stage bodies plus
  setup account for 449 µs, and `narrow_direct_systems` measures exactly that,
  but `narrow_saci`, the same work through `Pipeline::run`, measures 910.2 µs.
  The DataFusion benchmark runs the same three-stage Q6 over the same 12-column
  data with the same 108 µs of setup and lands at 456.7 µs. The one structural
  difference between them is that `tpch_q6`'s `ComputeStage` implements
  `run_slice` and the DataFusion bench's does not, so at a million rows the
  former fans out across rayon and the latter does not. Slice fan-out being a
  net loss on a 155 µs stage is consistent with the per-row curve in the
  stage-cost chart, but no A/B inside one benchmark confirms it.
- **Q6 and Q1 are noisy instruments.** Repeated runs of identical code have
  drifted up to ~20% on Q6 and ~6% on Q1; the dual-L3 topology is the likely
  cause. This run was tight (3.3% and 1.0% standard deviation), but treat
  sub-20% Q6 deltas and sub-6% Q1 deltas as noise unless they are pinned. The
  two instruments worth adjudicating a broad change on are
  `ipc_checkpoint_1000000rows` (±0.63%) and `parallelism_compute` (±0.19%).
- **Slice parallelism past ~16 threads.** 9.86× on 32 logical CPUs is well short
  of the ~16× that 16 physical cores suggest. `compute_row_ranges` splits
  uniformly and knows nothing about which L3 domain a worker sits on.
- **The WASM p99.** Mean per-call latency is 179.4 µs, but n=1000 is too few
  samples to characterise the tail.
- **Allocator tuning.** mimalloc's default `purge_delay` captures 3.85× of the
  4.0× available on this workload; every other option measured inside the noise
  floor.

The per-system tracing span costs **0.65%** of `stream_items_of_1` when
`saci-core/tracing` is compiled in without an active subscriber, because
`tracing` checks a global level filter before doing any work. A build with a
subscriber attached pays for what it asked to record, which is an operator
choice rather than framework overhead.
