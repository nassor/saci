+++
title = "Tumbling"
description = "Fixed, non-overlapping slices of event time: one period per window, and every row in exactly one of them."
template = "subpage.html"
weight = 1
[[extra.facts]]
label = "Geometry"
value = "Fixed <code>size_ms</code>, non-overlapping"
[[extra.facts]]
label = "A row lands in"
value = "Exactly one window"
[[extra.facts]]
label = "Closes when"
value = "The watermark passes the window's end"
+++

A tumbling window cuts event time into fixed slices that touch and never
overlap. Each is `size_ms` long and starts at a multiple of `size_ms` from the
epoch, shifted by `offset_ms`, so the windows are the same for every key and
for every run over the same data. Every row belongs to exactly one, which is
why the sink holds one row per period per key and why those rows can be summed
again without counting anything twice.

Reach for it when the question is "per period", like revenue per hour, errors
per minute, a daily rollup, or a billing interval. It is also the cheapest
kind to carry, since state holds one open group per key per period and drops
it the moment the period closes.

Tumbling is the wrong kind when the answer has to read "the last five minutes,
as of now". A burst landing on a boundary splits between two windows, and
nothing reports an interval that ends anywhere but at a boundary. That is
[sliding](@/service/processors/windowing/sliding.md).

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 214" role="img" aria-labelledby="tb-t tb-d">
        <title id="tb-t">Four fixed windows partitioning event time, two of them closed behind the watermark</title>
        <desc id="tb-d">
            An event-time axis runs from zero to one hundred and twenty seconds, cut into four
            thirty-second windows that touch and never overlap. Rows sit inside them, each in
            exactly one. Two rows two seconds apart, at twenty-nine and thirty-one seconds, fall
            on either side of the first boundary and so into different windows. The watermark
            stands at seventy-four seconds: windows zero and one end before it, so both are
            closed and emitted, while windows two and three end after it and stay open.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="42" y="40" width="150" height="56" rx="8"/>
            <rect class="hd hd-data" x="42" y="40" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="42" y="52" width="150" height="8"/>
            <text class="t-lbl" x="52" y="55">window 0</text>
            <rect class="blk blk-data" x="192" y="40" width="150" height="56" rx="8"/>
            <rect class="hd hd-data" x="192" y="40" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="192" y="52" width="150" height="8"/>
            <text class="t-lbl" x="202" y="55">window 1</text>
            <rect class="blk" x="342" y="40" width="150" height="56" rx="8"/>
            <rect class="hd" x="342" y="40" width="150" height="20" rx="8"/>
            <rect class="hd" x="342" y="52" width="150" height="8"/>
            <text class="t-lbl" x="352" y="55">window 2</text>
            <rect class="blk" x="492" y="40" width="150" height="56" rx="8"/>
            <rect class="hd" x="492" y="40" width="150" height="20" rx="8"/>
            <rect class="hd" x="492" y="52" width="150" height="8"/>
            <text class="t-lbl" x="502" y="55">window 3</text>
            <path class="ax" d="M42 118 H642"/>
            <text class="t-ax t-mid" x="42" y="134">0s</text>
            <text class="t-ax t-mid" x="192" y="134">30s</text>
            <text class="t-ax t-mid" x="342" y="134">60s</text>
            <text class="t-ax t-mid" x="492" y="134">90s</text>
            <text class="t-ax t-mid" x="642" y="134">120s</text>
        </g>
        <g class="anim anim-2">
            <rect class="bar-data" x="62" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="92" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="126" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="184" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="194" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="242" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="292" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="362" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="392" y="76" width="7" height="7" rx="1"/>
        </g>
        <g class="anim anim-3">
            <path class="ln" d="M192 26 V40"/>
            <text class="t-sm t-mid" x="192" y="20">29s and 31s: different windows</text>
        </g>
        <g class="anim anim-4">
            <path class="mark" d="M412 30 V124"/>
            <text class="t-sm t-ctl" x="418" y="24">watermark 74s</text>
            <text class="t-sm" x="0" y="166">Every row lands in exactly one window, so the windows partition the stream and their counts add up.</text>
            <text class="t-sm" x="0" y="182">Windows 0 and 1 end behind the watermark: both are closed and emitted, one row per key each.</text>
            <text class="t-sm" x="0" y="198">Windows 2 and 3 end ahead of it and stay open, so nothing of theirs reaches a sink yet.</text>
        </g>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> rows, and the windows already closed and emitted</span>
        <span class="k-control"><i></i> the watermark, tracked by the host</span>
        <span class="k-mute"><i></i> a window still open</span>
    </div>
    <figcaption class="dgm-cap">
        The boundaries come from <code>size_ms</code> and <code>offset_ms</code> alone, never
        from the data, so they are the same for every key and every re-run.
    </figcaption>
</div>

## What the example demonstrates

`examples/windowing/tumbling/tumbling.kdl` runs the whole shape in one workflow. Two
core NATS subjects fan into two processors carrying the same logic, one a
component and one a plugin, so their two tables should agree row for row.

| Stage | Node | What happens |
|-------|------|--------------|
| Sources | `sales_a`, `sales_b` | two core NATS subjects, pulled round-robin, one pass per arrival |
| WASM processor | `window_wasm` | merges both streams' batches into 30s tumbling windows, emits closed windows |
| Plugin | `window_plugin` | the identical logic as a native plugin |
| Sinks | `wasm_totals`, `plugin_totals` | one PostgreSQL table per processor |

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 270" role="img" aria-labelledby="win-title win-desc">
        <title id="win-title">Two subjects fan into two windowed processors, each writing its own PostgreSQL table</title>
        <desc id="win-desc">
            The sources sales_a and sales_b both link to both processors. window_wasm is a
            WebAssembly component and window_plugin a native plugin, and each declares the
            same window block: tumbling, thirty seconds, keyed by symbol. window_wasm writes
            closed-window rows to the table wasm_window_totals and window_plugin writes the
            same rows to plugin_window_totals.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="24" width="140" height="48" rx="8"/>
            <rect class="hd hd-data" x="0" y="24" width="140" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="140" height="8"/>
            <text class="t-lbl" x="12" y="39">sales_a</text>
            <text class="t-sm" x="12" y="60">windowing.sales.a</text>
            <rect class="blk blk-data" x="0" y="134" width="140" height="48" rx="8"/>
            <rect class="hd hd-data" x="0" y="134" width="140" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="146" width="140" height="8"/>
            <text class="t-lbl" x="12" y="149">sales_b</text>
            <text class="t-sm" x="12" y="170">windowing.sales.b</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M140 48 H260" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M140 48 C200 48 200 158 260 158" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M140 158 C200 158 200 48 260 48" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M140 158 H260" marker-end="url(#win-d)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="260" y="14" width="175" height="68" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="14" width="175" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="26" width="175" height="8"/>
            <text class="t-lbl t-bnd" x="272" y="29">window_wasm</text>
            <text class="t-sm" x="272" y="52">wasm component</text>
            <text class="t-sm t-ctl" x="272" y="70">tumbling 30s, key symbol</text>
            <rect class="blk blk-bnd" x="260" y="124" width="175" height="68" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="124" width="175" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="136" width="175" height="8"/>
            <text class="t-lbl t-bnd" x="272" y="139">window_plugin</text>
            <text class="t-sm" x="272" y="162">native plugin</text>
            <text class="t-sm t-ctl" x="272" y="180">tumbling 30s, key symbol</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M435 48 H480" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M435 158 H480" marker-end="url(#win-d)"/>
            <rect class="blk blk-data" x="480" y="24" width="170" height="48" rx="8"/>
            <rect class="hd hd-data" x="480" y="24" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="480" y="36" width="170" height="8"/>
            <text class="t-lbl" x="492" y="39">wasm_totals</text>
            <text class="t-sm" x="492" y="60">wasm_window_totals</text>
            <rect class="blk blk-data" x="480" y="134" width="170" height="48" rx="8"/>
            <rect class="hd hd-data" x="480" y="134" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="480" y="146" width="170" height="8"/>
            <text class="t-lbl" x="492" y="149">plugin_totals</text>
            <text class="t-sm" x="492" y="170">plugin_window_totals</text>
            <path class="ln" d="M0 220 H654"/>
            <text class="t-sm" x="0" y="240">The runner pulls the two subjects round-robin, one pass per arrival, and both processors receive both.</text>
            <text class="t-sm" x="0" y="256">Each merges those batches in its own state, then emits one row per closed window and symbol.</text>
        </g>
        <defs>
            <marker id="win-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the two processor runtimes</span>
        <span class="k-control"><i></i> the window block, host knowledge</span>
    </div>
    <figcaption class="dgm-cap">
        Identical inputs, identical windowing, two runtimes. The fan-in is four
        <code>link</code> lines; the merge itself happens inside each processor's own
        state, not in the runner.
    </figcaption>
</div>

Both processors declare the same block and emit one `WindowTotal` row per closed
`(window_id, symbol)` group:

```kdl,name=The block both processors declare
window kind="tumbling" size_ms=30000 time_field="timestamp_ms" allowed_lateness_ms=5000 {
    key_field "symbol"
}
```

Every key of the block is listed under
[Every key](@/service/processors/windowing/_index.md#every-key).

## Prerequisites

- A clone of the repository, from the root of which every command runs.
- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`.
- Docker for NATS and PostgreSQL.
- NATS and PostgreSQL are opt-in connectors, and this config also needs `plugin`: the commands
  below pass `--features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin`.

## 1. Build the processors

```bash,name=Build both processors
cargo build --release -p windowing-tumbling-wasm --target wasm32-wasip2
cargo build --release -p windowing-tumbling-plugin
```

Runs the same on Linux, macOS and Windows (PowerShell). The plugin artifact name
is platform specific. The config defaults to the Linux name, so only macOS and
Windows need `SACI_PLUGIN_LIB`:

| Platform | Plugin artifact | `SACI_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | `target/release/libwindowing_tumbling_plugin.so` | not needed (config default) |
| macOS | `target/release/libwindowing_tumbling_plugin.dylib` | `target/release/libwindowing_tumbling_plugin.dylib` |
| Windows | `target/release/windowing_tumbling_plugin.dll` | `target/release/windowing_tumbling_plugin.dll` |

## 2. Validate the config

Both `window` blocks are checked at load time. On macOS and Windows set
`SACI_PLUGIN_LIB` as in the table above:

Linux:

```bash,name=Validate the tumbling config
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate \
  --config examples/windowing/tumbling/tumbling.kdl --strict
```

macOS:

```bash,name=Validate with the dylib name
SACI_PLUGIN_LIB=target/release/libwindowing_tumbling_plugin.dylib \
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate \
  --config examples/windowing/tumbling/tumbling.kdl --strict
```

Windows (PowerShell):

```powershell
$env:SACI_PLUGIN_LIB = "target/release/windowing_tumbling_plugin.dll"
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate --config examples/windowing/tumbling/tumbling.kdl --strict
```

Expected output ends with `OK: all declared types resolved in built-in
registry`.

## 3. Start the containers

```bash,name=Start the containers
docker compose -f examples/windowing/docker-compose.yml up -d
```

Runs the same on all three platforms. That brings up `nats:2.11-alpine` and
`postgres:18-alpine` and runs `schema.sql` on first initialisation, creating one
table per mode. `PostgresSink` never issues `CREATE TABLE`, and PostgreSQL only
runs init scripts against an empty data directory, so a volume whose init ran
without `schema.sql` fails with `table ... does not exist`. Recreate it with
`down -v` then `up -d`, or apply the SQL by hand:

Linux/macOS:

```bash,name=Apply the schema by hand
docker compose -f examples/windowing/docker-compose.yml exec -T postgres \
  psql -U postgres -d saci < examples/windowing/schema.sql
```

Windows (PowerShell):

```powershell
Get-Content examples/windowing/schema.sql | docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d saci
```

## 4. Start the service

Linux/macOS:

```bash,name=Start the service
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve \
  --config examples/windowing/tumbling/tumbling.kdl
```

Windows (PowerShell):

```powershell
$env:SACI_PLUGIN_LIB = "target/release/windowing_tumbling_plugin.dll"
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve --config examples/windowing/tumbling/tumbling.kdl
```

This config enables the inspector and binds `127.0.0.1:8080`, so the startup
banner prints `dashboard at http://127.0.0.1:8080/ui` and the tables stay empty
until the publisher runs. Leave this terminal running.

## 5. Run the publisher

In another terminal:

```bash,name=Run the publisher
cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000
```

Runs the same on all three platforms. The publisher takes `--count` (0 runs
until Ctrl-C, the default), `--rate` messages per second (default 20), and
`--ts-step-ms` simulated milliseconds per message (default 2000). The rest are
`--gap-every` and `--gap-ms` (both 0 here), `--url` (default
`nats://localhost:4222`), `--subject-a` / `--subject-b` (defaults
`windowing.sales.a` / `windowing.sales.b`), and `--seed`. Those gaps are what the
[session](@/service/processors/windowing/session.md) example needs.

## 6. Read the window totals

The publisher sends `timestamp_ms` (simulated event time, advancing
`--ts-step-ms` per message), `symbol` and `amount`. At the defaults the simulated
clock runs 40 seconds per wall second, so a 30-second window closes roughly
every 0.75 wall seconds and the tables fill continuously.

```bash,name=Read the window totals
docker compose -f examples/windowing/docker-compose.yml exec -T postgres \
  psql -U postgres -d saci -c "SELECT * FROM public.wasm_window_totals ORDER BY window_id, symbol;"
```

Runs the same on all three platforms. Rows for every `window_id` except the
newest is the correct result. A window closes only once the watermark passes its
end. The watermark only moves with the data, so the last window before the
publisher stops stays open.

| Table | Holds |
|-------|-------|
| `public.wasm_window_totals` | one row per closed (window, symbol) group from the wasm processor |
| `public.plugin_window_totals` | the same rows from the plugin processor |

Both carry `window_id` (the window index, so the window starts at
`window_id * 30000` ms), `symbol`, `count` and `sum`. The sinks upsert on
`(window_id, symbol)`, so a late re-fire updates a row instead of duplicating it.
The two tables agree row for row.

Open http://127.0.0.1:8080/ui while it runs to watch the window chips and the
live watermarks. Stop the publisher and the service with Ctrl-C when you are
done, then `docker compose -f examples/windowing/docker-compose.yml down -v`.

## Files

| Path | What it is |
|------|-----------|
| `examples/windowing/tumbling/tumbling.kdl` | the workflow: two sources, two windowed processors, two sinks |
| `examples/windowing/tumbling/wasm/` | `windowing-tumbling-wasm`, the windowed WebAssembly processor (component) |
| `examples/windowing/tumbling/plugin/` | `windowing-tumbling-plugin`, the windowed native processor (cdylib) |
| `examples/windowing/windowed_publish.rs` | the publisher, shared by all three modes |
| `examples/windowing/docker-compose.yml` | NATS and PostgreSQL, with `schema.sql` on first init |
| `examples/windowing/schema.sql` | one table per mode, two of them tumbling's |

## Next

- [Sliding](@/service/processors/windowing/sliding.md): the same fan-in, with every row counted
  in four overlapping windows.
- [Several workflows in one process](@/service/processors/multiple-workflows.md): this processor,
  fed by a channel from another workflow.
