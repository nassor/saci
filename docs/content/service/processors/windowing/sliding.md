+++
title = "Sliding"
description = "Overlapping windows of one length, restarting every slide_ms, so each closed window answers what the last size_ms held."
template = "subpage.html"
weight = 2
[[extra.facts]]
label = "Geometry"
value = "<code>size_ms</code> long, restarting every <code>slide_ms</code>"
[[extra.facts]]
label = "A row lands in"
value = "<code>ceil(size_ms / slide_ms)</code> windows"
[[extra.facts]]
label = "Closes when"
value = "The watermark passes each window's own end"
+++

A sliding window is `size_ms` long like a tumbling one, but a new one starts
every `slide_ms`, so the windows overlap and one row is counted in
`ceil(size_ms / slide_ms)` of them at once. Each closed window answers "the
last `size_ms` up to this instant", and a fresh answer arrives every
`slide_ms` instead of once per period.

Reach for it when the aggregate has to be readable at a finer grain than its
own length. That covers a moving average, a rate alert on the last five
minutes refreshed every thirty seconds, and a trend line.

State and output both multiply by `ceil(size_ms / slide_ms)`, four times here,
so a small `slide_ms` under a large `size_ms` makes this the most expensive
kind. The rows also share their input, so summing several of them double
counts by design. Read one window, or compare windows, never add them.
`slide_ms` equal to `size_ms` is a tumbling window written the long way, and
larger than `size_ms` is refused at load.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 226" role="img" aria-labelledby="sl-t sl-d">
        <title id="sl-t">One row counted in four overlapping windows, of which only the earliest has closed</title>
        <desc id="sl-d">
            An event-time axis runs from zero to one hundred and twenty seconds. Four windows
            are drawn as stacked bars, each sixty seconds long, starting at zero, fifteen,
            thirty and forty-five seconds. A single row at fifty-nine seconds falls inside all
            four, so it is counted four times. The watermark stands at sixty-two seconds. Only
            the window from zero to sixty seconds ends before it, so only that one has closed
            and emitted its total; the other three stay open and emit nothing yet.
        </desc>
        <g class="anim anim-1">
            <text class="t-sm t-end" x="145" y="49">window 0: 0s to 60s</text>
            <rect class="blk blk-data" x="152" y="34" width="245" height="22" rx="5"/>
            <text class="t-sm t-end" x="145" y="77">window 1: 15s to 75s</text>
            <rect class="blk" x="213" y="62" width="245" height="22" rx="5"/>
            <text class="t-sm t-end" x="145" y="105">window 2: 30s to 90s</text>
            <rect class="blk" x="274" y="90" width="245" height="22" rx="5"/>
            <text class="t-sm t-end" x="145" y="133">window 3: 45s to 105s</text>
            <rect class="blk" x="336" y="118" width="245" height="22" rx="5"/>
            <path class="ax" d="M152 152 H642"/>
            <text class="t-ax t-mid" x="152" y="168">0s</text>
            <text class="t-ax t-mid" x="274" y="168">30s</text>
            <text class="t-ax t-mid" x="397" y="168">60s</text>
            <text class="t-ax t-mid" x="519" y="168">90s</text>
            <text class="t-ax t-mid" x="642" y="168">120s</text>
        </g>
        <g class="anim anim-2">
            <path class="ln" d="M393 30 V150"/>
            <rect class="bar-data" x="390" y="41" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="390" y="69" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="390" y="97" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="390" y="125" width="7" height="7" rx="1"/>
            <text class="t-sm t-data t-end" x="387" y="24">one row at 59s</text>
        </g>
        <g class="anim anim-3">
            <path class="mark" d="M405 30 V158"/>
            <text class="t-sm t-ctl" x="411" y="24">watermark 62s</text>
        </g>
        <g class="anim anim-4">
            <text class="t-sm" x="0" y="192">Only window 0 ends behind the watermark, so only it emits: one row per symbol, carrying the 59s row.</text>
            <text class="t-sm" x="0" y="208">Windows 1 to 3 hold the same row and stay open, each closing 15s after the one above it.</text>
        </g>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the row, counted once per window that contains it</span>
        <span class="k-control"><i></i> the watermark, tracked by the host</span>
        <span class="k-mute"><i></i> a window still open</span>
    </div>
    <figcaption class="dgm-cap">
        Every window is <code>size_ms</code> long; a new one starts every
        <code>slide_ms</code>. Four of them overlap any single instant, which is what makes
        one window's total a moving aggregate.
    </figcaption>
</div>

## What the example demonstrates

`examples/windowing/sliding/sliding.kdl` runs two core NATS subjects into one
processor, `window_sliding`, and one PostgreSQL table, with 60-second windows
starting every 15 seconds and keyed by `symbol`. One row of that table is a
60-second moving total, and consecutive rows of one symbol advance it by 15
seconds.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 228" role="img" aria-labelledby="sle-t sle-d">
        <title id="sle-t">Two subjects fan into one sliding processor writing one PostgreSQL table</title>
        <desc id="sle-d">
            The sources sales_a and sales_b both link to window_sliding, a WebAssembly component
            declaring a sliding window sixty seconds long restarting every fifteen seconds, keyed
            by symbol. The processor writes one row per closed window and symbol to the table
            sliding_window_totals through the sink sliding_totals.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="20" width="140" height="48" rx="8"/>
            <rect class="hd hd-data" x="0" y="20" width="140" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="32" width="140" height="8"/>
            <text class="t-lbl" x="12" y="35">sales_a</text>
            <text class="t-sm" x="12" y="56">windowing.sales.a</text>
            <rect class="blk blk-data" x="0" y="116" width="140" height="48" rx="8"/>
            <rect class="hd hd-data" x="0" y="116" width="140" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="128" width="140" height="8"/>
            <text class="t-lbl" x="12" y="131">sales_b</text>
            <text class="t-sm" x="12" y="152">windowing.sales.b</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M140 44 C200 44 200 80 250 80" marker-end="url(#sle-d-m)"/>
            <path class="arw arw-data" d="M140 140 C200 140 200 104 250 104" marker-end="url(#sle-d-m)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="250" y="56" width="200" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="250" y="56" width="200" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="250" y="68" width="200" height="8"/>
            <text class="t-lbl t-bnd" x="262" y="71">window_sliding</text>
            <text class="t-sm" x="262" y="94">wasm component</text>
            <text class="t-sm t-ctl" x="262" y="112">sliding 60s / 15s, key symbol</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M450 92 H484" marker-end="url(#sle-d-m)"/>
            <rect class="blk blk-data" x="488" y="68" width="170" height="48" rx="8"/>
            <rect class="hd hd-data" x="488" y="68" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="488" y="80" width="170" height="8"/>
            <text class="t-lbl" x="500" y="83">sliding_totals</text>
            <text class="t-sm" x="500" y="104">sliding_window_totals</text>
            <path class="ln" d="M0 176 H654"/>
            <text class="t-sm" x="0" y="196">The runner pulls the two subjects round-robin, one pass per arrival, and the processor merges each</text>
            <text class="t-sm" x="0" y="212">arrival into every open window that contains its rows, then writes the windows the watermark closed.</text>
        </g>
        <defs>
            <marker id="sle-d-m" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the processor runtime</span>
        <span class="k-control"><i></i> the window block, host knowledge</span>
    </div>
    <figcaption class="dgm-cap">
        One processor, one table. The fan-in is two <code>link</code> lines, and the merge
        happens inside the processor's own state rather than in the runner.
    </figcaption>
</div>

The processor emits one `SlidingTotal` row per closed `(window, symbol)` group,
carrying the window's bounds rather than an index:

```kdl,name=The block the processor declares
window kind="sliding" size_ms=60000 slide_ms=15000 time_field="timestamp_ms" allowed_lateness_ms=5000 {
    key_field "symbol"
}
```

`size_ms` is an exact multiple of `slide_ms`, which keeps a row in exactly
four distinct windows. When it is not a multiple, the window ids of one
timestamp repeat. Every key of the block is listed under
[Every key](@/service/processors/windowing/_index.md#every-key).

## Prerequisites

- A clone of the repository, from the root of which every command runs.
- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`.
- Docker for NATS and PostgreSQL.
- NATS and PostgreSQL are both opt-in connectors: the commands below pass
  `--features connector-nats,connector-postgresql,transformer-ndjson,wasm`.

## 1. Build the processor

```bash,name=Build the sliding processor
cargo build --release -p windowing-sliding-wasm --target wasm32-wasip2
```

Runs the same on Linux, macOS and Windows (PowerShell).

## 2. Validate the config

The `window` block is checked at load time, and so is the agreement between the
processor's `SlidingTotal` schema and the sink's declared fields.

```bash,name=Validate the sliding config
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- validate --config examples/windowing/sliding/sliding.kdl --strict
```

Runs the same on all three platforms. Expected output ends with `OK: all
declared types resolved in built-in registry`.

## 3. Start the containers

```bash,name=Start the containers
docker compose -f examples/windowing/docker-compose.yml up -d
```

Runs the same on all three platforms. One compose file serves all three
windowing modes. It brings up `nats:2.11-alpine` and `postgres:18-alpine` and
runs `schema.sql` on first initialisation, creating one table per mode.
`PostgresSink` never issues `CREATE TABLE`, and PostgreSQL only runs init
scripts against an empty data directory, so a volume whose init ran without
`sliding_window_totals` in `schema.sql` fails with `table ... does not exist`.
Recreate it with `down -v` then `up -d`, or apply the SQL by hand:

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

```bash,name=Start the service
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve --config examples/windowing/sliding/sliding.kdl
```

Runs the same on all three platforms. This config enables the inspector and
binds `127.0.0.1:8081`, one port per mode, so the startup banner prints
`dashboard at http://127.0.0.1:8081/ui`. The table stays empty until the
publisher runs. Leave this terminal running.

## 5. Run the publisher

In another terminal:

```bash,name=Run the publisher
cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000
```

Runs the same on all three platforms. At those flags the simulated clock runs 40
seconds of event time per wall second, so a window closes roughly every 0.4 wall
seconds and the table fills continuously.

## 6. Read the sliding totals

```bash,name=Read the sliding totals
docker compose -f examples/windowing/docker-compose.yml exec -T postgres \
  psql -U postgres -d saci -c "SELECT * FROM public.sliding_window_totals ORDER BY window_start_ms, symbol;"
```

Runs the same on all three platforms. The rows show the overlap. Each
`window_start_ms` is 15000 ms past the one before it, `window_end_ms` is always
`window_start_ms + 60000`, and any single instant of event time is covered by
four rows per symbol. The sink upserts on `(window_start_ms, symbol)`, so a late
re-fire updates a row instead of duplicating it.

Open http://127.0.0.1:8081/ui while it runs to watch the window chip and the
live watermark. Stop the publisher and the service with Ctrl-C when you are
done, then `docker compose -f examples/windowing/docker-compose.yml down -v`.

## Files

| Path | What it is |
|------|-----------|
| `examples/windowing/sliding/sliding.kdl` | the workflow: two sources, one windowed processor, one sink |
| `examples/windowing/sliding/wasm/` | `windowing-sliding-wasm`, the windowed WebAssembly processor |
| `examples/windowing/windowed_publish.rs` | the publisher, shared by all three modes |
| `examples/windowing/docker-compose.yml` | NATS and PostgreSQL, with `schema.sql` on first init |
| `examples/windowing/schema.sql` | one table per mode |

## Next

- [Session](@/service/processors/windowing/session.md): windows delimited by silence instead of
  by a clock.
- [Windowing](@/service/processors/windowing/_index.md): every `window` key, and what a dropped
  arrival looks like.
