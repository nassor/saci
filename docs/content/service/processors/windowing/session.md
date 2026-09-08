+++
title = "Session"
description = "Windows delimited by silence instead of by a clock, so a session's start and length come from the data."
template = "subpage.html"
weight = 3
[[extra.facts]]
label = "Geometry"
value = "Gap delimited, one session per key per burst"
[[extra.facts]]
label = "A row lands in"
value = "The session it extends, or a new one"
[[extra.facts]]
label = "Closes when"
value = "The watermark passes the last event plus <code>gap_ms</code>"
+++

A session window has no clock. It groups one key's consecutive events for as
long as they stay within `gap_ms` of each other, and ends wherever a longer
silence appears. The start, the length and the count all come from the data.
Two sessions of the same key need not be alike, and two keys' sessions need
not line up at all.

Reach for it when the unit of work is an activity rather than a period: a
user's visit, a device's connected stretch, a support conversation, one game
match. The gap is the entire model, so it is the only number to get right. Too
small splits one activity into fragments, and too large glues unrelated ones
together. A key that never falls silent keeps one session open forever,
holding its state and emitting nothing.

Take `gap_ms` from the idle time measured between real events. The closed
row's `session_end_ms` is the last event plus `gap_ms`, the instant nothing
more could join.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 214" role="img" aria-labelledby="se-t se-d">
        <title id="se-t">Two bursts of one key become two sessions of different lengths, of which the first has closed</title>
        <desc id="se-d">
            An event-time axis runs from zero to one hundred and twenty seconds. One symbol's
            events arrive in two bursts: five of them between two and eighteen seconds, then
            three more between sixty-two and seventy seconds. The silence between the bursts
            is forty-four seconds, far wider than the ten-second gap, so the bursts are two
            separate sessions. The first session is drawn from its first event at two seconds
            to ten seconds past its last, ending at twenty-eight seconds. The watermark stands
            at seventy seconds, past that end, so the first session has closed and emitted its
            total. The second session would end at eighty seconds, ahead of the watermark, so
            it stays open and emits nothing yet.
        </desc>
        <g class="anim anim-1">
            <path class="ax" d="M42 118 H642"/>
            <text class="t-ax t-mid" x="42" y="134">0s</text>
            <text class="t-ax t-mid" x="192" y="134">30s</text>
            <text class="t-ax t-mid" x="342" y="134">60s</text>
            <text class="t-ax t-mid" x="492" y="134">90s</text>
            <text class="t-ax t-mid" x="642" y="134">120s</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="52" y="40" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="52" y="40" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="52" y="52" width="130" height="8"/>
            <text class="t-lbl" x="62" y="55">session 2s to 28s</text>
            <rect class="bar-data" x="52" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="72" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="92" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="112" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="132" y="76" width="7" height="7" rx="1"/>
            <text class="t-sm" x="52" y="110">5 events, last at 18s</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk" x="352" y="40" width="90" height="56" rx="8"/>
            <rect class="hd" x="352" y="40" width="90" height="20" rx="8"/>
            <rect class="hd" x="352" y="52" width="90" height="8"/>
            <text class="t-lbl" x="362" y="55">session 62s</text>
            <rect class="bar-data" x="352" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="372" y="76" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="392" y="76" width="7" height="7" rx="1"/>
            <text class="t-sm" x="352" y="110">3 events so far</text>
            <path class="ln" d="M182 68 H352"/>
            <text class="t-sm t-mid" x="267" y="62">44s of silence</text>
        </g>
        <g class="anim anim-4">
            <path class="mark" d="M392 24 V124"/>
            <text class="t-sm t-ctl" x="398" y="30">watermark 70s</text>
            <text class="t-sm" x="0" y="166">The first session closed at its last event plus the 10s gap, the instant no further event could join it.</text>
            <text class="t-sm" x="0" y="182">The silence is more than gap_ms, so the second burst is a new session; it ends at 80s, ahead of the watermark.</text>
            <text class="t-sm" x="0" y="198">Neither length is configured: the data decided both.</text>
        </g>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> events, and the session they closed into</span>
        <span class="k-control"><i></i> the watermark, tracked by the host</span>
        <span class="k-mute"><i></i> a session still open</span>
    </div>
    <figcaption class="dgm-cap">
        A session block declares a gap and nothing else. There is no size to declare: the
        events are what set the bounds.
    </figcaption>
</div>

## What the example demonstrates

`examples/windowing/session/session.kdl` runs two core NATS subjects into one
processor, `window_session`, and one PostgreSQL table, with a 10-second gap
keyed by `symbol`. The processor carries its open sessions across batches in
its checkpoint, which is what lets one session span several arrivals. It
merges two of a key's sessions when an out-of-order row lands inside the
silence between them.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 228" role="img" aria-labelledby="see-t see-d">
        <title id="see-t">Two subjects fan into one session processor writing one PostgreSQL table</title>
        <desc id="see-d">
            The sources sales_a and sales_b both link to window_session, a WebAssembly component
            declaring a session window with a ten-second gap, keyed by symbol. The processor
            writes one row per closed session and symbol to the table session_window_totals
            through the sink session_totals.
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
            <path class="arw arw-data" d="M140 44 C200 44 200 80 250 80" marker-end="url(#see-d-m)"/>
            <path class="arw arw-data" d="M140 140 C200 140 200 104 250 104" marker-end="url(#see-d-m)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="250" y="56" width="200" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="250" y="56" width="200" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="250" y="68" width="200" height="8"/>
            <text class="t-lbl t-bnd" x="262" y="71">window_session</text>
            <text class="t-sm" x="262" y="94">wasm component</text>
            <text class="t-sm t-ctl" x="262" y="112">session 10s gap, key symbol</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M450 92 H484" marker-end="url(#see-d-m)"/>
            <rect class="blk blk-data" x="488" y="68" width="170" height="48" rx="8"/>
            <rect class="hd hd-data" x="488" y="68" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="488" y="80" width="170" height="8"/>
            <text class="t-lbl" x="500" y="83">session_totals</text>
            <text class="t-sm" x="500" y="104">session_window_totals</text>
            <path class="ln" d="M0 176 H654"/>
            <text class="t-sm" x="0" y="196">The runner pulls the two subjects round-robin, one pass per arrival, and the processor keeps every</text>
            <text class="t-sm" x="0" y="212">key's open session in its checkpoint, so one session spans as many arrivals as its key keeps busy.</text>
        </g>
        <defs>
            <marker id="see-d-m" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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
        One processor, one table. Both subjects feed one watermark, so a session of one symbol
        closes on whichever subject carried the arrival that passed it.
    </figcaption>
</div>

The processor emits one `SessionTotal` row per closed `(session, symbol)` group,
whose `session_end_ms` is the last event plus `gap_ms`:

```kdl,name=The block the processor declares
window kind="session" gap_ms=10000 time_field="timestamp_ms" allowed_lateness_ms=5000 {
    key_field "symbol"
}
```

Every key of the block is listed under
[Every key](@/service/processors/windowing/_index.md#every-key).

## Prerequisites

- A clone of the repository, from the root of which every command runs.
- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`.
- Docker for NATS and PostgreSQL.
- NATS and PostgreSQL are both opt-in connectors: the commands below pass
  `--features connector-nats,connector-postgresql,transformer-ndjson,wasm`.

## 1. Build the processor

```bash,name=Build the session processor
cargo build --release -p windowing-session-wasm --target wasm32-wasip2
```

Runs the same on Linux, macOS and Windows (PowerShell).

## 2. Validate the config

The `window` block is checked at load time, and so is the agreement between the
processor's `SessionTotal` schema and the sink's declared fields.

```bash,name=Validate the session config
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- validate --config examples/windowing/session/session.kdl --strict
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
`session_window_totals` in `schema.sql` fails with `table ... does not exist`.
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
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve --config examples/windowing/session/session.kdl
```

Runs the same on all three platforms. This config enables the inspector and
binds `127.0.0.1:8082`, one port per mode, so the startup banner prints
`dashboard at http://127.0.0.1:8082/ui`. The table stays empty until the
publisher runs. Leave this terminal running.

## 5. Run the publisher

A session ends where its own symbol's events stop for more than `gap_ms`. The
publisher draws each message's symbol from three at random, so with a fixed
step that happens by chance. A symbol not drawn for six messages in a row
leaves 12 000 ms of silence. Sessions do close, but nothing bounds their span,
and one run of 400 messages produced a 130-second session. `--gap-every` and
`--gap-ms` put a silence in every symbol's stream at the same instant instead.

Linux/macOS:

```bash,name=Run the publisher with event-time gaps
cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000 \
  --gap-every 20 --gap-ms 30000
```

Windows (PowerShell):

```powershell
cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000 --gap-every 20 --gap-ms 30000
```

Twenty messages 2000 ms apart is one burst spanning 38 000 ms of event time.
The 30-second jump after every twentieth message exceeds the 10-second gap
for every symbol at once. Each burst's sessions all close on the first
arrival after the jump, so no session outlives its burst.

## 6. Read the session totals

```bash,name=Read the session totals
docker compose -f examples/windowing/docker-compose.yml exec -T postgres \
  psql -U postgres -d saci -c "SELECT * FROM public.session_window_totals ORDER BY session_start_ms, symbol;"
```

Runs the same on all three platforms. Expect several rows per symbol per burst.
`session_end_ms - session_start_ms` varies between them, because it depends on
where that symbol's draws fell, and `session_end_ms` is always its last event
of the session plus 10000. The silence between one row's `session_end_ms` and
the next row's `session_start_ms` for the same symbol exceeds 10000 by
construction, because that silence is what ended the session. The sink upserts
on `(session_start_ms, symbol)`, so a late re-fire updates a row instead of
duplicating it.

A span longer than 48 000 ms means the gap flags did not reach the clock. With
them a session covers at most a whole burst, 38 000 ms of event time, plus the
10-second gap its end carries.

Open http://127.0.0.1:8082/ui while it runs to watch the window chip and the
live watermark. Stop the publisher and the service with Ctrl-C when you are
done, then `docker compose -f examples/windowing/docker-compose.yml down -v`.

## Files

| Path | What it is |
|------|-----------|
| `examples/windowing/session/session.kdl` | the workflow: two sources, one windowed processor, one sink |
| `examples/windowing/session/wasm/` | `windowing-session-wasm`, the windowed WebAssembly processor |
| `examples/windowing/windowed_publish.rs` | the publisher, shared by all three modes |
| `examples/windowing/docker-compose.yml` | NATS and PostgreSQL, with `schema.sql` on first init |
| `examples/windowing/schema.sql` | one table per mode |

## Next

- [Windowing](@/service/processors/windowing/_index.md): every `window` key, and what a dropped
  arrival looks like.
- [Several workflows in one process](@/service/processors/multiple-workflows.md): a windowed
  processor fed by a channel from another workflow.
