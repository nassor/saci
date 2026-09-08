+++
title = "Several workflows in one process"
description = "Two workflows in one process, bridged by a named channel."
template = "page.html"
weight = 3
+++
# Several workflows in one process

One config file can declare several workflows. Each has its own nodes, links, sources and sinks,
and they run concurrently in one process. A `link` never crosses a workflow boundary; a channel
does. `examples/multi_workflow/multi_workflow.kdl` runs that shape: one NATS stream routed in the
first workflow, half of it bridged into the second, windowed there, and both halves landing in
PostgreSQL.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 280" role="img" aria-labelledby="mw-title mw-desc">
        <title id="mw-title">Two workflows in one process, joined by one channel</title>
        <desc id="mw-desc">
            The route workflow reads the NATS subject multi.orders into the source orders_in,
            which links to the wasm processor router. The router has two labelled links: the
            rush branch goes to the PostgreSQL sink rush_sales, and the standard branch goes to
            the channel sink standard_bridge. A dashed edge leaves standard_bridge and enters
            the settle workflow's channel source standard_in, which links to the wasm processor
            window_sales, which links to the PostgreSQL sink window_totals. No link crosses
            between the two workflows; the channel is the only path.
        </desc>
        <g class="anim anim-1">
            <text class="t-sm t-ctl" x="0" y="12">workflow &quot;route&quot;</text>
            <rect class="blk blk-data" x="0" y="24" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="24" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="130" height="8"/>
            <text class="t-lbl" x="12" y="39">orders_in</text>
            <text class="t-sm" x="12" y="62">multi.orders</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M130 52 H165" marker-end="url(#mw-d)"/>
            <rect class="blk blk-bnd" x="165" y="24" width="120" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="165" y="24" width="120" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="165" y="36" width="120" height="8"/>
            <text class="t-lbl t-bnd" x="177" y="39">router</text>
            <text class="t-sm" x="177" y="62">wasm</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M285 52 C330 52 330 24 375 24" marker-end="url(#mw-d)"/>
            <text class="t-sm t-data t-end" x="367" y="18">rush</text>
            <rect class="blk blk-data" x="375" y="0" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="375" y="0" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="375" y="12" width="160" height="8"/>
            <text class="t-lbl" x="387" y="15">rush_sales</text>
            <text class="t-sm" x="387" y="36">public.rush_sales</text>
            <path class="arw arw-data" d="M285 52 C330 52 330 100 375 100" marker-end="url(#mw-d)"/>
            <text class="t-sm t-data t-end" x="367" y="94">standard</text>
            <rect class="blk blk-data" x="375" y="76" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="375" y="76" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="375" y="88" width="160" height="8"/>
            <text class="t-lbl" x="387" y="91">standard_bridge</text>
            <text class="t-sm" x="387" y="112">channel &quot;standard&quot;</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M455 124 V158 H65 V186" marker-end="url(#mw-b)"/>
            <text class="t-sm t-bnd t-mid" x="260" y="152">the channel, the one path between workflows</text>
            <text class="t-sm t-ctl" x="0" y="178">workflow &quot;settle&quot;</text>
            <rect class="blk blk-data" x="0" y="190" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="190" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="202" width="130" height="8"/>
            <text class="t-lbl" x="12" y="205">standard_in</text>
            <text class="t-sm" x="12" y="228">channel &quot;standard&quot;</text>
            <path class="arw arw-data" d="M130 218 H165" marker-end="url(#mw-d)"/>
            <rect class="blk blk-bnd" x="165" y="190" width="150" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="165" y="190" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="165" y="202" width="150" height="8"/>
            <text class="t-lbl t-bnd" x="177" y="205">window_sales</text>
            <text class="t-sm" x="177" y="228">tumbling 30s</text>
            <path class="arw arw-data" d="M315 218 H350" marker-end="url(#mw-d)"/>
            <rect class="blk blk-data" x="350" y="190" width="185" height="56" rx="8"/>
            <rect class="hd hd-data" x="350" y="190" width="185" height="20" rx="8"/>
            <rect class="hd hd-data" x="350" y="202" width="185" height="8"/>
            <text class="t-lbl" x="362" y="205">window_totals</text>
            <text class="t-sm" x="362" y="228">public.window_totals</text>
        </g>
        <defs>
            <marker id="mw-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="mw-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the stream, and the two tables it lands in</span>
        <span class="k-boundary"><i></i> the two processors, and the channel between the workflows</span>
        <span class="k-control"><i></i> the workflow each half belongs to</span>
    </div>
    <figcaption class="dgm-cap">
        The <code>settle</code> workflow also reads a second NATS subject,
        <code>multi.offsets</code>, into a source of its own. Both of its sources feed
        <code>window_sales</code>, which merges them into one set of windows.
    </figcaption>
</div>

## 1. Declare two workflows

Two `workflow` blocks in one file, each with its own id. Every node id, transformers included,
is unique across the whole file rather than within one workflow, and every `link` names two
nodes of its own workflow.

```kdl,name=Two workflows, one process
run_mode kind="stream"

workflow "route" {
    source "orders_in" type="NatsSource" component="Sale" transformer="ndjson_fmt" { /* ... */ }
    wasm "router" module="target/wasm32-wasip2/release/multi_workflow_router_wasm.wasm"
    sink "rush_sales" type="PostgresSink" component="Sale" { /* ... */ }

    link from="orders_in" to="router"
    link from="router" to="rush_sales" branch="rush"
}

workflow "settle" {
    source "offsets_in" type="NatsSource" component="Sale" transformer="settle_ndjson" { /* ... */ }
    wasm "window_sales" module="target/wasm32-wasip2/release/windowing_tumbling_wasm.wasm" {
        window kind="tumbling" size_ms=30000 time_field="timestamp_ms" allowed_lateness_ms=5000 {
            key_field "symbol"
        }
    }
    sink "window_totals" type="PostgresSink" component="WindowTotal" { /* ... */ }

    link from="offsets_in" to="window_sales"
    link from="window_sales" to="window_totals"
}
```

Each workflow gets its own runner, its own run and error counters, and its own card on the
dashboard. A `transformer` declared in one workflow serves that workflow only, which is why
`route` and `settle` each declare their own `ndjson` transformer.

## 2. Bridge them with a channel

A `ChannelSink` in one workflow and a `ChannelSource` in another meet on a shared `name`. That
is the only way a batch crosses a workflow boundary. Both halves declare the same `name`, the
same `buffer` and the same `schema_fields`.

```kdl,name=The two halves of one channel
// In workflow "route": the standard branch leaves through the channel.
sink "standard_bridge" type="ChannelSink" component="Sale" {
    config name="standard" buffer=64 {
        schema_fields "timestamp_ms" type="int64" nullable=#false
        schema_fields "symbol" type="utf8" nullable=#false
        schema_fields "amount" type="float64" nullable=#false
    }
}

// In workflow "settle": the same name picks up the other end.
source "standard_in" type="ChannelSource" component="Sale" {
    config name="standard" buffer=64 {
        schema_fields "timestamp_ms" type="int64" nullable=#false
        schema_fields "symbol" type="utf8" nullable=#false
        schema_fields "amount" type="float64" nullable=#false
    }
}
```

One name carries exactly one sink and exactly one source. Two sinks on one name, two sources on
one name, or a half with no partner are all refused before anything runs.

The sink is the channel's only writer, so the consumer's source reaches EOF when the producing
workflow's channel sink finishes and drops it. Until then, `standard_in` behaves like any other
live source: it waits.

## 3. Validate and run

Build the two processors first. The router is this example's own crate; the windowed half reuses
the tumbling windowing example's component unchanged.

```bash,name=Build both processors
cargo build --release -p multi-workflow-router-wasm --target wasm32-wasip2
cargo build --release -p windowing-tumbling-wasm --target wasm32-wasip2
```

Runs the same on Linux, macOS and Windows (PowerShell).

Start NATS and PostgreSQL. The compose file brings up `nats:2.11-alpine` and
`postgres:18-alpine` and runs `schema.sql` on first initialisation, which creates the two
tables.

```bash,name=Start the containers
docker compose -f examples/multi_workflow/docker-compose.yml up -d
```

Runs the same on all three platforms. PostgreSQL only runs its init scripts against an empty
data directory, so a volume created before `schema.sql` existed leaves the sinks failing with
`table ... does not exist`. Recreate it with `down -v` then `up -d`, or apply the SQL by hand:

Linux/macOS:

```bash,name=Apply the schema by hand
docker compose -f examples/multi_workflow/docker-compose.yml exec -T postgres \
  psql -U postgres -d saci < examples/multi_workflow/schema.sql
```

Windows (PowerShell):

```powershell
Get-Content examples/multi_workflow/schema.sql | docker compose -f examples/multi_workflow/docker-compose.yml exec -T postgres psql -U postgres -d saci
```

Validate the config. The channel pairing is checked here, before any connector opens:

Linux/macOS:

```bash,name=Validate the two-workflow config
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- validate \
  --config examples/multi_workflow/multi_workflow.kdl --strict
```

Windows (PowerShell):

```powershell
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- validate --config examples/multi_workflow/multi_workflow.kdl --strict
```

```text,name=What validate prints
OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
```

Then serve it, and publish into both subjects from a second terminal:

Linux/macOS:

```bash,name=Serve the config
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve \
  --config examples/multi_workflow/multi_workflow.kdl
```

Windows (PowerShell):

```powershell
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve --config examples/multi_workflow/multi_workflow.kdl
```

```bash,name=Publish into both subjects
cargo run -p saci-service --features connector-nats --example multi_workflow_publish -- --rate 20 --ts-step-ms 2000
```

Runs the same on all three platforms. The publisher sends `timestamp_ms`, `symbol` (AAPL, GOOG
or MSFT) and `amount` between 50.0 and 150.0, so the router's 100.0 threshold splits the stream
roughly in half. At the defaults the simulated clock runs 40 seconds per wall second, so a 30
second window closes about every 0.75 wall seconds.

Both tables fill, which is the proof that the bridge carried rows:

```bash,name=Read both tables
docker compose -f examples/multi_workflow/docker-compose.yml exec -T postgres \
  psql -U postgres -d saci -c 'SELECT count(*) FROM public.rush_sales; SELECT * FROM public.window_totals ORDER BY window_id, symbol;'
```

Runs the same on all three platforms. `rush_sales` holds every `rush` pass's rows untouched.
The router reads one row to decide, the first of the pass, so every row of a pass follows
that row's branch. At `--rate 20` the source collects one message per pass, so every row in
`rush_sales` has `amount` of 100.0 or more. Publish faster than one message per
`poll_timeout_ms` and a pass carries several, so a row under 100.0 rides along with a first
row over it. `window_totals` holds one row per closed window and symbol, with `window_id`,
`count` and `sum`. The newest window stays open, because a window closes only once the
watermark passes its end.

## 4. See both on the dashboard

The config enables the inspector and binds `127.0.0.1:8080`, so the startup banner prints
`dashboard at http://127.0.0.1:8080/ui`. Open it while the publisher runs.

The Pipelines tab draws one card per workflow, `route` and `settle`, each with its own
header, its own run and error badges and its own independently laid out graph. Below them
sits a `channel bridges` card listing `standard  standard_bridge -> standard_in` with its
live rate. That row is the bridge, the one edge no graph draws, because no `link` declares
it. The `window_sales` box carries its window chip, and its detail sheet lists the geometry,
the time field, the key field, the lateness budget and the live watermark.
[The live dashboard](@/service/operate/dashboard.md) walks the rest of the tabs.

## When it refuses to start

| Message | What to change |
|---|---|
| `channel 'standard': declares a ChannelSink but no ChannelSource` | Add the source half, in whichever workflow consumes the stream. |
| `channel 'standard': declares a ChannelSource but no ChannelSink` | Add the sink half, in whichever workflow produces the stream. |
| `channel 'standard': more than one ChannelSink declared` | One name carries one writer. Give the second producer its own channel name. |
| `channel 'standard': more than one ChannelSource declared` | One name carries one reader. Give the second consumer its own channel name. |
| `channel 'standard': the paired ChannelSource and ChannelSink declare different schemas` | Make both halves' `schema_fields` lists identical, name for name and type for type. |
| `channel 'standard': buffer 64 differs from the paired half's buffer 8` | Set the same `buffer` on both halves. |
| `ChannelSource config requires a 'name' key naming the shared channel` | Give the source half a `name` in its `config` block. |
| `ChannelSource config requires a 'schema_fields' list` | Declare the columns the channel carries. |

## Next

- [Tumbling windows](@/service/processors/windowing/tumbling.md): the windowed processor the
  `settle` workflow runs, on its own.
- [Channel](@/service/connectors/channel.md): every key of the two nodes that form the bridge.
