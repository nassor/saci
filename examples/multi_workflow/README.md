# Multi-workflow: one stream routed across two workflows, bridged in process

A service can declare several workflows in one config: each has its own
nodes, links, sources and sinks, and they run concurrently in one process. A
`ChannelSink` in one workflow meets a `ChannelSource` in another on a shared
channel `name`. That channel is the only crossing, because a `link` never
leaves its workflow. `multi_workflow.kdl` is that arrangement in one
long-running `saci-service` stream.

| Workflow | Node | What happens |
|----------|------|--------------|
| `route` | `orders_in` | core NATS subject `multi.orders`, pulled round-robin, sliced into one pass per chunk |
| `route` | `router` | wasm processor: routes each batch on its first row's `amount` |
| `route` | `rush_sales` | PostgreSQL sink for `rush` batches (`amount >= 100.0`), rows land untouched |
| `route` | `standard_bridge` | `ChannelSink` for `standard` batches, on channel `standard` |
| `settle` | `standard_in` | `ChannelSource` on channel `standard`: the bridged half of the stream |
| `settle` | `offsets_in` | second core NATS subject `multi.offsets`, same `Sale` schema |
| `settle` | `window_sales` | wasm processor (the tumbling windowing demo's, unchanged): merges both sources into 30s tumbling windows, emits closed windows |
| `settle` | `window_totals` | PostgreSQL sink for the closed windows |

The router declares the same `Sale` schema the tumbling windowing processor
uses, so rows bridge across the channel unchanged: `standard_bridge` and
`standard_in` both declare `name="standard"`. The dashboard draws one card per
workflow and a `channel bridges` card listing `standard` with its live rate.

## Prerequisites

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- A Docker daemon, for the NATS and PostgreSQL containers

## Build the processors

The router is this example's own crate; the windowed half reuses the tumbling
windowing example's component unchanged.

```text
cargo build --release -p multi-workflow-router-wasm --target wasm32-wasip2
cargo build --release -p windowing-tumbling-wasm --target wasm32-wasip2
```
Runs the same on Linux, macOS and Windows (PowerShell).

## Run it

Start NATS and PostgreSQL, then the service, then the publisher.

1. Start the containers:

```text
docker compose -f examples/multi_workflow/docker-compose.yml up -d
```

The compose file brings up `nats:2.11-alpine` and `postgres:18-alpine` and
runs `schema.sql` on first initialisation, which creates the two tables.
PostgreSQL only runs the init scripts when its data directory is empty: if
the volume was initialised before `schema.sql` existed (or by another
project's compose file), the sinks fail with `table ... does not exist`.
Recreate the volume (`docker compose -f examples/multi_workflow/docker-compose.yml
down -v`, then `up -d` again) or apply the SQL by hand:

```text
docker compose -f examples/multi_workflow/docker-compose.yml exec -T postgres psql -U postgres -d saci < examples/multi_workflow/schema.sql
```
Windows (PowerShell):

```powershell
Get-Content examples/multi_workflow/schema.sql | docker compose -f examples/multi_workflow/docker-compose.yml exec -T postgres psql -U postgres -d saci
```

Stop everything with `docker compose -f examples/multi_workflow/docker-compose.yml
down -v`.

2. Start the service:

```text
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve \
  --config examples/multi_workflow/multi_workflow.kdl
```
Windows (PowerShell), on one line:

```powershell
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve --config examples/multi_workflow/multi_workflow.kdl
```

3. Publish, in another terminal:

```text
cargo run -p saci-service --features connector-nats --example multi_workflow_publish -- --rate 20 --ts-step-ms 2000
```
Runs the same on all three platforms.

## What each table holds

The publisher's messages carry `timestamp_ms` (simulated event time,
advancing `--ts-step-ms` per message), `symbol` (AAPL, GOOG or MSFT) and
`amount` (50.0 to 150.0, so the router's 100.0 threshold splits the stream
roughly in half). At the defaults, 20 messages per second and 2000 ms of
simulated time each, the simulated clock runs 40 seconds per wall second, so
a 30-second window closes roughly every 0.75 wall seconds and `window_totals`
fills continuously.

| Table | Holds |
|-------|-------|
| `public.rush_sales` | every `rush` batch's rows, `amount >= 100.0`, untouched |
| `public.window_totals` | one row per closed (window, symbol) group from the settle workflow |

`rush_sales` upserts on `(timestamp_ms, symbol)`; `window_totals` has the
columns `window_id` (tumbling window index; the window start in milliseconds
is `window_id * 30000`), `symbol`, `count` and `sum`, upserting on
`(window_id, symbol)` so a re-run of the publisher or a late re-fire within
the lateness budget updates a row instead of duplicating it. The last window
seen before the publisher stops stays open: a window only closes once the
watermark passes its end, and the watermark only moves with the data.

```text
docker compose -f examples/multi_workflow/docker-compose.yml exec -T postgres psql -U postgres -d saci -c 'SELECT count(*) FROM public.rush_sales; SELECT count(*) FROM public.rush_sales WHERE amount < 100.0; SELECT * FROM public.window_totals ORDER BY window_id, symbol;'
```

## Restarting the publisher

The simulated clock starts at the same base on every run, so restarting the
publisher against a still-running service rewinds event time by the whole
previous run. `window_sales`'s watermark does not rewind with it. Every
arrival is then beyond the 5-second lateness budget, so the node drops all of
it and `window_totals` stops filling while `orders_in`, `offsets_in` and the
processor all keep reporting work. The service warns once on the
`saci::windowing` target, counts each dropped arrival in
`saci_window_late_arrivals_total`, and the `window_sales` sheet shows a
`dropped arrivals` row. Restart the service together with the publisher.

## What the dashboard shows

Open http://127.0.0.1:8080/ui. The Pipelines tab draws two cards, `route` and
`settle`, each with its own header, its own run/error badges and its own
independently laid out graph, plus a `channel bridges` card below them
listing `standard  standard_bridge → standard_in` with its live rate. The
`window_sales` box carries the `⟐30s` window chip; its detail sheet lists the
window geometry, the time field, the key field, the lateness budget and the
live watermark in UTC.

## The publisher's flags

`--count` (0 runs until Ctrl-C, the default), `--rate` messages per second
(default 20), `--ts-step-ms` simulated milliseconds per message (default
2000), `--url` (default `nats://localhost:4222`), `--subject-a` /
`--subject-b` (defaults `multi.orders` / `multi.offsets`), `--seed`.

Validate the config first, if you like:

```text
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- validate \
  --config examples/multi_workflow/multi_workflow.kdl --strict
```
Windows (PowerShell), on one line:

```powershell
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- validate --config examples/multi_workflow/multi_workflow.kdl --strict
```

## Files

| File / directory | What it is |
|------------------|------------|
| `multi_workflow.kdl` | the `route` and `settle` workflows, bridged by the `standard` channel |
| `multi_workflow_publish.rs` | the `saci-service` example that feeds both NATS subjects |
| `schema.sql` | the `rush_sales` and `window_totals` tables |
| `docker-compose.yml` | NATS 2.11 and PostgreSQL 18 |
| `wasm/` | the `multi-workflow-router-wasm` processor component (cdylib, wasm32-wasip2) |
