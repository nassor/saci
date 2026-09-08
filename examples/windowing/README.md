# Windowing: one runnable example per window kind

A `window` block turns an unbounded stream into per-key aggregates over
bounded slices of event time. The host validates the geometry, tracks the
node's watermark and injects the declaration into the processor's config; the
processor keeps the open windows in its checkpoint state and emits the closed
ones. `docs/content/service/processors/windowing/_index.md` covers what a
window is, what closes one and every key in the block. This file is the
commands.

Three demos, one per kind, each a long-running `saci-service` stream. All
three read the same two core NATS subjects, so run one at a time:

| Directory | Config | Kind | Processors |
|-----------|--------|------|------------|
| `tumbling/` | `tumbling/tumbling.kdl` | tumbling, 30 000 ms | a wasm component and a native plugin, identical logic |
| `sliding/` | `sliding/sliding.kdl` | sliding, 60 000 ms every 15 000 ms | one wasm component |
| `session/` | `session/session.kdl` | session, 10 000 ms gap | one wasm component |

Every mode keys on `symbol` with 5 000 ms of allowed lateness, and the three
share this directory's `docker-compose.yml`, `schema.sql` and
`windowed_publish.rs`. The `http` binds differ (8080, 8081, 8082), so two
modes at once would clash only on the subjects.

## Prerequisites

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- A Docker daemon, for the NATS and PostgreSQL containers

## Start the containers

Runs the same on every platform, from the repository root:

```text
docker compose -f examples/windowing/docker-compose.yml up -d
```

The compose file brings up `nats:2.11-alpine` and `postgres:18-alpine` and
runs `schema.sql` on first initialisation, which creates all four tables: one
per mode, and a second for tumbling's plugin.
PostgreSQL only runs the init scripts when its data directory is empty: if
the volume was initialised before those tables existed (or by another
project's compose file), the sinks fail with `table ... does not exist`.
Recreate the volume (`docker compose -f examples/windowing/docker-compose.yml
down -v`, then `up -d` again) or apply the SQL by hand:

```text
docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d saci < examples/windowing/schema.sql
```
Windows (PowerShell):

```powershell
Get-Content examples/windowing/schema.sql | docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d saci
```

Stop everything with `docker compose -f examples/windowing/docker-compose.yml
down -v`.

## Tumbling

Two NATS subjects fan into two processors carrying identical logic, one a
WebAssembly component and one a native plugin, so their two tables should
agree row for row.

| Stage | Node | What happens |
|-------|------|--------------|
| Sources | `sales_a`, `sales_b` | two core NATS subjects, pulled round-robin, one pass per arrival |
| WASM processor | `window_wasm` | merges both streams' batches into 30s tumbling windows, emits closed windows |
| Plugin | `window_plugin` | the identical logic as a native plugin |
| Sinks | `wasm_totals`, `plugin_totals` | one PostgreSQL table per processor |

Build, on every platform:

```text
cargo build --release -p windowing-tumbling-wasm --target wasm32-wasip2
cargo build --release -p windowing-tumbling-plugin
```

The plugin artifact name is platform specific. The config's default library
path (`target/release/libwindowing_tumbling_plugin.so`) is the Linux name, so
only macOS and Windows need `SACI_PLUGIN_LIB`:

| Platform | Plugin artifact | `SACI_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | target/release/libwindowing_tumbling_plugin.so | not needed (config default) |
| macOS | target/release/libwindowing_tumbling_plugin.dylib | target/release/libwindowing_tumbling_plugin.dylib |
| Windows | target/release/windowing_tumbling_plugin.dll | target/release/windowing_tumbling_plugin.dll |

Serve. Linux:

```text
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve \
  --config examples/windowing/tumbling/tumbling.kdl
```

macOS:

```text
SACI_PLUGIN_LIB=target/release/libwindowing_tumbling_plugin.dylib \
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve \
  --config examples/windowing/tumbling/tumbling.kdl
```

Windows (PowerShell), on one line, with `$env:SACI_PLUGIN_LIB` set as in the
table above:

```powershell
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve --config examples/windowing/tumbling/tumbling.kdl
```

Publish, in another terminal, on every platform:

```text
cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000
```

Read the totals:

```text
docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d saci -c 'SELECT * FROM public.wasm_window_totals ORDER BY window_id, symbol;'
```

| Table | Holds |
|-------|-------|
| `public.wasm_window_totals` | one row per closed (window, symbol) group from the wasm processor |
| `public.plugin_window_totals` | the same rows from the plugin processor |

Both carry `window_id` (tumbling window index; the window start in
milliseconds is `window_id * 30000`), `symbol`, `count` and `sum`. The sinks
upsert on `(window_id, symbol)`, so a re-run of the publisher or a late
re-fire within the lateness budget updates a row instead of duplicating it.

## Sliding

Two NATS subjects fan into one processor, `window_sliding`. A window is 60 000
ms long and a new one starts every 15 000 ms, so every row is counted in four
overlapping windows and one window's `sum` is a 60-second moving total.

Build, serve and publish, on every platform:

```text
cargo build --release -p windowing-sliding-wasm --target wasm32-wasip2
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve --config examples/windowing/sliding/sliding.kdl
cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000
```

Read the totals:

```text
docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d saci -c 'SELECT * FROM public.sliding_window_totals ORDER BY window_start_ms, symbol;'
```

`public.sliding_window_totals` carries `window_start_ms`, `window_end_ms`
(always the start plus 60 000), `symbol`, `count` and `sum`. Consecutive
starts are 15 000 ms apart and four rows per symbol cover any single instant,
which is the overlap no other kind shows. The sink upserts on
`(window_start_ms, symbol)`.

## Session

Two NATS subjects fan into one processor, `window_session`. A session lasts as
long as its symbol keeps producing events no more than 10 000 ms apart, so the
events decide its length while the config only sets the gap.

A session ends where its own symbol's events stop for more than 10 000 ms.
With a fixed step that happens by chance, since each message's symbol is drawn
from three at random, and nothing bounds a session's span. The publisher's gap
flags put a silence in every symbol's stream at the same instant instead, so a
session never outlives its burst. Build, serve and publish, on every platform:

```text
cargo build --release -p windowing-session-wasm --target wasm32-wasip2
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm -- serve --config examples/windowing/session/session.kdl
cargo run -p saci-service --features connector-nats --example windowed_publish -- --rate 20 --ts-step-ms 2000 --gap-every 20 --gap-ms 30000
```

Read the totals:

```text
docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d saci -c 'SELECT * FROM public.session_window_totals ORDER BY session_start_ms, symbol;'
```

`public.session_window_totals` carries `session_start_ms`, `session_end_ms`
(the burst's last event for that symbol plus 10 000), `symbol`, `count` and
`sum`. Expect one row per burst per symbol, with the span varying between
rows. A span (`session_end_ms - session_start_ms`) above 48 000 ms means the
gap flags did not reach the clock: with them a session covers at most a whole
burst, 38 000 ms, plus the 10-second gap its end carries. The sink upserts on
`(session_start_ms, symbol)`.

## Restarting the publisher

The simulated clock starts at the same base on every run, so restarting the
publisher against a still-running service rewinds event time by the whole
previous run. A watermark does not rewind with it. Every arrival is then
beyond the 5-second lateness budget, so the processor drops all of it and
the tables stop filling while the sources and processors keep reporting
work. The service warns once on the `saci::windowing` target, counts each
dropped arrival in `saci_window_late_arrivals_total`, and each processor's
sheet shows a `dropped arrivals` row. Restart the service together with the
publisher.

## What the dashboard shows

Open the mode's own address: http://127.0.0.1:8080/ui for tumbling, 8081 for
sliding, 8082 for session. Each processor box carries its window chip; the
detail sheet lists the window geometry, the time field, the key field, the
lateness budget, and the live watermark in UTC. The three wasm processors
report `window.open`, `window.closed` and `window.late_rows` through
host-io::metric and the tumbling plugin reports the same three through the
plugin ABI, so every sheet's processor-metrics section reads the same whichever
mode is running. Tumbling's two boxes stay in lockstep on identical inputs and
identical windowing logic, writing two tables.

## The publisher's flags

`--count` (0 runs until Ctrl-C, the default), `--rate` messages per second
(default 20), `--ts-step-ms` simulated milliseconds per message (default
2000), `--gap-every` messages per burst (default 0, no gaps), `--gap-ms`
simulated milliseconds each gap adds (default 0), `--url` (default
`nats://localhost:4222`), `--subject-a` / `--subject-b` (defaults
`windowing.sales.a` / `windowing.sales.b`), `--seed`. `--gap-every` needs
`--gap-ms`, and the timestamp stays a pure function of the message id either
way, so a re-run reproduces the same sequence.

Validate a config first, if you like (set `SACI_PLUGIN_LIB` as in the
tumbling table above on macOS and Windows):

```text
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate \
  --config examples/windowing/tumbling/tumbling.kdl --strict
```
Windows (PowerShell), on one line:

```powershell
cargo run -p saci-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate --config examples/windowing/tumbling/tumbling.kdl --strict
```

The sliding and session configs also need `--features connector-nats,connector-postgresql,transformer-ndjson,wasm`,
but no `plugin`: neither declares one.

## Files

| File / directory | What it is |
|------------------|------------|
| `tumbling/tumbling.kdl` | the stream workflow with the fan-in and both tumbling processors |
| `tumbling/wasm/` | the `windowing-tumbling-wasm` processor component (cdylib, wasm32-wasip2) |
| `tumbling/plugin/` | the `windowing-tumbling-plugin` native plugin (cdylib) |
| `sliding/sliding.kdl` | the sliding workflow: the same fan-in, one processor, one sink |
| `sliding/wasm/` | the `windowing-sliding-wasm` processor component |
| `session/session.kdl` | the session workflow: the same fan-in, one processor, one sink |
| `session/wasm/` | the `windowing-session-wasm` processor component |
| `windowed_publish.rs` | the `saci-service` example that feeds both NATS subjects |
| `schema.sql` | the four tables, mounted into the Postgres container |
| `docker-compose.yml` | NATS 2.11 and PostgreSQL 18 |
