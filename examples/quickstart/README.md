# Quick Start: NATS to PostgreSQL through two processors

One `saci-service` instance runs `quickstart.kdl`: a single pipeline chaining
two WebAssembly stages, written in two languages against one Arrow schema. A
NATS core subject feeds NDJSON authorisations in; the Go stage
(`validate-go.wasm`) marks each one valid or invalid, the C# stage
(`settle-cs.wasm`) writes the fee and the review tier, and the rows land in a
PostgreSQL `settlements` table. Delivery is at-most-once: the stream runner
acknowledges nothing and keeps no checkpoints, so a service killed mid-batch
loses that batch.

The `Order` schema also carries `usd_amount_display`, a column the Python
stage in the full polyglot pipeline derives. This Quick Start has no Python
stage, so the publisher writes it empty and it stays empty end to end.

## Prerequisites

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- `componentize-go` 0.4.1 with Go 1.25.5+, the .NET SDK 10, and `wasm-tools`
  1.246.2, all pinned in `examples/polyglot/PINS.md`
- A Docker daemon, for the NATS and PostgreSQL containers

## Run it

Every command here runs the same on Linux, macOS and Windows (PowerShell).

1. Build both processor components into `examples/quickstart/build/`:

```text
cargo xtask quickstart
```

This produces `validate-go.wasm` (the unmodified
`examples/polyglot/stages/go-validate` component) and `settle-cs.wasm`
(`examples/quickstart/stages/csharp-settle`).

2. Start NATS and PostgreSQL. `schema.sql` creates the destination table on
   first initialisation; `PostgresSink` never issues `CREATE TABLE`:

```text
docker compose -f examples/quickstart/docker-compose.yml up -d
```

3. Start the service. NATS and PostgreSQL are opt-in connectors, so the
   command names them:

```text
cargo run -p saci-service --features connector-nats,connector-postgresql -- serve -c examples/quickstart/quickstart.kdl
```

4. Publish 5000 authorisations at 500 per second. `--count 0` runs until
   Ctrl-C, for watching the dashboard against a live stream:

```text
cargo run -p saci-service --example quickstart_publish --features connector-nats -- --count 5000 --rate 500
```

5. Watch the dashboard while it runs. The Pipelines tab draws both stages
   chained, source to Go stage to C# stage to sink:

```text
http://127.0.0.1:8080/ui
```

6. Read the result. `review_tier` is the settlement decision the C# stage
   writes: `0` settled, `1` held for review (amount above `hold_above`), `2`
   rejected (the Go stage found the amount below `min_amount`):

```text
docker compose -f examples/quickstart/docker-compose.yml exec -T postgres psql -U postgres -d saci -c 'select review_tier, count(*), round(sum(fee)::numeric, 2) as fees from settlements group by review_tier order by review_tier;'
```

## Tear down

```text
docker compose -f examples/quickstart/docker-compose.yml down -v
```

## Files

| File | What it is |
|------|------------|
| `quickstart.kdl` | the one service: NATS in, Go stage, C# stage, PostgreSQL out |
| `quickstart_publish.rs` | the `saci-service` example that feeds the NATS subject |
| `docker-compose.yml` | NATS 2.11 and PostgreSQL 18, host ports 4222 and 5432 |
| `schema.sql` | the `settlements` table, mounted into the Postgres container |
| `stages/csharp-settle/` | the C# processor, written for this tutorial |
| `build/` | the two components `cargo xtask quickstart` produces (gitignored) |

The Go processor is `examples/polyglot/stages/go-validate`, reused unmodified.
A built component loads into any pipeline whose declared components match what
its `describe()` reports.
