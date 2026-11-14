# Integrity: every published row, proved end to end

`integrity_check` publishes a known workload, receives everything two SACI services emit, recomputes every derived field and checksum from what it published, and compares. It exits `0` on a PASS and non-zero on the first disagreement.

One run covers four source kinds (Kafka, a file, an in-process channel, NATS JetStream) plus PostgreSQL logical replication, four byte formats on the way out (ndjson, csv, arrow-ipc, parquet), three WebAssembly processors, per-batch branch routing, tumbling event-time windows over two merged streams, cross-batch processor state, and the workflow lifecycle API.

`integrity_check` is publisher, HTTP receiver, verifier and lifecycle driver in one binary. Every sink in both configs posts to it on `127.0.0.1:9099`.

<img src="../../docs/static/integrity-topology.svg" alt="integrity_check publishes to Kafka, a catalog file, NATS and PostgreSQL. The stream service runs ingest and settle, the CDC service runs audit, and all four sinks post back to integrity_check." width="660">

## Two services

`run_mode` belongs to a whole config, and the two halves need opposite modes.

A Kafka consumer, a JetStream consumer and a `ChannelSource` never report EOF, so only the stream runner can drive them. Every PostgreSQL read mode, `cdc_logical` included, reports EOF once it has caught up. In stream mode that EOF retires the source for good; the interval runner treats it as the end of one drain cycle and reads again.

| Config | `run_mode` | Workflows | Control plane |
|--------|-----------|-----------|---------------|
| `integrity.kdl` | `stream` | `ingest`, `settle` | `127.0.0.1:8088` |
| `integrity_audit.kdl` | `interval`, 1000 ms | `audit` | `127.0.0.1:8089` |

## Prerequisites

- Rust 1.99 or newer, plus the WASI target: `rustup target add wasm32-wasip2`
- A running Docker daemon, for the Kafka, NATS and PostgreSQL containers
- `cmake` and a C toolchain on `PATH`: `connector-kafka` vendors librdkafka

Run every command from the repository root. The commands below are identical on Linux, macOS and Windows (PowerShell) unless a step says otherwise.

## Run it

Use three terminals for steps 3 to 5, and keep the order.

1. Build the three processors:

   ```text
   cargo build --release -p integrity-classify-wasm -p integrity-aggregate-wasm -p integrity-audit-wasm --target wasm32-wasip2
   ```

   Three `.wasm` files appear under `target/wasm32-wasip2/release/`.

2. Start the containers:

   ```text
   docker compose -f examples/integrity/docker-compose.yml up -d
   ```

   `docker compose -f examples/integrity/docker-compose.yml ps` lists `kafka`, `nats` and `postgres`, each `healthy` after a few seconds. The compose file runs `apache/kafka:3.9.0`, `nats:2.11-alpine` with JetStream, and `postgres:18-alpine` with `wal_level=logical`. PostgreSQL applies `schema.sql` (table `public.order_audit`, `REPLICA IDENTITY FULL`, publication `saci_integrity_pub`) only when its data directory is empty.

3. Start the publisher and verifier, **before** either service:

   ```text
   cargo run -p saci --features service,transformer-arrow-ipc,transformer-csv,transformer-ndjson,transformer-parquet --example integrity_check
   ```

   It resets its shared state, then prints `start both services now` followed by the two `serve` commands, and waits for both control planes.

4. In a second terminal, start the stream half:

   ```text
   cargo run -p saci --features server,connector-kafka,connector-nats -- serve --config examples/integrity/integrity.kdl
   ```

5. In a third terminal, start the CDC half:

   ```text
   cargo run -p saci --features server,connector-postgresql -- serve --config examples/integrity/integrity_audit.kdl
   ```

   The publisher prints `both control planes report their workflows`, publishes for 120 seconds, drives the lifecycle checks, and ends with `VERDICT: PASS`.

To check the configs without running anything, add `validate` and `--strict`:

```text
cargo run -p saci --features server,connector-kafka,connector-nats -- validate --config examples/integrity/integrity.kdl --strict
cargo run -p saci --features server,connector-postgresql -- validate --config examples/integrity/integrity_audit.kdl --strict
```

Stop everything with Ctrl-C in each terminal, then `docker compose -f examples/integrity/docker-compose.yml down -v`.

### Why the publisher starts first

Before it publishes a byte, the publisher resets state shared with the services: it recreates the Kafka topic `integrity.orders`, deletes the consumer group `saci-integrity`, purges the JetStream stream `INTEGRITY`, truncates `public.order_audit`, drops the replication slot `saci_integrity_slot` and creates it again. Doing that under a live consumer would lose audit changes, so the publisher probes both control planes and exits with code `2` if either answers. There is no `--force`. Stop both services before a re-run.

The publisher also serves the sink endpoint, so a service that starts earlier spends its first batches on refused connections.

If the PostgreSQL volume predates `schema.sql`, the audit service fails on its first read with a missing-publication error. Recreate the volume with `down -v`, or apply the SQL by hand.

Linux/macOS:

```text
docker compose -f examples/integrity/docker-compose.yml exec -T postgres psql -U postgres -d saci < examples/integrity/schema.sql
```

Windows (PowerShell):

```powershell
Get-Content examples/integrity/schema.sql | docker compose -f examples/integrity/docker-compose.yml exec -T postgres psql -U postgres -d saci
```

## What flows where

| Workflow | Node | What it does |
|----------|------|--------------|
| `ingest` | `catalog_file` | `FileSource` over `catalog.csv`, read once as csv, then EOF |
| `ingest` | `orders_kafka` | `KafkaSource` on `integrity.orders`, ndjson, one message per batch |
| `ingest` | `classify` | wasm: joins orders with the accumulated catalog, derives `line_total`, `taxable`, `branch` and a checksum, routes the batch |
| `ingest` | `express_http`, `standard_http` | branch `express` posts ndjson, branch `standard` posts csv with a header row |
| `ingest` | `settle_bridge` | branch `all`: `ChannelSink` on channel `classified` |
| `settle` | `bridge_in`, `payments_nats` | `ChannelSource` on `classified`, and a JetStream durable consumer on `integrity.payments` |
| `settle` | `aggregate` | wasm, windowed: merges both streams into 10 s tumbling windows keyed by `region` |
| `settle` | `window_http` | closed windows, arrow-ipc |
| `audit` | `audit_cdc` | `PostgresSource`, `cdc_logical` over `public.order_audit` |
| `audit` | `verify_audit` | wasm, stateless: one verdict per change, `__op` renamed to `op` |
| `audit` | `audit_http` | verdicts, parquet |

## What the verifier asserts

| Endpoint | Format | Component | Assertion |
|----------|--------|-----------|-----------|
| `POST /sink/express` | ndjson | `ClassifiedOrder` | every express order arrives once, with the catalog's `taxable`, the recomputed `line_total`, `branch = "express"` and a matching FNV-1a checksum |
| `POST /sink/standard` | csv, header row | `ClassifiedOrder` | the same for `branch = "standard"`, and the header names the contract's columns in order |
| `POST /sink/window` | arrow-ipc | `RegionTotal` | summed over a `(window_id, region)` pair, the counts and totals match the orders and payments assigned to that window, and each emission's checksum matches its values |
| `POST /sink/audit` | parquet | `AuditVerdict` | every insert and update of `public.order_audit` arrives with `op`, `ok` and a matching checksum |

The two `ClassifiedOrder` sinks together must cover every published order, with no row on both and none on neither. That is the branch-routing proof. Orders, payments and audit changes are checked for completeness without exception.

`/sink/window` has one exclusion, and the report names it: it prints how many window groups were verified and how many were **not verified (excluded)**. The first `--window-warmup-secs` of simulated event time is excluded, because at startup `bridge_in` has nothing until Kafka has been polled and `classify` has run, so the node's first watermark comes from payments alone. Those groups are received and reported, never asserted.

## How the config behaves

- Catalog first. `catalog_file` is declared first so its single arrival is the first item the stream runner processes. `classify` keeps the catalog in its checkpoint state, and a sku it has not seen classifies as `taxable=false`, which the verifier does not expect. The guarantee is one arrival deep, which is why the catalog is one small file.
- One message, one batch. `classify` routes per batch and reads the first row's priority. The config sets `batch_size 1` next to `flow_control { enabled #false }`; without the second key, the runner's admission credit overwrites `batch_size` before every poll.
- Windows close on the watermark. A window closes when the watermark reaches its end. `allowed_lateness_ms` (2000) only decides which arriving rows are too late to merge, as in `examples/windowing/`.
- A re-fire is added, a retry is not. A group can fire again after a late but in-budget row, and the second emission carries a delta, so a consumer adds the emissions for a `(window_id, region)` pair. Every sink node is wrapped in `RetryingSink`, so a refused POST is re-sent byte for byte. The verifier counts a request body it has already seen once. It does not deduplicate on `checksum`, because two genuine re-fires with an identical delta would collapse into one.
- Event time is simulated. Both streams take event time from one monotonic clock that advances `--ts-step-ms` per published item. About 2.03 items are published per order, so event time runs at `rate * 2.03 * ts_step_ms` ms per wall second. A `ClassifiedOrder` crosses Kafka, `ingest` and the channel, while a `Payment` comes straight from NATS, and the watermark follows the maximum of both. Keep that product near or below 1000 so an order has the full 2000 ms budget. The defaults (`--rate 40`, `--ts-step-ms 10`) give about 813. Raise the rate only by lowering the step.
- No `store "redb"` block. In stream mode the runner passes every processor's checkpoint from one item to the next in memory, so `classify`'s catalog and `aggregate`'s open windows work without a file. A store would restore `aggregate`'s watermark from the previous run, which sits a whole run ahead of the new event time. Every arrival would then be late, and `/sink/window` would stay empty while both order sinks look complete. `examples/configs/redb.kdl` shows the store block.

## Delivery and lifecycle

Kafka's offset commit, JetStream's acknowledgements and the replication slot's confirmed LSN all advance at the start of the next `next_batch` call, not when a batch is handed to the workflow.

- `pause` and `resume` lose nothing. The runner keeps every source object alive, so the acknowledging call still happens on resume. The verifier pauses `settle`, keeps publishing, resumes, and asserts every payment from the pause lands in a window.
- `stop` and `start`, or a crash, redeliver. Stopping drops the workflow and its connectors, so the broker or the slot replays the last in-flight batch. The verifier stops and starts `audit`, then asserts nothing is missing and any repeated row is byte-identical to its first delivery.

`ingest` and `settle` each hold a Channel node. A channel is one mpsc pair created once per process, so the workflow cannot be rebuilt, and `stop`, `start` and `restart` answer **409 Conflict**. `pause` and `resume` still work. `audit` has no Channel node and its wasm node names a module on disk, so all five verbs work. The verifier asserts both halves of that contrast. [`docs/content/service/operate/workflows.md`](../../docs/content/service/operate/workflows.md) documents the lifecycle verbs.

## Reading the result

The run prints a per-check table, a summary and a verdict.

| Exit code | Meaning |
|-----------|---------|
| `0` | `VERDICT: PASS`: every published row is accounted for and every checksum matched |
| `1` | `VERDICT: FAIL`: a row is missing, duplicated with different values, or carries an unpredicted value |
| `2` | setup failed before publishing: a control plane answered, a container is unreachable, the publication is missing, the Kafka topic is unusable, or the replication slot cannot be created |

A failure names the component, the key of the row, and the expected and received values. The report keeps the first 40 failures and counts the rest. When the missing rows of a completeness failure are a contiguous prefix of the published range, the report says so and names the cause.

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| `cannot read replication slot 'saci_integrity_slot' with publication 'saci_integrity_pub'` | The publication is missing. Recreate the volume with `down -v` or apply `schema.sql` as above. |
| `logical decoding requires wal_level >= logical` | PostgreSQL is not running with `-c wal_level=logical`. Use this example's compose file. |
| `FileSource: cannot open ...catalog.csv` | Both configs default to a path relative to the repository root. Run from there, or set `SACI_CATALOG_FILE` to an absolute path. |
| every `RegionTotal` shows `order_count` 0 | Event time runs too fast for the bridge, so orders arrive past `allowed_lateness_ms` and are dropped. Check `aggregate.late_rows` and lower `--rate` or `--ts-step-ms`. |
| `/sink/window` empty while both order sinks are complete | `aggregate` saw its whole merged stream as late. Read `aggregate.late_rows` and `saci_window_late_arrivals_total`; the one healthy-run cause is a watermark restored from a previous run, which is why the config has no `store "redb"` block. |
| `stream not found: INTEGRITY` | `settle` provisions the JetStream stream and has not started. The publisher waits; if it never appears, check that NATS runs with `-js`. |
| `Address already in use` on 8088, 8089 or 9099 | Change `http bind` in the config, or the publisher's `--bind`. |
| `stop` on `ingest` or `settle` returns 409 | By design, both hold a Channel node. Use `pause` and `resume`. |
| the verifier reports orders "never published" | A previous run's Kafka messages were replayed. Deleting the topic keeps committed offsets, so the consumer group `saci-integrity` must go too; the publisher deletes both. |
| the publisher exits `2` naming a control plane that answered `/health` | A service is running. Stop both services and re-run. |
| `consumer group saci-integrity still has members 75s after the delete` | A service was killed instead of stopped, and the broker has not yet evicted the member. Re-run and let the publisher wait, or delete the group with `kafka-consumer-groups.sh --delete --group saci-integrity`. |
| the audit failure says the missing set is a contiguous prefix | The replication slot was dropped and recreated during the run. Check that no second publisher ran and the audit service was not running through the reset. |

## The publisher's flags

An unrecognised flag is an error.

| Flag | Default | What it does |
|------|---------|--------------|
| `--duration-secs` | `120` | how long to publish; `0` runs until Ctrl-C |
| `--rate` | `40` | orders per second, at least 1 |
| `--ts-step-ms` | `10` | simulated milliseconds per published item, at least 1 |
| `--seed` | `0x5eed1234abcd0007` | seeds the workload generator, so a run is reproducible |
| `--bind` | `127.0.0.1:9099` | address of the sink endpoint |
| `--stream-url` | `http://127.0.0.1:8088` | control plane of the stream half |
| `--audit-url` | `http://127.0.0.1:8089` | control plane of the CDC half |
| `--kafka-brokers` | `localhost:9092` | bootstrap servers for the orders topic |
| `--nats-url` | `nats://localhost:4222` | NATS server carrying the payments |
| `--pg-dsn` | `postgres://postgres:saci@127.0.0.1:5432/saci` | the audit database |
| `--catalog-file` | `examples/integrity/catalog.csv` | the catalog, the path both configs default to |
| `--drain-secs` | `20` | grace after the last publish for in-flight rows |
| `--window-warmup-secs` | `45` | simulated seconds excluded from the window assertion |
| `--lifecycle-delay-secs` | `15` | wait before the lifecycle sequence starts |
| `--pause-secs` | `6` | how long `settle` stays paused |
| `--stop-secs` | `10` | how long `audit` stays stopped |
| `--no-lifecycle` | off | skip the lifecycle sequence and assert on the data alone |
| `--quiet` | off | suppress per-body progress lines, keep the final report |

## Files

| File / directory | What it is |
|------------------|------------|
| `integrity.kdl` | the `ingest` and `settle` workflows, stream mode, port 8088 |
| `integrity_audit.kdl` | the `audit` workflow, interval mode, port 8089 |
| `integrity_check.rs` | the `saci` example: publisher, receiver, verifier and lifecycle driver |
| `catalog.csv` | the eight-sku catalog the `FileSource` reads |
| `schema.sql` | `public.order_audit`, its replica identity and the publication |
| `docker-compose.yml` | Kafka, NATS with JetStream, PostgreSQL with `wal_level=logical` |
| `wasm/classify/` | `integrity-classify-wasm`: join, derive and route |
| `wasm/aggregate/` | `integrity-aggregate-wasm`: the windowed merge |
| `wasm/audit/` | `integrity-audit-wasm`: the stateless CDC verdict |
