# Integrity: every published row, proved end to end

`integrity_check` publishes a known workload, receives everything the
pipelines emit, recomputes every derived field and every checksum from what it
published, compares the two, and exits non-zero on the first disagreement. The
run ends in a PASS or FAIL verdict.

What it exercises in one workload: four source kinds in the stream half
(Kafka, a local file, an in-process channel and NATS JetStream) plus
PostgreSQL logical replication in the second process, four byte formats on the
way out (ndjson, csv, arrow-ipc, parquet), three WebAssembly processors,
per-batch branch routing, tumbling event-time windows over a merged two-stream
fan-in, cross-batch processor state, and the workflow lifecycle API.

`integrity_check` is publisher, HTTP receiver, verifier and lifecycle driver
in one binary. Every sink in both configs posts to it.

## Why two processes

`run_mode` is a property of a whole config, and the two halves of this
example need opposite ones.

A Kafka consumer, a JetStream consumer and a `ChannelSource` never report
EOF, so only the stream runner can drive them. It pulls one batch from one
source per pass and blocks on the sources rather than polling them. Every
PostgreSQL read mode, `cdc_logical` included, reports EOF once it has caught
up with the change stream. In stream mode that EOF is terminal, the source
leaves the rotation for good, and a workflow whose last source has left
completes and cannot be started again. The interval runner reads the same
EOF as the end of one drain cycle and re-enters, which is what a CDC demo
has to show.

`integrity.kdl` therefore runs the two live workflows in stream mode on port
8088, and `integrity_audit.kdl` runs the CDC workflow in interval mode on port
8089.

| Config | `run_mode` | Workflows | Control plane |
|--------|-----------|-----------|---------------|
| `integrity.kdl` | `stream` | `ingest`, `settle` | 127.0.0.1:8088 |
| `integrity_audit.kdl` | `interval`, 1000 ms | `audit` | 127.0.0.1:8089 |

## Topology

| Workflow | Node | What happens |
|----------|------|--------------|
| `ingest` | `catalog_file` | `FileSource` over `catalog.csv`, read once as csv, then EOF |
| `ingest` | `orders_kafka` | `KafkaSource` on `integrity.orders`, ndjson, one message per batch |
| `ingest` | `classify` | wasm: joins orders against the accumulated catalog, derives `line_total`, `taxable`, `branch` and a checksum, routes the batch |
| `ingest` | `express_http` | branch `express`, ndjson over HTTP |
| `ingest` | `standard_http` | branch `standard`, csv with a header row over HTTP |
| `ingest` | `settle_bridge` | branch `all`, `ChannelSink` on channel `classified` |
| `settle` | `bridge_in` | `ChannelSource` on channel `classified` |
| `settle` | `payments_nats` | `NatsSource`, JetStream durable consumer on `integrity.payments` |
| `settle` | `aggregate` | wasm, windowed: merges both streams into 10s tumbling windows keyed by `region` |
| `settle` | `window_http` | closed windows, arrow-ipc over HTTP |
| `audit` | `audit_cdc` | `PostgresSource`, `cdc_logical` over `public.order_audit` |
| `audit` | `verify_audit` | wasm, stateless: one verdict per change, renaming `__op` to `op` |
| `audit` | `audit_http` | verdicts, parquet over HTTP |

`catalog_file` is declared first so its first arrival is the first item the
stream runner processes: `classify` keeps the catalog in its checkpoint
state, and a sku it has not seen yet classifies as `taxable=false`, which is
a different answer than the verifier expects. The guarantee is one arrival
deep, which is why the catalog is one small file.

`classify` routes per batch, so the config pins one Kafka message to one
batch with `batch_size 1` beside `flow_control { enabled #false }`: without
the second key the runner's admission credit overwrites `batch_size` before
every poll. `ClassifiedOrder.branch` is taken from the batch's first row, so
it always names the branch the rows were actually delivered on.

A window closes when the watermark reaches the window's end. The
`allowed_lateness_ms` budget is not added to that: it decides only which
arriving rows are too late to merge, matching `examples/windowing/`.

A group can fire more than once, and a second firing carries the delta rather
than a restated total: emitting a group removes it, so a late but in-budget
row recreates it from zero. A consumer of `/sink/window` therefore adds the
emissions for a `(window_id, region)` pair together; comparing any single
emission against the expected total fails as soon as one late row arrives.

Two different things put the same pair on the wire twice, and they need
opposite treatment. A re-fire is new information and must be added. A
`RetryingSink` retry is the same information twice and must not be, because
every sink node is wrapped and a refused POST is re-sent. Tell them apart by
the whole request body: a retry re-sends it byte for byte, so a body already
seen is a retry and every row in it is counted as one. Do not deduplicate on
the row's `checksum` instead. It covers `window_id`, `region`, the two counts
and the two amounts, and nothing that says which firing a row came from, so
two genuine re-fires carrying an identical delta would collapse into one and
the total would come up short with nothing to explain it.

Both streams carry simulated event time from one monotonic clock, advancing
`--ts-step-ms` per published item, so no wall clock is stamped anywhere. That
clock's speed is the one tuning constraint in this example. `ClassifiedOrder`
reaches `aggregate` through Kafka, `ingest`, `classify` and the channel,
while `Payment` reaches it straight off NATS, and the watermark is the
maximum over both. Simulated time advances at roughly
`rate * 2.03 * ts_step_ms` milliseconds per wall second (about 2.03 items are
published per order), so a real pipeline lag of `L` seconds becomes that
multiple of `L` in event-time lateness. Keep the product near or below 1000
and a `ClassifiedOrder` has the full 2000 ms budget to cross the bridge; the
defaults, `--rate 40` and `--ts-step-ms 10`, put it at about 813 ms per wall
second. Raising the rate means lowering the step.

## The four sinks

Every sink posts to the one endpoint `integrity_check` serves.

| Path | Format | Component | What the verifier asserts |
|------|--------|-----------|---------------------------|
| `POST /sink/express` | ndjson | `ClassifiedOrder` | every express order arrives exactly once, with the catalog's `taxable`, the recomputed `line_total`, `branch = "express"`, and a matching FNV-1a checksum |
| `POST /sink/standard` | csv with a header row | `ClassifiedOrder` | the same, for `branch = "standard"`, plus that the header names the contract's columns in order |
| `POST /sink/window` | arrow-ipc | `RegionTotal` | added over the emissions for a `(window_id, region)` pair, counting a re-sent body once, the counts and totals match the published orders and payments assigned to that window, and each emission's own checksum matches the values it carries |
| `POST /sink/audit` | parquet | `AuditVerdict` | every insert and every update of `public.order_audit` arrives, with `op`, `ok` and a matching checksum |

Together the two `ClassifiedOrder` sinks must cover every published order
with no row on both and no row on neither: that is the branch-routing proof.

Completeness is absolute for orders, payments and audit changes, over every
published row: each order reaches exactly one of the two `ClassifiedOrder`
sinks, each audit change reaches `/sink/audit`, and each payment is accounted
for in the window group its event time assigns it to.

`/sink/window` is the exception, and the report says so in those words: it
prints how many window groups were verified and how many were **not
verified (excluded)**. The first `--window-warmup-secs` of simulated event
time are excluded from the assertion, because at startup `bridge_in` can
produce nothing until Kafka has been polled, `classify` has run and the
channel has been written, so the node's first watermark is payment-only and
already ahead of any order. Those early groups are received and reported, not
asserted against. The window aggregate is the only component with an
exclusion, and it never excuses a missing order, payment or audit change.

## Prerequisites

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- A Docker daemon, for the Kafka, NATS and PostgreSQL containers
- `cmake` and a C toolchain on `PATH`: `connector-kafka` vendors librdkafka

## Build the processors

The same three builds work on every platform, from the repository root:

```text
cargo build --release -p integrity-classify-wasm --target wasm32-wasip2
cargo build --release -p integrity-aggregate-wasm --target wasm32-wasip2
cargo build --release -p integrity-audit-wasm --target wasm32-wasip2
```

## Run it

Four terminals, in this order.

1. Start the containers:

```text
docker compose -f examples/integrity/docker-compose.yml up -d
```

The compose file brings up `apache/kafka:3.9.0`, `nats:2.11-alpine` with
JetStream enabled, and `postgres:18-alpine` with `wal_level=logical`. It runs
`schema.sql` on first initialisation, which creates `public.order_audit`,
sets it to `REPLICA IDENTITY FULL` and creates the publication
`saci_integrity_pub`. PostgreSQL only runs its init scripts when the data
directory is empty: if the volume was initialised before `schema.sql` existed,
the audit service fails on its first read with a missing-publication error.
Recreate the volume, or apply the SQL by hand:

```text
docker compose -f examples/integrity/docker-compose.yml exec -T postgres psql -U postgres -d saci < examples/integrity/schema.sql
```
Windows (PowerShell):

```powershell
Get-Content examples/integrity/schema.sql | docker compose -f examples/integrity/docker-compose.yml exec -T postgres psql -U postgres -d saci
```

Stop everything with `docker compose -f examples/integrity/docker-compose.yml
down -v`.

2. Start the publisher and verifier, **before** either service:

```text
cargo run -p saci-service --example integrity_check
```

The order is enforced. The publisher resets every piece of shared state before
it publishes a byte, so that the run verifies its own stream rather than a
previous run's leftovers: it deletes and recreates the
Kafka topic `integrity.orders`, deletes the consumer group `saci-integrity`,
purges the JetStream stream `INTEGRITY`, truncates `public.order_audit` and
drops the replication slot `saci_integrity_slot`. Every one of those is
shared with a running service, so it probes both control planes first and
**refuses to run**, with exit code 2, if either answers. There is no
`--force`: resetting under a live consumer takes the topic away from a
subscribed source and the slot away from the CDC reader, and a logical slot
captures only WAL written after it exists, so the audit changes published
during the gap are gone and the run reports a loss it caused itself. Stop
both services before re-running.

Owning the slot is part of the same guarantee. Having dropped it, the
publisher creates `saci_integrity_slot` itself, with the statement the
connector's `slot_autocreate` uses, before it publishes anything; the audit
service then finds the slot already there and accepts it. Waiting only until
`GET /api/workflows` lists `audit` would not be enough, because the workflow
is listed once it is built and the slot appears one drain cycle later.

Nothing survives on the stream half's disk, because `integrity.kdl` declares
no `store "redb"` block. In stream mode the runner threads every processor's
checkpoint blob from one item to the next in memory, so `classify`'s catalog
and `aggregate`'s open windows work without a file. A store would also write
those blobs to disk and load them back when the workflow is built, and the
blob that comes back carries `aggregate`'s watermark: simulated event time
restarts at the same epoch on every run, so that watermark would sit a whole
run ahead of the first arrival, every arrival would be beyond its lateness
budget, and `/sink/window` would stay empty while both order sinks came out
complete. `examples/configs/redb.kdl` is where the store block is
demonstrated.

The publisher also serves the endpoint on 127.0.0.1:9099 that every sink
posts to, so a service starting earlier spends its first batches on refused
connections. It prints the two `serve` commands once the reset is done, and
continues on its own as soon as both control planes answer.

3. Start the stream half:

```text
cargo run -p saci-service --features connector-kafka,connector-nats -- serve \
  --config examples/integrity/integrity.kdl
```
Windows (PowerShell), on one line:

```powershell
cargo run -p saci-service --features connector-kafka,connector-nats -- serve --config examples/integrity/integrity.kdl
```

4. Start the CDC half:

```text
cargo run -p saci-service --features connector-postgresql -- serve --config examples/integrity/integrity_audit.kdl
```
Runs the same on all three platforms.

`connector-kafka`, `connector-nats` and `connector-postgresql` are the three features these two
configs need outside the default bundle; every other connector and transformer they use is on by
default.

Validate the configs first, if you like:

```text
cargo run -p saci-service --features connector-kafka,connector-nats -- validate \
  --config examples/integrity/integrity.kdl --strict
cargo run -p saci-service --features connector-postgresql -- validate \
  --config examples/integrity/integrity_audit.kdl --strict
```
Windows (PowerShell), one command per line:

```powershell
cargo run -p saci-service --features connector-kafka,connector-nats -- validate --config examples/integrity/integrity.kdl --strict
cargo run -p saci-service --features connector-postgresql -- validate --config examples/integrity/integrity_audit.kdl --strict
```

## Delivery semantics, and which workflow proves which

Kafka's offset commit, JetStream's acknowledgements and the replication slot's
confirmed LSN all advance at the *start of the next* `next_batch` call, not
when a batch is handed to the workflow.

- **`pause` and `resume` lose nothing.** The runner parks between passes and
  keeps every source object alive, so the call that acknowledges the last
  batch still happens once it resumes. The verifier pauses `settle`, keeps
  publishing throughout, resumes, and asserts that every payment published
  during the pause still lands in a window.
- **`stop` and `start`, and any crash, redeliver.** Stopping drops the built
  workflow and every connector it holds, so the acknowledging call never
  comes and the broker or the slot replays the last in-flight batch. The
  verifier stops and starts `audit`, then asserts that nothing is missing and
  that any repeated row is byte-identical to its first delivery.

`docs/content/service/operate/workflows.md` documents the lifecycle verbs and
their state transitions.

## Which workflows restart, and why

`ingest` and `settle` each hold a Channel node. A channel is one mpsc pair
created once per process, and the sink being its only sender is what gives
the source a real EOF, so the workflow cannot be torn down and built again:
`stop`, `start` and `restart` all answer **409 Conflict**. `pause` and
`resume` stay available, because they keep the runner and its resources
alive.

`audit` holds no Channel node and its wasm node names a module on disk, so it
rebuilds cleanly and all five verbs work. The verifier asserts both halves of
that contrast, which is why the example needs a workflow of each kind.

## Reading the result

The run prints a per-check table as it goes, then a summary and a verdict.

| Exit code | Meaning |
|-----------|---------|
| 0 | `VERDICT: PASS`, every published row accounted for and every checksum matched |
| 1 | `VERDICT: FAIL`, at least one row missing, duplicated with different values, or carrying a value the verifier did not predict |
| 2 | setup failed before publishing: a control plane answered, a container unreachable, the publication missing, the Kafka topic unusable, the replication slot not creatable |

A failure names the component, the key of the offending row, and the
expected and received values side by side. The first 40 failures are kept
verbatim; the rest are counted. When a completeness failure's missing set is
a contiguous prefix of the published range, the report says so and names the
cause instead of leaving a list of ids to interpret. Only a reader that
attached after the publisher started loses the oldest rows and nothing else.

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| `cannot read replication slot 'saci_integrity_slot' with publication 'saci_integrity_pub'` | the publication does not exist. `schema.sql` only runs on an empty PostgreSQL data directory; recreate the volume with `down -v` or apply the SQL by hand as above |
| `logical decoding requires wal_level >= logical` | the container is not running with `-c wal_level=logical`. Use this example's compose file, or restart PostgreSQL with that setting |
| `FileSource: cannot open ...catalog.csv` at service start | `catalog.csv` is committed, and both configs default to it as a path relative to the repository root. Run every command from there, or set `SACI_CATALOG_FILE` to an absolute path |
| every `RegionTotal` shows `order_count` 0 | simulated time is running too fast for the bridge: `ClassifiedOrder` rows arrive more than `allowed_lateness_ms` (2000) behind the payment-driven watermark and are dropped. Watch the `aggregate.late_rows` metric, and lower `--rate` or `--ts-step-ms` so `rate * 2.03 * ts_step_ms` stays near 1000 |
| `stream not found: INTEGRITY` from the publisher | the `settle` workflow provisions the JetStream stream, and it has not started yet. The publisher waits for it; if it never appears, check that NATS is running with `-js` |
| `Address already in use` on 8088, 8089 or 9099 | another process holds the port. Change the service's `http bind` in the config, or the publisher's `--bind` |
| `stop` on `ingest` or `settle` returns 409 | working as designed: both hold a Channel node. Use `pause` and `resume` |
| the verifier reports orders that were "never published" | a previous run's Kafka messages are being replayed. Deleting the topic does not drop committed offsets, so the consumer group `saci-integrity` must go too; `integrity_check` deletes both at startup |
| the publisher exits 2 naming a control plane that answered `/health` | a service is running, and the startup reset would delete the topic, the consumer group, the JetStream messages, the audit table and the replication slot out from under it. Stop both services and re-run |
| `consumer group saci-integrity still has members 75s after the delete` | a service was killed rather than stopped, and a member outlives its process until the broker's `session.timeout.ms` evicts it. `integrity_check` waits 75s for that eviction before it gives up. Re-run and let it wait, or delete the group with `kafka-consumer-groups.sh --delete --group saci-integrity` |
| the audit failure says the missing set is a contiguous PREFIX | the replication slot was dropped and remade during the run. The publisher creates it before publishing and nothing else should: check that no second publisher ran, and that the audit service was not running through the reset |
| `/sink/window` receives nothing and every closed group "never arrived", while both order sinks are complete | `aggregate`'s whole merged stream was late against its watermark. Read `aggregate.late_rows` and `saci_window_late_arrivals_total`; from a healthy run the one cause is a watermark restored from an earlier run, which is why this config declares no `store "redb"` block |

## The publisher's flags

Every flag the example accepts, and the default it uses when the flag is
absent. An unrecognised flag is an error, not a warning.

| Flag | Default | What it does |
|------|---------|--------------|
| `--duration-secs` | `120` | how long to publish. `0` runs until Ctrl-C |
| `--rate` | `40` | orders per second. Must be at least 1 |
| `--ts-step-ms` | `10` | simulated milliseconds per published item. Must be at least 1; see the clock-speed constraint above |
| `--seed` | `0x5eed1234abcd0007` | seeds the workload generator, so a run is reproducible |
| `--bind` | `127.0.0.1:9099` | address the verification endpoint listens on |
| `--stream-url` | `http://127.0.0.1:8088` | control plane of the stream half |
| `--audit-url` | `http://127.0.0.1:8089` | control plane of the CDC half |
| `--kafka-brokers` | `localhost:9092` | bootstrap servers for the orders topic |
| `--nats-url` | `nats://localhost:4222` | NATS server carrying the payments |
| `--pg-dsn` | `postgres://postgres:saci@127.0.0.1:5432/saci` | the audit database |
| `--catalog-file` | `examples/integrity/catalog.csv` | the catalog, the same path both configs default to |
| `--drain-secs` | `20` | grace after the last publish, for in-flight rows to arrive |
| `--window-warmup-secs` | `45` | simulated event-time seconds excluded from the window assertion |
| `--lifecycle-delay-secs` | `15` | wait before the lifecycle sequence starts |
| `--pause-secs` | `6` | how long `settle` stays paused |
| `--stop-secs` | `10` | how long `audit` stays stopped |
| `--no-lifecycle` | off | skip the lifecycle sequence and assert on the data alone |
| `--quiet` | off | suppress the per-body progress lines, keeping the final report |

## Files

| File / directory | What it is |
|------------------|------------|
| `integrity.kdl` | the `ingest` and `settle` workflows, stream mode, port 8088 |
| `integrity_audit.kdl` | the `audit` workflow, interval mode, port 8089 |
| `integrity_check.rs` | the `saci-service` example that publishes, receives, verifies and drives the lifecycle |
| `catalog.csv` | the eight-sku catalog the `FileSource` reads |
| `schema.sql` | `public.order_audit`, its replica identity and the publication |
| `docker-compose.yml` | Kafka 3.9, NATS 2.11 with JetStream, PostgreSQL 18 with `wal_level=logical` |
| `wasm/classify/` | `integrity-classify-wasm`, the join, derive and route processor |
| `wasm/aggregate/` | `integrity-aggregate-wasm`, the windowed merge processor |
| `wasm/audit/` | `integrity-audit-wasm`, the stateless CDC verdict processor |
