# Distributed Order Fulfillment

A 3-node SACI Raft cluster that exercises field-granular DAG scheduling,
parallel and sequential systems, world resources, checkpointing, Raft
consensus and structured tracing.

The three SACI nodes form a Raft cluster that replicates the work pool itself:
the master batches, row-range claims and checkpoints all live in each node's
own `cluster-app.redb`, applied from the raft log, so the cluster needs no
external store.

Node 1 bootstraps the Raft cluster and runs the order generator, which every
`--generator-interval` seconds produces 300 to 500 synthetic `Order` rows and
registers them as a master batch. All three nodes run a `DistributedRunner`
that claims, processes, checkpoints and acks batches. A claim is a raft-
committed state-machine transition, so exactly one node processes each row
range, and the claim-to-ack cycle gives at-least-once delivery.

Each batch runs a four-stage pipeline over `Order` rows:

| Stage | Systems | Access |
|-------|---------|--------|
| 0 | `ValidateOrderSystem`, `DetectFraudSystem`, `ConvertCurrencySystem` | parallel (`ParallelSystem`), disjoint field writes |
| 1 | `CheckInventorySystem` | sequential (`System`, exclusive world access) |
| 2 | `ApproveOrderSystem`, `ComputeTaxSystem` | parallel, disjoint field writes |
| 3 | `GenerateInvoiceSystem` | sequential, appends `Invoice` rows |

The field-level conflict analyser groups the parallel stages; the sequential
stages need exclusive access. A checkpoint is saved after stage 2
(`CheckpointStrategy::EveryNStages(2)`), and `GenerateInvoiceSystem` retries
with a fixed 3-attempt backoff. `Invoice` rows are appended to the in-memory
dataset; nothing writes them to disk in this example.

## Prerequisites

- Rust 1.95 or newer
- A Docker daemon, for the Compose path

## Run with Docker Compose

Compose builds the example and starts all three nodes. Every command here
runs the same on Linux, macOS and Windows (PowerShell).

```text
docker compose -f examples/distributed_fulfillment/docker-compose.yml up --build
```

Watch the logs in real time:

```text
docker compose -f examples/distributed_fulfillment/docker-compose.yml logs -f --tail=50 node1 node2 node3
```

Watch for a `cluster has leader` line once the Raft cluster elects a leader,
then the generator's `generator: registered batch` lines (with `node_id`,
`batch_id` and `rows` fields) to confirm work is flowing. The generator runs
only on the leader. Stop everything with:

```text
docker compose -f examples/distributed_fulfillment/docker-compose.yml down
```

## Run locally

Build the example binary:

```text
cargo build -p saci-service --example distributed_fulfillment --features service-cluster
```

Then run three nodes, one per terminal. Node 1 bootstraps the cluster and
runs the generator.

Terminal 1, Linux and macOS:

```text
RUST_LOG=trace ./target/debug/examples/distributed_fulfillment \
  --node-id 1 --bootstrap \
  --listen 127.0.0.1:9001 \
  --data-dir /tmp/fulfillment/node1 \
  --output-dir /tmp/fulfillment/output/node1 \
  --peers 127.0.0.1:9002,127.0.0.1:9003 \
  --generator-interval 10
```

Windows (PowerShell):

```powershell
$env:RUST_LOG = "trace"
.\target\debug\examples\distributed_fulfillment --node-id 1 --bootstrap --listen 127.0.0.1:9001 --data-dir C:\fulfillment\node1 --output-dir C:\fulfillment\output\node1 --peers 127.0.0.1:9002,127.0.0.1:9003 --generator-interval 10
```

Terminal 2, Linux and macOS:

```text
RUST_LOG=trace ./target/debug/examples/distributed_fulfillment \
  --node-id 2 \
  --listen 127.0.0.1:9002 \
  --data-dir /tmp/fulfillment/node2 \
  --output-dir /tmp/fulfillment/output/node2 \
  --peers 127.0.0.1:9001,127.0.0.1:9003
```

Windows (PowerShell):

```powershell
$env:RUST_LOG = "trace"
.\target\debug\examples\distributed_fulfillment --node-id 2 --listen 127.0.0.1:9002 --data-dir C:\fulfillment\node2 --output-dir C:\fulfillment\output\node2 --peers 127.0.0.1:9001,127.0.0.1:9003
```

Terminal 3, Linux and macOS:

```text
RUST_LOG=trace ./target/debug/examples/distributed_fulfillment \
  --node-id 3 \
  --listen 127.0.0.1:9003 \
  --data-dir /tmp/fulfillment/node3 \
  --output-dir /tmp/fulfillment/output/node3 \
  --peers 127.0.0.1:9001,127.0.0.1:9002
```

Windows (PowerShell):

```powershell
$env:RUST_LOG = "trace"
.\target\debug\examples\distributed_fulfillment --node-id 3 --listen 127.0.0.1:9003 --data-dir C:\fulfillment\node3 --output-dir C:\fulfillment\output\node3 --peers 127.0.0.1:9001,127.0.0.1:9002
```

## Observable behaviour

`--log-json` (or `RUST_LOG` at trace level) makes the structured fields
visible. The lines below come straight from the example's `tracing` calls.

| Log line | What it means |
|----------|---------------|
| `generator: registered batch` | the leader produced and registered a new master batch |
| `generator: skipping batch (not leader or cluster error)` | this node is not the leader, so it produced no batch |
| `cluster has leader` | the Raft cluster elected a leader and committed |
| per-system lines with `node_id`, `stage`, `system` | the pipeline executing on the claiming node |

Every node processes the batches it claims and checkpoints after stage 2, so
a node that dies mid-batch resumes from its last checkpoint rather than
restarting the row range.

## Files

| File / directory | What it is |
|------------------|------------|
| `main.rs` | the example binary: CLI, Raft node, `DistributedRunner` |
| `systems.rs` | the four-stage pipeline and its seven systems |
| `resources.rs` | the non-columnar resources: `FxRateTable`, `TaxRateTable`, `InventoryCatalog`, `NodeId` |
| `components.rs` | the `Order` and `Invoice` component definitions |
| `store.rs` | `FulfillmentStore`, the checkpoint and partition store wrapper |
| `generator.rs` | the synthetic `Order` batch generator |
| `docker-compose.yml` | the three nodes and their shared data volume |
| `Dockerfile` | the release build that Compose uses |
| `config/` | per-node YAML templates of the same settings |
