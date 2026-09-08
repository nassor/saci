+++
title = "Running a cluster"
description = "Bootstrap three nodes, check them, and add or remove one."
template = "page.html"
weight = 7
+++
# Running a cluster

A cluster runs one processor across several machines and hands each of them a
different slice of the same registered batch. The walkthrough below brings up
three nodes, bootstraps the group from one of them, and leaves `cluster status`
answering on each.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 250" role="img" aria-labelledby="svc-cl-t svc-cl-d">
        <title id="svc-cl-t">Three nodes share one replicated log, and each runs the same processor</title>
        <desc id="svc-cl-d">
            Three saci-service nodes are drawn as control-plane boxes, one on the left, one
            on the right and one below. Each carries the same wasm processor inside it,
            drawn on the WebAssembly boundary. All three point at one shared replicated
            log in the middle, which holds the registered batches, the row-range claims and
            the checkpoints. A node claims a row range from that log, runs the processor
            over it, and writes back a checkpoint, so no node reads its input from a source
            node.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="30" width="170" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="30" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="42" width="170" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="45">node id=1</text>
            <rect class="blk blk-bnd" x="10" y="60" width="150" height="36" rx="6"/>
            <text class="t-sm t-bnd" x="20" y="83">wasm settle</text>
            <path class="arw arw-ctl" d="M170 68 H246" marker-end="url(#svc-cl-c)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="490" y="30" width="170" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="490" y="30" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="490" y="42" width="170" height="8"/>
            <text class="t-lbl t-ctl" x="502" y="45">node id=2</text>
            <rect class="blk blk-bnd" x="500" y="60" width="150" height="36" rx="6"/>
            <text class="t-sm t-bnd" x="510" y="83">wasm settle</text>
            <path class="arw arw-ctl" d="M490 68 H414" marker-end="url(#svc-cl-c)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="250" y="30" width="160" height="76" rx="8"/>
            <rect class="hd hd-data" x="250" y="30" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="250" y="42" width="160" height="8"/>
            <text class="t-lbl t-data" x="262" y="45">replicated log</text>
            <text class="t-sm" x="262" y="68">registered batches</text>
            <text class="t-sm" x="262" y="84">claims, checkpoints</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="250" y="146" width="170" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="250" y="146" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="250" y="158" width="170" height="8"/>
            <text class="t-lbl t-ctl" x="262" y="161">node id=3</text>
            <rect class="blk blk-bnd" x="260" y="176" width="150" height="36" rx="6"/>
            <text class="t-sm t-bnd" x="270" y="199">wasm settle</text>
            <path class="arw arw-ctl" d="M335 146 V110" marker-end="url(#svc-cl-c)"/>
            <text class="t-sm" x="0" y="170">A producer registers</text>
            <text class="t-sm" x="0" y="186">one batch. Each node</text>
            <text class="t-sm" x="0" y="202">claims a row range</text>
            <text class="t-sm" x="0" y="218">and acks it when done.</text>
        </g>
        <defs>
            <marker id="svc-cl-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> the nodes and their coordination</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
    </div>
</div>

## 1. What a cluster runs

A cluster workflow declares exactly one `wasm` or `plugin` node and nothing
else: no `source`, no `sink`, no `link` and no `window` block. Work arrives
because a producer registers a batch with the cluster, and each node then
claims the next unprocessed row range of it, runs the processor over that
range, and acknowledges it.

<div class="note">
<span class="note-label">Constraint</span>
<p>
The refusal is at config-validation time, before anything starts. A cluster node
pulls its input from the shared log rather than reading a source, so a declared
<code>source</code> would be silently ignored and the workflow would sit idle.
Register batches from a producer instead.
</p>
</div>

Cluster mode is outside the default build. Build it with
`--features service-cluster`: [Install saci-service](@/service/install.md).
[Distributed processing](@/library/distributed.md) in the Library area covers
the claim loop, the leases and the checkpoints.

## 2. Write the cluster config

`mode "cluster"` changes the top of the document: drop `run_mode`, add the
timings and the peer list. There is no `store` block, because cluster state
lives in `node.data_dir`, and no `flow_control` block, because there is no
source to pace.

```kdl,name=The cluster header
mode "cluster"
bootstrap #true              // true on exactly one node, on first bring-up only
lease_ttl_ms 30000
election_timeout_ms 1500
heartbeat_interval_ms 300
snapshot_log_interval 10000  // snapshot, and purge behind it, every 10 000 entries

// id is stable across restarts. data_dir is where this node's own state lives,
// and it must be non-empty.
node id=1 name="saci-1" data_dir="/var/lib/saci/data"

// Every member, including this node. node.id must appear here.
// addr is the coordination transport address, not the HTTP control-plane port.
peer id=1 addr="10.0.0.1:9000"
peer id=2 addr="10.0.0.2:9000"
peer id=3 addr="10.0.0.3:9000"

workflow "settle" {
    wasm "settle" module="pipelines/settle.wasm" {
        config fx_eur="1.08"
    }
}

http bind="0.0.0.0:8080"
```

| Key | Default | Meaning |
|---|---|---|
| `bootstrap` | `#false` | create a fresh cluster here when `data_dir` is empty; `#false` on a node joining an existing one |
| `lease_ttl_ms` | 30000 | how long a claimed row range is held before another node may reclaim it |
| `election_timeout_ms` | 1500 | the floor of the randomised election timeout; the ceiling is twice it |
| `heartbeat_interval_ms` | 300 | heartbeat interval in milliseconds |
| `snapshot_log_interval` | 10000 | snapshot the state machine, and purge the log behind it, every N committed entries; must be at least 1 |

`lease_ttl_ms` must be at least three election timeouts, or a leader change
alone would let a second node claim a range that is still being processed.
Peers must be unique, at least one, and include `node.id`. Cluster mode
declares exactly one workflow.

`validate` confirms the header:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service validate --config node1.kdl
```

```text
OK: config is structurally valid
  node.id:  1
  mode:     cluster
  workflow: settle
```

## 3. Bootstrap three nodes

Three machines at `10.0.0.1` to `10.0.0.3`, coordination on port 9000 and HTTP
on port 8080, a binary built with `--features service-cluster`, and an empty
`node.data_dir` on each machine. No external service is involved.
`examples/configs/cluster.kdl` is the template.

**Prepare the data directories**, on all three nodes:

Linux/macOS:

```bash
mkdir -p /var/lib/saci/data
```

Windows (PowerShell):

```powershell
New-Item -ItemType Directory -Force -Path C:\saci\data
```

Every `SACI_*` variable below is an alternative to editing the file, so one
config can serve all three machines:

Linux/macOS:

```bash
export SACI_NODE_ID=1
export SACI_DATA_DIR=/var/lib/saci/data
export SACI_BOOTSTRAP=true
```

Windows (PowerShell):

```powershell
$env:SACI_NODE_ID = "1"
$env:SACI_DATA_DIR = "C:\saci\data"
$env:SACI_BOOTSTRAP = "true"
```

`examples/configs/cluster.kdl` reads all three through `${SACI_NODE_ID}`,
`${SACI_DATA_DIR}` and `${SACI_BOOTSTRAP}` placeholders. `SACI_NODE_ID` is also
the environment form of `--node-id`.

**Pre-flight on node 1**, the one with `bootstrap #true`:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service cluster init --config node1.kdl
```

`cluster init` validates the config, confirms the mode and the bootstrap flag,
and prints instructions. It starts nothing and writes nothing:

```text
OK: config is valid and cluster.bootstrap = true
  node.id:  1
  peers:    3

To bootstrap the cluster, start this node with:
  saci-service serve --config node1.kdl

IMPORTANT: run `saci-service serve` on ONE node first. After the leader is
elected, start the remaining nodes with bootstrap: false.
```

**Start node 1:**

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service serve --config node1.kdl
```

It creates its data directory contents while it bootstraps. Confirm them before
starting the other two.

Linux/macOS:

```bash
ls /var/lib/saci/data
```

Windows (PowerShell):

```powershell
Get-ChildItem C:\saci\data | Select-Object -ExpandProperty Name
```

```text
bootstrap.lock
cluster-app.redb
node-id
raft-log.redb
```

**Start nodes 2 and 3**, each with `bootstrap #false` and its own `node.id`:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service serve --config node2.kdl
saci-service serve --config node3.kdl
```

## 4. Check it

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service cluster status --addr http://10.0.0.1:8080
```

```text
node 1  mode=cluster
Note: cluster details are not available in v1. Full Raft metrics integration is planned for v1.1.
```

The three gauges on `/metrics` carry the detail: `saci_raft_term`,
`saci_raft_commit_index` and `saci_raft_leader_id`, which reports `-1` while
there is no leader.

Linux/macOS:

```bash
curl -s http://10.0.0.1:8080/metrics | grep '^saci_raft_'
# saci_raft_term{otel_scope_name="saci"} 2
# saci_raft_commit_index{otel_scope_name="saci"} 148
# saci_raft_leader_id{otel_scope_name="saci"} 1
```

Windows (PowerShell):

```powershell
curl.exe -s http://10.0.0.1:8080/metrics | Select-String '^saci_raft_'
```

Every node reporting leader id `1` means the group agrees on its leader.

## 5. Add or remove a node

`cluster join` and `cluster leave` do not change membership: both print this
procedure and exit 0. Membership is a config edit plus a restart.

**Adding a node**, which is also how a failed one is replaced:

1. Stop the failed node if it is still running.
2. Add a `peer` node for the new member to the config on every surviving node.
3. Write a config for the new node with `bootstrap #false` and an empty
   `data_dir`.
4. Restart every surviving node with the updated config.
5. Start the new node: `saci-service serve --config new-node.kdl`.

**Removing a node:**

1. Stop the node.
2. Remove its `peer` node from every remaining node's config.
3. Restart the remaining nodes.

## 6. Stop it

Ctrl-C, or `SIGTERM` on Linux and macOS, stops a node cleanly: the node
finishes or releases the row range it holds before exiting, and the last line
it prints is `saci-service stopped cleanly`. If it was the leader, a remaining
node calls an election once its own randomised timeout expires, which falls
between one and two times `election_timeout_ms`.

`SIGKILL` bypasses that. The claim's lease expires and another node reclaims
the range, so processing pauses by up to one lease plus one sweep interval for
any range in flight. Nothing is lost.

## What the node keeps on disk

Everything lives under `node.data_dir`, in four files.

- `raft-log.redb` holds this node's own log: its vote, its entries and its view
  of the membership.
- `cluster-app.redb` holds the replicated state: the registered batches, the
  row-range claims, the checkpoints and the per-instance heartbeats.
- `bootstrap.lock` records that a cluster was deliberately created here.
- `node-id` records which node the directory belongs to.

Every node carries a full copy of the replicated state, so a three-node cluster
keeps three copies. Recover a lost node by starting it with an empty data
directory and `bootstrap #false`: the leader refills it. Never copy a data
directory between nodes, because the `node-id` file in it belongs to the node
that wrote it.

`raft-log.redb` grows until the node snapshots and purges the log behind the
snapshot. `snapshot_log_interval` sets that cadence: a snapshot is taken once
that many entries have been committed since the last one. There is no manual
compaction command.

## Runbooks

### One node crashed permanently

1. Stop the failed node if it is still running.
2. Remove its `peer` node from the config on every surviving node.
3. On the replacement machine, write a config with `bootstrap #false`, a `peer`
   node carrying the new address, and an empty `data_dir`.
4. Restart every surviving node with the updated config.
5. Start the replacement. It joins as a follower and the leader replicates the
   state into it.

### The leader is degraded

There is no leadership transfer command. If the leader is unreachable, the
remaining nodes elect a new one automatically once a follower's randomised
election timeout expires, between 1.5 and 3 seconds at the defaults. Restart
the degraded node to trigger a clean election.

### The cluster partitioned

The majority side keeps its leader, or elects one, and keeps claiming ranges.
On the minority side every claim, checkpoint and ack fails after 30 seconds,
in-flight leases expire, and `saci_raft_leader_id` reports `-1`. A node that
restarts into a minority partition does start, settles as a follower, and then
reports the same `-1`.

When the partition heals, the minority-side nodes rejoin and resume claiming.
Ranges whose leases expired return to pending on the next sweep. No manual
action is required.

### `raft-log.redb` is growing

1. Check `saci_raft_commit_index`. If it has stopped advancing, the node is not
   committing and compaction cannot run.
2. Compaction is automatic, paced by `snapshot_log_interval`. Lowering it and
   restarting makes this node snapshot and purge more often, at the cost of
   writing the state machine out more often.
3. Registered batches and checkpoints travel in this log, so split a large
   input across several batches rather than registering one huge one.

## Every key

### node

| Key | Type | Default | What it does |
|---|---|---|---|
| `id` | integer | required | this node's id, stable across restarts, and it must appear in the peer list |
| `name` | string | none | display name on the dashboard and in `/status` |
| `data_dir` | string | required | where this node keeps its four files; must not be empty |

### the cluster header

| Key | Type | Default | What it does |
|---|---|---|---|
| `mode` | string | `"standalone"` | `"cluster"` selects the distributed runner |
| `bootstrap` | boolean | `#false` | create a fresh cluster here when `data_dir` is empty |
| `lease_ttl_ms` | integer | 30000 | how long a claimed row range is held; at least three election timeouts |
| `election_timeout_ms` | integer | 1500 | the floor of each node's randomised election timeout; the ceiling is twice it |
| `heartbeat_interval_ms` | integer | 300 | heartbeat interval in milliseconds |
| `snapshot_log_interval` | integer | 10000 | snapshot the state machine, and purge the log behind it, every N committed entries; must be at least 1 |

### peer

| Key | Type | Default | What it does |
|---|---|---|---|
| `id` | integer | required | that member's node id, unique across the list |
| `addr` | string | required | its coordination address, not its HTTP port |

## When it refuses to start

| Message | What to change |
|---|---|
| ``mode "cluster" does not take a `store` block: cluster state lives in node.data_dir`` | Delete the `store` block. |
| ``mode "cluster" does not take a `flow_control` block: a cluster workflow declares no source node, so there is no admission to govern`` | Delete the `flow_control` block. |
| `cluster mode runs exactly one 'wasm' or 'plugin' node with no source, sink or link (3 node(s), 2 link(s) declared)` | Delete every `source`, `sink` and `link`, and leave one processor node. |
| `cluster mode requires exactly one workflow; found 2` | Keep one `workflow` block. |
| `cluster mode requires at least one peer` | Declare a `peer` node for every member, this node included. |
| `cluster peers contain duplicate id: 2` | Give each `peer` a distinct `id`. |
| `node id 4 is not listed in cluster.peers` | Add a `peer` for this node, or correct `node.id`. |
| `lease_ttl_ms (1000) must be >= 3 × election_timeout_ms (1000) = 3000` | Raise `lease_ttl_ms`, or lower `election_timeout_ms`. |
| `snapshot_log_interval must be at least 1; a zero interval snapshots on every committed entry` | Set an interval of 1 or more; the default is 10000. |
| `data_dir "/var/lib/saci/data" contains 'raft-log.redb' but no 'bootstrap.lock'. This indicates an unclean shutdown before bootstrap completed. Restore from backup or delete the data directory to reinitialise.` | Restore the directory from a backup, or empty it and bootstrap again. |
| `node-id file contains 2 but config has node.id=1. Data directory belongs to a different node. Use the correct data_dir or update node.id.` | Point `data_dir` at this node's own directory, or correct `node.id`. |
| `node.data_dir must not be empty` | Set `data_dir`; a cluster node writes there. |

A `mode "cluster"` config in a binary built without the cluster feature fails at
startup, naming the flag to rebuild with. If coordination does not settle within
30 seconds, `serve` exits 1.

## Next

- [Logs, metrics and traces](@/service/operate/observability.md) is the gauge
  set every runbook above reads.
- [When it refuses to start, and when it fails](@/service/operate/troubleshooting.md)
  covers the refusals a standalone config can hit as well.
