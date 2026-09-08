+++
title = "Run modes and persistence"
description = "One pass, a paced loop, or a live stream, and what a restart resumes."
template = "page.html"
weight = 2
+++
# Run modes and persistence

`run_mode` decides what happens after a pass finishes: exit, wait, or never
stop. A `store "redb"` block decides what a restart picks up.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 228" role="img" aria-labelledby="svc-rm-t svc-rm-d">
        <title id="svc-rm-t">One pass drains the sources, runs the processors, writes the sinks, then publishes its counters</title>
        <desc id="svc-rm-d">
            A pass walks the workflow once: it drains every source, runs every processor,
            writes every sink, then publishes a counter snapshot that the status endpoint
            reports. A control-plane timer box labelled run_mode sits under the loop and
            feeds the arrow back to the sources: continuous waits 100 milliseconds,
            interval waits interval_ms, one_shot never returns, and stream replaces the
            loop with one pass per arriving chunk.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="44" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="44" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="130" height="8"/>
            <text class="t-lbl t-data" x="12" y="59">sources</text>
            <text class="t-sm" x="12" y="82">drain</text>
            <path class="arw arw-data" d="M130 72 H166" marker-end="url(#svc-rm-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="170" y="44" width="140" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="170" y="44" width="140" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="170" y="56" width="140" height="8"/>
            <text class="t-lbl t-bnd" x="182" y="59">processors</text>
            <text class="t-sm" x="182" y="82">rows in, rows out</text>
            <path class="arw arw-data" d="M310 72 H346" marker-end="url(#svc-rm-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="350" y="44" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="350" y="44" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="350" y="56" width="130" height="8"/>
            <text class="t-lbl t-data" x="362" y="59">sinks</text>
            <text class="t-sm" x="362" y="82">write</text>
            <path class="arw arw-ctl" d="M480 72 H516" marker-end="url(#svc-rm-c)"/>
            <rect class="blk blk-ctl" x="520" y="44" width="140" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="520" y="44" width="140" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="520" y="56" width="140" height="8"/>
            <text class="t-lbl t-ctl" x="532" y="59">/status</text>
            <text class="t-sm" x="532" y="82">counters publish</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="170" y="136" width="310" height="52" rx="8"/>
            <text class="t-lbl t-ctl" x="182" y="158">run_mode</text>
            <text class="t-sm" x="182" y="176">continuous &middot; one_shot &middot; interval &middot; stream</text>
            <path class="arw arw-ctl" d="M590 100 V162 H484" marker-end="url(#svc-rm-c)"/>
            <path class="arw arw-ctl" d="M166 162 H70 V104" marker-end="url(#svc-rm-c)"/>
            <text class="t-sm" x="0" y="216">A pass that ended backlogged re-enters at once, so the wait is an idle cadence.</text>
        </g>
        <defs>
            <marker id="svc-rm-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="svc-rm-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
        <span class="k-control"><i></i> pacing and what it publishes</span>
    </div>
</div>

## 1. Pick a run mode

`run_mode` applies to `mode "standalone"`. A pass checks for cancellation, then
walks every declared node once in topological order, so a node always runs
after every node that links into it. The counter snapshot publishes at the end
of the pass.

| `kind` | Between passes | Extra key | Pick it for |
|---|---|---|---|
| `continuous` | wait 100 ms, walk the graph again. The default. | none | a poller that should keep up with whatever arrives |
| `one_shot` | exit after the first pass | none | a cron entry, a backfill, a test harness |
| `interval` | wait `interval_ms`, then again | `interval_ms` | a scheduled drain of a file, a table or an endpoint |
| `stream` | no passes: one pass per arriving chunk | none | a live source, and any workload with a latency target |

```kdl,name=Run the workflow every five seconds
run_mode kind="interval" interval_ms=5000
```

The wait is skipped when a pass ends backlogged. A pass that spent its
[flow-control](@/service/operate/flow-control.md) admission credit while a
source was still live, or that left a carry-over slice, re-enters at once.
`interval_ms` and the 100 ms `continuous` pause are therefore an idle cadence,
not a throughput cap.

Confirm which mode is running by watching the counters move:

Linux/macOS:

```bash
curl -s http://localhost:8080/status \
  | jq '.standalone[0] | {iterations, rows_processed, iteration_errors}'

{
  "iterations": 41,
  "rows_processed": 148213,
  "iteration_errors": 0
}
```

Windows (PowerShell):

```powershell
curl.exe -s http://localhost:8080/status |
  ConvertFrom-Json |
  Select-Object -ExpandProperty standalone |
  Select-Object -First 1 iterations, rows_processed, iteration_errors
```

`mode "cluster"` reads no `run_mode`, because it runs as fast as work arrives,
and a `run_mode` line beside it is ignored rather than refused.
[Running a cluster](@/service/operate/cluster.md) is that shape.

## 2. Stream mode

`run_mode kind="stream"` replaces the pass loop with a per-item one. Each
arriving batch is sliced at the source's current
[flow-control](@/service/operate/flow-control.md) target, and each slice walks
the graph as its own item before the next one is pulled. Latency is bounded by
the workflow, not by a pacing timer.

```kdl,name=Stream mode
run_mode kind="stream"
```

| | `kind="stream"` |
|---|---|
| **Sources** | At least one, checked at load time. Several are pulled round-robin; an arriving batch is sliced into one item per chunk, except on a path to a windowed node, where one arrival is one item. |
| **Invocation** | One processor call per chunk. A chunk never spans two arrivals, though one arrival can span several chunks. |
| **Sinks** | Written per item. The finish call happens once, at exit. |
| **Processor state** | The checkpoint one item returns is handed back as the next item's prior, one per processor node. |
| **Durability** | At-most-once. A failed item is logged, counted and dropped, and the prior is left at its last good value. |
| **Counters** | `iterations` counts items, and `total_busy_micros` and `max_item_micros` report per-item cost. `/status` refreshes at most every 100 ms. |

Three source types need this mode. `tcp` is always live; a NATS source is live
unless its config sets `stop_at_end #true`, and a Kafka source is live unless
it sets `stop_at_end #true` or `compacted #true`. A live source never reaches
EOF, so any other run mode refuses the config rather than looping on a source
that cannot finish.

The rule does not run the other way. Some sources report end of file at the end
of every drain cycle rather than once: every `PostgresSource` read mode, and
any source given `stop_at_end #true`. The stream runner retires such a source
the first time it says so, and it leaves the rotation for good. A workflow
whose last source has left completes and cannot be started again. A batch mode
reads that same end of file as the end of one cycle and re-enters, so a CDC
read belongs in `continuous` or `interval` and never in `stream`.

`run_mode` is one setting for the whole config, not one per workflow. A
never-EOF source and a cycle-EOF source therefore cannot share a config, and a
service that needs both needs two configs and two processes.
`examples/integrity/` is that shape: `integrity.kdl` carries the Kafka, NATS
and channel workflows in `stream`, and `integrity_audit.kdl` carries the
PostgreSQL `cdc_logical` workflow in `interval`.

## 3. Persist cursors and priors

Without a `store` block the process keeps its cursors and processor state in
memory, so a restart begins from the beginning. One `store "redb"` block moves
them to a local file:

```kdl,name=The store block
store "redb" {
    // Required. The file, created on first use.
    path "/var/lib/saci/state.redb"

    // Carry processor state across continuous, interval and one_shot passes.
    // Default #false.
    batch_resume #true
}
```

The file holds three things: the raw pre-substitution config bytes, one state
blob per processor node, and one cursor per source the stream runner has
drained. `redb` is the only store kind, and any other id fails the parse.

What a restart picks up:

| Run mode | With no `store` block | With `store "redb"` |
|---|---|---|
| `continuous`, `interval` | every source starts from the beginning | every source still starts from the beginning, because no cursor is written in a batch mode; processor state resumes only with `batch_resume #true` |
| `one_shot` | the pass starts from the beginning | the same for the pass, and the same `batch_resume` rule for state |
| `stream` | cursors and state live in memory and are lost | both are written as items flow, whatever `batch_resume` says: a processor resumes from its last checkpoint, and a source resumes wherever its own connector does |

A processor node declaring a `window` block carries its state across passes in
memory whatever `store` says, because that is what makes its accumulator
survive being handed one chunk at a time. `batch_resume` decides only whether
that state is also written to the file.

The file is local and unreplicated, and the block is standalone persistence
only. `mode "cluster"` keeps its state under `node.data_dir` instead and takes
no `store` block.

Confirm the file was created:

Linux/macOS:

```bash
ls -l /var/lib/saci/state.redb
```

Windows (PowerShell):

```powershell
Get-Item C:\saci\state.redb | Select-Object Length, LastWriteTime
```

## Every key

### run_mode

| Key | Type | Default | What it does |
|---|---|---|---|
| `kind` | string | `"continuous"` | `"continuous"`, `"one_shot"`, `"interval"` or `"stream"` |
| `interval_ms` | integer | required with `kind="interval"` | the idle wait between passes in milliseconds; omitting it under that kind fails the parse |

### store

| Key | Type | Default | What it does |
|---|---|---|---|
| leading argument | string | required | the store kind, and `"redb"` is the only one |
| `path` | string | required | the file, created on first use; must not be empty |
| `batch_resume` | boolean | `#false` | carry the processor state across `continuous`, `interval` and `one_shot` passes: read back at startup, written after each pass |

## When it refuses to start

| Message | What to change |
|---|---|
| `source type 'tcp' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | Set `run_mode kind="stream"`, or give that source a config that ends (`stop_at_end #true` on NATS and Kafka). |
| `stream run mode requires at least one 'source' node (0) declared` | Declare a `source` in the workflow, or pick another `kind`. |
| ``mode "cluster" does not take a `store` block: cluster state lives in node.data_dir`` | Delete the `store` block; a cluster node persists under `node.data_dir`. |
| `store redb: path must not be empty` | Give `path` a real file path. |
| `unknown store kind 'sqlite' (expected "redb")` | Change the store's leading argument to `"redb"`. |
| `node.data_dir must not be empty` | Set `data_dir` on the `node` line; it is required even when only the cluster runner writes there. |

## Next

- [The command line](@/service/operate/_index.md) starts the mode you picked,
  and lists the example configs that already use each one.
- [Flow control](@/service/operate/flow-control.md) is what sizes each pass
  inside the mode.
