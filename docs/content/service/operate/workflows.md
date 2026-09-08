+++
title = "Workflow lifecycle"
description = "Pause, resume, stop, start and restart one workflow or the whole service, from the API or the dashboard."
template = "page.html"
weight = 6
aliases = ["/service/workflow-lifecycle/"]
+++
# Workflow lifecycle

A standalone service runs every declared workflow at once. The lifecycle
endpoints act on one of them, or on all of them together, to park it, drain
it, or build it again while the process stays up.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 216" role="img" aria-labelledby="svc-wl-t svc-wl-d">
        <title id="svc-wl-t">The three states a workflow moves between, and the verb that makes each move</title>
        <desc id="svc-wl-d">
            Three boxes in a row. Running sits in the middle: it is admitting rows.
            Paused sits on the left: the runner is parked, keeping every resource and
            all its in-memory state. Pause moves running to paused and resume moves it
            back. Stopped sits on the right: there is no runner at all, because stop
            drained it and dropped everything it held. Start builds it again and
            returns to running. Restart is stop followed by start, in one request.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="245" y="60" width="170" height="60" rx="8"/>
            <rect class="hd hd-data" x="245" y="60" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="245" y="72" width="170" height="8"/>
            <text class="t-lbl t-data" x="257" y="75">running</text>
            <text class="t-sm" x="257" y="105">admitting rows</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk" x="0" y="60" width="170" height="60" rx="8"/>
            <text class="t-lbl" x="12" y="86">paused</text>
            <text class="t-sm" x="12" y="105">parked, state kept</text>
            <path class="arw arw-ctl" d="M241 78 H178" marker-end="url(#svc-wl-c)"/>
            <text class="t-sm t-ctl" x="192" y="70">pause</text>
            <path class="arw arw-ctl" d="M174 102 H237" marker-end="url(#svc-wl-c)"/>
            <text class="t-sm t-ctl" x="186" y="120">resume</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk" x="490" y="60" width="170" height="60" rx="8"/>
            <text class="t-lbl" x="502" y="86">stopped</text>
            <text class="t-sm" x="502" y="105">no runner</text>
            <path class="arw arw-ctl" d="M419 78 H486" marker-end="url(#svc-wl-c)"/>
            <text class="t-sm t-ctl" x="440" y="70">stop</text>
            <path class="arw arw-ctl" d="M486 102 H423" marker-end="url(#svc-wl-c)"/>
            <text class="t-sm t-ctl" x="436" y="120">start</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 156 H654"/>
            <text class="t-sm" x="0" y="176">restart is stop then start, in one request. Stop drains the runner and finishes every sink first.</text>
            <text class="t-sm" x="0" y="200">Desired state is process local: a restarted service starts every workflow as configured.</text>
        </g>
        <defs>
            <marker id="svc-wl-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the runner, doing work</span>
        <span class="k-control"><i></i> the lifecycle verb that moves it</span>
    </div>
</div>

## 1. Read what is running

`GET /api/workflows` lists every controllable workflow in declaration order.

Linux/macOS:

```bash,name=List the workflows
curl -s http://localhost:8080/api/workflows | jq

[
  {
    "id": "orders",
    "name": "Orders",
    "state": "running",
    "since_unix_ms": 1761000000000,
    "runs": 1,
    "restartable": true
  }
]
```

Windows (PowerShell):

```powershell
Invoke-RestMethod http://localhost:8080/api/workflows | ConvertTo-Json -Depth 5
```

`GET /api/workflows/orders` returns that one entry, and `404` for an id no
config declares. `runs` counts the runners started for this workflow, so a
restart takes it to `2`. `saci_workflow_runs_total` counts passes instead and a
restart leaves it where it was.

## 2. Pause and resume one workflow

A paused workflow keeps its runner. Staged batches, carry-over slices, flow
controllers and window watermarks all survive, and nothing is admitted while it
is parked.

Linux/macOS:

```bash,name=Park a workflow, then release it
curl -s -X POST http://localhost:8080/api/workflows/orders/pause
curl -s -X POST http://localhost:8080/api/workflows/orders/resume
```

Windows (PowerShell):

```powershell
Invoke-RestMethod -Method Post http://localhost:8080/api/workflows/orders/pause
Invoke-RestMethod -Method Post http://localhost:8080/api/workflows/orders/resume
```

The runner parks between passes, so `pause` waits out the pass in flight and
answers `paused`. It answers `pausing` only when that pass outlasts the 5
second budget in the status codes below. `GET /status` reports the same state
beside that workflow's counters, and its `iterations` stops climbing.

In `run_mode kind="stream"` the park happens between items, so a pause settles
when the next message arrives. A source that is idle keeps the workflow at
`pausing` until it produces something.

**A pause loses nothing.** Kafka's offset commit, a JetStream acknowledgement
and a PostgreSQL replication slot's confirmed position all advance at the head
of a *later* call on the source, never when a batch is handed to the workflow.
A paused runner still holds that source object, so the call that acknowledges
the last batch happens as soon as it resumes.

## 3. Stop, start and restart one workflow

`stop` cancels the runner, which flushes every staged batch, calls `finish()`
on every sink and reports where flow control ended, then drops the built
workflow and everything its connectors hold. `start` builds it again from the
config already in memory. `restart` is the two in one request.

Linux/macOS:

```bash,name=Rebuild one workflow
curl -s -X POST http://localhost:8080/api/workflows/orders/restart | jq '.state, .runs'

"running"
2
```

Windows (PowerShell):

```powershell
Invoke-RestMethod -Method Post http://localhost:8080/api/workflows/orders/restart |
  Select-Object state, runs
```

A new runner counts from zero, so `/status` reports that workflow's
`iterations` and `rows_processed` from the start of the current run. `runs` is
what survives. The config file is not re-read, so a change on disk needs a
service restart.

**A stop redelivers the last in-flight batch.** Dropping the built workflow
drops its connectors, so the call that would have acknowledged the batch
already processed never comes. The broker or the cursor replays it after
`start`, which makes delivery across a rebuild at least once. A sink that
cannot absorb a repeat needs an idempotent write, such as
`write_mode "upsert"` on a PostgreSQL sink, or a consumer that deduplicates on
a key the rows carry.

`examples/integrity/` proves both halves of that. It publishes a known
workload, pauses one workflow while publishing continues, and stops and starts
another. The run fails if a single row is missing or arrives with different
values than it was sent with.

## 4. The whole service at once

`POST /api/service/{verb}` applies the same five verbs to every controllable
workflow. Each workflow is judged on its own state, so a verb some of them
cannot take still moves the rest.

Linux/macOS:

```bash,name=Park the whole service
curl -s -X POST http://localhost:8080/api/service/pause | jq '{settled, paused: [.applied[].state], refused}'

{
  "settled": true,
  "paused": [ "paused", "paused" ],
  "refused": []
}
```

Windows (PowerShell):

```powershell
Invoke-RestMethod -Method Post http://localhost:8080/api/service/pause |
  Select-Object settled, applied, refused
```

`applied` carries the resulting status of every workflow the verb reached, and
`refused` names the ones it could not, each with the reason the per-workflow
endpoint would have given. A partial sweep is a success: only a verb that
reached nothing answers `409`.

Stopping every workflow does not stop the service. The process stays up, the
control plane keeps answering, and `POST /api/service/start` brings them all
back. `Ctrl-C` is still what ends the process, draining whatever is running.

## 5. What cannot be rebuilt

Two shapes hold a resource created once per process, so tearing them down would
leave nothing to build again. Such a workflow reports `"restartable": false`
with a `restart_blocked_reason`, answers `409` to `start`, `stop` and
`restart`, and still pauses and resumes.

| Shape | Why |
|---|---|
| A `wasm` node with no `module`, or a `plugin` node with no `library` | Its runtime came from `ServiceBuilder::with_runtime` and the build consumed it |
| A `ChannelSource` or `ChannelSink` | It is bound to one in-process channel, and its sink is the only sender the consumer's end of file depends on |

`stop` is refused too, deliberately, because stopping a workflow that cannot
be started again would leave no way to bring it back.

## 6. Status codes

| Code | Means |
|---|---|
| `200` | The transition settled, and the body is the resulting status |
| `202` | Accepted, still in progress after 5 seconds; poll `GET /api/workflows` |
| `404` | No workflow with that id, or the control plane is not mounted |
| `409` | The verb is illegal from the current state, or the workflow cannot be rebuilt. For `/api/service/`, that no workflow at all accepted it |
| `503` | The workflow's supervisor is gone, which a completed workflow leaves behind |

A verb that is already satisfied is `200` with the status unchanged: `pause` on
a paused workflow, `stop` on a stopped one.

A workflow that finished its own work reports `completed`. Its supervisor has
returned, which is what lets a `one_shot` service exit, so `start` and
`restart` answer `503` rather than building it again. Bringing it back needs a
service restart.

## 7. The dashboard

The Pipelines tab carries the same verbs. One bar above the cards acts on every
workflow, and each card's header carries its own state badge, its start count
and its own buttons.

<img src="../../../dashboard/lifecycle.png" alt="The service-wide bar reads All workflows, 2 of 2 running, with pause all, stop all and restart all buttons; the orders card header below it carries its pass count, a running badge, a start count and its own pause, stop and restart buttons.">

A verb the service would refuse is disabled, and its tooltip says why. The
buttons are absent when the control plane is not mounted.

## 8. Turn it off

The endpoints are on by default, on the same unauthenticated port that already
serves the topology, the connector options and the log tail.

```kdl,name=No lifecycle endpoints
http bind="0.0.0.0:8080" control=#false
```

`control=#false` drops the routes from the router, so they answer `404` rather
than a refusal. Cluster mode never mounts them: it declares exactly one
workflow, and stopping that is stopping the node.
