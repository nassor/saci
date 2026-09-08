+++
title = "The live dashboard"
description = "The /ui dashboard: enable it, start the service, open it, and what each of the four tabs shows."
template = "page.html"
weight = 2
aliases = ["/service/dashboard/"]
+++
# The live dashboard

`saci-service` serves a dashboard at `/ui`, on the same port as the rest of the
control plane. It reads buffers the process already keeps in memory, so nothing
else has to be running. It draws a live graph of your workflow with a per-node
detail sheet, and serves the same numbers over HTTP.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 172" role="img" aria-labelledby="svc-dash-t svc-dash-d">
        <title id="svc-dash-t">The run loop fills in-memory buffers, the JSON API reads them, and the dashboard polls it</title>
        <desc id="svc-dash-d">
            The run loop on the left records spans, log events, metric samples and
            flow-control decisions into four in-memory ring buffers inside the
            saci-service process. The JSON API under /api reads those buffers and returns
            them without mutating anything. The dashboard at /ui, running in a browser,
            polls that API. No collector and no database sit anywhere in the path.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="44" width="130" height="60" rx="8"/>
            <rect class="hd hd-data" x="0" y="44" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="130" height="8"/>
            <text class="t-lbl t-data" x="12" y="59">run loop</text>
            <text class="t-sm" x="12" y="82">records as it goes</text>
            <path class="arw arw-data" d="M130 74 H166" marker-end="url(#svc-dash-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="170" y="44" width="160" height="60" rx="8"/>
            <rect class="hd hd-ctl" x="170" y="44" width="160" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="170" y="56" width="160" height="8"/>
            <text class="t-lbl t-ctl" x="182" y="59">four ring buffers</text>
            <text class="t-sm" x="182" y="82">spans, logs, samples,</text>
            <text class="t-sm" x="182" y="98">flow decisions</text>
            <path class="arw arw-ctl" d="M330 74 H366" marker-end="url(#svc-dash-c)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="370" y="44" width="140" height="60" rx="8"/>
            <rect class="hd hd-ctl" x="370" y="44" width="140" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="370" y="56" width="140" height="8"/>
            <text class="t-lbl t-ctl" x="382" y="59">GET /api/*</text>
            <text class="t-sm" x="382" y="82">read only, never</text>
            <text class="t-sm" x="382" y="98">blocks a pipeline</text>
            <path class="arw arw-ctl" d="M510 74 H546" marker-end="url(#svc-dash-c)"/>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="550" y="44" width="110" height="60" rx="8"/>
            <text class="t-lbl t-ctl" x="562" y="68">/ui</text>
            <text class="t-sm" x="562" y="88">in a browser</text>
            <path class="ln" d="M0 130 H654"/>
            <text class="t-sm" x="0" y="152">No collector, no database, no websocket: the tabs poll the same JSON you can curl.</text>
        </g>
        <defs>
            <marker id="svc-dash-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="svc-dash-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the data plane</span>
        <span class="k-control"><i></i> the control plane</span>
    </div>
</div>

## 1. Enable the inspector

Capture, the JSON API and the dashboard all switch on one key:
`observability.inspector.enabled`. It defaults to `#true`, so the dashboard is
on unless the config says otherwise. State it explicitly to make a
deployment's intent visible:

```kdl,name=The inspector block, with its defaults
observability {
    inspector {
        enabled #true            // #false: no capture, no /api/*, no /ui
        ui #true                 // #false: keeps /api/*, drops /ui
        retention_secs 3600
        sample_interval_secs 1
        max_spans 10000
        max_logs 10000
        max_samples 3600
        max_flow_decisions 1000
    }
}
```

Every key has a default, so the whole block may be omitted or given one key.
`ui=#false` drops `/ui` and its assets while keeping the JSON API, for a
service scraped by something else. `enabled=#false` drops both, and the dropped
routes answer 404 rather than 403. Confirm which way a running process is set:

```bash,name=Check whether capture is on
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/api/snapshot
200
```

Windows (PowerShell):

```powershell
(Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/snapshot).StatusCode
200
```

The four caps size four in-memory buffers, and each is bounded twice: by
`retention_secs` and by its own entry count.

| Buffer | Key | Default | Filled by |
|---|---|---|---|
| spans | `max_spans` | 10 000 | each span that closes |
| log events | `max_logs` | 10 000 | each log record |
| metric samples | `max_samples` | 3 600 | one sample per series per `sample_interval_secs` |
| flow decisions | `max_flow_decisions` | 1 000 | one entry per flow-control episode a source went through |

Time evictions run first on every push, then capacity ones. A capacity
eviction is counted and reported as `buffers.dropped`, so an undersized buffer
shows up in that number.

## 2. Start the service

Any config and any run mode serve the dashboard.
`examples/windowing/tumbling/tumbling.kdl` runs with the inspector on. Its NATS
sources, its PostgreSQL sinks and its `plugin` node all sit outside the default
build, so build with `--features connector-nats,connector-postgresql,plugin` or
point at your own file:

```bash,name=Start the service
saci-service serve --config examples/windowing/tumbling/tumbling.kdl
```

Runs the same on Linux, macOS and Windows (PowerShell). That config binds
`127.0.0.1:8080`, so the startup banner prints:

```text
saci-service listening on 127.0.0.1:8080
dashboard at http://127.0.0.1:8080/ui
```

## 3. Open the dashboard

The control plane binds `http.bind` in the config, `0.0.0.0:8080` by default.
Open the address a browser can reach. `curl` answers before a browser does:

```bash,name=The dashboard answers
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/ui
200
```

Windows (PowerShell):

```powershell
(Invoke-WebRequest -UseBasicParsing http://localhost:8080/ui).StatusCode
200
```

Then open http://localhost:8080/ui in a browser.

## 4. Read the four tabs

A fixed left rail carries node id, mode, uptime, a ready badge, the workflow
count, the workflow run counter, an error badge and the buffer counters.
Beside it, four tabs.

Each tab is addressed by the URL fragment, so `/ui#pipelines`, `/ui#traces`,
`/ui#logs` and `/ui#dead-letters` open on that tab and a link to one is
shareable.

**Pipelines** draws one card per declared workflow, each card its own animated
SVG holding one box per declared node, in depth columns from the entry nodes
rightwards, laid out independently of the other workflows. Nodes inside a
column are ordered by the average row of the nodes feeding them, which is what
keeps a fan-in from drawing as a braid. A box shows its title, its connector
type and component, the name of the number it is showing, that number, and a
sparkline of the same series. The card keeps its laid-out size and scrolls, so
a six-way fan-out stays readable instead of shrinking to fit. Clicking a box
opens its detail sheet in three sections: identity, live numbers and
configuration, plus the values a processor reported for itself and the retained
events naming that node.

Under the graph, each card carries one table per side and one chart. The
**sources** table gives each source its rows per second, batches per second,
current admission target, smoothed throughput and back-off count. The
**sinks** table gives each sink its rows per second, batches per second and
backlog, whose cell carries an `↑`, `→` or `↓` for the shape of its own
history.

Nothing else in either table plots history. That is the job of the node boxes'
sparklines and the chart below. A node that has recorded nothing shows a dash,
and so does a sink whose connector reports no backlog. `ChannelSink`,
`PostgresSink` and `S3Sink` report one, while `FileSink`, `HttpSink`, Kafka,
NATS and TCP do not. Clicking a row opens the same detail sheet the box does.

<img src="../../../dashboard/pipelines.png" alt="The Pipelines tab draws the multi-workflow example as one card per workflow, with a channel-bridges card below them.">

A workflow card's header carries its own run and error badges, read under
`workflow="<id>"`. A `channel bridges` card below the workflows lists every
in-process channel joining a `ChannelSink` to a `ChannelSource` across
workflows, with its live rate.

The same header carries that workflow's lifecycle state, its start count and
its own pause, stop and restart buttons. One bar above the cards acts on every
workflow at once. [Workflow lifecycle](@/service/operate/workflows.md) shows
the buttons and what each one does.

**Traces** lists the retained traces with root span name, start, duration
against a bar scaled to the slowest trace in view, span count and an error
badge. The list sorts newest or slowest and filters by root span name, both
over the fetched window, and the newest trace opens itself so the tab never
lands empty. What a trace is depends on `log_level`, which
[Which spans you will see](#which-spans-you-will-see) below sets out.

Selecting one renders an SVG waterfall, one bar per span, x as the offset from
trace start, over a time axis with tick labels and gridlines. Bars are coloured
by plane, carry a minimum width so a sub-millisecond span stays visible and
hoverable, and report both the span's total and its self time, the part no
retained child accounts for. Every event emitted inside the trace is listed
below it.

**Logs** tails up to a thousand records as aligned monospace columns:
timestamp, level chip, target, and a message that truncates to one line with
its structured fields as `key=value` chips. Clicking a row expands the whole
record, every field included, and pauses the tail; the badge then counts what
arrived while it was paused. Three filters apply to the fetched window without
a round trip: level, a substring over message, target and fields, and a scope
built from the workflow and node values the window carries. The newest four
hundred matching records are drawn and the footer says so, rather than dropping
the rest silently. At the default level this is where a failure is triaged,
because every error record carries `workflow`, the `iteration` it happened on,
and the failing node's own field, `source`, `processor` or `sink`.

<img src="../../../dashboard/logs.png" alt="The Logs tab tails the newest records with a level filter.">

**Dead letters** draws one card per workflow that declares a `dlq` block, with
the store it uses, how many letters and rows are waiting, when the automatic
replay next comes due and what the last replay did. The table below groups the
letters by the sink that refused them and the reason it gave, with the first
and last failure time and the highest replay count in the group. Each row
replays only its own group; the header's buttons replay or discard the whole
queue, and purge takes two clicks because nothing brings a discarded letter
back. A service where no workflow declares a block shows an empty state rather
than an error. [Dead letter queue](@/service/operate/dead-letter-queue.md) is
the block, the replay triggers and the HTTP routes behind this tab.

<img src="../../../dashboard/dead-letters.png" alt="The Dead letters tab shows one card per workflow with a queue, and one table row per sink and failure reason.">

## What windowing looks like

A processor node whose config declares a `window` block carries a `⟐` chip in
the box's top-right corner, `⟐30s` for tumbling, `⟐30s/5s` for sliding and
`⟐gap5s` for session. Its detail sheet then adds a **windowing** section: kind,
geometry, time field, grouping keys, allowed-lateness budget, and the node's
live watermark from `saci_window_watermark_seconds` under `processor="<id>"`,
as UTC wall-clock time. It is blank until the first timestamp arrives, so a
blank one on a busy node means the source is not producing.

A **dropped arrivals** row joins the watermark, in the destructive colour, once
`saci_window_late_arrivals_total` under the same id is above zero. It counts
arrivals whose every row was beyond the allowed lateness, which the node drops
whole. While it climbs, the sinks behind that node receive nothing however
busy the sources look. A healthy node carries no such row.
[Windowing](@/service/processors/windowing/_index.md) has the runnable example that
fills one, and names the two producers of a rewound stream.

<img src="../../../dashboard/windowing.png" alt="The windowed processor box carries the 30-second window chip, and its detail sheet lists the window geometry and the live watermark.">

## Colour and motion

A source box uses the data plane's colour. A sink is the same plane on its
write side and takes the write tint. A processor box uses the WebAssembly
boundary's colour. Control-plane facts, the windowing chip among them, use the
control-plane colour.

Each box also carries a solid accent bar on the side its data crosses the
process boundary: left for a source, right for a sink, both for a processor. A
legend under the graph names every colour, so the vocabulary is readable
without this page.

A sparkline plots the first derivative of a counter or a histogram sum, so its
curve is in the same unit as the number above it. A gauge is plotted as it is.

Edge dashes animate with a period of `clamp(0.25s, 8s / max(rate, 1), 8s)` and
a stroke width scaling with `log10(rate)`, so they speed up with throughput
without going solid. A zero rate is a static dim stroke, and motion stops under
`@media (prefers-reduced-motion: reduce)`.

## What the edge numbers mean

An edge is rated from whichever end of it measures what actually crossed.

| Edge | Reads | Unit |
|---|---|---|
| out of a source | `saci_rows_processed_total` under `source="<id>"` | rows |
| processor to processor | `saci_processor_rows_out_total` under `processor="<id>"` | rows |
| out of a processor, on a labelled link | the same series under `processor="<id>", branch="<name>"` | rows |
| processor to sink | `saci_sink_rows_written_total` under `sink="<id>"` | rows |

A processor-to-sink edge reads the sink's end, because
`saci_processor_rows_out_total` counts every row of the processor's output
dataset while a sink node takes one component. A windowing processor that
returns its input alongside its window totals would otherwise rate that edge
at its input rate beside a sink card reading zero.
`saci_sink_batches_written_total` rates no edge at all, because a batch is
whatever row count the upstream handed over. An edge whose end has not
sampled is omitted rather than shown as zero, and an unchosen branch shows no
number until it carries traffic. Every node writes its own attributed copy, so
two sources feeding one processor rate their two edges separately.

## What the node numbers mean

Each box reads the copy of its series carrying its own id.

| Box | Shows | Traces |
|---|---|---|
| source | records per second from `saci_rows_processed_total` | the same series |
| wasm or plugin processor | mean batch latency from `saci_processor_batch_duration_seconds`, plus a retry badge from `saci_processor_retries_total` | `saci_processor_rows_in_total` |
| a processor supplied from code | `saci_stage_duration_seconds`, which carries no attributes, so every such box shows the same process-wide mean | `saci_workflow_runs_total` under `workflow="<id>"`, its own workflow |
| sink | records per second from `saci_sink_rows_written_total` under `sink="<id>"` | the same series |

No box shows the unattributed copy of a series: that is the process-wide sum a
`/metrics` consumer reads. A sink reads its own two series rather than its
upstream's. Rows written come from `saci_sink_rows_written_total` under
`sink="<id>"`, and the backlog from `saci_sink_pending_rows` under the same id.
The sinks table shows that backlog, and it is absent, not zero, for a
connector that reports none.

## What a flow-control decision carries

A source's admission target is `saci_flow_target_rows` and its smoothed rate is
`saci_flow_throughput_rows_per_second`, both under `source="<id>"`. The snapshot
the dashboard polls adds the moments that target moved. Each episode inside the
requested window is one entry, oldest first, naming the rows either side and
the reason, on the same time axis as the series histories. A guard tripping to
the same size for the same cause pass after pass is one entry, and
`saci_flow_backoff_total` carries the trip count.
[Flow control](@/service/operate/flow-control.md#5-read-its-decisions) lists the
four kinds and their reasons.

## The throughput chart

Each workflow card ends in one chart over the polled window. The amber line is
the workflow's admitted throughput. It sums every one of its sources' row rates
into time buckets rather than by sample index, because the decimated histories
do not carry identical timestamps. A bucket no source sampled breaks the line
instead of reading as zero. The dashed step lines behind it are each source's
admission target on its own right-hand scale, drawn as steps because a target
holds until a decision moves it.

Each vertical line is one flow-control decision, at the moment it landed. Teal
is a target that grew, purple one that shrank, red a guard that backed off or
held at the floor, and a mixed cluster takes the red. Decisions landing within
a few pixels collapse into one line carrying its count, and hovering it shows
every decision in the group: the source, the rows either side, and the reason
verbatim. A workflow whose sources are all pinned with
`flow_control { rows N }`, or whose flow control is disabled, produces no
decisions at all, which is the normal reading of an empty axis.

## Node detail is allowlisted

A connector's `config` table holds `connection.dsn`, passwords and credential
file paths, so the topology copies values through a per-`type` allowlist. A key
outside it is dropped, never masked. A `type` the allowlist does not name gets
no detail at all.

| `type` | Keys shown |
|---|---|
| `NatsSource`, `NatsSink` | `mode.kind`, `mode.stream`, `mode.subject` |
| `PostgresSource` | `mode.kind`, `mode.table` |
| `PostgresSink` | `table`, `write_mode` |
| `TursoSource` | `mode.kind`, `mode.table` |
| `TursoSink` | `table`, `write_mode` |
| `S3Source`, `S3Sink` | `connection.bucket`, `prefix` |
| `FileSource`, `FileSink` | `path` |
| `KafkaSource`, `KafkaSink` | `topic` |
| `tcp` | `bind`, `connect` |
| `ChannelSource`, `ChannelSink` | `name` |

<img src="../../../dashboard/detail.png" alt="A source node's detail sheet lists its allowlisted connector options, its component and its node facts.">

## Polling

The Pipelines tab fetches the snapshot once a second. The Logs tab polls every
1.5 seconds unless the tail is paused, and the Traces tab re-lists every
4 seconds. An open trace's spans are a separate fetch, so a refresh never
disturbs the waterfall. Every timer skips its tick while the document is
hidden. The topology is fetched once, because it does not change while the
process runs.

## Which spans you will see

`log_level="debug"` is what fills the Traces tab with per-item trees.

| Span | Level | Opened per |
|---|---|---|
| `workflow.batch` | `debug` | pass, item or claim |
| `source.drain` | `debug` | source drained in a pass; batch modes only |
| `runtime.run` | `debug` | processor call |
| `sink.write` | `debug` | sink written in a pass |
| `processor.batch` | `debug` | batch inside a wasm or plugin processor |
| `pipeline.run` | `info` | native pipeline run |
| `pipeline.stage` | `info` | stage inside one |
| `system.execute` | `info` | transform inside a stage |
| `task_attempt` | `info`, retries only | retried attempt |

The default `log_level="error"` materialises no span at all, so the tab is
empty until the level is raised. At `log_level="info"` it shows traces rooted
at `pipeline.run` and carries no per-item detail. A workflow made only of
WebAssembly processors or native plugins contributes nothing even there,
because a processor's own spans never reach the host. `log_level="debug"`
restores the full per-item waterfall from `workflow.batch` down to
`processor.batch`.

`task_attempt` is the one `info` span usually absent as well: a first attempt
that succeeds opens no span, so a clean run shows zero of them.

The extra spans cost throughput. On the reference machine, per-item stream
latency measured through the service's own log subscriber is about 4.6 µs at
`info` and about 7.4 µs at `debug`. Raise the level to `debug` while you read a
waterfall, and run at a lower level the rest of the time.

## What a wasm trace shows

One `workflow.batch` root span per iteration, holding one `source.drain` per
source, one `runtime.run` per processor and one `sink.write` per sink, in
topological order. A `stream` run carries no `source.drain`: the poll that
produced the item closes before the root span opens. A `runtime.run` over a
WebAssembly processor holds one `processor.batch` carrying that processor's
rows in and out, its transforms run, its retries and the processor's own wall
time.

A processor's own inner spans never reach the host, so a WebAssembly processor
is one bar rather than a subtree. A processor supplied from code nests those
three levels under `runtime.run`, and they are `info` spans, which is why such
a workflow still traces at the default level.

<img src="../../../dashboard/traces.png" alt="Captured at log_level=debug: the Traces tab lists workflow.batch-rooted traces, and the selected one renders as a waterfall of workflow.batch, runtime.run, processor.batch and sink.write.">

## Read the same data over HTTP

Every tab is a view over five read-only routes, so anything the dashboard
shows can be scripted. The controls on a workflow card are the separate
lifecycle endpoints, on
[Workflow lifecycle](@/service/operate/workflows.md).

| Route | Body |
|---|---|
| `GET /api/topology` | the node, its mode, and the workflow graph |
| `GET /api/snapshot?window_secs=60` | one document with series, edge rates, span statistics, flow-control decisions and buffer occupancy |
| `GET /api/traces?limit=100` | newest-first trace summaries |
| `GET /api/traces/{trace_id}` | the spans and log lines of one trace, 404 once it ages out |
| `GET /api/logs?limit=200&level=warn` | newest-first log records, filtered at or above `level` |

`window_secs` is capped at 24 hours and `limit` at 1000. Every route reads the
buffers and returns. None mutate anything, and none block a pipeline.
`trace_id` and `span_id` are local to this process, so they do not correlate
with a collector's ids.

Linux/macOS:

```bash,name=Read the snapshot by hand
curl -s 'http://localhost:8080/api/snapshot?window_secs=60' \
  | jq '{buffers, edges: [.edges[] | {from, to, rate_per_sec}]}'

{
  "buffers": { "spans": 812, "logs": 4110, "samples": 3600,
               "flow_decisions": 2, "dropped": 0 },
  "edges": [
    { "from": "orders-in", "to": "validate", "rate_per_sec": 12034.5 },
    { "from": "validate", "to": "settle", "rate_per_sec": 12034.5 }
  ]
}
```

Windows (PowerShell):

```powershell
$snap = curl.exe -s 'http://localhost:8080/api/snapshot?window_secs=60' | ConvertFrom-Json
$snap.buffers
$snap.edges | Select-Object from, to, rate_per_sec
```

`rate_per_sec` is the derivative over a trailing window for a counter or a
histogram, and the raw value for a gauge. The window is the newest five sample
intervals, five seconds at the default `sample_interval_secs`. A source's rows
are counted as it admits them, and a sink's when it writes the staged batch at
the end of the pass. A window narrower than a pass therefore reads the two ends
of one flow as different throughputs. Cumulative totals reconcile exactly
either way.

## Next

- [Flow control](@/service/operate/flow-control.md) is the policy behind the
  admission targets and the decision markers on the chart.
- [When it refuses to start, and when it fails](@/service/operate/troubleshooting.md)
  is where the Logs tab points when a counter stops moving.
