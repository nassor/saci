+++
title = "Logs, metrics and traces"
description = "Pick the log format and level, probe the four endpoints, scrape the thirty-two series, export spans, and stop the process cleanly."
template = "page.html"
weight = 1
aliases = ["/service/observability/"]
+++
# Logs, metrics and traces

`saci-service` answers four probes on its control plane and keeps its own recent
telemetry in memory. Nothing has to be installed alongside it: no collector, no
scraper, no storage. `log_format` puts the logs in your aggregator's format,
`/health` is the endpoint you alert on, and `/metrics` serves the same numbers
the dashboard shows.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 350" role="img" aria-labelledby="svc-h-title svc-h-desc">
        <title id="svc-h-title">The control plane, the shared state, and the run loop</title>
        <desc id="svc-h-desc">
            Three tokio tasks share one ServiceState. The axum router serves health,
            ready, metrics and status. The run loop drains sources, calls the runtime,
            drains sinks, then publishes a statistics snapshot into the shared state. A
            watchdog task increments a liveness counter once a second, which is what the
            health endpoint reads. Because the router and the run loop are separate tasks,
            all four endpoints answer while a batch is in flight.
        </desc>
        <text class="t-title" x="0" y="14">One process, three tasks</text>
        <text class="t-sm" x="0" y="30">the run loop never touches a socket</text>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="44" width="200" height="160" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="44" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="58" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="59">axum Router</text>
            <rect class="row" x="8" y="74" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="88">GET /health   200 | 503</text>
            <rect class="row" x="8" y="98" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="112">GET /ready    200 | 503</text>
            <rect class="row" x="8" y="122" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="136">GET /metrics  text 0.0.4</text>
            <rect class="row" x="8" y="146" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="160">GET /status   JSON</text>
            <text class="t-sm" x="16" y="184">10 s timeout, then 408</text>
        </g>
        <path class="arw arw-ctl" d="M232 124 H203" marker-end="url(#svc-hc)"/>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="232" y="44" width="200" height="160" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="44" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="58" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="244" y="59">ServiceState</text>
            <text class="t-sm" x="244" y="92">everything an endpoint</text>
            <text class="t-sm" x="244" y="112">answers from</text>
            <text class="t-sm t-data" x="244" y="146">the newest stats snapshot</text>
            <text class="t-sm" x="244" y="184">cloned per request</text>
        </g>
        <path class="arw arw-data" d="M452 124 H435" marker-end="url(#svc-hd)"/>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="452" y="44" width="202" height="160" rx="8"/>
            <rect class="hd hd-data" x="452" y="44" width="202" height="22" rx="8"/>
            <rect class="hd hd-data" x="452" y="58" width="202" height="8"/>
            <text class="t-lbl t-data" x="464" y="59">run loop</text>
            <rect class="row" x="460" y="74" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="88">1  drain sources</text>
            <rect class="row" x="460" y="98" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="112">2  runtime.run_on(data)</text>
            <rect class="row" x="460" y="122" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="136">3  drain sinks</text>
            <rect class="row-w" x="460" y="146" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="160">4  publish stats, clear</text>
            <text class="t-sm" x="468" y="184">5  pace by run_mode</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="232" y="228" width="200" height="48" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="228" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="242" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="244" y="243">watchdog task</text>
            <text class="t-sm" x="244" y="266">liveness += 1 per second</text>
            <path class="arw arw-ctl" d="M332 226 V208" marker-end="url(#svc-hc)"/>
            <text class="t-sm" x="0" y="242">/status reads the snapshot</text>
            <text class="t-sm" x="0" y="256">published at step 4, so it</text>
            <text class="t-sm t-data" x="0" y="270">lags by one batch at most</text>
            <text class="t-sm" x="452" y="242">/health compares it with</text>
            <text class="t-sm" x="452" y="256">uptime and returns 503</text>
            <text class="t-sm t-ctl" x="452" y="270">after 5 s of silence</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 296 H654"/>
            <text class="t-sm" x="0" y="320">serve awaits the runner inline, and races it against the shutdown signal.</text>
        </g>
        <defs>
            <marker id="svc-hc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="svc-hd" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the data plane and what it publishes</span>
        <span class="k-control"><i></i> the control plane</span>
    </div>
    <figcaption class="dgm-cap">
        Every endpoint answers from shared state, never by asking the runner anything. They
        stay responsive under load, and <b>none of them can tell you how the current batch
        is going</b>. <code>/status</code> only moves when an iteration finishes.
    </figcaption>
</div>

## 1. Choose the log format and level

The `observability` block has five scalar keys: two for the log stream, two for
sampling, and one for span export.

```kdl,name=The observability block
observability {
    log_format "pretty"   // use "json" in a log aggregator
    log_level "info"
    sample_ratio 0.5
    error_sample_ratio 1.0
    otlp_endpoint "http://collector:4318"
}
```

| Key | Type | Default | What it does |
|---|---|---|---|
| `log_format` | string | `"pretty"` | `"pretty"` colours the output when stdout is a terminal; `"json"` emits one object per record, for Loki, CloudWatch or Datadog |
| `log_level` | string | `"error"` | any filter directive string, so `"debug"` and `"warn,saci=debug"` are both valid |
| `sample_ratio` | number | `1.0` | fraction of spans and events below ERROR that `log_level` admits and the subscriber keeps, 0.0 to 1.0; a value outside that range fails startup |
| `error_sample_ratio` | number | `1.0` | fraction of ERROR spans and events kept, 0.0 to 1.0; independent of `sample_ratio` |
| `otlp_endpoint` | string | none | collector root for OTLP over HTTP; omit the key and no span exporter is built |

`log_level` becomes the filter
`saci=<log_level>,tower_http=<log_level>,error`, plus three directives the
block never changes: `saci::flow_control=info`, `saci::windowing=warn` and
`saci::heal=warn`.
Directives match by prefix, so the one `saci` directive covers everything this
project logs, the dependency tree is error-only, and the flow-control,
windowing and self-healing lines pass whatever the level says. A set
`RUST_LOG` replaces the level directives; it does not replace those three.

The default is `error`, so a healthy service prints its banner, the
flow-control lines, and nothing else. The windowing target carries one
condition, a windowed node dropping every arrival it is handed, so a healthy
windowed workflow is silent there too:
[Windowing](@/service/processors/windowing/_index.md). No span exists at the default
level either, so `saci_stage_duration_seconds` and the dashboard's per-system
latency need `log_level "info"`.

Sampling is applied once, in the subscriber, so stdout, the `/ui` dashboard
and OTLP export see the same stream. The decision is made per root: a kept
root keeps its whole tree, and a dropped root drops its children with it. An
error inside a dropped trace is still rolled against `error_sample_ratio` and
reaches the Logs tab on its own. Metrics are never sampled.

Three things override the block, in this order: the flag beats the environment
variable, the environment variable beats the config file, and a set `RUST_LOG`
beats all of them.

Linux/macOS:

```bash
# The flag beats the config file
saci-service serve --config standalone.kdl --log-format json --log-level debug

# So does the environment variable
SACI_LOG_LEVEL=debug saci-service serve --config standalone.kdl

# RUST_LOG beats both: everything this project logs at info, the rest quiet
RUST_LOG="warn,saci=info" saci-service serve --config standalone.kdl
```

Windows (PowerShell):

```powershell
saci-service serve --config standalone.kdl --log-format json --log-level debug

$env:SACI_LOG_LEVEL = "debug"
saci-service serve --config standalone.kdl

$env:RUST_LOG = "warn,saci=info"
saci-service serve --config standalone.kdl
```

With `log_format "json"` each record is one object carrying its timestamp,
level, target and fields, which is what an aggregator parses. Either way the
first two lines a healthy start prints are:

```text
saci-service listening on 127.0.0.1:8080
dashboard at http://127.0.0.1:8080/ui
```

`log_level` also decides which spans exist, and those spans are what fill the
dashboard's Traces tab. [The live dashboard](@/service/operate/dashboard.md)
carries the level of every span name.

## 2. Probe it

Four probes answer on `http.bind`, `0.0.0.0:8080` by default, behind a
10 second request timeout.

| Endpoint | Body | Status |
|---|---|---|
| `GET /health` | `{ status, uptime_seconds, liveness_counter }` | 200 while the watchdog counter is within 5 s of uptime; 503 once it falls behind |
| `GET /ready` | `{ status }`, either `ready` or `not_ready` | 200 or 503 |
| `GET /metrics` | Prometheus exposition, version 0.0.4 | Always 200 |
| `GET /status` | `node_id`, `node_name`, `mode`, `uptime_seconds`, `build.version`, plus `standalone` or `cluster` | Always 200. The block that does not match the mode is `null` |

In standalone mode the `standalone` block is an array, one entry per declared
workflow, each carrying `workflow_id`, `iterations`, `rows_processed`,
`source_batches_drained`, `sink_batches_written`, `iteration_errors`,
`total_busy_micros` and `max_item_micros`, plus `state` when the lifecycle
endpoints are mounted; see
[Workflow lifecycle](@/service/operate/workflows.md). `"cluster"` reports
`null` even when the node is clustered; the three Raft gauges on `/metrics`
carry term, commit index and leader instead.

Linux/macOS:

```bash,name=Probing the endpoints
curl -s http://localhost:8080/ready
{"status":"ready"}

curl -s http://localhost:8080/status | jq '.standalone[0].iteration_errors'
0
```

Windows (PowerShell):

```powershell
curl.exe -s http://localhost:8080/ready
curl.exe -s http://localhost:8080/status | ConvertFrom-Json |
  Select-Object -ExpandProperty standalone |
  Select-Object -First 1 -ExpandProperty iteration_errors
```

`/health` is the one to alert on: the counter behind it goes stale within
5 seconds if the main loop wedges, and the endpoint then returns 503.

`/ready` reports the process, not the workflow. It flips once the runner is
spawned, so a 200 means the config loaded, every artifact matched every link
and the control plane is listening. It never means an iteration succeeded: read
`iterations` on `/status` for that.

`http disabled=#true` turns the whole control plane off, the dashboard with it.

<div class="note note-warn">
<span class="note-label">Security</span>
<p>
The control plane has no authentication, no TLS and no rate limiting. Bind it to
a loopback address or an internal network, and put a reverse proxy in front if it
has to be reachable further.
</p>
</div>

## 3. Scrape /metrics

`/metrics` serves Prometheus exposition 0.0.4 on the same port. A series
appears once its writer has recorded a value, so the Raft gauges show up on a
cluster node and the processor series once a processor has run a batch.

Linux/macOS:

```bash,name=Scrape the endpoint
curl -s localhost:8080/metrics | grep '^saci_'
# saci_liveness_counter{otel_scope_name="saci"} 6
# saci_workflow_runs_total{otel_scope_name="saci"} 50
# saci_ready{otel_scope_name="saci"} 1
# saci_uptime_seconds{otel_scope_name="saci"} 5.0141387
```

Windows (PowerShell):

```powershell
curl.exe -s localhost:8080/metrics | Select-String '^saci_'
```

Nineteen of the thirty-two series are written twice, and eighteen of those
pair an attribute-free form with one under the id of the node or workflow that
wrote it. `saci_processor_metric` is the exception: both its writes carry
`metric=` and only the second adds `processor=`. The unattributed value is
already the sum across every node of that kind, so a dashboard adds one form or
the other, never both. Select the empty attribute value, `source=""`,
`processor=""`, `sink=""` or `workflow=""`, for the process-wide number.

## 4. Export spans over OTLP

Set `otlp_endpoint` to a collector root and the process exports its span tree
over OTLP/HTTP. The exporter appends `/v1/traces`, so give it the root and not
the full path. What leaves is what `log_level` admitted and `sample_ratio`
kept, so an export needs a level that materialises spans at all.

```kdl,name=Span export to a local collector
observability {
    log_level "info"
    sample_ratio 0.1
    otlp_endpoint "http://127.0.0.1:4318"
}
```

The same thing without touching the config file, on either platform:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service serve --config standalone.kdl --otlp-endpoint http://127.0.0.1:4318
```

Export covers spans only. Metrics stay pull-only on `/metrics`: there is no
OTLP metrics exporter and no push path. What the spans are built from is on
[Tracing and metrics](@/library/tracing.md).

## 5. Stop it cleanly

Ctrl-C always stops the process, and `SIGTERM` does on Linux and macOS. The
signal cancels every runner, which flushes what it has staged and calls
`finish` on every sink. Each governed source reports where its flow control
ended. Then the HTTP server finishes any in-flight request, the tasks drain,
spans flush, and the last line is:

```text
saci-service stopped cleanly
```

That line is the check: a stop without it was not a drain. It is printed, not
logged, so `log_level` cannot hide it. The two drain stages share one 30
second budget; a process still alive at the end of it exits 1 without printing
the line.

On Windows there is no true `SIGTERM` equivalent, so only Ctrl-C drains. A
service manager that stops the process any other way gets no drain.

## Every series

| Series | Type | Attributes | What moves it |
|---|---|---|---|
| `saci_workflow_runs_total` | counter | `workflow`, plus an unattributed total | one workflow pass, which under an admission credit is one credit-sized admission per source rather than one drain to EOF |
| `saci_workflow_errors_total` | counter | `workflow`, plus an unattributed total | a processor run that failed, plus a lease renewal or a post-run persist failure in cluster mode. Source and sink failures stay on `/status`, so a processor alert is not diluted by them |
| `saci_stage_duration_seconds` | histogram | none | the lifetime of one stage inside a native pipeline |
| `saci_source_batches_drained_total` | counter | `source`, plus an unattributed total | one non-empty source drain, or one claimed partition in a cluster |
| `saci_sink_batches_written_total` | counter | `sink`, plus an unattributed total | one batch written to a sink. A cluster has no sinks, so it stays at zero there |
| `saci_sink_rows_written_total` | counter | `sink`, plus an unattributed total | the rows in each written batch, which is the comparable measure of what a sink is moving |
| `saci_sink_pending_rows` | gauge | `sink` only | one pass per sink that reports a backlog. This is the number flow control reads as congestion |
| `saci_rows_processed_total` | counter | `source`, plus an unattributed total | rows a source admitted into the workflow |
| `saci_liveness_counter` | gauge | none | the watchdog, once a second |
| `saci_ready` | gauge | none | the watchdog: 1 once the runner is spawned |
| `saci_uptime_seconds` | gauge | none | the watchdog |
| `saci_raft_commit_index` | gauge | none | a cluster node, once a second |
| `saci_raft_term` | gauge | none | the same |
| `saci_raft_leader_id` | gauge | none | the same. It reports `-1` when there is no leader, so a lost election shows up instead of a stale id |
| `saci_processor_batch_duration_seconds` | histogram | `processor`, plus an unattributed total | one processor call. This is the dashboard's per-node latency |
| `saci_processor_rows_in_total` | counter | `processor`, plus an unattributed total | the rows one processor call received |
| `saci_processor_rows_out_total` | counter | `processor`, and `branch` on a labelled link, plus an unattributed total | the rows one processor call returned. This is what rates a processor's outbound edge on the dashboard |
| `saci_processor_systems_run_total` | counter | `processor`, plus an unattributed total | the transforms one processor call ran |
| `saci_processor_retries_total` | counter | `processor`, plus an unattributed total | a retry inside one processor call |
| `saci_processor_metric` | histogram | `metric`, and `processor` on the second record | a value the processor itself reported. Distinct names are capped at 256 |
| `saci_window_watermark_seconds` | gauge | `processor` only | one processor call on a node declaring a `window` block: the newest event timestamp it has seen, in epoch seconds |
| `saci_window_late_arrivals_total` | counter | `processor` only | one arrival such a node dropped whole, every row of it beyond `allowed_lateness_ms`. A climbing count with live sources means its sinks are receiving nothing; see [Windowing](@/service/processors/windowing/_index.md) |
| `saci_flow_target_rows` | gauge | `source` only | one completed pass: that source's current admission target |
| `saci_flow_throughput_rows_per_second` | gauge | `source` only | the same cadence: that source's smoothed rate |
| `saci_flow_backoff_total` | counter | `source`, plus an unattributed total | one flow-control guard trip; see [Flow control](@/service/operate/flow-control.md) |
| `saci_connector_heals_total` | counter | `source` or `sink`, plus an unattributed total | one rebuild whose factory returned an instance, counted before that instance has run anything; see [Self-healing](@/service/operate/self-healing.md) |
| `saci_connector_heal_failures_total` | counter | `source` or `sink`, plus an unattributed total | one rebuild the factory refused. A fresh instance that fails its first operation is not counted here; it just schedules the next attempt |
| `saci_dlq_letters_recorded_total` | counter | `sink`, plus an unattributed total | one batch a sink refused that the dead letter queue stored; see [Dead letter queue](@/service/operate/dead-letter-queue.md) |
| `saci_dlq_rows_recorded_total` | counter | `sink`, plus an unattributed total | the rows in each stored batch |
| `saci_dlq_letters_replayed_total` | counter | `sink`, plus an unattributed total | one stored batch a replay delivered to its sink |
| `saci_dlq_letters_lost_total` | counter | `sink`, plus an unattributed total | one letter the store itself would not take. The batch is gone, because nothing is buffered in memory waiting for the store |
| `saci_dlq_letters` | gauge | `workflow` only | after every record and every replay: the letters that workflow is holding |

The dashboard reads these same thirty-two series and introduces none of its
own, so a dashboard number and a `/metrics` number are always the same value.

## What is not there

- **Export covers spans only.** `otlp_endpoint` installs a span exporter.
  Metrics stay pull-only on `/metrics`.
- **`/status` answers `"cluster": null` in cluster mode.** The three
  `saci_raft_*` gauges carry term, commit index and leader instead.
- **`/ready` reports the process, not the workflow.** The flag flips as soon as
  the runner is spawned, so it never means an iteration succeeded.
- **A native plugin's own metric names are log lines.** A plugin's metric call
  emits a trace event and writes no series. Five of the six `saci_processor_*`
  names carry the per-batch numbers the host reports, and a plugin records
  those exactly like a WebAssembly processor. `saci_processor_metric` is the
  sixth, and it stays empty for a plugin, because only a component's
  `host-io::metric` writes it.
- **`saci_stage_duration_seconds` follows the log filter.** It is derived from a
  span, and the filter is process-wide, so a filter that suppresses the
  engine's spans stops the histogram too.
- **An attributed series overlaps its own total.** Summing both forms counts
  every value twice. Select the empty attribute value for the process-wide
  number.

## Next

- [The live dashboard](@/service/operate/dashboard.md) reads all of this in a
  browser, with no scraper installed.
- [When it refuses to start, and when it fails](@/service/operate/troubleshooting.md)
  is where to look when a counter stops moving.
