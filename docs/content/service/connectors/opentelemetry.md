+++
title = "OpenTelemetry"
description = "OTLP/HTTP metrics in and out: a source that listens for exporters or reads a url, a sink that posts to a collector, and the ten-column Arrow view both share."
template = "subpage.html"
weight = 13
aliases = ["/connectors/opentelemetry/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "None: OTLP/HTTP, Protobuf or JSON"
[[extra.facts]]
label = "Run modes"
value = "Stream for a listening source, any for a stop_at_end pull"
[[extra.facts]]
label = "In the default build"
value = "no"
+++
An OpenTelemetry SDK or collector exports metrics to this source, and the sink exports a batch to
another collector. Either half runs alone: the source can feed a file or a database, and the sink
can take rows from anywhere.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 156" role="img" aria-labelledby="ot-t ot-d">
        <title id="ot-t">An OTLP exporter posting metrics through a SACI service to a collector</title>
        <desc id="ot-d">On the left, a box labelled exporter holds an OTLP/HTTP client. An arrow labelled request crosses to the right into a boundary box labelled saci-service, which holds a source node named metrics_in and a sink node named metrics_out joined by an internal link. A second arrow leaves the service box to the right and reaches a box labelled collector, and a short arrow above the first one points back from the service to the exporter carrying the success response.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="52" width="120" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="120" height="16" rx="8"/>
            <rect class="hd hd-data" x="0" y="62" width="120" height="6"/>
            <text class="t-lbl" x="10" y="64">exporter</text>
            <text class="t-sm" x="10" y="86">SDK or collector</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M120 78 H212" marker-end="url(#ot-a)"/>
            <text class="t-sm t-data" x="126" y="70">POST /v1/metrics</text>
            <path class="arw arw-ctl" d="M212 60 H120" marker-end="url(#ot-b)"/>
            <text class="t-sm" x="128" y="48">200 or a refusal</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="220" y="20" width="220" height="116" rx="8"/>
            <rect class="hd hd-bnd" x="220" y="20" width="220" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="220" y="32" width="220" height="6"/>
            <text class="t-lbl" x="230" y="34">saci-service</text>
            <rect class="blk blk-data" x="238" y="52" width="88" height="40" rx="8"/>
            <text class="t-lbl" x="246" y="70">source</text>
            <text class="t-sm" x="246" y="86">metrics_in</text>
            <rect class="blk blk-data" x="336" y="52" width="88" height="40" rx="8"/>
            <text class="t-lbl" x="344" y="70">sink</text>
            <text class="t-sm" x="344" y="86">metrics_out</text>
            <path class="arw arw-data" d="M326 72 H336" marker-end="url(#ot-a)"/>
            <text class="t-sm" x="280" y="122">rows in one workflow</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M440 78 H540" marker-end="url(#ot-a)"/>
            <text class="t-sm t-data" x="446" y="70">one export per batch</text>
            <rect class="blk blk-data" x="548" y="52" width="112" height="52" rx="8"/>
            <rect class="hd hd-data" x="548" y="52" width="112" height="16" rx="8"/>
            <rect class="hd hd-data" x="548" y="62" width="112" height="6"/>
            <text class="t-lbl" x="558" y="64">collector</text>
            <text class="t-sm" x="558" y="86">or a backend</text>
        </g>
        <defs>
            <marker id="ot-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="ot-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--ctl-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> metrics and requests</span>
        <span class="k-control"><i></i> the response</span>
        <span class="k-boundary"><i></i> a service</span>
    </div>
</div>

## What you need

- A free port for the source's `bind`. The listener is opened while the config is built, `validate`
  included, so a busy port fails before the service starts.
- The `connector-opentelemetry` feature on both the `validate` and the `serve` run. It is not in
  the default bundle, because an OpenTelemetry deployment is a deliberate choice of endpoint.
- `run_mode kind="stream"` whenever a listening source is declared. It blocks for the next request
  and never reaches EOF, and validation rejects it in any other run mode. A `stop_at_end` pull
  source reaches EOF after one body and runs in either mode.
- An endpoint the sink can post to for the `serve` step. The sink opens no connection while the
  config is built, so `validate` succeeds whether or not that endpoint is up.
- `schema_fields` on a node only when the pipeline wants columns other than the canonical ten. Both
  ends then declare the same list, because the workflow graph check compares the source's schema
  against the sink's field for field.

No `transformer` node and no `format` key. An OTLP request is the wire format, so there is nothing
for a transformer to decode.

## 1. Receive: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>a listening source needs run_mode kind="stream"</em></div>

```kdl
source "metrics_in" type="OpenTelemetryMetricsSource" component="Metric" {
    config {
        bind "0.0.0.0:4318"
        path "/v1/metrics"
        auth {
            bearer "${SACI_OTLP_TOKEN:-dev-token}"
        }
        buffer 64
        max_body_bytes 16777216
    }
}
```

</div>

`bind` defaults to `127.0.0.1:4318`, the standard OTLP/HTTP port, and `path` to `/v1/metrics`, so
an SDK or collector needs no configuration of its own to reach a listener that names neither.
`validate` binds the address, so it must be free before the service starts.

`auth { bearer }` makes every request present `authorization: Bearer <token>`, compared in constant
time with the scheme read without regard to case. `buffer` is how many requests queue before one in
flight waits for its response, which is how backpressure reaches the sender. `max_body_bytes` caps
one body, announced or discovered.

One listener serves HTTP/1.1 and HTTP/2 on that port, since the HTTP/2 connection preface is
sniffed. There is no TLS block: an OTLP/HTTP exporter that needs TLS posts to a TLS terminator in
front of this listener.

## 2. A url instead of a listener

`mode kind="pull"` reads a url that answers one OTLP body rather than waiting for a request to
arrive. Both modes build the same rows.

<div class="code">
<div class="code-cap"><span>KDL</span><em>stop_at_end is what lets the batch runner drive it</em></div>

```kdl
source "metrics_in" type="OpenTelemetryMetricsSource" component="Metric" {
    config {
        mode kind="pull" url="http://127.0.0.1:8888/v1/metrics" stop_at_end=#true
        buffer 64
    }
}
```

</div>

`interval_ms` sets the gap between reads and defaults to `10000`; it is ignored with
`stop_at_end`, which reads once and reaches EOF. `headers` sends the same plain header pairs on
every read. A repeating pull is live and needs the stream runner, exactly as the listening mode
does.

`bind`, `path`, `auth`, `buffer` and `max_body_bytes` are the shorthand for the listening mode, and
declaring one beside a `mode` block is a configuration error: the block sets those keys for
whichever kind it names.

## 3. Export: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>a base url gains the standard path</em></div>

```kdl
sink "metrics_out" type="OpenTelemetryMetricsSink" component="Metric" {
    config {
        url "http://collector.internal:4318"
        encoding "protobuf"
        service_name "checkout"
        batch_rows 8192
        timeout_ms 10000
        headers {
            authorization "Bearer ${SACI_OTLP_ENDPOINT_TOKEN:-}"
        }
    }
}
```

</div>

`url` takes an endpoint in either form: a base url gains `/v1/metrics`, and one that already names
the path keeps it. `encoding` is `"protobuf"` (the default, and what an empty `content-type` means)
or `"json"`, and the body and its `content-type` follow it.

Rows are grouped into one resource per distinct `resource_attributes`, one scope per distinct
scope, and one metric per distinct name, kind, unit, description, temporality and monotonic flag. A
batch larger than `batch_rows` goes out as several requests, so one export stays inside a
collector's body limit. `metric_name` names the metric for a batch whose name column is empty, and
defaults to `saci_metric`.

## 4. Validate and run

`examples/configs/opentelemetry_metrics.kdl` declares a listening source and a sink and sets
`run_mode kind="stream"`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>this binds the source's port, so it must be free</em></div>

```text
saci-service validate --config examples/configs/opentelemetry_metrics.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: metrics (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The process stays up, accepting OTLP/HTTP requests on `127.0.0.1:4318` and posting
each batch to `http://127.0.0.1:4319/v1/metrics`:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>both endpoints are overridable</em></div>

```bash
export SACI_OTLP_BIND='127.0.0.1:4318'
export SACI_OTLP_ENDPOINT='http://127.0.0.1:4319'
saci-service serve --config examples/configs/opentelemetry_metrics.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same three steps</em></div>

```powershell
$env:SACI_OTLP_BIND = "127.0.0.1:4318"
$env:SACI_OTLP_ENDPOINT = "http://127.0.0.1:4319"
saci-service serve --config examples/configs/opentelemetry_metrics.kdl
```

</div>

A first metric that reaches the collector proves all three hops: the exporter, this service, and
the endpoint it forwards to.

## What one row is

A source with no `schema_fields` emits the canonical schema, one row per OTLP data point, so a
metric with three data points is three rows and the metric's own name, unit and description repeat
on each.

| Column | Type | Null | What it holds |
|---|---|---|---|
| `name` | Utf8 | no | the metric's name |
| `value` | Float64 | no | a gauge's or sum's value, a histogram's or exponential histogram's sum, a summary's sum |
| `timestamp` | UInt64 | no | the data point's time, in Unix nanoseconds |
| `kind` | Utf8 | no | `gauge`, `sum`, `histogram`, `exponential_histogram` or `summary` |
| `unit` | Utf8 | yes | the metric's unit |
| `description` | Utf8 | yes | the metric's description |
| `attributes` | Utf8 | yes | the data point's attributes as a JSON object |
| `resource_attributes` | Utf8 | yes | the resource's attributes as a JSON object |
| `count` | UInt64 | yes | a histogram's, exponential histogram's or summary's count |
| `start_timestamp` | UInt64 | yes | the data point's start time, in Unix nanoseconds |

`schema_fields` replaces that list with the columns a pipeline declares, and the nodes on either
side of a `link` must declare the same list: the graph check compares the source's schema against
the sink's, field for field. A sink needs no declaration to read a batch, because the conversion
reads the batch's own columns by name; declaring one is how the check is satisfied. Each declared
column is
filled from the canonical column of the same name where one exists, and from the attribute of that
name otherwise. The metric value goes to a column named `value` when the schema declares one, and
otherwise to the first numeric column no canonical name claims, which is why a schema of `id`,
`label` and `total` reads the value from `id`. A column the request has no value for is null, and a
request that leaves a column declared `nullable=#false` without one is refused with a 400 naming
the column.

The kind column is what decides the OTLP data variant a sink writes back. A `sum` row is exported
as a cumulative sum that may decrease, and a `histogram`, `exponential_histogram` or `summary` row
as the variant of its kind.

## Attributes and reserved keys

`attributes` and `resource_attributes` are JSON objects, one key per OTLP attribute, each in its
JSON form. A `doubleValue` JSON has no number for travels as the string `"NaN"`, `"Infinity"` or
`"-Infinity"`, and a `bytesValue` as `{"otel.bytes": "<base64>"}`.

Parts of a data point have no canonical column, and travel in `attributes` under keys this
connector owns. A sink consumes each of these when it rebuilds a request, so a user attribute must
not reuse one.

| Key | Value | What it carries |
|---|---|---|
| `otel.scope` | object | the instrumentation scope: `name`, `version`, and an `attributes` object |
| `otel.aggregation_temporality` | string | `cumulative`, `delta` or `unspecified` |
| `otel.monotonic` | bool | a sum's monotonic flag |
| `otel.distribution` | object | the buckets of a histogram or exponential histogram, or a summary's quantiles |
| `otel.bytes` | string | a `bytesValue` attribute, in base64, inside an object of its own |

A `doubleValue` that is not finite is refused on export, because OTLP/JSON has no number for `NaN`
or `Infinity` and this connector will not write a body one of its two encodings cannot carry. A
`u64` beyond `i64::MAX` has no `asInt` form and goes as a double, the one loss the export direction
takes.

Exemplars, `Metric.metadata`, both `schema_url` fields and every `dropped_attributes_count` have no
column and no reserved key, so a data point carries none of them through the Arrow view.

## What the listener answers

| Status | When |
|---|---|
| `200` | the body was taken, whole or with the part no column could hold reported in `partial_success` |
| `400` | the body is not an OTLP request in the encoding its `content-type` names, or no declared column could hold it |
| `401` | `auth { bearer }` is set and the request's token is missing or wrong |
| `404` | another path |
| `405` | another method, with `allow: POST` |
| `413` | the body is over `max_body_bytes`, announced or discovered |
| `415` | the `content-type` is neither `application/x-protobuf` nor `application/json` |
| `503` | the source was dropped before the body could be queued |

A `400` is also handed to the runner as a source error, so a sender posting undecodable bodies
shows up in a run's counters. A body whose data points all convert queues its rows in one send, so
a retry of a request the listener refused duplicates no row.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `bind` | string | `127.0.0.1:4318` | the address the listener accepts requests on, listening mode only |
| `path` | string | `/v1/metrics` | the one path that accepts a body |
| `auth.bearer` | string | none | the token every request must present |
| `buffer` | integer | `64` | requests queued before backpressure |
| `max_body_bytes` | integer | `16777216` | the largest body accepted, 16 MiB by default |
| `mode.kind` | string | `push` | `push` listens, `pull` reads `mode.url` |
| `mode.url` | string | required for pull | the url a body is read from |
| `mode.interval_ms` | integer | `10000` | the gap between pull reads |
| `mode.headers` | table | none | headers every pull read carries |
| `mode.stop_at_end` | bool | `false` | read once and finish instead of repeating |
| `mode.max_body_bytes` | integer | `16777216` | the largest pull body accepted |
| `schema_fields` | list of fields | none | the columns to emit instead of the canonical ten |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `url` | string | required | the endpoint every export is posted to |
| `encoding` | string | `protobuf` | `protobuf` or `json` |
| `headers` | table | none | header pairs every export carries |
| `timeout_ms` | integer | `10000` | how long one export waits for a response |
| `metric_name` | string | `saci_metric` | the name for a batch whose name column is empty |
| `service_name` | string | none | the `service.name` resource attribute every export carries |
| `batch_rows` | integer | `8192` | rows one export carries at most |
| `schema_fields` | list of fields | none | the schema this node declares to the workflow graph instead of the canonical ten |

An unrecognised key inside `config` is a configuration error on this connector, and so is a
shorthand key beside a `mode` block.

## When it refuses to start

| Message | What to change |
|---|---|
| `OpenTelemetryMetricsSource: cannot bind '{bind}': {e}` | free the port, or bind another address |
| `OpenTelemetryMetricsSource: mode.path must start with '/', and it is '{path}'` | make the path absolute |
| `OpenTelemetryMetricsSource: mode.url must start with 'http://' or 'https://'` | give the pull url its scheme |
| `OpenTelemetryMetricsSource: bind, path, auth, buffer and max_body_bytes belong to the push mode, and a 'mode' block sets them for whichever kind it names` | keep one of the two forms |
| `OpenTelemetryMetricsSource: mode.interval_ms must be at least 1 when the pull mode repeats` | raise the interval, or set `stop_at_end=#true` |
| `OpenTelemetryMetricsSource: mode.auth.bearer must not be empty` | name the token, or drop the block |
| `OpenTelemetryMetricsSink: url must start with 'http://' or 'https://'` | give the endpoint its scheme |
| `OpenTelemetryMetricsSink: batch_rows must be at least 1` | raise the count, or drop the key |
| `OpenTelemetryMetricsSink: 'not a name' is not a valid header name` | fix the header pair |
| `source type 'OpenTelemetryMetricsSource' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | set `run_mode kind="stream"`, or make the pull read `stop_at_end` |

## Next

- [HTTP](@/service/connectors/http.md), for endpoints that speak a byte format rather than OTLP.
- [Run modes and persistence](@/service/config/run-modes.md), for the stream mode a listening
  source needs.
- [Observability](@/service/operate/observability.md), for the service's own series, which is where
  the effect of a metrics bridge shows up.
