+++
title = "HTTP"
description = "One GET in, one request per batch out. The body is a whole document in whatever format the transformer you name writes."
template = "subpage.html"
weight = 2
aliases = ["/connectors/http/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "Required on both halves"
[[extra.facts]]
label = "Run modes"
value = "Any: one GET is finite, so the source reaches EOF"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
A workflow pulls one document over HTTP, processes the rows, and posts each processed batch to a
second endpoint.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="ht-t ht-d">
        <title id="ht-t">One GET into a workflow, one POST per batch back out</title>
        <desc id="ht-d">A GET url box on the left feeds a source node named orders_in. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named orders_out. The sink sends one POST per batch to the POST url box on the right.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">GET url</text>
            <text class="t-sm" x="10" y="74">one response</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#ht-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">orders_in</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#ht-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#ht-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">orders_out</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#ht-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">POST url</text>
            <text class="t-sm" x="566" y="74">one per batch</text>
        </g>
        <defs>
            <marker id="ht-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> endpoints and nodes</span>
        <span class="k-boundary"><i></i> the processor</span>
    </div>
</div>

## What you need

- An endpoint serving the document the source reads. The example config defaults to
  `http://127.0.0.1:8081/orders.csv`, overridable with `SACI_HTTP_SOURCE_URL`.
- An endpoint accepting the sink's requests. The example config defaults to
  `http://127.0.0.1:8082/ingest`, overridable with `SACI_HTTP_SINK_URL`.
- Nothing extra for `https`. The peer is verified against the machine's trust store, and the
  verification has no off switch.
- The processor component the config names, at `pipelines/orders.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from;
  `validate --connectors-only` below runs without it.

## 1. Declare the format

The body is a whole document in one format, so both halves name a declared `transformer`.

<div class="code">
<div class="code-cap"><span>KDL</span><em>one declared transformer serves the response body and every request body</em></div>

```kdl
transformer "orders_csv" format="csv" {
    options has_headers=#true
}
```

</div>

Any of [csv](@/service/formats/csv.md), [ndjson](@/service/formats/ndjson.md),
[parquet](@/service/formats/parquet.md), [avro](@/service/formats/avro.md) and
[arrow-ipc](@/service/formats/arrow-ipc.md) works. A format that reads its own schema needs
`schema_from "body"`, covered in step 2.

## 2. Read from an endpoint: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>headers is a nested table, one entry per line</em></div>

```kdl
source "orders_in" type="HttpSource" component="Order" transformer="orders_csv" {
    config {
        url "https://data.internal/orders.csv"
        timeout_ms 30000
        headers {
            accept "text/csv"
        }

        schema_fields "id" type="Int64" nullable=#false
    }
}
```

</div>

`url` is the resource to GET, and one GET is the whole stream. `timeout_ms` is the budget for the
whole request, connect through body. `headers` is a table of header name to value, sent on the
request.

`schema_from` decides what the format is handed. `"config"`, the default, hands over
`schema_fields`, which csv requires and ndjson infers without. `"body"` hands over nothing, which is
the only thing parquet, avro and arrow-ipc accept, and the schema the body turns out to carry then
has to equal `schema_fields` field for field. Either way `schema_fields` stays declared, because the
link check between this node and the next one reads it.

## 3. Write to an endpoint: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>one batch is one request and one whole document</em></div>

```kdl
sink "orders_out" type="HttpSink" component="Order" transformer="orders_csv" {
    config {
        url "https://ingest.internal/v1/orders"
        method "POST"
        headers {
            "content-type" "text/csv"
        }

        schema_fields "id" type="Int64" nullable=#false
    }
}
```

</div>

`method` defaults to `POST` and takes any valid HTTP method. `schema_fields` is required here,
because that is the schema each body is written with.

## 4. Validate and run

`examples/configs/http.kdl` declares both nodes. `--connectors-only` builds both halves and skips
the processor module the config names.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>no request is sent: validation is offline</em></div>

```text
saci-service validate --config examples/configs/http.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: orders (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The config sets `run_mode kind="one_shot"`, so the resource is read once:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>one GET, then one request per batch</em></div>

```text
saci-service serve --config examples/configs/http.kdl
```

</div>

The receiving endpoint sees one request per batch, each body a complete document: `csv` repeats its
header row every time, `parquet` and `avro` send one complete container, and `arrow-ipc` one
complete stream.

## How it delivers

Neither half touches the network while the config is built, so `validate` passes against a url that
is down, a host that does not resolve, and a certificate the trust store rejects. All three surface
during `serve`, on the first batch, as `HttpSource: cannot GET {url}` or
`HttpSink: cannot POST {url}`.

The connector holds no connection open across a failure and never reconnects on its own. Whether a
refused request is attempted again is the node's `retry` policy, four attempts with exponential
backoff by default. A retry sends the same body again, so an endpoint that took the first attempt
but failed to answer sees that batch twice.

The sink writes each body from an empty buffer, so nothing accumulates between batches and there is
no flush threshold to configure. The source sends its one GET and never re-fetches, so later
passes of an `interval` or `continuous` run mode admit no rows.

HTTPS verification cannot be turned off. There is no `insecure`, `verify` or `ca_file` key. Roots
come from the machine's own trust store, so a private CA has to be installed there. A self-signed
endpoint on a trusted network is reached over plain `http://` instead.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `url` | string | required | the resource to GET |
| `timeout_ms` | integer | `30000` | budget for the whole request, connect through body |
| `headers` | table | none | header name to value, sent on the request |
| `schema_from` | `"config"` or `"body"` | `"config"` | whether the format is handed `schema_fields` to project onto, or reads the body's own schema and is checked against it |
| `schema_fields` | list of fields | the format decides, required by `schema_from "body"` | the declared column list |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `url` | string | required | the endpoint each batch is sent to |
| `method` | string | `POST` | the HTTP method |
| `timeout_ms` | integer | `30000` | budget for the whole request |
| `headers` | table | none | header name to value, sent on every request |
| `schema_fields` | list of fields | required | the schema each body is written with |

### headers

| Key | Type | Default | What it does |
|---|---|---|---|
| any header name | string | none | one entry per header; the name and the value are both validated when the node is built |

An unrecognised key inside `config` is ignored rather than rejected on this connector.

## When it refuses to start

| Message | What to change |
|---|---|
| `HttpSource config requires a 'url' string field` | add `url "..."` to the source's `config` |
| `HttpSink config requires a 'url' string field` | add `url "..."` to the sink's `config` |
| `HttpSource config.headers must be a table of string values` | write `headers` as a child node of name and value pairs |
| `HttpSink config.headers['{name}'] must be a string` | quote the header value |
| `HttpSource moves bytes and needs a 'transformer' key naming a declared transformer` | add `transformer="<id>"` to the node |
| `HttpSink: '{method}' is not a valid HTTP method: {e}` | use a real method name, such as `POST` or `PUT` |
| `HttpSource: '{name}' is not a valid header name: {e}` | fix the header name |
| `HttpSink: header '{name}' has an invalid value: {e}` | fix the header value |
| `HttpSource config.schema_from must be "config" or "body"` | set `schema_from` to `"config"` or `"body"` |
| `HttpSource: schema_from "body" needs a 'schema_fields' list to check the body's own schema against` | keep `schema_fields` declared even in `"body"` mode |

Two more wait for the first batch rather than the config check:
`HttpSource: body from {url} carries schema [...] but the config declared [...]` means a
`schema_from "body"` source found other columns than the declared ones, and
`parquet: declared column 'X' is not in the file (file columns: ...)` means the default
`schema_from "config"` projected onto a column the body does not carry.

## Next

- [csv](@/service/formats/csv.md), the format this page declares.
- [Sources and sinks](@/service/connectors/_index.md), the other seven transports.
