+++
title = "HTTP"
description = "A document read in, one request per batch out, and a third node type that listens for documents pushed in. HTTP/1.1, HTTP/2 or HTTP/3 on every half."
template = "subpage.html"
weight = 2
aliases = ["/connectors/http/"]
[[extra.facts]]
label = "Direction"
value = "Source, sink and listening source"
[[extra.facts]]
label = "Format"
value = "Required on all three node types"
[[extra.facts]]
label = "Run modes"
value = "Any for a document read; stream for a streaming read and for HttpPushSource"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
A workflow pulls one document over HTTP, processes the rows, and posts each processed batch to a
second endpoint. A third node type listens, so another service can push documents in instead of
serving them.

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
  verification has no off switch. A `tls` block adds a private CA or a client certificate.
- `run_mode kind="stream"` whenever an `HttpPushSource`, a `mode kind="lines"` or a
  `mode kind="events"` source is declared. All three are live and validation rejects any other run
  mode. A document read and the sink carry no such rule.
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

## Which version each request speaks

`http_version` picks the HTTP version a client half speaks. The source and the sink both take it,
and it defaults to `"auto"`.

| Value | What it does |
|---|---|
| `"auto"` | ALPN picks `h2` or `http/1.1` on an `https` url; HTTP/1.1 on an `http` url |
| `"1.1"` | HTTP/1.1, whatever the peer offers |
| `"2"` | HTTP/2 by prior knowledge: h2c on an `http` url, an h2-only ALPN offer on an `https` one |
| `"3"` | HTTP/3 over QUIC, which needs an `https` url |

`"auto"` never reaches HTTP/3. Nothing here reads an `Alt-Svc` advertisement and there is no h2c
upgrade handshake, so HTTP/2 on cleartext and HTTP/3 anywhere are asked for by name. A `"3"` next
to an `http` url is refused while the config is built.

## TLS on the client halves

Both client halves verify the peer against the machine's trust store with no extra keys. A `tls`
block extends that for one node.

<div class="code">
<div class="code-cap"><span>KDL</span><em>a private CA and a client certificate, on the source or the sink</em></div>

```kdl
tls {
    ca_file "/etc/saci/private-ca.pem"
    cert_file "/etc/saci/client.pem"
    key_file "/etc/saci/client.key"
}
```

</div>

`ca_file` adds the certificates in that PEM file as extra roots, so a private CA is trusted for
this node without being installed on the machine. `cert_file` and `key_file` present a client
certificate and go together. Each file is read while the node is built, so a path that does not
exist, or a key file holding no key, fails before the service starts.

## Read one document, or a body that never ends

`mode` picks the read shape, and its absence is `kind="document"`.

<div class="code">
<div class="code-cap"><span>KDL</span><em>a live event stream, filtered to one event name</em></div>

```kdl
mode kind="events" event="order" batch_size=500 poll_timeout_ms=250 reconnect_ms=1000
```

</div>

`kind="document"` reads one finite document. The body spools to a temp file as it arrives, the
format's stream reader decodes it, and the source reports EOF at the end of that stream and
re-arms. The next call issues a fresh GET, which is what makes `interval` re-read the resource on
every tick.

`kind="lines"` reads a body that never ends as newline-delimited records, and `kind="events"` reads
one as `text/event-stream`. Each record goes through the format's message codec. A batch closes at
`batch_size` records or `poll_timeout_ms` after the batch's first record, and a body that does end
is requested again `reconnect_ms` later. `event` narrows an event stream to one event name; without
it every event's data is decoded.

Both streaming kinds are live sources. They never reach EOF, so they need `run_mode kind="stream"`
and validation rejects every other run mode. Both also need `schema_fields`, because the message
decoder is built before the first byte arrives, and both refuse `schema_from "body"`.

`max_body_bytes` caps what a source reads. A document read refuses a `content-length` above the cap
before the body is read and fails a body that grows past it; a streaming read caps one record.
Absent means no cap.

`pagination "link"` and `conditional #true` apply to `kind="document"` only. `pagination "link"`
follows `Link: <...>; rel="next"` from each page and feeds every page's rows through as one read,
reporting EOF once a page carries no next link. `conditional #true` keeps each response's `etag`
and `last-modified` and sends them back as `if-none-match` and `if-modified-since`, so an unchanged
resource answers `304` and the read decodes nothing.

## What a body carries

The sink derives a `content-type` from the format it writes, unless `headers` already names one.

| Format | `content-type` |
|---|---|
| `csv` | `text/csv` |
| `ndjson` | `application/x-ndjson` |
| `parquet` | `application/vnd.apache.parquet` |
| `avro` | `avro/binary` |
| `arrow-ipc` | `application/vnd.apache.arrow.stream` |

`content_encoding` compresses each request body whole and declares what it did. It takes `"gzip"`,
`"br"` or `"zstd"`, and is refused next to a `content-encoding` entry in `headers`.

Response bodies are decompressed transparently, because every request carries an `accept-encoding`
listing the encodings this connector reads. A `headers` entry for `accept-encoding` replaces that
one, which is how a node asks for an uncompressed response.

## Accept pushed documents: the HttpPushSource node

`HttpPushSource` listens instead of dialling. One request carries one whole document in the format
its `transformer` names, exactly what `HttpSink` writes, so a sink in one service feeds this source
in another.

<div class="code">
<div class="code-cap"><span>KDL</span><em>a listening source needs run_mode kind="stream"</em></div>

```kdl
source "orders_in" type="HttpPushSource" component="Order" transformer="orders_json" {
    config {
        bind "127.0.0.1:9700"
        path "/orders"
        methods "POST" "PUT"
        max_body_bytes 67108864
        buffer 64
        auth {
            bearer "dev-token"
        }

        schema_fields "id" type="Int64" nullable=#false
    }
}
```

</div>

`bind` is the `host:port` the listener accepts on, opened while the config is built, `validate`
included, so a busy port fails before the service starts. `path` is the one path that accepts
documents and defaults to `/`. `methods` takes one method name or a list of them and defaults to
`POST`. `max_body_bytes` caps one body at 64 MiB. `buffer` is how many decoded batches queue before
a request in flight waits for its response, which is where backpressure reaches the client.
`schema_fields` is required, and `schema_from` works as it does on the source.

One TCP listener serves HTTP/1.1 and HTTP/2. The HTTP/2 connection preface is sniffed, so a client
with prior knowledge and an HTTP/1.1 client share the port. A `tls` block here takes `cert_file`
and `key_file` and terminates TLS, offering `h2` and `http/1.1` over ALPN; `ca_file` is refused on
this node, because callers are authenticated by `auth { bearer }` instead of by a client
certificate. `quic_bind` serves HTTP/3 on its own UDP `host:port` and needs that `tls` block, and
every TCP response then advertises it as `alt-svc`.

`auth { bearer "token" }` demands `authorization: Bearer <token>` from every request, compared in
constant time.

| Answer | When |
|---|---|
| `202` | the document was queued, whether or not it held rows |
| `400` | the format could not read the document; the same error goes down the channel, so the runner counts it |
| `401` | `auth { bearer }` is set and the request's token is missing or wrong |
| `404` | any path other than `path` |
| `405` | a method `methods` does not list; the response carries `allow` |
| `413` | the body is over `max_body_bytes` |
| `500` | the spool file could not be written |

A document is queued whole or not at all, so a client that retries a request answered `400` or
`413` duplicates no rows.

`examples/configs/http_push.kdl` declares this node feeding an `HttpSink`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>this binds the source's port, so it must be free</em></div>

```text
saci-service validate --config examples/configs/http_push.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: orders (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

## How it delivers

No client half touches the network while the config is built, so `validate` passes against a url
that is down, a host that does not resolve, and a certificate the trust store rejects. All three
surface during `serve`, on the first batch, as `HttpSource: cannot GET {url}` or
`HttpSink: cannot POST {url}`. A push source binds its sockets while the config is built instead,
so a busy port fails at load.

The connector holds no connection open across a failure and reconnects on its own only in a
streaming read, which requests its body again after `reconnect_ms`. Whether a refused request is
attempted again is the node's `retry` policy, four attempts with exponential backoff by default. A
retry sends the same body again, so an endpoint that took the first attempt but failed to answer
sees that batch twice.

The sink writes each body from an empty buffer, so nothing accumulates between batches and there is
no flush threshold to configure. A document read re-arms after EOF, so a later pass of an
`interval` or `continuous` run mode issues a fresh GET.

HTTPS verification cannot be turned off. There is no `insecure` or `verify` key. Roots come from the
machine's own trust store, and `tls.ca_file` adds a private CA to them for one node.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `url` | string | required | the resource to GET |
| `timeout_ms` | integer | `30000` | budget for the whole request, connect through body; in a streaming mode the connect budget and the idle gap between body chunks |
| `headers` | table | none | header name to value, sent on the request |
| `http_version` | `"auto"`, `"1.1"`, `"2"` or `"3"` | `"auto"` | which HTTP version to speak |
| `tls` | table | none | extra roots and a client certificate for this node |
| `max_body_bytes` | integer | no cap | the largest body to read, one record in a streaming mode |
| `mode` | table | `kind="document"` | the read shape |
| `pagination` | `"none"` or `"link"` | `"none"` | follow `Link` `rel="next"` across pages, `kind="document"` only |
| `conditional` | boolean | `#false` | send the last response's validators, `kind="document"` only |
| `schema_from` | `"config"` or `"body"` | `"config"` | whether the format is handed `schema_fields` to project onto, or reads the body's own schema and is checked against it |
| `schema_fields` | list of fields | the format decides, required by `schema_from "body"` and by both streaming kinds | the declared column list |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `url` | string | required | the endpoint each batch is sent to |
| `method` | string | `POST` | the HTTP method |
| `timeout_ms` | integer | `30000` | budget for the whole request |
| `headers` | table | none | header name to value, sent on every request |
| `http_version` | `"auto"`, `"1.1"`, `"2"` or `"3"` | `"auto"` | which HTTP version to speak |
| `tls` | table | none | extra roots and a client certificate for this node |
| `content_encoding` | `"gzip"`, `"br"` or `"zstd"` | none | compress each body whole and declare it |
| `schema_fields` | list of fields | required | the schema each body is written with |

Push source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `bind` | string | required | the `host:port` the listener accepts on |
| `path` | string | `/` | the one path that accepts documents |
| `methods` | string or list | `POST` | the methods that accept documents |
| `max_body_bytes` | integer | `67108864` | the largest body to accept, 64 MiB |
| `buffer` | integer | `64` | decoded batches queued before backpressure |
| `tls` | table | none | `cert_file` plus `key_file` terminate TLS on this listener |
| `quic_bind` | string | none | a UDP `host:port` serving HTTP/3, advertised as `alt-svc`; needs `tls` |
| `auth` | table | none | `bearer` demands that token from every request |
| `schema_from` | `"config"` or `"body"` | `"config"` | as on the source |
| `schema_fields` | list of fields | required | the schema every document is read against |

### headers

| Key | Type | Default | What it does |
|---|---|---|---|
| any header name | string | none | one entry per header; the name and the value are both validated when the node is built |

### tls

| Key | Type | Default | What it does |
|---|---|---|---|
| `ca_file` | string | none | PEM file whose certificates become extra roots; the client halves only |
| `cert_file` | string | none | PEM certificate chain to present |
| `key_file` | string | none | PEM private key for `cert_file` |

### mode

| Key | Type | Default | What it does |
|---|---|---|---|
| `kind` | `"document"`, `"lines"` or `"events"` | `"document"` | the read shape |
| `event` | string | none | the one event name to decode, `kind="events"` only |
| `batch_size` | integer | `1000` | records per batch, the streaming kinds only |
| `poll_timeout_ms` | integer | `1000` | how long a partly filled batch waits for its next record |
| `reconnect_ms` | integer | `1000` | how long to wait before requesting a body again |

### auth

| Key | Type | Default | What it does |
|---|---|---|---|
| `bearer` | string | none | the token every request must present as `authorization: Bearer <token>` |

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
| `HttpSource config.http_version must be "auto", "1.1", "2" or "3"` | use one of the four spellings |
| `HttpSink: http_version "3" needs an https url: HTTP/3 runs over TLS` | point `url` at `https`, or pick another version |
| `HttpSource config.tls must be a table` | write `tls` as a child node of string paths |
| `HttpSource config.tls['{key}'] is not a tls key: expected 'ca_file', 'cert_file' or 'key_file'` | drop the unknown key |
| `HttpSource config.max_body_bytes must be an integer of at least 1` | give the cap a positive byte count |
| `HttpSource config.mode.kind must be "document", "lines" or "events"` | name one of the three kinds |
| `HttpSource config.mode['{key}'] must be an integer of at least 1` | give `batch_size`, `poll_timeout_ms` or `reconnect_ms` a positive value |
| `HttpSource: mode.event applies to mode kind="events" only` | drop `event`, or set `kind="events"` |
| `HttpSource: mode kind="{kind}" needs a 'schema_fields' list: a message decoder is built before the first byte arrives` | declare `schema_fields` on a streaming read |
| `HttpSource: schema_from "body" applies to mode kind="document" only` | keep `schema_from "config"` on a streaming read |
| `HttpSource: pagination applies to mode kind="document" only` | drop `pagination`, or read a document |
| `HttpSource: conditional applies to mode kind="document" only` | drop `conditional`, or read a document |
| `HttpSink config.content_encoding must be "gzip", "br" or "zstd"` | name one of the three encodings |
| `HttpSink: 'content-encoding' is set by content_encoding; remove it from headers` | drop the header and keep the key |
| `HttpPushSource config requires a 'bind' string` | add `bind "host:port"` to the push source's `config` |
| `HttpPushSource config.path must be a string starting with '/'` | write `path` as an absolute path |
| `HttpPushSource config.methods must be one HTTP method name or a list of them` | write one method name, or several as arguments |
| `HttpPushSource config.auth.bearer must be a non-empty string` | give `auth` a `bearer` token |
| `HttpPushSource config.tls needs both 'cert_file' and 'key_file'` | name both files, or neither |
| `HttpPushSource config.tls['ca_file'] is not a listener key: a push source authenticates callers with auth { bearer }, not with a client certificate` | drop `ca_file` and use `auth { bearer }` |
| `HttpPushSource: quic_bind needs a 'tls' block: HTTP/3 runs over TLS` | add `tls`, or drop `quic_bind` |
| `HttpPushSource: cannot bind '{bind}': {e}` | free the port, or bind another address |
| `source type 'HttpPushSource' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | set `run_mode kind="stream"` |

Two more wait for the first batch rather than the config check:
`HttpSource: body from {url} carries schema [...] but the config declared [...]` means a
`schema_from "body"` source found other columns than the declared ones, and
`parquet: declared column 'X' is not in the file (file columns: ...)` means the default
`schema_from "config"` projected onto a column the body does not carry.

## Next

- [csv](@/service/formats/csv.md), the format this page declares.
- [Sources and sinks](@/service/connectors/_index.md), the other fourteen transports.
- [Run modes and persistence](@/service/config/run-modes.md), for the stream mode a listening or
  streaming source needs.
