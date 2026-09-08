+++
title = "TCP"
description = "Two halves of one frame: a source that listens and a sink that dials, with the payload decoded and encoded by the transformer you name."
template = "subpage.html"
weight = 7
aliases = ["/connectors/tcp/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "Required on both halves; one message per frame"
[[extra.facts]]
label = "Run modes"
value = "Stream for the source, any for the sink"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
A workflow accepts framed batches on a listening port, processes them as they arrive, and forwards
each result to a collector it dials.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="tc-t tc-d">
        <title id="tc-t">A producer dialling in on one side, a collector dialled out to on the other</title>
        <desc id="tc-d">A client dials in box on the left feeds a source node named ticks_in, which listens. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named ticks_out. That sink dials the server box on the right and writes one frame per message.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">client</text>
            <text class="t-sm" x="10" y="74">dials in</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#tc-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">ticks_in</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#tc-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#tc-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">ticks_out</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#tc-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">server</text>
            <text class="t-sm" x="566" y="74">dialled out to</text>
        </g>
        <defs>
            <marker id="tc-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> peers and nodes</span>
        <span class="k-boundary"><i></i> the processor</span>
    </div>
</div>

## What you need

- A free port for the source's `bind`. The listener is opened while the config is built, `validate`
  included, so a busy port fails before the service starts.
- A producer that writes frames to that port. The two halves speak the same frame, so a `tcp` sink
  in one service feeds a `tcp` source in another.
- A collector listening on the sink's `connect` address, before the first batch is written.
- `run_mode kind="stream"` whenever a `tcp` source is declared. The source blocks for the next
  frame and never reaches EOF, and validation rejects it in any other run mode. A `tcp` sink carries
  no such rule.
- The processor component the config names, at `pipelines/ticks.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from;
  `validate --connectors-only` below runs without it.

## 1. Declare the format

A frame's payload is bytes, so both halves name a declared `transformer`.

<div class="code">
<div class="code-cap"><span>KDL</span><em>one whole batch per frame</em></div>

```kdl
transformer "ipc" format="arrow-ipc"
```

</div>

[arrow-ipc](@/service/formats/arrow-ipc.md) puts one whole batch in a frame and is the natural fit.
Any format with a message codec works. [ndjson](@/service/formats/ndjson.md),
[csv](@/service/formats/csv.md) and [avro](@/service/formats/avro.md) put one row in a frame, and
[parquet](@/service/formats/parquet.md) a whole batch.

## 2. Read from a listening port: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>a tcp source needs run_mode kind="stream"</em></div>

```kdl
source "ticks_in" type="tcp" component="Tick" transformer="ipc" {
    config {
        bind "0.0.0.0:9500"
        buffer 64
        max_frame_bytes 8388608

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

`bind` is the address producers connect to, and several may connect at once. `buffer` is how many
decoded batches queue before the source pushes back on its producers. `max_frame_bytes` caps one
frame, and a producer that exceeds it loses its own connection and nothing else.

`schema_fields` is required, because the decoder is built from it while the node is built. That is
also where a format with no message decoder is refused.

## 3. Write to a collector: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>the sink resolves the address at build and dials on the first batch</em></div>

```kdl
sink "ticks_out" type="tcp" component="EnrichedTick" transformer="ipc" {
    config {
        connect "collector.internal:9600"

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

`connect` is the peer this sink dials. `schema_fields` is required here too, and one batch becomes
one frame per message the format encodes, which for arrow-ipc is one frame per batch.

## 4. Validate and run

`examples/configs/tcp.kdl` declares both nodes and sets `run_mode kind="stream"`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>this binds the source's port, so it must be free</em></div>

```text
saci-service validate --config examples/configs/tcp.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: ticks (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The process stays up, accepting frames on `0.0.0.0:9500` and forwarding each result to
`127.0.0.1:9600`:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>both addresses are overridable</em></div>

```bash
export SACI_TCP_BIND='0.0.0.0:9500'
export SACI_TCP_CONNECT='127.0.0.1:9600'
saci-service serve --config examples/configs/tcp.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same three steps</em></div>

```powershell
$env:SACI_TCP_BIND = "0.0.0.0:9500"
$env:SACI_TCP_CONNECT = "127.0.0.1:9600"
saci-service serve --config examples/configs/tcp.kdl
```

</div>

The collector sees one frame per processed batch.

## How it delivers

The source is live and never reaches EOF, so only stream mode drives it. One frame decodes to one
batch, and the runner hands that batch to the workflow in chunks of the source's admission target,
so a frame larger than the target is several passes rather than one.

A bad frame costs one connection and nothing more. An oversized frame, a truncated payload, a frame
that decodes to no batch, or a payload whose schema is not the declared one all close that one
connection and log the reason. The listener and every other producer keep running.

The sink dials late and never redials. The address is resolved while the config is built, so
`validate` passes while the peer is down. The dial happens on the first batch, where an unreachable
peer surfaces as `TcpSink: cannot connect to {peer}`. One connection then serves the sink's whole
lifetime, so a peer that goes away mid-run fails every later write on that socket.

A clean close between frames is a normal disconnect, and the sink's own shutdown closes its write
half that way.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `bind` | string | required | the address the listener accepts producers on |
| `buffer` | integer | `64` | decoded batches queued before backpressure |
| `max_frame_bytes` | integer | `8388608` | the largest frame accepted, 8 MiB by default |
| `schema_fields` | list of fields | required | the schema each payload decodes against |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `connect` | string | required | the peer this sink dials |
| `schema_fields` | list of fields | required | the schema the frames are written with |

An unrecognised key inside `config` is ignored rather than rejected on this connector.

## When it refuses to start

| Message | What to change |
|---|---|
| `tcp source config requires a 'bind' string` | add `bind "host:port"` to the source's `config` |
| `tcp sink config requires a 'connect' string` | add `connect "host:port"` to the sink's `config` |
| `tcp moves bytes and needs a 'transformer' key naming a declared transformer` | add `transformer="<id>"` to the node |
| `format '{format}' does not support decoding discrete messages` | name a format that decodes discrete messages |
| `TcpSink: format '{format}' has no message codec` | the same, for the sink |
| `TcpIngestSource: cannot bind '{bind}': {e}` | free the port, or bind another address |
| `TcpSink: cannot resolve 'connect' address '{connect}': {e}` | fix the host name or the port |
| `TcpSink: 'connect' address '{connect}' resolved to no address` | the same, when the name resolves to nothing |
| `source type 'tcp' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | set `run_mode kind="stream"`, or drop the `tcp` source |

## Next

- [arrow-ipc](@/service/formats/arrow-ipc.md), the format this page declares.
- [Run modes and persistence](@/service/config/run-modes.md), for the stream mode this source needs.
