+++
title = "SACI"
description = "One SACI service pushing batches to another: a sink that dials, a source that listens, and a handshake that names the service, workflow and node every batch came from."
template = "subpage.html"
weight = 8
aliases = ["/connectors/saci/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "None: Arrow IPC on the wire"
[[extra.facts]]
label = "Run modes"
value = "Stream for the source, any for the sink"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
A workflow in one service pushes its results to a workflow in another, and the receiving side knows
which service, workflow and sink node produced every batch.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 156" role="img" aria-labelledby="sc-t sc-d">
        <title id="sc-t">A sink in service A opening a session to a source in service B</title>
        <desc id="sc-d">On the left, a boundary box labelled service A holds a sink node named ticks_out. An arrow crosses to the right, into a boundary box labelled service B holding a source node named ticks_in. The arrow is annotated hello, accept, then batches, and a second short arrow points back from the source to the sink carrying the accept or reject answer.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-bnd" x="0" y="20" width="240" height="84" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="20" width="240" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="32" width="240" height="6"/>
            <text class="t-lbl" x="10" y="34">service A</text>
            <rect class="blk blk-data" x="118" y="48" width="104" height="44" rx="8"/>
            <rect class="hd hd-data" x="118" y="48" width="104" height="16" rx="8"/>
            <rect class="hd hd-data" x="118" y="58" width="104" height="6"/>
            <text class="t-lbl" x="128" y="60">sink</text>
            <text class="t-sm" x="128" y="82">ticks_out</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M222 62 H438" marker-end="url(#sc-a)"/>
            <text class="t-sm t-data" x="248" y="54">hello, then batches</text>
            <path class="arw arw-ctl" d="M438 88 H222" marker-end="url(#sc-b)"/>
            <text class="t-sm" x="252" y="108">accept or reject</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="420" y="20" width="240" height="84" rx="8"/>
            <rect class="hd hd-bnd" x="420" y="20" width="240" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="420" y="32" width="240" height="6"/>
            <text class="t-lbl" x="430" y="34">service B</text>
            <rect class="blk blk-data" x="442" y="48" width="104" height="44" rx="8"/>
            <rect class="hd hd-data" x="442" y="48" width="104" height="16" rx="8"/>
            <rect class="hd hd-data" x="442" y="58" width="104" height="6"/>
            <text class="t-lbl" x="452" y="60">source</text>
            <text class="t-sm" x="452" y="82">ticks_in</text>
        </g>
        <defs>
            <marker id="sc-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="sc-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--ctl-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> nodes and batches</span>
        <span class="k-control"><i></i> the handshake answer</span>
        <span class="k-boundary"><i></i> a service</span>
    </div>
</div>

## What you need

- A free port for the source's `bind`. The listener is opened while the config is built, `validate`
  included, so a busy port fails before the service starts.
- Another `saci-service` with a `saci` sink pointing at that port. Both halves are in the default
  build, so no flag is needed on either side.
- `run_mode kind="stream"` whenever a `saci` source is declared. The source blocks for the next
  batch and never reaches EOF, and validation rejects it in any other run mode. A `saci` sink
  carries no such rule.
- Matching `schema_fields` on the two nodes. The sink announces its schema and the source refuses a
  peer that declares anything else, so a disagreement surfaces at the handshake.
- The processor component the config names, at `pipelines/ticks.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from;
  `validate --connectors-only` below runs without it.

No `transformer` node and no `format` key. Both ends are SACI services that already hold
`RecordBatch`es, so Arrow IPC is the wire format and there is nothing to choose.

## 1. Receive from a peer: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>a saci source needs run_mode kind="stream"</em></div>

```kdl
source "ticks_in" type="saci" component="Tick" {
    config {
        bind "0.0.0.0:9700"
        buffer 64
        max_frame_bytes 8388608

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

`bind` is the address peer services connect to, and several may connect at once. `buffer` is how
many batches queue before the source pushes back on its peers through TCP flow control.
`max_frame_bytes` caps one frame, and a peer that exceeds it loses its own session and nothing else.

`schema_fields` is required, and it is what a peer's hello is checked against.

## 2. Push to a peer: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>the sink resolves every address at build and dials on the first batch</em></div>

```kdl
sink "ticks_out" type="saci" component="EnrichedTick" {
    config {
        connect "b.internal:9700" "b-standby.internal:9700"
        handshake_timeout_ms 5000

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

`connect` takes one address or a list of them, tried in order until one accepts a session, so a set
of downstream services acts as failover peers. `handshake_timeout_ms` bounds the wait for each
peer's answer.

## 3. Validate and run

`examples/configs/saci.kdl` declares both nodes and sets `run_mode kind="stream"`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>this binds the source's port, so it must be free</em></div>

```text
saci-service validate --config examples/configs/saci.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: ticks (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The process stays up, accepting sessions on `0.0.0.0:9700` and pushing each result to
`127.0.0.1:9701`:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>both addresses are overridable</em></div>

```bash
export SACI_SACI_BIND='0.0.0.0:9700'
export SACI_SACI_CONNECT='127.0.0.1:9701'
saci-service serve --config examples/configs/saci.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same three steps</em></div>

```powershell
$env:SACI_SACI_BIND = "0.0.0.0:9700"
$env:SACI_SACI_CONNECT = "127.0.0.1:9701"
saci-service serve --config examples/configs/saci.kdl
```

</div>

## How it delivers

A session opens with the sink's hello: the protocol version, the service, workflow and sink node
calling, and the schema every batch will carry. The source answers `accept` or `reject`, and
nothing after that answer is negotiated. Each following frame carries one batch.

Three things earn a refusal, and each names itself in the sink's error:

| What the source found | What the sink is told |
|---|---|
| a version it does not speak | `protocol version {v} is not supported; this source speaks version 1` |
| a first frame that is not a hello | `expected a hello frame, got frame kind {k}` |
| fields other than its own | `schema mismatch: the peer declares {…}, this source declares {…}` |

A refusal is a `configuration` error, not a transport one, because the disagreement is in the two
config files. The sink returns it at once rather than trying the next address, since every peer in
the list would answer the same way.

The other two answers are about the network, and they are what `connect`'s order is for. A peer
that cannot be dialled, that errors mid-handshake, or that stays silent past
`handshake_timeout_ms`, is skipped and the next is tried. With no peer left the sink reports
`SaciSink: no peer accepted a session:` followed by every address and what it said.

A write the sink sees fail drops the session, so the runner's retry redials and that batch is sent
again, possibly to the next peer. There is no acknowledgement frame: a batch the socket already
accepted is not confirmed by the peer, so one lost with the session is not resent.

A protocol violation after the handshake costs one session and nothing more. An oversized frame, a
truncated body, a frame that is not a data frame, or Arrow IPC bytes that will not decode all close
that one session and log the reason. The listener and every other peer keep running.

The source is live and never reaches EOF, so only stream mode drives it. The sink's own `finish`
closes its write half between frames, which the peer reads as a normal disconnect.

## The peer in metrics and traces

Every series the source emits carries the receiving node's `workflow` and `source`. A session that
got as far as a hello carries `peer_service`, `peer_workflow` and `peer_sink` as well, so a
receiver fed by several services reads them apart. `peer_service` is the sending service's `node
name`, or its decimal `node id` when unnamed.

| Series | Extra labels | What it counts |
|---|---|---|
| `saci_peer_source_sessions_total` | `outcome` = `accepted` or `rejected` | sessions this source decided on |
| `saci_peer_source_batches_total` | | batches received |
| `saci_peer_source_rows_total` | | rows received |
| `saci_peer_source_bytes_total` | | data frame body bytes received |
| `saci_peer_source_errors_total` | `kind` = `frame`, `decode` or `schema` | sessions closed on a violation |

A session refused before its hello parsed has no peer to name, so it records `outcome="rejected"`
with the base labels only.

The sink opens a `peer.send` span per batch and the source a `peer.receive` span per received
frame, both carrying the same peer fields. Both are `debug` spans, the level `workflow.batch`
opens at, so they exist only when a service's `observability.log_level` is `"debug"` (or a
`RUST_LOG` override reaches them), not under the default `"error"`. With
`observability.otlp_endpoint` set on **both** services, the sink writes its span's W3C
`traceparent` into the frame and the source adopts it, so the two spans join one trace. Without an
OTLP endpoint there is no exported context to carry: the frame's traceparent is empty and the
receiving span is a root that still carries the peer fields. The inspector's traces tab never
links across processes, because its ids are `tracing` span ids; the fields show there either way.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `bind` | string | required | the address the listener accepts peer services on |
| `buffer` | integer | `64` | batches queued before backpressure |
| `max_frame_bytes` | integer | `8388608` | the largest frame accepted, 8 MiB by default |
| `schema_fields` | list of fields | required | the schema a peer's hello must declare |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `connect` | string or list of strings | required | the peers this sink dials, in order |
| `handshake_timeout_ms` | integer | `5000` | how long to wait for a peer's answer |
| `schema_fields` | list of fields | required | the schema announced to the peer |

An unrecognised key inside `config` is ignored rather than rejected on this connector.

## When it refuses to start

| Message | What to change |
|---|---|
| `saci source config requires a 'bind' string` | add `bind "host:port"` to the source's `config` |
| `saci sink config requires 'connect', one address string or a list of them` | add `connect "host:port"` to the sink's `config` |
| `saci needs the node identity (service, workflow, node) the host binds to every connector it builds` | declare the node inside a `workflow`; the host binds this itself |
| `SaciSource: cannot bind '{bind}': {e}` | free the port, or bind another address |
| `SaciSink: cannot resolve 'connect' address '{connect}': {e}` | fix the host name or the port |
| `SaciSink: 'connect' address '{connect}' resolved to no address` | the same, when the name resolves to nothing |
| `source type 'saci' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | set `run_mode kind="stream"`, or drop the `saci` source |

## Next

- [TCP](@/service/connectors/tcp.md), for a framed socket whose payload format you choose.
- [Run modes and persistence](@/service/config/run-modes.md), for the stream mode this source needs.
- [Observability](@/service/operate/observability.md), for the `/metrics` endpoint these series
  are served on and the `otlp_endpoint` the traceparent needs.
