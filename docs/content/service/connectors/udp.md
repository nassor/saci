+++
title = "UDP"
description = "Fire and forget in both directions: a source that receives datagrams and a sink that sends them, with an optional liveness probe telling you whether the peer is still there."
template = "subpage.html"
weight = 12
aliases = ["/connectors/udp/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "Required on both halves; one message per datagram"
[[extra.facts]]
label = "Run modes"
value = "Stream for the source, any for the sink"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
A workflow receives datagrams on a bound port, processes each as it arrives, and sends every result
on to the collectors it names.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="ud-t ud-d">
        <title id="ud-t">A producer sending datagrams in on one side, a collector sent to on the other</title>
        <desc id="ud-d">A client sends in box on the left feeds a source node named ticks_in, which receives datagrams. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named ticks_out. That sink sends one datagram per message to the collector box on the right, which acknowledges nothing.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">client</text>
            <text class="t-sm" x="10" y="74">sends in</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#ud-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">ticks_in</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#ud-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#ud-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">ticks_out</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#ud-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">collector</text>
            <text class="t-sm" x="566" y="74">no reply</text>
        </g>
        <defs>
            <marker id="ud-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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

- A free port for the source's `bind`. The socket is opened while the config is built, `validate`
  included, so a busy port fails before the service starts.
- A producer that sends datagrams to that port. The two halves speak the same datagram, so a `udp`
  sink in one service feeds a `udp` source in another.
- A collector on the sink's `connect` address. It receives whatever arrives and answers nothing,
  unless you enable the probe below.
- `run_mode kind="stream"` whenever a `udp` source is declared. The source blocks for the next
  datagram and never reaches EOF, and validation rejects it in any other run mode. A `udp` sink
  carries no such rule.
- The processor component the config names, at `pipelines/ticks.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from;
  `validate --connectors-only` below runs without it.

## 1. Declare the format

A datagram's payload is bytes, so both halves name a declared `transformer`.

<div class="code">
<div class="code-cap"><span>KDL</span><em>one row per datagram</em></div>

```kdl
transformer "json" format="ndjson"
```

</div>

[ndjson](@/service/formats/ndjson.md) puts one row in a datagram and is the natural fit, which is
why `connector-udp` pulls it in. Any format with a message codec works.
[csv](@/service/formats/csv.md) and [avro](@/service/formats/avro.md) are also one row per datagram;
[arrow-ipc](@/service/formats/arrow-ipc.md) and [parquet](@/service/formats/parquet.md) put a whole
batch in one, so keep the source's admission target below `max_datagram_bytes` when you name either.

## 2. Read from a bound port: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>a udp source needs run_mode kind="stream"</em></div>

```kdl
source "ticks_in" type="udp" component="Tick" transformer="json" {
    config {
        bind "0.0.0.0:9700"
        buffer 64

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

`bind` is the address producers send to, and any number may send at once. `buffer` is how many
decoded batches queue before the receive loop stops reading the socket. Add
`multicast_group "239.255.0.1"` to join a group of the same IP family as `bind`, for a feed several
receivers read; run one receiver per port per host, since the socket takes no address reuse option.

`schema_fields` is required, because the decoder is built from it while the node is built. That is
also where a format with no message decoder is refused.

## 3. Write to a collector: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>the sink resolves every address at build and opens its sockets on the first batch</em></div>

```kdl
sink "ticks_out" type="udp" component="EnrichedTick" transformer="json" {
    config {
        connect "collector.internal:9701"
        max_datagram_bytes 65507

        probe kind="icmp" interval_ms=5000 timeout_ms=1000 on_failure="refuse"

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

`connect` is one address or a list of them; every message goes to every peer. `max_datagram_bytes`
caps one message, and a batch holding a larger one is refused before any datagram is sent. The
`probe` block is the sink's only way of learning whether a peer is still there, and the line alone
is enough: every property has a default. `kind="icmp"` is the kind for a collector that is not a
SACI service, because it reads only what the OS relays and needs no answer; the default
`kind="echo"` waits for a datagram back, which only another service's `udp` source sends. One round
probes every peer at the same time, so it costs one `timeout_ms` however many addresses `connect`
names. `schema_fields` is required here too, and one batch becomes one datagram per message the
format encodes.

## 4. Validate and run

`examples/configs/udp.kdl` declares both nodes and sets `run_mode kind="stream"`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>this binds the source's port, so it must be free</em></div>

```text
saci-service validate --config examples/configs/udp.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: ticks (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The process stays up, receiving datagrams on `0.0.0.0:9700` and sending each result to
`127.0.0.1:9701`:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>both addresses are overridable</em></div>

```bash
export SACI_UDP_BIND='0.0.0.0:9700'
export SACI_UDP_CONNECT='127.0.0.1:9701'
saci-service serve --config examples/configs/udp.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same three steps</em></div>

```powershell
$env:SACI_UDP_BIND = "0.0.0.0:9700"
$env:SACI_UDP_CONNECT = "127.0.0.1:9701"
saci-service serve --config examples/configs/udp.kdl
```

</div>

The collector sees one datagram per processed message.

## How it delivers

UDP delivers nothing. A datagram that leaves the socket is neither acknowledged nor retransmitted,
so a peer that is down, a receive buffer that is full and a fragment lost on the path all look
exactly like a delivered datagram to the sender. Use this connector where that is the trade you
want: metrics, logs, telemetry, anything whose value is in arriving soon rather than arriving at
all.

One datagram carries exactly one message, with no length prefix, because the datagram boundary is
the frame. That is also the cap: a message above `max_datagram_bytes` cannot be sent at all, and the
sink refuses the whole batch holding it rather than delivering part of one. With `ndjson`, `csv` or
`avro` a message is one row and the cap is far away. With `arrow-ipc` or `parquet` a message is a
whole batch, so keep the workflow's `max_chunk_bytes` below the cap.

A bad datagram costs itself and nothing more. An undecodable payload, or one that decodes to no
rows, is dropped with a warning and the decoder is replaced, so whatever it was holding goes with
it. There is no connection to close, so the next datagram from that producer or any other is
decoded as if the bad one had never arrived.

Backpressure is the kernel's. Once `buffer` batches are queued the receive loop stops reading, the
socket's receive buffer fills, and the OS drops datagrams. Nothing tells the producer.

The probe is what notices a peer. `kind="echo"` sends a zero-length datagram and waits for one back;
every `udp` source answers a zero-length datagram from its sender and never decodes it, so this kind
gets a real answer out of the peer's own receive loop. A peer that is not a `udp` source never
answers, so it needs `kind="icmp"` instead, which reads only what the OS reports: a closed port on a
reachable host answers with an ICMP port-unreachable, and silence is the only "alive" it has.
Neither kind can tell a live peer from a host whose firewall drops the report.

Each round clears what the socket already holds before it asks, so its verdict is about that round
and not an answer left over from the last one. Every peer is probed at once, so a round costs one
`timeout_ms` however many addresses `connect` names, and `interval_ms` is the gap between rounds.

`on_failure="refuse"` stops sending to a peer that failed its last round, and a batch no peer
accepted is a `write_batch` error, which is what the node's `retry` policy, the
[self-healing](@/service/operate/self-healing.md) layer and the
[dead letter queue](@/service/operate/dead-letter-queue.md) act on. `on_failure="warn"` logs the
transition and keeps sending. With several `connect` addresses the write fails only when the batch
reached nobody, so a live peer never receives the runner's retry of a batch it already took. A batch
the format encodes to no messages succeeds whatever the peers are doing, since there is nothing for
one to refuse.

Reconnecting is the `heal` block and nothing else. A rebuilt sink re-resolves every `connect`
address and opens fresh sockets, which is the whole of what reconnecting means for a datagram
socket. `broadcast #true` sets `SO_BROADCAST` on every socket, for a broadcast address; it cannot be
combined with `probe`, and neither can a multicast `connect` address, because a probe needs one peer
to read an answer from. A multicast `connect` address is addressed per datagram and takes the host's
default outgoing interface and multicast TTL, so it reaches the local link and no further.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `bind` | string | required | the address the socket receives datagrams on |
| `buffer` | integer | `64` | decoded batches queued before the receive loop stops reading |
| `multicast_group` | string | none | a multicast address to join, of the same IP family as `bind` |
| `schema_fields` | list of fields | required | the schema each payload decodes against |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `connect` | string or list | required | the peers this sink sends every message to |
| `broadcast` | bool | `#false` | set `SO_BROADCAST` on every socket |
| `max_datagram_bytes` | integer | `65507` | the largest message sent, the IPv4 payload maximum |
| `probe` | node | none | the liveness probe; the bare line enables the defaults below |
| `probe kind` | string | `"echo"` | `"echo"` waits for a datagram back, `"icmp"` only for a report of death |
| `probe interval_ms` | integer | `5000` | the gap between rounds |
| `probe timeout_ms` | integer | `1000` | how long one round waits for an answer |
| `probe on_failure` | string | `"refuse"` | `"refuse"` stops sending to a failed peer, `"warn"` logs and keeps sending |
| `schema_fields` | list of fields | required | the schema the datagrams are written with |

An unrecognised key inside `config` is ignored rather than rejected on this connector. A key from
the tables above written with the wrong type is refused, so a typo reaches you as a message rather
than as the default. `buffer` is the exception: it is clamped to at least 1 and falls back to 64.

## When it refuses to start

| Message | What to change |
|---|---|
| `udp source config requires a 'bind' string` | add `bind "host:port"` to the source's `config` |
| `udp sink config requires 'connect', one address string or a list of them` | add `connect "host:port"` to the sink's `config` |
| `udp moves bytes and needs a 'transformer' key naming a declared transformer` | add `transformer="<id>"` to the node |
| `format '{format}' does not support decoding discrete messages` | name a format that decodes discrete messages |
| `UdpSink: format '{format}' has no message codec` | the same, for the sink |
| `udp source 'multicast_group' is not an IP address: '{value}'` | write the group as a bare address, `"239.255.0.1"` |
| `udp source 'multicast_group' must be a string` | quote the address |
| `udp sink 'max_datagram_bytes' must be a positive integer` | write a whole number of bytes |
| `udp sink 'broadcast' must be a boolean` | write `#true` or `#false` |
| `udp sink 'probe' must be a node with optional kind, interval_ms, timeout_ms and on_failure properties` | write `probe` as a node, not a value |
| `udp sink probe 'kind' must be a string` | quote the kind |
| `udp sink probe 'interval_ms' must be a positive integer` | write a whole number of milliseconds |
| `udp sink probe 'timeout_ms' must be a positive integer` | the same, for the round's wait |
| `udp sink probe 'on_failure' must be a string` | quote the answer |
| `udp sink probe 'kind' must be "echo" or "icmp", got '{value}'` | pick one of the two kinds |
| `udp sink probe 'on_failure' must be "refuse" or "warn", got '{value}'` | pick one of the two answers |
| `UdpSource: cannot bind '{bind}': {e}` | free the port, or bind another address |
| `UdpSource: set_nonblocking failed: {e}` | the OS refused to put the socket in non-blocking mode |
| `UdpSource: local_addr failed: {e}` | the OS could not report the address the socket bound |
| `UdpSource: 'multicast_group' {group} is not a multicast address` | name an address in the multicast range |
| `UdpSource: 'multicast_group' {group} and 'bind' {bind} are not the same IP family` | match the group's family to the bind address |
| `UdpSource: cannot join multicast group {group}: {e}` | check the host's interfaces and routing |
| `UdpSink: 'connect' names no address` | give `connect` at least one address |
| `UdpSink: cannot resolve 'connect' address '{connect}': {e}` | fix the host name or the port |
| `UdpSink: 'connect' address '{connect}' resolved to no address` | the same, when the name resolves to nothing |
| `UdpSink: 'max_datagram_bytes' must be between 1 and 65507, got {n}` | lower the cap to an IPv4 payload |
| `UdpSink: 'probe' needs one answering peer; it cannot be combined with 'broadcast' or a multicast 'connect' address` | drop the probe, or the broadcast and multicast peers |
| `UdpSink: probe 'interval_ms' and 'timeout_ms' must be at least 1` | give both a positive number of milliseconds |
| `source type 'udp' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | set `run_mode kind="stream"`, or drop the `udp` source |

## Next

- [ndjson](@/service/formats/ndjson.md), the format this page declares.
- [TCP](@/service/connectors/tcp.md), when every message has to arrive.
- [Self-healing](@/service/operate/self-healing.md), for what a refused write reaches.
- [Run modes and persistence](@/service/config/run-modes.md), for the stream mode this source needs.
