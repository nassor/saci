+++
title = "Channel"
description = "An in-process pair dressed as IO: the transport for bridging one workflow to another inside the same service."
template = "subpage.html"
weight = 9
aliases = ["/connectors/channel/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "None: batches arrive already typed, so no transformer is named"
[[extra.facts]]
label = "Run modes"
value = "Any"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
One workflow hands part of its stream to a second workflow in the same process, with no broker
and no file in between.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 132" role="img" aria-labelledby="ch-t ch-d">
        <title id="ch-t">Two workflows in one process, joined by a named channel</title>
        <desc id="ch-d">The workflow named route holds a sink node called standard_bridge. That sink writes into a channel box named standard, which a source node called standard_in reads inside the workflow named settle. The two workflow frames are the config, the nodes and the channel are the data plane, and no link crosses between the frames.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="20" width="240" height="92" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="20" width="240" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="32" width="240" height="8"/>
            <text class="t-lbl" x="12" y="35">workflow "route"</text>
            <rect class="blk blk-data" x="16" y="56" width="208" height="40" rx="8"/>
            <text class="t-lbl" x="28" y="72">sink standard_bridge</text>
            <text class="t-sm" x="28" y="88">type="ChannelSink"</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M240 76 H262" marker-end="url(#ch-a)"/>
            <rect class="blk blk-data" x="266" y="52" width="128" height="48" rx="8"/>
            <rect class="hd hd-data" x="266" y="52" width="128" height="20" rx="8"/>
            <rect class="hd hd-data" x="266" y="64" width="128" height="8"/>
            <text class="t-lbl" x="278" y="67">channel</text>
            <text class="t-sm t-data" x="278" y="90">name "standard"</text>
            <path class="arw arw-data" d="M394 76 H416" marker-end="url(#ch-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="420" y="20" width="240" height="92" rx="8"/>
            <rect class="hd hd-ctl" x="420" y="20" width="240" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="420" y="32" width="240" height="8"/>
            <text class="t-lbl" x="432" y="35">workflow "settle"</text>
            <rect class="blk blk-data" x="436" y="56" width="208" height="40" rx="8"/>
            <text class="t-lbl" x="448" y="72">source standard_in</text>
            <text class="t-sm" x="448" y="88">type="ChannelSource"</text>
        </g>
        <defs>
            <marker id="ch-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> nodes and the channel</span>
        <span class="k-control"><i></i> the workflows</span>
    </div>
</div>

## What you need

- Nothing outside the process. The channel is memory, so there is no server, no port and no file.
- Both halves in one config file. They may sit in the same workflow or in two different ones, and
  they meet by the `name` they both declare.
- Exactly one sink and one source per `name`. Two of either half, or only one half, is rejected
  before any connector is built.
- No transformer. Batches cross already typed, so no format is involved.

## 1. Read from a channel: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>the consuming half of the bridge</em></div>

```kdl
workflow "settle" {
    source "standard_in" type="ChannelSource" component="Sale" {
        config name="standard" buffer=8 {
            schema_fields "id" type="Int64" nullable=#false
        }
    }
}
```

</div>

`name` is the channel this half joins. `buffer` is how many batches the channel holds before the
producing sink waits, and both halves have to declare the same value. `schema_fields` is required,
and both halves have to agree on it. A batch whose schema differs is refused.

## 2. Write to a channel: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>the producing half, in another workflow of the same config</em></div>

```kdl
workflow "route" {
    sink "standard_bridge" type="ChannelSink" component="Sale" {
        config name="standard" buffer=8 {
            schema_fields "id" type="Int64" nullable=#false
        }
    }
}
```

</div>

The two halves resolve to one channel through the `name`. No `link` crosses a workflow boundary, so
a channel is the only way rows move between workflows.

## 3. Validate and run

`examples/multi_workflow/multi_workflow.kdl` is a worked pair: the `route` workflow splits a stream
and bridges half of it to the `settle` workflow on the channel `standard`.

Two `NatsSource`s and two `PostgresSink`s sit alongside the channel, and neither
connector is in the default build, so the two commands below need a binary built
with `--features connector-nats,connector-postgresql`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>the pairing rule is checked here, before anything is built</em></div>

```text
saci-service validate --config examples/multi_workflow/multi_workflow.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: route (sources: 1, sinks: 2)
  workflow: settle (sources: 2, sinks: 1)
OK: all declared types resolved in built-in registry
```

That config's other nodes need a NATS server and a PostgreSQL database, so start both before
serving it:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>brings up NATS and PostgreSQL with the example's tables</em></div>

```text
docker compose -f examples/multi_workflow/docker-compose.yml up -d
```

</div>

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>both workflows run concurrently in one process</em></div>

```text
saci-service serve --config examples/multi_workflow/multi_workflow.kdl
```

</div>

The dashboard at `http://127.0.0.1:8080/ui` then draws one card per workflow plus a
`channel bridges` card listing `standard  standard_bridge → standard_in` with its live rate.

## How it delivers

Rows cross in memory, so nothing is encoded, written or acknowledged. The channel holds `buffer`
batches; a producer that gets ahead waits for the consumer.

End of stream is the producing side finishing. The sink is the channel's only writer, so once its
workflow is done the consuming source reports EOF and its own workflow drains and ends.

A batch that does not match the declared schema is refused, and the error names both schemas.

A channel is process memory, so nothing survives a restart. Rows in flight when the process stops
are gone.

## Every key

Source and sink take the same three keys:

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | string | required | the channel both halves join |
| `buffer` | integer | `8` | batches the channel holds before the writer waits; both halves must agree |
| `schema_fields` | list of fields | required | the schema batches carry; both halves must agree |

An unrecognised key inside `config` is ignored rather than rejected on this connector.

## When it refuses to start

| Message | What to change |
|---|---|
| `channel '<name>': declares a ChannelSink but no ChannelSource` | declare the missing half, in this workflow or another |
| `channel '<name>': more than one ChannelSink declared` | keep exactly one sink for that `name` |
| `ChannelSource config requires a 'name' key naming the shared channel` | add `name="..."` to the `config` |
| `ChannelSource config requires a 'schema_fields' list` | declare `schema_fields` in the `config` |
| `channel '<name>': the paired ChannelSource and ChannelSink declare different schemas` | make both `schema_fields` lists identical |
| `channel '<name>': buffer <n> differs from the paired half's buffer <m>` | give both halves the same `buffer` |

## Next

- [Several workflows in one process](@/service/processors/multiple-workflows.md), the pattern this connector exists for.
- [Sources and sinks](@/service/connectors/_index.md), the other seven transports.
