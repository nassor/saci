+++
title = "Sources and sinks"
description = "Eleven transports a workflow can read from and write to, one page each, with the config node they accept."
template = "section.html"
sort_by = "weight"
weight = 4
aliases = ["/connectors/"]
+++

A workflow reads rows through a `source` node and writes them through a `sink` node. Eleven
transports ship with `saci-service`, and the pages below walk each one from an empty config file to a
running pipeline.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 803 168" role="img" aria-labelledby="cx-t cx-d">
        <title id="cx-t">The eleven transports a workflow can name on a source or a sink node</title>
        <desc id="cx-d">Eleven boxes across the top name the transports: file, http, kafka, nats, postgres, turso, redb, s3, tcp, saci and channel. Each one drops into a shared line, and that line feeds a single workflow box below. One workflow may name any mix of them, one node per source or sink.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="6" y="33">file</text>
            <rect class="blk blk-data" x="73" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="79" y="33">http</text>
            <rect class="blk blk-data" x="146" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="152" y="33">kafka</text>
            <rect class="blk blk-data" x="219" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="225" y="33">nats</text>
            <rect class="blk blk-data" x="292" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="298" y="33">postgres</text>
            <rect class="blk blk-data" x="365" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="371" y="33">turso</text>
            <rect class="blk blk-data" x="438" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="444" y="33">redb</text>
            <rect class="blk blk-data" x="511" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="517" y="33">s3</text>
            <rect class="blk blk-data" x="584" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="590" y="33">tcp</text>
            <rect class="blk blk-data" x="657" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="663" y="33">saci</text>
            <rect class="blk blk-data" x="730" y="8" width="68" height="40" rx="8"/>
            <text class="t-lbl" x="736" y="33">channel</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M34 48 V72"/>
            <path class="arw arw-data" d="M107 48 V72"/>
            <path class="arw arw-data" d="M180 48 V72"/>
            <path class="arw arw-data" d="M253 48 V72"/>
            <path class="arw arw-data" d="M326 48 V72"/>
            <path class="arw arw-data" d="M399 48 V72"/>
            <path class="arw arw-data" d="M472 48 V72"/>
            <path class="arw arw-data" d="M545 48 V72"/>
            <path class="arw arw-data" d="M618 48 V72"/>
            <path class="arw arw-data" d="M691 48 V72"/>
            <path class="arw arw-data" d="M764 48 V72"/>
            <path class="ln" d="M34 72 H764"/>
            <text class="t-sm t-data" x="42" y="88">rows in, rows out</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M399 72 V104" marker-end="url(#cx-d1)"/>
            <rect class="blk blk-ctl" x="309" y="108" width="180" height="52" rx="8"/>
            <rect class="hd hd-ctl" x="309" y="108" width="180" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="309" y="120" width="180" height="8"/>
            <text class="t-lbl" x="321" y="123">workflow</text>
            <text class="t-sm" x="321" y="146">source, sink, link</text>
        </g>
        <defs>
            <marker id="cx-d1" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> transports</span>
        <span class="k-control"><i></i> the config</span>
    </div>
</div>

## Pick a connector

Every row is a source and a sink, so one transport serves both ends of a workflow. `Needs a format`
means the node names a declared [transformer](@/service/formats/_index.md); the four row-carrying
connectors take none.

| Connector | Source | Sink | Needs a format | Run modes | In the default build |
|---|---|---|---|---|---|
| [File](@/service/connectors/file.md) | yes | yes | yes | any | yes |
| [HTTP](@/service/connectors/http.md) | yes | yes | yes | any | yes |
| [Kafka](@/service/connectors/kafka.md) | yes | yes | yes | stream, or any run mode with `stop_at_end` or `compacted` | no: build with `--features connector-kafka` |
| [NATS](@/service/connectors/nats.md) | yes | yes | yes | stream, or any run mode with `stop_at_end` | no: build with `--features connector-nats` |
| [PostgreSQL](@/service/connectors/postgresql.md) | yes | yes | no | any | no: build with `--features connector-postgresql` |
| [Turso](@/service/connectors/turso.md) | yes | yes | no | any | no: build with `--features connector-turso` |
| [redb](@/service/connectors/redb.md) | yes | yes | yes | any | yes |
| [S3](@/service/connectors/s3.md) | yes | yes | yes | any | no: build with `--features connector-s3` |
| [TCP](@/service/connectors/tcp.md) | yes | yes | yes | stream for the source, any for the sink | yes |
| [SACI](@/service/connectors/saci.md) | yes | yes | no | stream for the source, any for the sink | yes |
| [Channel](@/service/connectors/channel.md) | yes | yes | no | any | yes |

A node picks its transport with a `type` string: `FileSource` and `FileSink`, `HttpSource` and
`HttpSink`, `KafkaSource` and `KafkaSink`, `NatsSource` and `NatsSink`, `PostgresSource` and
`PostgresSink`, `TursoSource` and `TursoSink`, `RedbSource` and `RedbSink`, `S3Source` and
`S3Sink`, `ChannelSource` and `ChannelSink`, `tcp` in lower case for both halves of TCP, and
`saci` for both halves of a service-to-service link.

Reading a query result instead of a transport is a Rust-only path with no config `type`:
[SQL results as a source](@/library/service/datafusion.md) covers it.

## The shape every node shares

A `source` or `sink` node carries the same five properties whatever the transport:

| Property | What it does |
|---|---|
| `type` | the transport, from the table above |
| `component` | the row type the node fills on the way in, or drains on the way out |
| `transformer` | the id of a declared `transformer` node, on every connector that moves bytes |
| `retry` | an optional child overriding the retry policy for this node |
| `config` | the child node holding everything the transport itself reads |

`transformer` is a property of the node, never a key inside `config`. The format is resolved before
the transport is built, so a byte-carrying node with no `transformer` is rejected at load time
rather than at the first batch.

`schema_fields` lives inside `config`, one entry per column, and the format decides what it means:
required, optional, or a projection target for the self-describing formats, which read only those
columns. Each connector page states which of the two ends needs it. An entry takes the column name
as its leading argument, then `type` and `nullable`, which defaults to `#true`.

The `type` names every connector but PostgreSQL and Turso accepts are `Boolean`, `Int8`, `Int16`,
`Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `Float64`, `Utf8`, `LargeUtf8`,
`Binary`, `Date32` and `Date64`, in any case. PostgreSQL and Turso each read their own narrower
vocabulary, listed on their pages.

A failed write retries with exponential backoff before the error reaches the runner: four attempts,
a 100 ms base, 2.0x growth, a 30 s cap and 0.1 jitter. A `retry` child overrides those per node, and
`max_attempts=1` turns retrying off. A source takes the same policy in every run mode except
`stream`, where the runner already re-polls a failed source itself.
[Workflows and links](@/service/config/workflows.md) has the key table.

How many rows a source hands over per pass is not a connector key. The runner asks each source for
an admission target and resizes it as the pipeline runs. `batch_size`, `batch_rows` and their twins
therefore only take effect when the runner sends no hint.
[Flow control](@/service/operate/flow-control.md) says when that is.

Teaching `saci-service` a transport it does not ship means writing a source or a sink in Rust and
registering it in your own binary. [Writing a connector](@/library/service/connectors.md) is that
route.

## Next

- [Formats](@/service/formats/_index.md), the byte formats a `transformer` node names.
- [Workflows and links](@/service/config/workflows.md), the nodes these sources and sinks sit in.
