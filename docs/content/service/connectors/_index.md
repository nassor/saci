+++
title = "Sources and sinks"
description = "Fifteen transports a workflow can read from and write to, one page each, with the config node they accept."
template = "section.html"
sort_by = "weight"
weight = 4
aliases = ["/connectors/"]
+++

A workflow reads rows through a `source` node and writes them through a `sink` node. Fifteen
transports ship with `saci-service`, and the pages below walk each one from an empty config file to a
running pipeline.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 1570 168" role="img" aria-labelledby="cx-t cx-d">
        <title id="cx-t">The fifteen transports a workflow can name on a source or a sink node</title>
        <desc id="cx-d">Fifteen boxes across the top name the transports: file, http, kafka, nats, postgres, turso, clickhouse, redb, s3, tcp, udp, saci, opentelemetry, channel and mariadb. Each one drops into a shared line, and that line feeds a single workflow box below. One workflow may name any mix of them, one node per source or sink.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="6" y="33">file</text>
            <rect class="blk blk-data" x="105" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="111" y="33">http</text>
            <rect class="blk blk-data" x="210" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="216" y="33">kafka</text>
            <rect class="blk blk-data" x="315" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="321" y="33">nats</text>
            <rect class="blk blk-data" x="420" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="426" y="33">postgres</text>
            <rect class="blk blk-data" x="525" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="531" y="33">turso</text>
            <rect class="blk blk-data" x="630" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="636" y="33">clickhouse</text>
            <rect class="blk blk-data" x="735" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="741" y="33">redb</text>
            <rect class="blk blk-data" x="840" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="846" y="33">s3</text>
            <rect class="blk blk-data" x="945" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="951" y="33">tcp</text>
            <rect class="blk blk-data" x="1050" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="1056" y="33">udp</text>
            <rect class="blk blk-data" x="1155" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="1161" y="33">saci</text>
            <rect class="blk blk-data" x="1260" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="1266" y="33">opentelemetry</text>
            <rect class="blk blk-data" x="1365" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="1371" y="33">channel</text>
            <rect class="blk blk-data" x="1470" y="8" width="100" height="40" rx="8"/>
            <text class="t-lbl" x="1476" y="33">mariadb</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M50 48 V72"/>
            <path class="arw arw-data" d="M155 48 V72"/>
            <path class="arw arw-data" d="M260 48 V72"/>
            <path class="arw arw-data" d="M365 48 V72"/>
            <path class="arw arw-data" d="M470 48 V72"/>
            <path class="arw arw-data" d="M575 48 V72"/>
            <path class="arw arw-data" d="M680 48 V72"/>
            <path class="arw arw-data" d="M785 48 V72"/>
            <path class="arw arw-data" d="M890 48 V72"/>
            <path class="arw arw-data" d="M995 48 V72"/>
            <path class="arw arw-data" d="M1100 48 V72"/>
            <path class="arw arw-data" d="M1205 48 V72"/>
            <path class="arw arw-data" d="M1310 48 V72"/>
            <path class="arw arw-data" d="M1415 48 V72"/>
            <path class="arw arw-data" d="M1520 48 V72"/>
            <path class="ln" d="M50 72 H1520"/>
            <text class="t-sm t-data" x="58" y="88">rows in, rows out</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M784 72 V104" marker-end="url(#cx-d1)"/>
            <rect class="blk blk-ctl" x="694" y="108" width="180" height="52" rx="8"/>
            <rect class="hd hd-ctl" x="694" y="108" width="180" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="694" y="120" width="180" height="8"/>
            <text class="t-lbl" x="706" y="123">workflow</text>
            <text class="t-sm" x="706" y="146">source, sink, link</text>
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
means the node names a declared [transformer](@/service/formats/_index.md); the seven row-carrying
connectors take none.

| Connector | Source | Sink | Needs a format | Run modes | In the default build |
|---|---|---|---|---|---|
| [File](@/service/connectors/file.md) | yes | yes | yes | any | yes |
| [HTTP](@/service/connectors/http.md) | yes | yes | yes | any | yes |
| [Kafka](@/service/connectors/kafka.md) | yes | yes | yes | stream, or any run mode with `stop_at_end` or `compacted` | no: build with `--features connector-kafka` |
| [NATS](@/service/connectors/nats.md) | yes | yes | yes | stream, or any run mode with `stop_at_end` | no: build with `--features connector-nats` |
| [PostgreSQL](@/service/connectors/postgresql.md) | yes | yes | no | any | no: build with `--features connector-postgresql` |
| [MariaDB](@/service/connectors/mariadb.md) | yes | yes | no | any | no: build with `--features connector-mariadb` |
| [Turso](@/service/connectors/turso.md) | yes | yes | no | any | no: build with `--features connector-turso` |
| [ClickHouse](@/service/connectors/clickhouse.md) | yes | yes | no | any | no: build with `--features connector-clickhouse` |
| [redb](@/service/connectors/redb.md) | yes | yes | yes | any | yes |
| [S3](@/service/connectors/s3.md) | yes | yes | yes | any | no: build with `--features connector-s3` |
| [TCP](@/service/connectors/tcp.md) | yes | yes | yes | stream for the source, any for the sink | yes |
| [UDP](@/service/connectors/udp.md) | yes | yes | yes | stream for the source, any for the sink | yes |
| [SACI](@/service/connectors/saci.md) | yes | yes | no | stream for the source, any for the sink | yes |
| [OpenTelemetry](@/service/connectors/opentelemetry.md) | yes | yes | no | stream for the source, any for the sink | no: build with `--features connector-opentelemetry` |
| [Channel](@/service/connectors/channel.md) | yes | yes | no | any | yes |

A node picks its transport with a `type` string: `FileSource` and `FileSink`, `HttpSource` and
`HttpSink`, `KafkaSource` and `KafkaSink`, `NatsSource` and `NatsSink`, `PostgresSource` and
`PostgresSink`, `MariadbSource` and `MariadbSink`, `TursoSource` and `TursoSink`,
`ClickhouseSource` and `ClickhouseSink`, `RedbSource` and `RedbSink`, `S3Source` and `S3Sink`,
`OpenTelemetryMetricsSource` and `OpenTelemetryMetricsSink`, `ChannelSource` and `ChannelSink`,
`tcp` in lower case for both halves of TCP, `udp` for both halves of UDP, and `saci` for both
halves of a service-to-service link.

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

The `type` names every connector but PostgreSQL, MariaDB and Turso accepts are `Boolean`, `Int8`, `Int16`,
`Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `Float64`, `Utf8`, `LargeUtf8`,
`Binary`, `Date32` and `Date64`, in any case. PostgreSQL, MariaDB and Turso each read their own
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
