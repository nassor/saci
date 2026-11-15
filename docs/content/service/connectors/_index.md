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
    <div class="dgm-scroll"><svg viewBox="0 0 660 242" role="img" aria-labelledby="cx-t cx-d">
        <title id="cx-t">A source node turns a transport into rows for the workflow, and a sink node turns rows back into a transport</title>
        <desc id="cx-d">Two lanes run left to right. In the top lane, a byte transport (file, http, kafka, nats, redb, s3, tcp or udp) hands bytes to a transformer, which decodes them into rows for the workflow. In the bottom lane, a row transport (postgres, mariadb, turso, clickhouse, saci, channel or opentelemetry) hands rows to the workflow directly, with no format. The right half mirrors the left: the workflow's rows pass through a transformer to a byte transport, or go straight to a row transport. A bracket over the left half names it the source node, and one over the right half the sink node.</desc>
        <g class="anim anim-1">
            <text class="t-lbl t-data t-mid" x="126" y="14">source node</text>
            <text class="t-sm t-mid" x="126" y="28">a type, plus a transformer for bytes</text>
            <path class="ln" d="M0 42 V36 H252 V42"/>
            <rect class="blk blk-data" x="0" y="52" width="140" height="74" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="140" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="64" width="140" height="6"/>
            <text class="t-lbl" x="10" y="66">byte transports</text>
            <text class="t-sm" x="10" y="86">file  http  kafka</text>
            <text class="t-sm" x="10" y="100">nats  redb  s3</text>
            <text class="t-sm" x="10" y="114">tcp  udp</text>
            <rect class="blk blk-data" x="0" y="146" width="140" height="88" rx="8"/>
            <rect class="hd hd-data" x="0" y="146" width="140" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="158" width="140" height="6"/>
            <text class="t-lbl" x="10" y="160">row transports</text>
            <text class="t-sm" x="10" y="180">postgres  mariadb</text>
            <text class="t-sm" x="10" y="194">turso  clickhouse</text>
            <text class="t-sm" x="10" y="208">saci  channel</text>
            <text class="t-sm" x="10" y="222">opentelemetry</text>
            <path class="arw arw-data" d="M140 89 H158" marker-end="url(#cx-a)"/>
            <rect class="blk blk-bnd" x="162" y="52" width="90" height="74" rx="8"/>
            <rect class="hd hd-bnd" x="162" y="52" width="90" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="162" y="64" width="90" height="6"/>
            <text class="t-lbl" x="170" y="66">transformer</text>
            <text class="t-sm t-bnd" x="170" y="86">csv ndjson</text>
            <text class="t-sm t-bnd" x="170" y="100">avro parquet</text>
            <text class="t-sm t-bnd" x="170" y="114">arrow-ipc</text>
            <path class="arw arw-data" d="M252 89 H270" marker-end="url(#cx-a)"/>
            <text class="t-sm t-mid" x="207" y="184">no format</text>
            <path class="arw arw-data" d="M140 190 H270" marker-end="url(#cx-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="274" y="52" width="112" height="182" rx="8"/>
            <rect class="hd hd-ctl" x="274" y="52" width="112" height="18" rx="8"/>
            <rect class="hd hd-ctl" x="274" y="64" width="112" height="6"/>
            <text class="t-lbl" x="284" y="66">workflow</text>
            <text class="t-lbl t-mid" x="330" y="138">processors</text>
            <text class="t-sm t-ctl t-mid" x="330" y="154">rows in, rows out</text>
        </g>
        <g class="anim anim-3">
            <text class="t-lbl t-data t-mid" x="534" y="14">sink node</text>
            <text class="t-sm t-mid" x="534" y="28">a type, plus a transformer for bytes</text>
            <path class="ln" d="M408 42 V36 H660 V42"/>
            <path class="arw arw-data" d="M386 89 H404" marker-end="url(#cx-a)"/>
            <rect class="blk blk-bnd" x="408" y="52" width="90" height="74" rx="8"/>
            <rect class="hd hd-bnd" x="408" y="52" width="90" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="408" y="64" width="90" height="6"/>
            <text class="t-lbl" x="416" y="66">transformer</text>
            <text class="t-sm t-bnd" x="416" y="86">csv ndjson</text>
            <text class="t-sm t-bnd" x="416" y="100">avro parquet</text>
            <text class="t-sm t-bnd" x="416" y="114">arrow-ipc</text>
            <path class="arw arw-data" d="M498 89 H516" marker-end="url(#cx-a)"/>
            <rect class="blk blk-data" x="520" y="52" width="140" height="74" rx="8"/>
            <rect class="hd hd-data" x="520" y="52" width="140" height="18" rx="8"/>
            <rect class="hd hd-data" x="520" y="64" width="140" height="6"/>
            <text class="t-lbl" x="530" y="66">byte transports</text>
            <text class="t-sm" x="530" y="86">file  http  kafka</text>
            <text class="t-sm" x="530" y="100">nats  redb  s3</text>
            <text class="t-sm" x="530" y="114">tcp  udp</text>
            <text class="t-sm t-mid" x="453" y="184">no format</text>
            <path class="arw arw-data" d="M386 190 H516" marker-end="url(#cx-a)"/>
            <rect class="blk blk-data" x="520" y="146" width="140" height="88" rx="8"/>
            <rect class="hd hd-data" x="520" y="146" width="140" height="18" rx="8"/>
            <rect class="hd hd-data" x="520" y="158" width="140" height="6"/>
            <text class="t-lbl" x="530" y="160">row transports</text>
            <text class="t-sm" x="530" y="180">postgres  mariadb</text>
            <text class="t-sm" x="530" y="194">turso  clickhouse</text>
            <text class="t-sm" x="530" y="208">saci  channel</text>
            <text class="t-sm" x="530" y="222">opentelemetry</text>
        </g>
        <defs>
            <marker id="cx-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> transports and rows</span>
        <span class="k-boundary"><i></i> transformers</span>
        <span class="k-control"><i></i> the workflow</span>
    </div>
</div>

## Pick a connector

Every row is a source and a sink, so one transport serves both ends of a workflow. `Needs a format`
means the node names a declared [transformer](@/service/formats/_index.md); the seven row-carrying
connectors take none.

| Connector | Source | Sink | Needs a format | Run modes | In the server build |
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
route. The binary is a crate that depends on `saci` with the `service` feature, plus the built-in
connectors it keeps:

```toml
[dependencies]
saci = { version = "0.1", default-features = false, features = ["service", "connector-file", "transformer-csv"] }
```

Its `main` hands the factory to the same `serve` and `validate` subcommands as `saci-service`:

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main_with(|builder| builder.register_source(MyFactory))
}
```

## Next

- [Formats](@/service/formats/_index.md), the byte formats a `transformer` node names.
- [Workflows and links](@/service/config/workflows.md), the nodes these sources and sinks sit in.
