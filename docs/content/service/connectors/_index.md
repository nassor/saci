+++
title = "Sources and sinks"
description = "The connectors a workflow reads from and writes to, one page per transport, with the config node each accepts."
template = "section.html"
sort_by = "weight"
weight = 4
aliases = ["/connectors/"]
+++

A workflow reads rows through a `source` node and writes them through a `sink` node. Sixteen
connectors ship with `saci-service`, and the pages below walk each transport from an empty config file to a
running pipeline.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 242" role="img" aria-labelledby="cx-t cx-d">
        <title id="cx-t">A source node turns a transport into rows for the workflow, and a sink node turns rows back into a transport</title>
        <desc id="cx-d">Two lanes run left to right. In the top lane, a byte transport (file, http, http-listen, kafka, nats, redb, s3, tcp or udp) hands bytes to a codec, which decodes them into rows for the workflow. In the bottom lane, a row transport (postgresql, mariadb, turso, clickhouse, peer, channel or otlp-metrics) hands rows to the workflow directly, with no format. The right half mirrors the left: the workflow's rows pass through a codec to a byte transport, or go straight to a row transport. A bracket over the left half names it the source node, and one over the right half the sink node.</desc>
        <g class="anim anim-1">
            <text class="t-lbl t-data t-mid" x="126" y="14">source node</text>
            <text class="t-sm t-mid" x="126" y="28">a connector, plus a codec for bytes</text>
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
            <text class="t-sm" x="10" y="180">postgresql  mariadb</text>
            <text class="t-sm" x="10" y="194">turso  clickhouse</text>
            <text class="t-sm" x="10" y="208">peer  channel</text>
            <text class="t-sm" x="10" y="222">otlp-metrics</text>
            <path class="arw arw-data" d="M140 89 H158" marker-end="url(#cx-a)"/>
            <rect class="blk blk-bnd" x="162" y="52" width="90" height="74" rx="8"/>
            <rect class="hd hd-bnd" x="162" y="52" width="90" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="162" y="64" width="90" height="6"/>
            <text class="t-lbl" x="170" y="66">codec</text>
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
            <text class="t-sm t-mid" x="534" y="28">a connector, plus a codec for bytes</text>
            <path class="ln" d="M408 42 V36 H660 V42"/>
            <path class="arw arw-data" d="M386 89 H404" marker-end="url(#cx-a)"/>
            <rect class="blk blk-bnd" x="408" y="52" width="90" height="74" rx="8"/>
            <rect class="hd hd-bnd" x="408" y="52" width="90" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="408" y="64" width="90" height="6"/>
            <text class="t-lbl" x="416" y="66">codec</text>
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
            <text class="t-sm" x="530" y="180">postgresql  mariadb</text>
            <text class="t-sm" x="530" y="194">turso  clickhouse</text>
            <text class="t-sm" x="530" y="208">peer  channel</text>
            <text class="t-sm" x="530" y="222">otlp-metrics</text>
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
        <span class="k-boundary"><i></i> codecs</span>
        <span class="k-control"><i></i> the workflow</span>
    </div>
</div>

## Pick a connector

Each connector provides a source, a sink or both, and the `Source` and `Sink` columns say which.
`Needs a format` means the node names a declared codec, a [`codec` node](@/service/formats/_index.md); the seven row-carrying
connectors take none.

| Connector | Source | Sink | Needs a format | Run modes | Feature | In the server build |
|---|---|---|---|---|---|---|
| [`file`](@/service/connectors/file.md) | yes | yes | yes | any | `connector-file` | yes |
| [`http`](@/service/connectors/http.md) | yes | yes | yes | any | `connector-http` | yes |
| [`http-listen`](@/service/connectors/http.md) | yes | no | yes | stream | `connector-http` | yes |
| [`kafka`](@/service/connectors/kafka.md) | yes | yes | yes | stream, or any run mode with `stop_at_end` or `compacted` | `connector-kafka` | no |
| [`nats`](@/service/connectors/nats.md) | yes | yes | yes | stream, or any run mode with `stop_at_end` | `connector-nats` | no |
| [`postgresql`](@/service/connectors/postgresql.md) | yes | yes | no | any | `connector-postgresql` | no |
| [`mariadb`](@/service/connectors/mariadb.md) | yes | yes | no | any | `connector-mariadb` | no |
| [`turso`](@/service/connectors/turso.md) | yes | yes | no | any | `connector-turso` | no |
| [`clickhouse`](@/service/connectors/clickhouse.md) | yes | yes | no | any | `connector-clickhouse` | no |
| [`redb`](@/service/connectors/redb.md) | yes | yes | yes | any | `connector-redb` | yes |
| [`s3`](@/service/connectors/s3.md) | yes | yes | yes | any | `connector-s3` | no |
| [`tcp`](@/service/connectors/tcp.md) | yes | yes | yes | stream for the source, any for the sink | `connector-tcp` | yes |
| [`udp`](@/service/connectors/udp.md) | yes | yes | yes | stream for the source, any for the sink | `connector-udp` | yes |
| [`peer`](@/service/connectors/peer.md) | yes | yes | no | stream for the source, any for the sink | `connector-peer` | yes |
| [`otlp-metrics`](@/service/connectors/otlp-metrics.md) | yes | yes | no | stream for the source, any for the sink | `connector-otlp` | no |
| [`channel`](@/service/connectors/channel.md) | yes | yes | no | any | `connector-channel` | yes |

The `server` feature includes every connector marked `yes`. A connector marked `no` needs its
feature added to the build, for example `--features server,connector-kafka`.

A node picks its connector with the `connector` property, which holds the lowercase name from the
first column: `file`, `http`, `http-listen`, `kafka`, `nats`, `postgresql`, `mariadb`, `turso`,
`clickhouse`, `redb`, `s3`, `otlp-metrics`, `channel`, `tcp`, `udp` or `peer`, the link between two
services. The node kind, `source` or `sink`, gives the direction. A name no connector registers is
refused with `no connector named '{name}' is registered`, and a name whose connector lacks that
role with `connector '{name}' has no source` or `connector '{name}' has no sink`.

## The shape every node shares

A `source` or `sink` node carries the same five properties whatever the transport:

| Property | What it does |
|---|---|
| `connector` | the connector, from the table above |
| `component` | the row type the node fills on the way in, or drains on the way out |
| `codec` | the id of a declared `codec` node, on every connector that moves bytes |
| `retry` | an optional child overriding the retry policy for this node |
| `config` | the child node holding everything the connector itself reads |

`codec` is a property of the node, never a key inside `config`. The format is resolved before
the transport is built, so a byte-carrying node with no `codec` is rejected at load time
rather than at the first batch.

`field` lines live inside `config`, one per column, and the format decides what they mean:
required, optional, or a projection target for the self-describing formats, which read only those
columns. Each connector page states which of the two ends needs them. A `field` line takes the column
name as its leading argument, then `type`, `nullable`, which defaults to `#true`, and the keys
`precision` and `scale` for `decimal128`, `item` for `list` and `native_type` for a database
connector. An unknown key on a `field` line is an error.

The `type` names are lower case: `boolean`, `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`,
`uint32`, `uint64`, `float32`, `float64`, `utf8`, `largeutf8`, `json`, `binary`, `date32`, `date64`,
`time64_micros`, `timestamp_micros`, `timestamp_micros_utc`, `timestamp_millis`, `uuid`,
`decimal128`, `interval_month_day_nano` and `list`. `decimal128` needs `precision` and `scale`, and
`list` needs `item`. `postgresql`, `mariadb`, `turso` and `clickhouse` each map their own subset,
listed on their pages, and refuse any other with `{connector}: field '{name}' has type '{ty}',
which this connector does not map`.

A workflow can declare a schema once and share it. A `schema` node inside the `workflow` holds
`field` lines, and a source or sink whose `component` equals the schema id and that declares no
`field` lines of its own takes that schema's fields. A node that declares its own `field` lines keeps
them. Schema ids are unique within a workflow, and each schema holds at least one valid `field`.

<div class="code">
<div class="code-cap"><span>KDL</span><em>the source takes the orders schema because its component matches</em></div>

```kdl
workflow "orders" {
    schema "orders" {
        field "id" type="int64" nullable=#false
        field "status" type="utf8"
    }
    source "orders_in" connector="file" component="orders" codec="orders_csv" {
        config path="/data/orders.csv"
    }
}
```

</div>

A failed write retries with exponential backoff before the error reaches the runner: four attempts,
a 100 ms base, 2.0x growth, a 30 s cap and 0.1 jitter. A `retry` child overrides those per node, and
`max_attempts=1` turns retrying off. A source takes the same policy in every run mode except
`stream`, where the runner already re-polls a failed source itself.
[Workflows and links](@/service/config/workflows.md) has the key table.

How many rows a source hands over per pass is not a connector key. The runner asks each source for
an admission target and resizes it as the pipeline runs. `batch_rows` and the keys like it
therefore only take effect when the runner sends no hint.
[Flow control](@/service/operate/flow-control.md) says when that is.

Teaching `saci-service` a transport it does not ship means writing a `Connector` in Rust, which
provides a source, a sink or both, and registering it in your own binary with
`ServiceBuilder::connector`. [Writing a connector](@/library/service/connectors.md) is that
route. The binary is a crate that depends on `saci` with the `service` feature, plus the built-in
connectors it keeps:

```toml
[dependencies]
saci = { version = "0.1", default-features = false, features = ["service", "connector-file", "codec-csv"] }
```

Its `main` hands the connector to the same `serve` and `validate` subcommands as `saci-service`:

```rust
fn main() -> std::process::ExitCode {
    saci::service::cli::main_with(|builder| builder.connector(MyConnector))
}
```

## Next

- [Formats](@/service/formats/_index.md), the byte formats a `codec` node names.
- [Workflows and links](@/service/config/workflows.md), the nodes these sources and sinks sit in.
