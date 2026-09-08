+++
title = "Formats"
description = "A connector moves bytes, a transformer gives them meaning. A declared transformer node picks csv, ndjson, parquet, avro or arrow-ipc."
template = "section.html"
sort_by = "weight"
weight = 5
aliases = ["/transformers/"]
+++

A connector owns a transport: a path, a socket, a topic, a bucket. Giving those bytes meaning is a
`transformer` node's job, and five formats ship with `saci-service`: `csv`, `ndjson`, `parquet`,
`avro` and `arrow-ipc`.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 196" role="img" aria-labelledby="tf-a-title tf-a-desc">
        <title id="tf-a-title">Bytes entering a file source, decoded by a transformer, arriving at the workflow as rows</title>
        <desc id="tf-a-desc">A CSV file of bytes enters a FileSource node, which reads it in batches rather than whole. Inside that node the declared transformer, format csv, decodes the bytes one batch at a time. The node forwards each batch to the workflow, whose columns are typed Int64 and Float64. Naming a different transformer replaces the inner box and nothing else.</desc>
        <text class="t-title" x="0" y="14">One file, one format</text>
        <text class="t-sm" x="0" y="30">path = "orders.csv", transformer = "orders_csv"</text>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="52" width="112" height="88" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="112" height="22" rx="8"/>
            <rect class="hd hd-data" x="0" y="64" width="112" height="10"/>
            <text class="t-lbl t-data" x="10" y="67">orders.csv</text>
            <text class="t-sm" x="10" y="92">id,price</text>
            <text class="t-sm" x="10" y="106">1,99.50</text>
            <text class="t-sm" x="10" y="120">2,240.00</text>
            <path class="arw arw-data" d="M112 96 H150" marker-end="url(#tf-d)"/>
            <text class="t-sm" x="114" y="88">bytes</text>
            <rect class="blk blk-data" x="158" y="44" width="316" height="132" rx="8"/>
            <rect class="hd hd-data" x="158" y="44" width="316" height="22" rx="8"/>
            <rect class="hd hd-data" x="158" y="56" width="316" height="10"/>
            <text class="t-lbl t-data" x="168" y="59">FileSource</text>
            <text class="t-sm" x="168" y="82">reads in batches, never whole</text>
            <text class="t-sm" x="168" y="168">the transport: open, read, send</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="178" y="90" width="276" height="66" rx="8"/>
            <rect class="hd hd-bnd" x="178" y="90" width="276" height="22" rx="8"/>
            <rect class="hd hd-bnd" x="178" y="102" width="276" height="10"/>
            <text class="t-lbl t-bnd" x="188" y="105">transformer format="csv"</text>
            <text class="t-sm" x="188" y="126">decodes the bytes</text>
            <text class="t-sm" x="188" y="146">one batch at a time</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M474 110 H540" marker-end="url(#tf-d)"/>
            <text class="t-sm t-data" x="478" y="102">rows</text>
            <rect class="blk blk-data" x="548" y="44" width="106" height="132" rx="8"/>
            <rect class="hd hd-data" x="548" y="44" width="106" height="22" rx="8"/>
            <rect class="hd hd-data" x="548" y="56" width="106" height="10"/>
            <text class="t-lbl t-data" x="558" y="59">the workflow</text>
            <rect class="row" x="556" y="78" width="90" height="18" rx="3"/>
            <text class="t-sm" x="562" y="91">id    Int64</text>
            <rect class="row" x="556" y="100" width="90" height="18" rx="3"/>
            <text class="t-sm" x="562" y="113">price Float64</text>
            <path class="ln" d="M556 128 H646"/>
            <text class="t-sm t-data" x="562" y="144">2 rows</text>
            <text class="t-sm" x="562" y="166">appended whole</text>
        </g>
        <defs>
            <marker id="tf-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> bytes and batches</span>
        <span class="k-boundary"><i></i> the format boundary</span>
    </div>
    <figcaption class="dgm-cap">
        Point <code>transformer</code> at a <code>parquet</code> node instead and only the inner
        box changes. The path, the node and the rows the workflow receives stay exactly as they
        are.
    </figcaption>
</div>

## Declare a transformer

A workflow declares each byte format it needs as its own `transformer` node. The node's leading
argument is its id, `format` names the format, and an `options` child holds that format's own
settings. Every source or sink that moves bytes names one declared transformer through its own
`transformer` key.

There is no default and no sniffing. A file connector does not read the extension, and Kafka,
NATS and TCP each name a transformer like everything else.

<div class="code">
<div class="code-cap"><span>KDL</span><em>options belongs to the format, the transformer key to the node; both nodes sit inside one workflow</em></div>

```kdl
transformer "orders_csv" format="csv" {
    options has_headers=#true
}

source "orders_in" type="FileSource" component="Order" transformer="orders_csv" {
    config path="/data/orders.csv" {
        schema_fields "id" type="Int64" nullable=#false
    }
}
```

</div>

Each format defines its own `options` keys, and its page lists them with their defaults. Omit the
`options` child and every option keeps its default. Two `transformer` nodes may name the same
`format` with different options, so one workflow reads Confluent framed Avro on one topic and
single-object Avro on another.

## Pick a format

| `format` | Reads a stream | Writes a stream | Per-message | Declared schema |
|---|---|---|---|---|
| [`csv`](@/service/formats/csv.md) | yes | yes | one per row | required |
| [`ndjson`](@/service/formats/ndjson.md) | yes | yes | one per row | optional reading a stream, required on messages |
| [`parquet`](@/service/formats/parquet.md) | yes | yes | one per batch | optional reading a file, required writing and on messages |
| [`avro`](@/service/formats/avro.md) | yes | yes | one per row | optional reading a file, required writing and on messages |
| [`arrow-ipc`](@/service/formats/arrow-ipc.md) | yes | yes | one per batch | optional reading a stream, required writing and on messages |

All five are in the default build, so no format needs a build of its own.

The `Declared schema` column is about a source. A sink requires `schema_fields` whatever the format,
because that is the schema the rows are written with.

A self-describing format reads the source's own schema when a source declares none, so every column
arrives. Declare one and it becomes a projection: the columns it names are delivered, a column it
names that the source does not carry is an error, and the rest are dropped.

`Per-message` decides how a batch splits on the way out. One message per row is what a Kafka
`key_field` or a NATS `subject_field` needs, because each payload then has one row to take its key
from. One message per batch carries the whole batch in a single payload.

## Which connectors carry which formats

Every connector that moves bytes accepts all five formats. Which surface it uses is what changes:

| Connector | Surface | Formats | What to watch |
|---|---|---|---|
| [File](@/service/connectors/file.md), [HTTP](@/service/connectors/http.md), [S3](@/service/connectors/s3.md) | one whole stream | all five | a self-describing format reads the stream's own schema or projects onto `schema_fields`; `schema_from "body"` on HTTP and `schema_from "object"` on S3 read the stream's own and check it |
| [Kafka](@/service/connectors/kafka.md), [NATS](@/service/connectors/nats.md) | discrete messages | all five | the per-row keys need csv, ndjson or avro; parquet and arrow-ipc emit one message per batch |
| [TCP](@/service/connectors/tcp.md) | discrete messages, one per frame | all five | arrow-ipc is the natural pairing, one frame per batch |

[PostgreSQL](@/service/connectors/postgresql.md) and
[Channel](@/service/connectors/channel.md) carry rows rather than bytes and name no transformer at
all.

The same bytes work on either surface, so a file written with `arrow-ipc` or `parquet` decodes
through the message path and one Kafka payload in those formats opens as a stream.

Teaching `saci-service` a format it does not ship means writing a transformer in Rust and
registering it in your own binary. [Writing a transformer](@/library/service/transformers.md)
shows how.

## Next

- [csv](@/service/formats/csv.md), the format most first pipelines start with.
- [Sources and sinks](@/service/connectors/_index.md), the transports that name a format.
