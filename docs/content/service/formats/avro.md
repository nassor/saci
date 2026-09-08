+++
title = "avro"
description = "Container files on the stream surface, single-object or Confluent framing on messages."
template = "subpage.html"
weight = 4
aliases = ["/transformers/avro/"]
[[extra.facts]]
label = "Reads a stream"
value = "yes"
[[extra.facts]]
label = "Writes a stream"
value = "yes"
[[extra.facts]]
label = "Per-message"
value = "One message per row"
[[extra.facts]]
label = "Needs schema_fields"
value = "Optional reading a file, required writing and on messages"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
Avro object container files on a stream, and framed single records on a message transport. The two
surfaces disagree about the schema, because a container file carries its own header and a message
does not.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 132" role="img" aria-labelledby="av-t av-d">
        <title id="av-t">An avro transformer decoding bytes into rows and encoding rows back into bytes</title>
        <desc id="av-d">Bytes on the left and rows on the right, with a transformer node in the middle whose format is avro. The upper arrow runs left to right and is the decode path, which reads a container file's own header or a framed message. The lower arrow runs right to left and is the encode path, which needs a declared schema. The schema_id option selects the framing.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="150" height="64" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="150" height="8"/>
            <text class="t-lbl" x="12" y="49">bytes</text>
            <text class="t-sm" x="12" y="70">container file,</text>
            <text class="t-sm" x="12" y="86">or one framed row</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M150 50 H254" marker-end="url(#av-a)"/>
            <text class="t-sm t-mid" x="202" y="42">decode</text>
            <rect class="blk blk-ctl" x="258" y="26" width="144" height="80" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="26" width="144" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="38" width="144" height="8"/>
            <text class="t-lbl" x="270" y="41">transformer</text>
            <text class="t-sm t-ctl" x="270" y="62">format="avro"</text>
            <text class="t-sm" x="270" y="80">options compression</text>
            <text class="t-sm" x="270" y="96">options schema_id</text>
            <path class="arw arw-data" d="M254 86 H150" marker-end="url(#av-a)"/>
            <text class="t-sm t-mid" x="202" y="104">encode</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M406 50 H510" marker-end="url(#av-a)"/>
            <path class="arw arw-data" d="M510 86 H406" marker-end="url(#av-a)"/>
            <rect class="blk blk-data" x="514" y="34" width="146" height="64" rx="8"/>
            <rect class="hd hd-data" x="514" y="34" width="146" height="20" rx="8"/>
            <rect class="hd hd-data" x="514" y="46" width="146" height="8"/>
            <text class="t-lbl" x="526" y="49">rows</text>
            <text class="t-sm" x="526" y="70">id    Int64</text>
            <text class="t-sm" x="526" y="86">price Float64</text>
        </g>
        <defs>
            <marker id="av-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> bytes and rows</span>
        <span class="k-control"><i></i> the declared format</span>
    </div>
</div>

## 1. Declare it

<div class="code">
<div class="code-cap"><span>KDL</span><em>one format, two declared instances: a compressed container file and a Confluent framed topic</em></div>

```kdl
transformer "avro_file" format="avro" {
    options compression="zstd"
}

transformer "avro_topic" format="avro" {
    options schema_id=42
}
```

</div>

## 2. Name it from a source or sink

<div class="code">
<div class="code-cap"><span>KDL</span><em>the file source declares no columns; the topic source declares them</em></div>

```kdl
source "orders_in" type="FileSource" component="Order" transformer="avro_file" {
    config path="/data/orders.avro"
}

source "orders_stream" type="KafkaSource" component="Order" transformer="avro_topic" {
    config {
        brokers "localhost:9092"
        topic "orders-raw"
        stop_at_end #true

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

Any byte-carrying connector names it the same way: see
[Sources and sinks](@/service/connectors/_index.md).

## 3. Give it a schema

Reading a container file needs no declared schema, and the file's header carries it. Declare one and
it becomes a projection: a column the container does not carry is an error, and a column it carries
and the declaration does not name is dropped.

Writing a file needs one, and so does either direction on messages. The decoder does two jobs with
it: it becomes the Avro form every payload is decoded against, and it is the target the decoded
rows are cast to. The encoder never reads it, and writes each batch under the batch's own schema.

That cast matters because Avro has no narrow integer of its own, so a narrow column travels as a
wider `int`. A value that does not fit the declared column is an error rather than a null.

## Every option

| Key | Type | Default | What it does |
|---|---|---|---|
| `compression` | string | `null` | container-file compression: `null`, `deflate`, `snappy` or `zstd` |
| `schema_id` | integer | absent | the Confluent registry id: the encoder frames every message under it, and the decoder accepts a Confluent framed payload only when it is set; it must fit in 32 bits |

## How it behaves

Two framings exist on the message surface, and `schema_id` picks which one this transformer writes.
Without it the encoder emits single-object framing; with it, the Confluent framing carrying that
registry id.

Decoding is looser than encoding. Single-object framing is always accepted. The Confluent framing is
accepted only when `schema_id` is set, so a topic that carries both framings needs the option set
and reads either one.

`compression` applies to the container writer alone and changes nothing on a message.

## When it refuses to start

| Message | What to change |
|---|---|
| `avro: option 'compression' must be a string` | quote the value |
| `avro: option 'compression' must be one of null, deflate, snappy, zstd` | pick one of those four |
| `avro: option 'schema_id' must be an integer` | write a bare number |
| `avro: option 'schema_id' must fit in a u32` | use the registry id, which is a 32-bit number |
| `avro: the declared schema has no Avro form: {e}` | replace the column type the message names with one Avro can express |

Some errors wait for a payload rather than the config check:
`avro: payload carries the Confluent prefix; set option 'schema_id' to its registry id`,
`avro: payload is not framed; expected single-object encoding (0xC3 0x01) or the Confluent prefix
(0x00)`, and `avro: casting to the declared schema: {e}` for a value that does not fit its declared
column, or for a declared column the container or payload does not carry.

## Next

- [Formats](@/service/formats/_index.md), and the other four.
- [Kafka](@/service/connectors/kafka.md), the transport this page reads from.
