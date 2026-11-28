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
label = "Needs field lines"
value = "Optional reading a file, required writing and on messages"
[[extra.facts]]
label = "In the server build"
value = "yes"
+++
Avro object container files on a stream, and framed single records on a message transport. The two
surfaces disagree about the schema, because a container file carries its own header and a message
does not.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 132" role="img" aria-labelledby="av-t av-d">
        <title id="av-t">An avro codec decoding bytes into rows and encoding rows back into bytes</title>
        <desc id="av-d">Bytes on the left and rows on the right, with a codec in the middle whose format is avro. The upper arrow runs left to right and is the decode path, which reads a container file's own header or a framed message. The lower arrow runs right to left and is the encode path, which needs a declared schema. The schema_id option selects the framing.</desc>
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
            <text class="t-lbl" x="270" y="41">codec</text>
            <text class="t-sm t-ctl" x="270" y="62">format="avro"</text>
            <text class="t-sm" x="270" y="80">config compression</text>
            <text class="t-sm" x="270" y="96">config schema_id</text>
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
            <text class="t-sm" x="526" y="70">id    int64</text>
            <text class="t-sm" x="526" y="86">price float64</text>
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
codec "avro_file" format="avro" {
    config compression="zstd"
}

codec "avro_topic" format="avro" {
    config schema_id=42
}
```

</div>

## 2. Name it from a source or sink

<div class="code">
<div class="code-cap"><span>KDL</span><em>the file source declares no columns; the topic source declares them</em></div>

```kdl
source "orders_in" connector="file" component="Order" codec="avro_file" {
    config path="/data/orders.avro"
}

source "orders_stream" connector="kafka" component="Order" codec="avro_topic" {
    config {
        brokers "localhost:9092"
        topic "orders-raw"
        stop_at_end #true

        field "id" type="int64" nullable=#false
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
| `schema_id` | integer | absent | static 32-bit Confluent registry id used when `registry_url` is unset |
| `registry_url` | string | absent | base URL for an external Confluent, Apicurio v2, or AWS Glue schema registry |
| `registry_kind` | string | `"confluent"` | registry protocol dialect: `"confluent"`, `"apicurio"`, or `"glue"` |
| `subject` | string | absent | subject (Confluent) or artifact id (Apicurio) used to register or resolve schemas on write |
| `auto_register` | bool | `#true` | register unseen writer schemas on encode (`#true`) or resolve an existing registration (`#false`) |
| `compatibility` | string | `"NONE"` | compatibility mode checked against previous versions: `NONE`, `BACKWARD`, `BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE` |
| `apicurio_group` | string | `"default"` | Apicurio artifact group id |
| `apicurio_eight_byte_id` | bool | `#false` | frame Apicurio messages with `0x00` + 8-byte big-endian ID instead of 4-byte ID |
| `glue_registry_name` | string | `"default-registry"` | AWS Glue registry name |
| `glue_schema_name` | string | `subject` | AWS Glue schema name |
| `aws_region` | string | `"us-east-1"` | AWS region used for Glue SigV4 signing (reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`) |
| `auth_basic_user` | string | absent | HTTP Basic username (e.g. Confluent Cloud API key) |
| `auth_basic_password` | string | `""` | HTTP Basic password / secret |
| `auth_bearer_token` | string | absent | HTTP `Authorization: Bearer` token |

## External schema registry

Set `registry_url` to resolve writer schemas dynamically by wire prefix on decode (5-byte Confluent `0x00` + `u32`, 5-byte or 9-byte Apicurio `0x00` + `u32`/`u64`, or 18-byte AWS Glue `0x03 0x00` + UUID) and evolve decoded batches to the workflow's declared target schema. On encode, `AvroCodec` checks compatibility according to `compatibility` and caches registered schema IDs.

```kdl,name=Confluent Schema Registry with backward compatibility
codec "orders_avro" format="avro" {
    config {
        registry_url "https://registry.example.com"
        registry_kind "confluent"
        subject "orders-value"
        auto_register #true
        compatibility "BACKWARD"
        auth_basic_user "${SR_KEY}"
        auth_basic_password "${SR_SECRET}"
    }
}
```

## How it behaves

Two framings exist on the message surface, and `schema_id` picks which one this codec writes.
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
| `avro: unknown option '{key}'` | remove the key; `compression` and `schema_id` are the only two |

Some errors wait for a payload rather than the config check:
`avro: payload carries the Confluent prefix; set option 'schema_id' to its registry id`,
`avro: payload is not framed; expected single-object encoding (0xC3 0x01) or the Confluent prefix
(0x00)`, and `avro: casting to the declared schema: {e}` for a value that does not fit its declared
column, or for a declared column the container or payload does not carry.

## Next

- [Formats](@/service/formats/_index.md), and the other four.
- [Kafka](@/service/connectors/kafka.md), the transport this page reads from.
