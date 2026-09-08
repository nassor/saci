+++
title = "ndjson"
description = "One JSON object per line, on files and on messages alike, and the only format that infers a schema."
template = "subpage.html"
weight = 2
aliases = ["/transformers/ndjson/"]
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
value = "Optional reading a stream, required on messages"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
Newline delimited JSON, one object per row, on files and on messages alike. It is the one format
that can work out its own schema.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 132" role="img" aria-labelledby="nd-t nd-d">
        <title id="nd-t">An ndjson transformer decoding bytes into rows and encoding rows back into bytes</title>
        <desc id="nd-d">Bytes on the left and rows on the right, with a transformer node in the middle whose format is ndjson. The upper arrow runs left to right and is the decode path, which infers a schema when none is declared. The lower arrow runs right to left and is the encode path, one JSON object per row.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="150" height="64" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="150" height="8"/>
            <text class="t-lbl" x="12" y="49">bytes</text>
            <text class="t-sm" x="12" y="70">{"id":1,</text>
            <text class="t-sm" x="12" y="86"> "price":99.5}</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M150 50 H254" marker-end="url(#nd-a)"/>
            <text class="t-sm t-mid" x="202" y="42">decode</text>
            <rect class="blk blk-ctl" x="258" y="26" width="144" height="80" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="26" width="144" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="38" width="144" height="8"/>
            <text class="t-lbl" x="270" y="41">transformer</text>
            <text class="t-sm t-ctl" x="270" y="62">format="ndjson"</text>
            <text class="t-sm" x="270" y="80">options</text>
            <text class="t-sm" x="270" y="96">infer_max</text>
            <path class="arw arw-data" d="M254 86 H150" marker-end="url(#nd-a)"/>
            <text class="t-sm t-mid" x="202" y="104">encode</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M406 50 H510" marker-end="url(#nd-a)"/>
            <path class="arw arw-data" d="M510 86 H406" marker-end="url(#nd-a)"/>
            <rect class="blk blk-data" x="514" y="34" width="146" height="64" rx="8"/>
            <rect class="hd hd-data" x="514" y="34" width="146" height="20" rx="8"/>
            <rect class="hd hd-data" x="514" y="46" width="146" height="8"/>
            <text class="t-lbl" x="526" y="49">rows</text>
            <text class="t-sm" x="526" y="70">id    Int64</text>
            <text class="t-sm" x="526" y="86">price Float64</text>
        </g>
        <defs>
            <marker id="nd-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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
<div class="code-cap"><span>KDL</span><em>the options child is optional; infer_max is its one key</em></div>

```kdl
transformer "orders_json" format="ndjson" {
    options infer_max=4096
}
```

</div>

## 2. Name it from a source or sink

<div class="code">
<div class="code-cap"><span>KDL</span><em>a topic of JSON records, decoded against the declared columns</em></div>

```kdl
source "orders_in" type="KafkaSource" component="Order" transformer="orders_json" {
    config {
        brokers "localhost:9092"
        topic "orders-raw"
        group_id "saci-orders"
        stop_at_end #true

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

Any byte-carrying connector names it the same way: see
[Sources and sinks](@/service/connectors/_index.md).

## 3. The schema it infers

`schema_fields` is optional on a stream read. Leave it out and the reader works the schema out from
the first `infer_max` records; it still sees every record afterwards:

<div class="code">
<div class="code-cap"><span>KDL</span><em>a file source with no declared columns at all</em></div>

```kdl
transformer "orders_json" format="ndjson" {
    options infer_max=4096
}

source "orders_in" type="FileSource" component="Order" transformer="orders_json" {
    config path="/data/orders.ndjson"
}
```

</div>

On messages the declared schema is required and builds the decoder directly. There is no inference
there, because one decoder is created and then fed payloads one at a time.

## Every option

| Key | Type | Default | What it does |
|---|---|---|---|
| `infer_max` | integer | `1024` | how many records schema inference reads; at least 1 |

## How it behaves

Encoding emits one payload per row and carries no line terminator of its own, so a message is one
JSON object with nothing after it.

Decoding accepts a payload with or without a trailing newline, and a payload that carries several
objects decodes to several rows.

## When it refuses to start

| Message | What to change |
|---|---|
| `ndjson: option 'infer_max' must be an integer` | write `infer_max=4096`, not a string |
| `ndjson: option 'infer_max' must be at least 1` | raise `infer_max` to 1 or more |

## Next

- [Formats](@/service/formats/_index.md), and the other four.
- [Kafka](@/service/connectors/kafka.md), the transport this page reads from.
