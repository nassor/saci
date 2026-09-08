+++
title = "arrow-ipc"
description = "Arrow's own stream format on both surfaces: one message per batch, and one stream per file."
template = "subpage.html"
weight = 5
aliases = ["/transformers/arrow-ipc/"]
[[extra.facts]]
label = "Reads a stream"
value = "yes"
[[extra.facts]]
label = "Writes a stream"
value = "yes"
[[extra.facts]]
label = "Per-message"
value = "One message per batch"
[[extra.facts]]
label = "Needs schema_fields"
value = "Optional reading a stream, required writing and on messages"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
Arrow's own stream format, over a message transport and as whole files. There is no decoding cost
worth naming, because the payload already holds the columns the workflow works on.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 132" role="img" aria-labelledby="ai-t ai-d">
        <title id="ai-t">An arrow-ipc transformer decoding a stream into rows and encoding rows back into a stream</title>
        <desc id="ai-d">Bytes on the left and rows on the right, with a transformer node in the middle whose format is arrow-ipc. The upper arrow runs left to right and is the decode path, which reads the schema out of the stream header. The lower arrow runs right to left and is the encode path, which writes one whole stream per call. One message is one whole stream.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="150" height="64" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="150" height="8"/>
            <text class="t-lbl" x="12" y="49">bytes</text>
            <text class="t-sm" x="12" y="70">schema header,</text>
            <text class="t-sm" x="12" y="86">batch, end marker</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M150 50 H254" marker-end="url(#ai-a)"/>
            <text class="t-sm t-mid" x="202" y="42">decode</text>
            <rect class="blk blk-ctl" x="258" y="26" width="144" height="80" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="26" width="144" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="38" width="144" height="8"/>
            <text class="t-lbl" x="270" y="41">transformer</text>
            <text class="t-sm t-ctl" x="270" y="62">format="arrow-ipc"</text>
            <text class="t-sm" x="270" y="80">no options</text>
            <text class="t-sm" x="270" y="96">one whole stream</text>
            <path class="arw arw-data" d="M254 86 H150" marker-end="url(#ai-a)"/>
            <text class="t-sm t-mid" x="202" y="104">encode</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M406 50 H510" marker-end="url(#ai-a)"/>
            <path class="arw arw-data" d="M510 86 H406" marker-end="url(#ai-a)"/>
            <rect class="blk blk-data" x="514" y="34" width="146" height="64" rx="8"/>
            <rect class="hd hd-data" x="514" y="34" width="146" height="20" rx="8"/>
            <rect class="hd hd-data" x="514" y="46" width="146" height="8"/>
            <text class="t-lbl" x="526" y="49">rows</text>
            <text class="t-sm" x="526" y="70">price Float64</text>
            <text class="t-sm" x="526" y="86">already columnar</text>
        </g>
        <defs>
            <marker id="ai-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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
<div class="code-cap"><span>KDL</span><em>no options, so the node is one line</em></div>

```kdl
transformer "ipc" format="arrow-ipc"
```

</div>

## 2. Name it from a source or sink

<div class="code">
<div class="code-cap"><span>KDL</span><em>a tcp source straight to a Kafka sink, both on one declared transformer</em></div>

```kdl
run_mode kind="stream"

workflow "ticks" {
    transformer "ipc" format="arrow-ipc"

    source "ticks_in" type="tcp" component="Tick" transformer="ipc" {
        config {
            bind "0.0.0.0:9500"

            schema_fields "price" type="Float64" nullable=#false
        }
    }

    sink "ticks_out" type="KafkaSink" component="Tick" transformer="ipc" {
        config {
            brokers "localhost:9092"
            topic "ticks-enriched"

            schema_fields "price" type="float64" nullable=#false
        }
    }

    link from="ticks_in" to="ticks_out"
}
```

</div>

On files the same transformer works on the stream surface, and there the source needs no columns at
all:

<div class="code">
<div class="code-cap"><span>KDL</span><em>truncate is what keeps the output readable</em></div>

```kdl
workflow "ticks_files" {
    transformer "ipc" format="arrow-ipc"

    source "ticks_in" type="FileSource" component="Tick" transformer="ipc" {
        config path="/data/ticks.arrows"
    }

    sink "ticks_out" type="FileSink" component="Tick" transformer="ipc" {
        config path="/data/ticks-enriched.arrows" {
            truncate #true

            schema_fields "price" type="Float64" nullable=#false
        }
    }

    link from="ticks_in" to="ticks_out"
}
```

</div>

## 3. Give it a schema

A stream read takes its schema from the stream. Declare `schema_fields` as well and it becomes a
projection target: a column the declaration does not name is dropped, a column it names and the
stream lacks is an error, and a column whose type differs is cast. An HTTP or S3 source that wants
the stream's own schema instead keeps `schema_fields` for the link check and reads with
`schema_from "body"` or `schema_from "object"`, which compares the two rather than projecting.

On a message node the declared schema is required, and a payload carrying other columns is
projected onto it the same way.

A sink always declares it, because that is the schema the stream header is written with.

## Every option

None. The `options` table is accepted and no key is read from it.

## How it behaves

One message is one whole stream: a schema header, one batch, then the end-of-stream marker. A Kafka
`key_field` is refused on this format, since there is no single row to key.

Because a stream ends with that marker, a second run appended to the same file does not read back.
A file sink over this format wants `truncate #true` or a path of its own.

The header is written when the output opens, so a run that writes no batch still leaves a readable,
zero-row stream. It is read when the input opens, so a handle that is not an Arrow stream is refused
before any batch is parsed.

The bytes are interchangeable across the two surfaces. The stream a file sink wrote decodes
through the message path, and one Kafka payload opens as a stream. No row count is available
without reading, so this format reports no row estimate.

## When it refuses to start

| Message | What to change |
|---|---|
| `tcp config requires a 'schema_fields' list` | declare `schema_fields` on the message node, where it is required |

Some errors wait for the bytes rather than the config check:
`arrow-ipc: casting to the declared schema: ...` means the stream is missing a column the
declaration names, or a value does not fit its declared type, and `arrow-ipc: stream header: {e}`
means the bytes are not an Arrow stream, whether a file source opened them or a payload carried
them.

## Next

- [Formats](@/service/formats/_index.md), and the other four.
- [TCP](@/service/connectors/tcp.md), the transport this page pairs with.
