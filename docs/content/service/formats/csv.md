+++
title = "csv"
description = "Text with no types of its own, so reading needs a declared schema."
template = "subpage.html"
weight = 1
aliases = ["/transformers/csv/"]
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
value = "Required reading a stream, and on messages"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
Comma separated text, on files and on messages alike. The format carries no types of its own, so
the schema you declare is what gives the columns their types.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 132" role="img" aria-labelledby="cs-t cs-d">
        <title id="cs-t">A csv transformer decoding bytes into rows and encoding rows back into bytes</title>
        <desc id="cs-d">Bytes on the left and rows on the right, with a transformer node in the middle whose format is csv. The upper arrow runs left to right and is the decode path, which needs the declared schema. The lower arrow runs right to left and is the encode path, which takes the columns of the batch it is handed.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="150" height="64" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="150" height="8"/>
            <text class="t-lbl" x="12" y="49">bytes</text>
            <text class="t-sm" x="12" y="70">id,price</text>
            <text class="t-sm" x="12" y="86">1,99.50</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M150 50 H254" marker-end="url(#cs-a)"/>
            <text class="t-sm t-mid" x="202" y="42">decode</text>
            <rect class="blk blk-ctl" x="258" y="26" width="144" height="80" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="26" width="144" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="38" width="144" height="8"/>
            <text class="t-lbl" x="270" y="41">transformer</text>
            <text class="t-sm t-ctl" x="270" y="62">format="csv"</text>
            <text class="t-sm" x="270" y="80">options</text>
            <text class="t-sm" x="270" y="96">has_headers</text>
            <path class="arw arw-data" d="M254 86 H150" marker-end="url(#cs-a)"/>
            <text class="t-sm t-mid" x="202" y="104">encode</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M406 50 H510" marker-end="url(#cs-a)"/>
            <path class="arw arw-data" d="M510 86 H406" marker-end="url(#cs-a)"/>
            <rect class="blk blk-data" x="514" y="34" width="146" height="64" rx="8"/>
            <rect class="hd hd-data" x="514" y="34" width="146" height="20" rx="8"/>
            <rect class="hd hd-data" x="514" y="46" width="146" height="8"/>
            <text class="t-lbl" x="526" y="49">rows</text>
            <text class="t-sm" x="526" y="70">id    Int64</text>
            <text class="t-sm" x="526" y="86">price Float64</text>
        </g>
        <defs>
            <marker id="cs-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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
<div class="code-cap"><span>KDL</span><em>has_headers is this format's one option</em></div>

```kdl
transformer "csv_fmt" format="csv" {
    options has_headers=#true
}
```

</div>

## 2. Name it from a source or sink

<div class="code">
<div class="code-cap"><span>KDL</span><em>the transformer key is a property of the node, not a config key</em></div>

```kdl
source "orders_in" type="FileSource" component="Order" transformer="csv_fmt" {
    config path="/data/orders.csv" {
        schema_fields "id" type="Int64" nullable=#false
        schema_fields "total" type="Float64" nullable=#false
    }
}
```

</div>

Any byte-carrying connector names it the same way: see
[Sources and sinks](@/service/connectors/_index.md).

## 3. Give it a schema

Reading needs a declared schema, on a stream and on messages alike, because the text says nothing
about types. The `schema_fields` list is what names the columns and picks how each field is parsed.

Writing takes the columns of each batch it is handed and stores no schema of its own, so
`schema_fields` on a sink is the connector's requirement rather than this format's.

## Every option

| Key | Type | Default | What it does |
|---|---|---|---|
| `has_headers` | bool | `#true` | on a stream, the reader expects a header row and the writer emits one |

The factory reads that one key and ignores anything else in the `options` table.

## How it behaves

`has_headers` governs the stream surface alone, where the reader expects a header row and the
writer emits one, so a stream round trip is symmetric.

A message has no header row in either direction, whatever the option says. One payload is one
record, which leaves no line to spare, and the decoder works from the declared schema. Keeping the
option off the message surface is what lets any consumer decode a topic without knowing the
producer's setting.

Text plus a declared schema round-trips the boolean, integer, float, `utf8` and date columns the
schema names, unsigned integers included: the declared type parses the field whatever the digits
look like. The reader has no parser for `binary` or `largeutf8`, so a schema naming either fails
the read. The one value csv cannot carry is an empty string, which it writes as nothing and reads
back as a null.

Encoding emits one payload per row, with no header line and no record terminator. Decoding accepts a
payload either with or without a trailing newline, so a producer that left one on and one that did
not both decode to one row.

## When it refuses to start

| Message | What to change |
|---|---|
| `csv: option 'has_headers' must be a boolean` | write `has_headers=#true` or `#false`, not a string |
| `csv: reading needs a declared schema; add schema_fields` | declare `schema_fields` on the source reading this format |

Some errors wait for a payload rather than the config check: `csv: empty payload` means a message
arrived with no bytes in it.

## Next

- [Formats](@/service/formats/_index.md), and the other four.
- [File](@/service/connectors/file.md), the transport this page reads from.
