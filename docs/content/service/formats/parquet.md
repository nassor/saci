+++
title = "parquet"
description = "Self describing columnar files: the footer carries the schema and the row counts."
template = "subpage.html"
weight = 3
aliases = ["/transformers/parquet/"]
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
value = "Optional reading a file, required writing and on messages"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
Columnar files that describe themselves. The footer carries every column, its type and the row count
per row group, so a source reads its schema out of the file. Declaring `schema_fields` instead
narrows the read to those columns.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 132" role="img" aria-labelledby="pq-t pq-d">
        <title id="pq-t">A parquet transformer decoding a file into rows and encoding rows back into a file</title>
        <desc id="pq-d">Bytes on the left and rows on the right, with a transformer node in the middle whose format is parquet. The upper arrow runs left to right and is the decode path, which reads the schema from the file's footer. The lower arrow runs right to left and is the encode path, which needs a declared schema. One message is one whole file.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="150" height="64" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="46" width="150" height="8"/>
            <text class="t-lbl" x="12" y="49">bytes</text>
            <text class="t-sm" x="12" y="70">row groups</text>
            <text class="t-sm" x="12" y="86">+ footer</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M150 50 H254" marker-end="url(#pq-a)"/>
            <text class="t-sm t-mid" x="202" y="42">decode</text>
            <rect class="blk blk-ctl" x="258" y="26" width="144" height="80" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="26" width="144" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="258" y="38" width="144" height="8"/>
            <text class="t-lbl" x="270" y="41">transformer</text>
            <text class="t-sm t-ctl" x="270" y="62">format="parquet"</text>
            <text class="t-sm" x="270" y="80">no options</text>
            <text class="t-sm" x="270" y="96">snappy, fixed</text>
            <path class="arw arw-data" d="M254 86 H150" marker-end="url(#pq-a)"/>
            <text class="t-sm t-mid" x="202" y="104">encode</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M406 50 H510" marker-end="url(#pq-a)"/>
            <path class="arw arw-data" d="M510 86 H406" marker-end="url(#pq-a)"/>
            <rect class="blk blk-data" x="514" y="34" width="146" height="64" rx="8"/>
            <rect class="hd hd-data" x="514" y="34" width="146" height="20" rx="8"/>
            <rect class="hd hd-data" x="514" y="46" width="146" height="8"/>
            <text class="t-lbl" x="526" y="49">rows</text>
            <text class="t-sm" x="526" y="70">id    Int64</text>
            <text class="t-sm" x="526" y="86">price Float64</text>
        </g>
        <defs>
            <marker id="pq-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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
transformer "pq" format="parquet"
```

</div>

## 2. Name it from a source or sink

<div class="code">
<div class="code-cap"><span>KDL</span><em>one declared transformer serves both ends</em></div>

```kdl
source "orders_in" type="FileSource" component="Order" transformer="pq" {
    config path="/data/orders.parquet"
}

sink "orders_out" type="FileSink" component="EnrichedOrder" transformer="pq" {
    config path="/data/enriched.parquet" {
        truncate #true

        schema_fields "id" type="Int64" nullable=#false
        schema_fields "revenue" type="Float64" nullable=#false
    }
}
```

</div>

`truncate #true` is what makes the output readable. A file ends in a footer, so a second run's
bytes appended to the same file do not read back. Any byte-carrying connector names this format
the same way: see [Sources and sinks](@/service/connectors/_index.md).

## 3. Give it a schema

A source may declare none, and then the file's own schema governs: the footer carries it. Declare
`schema_fields` and it becomes a projection instead, pushed into the reader so only those column
chunks are read at all. A declared column the file does not carry is a configuration error naming
both the column and the file's columns.

A sink requires one, because the schema is baked into the writer when the file is created.

On messages the declared schema is required, and a payload carrying other columns is projected onto
it the same way.

## Every option

None. The `options` table is accepted and no key is read from it.

## How it behaves

One message is one whole file, footer included. The magic bytes, the per-column page headers and
the footer come to about 470 bytes for a three-column row type before a single row is written, and
every payload pays that again. A message transport carrying this format wants batches, not rows.

Compression is always Snappy.

It is the one format that reports a row estimate, and only on the stream surface: opening the file
sums the row counts in the footer's row-group metadata, so no data page is touched. A file with no
rows reports nothing rather than zero. A projection does not change it: the estimate counts rows,
not columns.

One window is one batch: each payload is decoded as it is pushed, and flush concatenates every
batch the window decoded.

## When it refuses to start

| Message | What to change |
|---|---|
| `parquet: declared column 'X' is not in the file (file columns: ...)` | name a column the file carries, or drop the declaration to read every column |
| `FileSink config requires a 'schema_fields' list` | declare `schema_fields` on the sink writing this format |

Some errors wait for a payload rather than the config check:
`parquet: payload's declared column 'X' is missing (payload columns: ...)` means a producer wrote
other columns, `parquet: casting to the declared schema: ...` means a value does not fit its
declared type, and `parquet: file header: ...` means the payload is not a Parquet file at all.

## Next

- [Formats](@/service/formats/_index.md), and the other four.
- [File](@/service/connectors/file.md), the transport this page reads from.
