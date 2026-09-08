+++
title = "File"
description = "One transport for every local file. The byte format is a declared transformer, named by the required transformer key."
template = "subpage.html"
weight = 1
aliases = ["/connectors/file/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "Required on both halves, and never inferred from the extension"
[[extra.facts]]
label = "Run modes"
value = "Any: the source reaches EOF at the end of the file"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
A workflow reads `orders.csv` off the disk, hands the rows to a processor, and writes the result
to a second file.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="fl-t fl-d">
        <title id="fl-t">A file read into a workflow and written back out to a second file</title>
        <desc id="fl-d">orders.csv on the left feeds a source node named orders_in. That source hands rows to a WebAssembly processor, drawn as a boundary box. The processor hands its rows to a sink node named orders_out, which writes out.csv on the right. The two file boxes and the two nodes are data plane; the processor is the sandbox boundary.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">orders.csv</text>
            <text class="t-sm" x="10" y="74">on disk</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#fl-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">orders_in</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#fl-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#fl-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">orders_out</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#fl-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">out.csv</text>
            <text class="t-sm" x="566" y="74">appended</text>
        </g>
        <defs>
            <marker id="fl-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> files and nodes</span>
        <span class="k-boundary"><i></i> the processor</span>
    </div>
</div>

## What you need

- No external service. Both halves work on local paths.
- A parent directory that already exists for the sink's `path`. The output file is opened while the
  config is built, `validate` included, so a missing directory fails before the pipeline runs.
- The file this workflow reads. `examples/configs/fixtures/orders.csv` is the one the example config
  below uses.
- The processor component the config names, at `pipelines/orders.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from.
  `validate --connectors-only` below runs without it.

## 1. Declare the format

A file carries bytes, so the node names a declared `transformer` and that transformer names the
format. Nothing is inferred from the extension.

<div class="code">
<div class="code-cap"><span>KDL</span><em>one declared transformer serves both halves</em></div>

```kdl
transformer "orders_csv" format="csv" {
    options has_headers=#true
}
```

</div>

Pick from [csv](@/service/formats/csv.md), [ndjson](@/service/formats/ndjson.md),
[parquet](@/service/formats/parquet.md), [avro](@/service/formats/avro.md) and
[arrow-ipc](@/service/formats/arrow-ipc.md). The format also decides what the source's
`schema_fields` means: csv requires it, ndjson infers without it, and the other three read the
file's own schema when it is absent and project onto it when it is declared.

## 2. Read from a file: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>path is the only key the transport itself needs</em></div>

```kdl
source "orders_in" type="FileSource" component="Order" transformer="orders_csv" {
    config path="/data/orders.csv" {
        schema_fields "id" type="Int64" nullable=#false
        schema_fields "amount" type="Float64"
        schema_fields "status" type="Utf8"
    }
}
```

</div>

`path` is the file to read. `component` names the row type the rows land in, and `transformer`
names the node declared in step 1. The source reads the file in batches, so a file larger than
memory still runs.

## 3. Write to a file: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>truncate replaces the file instead of appending to it</em></div>

```kdl
sink "orders_out" type="FileSink" component="Order" transformer="orders_csv" {
    config path="/data/orders-out.csv" {
        truncate #true

        schema_fields "id" type="Int64" nullable=#false
        schema_fields "amount" type="Float64"
        schema_fields "status" type="Utf8"
    }
}
```

</div>

A sink always declares `schema_fields`, whatever the format, because that is the schema the rows are
written with. `truncate #true` empties the file when the config is built; the default keeps whatever
is already in it and adds rows after.

## 4. Validate and run

`examples/configs/standalone.kdl` is this pair over
`examples/configs/fixtures/orders.csv`. `--connectors-only` checks both nodes and skips the
processor module the config names.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>the source, the sink and the transformer, all built for real</em></div>

```text
saci-service validate --config examples/configs/standalone.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: orders (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The config sets `run_mode kind="one_shot"`, so the process reads the file once and
exits:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>writes /tmp/saci-standalone-orders-out.csv</em></div>

```text
saci-service serve --config examples/configs/standalone.kdl
```

</div>

The output file holds one row per input row, as the processor left them.

## How it delivers

The source reads to the end of the file and reports EOF, so every run mode can drive it. The file is
opened once and never reopened, so later passes of an `interval` or `continuous` run mode admit no
rows.

The sink appends by default. A format that terminates its output does not survive that: `parquet`,
`avro` and `arrow-ipc` each close with a footer or an end-of-stream marker, so a second run's bytes
appended to the same file do not read back. Those configs want `truncate #true` or a fresh path.
`ndjson`, and `csv` without headers, append cleanly.

Every batch is synced to disk as it lands, so a `csv` or `ndjson` file holds every batch the sink
returned on. A footer format writes its footer when the run finishes, so a process killed before
that leaves a `parquet`, `avro` or `arrow-ipc` file that does not read back.

The output file is opened when the config is built, `validate` included. That is where a missing
parent directory or a read-only path surfaces, rather than on the first batch. It also means
`truncate #true` empties the file during `validate`, not only during `serve`.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `path` | string | required | the file to read |
| `schema_fields` | list of fields | the format decides | the declared column list handed to the format |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `path` | string | required | the file to write |
| `schema_fields` | list of fields | required | the schema the rows are written with |
| `truncate` | bool | `#false` | replace the file on build instead of appending to it |

An unrecognised key inside `config` is ignored rather than rejected on this connector, so a
misspelled `truncate` silently keeps the default.

## When it refuses to start

| Message | What to change |
|---|---|
| `FileSource config requires a 'path' string field` | add `path="..."` to the source's `config` |
| `FileSink config requires a 'path' string field` | add `path="..."` to the sink's `config` |
| `FileSource moves bytes and needs a 'transformer' key naming a declared transformer` | add `transformer="<id>"` to the node, naming a declared `transformer` |
| `FileSource: cannot open {path:?}: {e}` | point `path` at a file that exists and is readable |
| `FileSink: cannot create {path:?}: {e}` | create the parent directory, or fix the permissions, for a `truncate #true` sink |
| `FileSink: cannot open {path:?} for append: {e}` | the same, for the default appending sink |
| `FileSink config requires a 'schema_fields' list` | declare `schema_fields` in the sink's `config` |

## Next

- [csv](@/service/formats/csv.md), the format this page declares.
- [Sources and sinks](@/service/connectors/_index.md), the other nine transports.
