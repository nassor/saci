+++
title = "ClickHouse"
description = "Polling, dump or custom queries on the way in; native Arrow IPC streaming bulk load on the way out."
template = "subpage.html"
weight = 7
aliases = ["/connectors/clickhouse/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "None: rows arrive already typed, so no transformer is named"
[[extra.facts]]
label = "Run modes"
value = "Any: every mode reaches EOF once it is caught up"
[[extra.facts]]
label = "In the default build"
value = "no: build with <code>--features connector-clickhouse</code>"
+++
A workflow reads rows out of one ClickHouse table, processes the data, and writes the results to another table.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="ch-t ch-d">
        <title id="ch-t">One ClickHouse table read into a workflow and the result written to another table</title>
        <desc id="ch-d">A table box on the left feeds a source node named ch_events. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named ch_enriched. The sink writes the table box on the right. No transformer sits in the chain because ClickHouse exchanges native Arrow streams directly.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">table</text>
            <text class="t-sm" x="10" y="74">events_in</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#ch-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">ch_events</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#ch-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#ch-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">ch_enriched</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#ch-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">table</text>
            <text class="t-sm" x="566" y="74">events_out</text>
        </g>
        <defs>
            <marker id="ch-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> tables and nodes</span>
        <span class="k-boundary"><i></i> the processor</span>
    </div>
</div>

## What you need

- A binary built with `--features connector-clickhouse`. ClickHouse requires an external server, so it sits outside the default build.
- A reachable ClickHouse HTTP endpoint (default port 8123), version 22.2 or later. The sink sends
  `insert_deduplication_token` with every `INSERT`, and a server that does not know the setting
  refuses it. The suite runs against 24.8.

Start a local server with Docker:

Runs the same on Linux, macOS and Windows (PowerShell):

```text
docker run -d --name saci-clickhouse -p 8123:8123 clickhouse/clickhouse-server:24.8-alpine
```

## Write the configuration

The connector exchanges Apache Arrow stream batches directly with ClickHouse over HTTP.

```kdl
source "ch_events" type="ClickhouseSource" component="Event" {
    config {
        name "ch_events"
        batch_rows 8192

        connection {
            url "${SACI_CLICKHOUSE_URL:-http://localhost:8123}"
            database "default"
        }

        mode kind="polling" table="events_in" cursor_column="id"

        schema_fields "id" type="uint64" nullable=#false
        schema_fields "device_id" type="utf8" nullable=#true
        schema_fields "metric" type="float64" nullable=#true
    }
}

sink "ch_enriched" type="ClickhouseSink" component="EnrichedEvent" {
    config {
        name "ch_enriched"
        table "events_out"
        write_mode "append"
        truncate_before_first_write #false

        connection {
            url "${SACI_CLICKHOUSE_URL:-http://localhost:8123}"
            database "default"
        }

        schema_fields "id" type="uint64" nullable=#false
        schema_fields "device_id" type="utf8" nullable=#true
        schema_fields "metric" type="float64" nullable=#true
    }
}
```

## Validate and run

Linux and macOS:

```text
export SACI_CLICKHOUSE_URL="http://localhost:8123"
saci-service validate --config examples/configs/clickhouse.kdl
saci-service serve --config examples/configs/clickhouse.kdl
```

Windows (PowerShell):

```powershell
$env:SACI_CLICKHOUSE_URL = "http://localhost:8123"
saci-service validate --config examples/configs/clickhouse.kdl
saci-service serve --config examples/configs/clickhouse.kdl
```

## Source modes

The source supports three read strategies chosen by `mode kind`:

- `polling`: queries an incremental range ordered by `cursor_column`, comparing a date/time
  cursor as a ClickHouse datetime literal.
- `dump`: scans the table with `LIMIT` and `OFFSET`. The order is whatever the server returns, so
  a table written to during a scan can repeat or skip a row.
- `query`: executes a custom `SELECT` statement.

## How it delivers

The source keeps its cursor in memory, so a restart reads again from `initial_cursor`, or from the
start of the table when none is set. Delivery is at-least-once.

The sink sends one `INSERT` per `chunk_rows` rows. ClickHouse commits each `INSERT` on its own, so a
flush is not atomic: one that fails on its third chunk has already written the first two.

The batch whose `write_batch` call failed is not kept: that call returned the error, and the
service's retry wrapper hands the same batch back, so keeping it too would write its rows twice. Rows
that earlier calls accepted under `flush_rows` stay buffered for the next write or `finish`, and a
failed `finish` keeps what it held. `flush_rows 0`, the default, flushes every batch; a higher value
holds rows in memory until it is reached, and a process killed before the flush loses them.

The retry sends only what is missing. Chunks the server confirmed are not sent again. A chunk whose
outcome is unknown, because its request failed or the sink stopped waiting for it, goes out again
under the same `insert_deduplication_token`. A table that deduplicates drops that second copy: a
replicated `MergeTree` does by default, and a plain `MergeTree` does once it is created with
`SETTINGS non_replicated_deduplication_window = 100` or another value above zero. On any other table
the chunk is written twice.

The token names the sink, the flush and the chunk's position and content, and the sink sets it on
every `INSERT`, replacing one given in `connection.settings`. Two flushes of identical rows are both
kept, where the table's own content-based deduplication would drop the second. The token needs
ClickHouse 22.2 or later, and the table's deduplication window has to span the flushes between the
failure and the retry. A `connection.settings` entry that turns insert deduplication off
(`deduplicate_insert`, or the older `insert_deduplicate`) turns it off here too.

A call that is dropped before it returns (a runner that cancels it, a `timeout` around it) is handled
the same way: buffered rows are kept, and a dropped `write_batch` is treated like a failed one. A
request the dropped call had sent keeps running on the server and may commit, so the next call waits
for its answer, up to `connection.query_timeout_ms`, before it sends anything. A chunk that landed is
not sent again, and a `TRUNCATE` cannot run after the rows of the retry. A request still unanswered
at the bound is given up on, and the token above covers it. `pending_rows` still counts a dropped
call's rows until the next call.

`truncate_before_first_write` runs `TRUNCATE TABLE`, which is not transactional, at the start of every
attempt of the first flush until one completes. A first flush that failed after its truncate and some
chunks leaves them in the table. The retry truncates again and writes the whole flush, so the table
ends with this run's rows once, unless a request the sink gave up on commits after that `TRUNCATE`:
that chunk then sits beside the retry's copy. After the first flush has completed the table is never
truncated again, and a run that writes no row leaves it alone.

## Every key

### Source keys

| Key | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Instance name matching node id |
| `connection.url` | string | required | ClickHouse HTTP URL |
| `connection.user` | string | none | Authentication user |
| `connection.password` | string | none | Authentication password |
| `connection.database` | string | `default` | Target database |
| `connection.query_timeout_ms` | integer | `30000` | Query execution timeout in milliseconds |
| `connection.compression` | string | `lz4` | Compression codec (`lz4` or `none`) |
| `mode.kind` | string | required | Read strategy (`polling`, `dump`, `query`) |
| `mode.table` | string | required for polling and dump | Table name to read |
| `mode.cursor_column` | string | required for polling | Column name used as progress cursor |
| `mode.initial_cursor` | string | none | Starting cursor value |
| `mode.tiebreak_column` | string | none | Optional secondary sort column; resumes inside a repeated cursor value |
| `mode.query` | string | required for query | Custom SQL query |
| `batch_rows` | integer | `8192` | Maximum rows per emitted batch |
| `max_batches_per_cycle` | integer | `0` | Batch limit per pass (0 is unconstrained) |
| `schema_fields` | list | required | Declared Arrow schema fields |

### Sink keys

| Key | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Instance name matching node id |
| `connection.url` | string | required | ClickHouse HTTP URL |
| `connection.database` | string | `default` | Target database |
| `table` | string | required | Destination table name |
| `write_mode` | string | `append` | Write strategy (`append`) |
| `chunk_rows` | integer | `65536` | Maximum rows per chunk (0 = no limit) |
| `flush_rows` | integer | `0` | Buffer threshold before flush (0 flushes every batch) |
| `truncate_before_first_write` | boolean | `#false` | Runs TRUNCATE TABLE at the start of the first flush, again on a retry until a flush completes |
| `schema_fields` | list | required | Declared Arrow schema fields |
