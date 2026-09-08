+++
title = "Turso"
description = "A SQLite-compatible, in-process database: embedded in the service's own process, or a synced replica of a remote Turso Cloud or sqld endpoint."
template = "subpage.html"
weight = 10
aliases = ["/connectors/turso/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "None: batches arrive already typed, so no transformer is named"
[[extra.facts]]
label = "Run modes"
value = "Any"
[[extra.facts]]
label = "In the default build"
value = "no: build with --features connector-turso"
+++
A workflow reads rows out of a Turso database and writes rows back into one, whether the database is
a single local file or a replica kept in sync with a remote endpoint.

The connector is built on the [`turso`](https://crates.io/crates/turso) engine, an in-process,
SQLite-compatible database. This page pins the engine at `0.7.2`.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 156" role="img" aria-labelledby="tu-t tu-d">
        <title id="tu-t">How a Turso node reads and writes, embedded or synced</title>
        <desc id="tu-d">A source box on the left reads through one of three modes: polling, dump or cdc. Both a source and a sink reach a Turso database box in the middle, which is either a local file or an embedded replica synced to a remote endpoint. A sink box on the right writes through append, upsert or ignore_conflicts, and may optionally capture its own changes.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="20" width="180" height="116" rx="8"/>
            <text class="t-lbl" x="12" y="42">source</text>
            <text class="t-sm" x="12" y="64">polling</text>
            <text class="t-sm" x="12" y="82">dump</text>
            <text class="t-sm" x="12" y="100">cdc</text>
            <text class="t-sm t-data" x="12" y="124">reads rows</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M180 78 H236" marker-end="url(#tu-a)"/>
            <rect class="blk blk-data" x="240" y="20" width="180" height="116" rx="8"/>
            <rect class="hd hd-data" x="240" y="20" width="180" height="20" rx="8"/>
            <rect class="hd hd-data" x="240" y="32" width="180" height="8"/>
            <text class="t-lbl" x="252" y="35">Turso</text>
            <text class="t-sm" x="252" y="64">connection.path</text>
            <text class="t-sm" x="252" y="82">local file</text>
            <text class="t-sm t-data" x="252" y="112">connection.remote</text>
            <text class="t-sm t-data" x="252" y="128">Turso Cloud / sqld</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M420 78 H476" marker-end="url(#tu-a)"/>
            <rect class="blk blk-data" x="480" y="20" width="180" height="116" rx="8"/>
            <text class="t-lbl" x="492" y="42">sink</text>
            <text class="t-sm" x="492" y="64">append</text>
            <text class="t-sm" x="492" y="82">upsert</text>
            <text class="t-sm" x="492" y="100">ignore_conflicts</text>
            <text class="t-sm t-data" x="492" y="124">capture (optional)</text>
        </g>
        <defs>
            <marker id="tu-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> nodes and the database</span>
    </div>
</div>

## What you need

- Build the binary with the connector: `cargo run -p saci-service --features connector-turso`.
- For an embedded database, a writable path. The file is created on first use.
- For a synced replica, the local path plus a `remote` block naming the endpoint URL and an auth
  token. A `turso://` or `libsql://` URL is rewritten to `https://`.
- No transformer. Batches arrive and leave already typed.

## 1. Read from a database: the source node

A source returns `EOF` once it is caught up, so both the interval runner and the stream runner can
drive it. One `mode` child picks the read strategy.

<div class="code">
<div class="code-cap"><span>KDL</span><em>an embedded database, read incrementally by a cursor column</em></div>

```kdl
source "turso_orders" type="TursoSource" component="OrderChange" {
    config name="turso_orders" batch_rows=4096 {
        connection path="orders.db"
        mode kind="polling" table="orders" cursor_column="id"
        schema_fields "id" type="int64" nullable=#false
        schema_fields "status" type="utf8" nullable=#true
        schema_fields "total" type="float64" nullable=#true
    }
}
```

</div>

`kind="polling"` runs an incremental query ordered by a cursor column and resumes from a durable
offset row in the same database. It sees inserts, and updates only when the cursor column is an
`updated_at`-style value the writer bumps. It never sees deletes.

`kind="dump"` paginates the whole table and starts the scan over once it is exhausted, so every
scan re-reads every row. It has no cursor. `max_batches_per_cycle` yields control mid-scan without
restarting it.

`kind="cdc"` reads the engine's change table. It sees inserts, updates and deletes. Capture is
**per connection**: a connection records only the changes made through that same connection, so a
`cdc` source only observes a table some writer opted into. Enable it on the writing connection with
`PRAGMA capture_data_changes_conn('full')`, or through the sink's own `capture` block
below. A missing change table is a loud error naming that pragma.

```kdl
mode kind="cdc" table="orders" cdc_table="turso_cdc"
```

`cdc` fills six reserved field names from the change stream. Declare the ones you want; every other
field is decoded from the change image.

| Field | Required `type` | Value |
|---|---|---|
| `__op` | `utf8` | `"I"`, `"U"` or `"D"` |
| `__change_id` | `int64` | the change's `change_id` |
| `__change_time` | `int64` | Unix epoch seconds |
| `__txn_id` | `int64` | the change's `change_txn_id` |
| `__table` | `utf8` | the record's table name |
| `__rowid` | `int64` | the changed row's rowid |

A COMMIT record carries no image and is skipped; its `change_id` still advances the cursor. The
engine's change-image decoder refuses BLOB values, so a `binary` field cannot be read in `cdc` mode.

To keep a local replica in sync with a remote endpoint instead of a single file, add a `remote`
block. The service substitutes the secrets before the config is parsed.

<div class="code">
<div class="code-cap"><span>KDL</span><em>a synced replica: the same node reads through it unchanged</em></div>

```kdl
connection path="replica.db" {
    remote url="${SACI_TURSO_URL}" token="${SACI_TURSO_TOKEN}" bootstrap_if_empty=#true
}
```

</div>

A synced source pulls remote changes once at the start of each drain cycle, so a whole cycle sees
one snapshot, and a synced sink pushes after its final flush. The `remote` block also takes
`long_poll_timeout_ms`, a server-side long-poll budget, and `logical_mvcc_pull`, an override that
forces MVCC logical-log pulls when a server will not auto-negotiate them.

## 2. Write to a database: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>an upserting sink in the same file</em></div>

```kdl
sink "turso_enriched" type="TursoSink" component="EnrichedOrder" {
    config name="turso_enriched" table="enriched_orders" \
        write_mode="upsert" conflict_columns="id" {
        connection path="orders.db"
        schema_fields "id" type="int64" nullable=#false
        schema_fields "status" type="utf8" nullable=#true
        schema_fields "total" type="float64" nullable=#true
    }
}
```

</div>

Rows buffer as they arrive and flush in one transaction once `chunk_rows` rows are pending, so a
pipeline iteration lands atomically downstream.

`write_mode="append"` inserts directly. `"upsert"` adds `ON CONFLICT (…) DO UPDATE`, and
`"ignore_conflicts"` the same with `DO NOTHING`. Both need `conflict_columns`, and the engine
resolves that conflict target when the statement is prepared: a column set with no matching
`PRIMARY KEY` or `UNIQUE` constraint is refused as the sink connects, naming the columns.

A `capture` block turns the sink into the writer a `cdc` source can read: the sink enables change
capture on its own connection, so its writes are recorded in the change table.

```kdl
capture mode="full"
```

`transaction="deferred"` (the default) and `"immediate"` are the two ordinary `BEGIN` forms.
`"concurrent"` is the MVCC path: the sink sets `journal_mode = 'mvcc'` on its connection and
flushes between `BEGIN CONCURRENT` and `COMMIT`, so several connections write at once. A
write-write conflict rolls the transaction back and is retried up to `conflict_retries` times.

```kdl
transaction "concurrent"
conflict_retries 16
```

The engine makes MVCC and change capture mutually exclusive, so `transaction "concurrent"` cannot be
combined with a `capture` block.

## Encryption at rest

`connection.encryption` turns on page-level encryption for an embedded database. The engine
encrypts every page, the database file and the WAL; the key is never stored on disk.

```kdl
connection path="sealed.db" {
    encryption cipher="aegis256" hexkey="${SACI_TURSO_KEY}"
}
```

`cipher` names the algorithm: `aegis256` (the recommendation) and the other AEGIS variants, or
`aes128gcm`/`aes256gcm`. `hexkey` is 32 hex digits for a 128-bit cipher and 64 for a 256-bit one. A
synced replica takes no local encryption, so `encryption` and `remote` cannot be combined.

## 3. Validate and run

`examples/configs/turso.kdl` is a worked pipeline between two tables of one embedded file. Run it
from the repository root, so the binary loads the gitignored `.env`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>validate opens no database: both factories stop after parsing</em></div>

```text
cargo run -p saci-service --features connector-turso,wasm --bin saci-service -- \
  validate --connectors-only --config examples/configs/turso.kdl
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
```

The first drain cycle is where a missing table or file surfaces.

## How it delivers

The `polling` and `cdc` cursors are committed at the start of the next fetch, so a crash mid-cycle
replays that cycle: delivery is at-least-once, matching the rest of the engine. Configure the sink
`write_mode="upsert"` or `"ignore_conflicts"` to make a replay idempotent.

A synced sink pushes after its final flush. A synced source pulls at the start of each cycle. Both
work on the local replica; the engine's own sync protocol carries the rows to the remote.

Nothing is retried inside the connector: a failed operation reaches the node's `retry` policy, and
then the runner.

## Declared types

SQLite is dynamically typed, so every value is coerced to the `type` you declare. A value that does
not fit is a loud error naming the column, the row and the value. A date or timestamp column is
declared `utf8`, because SQLite stores those as text.

| `type` | Accepted value | Arrow |
|---|---|---|
| `utf8` | `TEXT` | `Utf8` |
| `int64` | `INTEGER` | `Int64` |
| `float64` | `REAL`, or an `INTEGER` widened | `Float64` |
| `bool` | `INTEGER` 0/1 | `Boolean` |
| `binary` | `BLOB` | `Binary` |
| `decimal128` | `TEXT` or `INTEGER`, with `precision` and `scale` | `Decimal128` |

There is no unsigned integer: the engine's only integer is a signed 64-bit one, so a `uint64`
schema field is refused while the node's config is parsed.

## Every key

Source keys:

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | string | required | connector name, used in metrics and error prefixes |
| `batch_rows` | integer | `8192` | rows per emitted batch, and the query's `LIMIT` |
| `max_batches_per_cycle` | integer | `0` | batches per cycle; `0` drains until caught up |
| `connection` | table | required | `path`, plus an optional `encryption` block and an optional `remote` block |
| `mode` | table | required | `kind="polling"`, `"dump"` or `"cdc"`, with the mode's own keys |
| `sync` | table | both `#true` | `pull_before_read`, `push_after_write` |
| `schema_fields` | list of fields | required | the Arrow schema |

`polling` adds `table`, `cursor_column`, an optional `tiebreak_column`, `offset_table`
(default `saci_source_offsets`) and an optional `initial`. `dump` adds `table`. `cdc` adds `table`,
`cdc_table` (default `turso_cdc`), `offset_table` and `retention` (`keep`, or `delete_acked` to
prune acknowledged changes; refused on a synced connection).

`connection.remote` adds `url`, `token`, `bootstrap_if_empty` (default `#true`),
`long_poll_timeout_ms` and `logical_mvcc_pull`. `connection.encryption` adds `cipher` and `hexkey`.

Sink keys:

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | string | required | connector name, used in metrics and error prefixes |
| `table` | string | required | the target table |
| `write_mode` | string | `append` | `append`, `upsert` or `ignore_conflicts` |
| `conflict_columns` | list of strings | none | the conflict target; required by `upsert` and `ignore_conflicts` |
| `chunk_rows` | integer | `65536` | rows buffered before a transaction commits |
| `truncate_before_first_write` | boolean | `#false` | delete every existing row before the first write |
| `transaction` | string | `deferred` | `deferred`, `immediate` or `concurrent` |
| `conflict_retries` | integer | `8` | retries after a write-write conflict; `concurrent` only |
| `connection` | table | required | `path`, plus an optional `encryption` block and an optional `remote` block |
| `sync` | table | both `#true` | `pull_before_read`, `push_after_write` |
| `capture` | table | none | `mode` (`id`, `before`, `after`, `full`) and an optional `table` |
| `schema_fields` | list of fields | required | the Arrow schema |

An unrecognised key inside `config` is a load-time error, not a silently ignored setting.

## When it refuses to start

| Message | What to change |
|---|---|
| `connection.path must not be empty` | give the node a database file |
| `connection.remote.token must not be empty` | supply the auth token, usually as `${SACI_TURSO_TOKEN}` |
| `cursor_column '…' is not a declared schema_field` | declare it, or fix its spelling |
| `cursor_column '…' is "binary"; a cursor must be int64, float64 or utf8` | pick an ordered column |
| `"…" prefixed field names are reserved` | `__`-prefixed names exist only in the `cdc` mode |
| `retention "delete_acked" is not allowed on a synced connection` | leave the change table to the sync engine, or use `keep` |
| `change table 'turso_cdc' does not exist` | run `PRAGMA capture_data_changes_conn('full')` on the writing connection |
| `conflict_columns […] do not match a PRIMARY KEY or UNIQUE constraint` | add a unique index, or fix the column list |
| `connection.encryption.hexkey for '…' must be … hex digits` | use 32 hex digits for a 128-bit cipher, 64 for a 256-bit one |
| `connection.encryption applies to an embedded database only` | drop the `remote` block or the `encryption` block |
| `transaction "concurrent" needs MVCC, which the engine makes mutually exclusive with change capture` | drop the `capture` block, or use another transaction mode |
| `table '…' has no column '…'` | fix the target table or the declared schema |

## Next

- [Sources and sinks](@/service/connectors/_index.md), the other transports.
- [Run modes and persistence](@/service/config/run-modes.md), how a source that returns EOF is driven.
