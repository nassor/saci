+++
title = "MariaDB"
description = "Polling, a trigger changelog or the row binlog on the way in; batched upserts and applied changes, deletes included, on the way out. One connector for MariaDB and MySQL servers."
template = "subpage.html"
weight = 14
aliases = ["/connectors/mariadb/"]
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
value = "no: build with <code>--features connector-mariadb</code>"
+++
A workflow reads changes out of one table on a MariaDB or MySQL server, processes the rows, and
writes the result into a table on the same or another server. One connector serves both products:
a `mysql://` and a `mariadb://` URL are the same thing, and the connector asks the server which one
answered and adapts the few statements that differ. It supports MariaDB 10.11, 11.4 and 11.8 and
MySQL 8.4 and 9.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="mdb-t mdb-d">
        <title id="mdb-t">One table read into a workflow and the result written to another table</title>
        <desc id="mdb-d">A table box on the left feeds a source node named maria_orders. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named maria_enriched. The sink writes the table box on the right. The two tables may live on a MariaDB server, a MySQL server, or one of each. No transformer sits anywhere in the chain, because rows arrive already typed.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">table</text>
            <text class="t-sm" x="10" y="74">shop.orders</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#mdb-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">maria_orders</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#mdb-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#mdb-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">maria_enriched</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#mdb-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">table</text>
            <text class="t-sm" x="566" y="74">enriched_orders</text>
        </g>
        <defs>
            <marker id="mdb-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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

- A binary built with `--features connector-mariadb`; MariaDB and MySQL are one of eight
  connectors that need a specific service installed and running, so they sit outside the default
  build.
- A reachable server. One in a container is enough. These commands start one of each, with the
  settings the `cdc_binlog` mode needs. Run one of them; they run the same on Linux, macOS and
  Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>MariaDB 11.8, the row binlog on</em></div>

```text
docker run --rm -p 3306:3306 -e MARIADB_ROOT_PASSWORD=saci -e MARIADB_DATABASE=saci mariadb:11.8 --log-bin=mariadb-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1 --log-bin-trust-function-creators=1
```

</div>

<div class="code">
<div class="code-cap"><span>Shell</span><em>MySQL 8.4, the row binlog and GTIDs on</em></div>

```text
docker run --rm -p 3306:3306 -e MYSQL_ROOT_PASSWORD=saci -e MYSQL_DATABASE=saci mysql:8.4 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1 --gtid-mode=ON --enforce-gtid-consistency=ON --log-bin-trust-function-creators=1
```

</div>

Either one listens on port 3306 with the DSN `mysql://root:saci@127.0.0.1:3306/saci`. Wait for the
log line `ready for connections`; both images restart once while they initialise.

- `polling` and `cdc_trigger` need no server setting. `cdc_binlog` and the grants every mode needs
  are listed under [Server setup](#server-setup).
- The processor component the config names, at `pipelines/orders.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from;
  `validate --connectors-only` below runs without it.
- No transformer. Rows arrive typed, so this connector names no format at all.

## Choose how to read changes

One `mode` node picks how changes are found. The choice decides whether deletes arrive, what the
server and the user must allow, and what each read costs.

`polling` runs an incremental query over a live table, ordered by a cursor column. It sees inserts,
and it sees updates only when the writer bumps the cursor column, as an `updated_at` value. It never
sees deletes. It needs no server setting and only `SELECT`.

`cdc_trigger` reads a changelog table. With `install_triggers #false` that table is your own outbox,
read the way `polling` reads a table, with no change to the watched table. With
`install_triggers #true` the connector creates a changelog table and three triggers on the watched
table, so inserts, updates and deletes all arrive; it needs `TRIGGER` and `CREATE`, and no binlog or
replication privilege.

`cdc_binlog` reads the row binlog. Inserts, updates and deletes arrive with their transaction
metadata, and nothing is installed on the table. It needs the binlog settings and the replication
grants under [Server setup](#server-setup). `position "file"` resumes by binlog file and position on
both products; `position "gtid"` resumes by GTID set on MySQL only. `snapshot "initial"` reads the
table's existing rows first, as one consistent snapshot, then streams every change made after it. A
schema history keeps the table's definition as it was, so a restart from a position before an
`ALTER TABLE` still decodes each row as it was written; see [How it delivers](#how-it-delivers).

| Mode | Deletes | Needs from the server | Orders by |
|---|---|---|---|
| `polling` | no | `SELECT` | the cursor column |
| `cdc_trigger`, outbox | what your writer puts in the outbox | `SELECT`, and `DELETE` for `delete_acked` | the outbox cursor column |
| `cdc_trigger`, installed | yes | `TRIGGER`, `CREATE`, `INSERT`, `DELETE` | the changelog's `__seq` |
| `cdc_binlog`, `file` | yes | binlog on, replication grants | the binlog |
| `cdc_binlog`, `gtid` | yes | the same on MySQL with `gtid_mode = ON` | the binlog |

The two trigger modes and `polling` re-query a table every cycle. `cdc_binlog` holds one stream open
per cycle and buffers each transaction whole until it commits, so a single transaction of a million
rows is a million rows in memory.

## 1. Read from a table: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>the whole config table is read by the connector, so name is repeated inside it</em></div>

```kdl
source "maria_orders" type="MariadbSource" component="OrderChange" {
    config {
        name "maria_orders"

        connection dsn="${SACI_MARIADB_DSN}" ssl_mode="verify_identity"

        mode kind="polling" {
            table "shop.orders"
            cursor_column "updated_at"
            tiebreak_column "id"
        }

        schema_fields "id" type="int64" nullable=#false
        schema_fields "updated_at" type="timestamp_micros" nullable=#false
        schema_fields "total" type="decimal128" precision=12 scale=2
    }
}
```

</div>

`name` labels this node's metrics, keys its offset row and prefixes every error it raises. It takes
ASCII letters, digits, `_` and `-`, up to 191 characters. `connection.dsn` is the only required
connection key, and `${SACI_MARIADB_DSN}` reads it from the environment. A table written without a
schema belongs to the database in the DSN; a DSN with no database needs `schema.table`.

A `polling` source needs `table` and `cursor_column`. The cursor column must be a declared field of
type `int8` to `int64`, `uint8` to `uint64`, `utf8`, `date32`, `timestamp_micros` or
`timestamp_micros_utc`. A `tiebreak_column` keeps a non-unique cursor lossless: without it a batch
boundary inside a run of equal cursor values skips the rest of the run. The source keeps its
position in `offset_table`, created for itself when `offset_table_autocreate` is `#true`, and a
`where_clause` is added to every query as written.

The outbox form of `cdc_trigger` takes the same keys and adds `retention`:

<div class="code">
<div class="code-cap"><span>KDL</span><em>an outbox the application fills; acknowledged rows are deleted</em></div>

```kdl
mode kind="cdc_trigger" {
    table "shop.orders_outbox"
    cursor_column "seq"
    retention "delete_acked"
}
```

</div>

With `install_triggers #true` the connector creates the changelog table `saci_cdc_<name>` (a `-` in
the name becomes `_`) and the triggers `saci_<name>_ai`, `saci_<name>_au` and `saci_<name>_ad` on the
watched table, once per session and only when they are missing. The cursor is the changelog's own
`__seq`, so `cursor_column`, `tiebreak_column` and `where_clause` are refused.

<div class="code">
<div class="code-cap"><span>KDL</span><em>the connector installs the triggers; deletes arrive</em></div>

```kdl
mode kind="cdc_trigger" {
    table "shop.orders"
    install_triggers #true
}

schema_fields "__op" type="utf8" nullable=#false
schema_fields "__seq" type="uint64" nullable=#false
schema_fields "id" type="int64" nullable=#false
schema_fields "status" type="utf8"
```

</div>

`cdc_binlog` needs `table`. Rows of every other table in the binlog are skipped and counted.

<div class="code">
<div class="code-cap"><span>KDL</span><em>an initial snapshot, then the stream, by file and position</em></div>

```kdl
mode kind="cdc_binlog" {
    table "shop.orders"
    position "file"
    snapshot "initial"
    server_id 4100
}

schema_fields "__op" type="utf8" nullable=#false
schema_fields "__binlog_file" type="utf8" nullable=#false
schema_fields "__binlog_pos" type="uint64" nullable=#false
schema_fields "id" type="int64" nullable=#false
schema_fields "status" type="utf8"
```

</div>

The source registers with the server as a replica, so each source on one server needs its own
`server_id`. The default is `4000000000` plus a hash of `name` modulo `100000000`; `0` is refused.
`initial` is where a source with no stored position starts: `"now"` (the default), `"beginning"`, or
a literal, `file:<binlog name>:<position>` under `position "file"` and a GTID set under
`position "gtid"`. `snapshot "initial"` needs `initial` to stay `"now"`, because the snapshot pins
its own start position. It pages the table by its primary key, so the table must have one and every
key column must be a declared field, none of them `decimal`, `float` or `double`.

Reserved columns carry what the change stream knows. A field whose name starts with `__` is filled
by the stream instead of read from the row, and must carry the type shown:

| Field | Type | `cdc_binlog` | Installed `cdc_trigger` |
|---|---|---|---|
| `__op` | `utf8` | `I`, `U`, `D`, or `R` for a snapshot row | `I`, `U`, `D` |
| `__table` | `utf8` | `schema.table` | `schema.table` |
| `__binlog_file` | `utf8` | the binlog file | not available |
| `__binlog_pos` | `uint64` | the position of the transaction's last event | not available |
| `__gtid` | `utf8` | the transaction's GTID, NULL on MariaDB | not available |
| `__seq` | `uint64` | not available | the changelog's sequence number |
| `__commit_ts` | `timestamp_micros_utc` | the transaction's commit time | the change's time |

An update that changes a primary key arrives as a `D` of the old row, then an `I` of the new one.
`polling` refuses every `__` field. An outbox reads every column as an ordinary column, so an outbox
may carry its own `__op` for an `apply_changes` sink.

`schema_fields` is required and uses this connector's own type vocabulary, listed under
[Every key](#every-key) below.

## 2. Write to a table: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>upsert makes a replayed drain cycle idempotent</em></div>

```kdl
sink "maria_enriched" type="MariadbSink" component="EnrichedOrder" {
    config {
        name "maria_enriched"
        table "shop.enriched_orders"
        write_mode "upsert"
        conflict_columns "id"
        dedupe_order_column "source_seq"
        chunk_rows 1000

        connection dsn="${SACI_MARIADB_SINK_DSN}" ssl_mode="verify_identity"

        schema_fields "id" type="int64" nullable=#false
        schema_fields "source_seq" type="uint64" nullable=#false
        schema_fields "total" type="decimal128" precision=18 scale=4 mariadb_type="decimal"
    }
}
```

</div>

`write_mode` is `append`, `upsert`, `ignore_conflicts` or `apply_changes`. `append` is a plain
multi-row `INSERT`, and a duplicate key is an error. The other three need `conflict_columns`, and
those columns must be exactly the table's primary key or one of its unique indexes, compared as a
set without regard to case. The sink checks this once per session, because `ON DUPLICATE KEY UPDATE`
resolves on whichever unique index a row collides with.

`upsert` rewrites `update_columns`, or every written column except the conflict key when
`update_columns` is omitted. `ignore_conflicts` leaves an existing row exactly as it is, including a
`ON UPDATE CURRENT_TIMESTAMP` column; it never uses `INSERT IGNORE`, which would also hide
truncation and type errors. A table that holds nothing but its key has nothing to update, so
`upsert` resolves a collision the way `ignore_conflicts` does.

`dedupe_order_column` is for `upsert` and `ignore_conflicts` when one flush can repeat a conflict
key. The row with the greatest value of that column wins and a tie goes to the later row. Without it
the rows apply in batch order: the last one wins under `upsert`, the first under `ignore_conflicts`,
which leaves whatever row got there first. Values compare exactly, not by the column's
collation, so `'a'` and `'A'` count as two keys there. A row with a NULL key part never conflicts and
is always kept.

`apply_changes` reads a declared `__op` field of type `utf8` and applies each row in order: `I`, `U`
and `R` write the row, `D` deletes it by `conflict_columns`. It is how a `cdc_binlog` source mirrors a
table, deletes included.

<div class="code">
<div class="code-cap"><span>KDL</span><em>the __op field is read for the operation and is never written to the table</em></div>

```kdl
sink "maria_mirror" type="MariadbSink" component="OrderChange" {
    config {
        name "maria_mirror"
        table "shop.orders_copy"
        write_mode "apply_changes"
        conflict_columns "id"

        connection dsn="${SACI_MARIADB_SINK_DSN}" ssl_mode="verify_identity"

        schema_fields "__op" type="utf8" nullable=#false
        schema_fields "id" type="int64" nullable=#false
        schema_fields "status" type="utf8"
    }
}
```

</div>

Every field whose name starts with `__` is read for its meaning and never written, and none may be a
conflict or update column. `dedupe_order_column` is refused here, because the row order is the change
order. Any `__op` other than `I`, `U`, `R` or `D`, a NULL one included, fails the flush before
anything is written. A `D` row whose key has a NULL part is refused for the same reason: `k = NULL`
matches no row.

`mariadb_type` asserts the server column's `DATA_TYPE` on the first connection, for example
`"decimal"`. It changes no decoding.

## 3. Validate and run

`examples/configs/mariadb.kdl` replicates `shop.orders` through the binlog with an initial snapshot
and applies every change, deletes included, to `shop.enriched_orders`.
`examples/configs/mariadb_trigger.kdl` captures the same table with installed triggers and upserts
the result.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>no database connection is opened by validation</em></div>

```text
saci-service validate --config examples/configs/mariadb.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: orders (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The config sets `run_mode kind="interval" interval_ms=2000`, so the source is redrained
every two seconds:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>both DSNs are required: the config has no default for them</em></div>

```bash
export SACI_MARIADB_DSN='mysql://saci@localhost:3306/shop'
export SACI_MARIADB_SINK_DSN='mysql://saci@localhost:3307/shop'
saci-service serve --config examples/configs/mariadb.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same three steps</em></div>

```powershell
$env:SACI_MARIADB_DSN = "mysql://saci@localhost:3306/shop"
$env:SACI_MARIADB_SINK_DSN = "mysql://saci@localhost:3307/shop"
saci-service serve --config examples/configs/mariadb.kdl
```

</div>

The snapshot arrives first as `R` rows. After it, every insert, update and delete on `shop.orders`
shows up in `shop.enriched_orders` within a drain cycle or two.

To see the connector alone, without a service, `examples/connectors/mariadb_roundtrip.rs` copies 250
rows between two tables through a polling source and an upsert sink:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>against the MariaDB container from above</em></div>

```bash
export SACI_MARIADB_DSN='mysql://root:saci@127.0.0.1:3306/saci'
cargo run -p saci-connector-mariadb --example mariadb_roundtrip
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same two steps</em></div>

```powershell
$env:SACI_MARIADB_DSN = "mysql://root:saci@127.0.0.1:3306/saci"
cargo run -p saci-connector-mariadb --example mariadb_roundtrip
```

</div>

```text
inserted 250 row(s) into saci_roundtrip_in
source drained 250 row(s) into the dataset
sink wrote 250 row(s) to saci_roundtrip_out
first 3 row(s) as Arrow saw them:
  id=1 label=row-1 amount=10001 (unscaled, scale 4)
  id=2 label=row-2 amount=20002 (unscaled, scale 4)
  id=3 label=row-3 amount=30003 (unscaled, scale 4)
saci_roundtrip_out holds 250 row(s)
OK: 250 row(s) round-tripped with every column equal
```

## How it delivers

Every mode reports EOF once it is caught up, so any run mode drives this source. An `interval` run
mode is the usual choice. Each tick is one drain cycle over whatever arrived since the last one.

Delivery is at-least-once. The position advances in memory as each batch is handed over, and the
durable position, the row in `offset_table`, is written at the start of the next cycle. A crash
mid-cycle replays that whole cycle, which is why a sink that upserts, ignores conflicts or applies
changes absorbs it: a replayed cycle rewrites the same rows instead of duplicating them. A lost
connection does not replay anything, because the in-memory position outlives a reconnect. A
`next_batch` the host cancels before it returns (the stream runner does on shutdown) may leave its
connection mid-statement, so the next call opens a new one and carries on from the same position; a
`cdc_binlog` snapshot, whose read view belongs to its connection, starts over and repeats its `R` rows.

`cdc_binlog` stores `file:<name>:<position>` or `gtid:<set>` in the offset row. Transactions that
touch no row of this table still move the position, so a quiet table on a busy server does not
rescan the same range or point at a purged file; a rotation of the log moves the stored file too.
Writes to the offset table and to the schema history table do not count: the source's own stores are
in the binlog it reads, and treating them as news would make an idle source write a position every
cycle, forever. They
still move the position in memory, so on a server where only this source writes, the stored
position can age past the binlog's expiry; a restart then reports the purged position. A
transaction is acknowledged by its last row, so
`max_batches_per_cycle` never ends a cycle inside one: with the cap reached and a transaction open,
the rest of that transaction goes out and the cycle ends after it, so a transaction larger than the
cap is read once. `max_changes_per_cycle` stops a cycle at the first commit that reaches it, and one
transaction can exceed it.

The snapshot holds its progress in memory only. A source dropped mid-snapshot snapshots again from
the start, and the `R` rows repeat. `upsert` and `apply_changes` absorb that.

A cursor source (`polling` and both trigger forms) reads with a binary-protocol query ordered by the
cursor, with `batch_rows` as the `LIMIT`, and stores the cursor as text. With `initial "now"` and
nothing stored, "now" is the table's newest cursor at the first read, stored by the next cycle;
`"beginning"` reads everything. A literal must be a value of the cursor column's type, checked when
the node is built, and a stored offset that is not one is refused. Integer cursors bind as typed
integers, exact above 2^53 and, unsigned, above 2^63. Date and timestamp cursors compare to the
microsecond. A `utf8` cursor compares under the column's collation, so a cursor whose values differ
only by case needs a `tiebreak_column`. The cursor column must be `NOT NULL` and must not hold zero
dates: a NULL cursor stops the source with an error.

With a `tiebreak_column` the read is `cursor > ? OR (cursor = ? AND tiebreak > ?)`. The tiebreak
column must be `NOT NULL` as well: a NULL tiebreak value on the last row of a batch stops the source
with an error, because the rest of that run of equal cursors could not be found again. Give the table
an index on `(cursor, tiebreak)`, so the `ORDER BY` needs no sort. The row-value form
`(cursor, tiebreak) > (?, ?)` is not used: neither server range-scans it, so every batch would read
the index from its start.

A cursor the server numbers with `AUTO_INCREMENT` (an outbox's id, the installed changelog's `__seq`,
a polled table's id) is handed out when a row is written, not when its transaction commits. If
transaction A takes 5 and stays open while B takes 6 and commits, a reader that had moved past 6
would never see 5, and `delete_acked` would prune it too. The source therefore treats a jump of
more than one step between neighbouring rows as a hole that a transaction may still fill. It hands
over the rows before the hole, holds the rows after it, and reads again each cycle.

What lets a hole go depends on what the source can see. With the `PROCESS` privilege it reads
`information_schema.INNODB_TRX`: after it first sees a hole, it notes the running transactions that
have modified rows or wait on a lock. The server rebuilds that table only after 0.1 second without a
read, so a monitor that polls it faster keeps an old list, one that can predate the writer. The
source reads the table inside a transaction of its own, with a number in the statement, and accepts
the list only when it shows that statement running. It asks up to three times, 150 milliseconds
apart, and a hole it gets no current list for waits
`gap_timeout_ms` like one with no `PROCESS`. A hole whose transactions have all finished cannot be
filled, so the rows behind it go out at once. A rolled-back transaction, a failed statement, a row
deleted before the source read it, and the numbers a bulk insert (`INSERT ... SELECT`,
`LOAD DATA`) reserves and never uses all leave holes of that kind.

While a noted transaction is still running, the hole is held past `gap_timeout_ms`, so a slow
writer's row still arrives in order. `gap_max_wait_ms` (5 minutes by default, `0` for no limit) ends
that wait. The list cannot say which tables a transaction touches, and an idle connection with a
transaction left open would otherwise stop the source for good. At the limit the source passes the
hole, logs a warning and counts it.

Without `PROCESS` the server refuses the probe the source makes when it connects. The source logs
once that it fell back, and only time is left. A hole still empty `gap_timeout_ms` after the source
first saw it (10 seconds by default) is passed, and each permanent hole holds the rows behind it for
that long once. Keep `gap_timeout_ms` above the longest transaction that writes the table: a
transaction open longer has its hole passed first, and the row it commits afterwards is not
delivered. `0` reads past holes at once.

`saci_mariadb_source_sequence_gaps_total` counts the holes passed, by `outcome`: `settled` when
nothing could fill the hole, `timeout` and `cap` when a row may still commit into it and would be
lost. With `initial "now"` a transaction that wrote its row before the source's first read is lost
the same way.

The check covers an integer cursor column that the server declares `AUTO_INCREMENT`, in `polling`
and in both `cdc_trigger` forms. Any other cursor, a timestamp or an application-assigned id, is read
as before. The step is the global `auto_increment_increment`, read on each connection: a server
that numbers by 3 shows no holes between neighbours, and a missing number is one. Several servers
that number one table with a shared increment, as a multi-primary cluster does, are checked against
that increment alone, and `INNODB_TRX` lists only the transactions of the server it is read on. The
source counts a transaction that has written a row or is waiting on a lock, which covers an insert
held up by a duplicate key check. A writer in the instant between taking its number and either one
is not seen.

A `where_clause` does not hide holes. The source reads the cursor values of the next window with no
filter, finds the holes there, and reads the filtered rows only up to the last number before the
first open hole. A number the filter drops is not a hole, and a window the filter empties moves the
offset past it. The window is four batches wide, at least 256 numbers, and the extra read is an index
scan of the cursor column.

The clocks and the noted transactions are kept in memory. The stored offset never passes an open
hole, so a restarted source reads each hole again and starts its wait from zero, and a source
restarted more often than the wait never passes a hole. A run that stops at the first end of data
delivers the rows before a hole and leaves the rest for the next run. A source with no stored offset
expects its first number within one step of 0, so a table whose first rows were deleted holds its
first read back until `INNODB_TRX` or the timeout passes the hole.

An outbox with `retention "delete_acked"` deletes the rows at or before the acknowledged offset at the
start of the next cycle, after the offset is stored, and never past an open hole. The source counts
those rows and deletes exactly that many in cursor order: a range `DELETE` reads one row past its
range, an open writer's uncommitted insert can be that row, and InnoDB makes the delete wait for the
writer. The limit stops the scan early only when the server walks an index in cursor order, so
index the cursor column, and the cursor and tiebreak pair when there is a tiebreak. A writer left
open below the offset, behind a hole passed at a limit, still holds a record the delete needs: the
delete waits 2 seconds, then the source leaves the prune and tries again 5 seconds later. An
installed changelog defaults to `delete_acked`, an outbox to `keep`.

Installed triggers are set up once per session on the first read. The connector reads the watched
table's columns and primary key, creates the changelog only when it is missing, and creates each
trigger only when it does not already exist with the same body (whitespace is ignored when bodies are
compared, so a trigger restored from a dump still matches). The three triggers are not created
atomically: a crash between them leaves a subset that the next start completes, and writes made in
between are not captured. Changes made before the first read are not captured either; use
`cdc_binlog` with `snapshot "initial"` for existing rows. The changelog holds `__seq`
(`BIGINT UNSIGNED AUTO_INCREMENT`), `__op` (`CHAR(1)`), `__commit_ts` (`TIMESTAMP(6)`, the server's
UTC time when the trigger fired) and every watched column with its exact column type and, for text,
enum and set columns, its character set, all nullable. An update that changes a primary key column
is recorded as `D` with the old key then `I` with the new one, in two consecutive `__seq` values; a
table with no primary key records every update as `U`. The triggers are created under an empty
`sql_mode`, because a trigger runs under the mode in force when it was created, so a zero date in the
watched table copies into the changelog and never fails the application's own `UPDATE`. The same mode
truncates a value too long for a changelog column that is narrower than the table's column, so a
running source compares the two tables' column types before it hands over a batch of rows, and a type
or character set that changed since the install stops it with a `no longer holds` error. A column
added to or dropped from the table does not.

`cdc_binlog` runs one dump per cycle with a non-blocking request, on a connection of its own and with
the source's `server_id`. The server ends the stream when it has sent everything, which is the end of
the cycle. Rows are buffered until their transaction commits, then emitted together. Every row of a
transaction carries the position of the event that ends it, its commit time (whole seconds, from the
commit event) and, on MySQL, its GTID (`uuid:n`, or `uuid:tag:n` for a tagged one). The position of a
fresh source (`now`, `beginning` or the literal) is stored as soon as it is resolved, so a restart
before the first change keeps the same start; a server that has executed nothing resolves `now` to an
empty GTID set, stored as `gtid:`. A stored offset of the other kind than `position` is refused.
Row events of other tables are skipped without decoding and counted, one per row event, in
`saci_mariadb_source_skipped_changes_total`.

A row is decoded with the definition the table had when the row was written. The source keeps a schema
history for that: the watched table's columns and primary key as the server reports them (names,
types, precision, scale, fractional seconds, signedness, character set), each tagged with the binlog
position from which it holds. The history is one row per definition in `<offset_table>_schema`,
beside the offset table, keyed by the source's `name` and the table, so sources that share the table
or the history table never read each other's rows.

A source records the catalog's definition at the position it starts from: always when it has no stored
position, unless the history already holds that definition there, and for a stored position only when
no definition is recorded at or before it. It records again whenever a DDL statement changes the
definition. After a DDL the catalog is read at the next row event of the table, or at the end of the
stream, and a definition that differs is checked against that row event's table map and recorded with
the position of the last DDL read. The snapshot records the definition it read at the position it pins.

A restart takes the definition in force at the stored position, then switches to each recorded one as
the stream reaches its position. While a recorded definition is ahead the catalog is not read, because
it may be newer than the DDL being replayed. Under `position "file"` the files compare by the number
in their name; under `position "gtid"` a definition holds from the executed GTID set after its DDL,
and a restart takes the newest one the stored set contains.

A column added since the stored position reads NULL for the rows written before it, so its field must
be nullable: a field declared `nullable=#false` is refused by name. A column dropped since still
decodes the older rows and reads NULL after the drop. A declared column that is in neither the
definition at the stored position nor the current catalog is refused as a typo.

Each row event's table map also names every column's type, and the declared columns are compared with
the definition in use (integer width, decimal precision and scale, temporal and bit kinds, text against
number). A position with no recorded definition (one stored with `schema_history #false`, or whose
history rows were deleted) takes the catalog as the definition there, so a row written before an
`ALTER TABLE` is refused with `changed shape under the binlog` or `changed under the binlog`, naming
the column. A source stopped across two `ALTER TABLE` statements with rows written between them is
refused the same way, because only the catalog after the second one is left to read the first one's
rows. A change the table map cannot show, such as a rename or a switch between signed and unsigned,
is read correctly only when its definition was recorded or is the only change since the position.

Once a position is stored, the definitions before the one in force at it are deleted, because a
restart never starts earlier. Rewinding the offset row past the retained definitions is refused by
the table map check. A failed delete is counted in `saci_mariadb_source_errors_total` with `kind`
`schema` and does not stop the source. `schema_history #false` keeps no table and no history: rows are
decoded with the catalog, and a DDL is picked up as it is read.

The dump connection stays open while the pipeline works on a batch. If that takes longer than the
server's network write timeout, the server drops it, the cycle fails, and the retry reads again from
the committed position. Not understood: XA transactions, `TRUNCATE TABLE` (it writes no row event),
statement-format changes, and savepoint rollbacks inside a transaction (the rolled-back rows still
appear). A partial JSON update is refused up front by the `binlog_row_value_options` check, and
MariaDB's compressed row events (`log_bin_compress`) by a check of its own: the reader cannot decode
them, so a server that compresses is refused at the start and a compressed row event of the table
already in the binlog stops the source with an error instead of being skipped. A MariaDB unsigned
integer arrives sign-extended in the binlog and is masked back to its width, so binlog and polling
rows are identical.

The snapshot, with no stored offset, runs on its own connection: `FLUSH TABLES WITH READ LOCK`, read the
position, `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY`, `UNLOCK TABLES`. The lock is held
for those statements only. The table is then read in primary-key pages of `batch_rows` as `R` rows,
all carrying the pinned file and position and the snapshot's start time, with `__gtid` NULL. The stream
starts at the pinned position, so a change made during the snapshot arrives once, from the stream, and
the `R` row keeps its old value.

The connection is opened lazily, so `validate` parses the DSN and stops there. An unreachable server,
a missing table and a mistyped column all surface on the first drain cycle, and the declared schema
is checked against the live catalog then. Every session runs in UTC (`time_zone = '+00:00'`) and
`utf8mb4`, so a value's text form is the same whatever the server's own defaults are.

The sink writes one flush as one transaction. A flush that fails rolls back whole. The batch whose
`write_batch` call failed is not kept: that call returned the error, and the service's retry wrapper
hands the same batch back, so keeping it too would write its rows twice. Rows that earlier calls
accepted under `flush_rows` stay buffered for the next write or `finish`, and a failed `finish`
keeps what it held. `flush_rows 0`, the default, makes that one transaction per batch; a higher
value holds rows in memory until it is reached, and a process killed before the flush loses them.
Rows go out in statements of `min(chunk_rows, 65535 / written columns)` rows, because a prepared
statement carries at most 65535 placeholders, and of at most the session's `max_allowed_packet`
bytes, counted from the values with a margin, so a batch of large blobs is split into more statements
rather than refused. A `DELETE` carries at most 1000 keys: past a few thousand keys both servers stop
using the index range and scan the table, which took minutes on MySQL for 16 383 composite keys, and
1000 take under 30 milliseconds.

If the connection is lost while `COMMIT` is in flight, the outcome is unknown: the server may have
committed. The sink reports the error, drops the session and the caller re-drives the batch, so the
rows can be written twice. `upsert`, `ignore_conflicts` and `apply_changes` absorb that; `append`
into a table with a unique key fails on the repeat, and without one it duplicates the rows.

A call that is dropped before it returns (a runner that cancels it, a `timeout` around it) is handled
the same way. The next call discards the session, so the server rolls the unfinished transaction back,
and reconnects. Buffered rows are kept, a dropped `write_batch` is treated like a failed one (its
batch is taken out, the caller re-drives it), and `truncate_before_first_write` clears the target again
because no flush has returned. If the dropped flush had already committed, the re-drive writes its
rows a second time, which only `append` shows. `saci_mariadb_sink_pending_rows` and `pending_rows`
still count a dropped call's rows until the next call.

`truncate_before_first_write` runs `DELETE FROM` on the target inside the first flush's transaction,
so a failed first flush leaves the old rows in place. It runs once per sink and never again after a
reconnect. It does not reset an `AUTO_INCREMENT` counter, and `TRUNCATE` is not used because it
commits implicitly.

The sink session runs a strict `sql_mode`. An out-of-range integer or decimal, a year or enum value
the column lacks, a string or binary longer than its column, text the column's character set cannot
hold, invalid JSON, and NULL for a `NOT NULL` column are errors that fail the flush and write
nothing, never clamps. The server's message follows the table name, as in
`MariadbSink: INSERT into 'shop.t' failed: Out of range value for column 'v' at row 1 [1264/22003]`.

A few cases the server does not treat as errors, and what the sink does about them:

- A value with more fraction digits than a `time`, `datetime` or `timestamp` column keeps is rounded
  by MySQL and truncated by MariaDB, silently. The sink refuses it first:
  `MariadbSink: column 'x' row N: the value carries fraction digits beyond the K the server column
  keeps`. Whole-second data into `DATETIME(0)` is fine.
- A `VARCHAR` given trailing spaces past its width is cut with a server warning and no error.
- MariaDB writes the current time for NULL into a `TIMESTAMP NOT NULL` column; MySQL raises an error.
- `TIMESTAMP` ends at 2038-01-19 on MySQL and on MariaDB 10.11, and reaches past 2100 on MariaDB 11.8.

The sink keeps no prepared-statement cache. It prepares its statements inside each flush and closes
them before the transaction ends. MySQL 8.4 and 9 can re-prepare a cached statement against a
redefined table with stale parameter state, which stored a wrong value without an error when a
column's type changed under a running sink. A source keeps a cache of 32 statements per session,
rebuilt on every reconnect, but the statements it runs once per batch or cycle bypass it: the cursor
reads, the `initial "now"` lookup, the `delete_acked` delete and the offset table's read and write each
prepare, execute and close their statement (`exec_closing`), so the server's statement count does not
grow and a table altered or recreated between cycles is read through its new definition.

Before each flush the sink checks its session and reconnects if the server or a proxy dropped it.

`connection.statement_timeout_ms` is `max_statement_time` on MariaDB, which interrupts any
statement. On MySQL it is `max_execution_time`, which covers `SELECT` only, so a slow `INSERT` or
`DELETE`, the `delete_acked` delete and trigger DDL included, is not interrupted. A statement timeout
is error 1969 on MariaDB and 3024 on MySQL. A server error leaves the connection usable; an I/O or
driver error drops it and the next cycle reconnects. `max_allowed_packet` limits a whole statement,
not one value: the sink splits a flush to fit it, so only a single row larger than the limit fails
the flush, on both products, and drops the connection on MariaDB.

## Every key

Source, at the top of `config`:

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | string | required | labels this node's metrics, keys its offset row and prefixes its errors |
| `connection` | node | required | how to reach the server |
| `mode` | node | required | `kind="polling"`, `"cdc_trigger"` or `"cdc_binlog"`, plus that mode's keys |
| `batch_rows` | integer | `8192` | rows per query when the runner sends no admission hint; at least 1 |
| `max_batches_per_cycle` | integer | `0` | batches per drain cycle; `0` is no cap |
| `schema_fields` | list of fields | required | the declared columns, in the order the component holds them |

Sink, at the top of `config`:

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | string | required | labels this node's metrics and prefixes its errors |
| `connection` | node | required | how to reach the server |
| `table` | string | required | the target table, `schema.table` or a bare name in the DSN's database |
| `write_mode` | string | `append` | `append`, `upsert`, `ignore_conflicts` or `apply_changes` |
| `conflict_columns` | list | empty | the conflict key, required by every mode but `append` |
| `update_columns` | list | empty | which columns an upsert rewrites; empty means all but the conflict key; `upsert` and `apply_changes` only |
| `dedupe_order_column` | string | absent | which row wins when one flush repeats a conflict key; `upsert` and `ignore_conflicts` only |
| `chunk_rows` | integer | `1000` | rows per `INSERT`, held under 65535 placeholders; at least 1 |
| `flush_rows` | integer | `0` | rows buffered before a flush; `0` flushes on every batch |
| `truncate_before_first_write` | bool | `#false` | empty the target inside the first flush |
| `schema_fields` | list of fields | required | the columns written, in the component's order |

A misspelled key anywhere in this `config` fails the parse and is named in the error.

### connection

| Key | Type | Default | What it does |
|---|---|---|---|
| `dsn` | string | required | a `mysql://` or `mariadb://` URL, `mysql://user:password@host:3306/db` |
| `user`<br>`password` | string | absent | override the DSN's user and password |
| `password_file` | string | absent | a file whose trimmed contents override the password |
| `connect_timeout_ms` | integer | `5000` | budget for one connect; `0` waits as long as the operating system does |
| `statement_timeout_ms` | integer | `30000` | server-side statement timeout, `0` for none; see above for MySQL |
| `ssl_mode` | string | `verify_identity` | `disabled`, `required`, `verify_ca` or `verify_identity` |
| `ssl_ca` | string | absent | a PEM bundle of trusted roots; absent means the built-in web roots |
| `reconnect` | node | see below | how a dropped session is re-established |

`required` encrypts and accepts any certificate, an IP-address host included. `verify_identity` checks
the chain and that the certificate names the host you connect to, an IP address included, which then
needs an IP subject alternative name. `verify_ca` is meant to check the chain only, but the driver's
host-name skip does not work with the TLS library in this build, so today it checks the host name
too and refuses a certificate that names another host, as `verify_identity` does. Connect by a name
the certificate carries, or use `required` for an encrypted but unverified session.
Anything but `disabled` needs the `tls` feature, which the
`connector-mariadb` service feature turns on. A `reconnect` child takes `max_attempts` `3`,
`base_delay_ms` `100`, `multiplier` `2.0`, `max_delay_ms` `30000` and `jitter` `0.1`. Nothing in the
connector logs a DSN, user or password: messages name the target as `host:port/db`.

### mode

| Key | Type | Default | What it does |
|---|---|---|---|
| `kind` | string | required | `polling`, `cdc_trigger` or `cdc_binlog` |
| `table` | string | required | the table read, or the outbox, in every kind |
| `cursor_column` | string | required for `polling` and an outbox | the column the cursor walks; refused with `install_triggers #true` |
| `tiebreak_column` | string | absent | breaks ties when the cursor column repeats; refused with `install_triggers #true` |
| `initial` | string | `"beginning"`; `"now"` for `cdc_binlog` | where a source with no stored position starts |
| `offset_table` | string | `"saci_source_offsets"` | where the position is kept |
| `offset_table_autocreate` | bool | `#true` | create the offset table, and for `cdc_binlog` the schema history table, when missing |
| `schema_history` | bool | `#true` | `cdc_binlog` only: keep the table's definitions, so a restart decodes each row with the one in force when it was written |
| `schema_history_table` | string | `<offset_table>_schema` | `cdc_binlog` only: where the definitions are kept; at most 64 characters a part; refused with `schema_history #false` |
| `where_clause` | string | absent | an extra filter on the read, added as written; refused with `install_triggers #true` |
| `install_triggers` | bool | `#false` | `cdc_trigger` only: install the changelog table and triggers |
| `changelog_table` | string | `saci_cdc_<name>` | `cdc_trigger` with `install_triggers #true` only; at most 64 characters |
| `retention` | string | `keep`; `delete_acked` with installed triggers | `cdc_trigger` only: `keep`, or `delete_acked` to delete changelog rows once read |
| `gap_timeout_ms` | integer | `10000` | `polling` and `cdc_trigger` only: how long a hole in an `AUTO_INCREMENT` cursor holds back the rows after it when `INNODB_TRX` cannot be read; `0` reads past holes; at most `3600000` |
| `gap_max_wait_ms` | integer | `300000` | `polling` and `cdc_trigger` only: how long a hole is held while `INNODB_TRX` shows a transaction that could fill it; `0` for no limit; not below `gap_timeout_ms`, at most `86400000` |
| `server_id` | integer | derived from `name` | `cdc_binlog` only: the replica id, unique per server, not `0` |
| `position` | string | `file` | `cdc_binlog` only: `file`, or `gtid` on MySQL |
| `snapshot` | string | `never` | `cdc_binlog` only: `never` or `initial` |
| `max_changes_per_cycle` | integer | `10000` | `cdc_binlog` only: decoded changes after which a cycle ends at a transaction boundary |

`initial` for `polling` and an outbox is `"beginning"`, `"now"` (start after the table's current
highest cursor value) or a literal cursor value of the cursor column's type.

### schema_fields

| Key | Type | Default | What it does |
|---|---|---|---|
| the column name | string | required | the leading argument of the entry; matched to the server by name, without regard to case |
| `type` | string | required | one of the names below |
| `nullable` | bool | `#true` | whether the column may hold nulls |
| `precision`<br>`scale` | integer | required for `decimal128` | the decimal width, 1 to 38, and its fractional digits, 0 up to the precision |
| `mariadb_type` | string | absent | the server's `DATA_TYPE` for this column, asserted on the first connection |

The accepted `type` names are `boolean`, `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`,
`uint32`, `uint64`, `float32`, `float64`, `decimal128`, `utf8`, `binary`, `date32`, `time64_micros`,
`timestamp_micros`, `timestamp_micros_utc` and `json`. `timestamp_micros` is a `datetime` with no
zone, `timestamp_micros_utc` an instant read as UTC, and `json` carries a document as `utf8` text.
The vocabulary has no list type. Columns the server has and the config does not declare are ignored.
Every declared column must exist on the server, and a mismatch is one error that lists every
problem.

### Type reference

Each row names a server column type, the `type` it maps to when nothing forces it, and every other
`type` it can fill. An unsigned integer column takes the unsigned default. An integer column fills any
integer type that holds all of its values: the same signedness at least as wide, and for an unsigned
column also a signed type strictly wider. `boolean` is `tinyint` only. A column type with no row
here is refused. A type that fills `utf8` is read as the server's own text form, and a `decimal128`
over a `decimal` column must match the column's precision and scale on a source and be no wider than
the column on either side of the point on a sink.

| Server type | Default `type` | Other `type` it fills |
|---|---|---|
| `tinyint` | `int8` | `boolean`, `int16`, `int32`, `int64`, `utf8` |
| `smallint` | `int16` | `int32`, `int64`, `utf8` |
| `mediumint`, `int` | `int32` | `int64`, `utf8` |
| `bigint` | `int64` | `utf8` |
| `year` | `int16` | `int32`, `int64`, `utf8` |
| `bit` | `uint64` | `binary` |
| `float` | `float32` | `float64` |
| `double` | `float64` | none |
| `decimal` | `decimal128` | `utf8` |
| `char`, `varchar`, `tinytext`, `text`, `mediumtext`, `longtext` | `utf8` | `json`, `binary` |
| `binary`, `varbinary`, `tinyblob`, `blob`, `mediumblob`, `longblob` | `binary` | none |
| `enum`, `set` | `utf8` | none |
| `json` (MySQL) | `json` | `utf8` |
| `date` | `date32` | `utf8` |
| `time` | `time64_micros` | `utf8` |
| `datetime` | `timestamp_micros` | `utf8` |
| `timestamp` | `timestamp_micros_utc` | `utf8` |
| `geometry`, `point`, `linestring`, `polygon`, `multipoint`, `multilinestring`, `multipolygon`, `geometrycollection`, `geomcollection`, `vector` | `binary` | none |
| `uuid`, `inet4`, `inet6` (MariaDB) | `utf8` | none |

A `uuid`, `inet4` or `inet6` column cannot be read from the binlog. A `char`, `varchar` or text
column in a character set other than `utf8mb4`, `utf8mb3`, `utf8` or `ascii` cannot be read from
the binlog either; read it with `polling` or convert the column. A MariaDB `JSON` column is
`longtext` and reads as `utf8`. MySQL's `json` is read in a canonical form, compact with keys sorted,
in every mode, so a document reads identically from a poll and from the binlog.

How values read:

- A zero date (`0000-00-00`) into `date32` or a timestamp becomes NULL on a nullable field and an
  error otherwise: `column 'x': zero date; declare the field nullable`.
- A `time` outside `00:00:00` to `24:00:00` into `time64_micros` is refused: `column 'x': TIME value
  '-12:00:00' is outside a time of day; declare the field utf8`.
- A `tinyint` into `boolean` must be 0 or 1.
- A value that is not valid UTF-8 in a `utf8` field is refused: `column 'x': not valid UTF-8; declare
  it binary`.
- NULL in a field declared `nullable=#false` is refused: `column 'x': NULL in a field declared
  nullable=#false; declare the field nullable`.
- A `timestamp` reads as the instant in UTC.

Values write as `Int`, `UInt`, floats, exact decimal text for `decimal128`, text, bytes, and civil
date and time values; a timestamp is written as its UTC wall clock whatever zone the Arrow field
carries. A float that is not finite, a year beyond 0000 to 9999 and a time outside one day are refused
with the column and the row. A geometry value is written in the server's internal form, a 4-byte SRID
followed by WKB, which is the form a read returns.

### Metrics

With the `metrics` feature the source records `saci_mariadb_source_batches_total`,
`saci_mariadb_source_rows_total`, `saci_mariadb_source_query_duration_seconds`,
`saci_mariadb_source_errors_total` and `saci_mariadb_source_skipped_changes_total`, labelled `name`
and `mode`; errors also carry `kind` (`connect`, `query`, `decode`, `offset`, `schema`, `binlog`, `trigger` or
`snapshot`). The sink records `saci_mariadb_sink_flushes_total`, `saci_mariadb_sink_rows_total`,
`saci_mariadb_sink_flush_duration_seconds`, `saci_mariadb_sink_errors_total` (`kind` is `connect`,
`encode`, `write` or `commit`) and `saci_mariadb_sink_pending_rows`, labelled `name`.

A `polling` or `cdc_trigger` source also records `saci_mariadb_source_sequence_gaps_total`, the
holes in an `AUTO_INCREMENT` cursor it passed, labelled `outcome` (`settled`, `timeout` or `cap`),
and `saci_mariadb_source_sequence_held_rows`, a gauge of the rows it held behind an open hole at its
last read.

## Server setup

`cdc_binlog` needs MySQL 8.2 or newer, or MariaDB 10.5.2 or newer, with these settings. They are
checked at the first connect, and a missing one is refused by name.

- `log_bin` on, `binlog_format = ROW` and `binlog_row_image = FULL`.
- On MySQL, `binlog_row_value_options` empty, so a partial JSON update never reaches the stream.
- For `position "gtid"`, MySQL with `gtid_mode = ON`.
- A `server_id` on the source server different from the `server_id` the connector registers with.

Grants, per mode. Replace `saci` and `shop` with your own user and schema.

| Mode | Source user |
|---|---|
| `polling`, outbox | `GRANT SELECT ON shop.* TO 'saci'@'%'` |
| outbox with `delete_acked` | the above plus `DELETE` |
| installed `cdc_trigger` | `GRANT SELECT, TRIGGER, CREATE, INSERT, DELETE ON shop.* TO 'saci'@'%'`: `SELECT` and `TRIGGER` on the watched table, `CREATE` on the changelog's schema for the first start only, `INSERT` on the changelog because the trigger writes as its definer, `SELECT` and `DELETE` on the changelog for the reader and `delete_acked` |
| `cdc_binlog` on MySQL | `GRANT SELECT ON shop.*` plus `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.*` |
| `cdc_binlog` on MariaDB | `GRANT SELECT ON shop.*` plus `GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.*` |
| `snapshot "initial"` | the binlog grants plus `RELOAD`, for a brief global read lock |
| sink | `GRANT SELECT, INSERT, UPDATE ON shop.*`, plus `DELETE` for `apply_changes` and `truncate_before_first_write` |

Every source also stores its position in `offset_table`, so its user needs `SELECT`, `INSERT` and
`UPDATE` on it. `CREATE` is needed only when the table is missing and `offset_table_autocreate` is
`#true`: the connector creates the table only when the catalog says it is missing, so a pre-created
table needs no `CREATE` privilege. A missing dump privilege surfaces as error 1045 on the dump
request and a missing status privilege as 1227; both messages carry the grant to add. Creating a
trigger while binary logging is on needs `SUPER`, or
`log_bin_trust_function_creators = 1` (error 1419 otherwise). Either set the variable, or create the
triggers as a privileged user and run the source with `install_triggers #false` over the changelog.
A missing `TRIGGER` privilege is error 1142 and the message names the grants.

`PROCESS` is optional for `polling` and both `cdc_trigger` forms over an `AUTO_INCREMENT` cursor.
`GRANT PROCESS ON *.* TO 'saci'@'%'` lets the source read `INNODB_TRX`, which passes a hole nothing
can fill at once and waits for a slow writer instead of timing out. Without it the source logs once
that it fell back to `gap_timeout_ms`.

A `cdc_binlog` source with `schema_history #true` keeps its definitions in a second table and needs
`SELECT` and `INSERT` on it. `DELETE` prunes it: without that privilege the source runs and the table
grows, with each refused delete counted. `CREATE` is needed under the same conditions as for the
offset table, and `schema_history #false` needs none of these.

The connector never drops a table or a trigger it installed. To remove an installed capture, run
these on the server, with the source's `name` in place of `<name>` and `-` written as `_`:

<div class="code">
<div class="code-cap"><span>SQL</span><em>removes the triggers, the changelog and the stored position</em></div>

```sql
DROP TRIGGER shop.saci_<name>_ai;
DROP TRIGGER shop.saci_<name>_au;
DROP TRIGGER shop.saci_<name>_ad;
DROP TABLE shop.saci_cdc_<name>;
DELETE FROM shop.saci_source_offsets WHERE source_name = '<name>';
```

</div>

Deleting a source's row from the offset table makes it start again from `initial`. A cursor source
stores its cursor and tiebreak values as text there, so a row can be edited to reset a source to a
chosen value. A `cdc_binlog` source's definitions sit in the schema history table under the same
`name`; delete them with the offset row when the source is gone for good.

## When it refuses to start

Each message is prefixed with the half it came from, `MariadbSource:` or `MariadbSink:`. Config
refusals come when the node is built; the server refusals come on the first drain cycle.

| Message | What to change |
|---|---|
| `connection.dsn must be a 'mysql://' or 'mariadb://' URL` | use one of those schemes |
| `connection.dsn names no database, so mode.table '{t}' must be written schema.table` | write `schema.table`, or put a database in the DSN; the sink says `table '{t}'` |
| `connection.ssl_mode '{mode}' needs the 'tls' feature of saci-connector-mariadb` | use a build carrying TLS, or set `ssl_mode "disabled"` |
| `connection.reconnect.max_attempts must be at least 1` | raise it; `multiplier` must be at least `1.0` and `jitter` within `0.0` to `1.0` |
| `schema_fields declares '{name}' more than once` | declare each column once |
| `field '{name}': type "decimal128" requires 'precision'` | add `precision` and `scale` |
| `field '{name}': precision and scale apply to type "decimal128" only, not "{type}"` | drop the two keys |
| `field '{name}': mariadb_type '{t}' is not a column type this connector reads or writes` | name a `DATA_TYPE` from the type reference |
| `mode.cursor_column '{c}' has type "{type}", which cannot order a cursor` | declare the cursor column as an integer, `utf8`, `date32` or timestamp type |
| `mode.tiebreak_column '{c}' repeats mode.cursor_column` | pick a different tiebreak column |
| `schema_fields '{name}' uses the reserved __ prefix, which only cdc_binlog and an installed cdc_trigger fill` | drop the field, or switch mode |
| `schema_fields '{name}' uses the reserved __ prefix, and a cdc_binlog source fills only {names}` | use one of the names in the reserved column table |
| `reserved column '{name}' must be declared type "{expected}", not "{actual}"` | declare the type the table shows |
| `mode.cursor_column cannot be set with install_triggers #true; the cursor is the changelog's own __seq` | drop it; the same applies to `tiebreak_column` and `where_clause` |
| `mode.cursor_column is required with install_triggers #false` | name the outbox's ordering column |
| `mode.changelog_table applies with install_triggers #true only` | drop it, or install the triggers |
| `mode.changelog_table '{part}' is {n} characters, over the 64 a table name may have` | set a shorter `changelog_table` |
| `mode.server_id must not be 0` | pick another id |
| `mode.snapshot "initial" pins its own start position, so mode.initial must stay "now"` | drop `initial`, or drop the snapshot |
| `mode.initial '{x}' must be "now", "beginning" or 'file:<binlog name>:<position>' under position "file"` | write a file position, or use a GTID set under `position "gtid"` |
| `write_mode "{mode}" requires a non-empty conflict_columns` | add `conflict_columns` |
| `{key} applies to write_mode "upsert", "ignore_conflicts" or "apply_changes", not "append"` | drop the key, or pick another mode |
| `update_columns '{c}' is also a conflict column` | remove it from `update_columns` |
| `dedupe_order_column cannot be set with write_mode "apply_changes"` | drop it; the rows apply in order |
| `write_mode "apply_changes" needs a declared '__op' field of type "utf8"` | declare `__op` as `utf8` |
| `{key} '{c}' is a reserved __ column, which write_mode "apply_changes" reads and never writes` | name a table column |
| `declared schema does not match the server: column '{c}': the server type '{t}' cannot fill a declared "{type}"; declare one of {types}` | use a type from the list, which comes from the type reference |
| `table '{t}' does not exist on {host:port/db}, or the connecting user cannot see it` | create the table, or grant `SELECT` on it |
| `conflict_columns ({c}) match no PRIMARY KEY or UNIQUE index of '{t}'; ON DUPLICATE KEY UPDATE would resolve on a different key` | name exactly a primary key or a unique index |
| `cdc_binlog needs binary logging on {target}` | start the server with `log_bin` |
| `cdc_binlog needs binlog_format = ROW on {target}` | set `binlog_format = ROW`; `binlog_row_image = FULL` is checked the same way |
| `cdc_binlog needs binlog_row_value_options = '' on {target}` | clear it on MySQL |
| `position "gtid" needs gtid_mode = ON on {target}` | enable GTIDs, or use `position "file"`; MariaDB takes `file` only |
| `snapshot "initial" needs '{t}' to have a primary key` | add a primary key, or drop the snapshot |
| `snapshot "initial" pages '{t}' by its primary key, so every key column must be declared in schema_fields (missing: {cols})` | declare the key columns |
| `snapshot "initial" needs the RELOAD privilege for a brief global read lock` | grant `RELOAD` |
| `trigger '{n}' exists with a different body; drop it or set install_triggers #false` | drop the trigger, or stop installing |
| `changelog table '{t}' no longer matches '{w}'; drop it and its triggers to rebuild, or set install_triggers #false` | drop the changelog and triggers; the next start rebuilds them |
| `changelog table '{t}' no longer holds what '{w}' does ({detail}), so rows the triggers captured since may be truncated` | a column's type changed under a running source; drop the changelog and triggers to rebuild |
| `with binary logging on, creating triggers needs SUPER or log_bin_trust_function_creators = 1` | grant `SUPER`, or set the variable |
| `the connecting user needs the TRIGGER privilege on '{t}' and INSERT on '{c}'` | grant both |
| `a '{type}' column cannot be read from the binlog, which carries it as raw bytes` | leave it out of `schema_fields`, or read with `polling` |
| `a partial JSON update reached the binlog; set binlog_row_value_options = '' on the server` | clear it on MySQL |
| `cdc_binlog needs log_bin_compress = OFF on {target}` | switch off MariaDB's binlog compression; the reader cannot decode compressed row events |
| `a row event compressed by MariaDB's log_bin_compress, which this reader cannot decode` | a compressed event of the table is in the binlog; set `log_bin_compress = OFF` and start the source after it with `mode.initial` |
| `row {n} has __op '{x}'; apply_changes understands I, U, R and D` | fix the upstream `__op` values |
| `INSERT into '{t}' failed: {server message}` | fix the data, or the column it names |
| `DELETE from '{t}' failed: {server message}`, `cannot commit the write to '{t}': {error}` | read the server message; the flush rolled back whole |
| `row {n} has __op 'D' with a NULL conflict key, which matches no row` | give the delete a key |
| `column '{c}' row {n}: the value carries fraction digits beyond the {k} the server column keeps` | round the value, or widen the server column |
| `{key} '{c}' does not name a declared schema_fields entry` | declare the column, or fix the name in `cursor_column`, `tiebreak_column`, `conflict_columns`, `update_columns` or `dedupe_order_column` |
| `connection.dsn must not be empty` | give a URL |
| `cannot parse connection.dsn: {error}` | fix the URL; the message never repeats it |
| `cannot read connection.password_file '{path}': {error}` | fix the path or its permissions |
| `cannot read connection.ssl_ca '{path}': {error}`, `connection.ssl_ca '{path}' is empty` or `contains no PEM certificate` | point it at a PEM bundle |
| `'name' '{name}' contains '{c}'; only ASCII letters, digits, '_' and '-' are allowed` | rename it; it is also limited to 191 characters and may not be empty |
| `{key} '{t}' must be 'table' or 'schema.table', not a longer path` | write one or two parts; an empty part is refused too |
| `schema_fields must declare at least one field`, `schema_fields[{i}] has an empty 'name'` | declare at least one named field |
| `field '{name}': type "decimal128" requires 'scale'`, `decimal128 precision must be within 1..=38`, `decimal128 scale must be within 0..=precision` | fix the two keys |
| `batch_rows must be at least 1`, `chunk_rows must be at least 1`, `mode.max_changes_per_cycle must be at least 1` | raise it |
| `mode.where_clause must not be empty; leave it out to read every row` | drop the key, or give a predicate |
| `mode.initial must be "beginning", "now", or a literal cursor value` | give a value; under `cdc_binlog` it is `"now"`, `"beginning"` or a position literal |
| `mode.initial '{x}' is not {type}, which cursor column '{c}' needs` | give a literal of the cursor's type |
| `mode.initial '{x}' is a file position, but position is "gtid"`, `... is not a GTID set ({reason})` | give a GTID set, `"now"` or `"beginning"` |
| `the offset stored in '{offset_table}' for this source ('{v}') is not {type}, which cursor column '{c}' needs; delete its row or set the cursor column back` | delete the source's offset row |
| `the offset row for this source in '{offset_table}' is not a binlog position: {reason}` | delete the row, or restore a `file:` or `gtid:` value |
| `cannot create offset table '{t}': {error}` | grant `CREATE`, or create the table and set `offset_table_autocreate #false` |
| `cannot create schema history table '{t}': {error}; grant CREATE, or create it and set mode.offset_table_autocreate #false` | grant `CREATE`, create the table, or set `schema_history #false` |
| `cannot read schema history table '{t}': {error}; grant SELECT on it`, `cannot write schema history table '{t}': {error}; grant INSERT on it` (or `; create it, or leave mode.offset_table_autocreate #true to have it created` when the table is missing), `cannot prune schema history table '{t}': {error}; grant DELETE on it` | grant the privilege or create the table; a refused prune is counted and does not stop the source |
| `row {n} of schema history table '{t}' is not usable: {reason}; delete this source's rows from it` | delete the source's rows from the history table, which are then recorded again |
| `mode.schema_history_table '{part}' is {n} characters, over the 64 a table name may have; set mode.schema_history_table to a shorter name` | name the table, or shorten `offset_table` |
| `mode.schema_history_table applies with schema_history #true only; drop it, or keep the history` | drop the key |
| `column '{c}' is declared nullable=#false, but a definition of '{t}' in the schema history has no such column ...; declare the field nullable` | declare it nullable, or start the source after the column was added |
| `'{sql}' returned no row on {target}; binary logging is off`, `{target} lists no binary logs; binary logging is off` | enable `log_bin` |
| `cdc_binlog needs MySQL 8.2 or later, or MariaDB 10.5.2 or later; the server reports {product} {version}` | use a newer server |
| `the stored position is no longer on the server (purged?): delete the source's row from '{offset_table}' and set mode.initial` | follow it |
| `table '{t}' changed shape under the binlog (binlog has {n} columns, catalog has {m}); the schema history in '{h}' holds no definition that decodes them ...`, `table '{t}' changed under the binlog (column '{c}' is {x} in the binlog and {y} in the catalog); ...` | the table was altered more than once while the source was stopped, or the position has no recorded definition; restart from a position after the change. With `schema_history #false` the tail reads `mode.schema_history is #false, so only the table's current definition decodes rows` |
| `cannot connect to {host:port/db}: {error}`, including `no answer within {n} ms` | check the host, the port and `connect_timeout_ms` |
| `cannot run '{statement}' on {host:port/db}: {error}` | a session setup statement failed; the server refused the time zone, character set, statement timeout or sink `sql_mode` setting |
| `cannot read the columns of '{t}': {error}`, `cannot read the keys of '{t}': {error}` | grant the user access to the table |
| `declared schema does not match the server: column '{c}' is declared but the server has no such column (server columns: {list})` | fix the name or add the column; every problem is listed in one message, separated by `;` |
| `column '{c}': mariadb_type '{asserted}' is declared but the server column is '{actual}'` | fix `mariadb_type` |
| `column '{c}': the server type '{t}' is not supported by this connector` | leave the column out of `schema_fields` |
| `column '{c}': declared decimal128({p}, {s}) does not fit the server column '{t}', which holds {n} digit(s) before the point and {m} after it` | on a sink, declare a precision and scale no wider than the column |
| `column '{c}': declared decimal128({p}, {s}) but the server column is '{t}'; declare precision {P} and scale {S}` | on a source, match the column exactly |
| `column '{c}': the server column '{t}' is wider than the 38 digits of decimal128; declare it "utf8"` | declare `utf8` |
| `column '{c}': the binlog carries '{t}' text in the column's character set '{cs}', which is not UTF-8; convert the column to utf8mb4 or read with mode kind "polling"` | convert it, or poll |
| `column '{c}': cannot read the labels of '{t}': {reason}` | the `enum` or `set` definition could not be parsed |
| `schema_fields '{name}' uses the reserved __ prefix, and an installed cdc_trigger fills only __op, __seq, __commit_ts, __table` | use those names |
| `snapshot "initial" pages '{t}' by its primary key, so key column '{c}' must not be decimal, float or double` | snapshot a table with another key, or skip the snapshot |
| `cannot lock {host:port/db} for the snapshot: {error}`, `snapshot statement '{s}' failed on {host:port/db}: {error}`, `cannot read a snapshot page of '{t}': {error}` | read the server message; the first adds the `RELOAD` hint |
| `table '{t}' has a column named '{c}', which the changelog reserves; rename it or read the table with mode kind "polling"` | rename the column (`__seq`, `__op` or `__commit_ts`), or poll |
| `cannot create changelog table '{t}': {error}`, with `; grant CREATE on '{schema}' or create '{t}' yourself` for a privilege error | grant `CREATE` or create it |
| `mode.gap_timeout_ms {n} is over the 3600000 (one hour) a hole may be waited for; give a shorter wait, or 0 to read past holes` | lower it |
| `mode.gap_max_wait_ms {n} is over the 86400000 (one day) a hole may be held; give a shorter wait, or 0 for no limit` | lower it |
| `mode.gap_max_wait_ms {n} is below mode.gap_timeout_ms {m}; the limit on a hole a transaction may still fill cannot be shorter than the fallback wait (or give 0 for no limit)` | raise it, or use `0` |
| `mode.gap_max_wait_ms has no effect with mode.gap_timeout_ms 0, which reads past holes at once; drop the key` | drop it |
| `mode.gap_timeout_ms applies to an integer cursor column, and '{c}' is declared "{type}"; drop the key`, and the same for `mode.gap_max_wait_ms` | drop it: only an integer cursor is a sequence |
| `cannot read the AUTO_INCREMENT settings of '{t}': {error}` | read the server message |
| `cannot count acknowledged rows of '{t}': {error}`, `cannot prune acknowledged rows from '{t}': {error}` | grant `SELECT` and `DELETE` on the table, and read the server message |
| `cannot create trigger '{n}' on '{t}': {error}`, `cannot list the triggers of '{t}': {error}`, `cannot {step} to install triggers: {error}` | read the server message; the first adds the `TRIGGER` or `SUPER` remedy |
| a drift detail after `no longer matches`: `column '{c}' is missing from the changelog`, `column '{c}' is {x} in the changelog and {y} in the table`, `column '{c}' exists only in the changelog`, or a wrong type for `__seq`, `__op` or `__commit_ts` | drop the changelog and triggers to rebuild |
| `cannot look up`, `cannot read` or `cannot write offset table '{t}': {error}` | grant `SELECT`, `INSERT` and `UPDATE` on it |
| `cannot read {the binlog settings\|binlog_row_value_options\|gtid_mode} on {host:port/db}: {error}` | the user could not read a server variable |
| `mode.initial '{x}': {reason}`, where the reason is `'{p}' is not a byte position`, `'{t}' is not a UUID`, `'{s}' is not a transaction number or range` or `invalid GTID set` | fix the literal |
| `cannot read gtid_purged on {host:port/db}: {error}` | grant access to the variable |
| a dump or status error ending `; grant REPLICATION SLAVE and REPLICATION CLIENT` (MySQL) or `; grant REPLICATION SLAVE and BINLOG MONITOR` (MariaDB) | grant them |
| `MariadbSource config: {error}` and `MariadbSink config: {error}` | the config did not parse; an unknown key is named in the error |

## Next

- [Sources and sinks](@/service/connectors/_index.md), the other transports.
- [Workflows and links](@/service/config/workflows.md), the nodes these two sit in.
