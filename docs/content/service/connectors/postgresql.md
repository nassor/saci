+++
title = "PostgreSQL"
description = "Polling, a trigger outbox or logical decoding on the way in; a binary bulk load, staged when a cast or an upsert needs it, on the way out."
template = "subpage.html"
weight = 5
aliases = ["/connectors/postgresql/"]
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
value = "no: build with <code>--features connector-postgresql</code>"
+++
A workflow reads changes out of one table, processes the rows, and upserts the result into another
table.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="pg-t pg-d">
        <title id="pg-t">One table read into a workflow and the result written to another table</title>
        <desc id="pg-d">A table box on the left feeds a source node named pg_orders. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named pg_enriched. The sink writes the table box on the right. No transformer sits anywhere in the chain, because rows arrive already typed.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">table</text>
            <text class="t-sm" x="10" y="74">public.orders</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#pg-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">pg_orders</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#pg-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#pg-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">pg_enriched</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#pg-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">table</text>
            <text class="t-sm" x="566" y="74">enriched_orders</text>
        </g>
        <defs>
            <marker id="pg-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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

- A binary built with `--features connector-postgresql`; PostgreSQL is one of five connectors
  that need a specific service installed and running, so it sits outside the default build.
- A reachable server. One in a container is enough, and this command starts it with the settings
  logical decoding needs:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>wal_level and the replication slots are server settings, not connector keys</em></div>

```text
docker run --rm -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=saci -e POSTGRES_DB=postgres postgres:18-alpine postgres -c wal_level=logical -c max_replication_slots=8
```

</div>

- For `kind="cdc_logical"` only: PostgreSQL 10 or newer, `wal_level = logical`,
  `max_replication_slots` of at least 1, and a connecting role carrying the `REPLICATION`
  attribute. Neither the setting nor the privilege is probed up front. The server rejects the slot
  query instead, and the error names which one is missing.
- For `kind="cdc_logical"` again: a publication covering the table, for example
  `CREATE PUBLICATION saci_orders_pub FOR TABLE public.orders;`. A `DELETE` carries only the
  replica-identity columns unless the table is `REPLICA IDENTITY FULL`, so declare every other
  column nullable.
- The processor component the config names, at `pipelines/orders.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from;
  `validate --connectors-only` below runs without it.
- No transformer. Rows arrive typed, so this connector names no format at all.

## 1. Read from a table: the source node

One `mode` node picks how changes are found: `polling` walks a cursor column, `cdc_trigger` reads a
trigger-written outbox table, and `cdc_logical` decodes the write-ahead log.

<div class="code">
<div class="code-cap"><span>KDL</span><em>the whole config table is read by the connector, so name is repeated inside it</em></div>

```kdl
source "pg_orders" type="PostgresSource" component="OrderChange" {
    config {
        name "pg_orders"

        connection dsn="${SACI_PG_DSN}" sslmode="require"

        mode kind="polling" {
            table "public.orders"
            cursor_column "id"
        }

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

`name` labels this node's metrics and prefixes every error it raises. `connection.dsn` is the only
required connection key, and `${SACI_PG_DSN}` reads it from the environment.

`polling` and `cdc_trigger` both need `table` and `cursor_column`, and remember where they stopped
in an offset table they create for themselves. `cdc_logical` needs `slot`, `publication` and
`table`, and creates the slot unless `slot_autocreate` says otherwise.

`schema_fields` is required and uses this connector's own type vocabulary, listed under
[Every key](#every-key) below.

## 2. Write to a table: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>upsert makes a replayed drain cycle idempotent</em></div>

```kdl
sink "pg_enriched" type="PostgresSink" component="EnrichedOrder" {
    config {
        name "pg_enriched"
        table "public.enriched_orders"
        write_mode "upsert"
        conflict_columns "id"
        dedupe_order_column "source_lsn"
        chunk_rows 65536

        connection dsn="${SACI_PG_DSN}" sslmode="require"

        schema_fields "id" type="int64" nullable=#false
        schema_fields "source_lsn" type="int64" nullable=#false
        schema_fields "total" type="decimal128" precision=18 scale=4 nullable=#true pg_type="numeric(18,4)"
    }
}
```

</div>

`write_mode` is `append`, `upsert` or `ignore_conflicts`. The last two need `conflict_columns`, the
key the conflict is detected on. An omitted `update_columns` rewrites every declared column except
those.

`dedupe_order_column` is required when one batch can repeat a conflict key, and the highest value
of that column wins. Without it the server refuses to touch the same row twice in one statement.

`pg_type` pins the exact server column, modifier included, so a narrower `scale` than the column's
own is refused before any row is written.

## 3. Validate and run

`examples/configs/postgresql.kdl` reads `public.orders` through logical decoding and upserts into
`public.enriched_orders`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>no database connection is opened by validation</em></div>

```text
saci-service validate --config examples/configs/postgresql.kdl --connectors-only
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
<div class="code-cap"><span>Bash</span><em>the DSN is required: the config has no default for it</em></div>

```bash
export SACI_PG_DSN='postgres://saci@localhost:5432/app'
saci-service serve --config examples/configs/postgresql.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same two steps</em></div>

```powershell
$env:SACI_PG_DSN = "postgres://saci@localhost:5432/app"
saci-service serve --config examples/configs/postgresql.kdl
```

</div>

Every two seconds new rows in `public.orders` appear in `public.enriched_orders`, keyed by `id`.

## How it delivers

Every mode reports EOF once it is caught up, so any run mode drives this source. An `interval` run
mode is the usual choice. Each tick is one drain cycle over whatever arrived since the last one.

Delivery is at-least-once. The cursor advances in memory as each batch is handed over, and the
durable position, the row in `offset_table` or the slot's confirmed LSN, is written at the start of
the next cycle. A crash mid-cycle therefore replays that whole cycle, not just its last batch,
which is why the example sink upserts: a replayed cycle rewrites the same rows instead of
duplicating them.

`cdc_logical` moves the slot only to the newest commit whose changes reached the pipeline, so a
cycle `max_batches_per_cycle` cuts short leaves the rest behind the slot and decodes it again.

The connection is opened lazily, so `validate` parses the DSN and stops there. An unreachable
server, a missing table and a mistyped column all surface on the first drain cycle.

One flush is one transaction, so a write that fails leaves the target as it was. `flush_rows 0`,
the default, makes that one transaction per batch; a higher value holds rows in memory until it is
reached, and a process killed before the flush loses them.

The sink bulk loads in binary form. A column that needs a cast, and every `upsert` or
`ignore_conflicts` write, is staged through a temporary table first, so the server applies the
target's own type rules on the way in. A column typed as a domain keeps its constraints during the
load.

Every session pins its date, interval, float, byte and money formatting and runs in UTC, so a
value's text form is the same whatever the server's own defaults are.

## Every key

Source, at the top of `config`:

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | string | required | labels this node's metrics and prefixes its errors |
| `connection` | node | required | how to reach the server |
| `mode` | node | required | `kind="polling"`, `"cdc_trigger"` or `"cdc_logical"`, plus that mode's keys |
| `batch_rows` | integer | `8192` | rows per query when the runner sends no admission hint |
| `max_batches_per_cycle` | integer | `0` | batches per drain cycle; `0` is no cap |
| `notify` | node | absent | wait on a `LISTEN` channel instead of a timer; takes a required `channel` and `timeout_ms`, default `30000` |
| `schema_fields` | list of fields | required | the declared columns, in the order the component holds them |

Sink, at the top of `config`:

| Key | Type | Default | What it does |
|---|---|---|---|
| `name` | string | required | labels this node's metrics and prefixes its errors |
| `connection` | node | required | how to reach the server |
| `table` | string | required | the target table |
| `write_mode` | string | `append` | `append`, `upsert` or `ignore_conflicts` |
| `conflict_columns` | list | empty | the conflict key, required by `upsert` and `ignore_conflicts` |
| `update_columns` | list | empty | which columns an upsert rewrites; empty means all but the conflict key |
| `dedupe_order_column` | string | absent | which row wins when one batch repeats a conflict key |
| `chunk_rows` | integer | `65536` | rows per bulk-load chunk inside one transaction |
| `flush_rows` | integer | `0` | rows buffered before a flush; `0` flushes on every batch |
| `truncate_before_first_write` | bool | `#false` | empty the target before the first write |
| `schema_fields` | list of fields | required | the columns written, in the component's order |

A misspelled key anywhere in this `config` fails the parse and is named in the error.

### connection

| Key | Type | Default | What it does |
|---|---|---|---|
| `dsn` | string | required | the connection string |
| `user`, `password`, `password_file`, `application_name`, `sslrootcert` | string | absent | overrides and additions to the DSN |
| `connect_timeout_ms` | integer | `5000` | budget for one connect |
| `statement_timeout_ms` | integer | `30000` | server-side statement timeout |
| `sslmode` | string | `prefer` | `disable`, `prefer` or `require` |
| `reconnect` | node | see below | how a dropped session is re-established |

A `reconnect` child takes `max_attempts` `3`, `base_delay_ms` `100`, `multiplier` `2.0`,
`max_delay_ms` `30000` and `jitter` `0.1`.

### mode

| Key | Type | Default | What it does |
|---|---|---|---|
| `kind` | string | required | `polling`, `cdc_trigger` or `cdc_logical` |
| `table` | string | required | the table read, in every kind |
| `cursor_column` | string | required for `polling` and `cdc_trigger` | the monotonic column the cursor walks |
| `tiebreak_column` | string | absent | breaks ties when the cursor column repeats |
| `initial` | string | `"beginning"` | where a fresh cursor starts |
| `offset_table` | string | `"saci_source_offsets"` | where the cursor is kept |
| `offset_table_autocreate` | bool | `#true` | create that table when it is missing |
| `where_clause` | string | absent | an extra filter on the read |
| `retention` | string | `keep` | `keep`, or `delete_acked` to delete outbox rows once read; `cdc_trigger` only |
| `slot` | string | required for `cdc_logical` | the replication slot |
| `publication` | string | required for `cdc_logical` | the publication covering the table |
| `slot_autocreate` | bool | `#true` | create the slot when it is missing |
| `max_changes_per_cycle` | integer | `10000` | decoded changes per drain cycle |

### schema_fields

| Key | Type | Default | What it does |
|---|---|---|---|
| the column name | string | required | the leading argument of the entry |
| `type` | string | required | one of the names below |
| `nullable` | bool | `#true` | whether the column may hold nulls |
| `precision`, `scale` | integer | required for `decimal128` | the decimal width, and its fractional digits |
| `item` | string | required for `list` | the element type, which is any scalar name below |
| `pg_type` | string | absent | the server type the column really has; see below |

The accepted `type` names are `boolean`, `int16`, `int32`, `int64`, `float32`, `float64`, `utf8`,
`binary`, `date32`, `time64_micros`, `timestamp_micros`, `timestamp_micros_utc`, `uuid`, `json`,
`decimal128`, `interval_month_day_nano` and `list`. That vocabulary is this connector's own: it adds
the temporal, `uuid`, `json`, `decimal128`, `interval_month_day_nano` and `list` names, and it drops
`int8`, the unsigned widths, `largeutf8` and `date64`.

Every PostgreSQL type is readable and writable. One with no direct counterpart arrives as `utf8` in
the server's own canonical text form. A `money` column declared `decimal128` needs a `scale` of at
least 2, because `money` carries two fractional digits and a narrower scale would lose them.

### pg_type

`pg_type` is an optional key on each `schema_fields` entry, on both halves. It names the PostgreSQL
type the column really has, which does two things `type` alone cannot: it asserts the server's
column really is that type, and it picks the wire form the values travel in, binary or that type's
own text. It never changes the column's own type; `type` alone decides that.

It accepts a plain type name, a SQL alias (`int`, `bigint`, `double precision`,
`timestamp with time zone`), an array either way (`text[]` or `_text`), a modifier anywhere after
the name (`numeric(12,2)`, `varchar(64)`, `timestamp(3) with time zone`), and a schema-qualified name
(`public.mood`). A quoted part keeps its case exactly and takes no alias: `"char"` names the
one-byte internal type, unquoted `char` is the `bpchar` alias, and `public."Mood"` names a
mixed-case enum that `public.mood` never matches.

On a source, `pg_type` asserts the server's column really is that type. Reading a column whose
default is not `utf8` as `type = "utf8"` needs `pg_type` naming that exact type; without it the
column is refused once the connection opens and the real type is known.

On a sink, `pg_type` picks the parameter type the value is encoded to. A declared integer forced
into a narrower one (`type = "int64"` into `pg_type = "int2"`) turns on a per-value range check. An
out-of-range value is refused rather than truncated, and the error names the column, the row and
the value. A source never widens this way, and the identical declaration there is a load-time
error.

A modifier in `pg_type` must equal the server column's own modifier on a sink, and a mismatch is
refused once the connection opens, naming both spellings. On a source it asserts the type and not
the modifier, so a `decimal128` read from a wider `numeric` is still protected per value. A value
carrying more fractional digits than the declared `scale` is refused rather than rounded.

A malformed name, or a two-dimensional array such as `int4[][]`, is refused at load time. An enum, a
domain, a composite or an extension type such as `citext` parses at load time and is resolved
against the server's catalog on the first connection.

### Type reference

The canonical table has one row per PostgreSQL type. Each row names that type, the `type` a column
of it maps to when nothing forces it, and any other `type` it also fills over the binary wire when
declared explicitly. Every type not listed as its own `type` name arrives as `utf8` by the rule
above, and `pg_type` naming it explicitly is what makes that a declared choice.

`numeric` is the one type whose default is not what its own binary route accepts. An unconstrained
column carries no modifier to size a `decimal128` from, so it defaults to `utf8`. It takes
`decimal128` only when `precision` and `scale` are declared.

| PostgreSQL type | default `type` | other `type` over the binary wire |
|---|---|---|
| `aclitem` | `utf8` | none |
| `bit` | `utf8` | none |
| `bool` | `boolean` | none |
| `box` | `utf8` | none |
| `bpchar` | `utf8` | none |
| `bytea` | `binary` | none |
| `"char"` | `utf8` | none |
| `cid` | `int64` | none |
| `cidr` | `utf8` | none |
| `circle` | `utf8` | none |
| `citext` | `utf8` | none |
| `date` | `date32` | none |
| `datemultirange` | `utf8` | none |
| `daterange` | `utf8` | none |
| `float4` | `float32` | none |
| `float8` | `float64` | none |
| `gtsvector` | `utf8` | none |
| `hstore` | `utf8` | none |
| `inet` | `utf8` | none |
| `int2` | `int16` | none |
| `int4` | `int32` | none |
| `int4multirange` | `utf8` | none |
| `int4range` | `utf8` | none |
| `int8` | `int64` | none |
| `int8multirange` | `utf8` | none |
| `int8range` | `utf8` | none |
| `interval` | `interval_month_day_nano` | none |
| `json` | `json` | none |
| `jsonb` | `json` | none |
| `jsonpath` | `utf8` | none |
| `line` | `utf8` | none |
| `lseg` | `utf8` | none |
| `ltree` | `utf8` | none |
| `macaddr` | `utf8` | none |
| `macaddr8` | `utf8` | none |
| `money` | `decimal128` | none |
| `name` | `utf8` | none |
| `numeric` | `utf8` | `decimal128` |
| `nummultirange` | `utf8` | none |
| `numrange` | `utf8` | none |
| `oid` | `int64` | none |
| `path` | `utf8` | none |
| `pg_brin_bloom_summary` | `utf8` | none |
| `pg_brin_minmax_multi_summary` | `utf8` | none |
| `pg_dependencies` | `utf8` | none |
| `pg_lsn` | `utf8` | none |
| `pg_mcv_list` | `utf8` | none |
| `pg_ndistinct` | `utf8` | none |
| `pg_node_tree` | `utf8` | none |
| `pg_snapshot` | `utf8` | none |
| `point` | `utf8` | none |
| `polygon` | `utf8` | none |
| `refcursor` | `utf8` | none |
| `regclass` | `utf8` | none |
| `regcollation` | `utf8` | none |
| `regconfig` | `utf8` | none |
| `regdictionary` | `utf8` | none |
| `regnamespace` | `utf8` | none |
| `regoper` | `utf8` | none |
| `regoperator` | `utf8` | none |
| `regproc` | `utf8` | none |
| `regprocedure` | `utf8` | none |
| `regrole` | `utf8` | none |
| `regtype` | `utf8` | none |
| `text` | `utf8` | none |
| `tid` | `utf8` | none |
| `time` | `time64_micros` | none |
| `timestamp` | `timestamp_micros` | none |
| `timestamptz` | `timestamp_micros_utc` | none |
| `timetz` | `utf8` | none |
| `tsmultirange` | `utf8` | none |
| `tsquery` | `utf8` | none |
| `tsrange` | `utf8` | none |
| `tstzmultirange` | `utf8` | none |
| `tstzrange` | `utf8` | none |
| `tsvector` | `utf8` | none |
| `txid_snapshot` | `utf8` | none |
| `unknown` | `utf8` | none |
| `uuid` | `uuid` | none |
| `varbit` | `utf8` | none |
| `varchar` | `utf8` | none |
| `xid` | `int64` | none |
| `xid8` | `utf8` | none |
| `xml` | `utf8` | none |

`list` and its `item` fill the row of the element type instead: `text[]` with `item = "utf8"`
reaches `list` over `utf8`'s own binary route. `item = "utf8"` over any other element type needs
`pg_type` naming the array (`int4[]` or `_int4`) to read it as text on purpose.

## When it refuses to start

Each message is prefixed with the half it came from, `PostgresSource:` or `PostgresSink:`.

| Message | What to change |
|---|---|
| `notify is not supported with mode kind = "cdc_logical"; the slot interface has no notification channel` | drop the `notify` node, or pick another mode |
| `retention = "delete_acked" applies to kind = "cdc_trigger" only, not "polling": deleting rows from a live table would destroy data` | keep `retention "keep"` on a polling source |
| `schema_fields '{name}' uses the reserved '__' prefix, which only kind = "cdc_logical" fills, not "{kind}"` | drop the metadata column, or switch to `cdc_logical` |
| `schema_fields '{other}' is not a reserved metadata column; the '__' prefix is reserved, and the known names are {names}` | use one of `__op`, `__lsn`, `__xid`, `__commit_ts` and `__table` |
| `reserved column '{name}' must be declared type "{expected}", not "{actual}"` | declare `utf8` for `__op` and `__table`, `int64` for `__lsn` and `__xid`, `timestamp_micros_utc` for `__commit_ts` |
| `write_mode "{mode}" requires a non-empty conflict_columns` | add `conflict_columns` to the sink |
| `update_columns '{column}' is also a conflict column; a conflict key cannot be rewritten by its own upsert` | remove that column from `update_columns` |
| `connection.sslmode = "require" needs the 'tls' feature`, naming the build it is missing from | use a build carrying TLS, or set `sslmode "prefer"` |
| `'item' applies to type "list" only, not "{type}"` | drop `item`, or declare `type "list"` |
| `type "list" requires 'item'` | add `item` naming the element type |
| `'item' must be a scalar type, not "list"; Arrow's List is one-dimensional` | name a scalar element type |
| `precision/scale apply to type "decimal128" only, not "{type}"` | drop the two keys, or declare `decimal128` |
| `pg_type = "{name}" is multi-dimensional; Arrow's List is one-dimensional, so only a single '[]' is supported` | name a one-dimensional array |
| `pg_type = "{name}" names an array type, so declare type = "list" with an 'item', not "{type}"` | declare `type "list"` with an `item` |
| `pg_type = "{name}" cannot carry a declared "{type}"; {options}` | pick one of the `type` names the message lists |
| `pg_type = "{name}" keeps {scale} fractional digit(s) but the field declares scale {declared_scale}; a narrower target would round rather than error` | raise `scale` to the column's own |
| `money keeps 2 fractional digits, so scale {scale} would lose them; declare scale = 2 or wider` | declare `scale 2` or wider |

## Next

- [Sources and sinks](@/service/connectors/_index.md), the other seven transports.
- [Workflows and links](@/service/config/workflows.md), the nodes these two sit in.
