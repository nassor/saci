+++
title = "redb"
description = "An embedded key/value file as a transport. One entry per batch, in the byte format a declared transformer names."
template = "subpage.html"
weight = 11
aliases = ["/connectors/redb/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "Required on both halves"
[[extra.facts]]
label = "Run modes"
value = "Any: the source reaches EOF after the last entry"
[[extra.facts]]
label = "In the default build"
value = "yes"
+++
[redb](https://github.com/cberner/redb) is an embedded key/value store: one file, no server. This
connector uses it as a transport. The sink stores each batch as one entry; the source reads those
entries back in key order.

## What one entry is

One entry per non-empty batch. The value is a self-contained document in whatever format the node's
`transformer` names: a csv with its header row, a block of ndjson lines, one whole parquet or avro
container. A batch with no rows writes nothing.

The key is `{key_prefix}{seq:020}{key_suffix}`, where `seq` counts batches from zero and is padded
to twenty digits, the width of `u64::MAX`. Lexicographic key order is therefore the order the
batches arrived in, and the source needs no sort.

A reopened sink reads the highest key already carrying its `key_prefix` and `key_suffix`, and
continues the sequence from there. A rerun adds entries after the ones already in the table instead
of replacing them. A key that already exists is refused rather than overwritten.

The source reads every entry whose key starts with `key_prefix` and ends with `key_suffix`. It does
not parse the middle, so an entry another writer put in the same table under a matching name reaches
the transformer and fails there by name.

## What you need

- No external service. The file is local disk.
- A directory for the file. The sink creates it; the source does not.
- The processor component, when the config declares one. The example below declares none.

## 1. Declare the format

A redb entry carries bytes, so the node names a declared `transformer`.

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
[arrow-ipc](@/service/formats/arrow-ipc.md).

## 2. Store rows: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>directory is the only key the transport itself needs</em></div>

```kdl
sink "orders_out" type="RedbSink" component="Order" transformer="orders_csv" {
    config {
        directory "/data/orders"
        file "orders.redb"
        key_prefix "orders/"
        key_suffix ".csv"

        schema_fields "id" type="Int64" nullable=#false
        schema_fields "amount" type="Float64"
        schema_fields "status" type="Utf8"
    }
}
```

</div>

`schema_fields` is required: it is the schema the rows are written with. The file and the table are
created on the first write.

## 3. Read rows back: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>the same four keys the sink used, so the keys line up</em></div>

```kdl
source "orders_in" type="RedbSource" component="Order" transformer="orders_csv" {
    config {
        directory "/data/orders"
        file "orders.redb"
        key_prefix "orders/"
        key_suffix ".csv"

        schema_fields "id" type="Int64" nullable=#false
        schema_fields "amount" type="Float64"
        schema_fields "status" type="Utf8"
    }
}
```

</div>

`schema_fields` is required here too. The service reads `Source::schema()` at load time, before the
file is opened, so the schema cannot come from the table. The source hands that schema to the format
as a projection target, so a self-describing format is cast back to the declared column types.

## 4. Validate and run

`examples/configs/redb_connector.kdl` reads `examples/configs/fixtures/orders.csv` and stores it in
`/tmp/saci-redb-connector/orders.redb`.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>the source, the sink and the transformer, all built for real</em></div>

```text
saci-service validate --config examples/configs/redb_connector.kdl --connectors-only
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
<div class="code-cap"><span>Shell</span><em>writes /tmp/saci-redb-connector/orders.redb</em></div>

```text
saci-service serve --config examples/configs/redb_connector.kdl
```

</div>

The log carries one `RedbSink: entry committed` line per batch, naming the key and the row count,
then `RedbSink: compacted`.

## One writer, many readers

redb locks the file for a database handle's lifetime. The sink opens it for writing and takes an
exclusive operating-system lock, from the moment it is built until `finish`, which is when it
drops the handle. The source opens it read-only and takes a shared lock, on its first batch, and
drops it at EOF.

Several sources may therefore read one file at once, and a source needs no write permission on it.
A sink excludes every other handle: while one holds the file, a second sink or any source fails
with an error naming the path, so reading back what a run wrote needs a second run.

`check_integrity` is the one exception on the source side. The check may repair, which is a write,
so the source opens the file read-write for the check's duration before it takes its shared lock.

## A file no sink closed

A read-only open never repairs, so a source refuses a file whose last write left no allocator
state table and names the path:

`RedbSource: <path> was not shut down cleanly and a read-only open cannot repair it`.

Reaching that state takes a sink running with both `quick_repair #false` and
`two_phase_commit #false` whose process died before `finish`. Either knob writes that table on
every commit, `quick_repair` forces `two_phase_commit` on, and both default on, so a
default-configured sink leaves a readable file even when it is killed. A sink that exits normally
always does.

Two things recover such a file, both by opening it read-write: `check_integrity #true` on the
source, or one run of a `RedbSink` over it.

## Durability and compaction

The sink commits each batch in its own transaction, with redb's write-safety knobs on by default:

- `durability "immediate"`: the commit has reached the disk when it returns. `durability "none"`
  leaves it in the page cache, and `finish` runs one empty immediate commit to make every earlier
  one durable before the file closes.
- `two_phase_commit`: one extra fsync per commit, and the file is in a committed state at every
  instant.
- `quick_repair`: commits carry page checksums, so recovery after a crash is instant instead of a
  full-file walk. It forces `two_phase_commit` on, whatever that key says.

`compact` reclaims the file's free space at `finish`. `check_integrity` walks the whole file at
open and is off by default: the check costs a full pass every time, and with the write-safety
defaults on there is nothing to repair. It earns its keep on a source pointed at a file the
section above describes, which is the one case a read-only open cannot handle alone.

## The file as a queue

`consume #true` on the source deletes the entries that instance handed over,
once, at the end of the run. What one run read is gone, so a second run reads
whatever was written since. A source dropped without finishing deletes
nothing, so its entries are delivered again rather than lost.

The delete needs a read-write open, which is exclusive, so it takes the file
for the duration of one transaction after the read handle is released. This is
the mode the [dead letter queue](@/service/operate/dead-letter-queue.md) runs
its store's source half in.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `directory` | string | required | the directory the file lives in |
| `file` | string | `saci.redb` | the file name inside it |
| `table` | string | `records` | the table name inside the file |
| `key_prefix` | string | `""` | only entries whose key starts with it are read |
| `key_suffix` | string | `""` | only entries whose key ends with it are read |
| `check_integrity` | bool | `#false` | walk the whole file at open and refuse a corrupted one |
| `cache_size_bytes` | integer | redb's own default | page cache budget |
| `consume` | bool | `#false` | delete the entries this instance yielded when the run finishes |
| `schema_fields` | list of fields | required | the declared column list handed to the format |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `directory` | string | required | the directory the file lives in, created if absent |
| `file` | string | `saci.redb` | the file name inside it |
| `table` | string | `records` | the table name inside the file |
| `key_prefix` | string | `""` | prepended to every generated key |
| `key_suffix` | string | `""` | appended to every generated key |
| `check_integrity` | bool | `#false` | walk the whole file at open and refuse a corrupted one |
| `cache_size_bytes` | integer | redb's own default | page cache budget |
| `compact` | bool | `#true` | reclaim the file's free space at `finish` |
| `durability` | `immediate` or `none` | `immediate` | how hard each commit tries before it reports success |
| `two_phase_commit` | bool | `#true` | one extra fsync per commit |
| `quick_repair` | bool | `#true` | page checksums, for instant crash recovery |
| `schema_fields` | list of fields | required | the schema the rows are written with |

An unrecognised key inside `config` is rejected by name on both halves.

## When it refuses to start

| Message | What to change |
|---|---|
| ``RedbSource config: missing field `directory` `` | add `directory "..."` to the node's `config` |
| `RedbSink: 'file' must not be empty` | give `file` a name, or drop the key and take `saci.redb` |
| `RedbSink: 'table' must not be empty` | give `table` a name, or drop the key and take `records` |
| `RedbSink: 'cache_size_bytes' must be greater than zero` | raise the budget, or drop the key |
| `RedbSource moves bytes and needs a 'transformer' key naming a declared transformer` | add `transformer="<id>"` to the node, naming a declared `transformer` |
| `RedbSink: cannot create directory {dir}: {e}` | fix the permissions on the parent path |
| `RedbSink: cannot open {path}: {e}` | another handle holds the file, or the path is not writable |
| `RedbSource: cannot open {path}: {e}` | the file is absent, or a sink in this process still holds it |
| `RedbSource: {path} was not shut down cleanly and a read-only open cannot repair it` | set `check_integrity #true` on the source, or run a `RedbSink` over the file once |
| `RedbSink: key '{key}' already exists in table '{table}' of {path}` | another writer took that key; give this sink its own `key_prefix` or `table` |

## Next

- [csv](@/service/formats/csv.md), the format this page declares.
- [Sources and sinks](@/service/connectors/_index.md), the other nine transports.
