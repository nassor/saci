+++
title = "Dead letter queue"
description = "Store the batches a sink refuses, read what is waiting and why, and replay it when the sink comes back."
template = "page.html"
weight = 5
aliases = ["/service/dead-letter-queue/"]
+++
# Dead letter queue

A batch a sink refuses is logged, counted and dropped. Those rows are gone,
and nothing on `/metrics` says which rows they were. A `dlq` block stores each
refused batch in a connector of your choice instead, and replays it when the
sink starts accepting again.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 216" role="img" aria-labelledby="svc-dlq-t svc-dlq-d">
        <title id="svc-dlq-t">A refused batch is stored as one envelope row and offered to the sink again later</title>
        <desc id="svc-dlq-d">
            A processor on the left hands a record batch to a sink, which refuses the
            write. The batch travels instead to a store, where it is held as one row
            carrying the workflow, the sink id, the failure reason and the batch itself as
            Arrow IPC bytes. Later, at the head or the tail of a pass, the store is read
            back and each stored batch is offered to its sink again. A batch the sink
            accepts leaves the store; one it refuses again is written back with its replay
            count raised.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="48" width="120" height="56" rx="8"/>
            <text class="t-lbl t-data" x="12" y="72">processor</text>
            <text class="t-sm" x="12" y="90">one batch</text>
            <path class="arw arw-data" d="M120 76 H156" marker-end="url(#svc-dlq-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="160" y="48" width="120" height="56" rx="8"/>
            <rect class="hd hd-data" x="160" y="48" width="120" height="20" rx="8"/>
            <rect class="hd hd-data" x="160" y="60" width="120" height="8"/>
            <text class="t-lbl t-data" x="172" y="63">sink</text>
            <text class="t-sm" x="172" y="86">write refused</text>
            <path class="arw arw-ctl" d="M280 76 H316" marker-end="url(#svc-dlq-c)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="320" y="34" width="180" height="84" rx="8"/>
            <rect class="hd hd-bnd" x="320" y="34" width="180" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="320" y="46" width="180" height="8"/>
            <text class="t-lbl t-bnd" x="332" y="49">store</text>
            <text class="t-sm" x="332" y="74">workflow, sink, reason</text>
            <text class="t-sm" x="332" y="92">rows, replays</text>
            <text class="t-sm" x="332" y="110">payload: arrow ipc</text>
            <path class="arw arw-ctl" d="M500 76 H536" marker-end="url(#svc-dlq-c)"/>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="540" y="48" width="120" height="56" rx="8"/>
            <text class="t-lbl t-ctl" x="552" y="72">replay</text>
            <text class="t-sm" x="552" y="90">back to the sink</text>
            <path class="ln" d="M0 156 H654"/>
            <text class="t-sm" x="0" y="176">A processor error is not a dead letter: the batch is still upstream's to hand over again.</text>
            <text class="t-sm" x="0" y="200">A letter the sink refuses again is written back, and its replay count rises.</text>
        </g>
        <defs>
            <marker id="svc-dlq-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="svc-dlq-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the store</span>
        <span class="k-control"><i></i> the replay decision</span>
    </div>
</div>

## 1. What it does for you

Every batch a sink's write refuses becomes one letter: the batch itself as
Arrow IPC bytes, plus the workflow, the sink's declared id, its component, the
failure as the sink reported it, the row count, a timestamp and a replay
count. The letter goes into the store before the pass continues.

A replay reads the store back and offers each batch to its sink again. One the
sink accepts is gone from the store; one it refuses again is written back with
a fresh timestamp and its replay count raised by one. Replays run at four
moments: at the workflow's first pass, whenever a self-healed sink reports
itself rebuilt, on a widening schedule while letters wait, and when you ask.

Only a sink write failure is captured. A processor error is not a dead letter:
the batch it failed on came from upstream and is still upstream's, so storing
it would run the processor twice on data it never accepted.

## 2. Turn it on

One block per workflow, in four shapes. The first three are the same thing:

```kdl,name=Defaults
workflow "orders" {
    dlq
}
```

That is a `redb` file at `<node.data_dir>/dlq/orders.redb`, in a table called
`dead_letters`, replayed at the head of each pass. `dlq "redb"` says the same
thing with the store named. Adding a body configures the store:

```kdl,name=A redb store, configured
workflow "orders" {
    dlq "redb" {
        replay "after_sources"
        directory "/var/lib/saci/dlq"
        source { check_integrity #true }
    }
}
```

Every key outside `replay`, `source` and `sink` belongs to the store's
connector and is handed to both halves of it. A key only one half accepts goes
in that half's own block, which wins over a shared value of the same name.

```kdl,name=A Kafka store
workflow "orders" {
    dlq "kafka" {
        brokers "broker:9092"
        topic "saci-dlq-orders"
        source {
            group_id "saci-dlq-orders"
            poll_timeout_ms 20000
        }
    }
}
```

`poll_timeout_ms` is worth raising on the source half. It is the whole budget
the drain has to join its consumer group, take a partition assignment and
fetch, and with `stop_at_end` injected an elapsed window ends the drain: at
the 1000 ms default a cold group join can report an empty store that is not
empty. A Kafka store never reports `known` for that reason, so a drain that
came back empty leaves the `heal` trigger live. A healed sink, a new letter or
a replay request reads the topic again; a workflow where none of the three
happens does not.

```kdl,name=A NATS store, JetStream only
workflow "orders" {
    dlq "nats" {
        connection {
            servers "nats://localhost:4222"
        }
        source {
            poll_timeout_ms 2000
            mode kind="jetstream" {
                stream "SACI_DLQ"
                filter_subjects "saci.dlq.orders"
                durable_name "saci-dlq-orders"
            }
        }
        sink {
            mode kind="jetstream" {
                stream "SACI_DLQ"
                subject "saci.dlq.orders"
            }
        }
    }
}
```

The two `mode` blocks take different keys: a JetStream consumer filters with
`filter_subjects` and a publisher names one `subject`, so each goes in the
half that takes it. `poll_timeout_ms` is a source key as well, and a sink half
given it refuses the workflow at load time with
`` NatsSink config: unknown field `poll_timeout_ms`, expected one of
`connection`, `mode`, `schema_fields` ``. Both halves are built when the
workflow is, so either one's bad key is a startup failure rather than
something a replay discovers later.

`examples/configs/dlq.kdl` is a runnable version of the first shape against a
sink that is not listening.

## 3. Read what is waiting

The dashboard's Dead letters tab is one card per queue and one row per
`(sink, reason)` group. The same thing over HTTP, for one workflow:

```bash,name=Linux/macOS
curl -s localhost:8080/api/workflows/orders/dlq
```

```powershell,name=Windows (PowerShell)
(Invoke-WebRequest localhost:8080/api/workflows/orders/dlq).Content
```

```json,name=Expected output
{
  "workflow": "orders",
  "store": "redb",
  "replay": "before_sources",
  "known": true,
  "letters": 1,
  "rows": 5,
  "groups": [
    {
      "sink": "orders_out",
      "reason": "HttpSink: cannot POST http://127.0.0.1:18099/orders: error sending request",
      "letters": 1,
      "rows": 5,
      "first_failed_at_unix_ms": 1789474953080,
      "last_failed_at_unix_ms": 1789474953080,
      "max_replays": 1
    }
  ],
  "last_replay": {
    "trigger": "schedule",
    "started_at_unix_ms": 1789474951024,
    "duration_ms": 2068,
    "delivered": 0,
    "retained": 1,
    "purged": 0,
    "lost": 0
  },
  "next_auto_replay_unix_ms": 1789474953997,
  "replay_pending": false
}
```

`GET /api/dlq` answers the same shape for every declared queue, in declaration
order. Both routes exist only when at least one workflow declares a block; a
service with none has no `/api/dlq` at all.

`known` is `false` until a replay of this process has read the store through
without error, and stays `false` for a `kafka` store, whose source half ends a
drain on an elapsed poll window as well as on a caught-up partition. While it
is `false` the counts cover what this process recorded itself, not what an
earlier run left behind. `reason` is the sink's own error with any
`RetryExhausted` wrapper removed, so two failures of one cause group together
whatever the attempt count was.

The four counters:

```bash,name=Linux/macOS
curl -s localhost:8080/metrics | grep saci_dlq
```

```powershell,name=Windows (PowerShell)
(Invoke-WebRequest localhost:8080/metrics).Content -split "`n" | Select-String saci_dlq
```

```text,name=Expected output
saci_dlq_letters_recorded_total{otel_scope_name="saci"} 1
saci_dlq_letters_recorded_total{sink="orders_out",otel_scope_name="saci"} 1
saci_dlq_rows_recorded_total{otel_scope_name="saci"} 5
saci_dlq_letters_replayed_total{otel_scope_name="saci"} 1
saci_dlq_letters{workflow="orders",otel_scope_name="saci"} 0
```

`saci_dlq_letters_lost_total` is the one to alert on: a letter the store
itself would not take is gone, because nothing is buffered in memory waiting
for the store to come back.

The log lines ride the `saci::dlq` target, enabled at `warn` whatever
`log_level` says and never sampled, for the same reason the self-healing lines
are:

```text,name=One letter, recorded and returned
WARN saci::dlq: dead letter recorded workflow=orders sink="orders_out" rows=5 reason=HttpSink: cannot POST ...
WARN saci::dlq: dead letter replay finished workflow=orders trigger="schedule" delivered=0 retained=1 purged=0 lost=0
WARN saci::dlq: dead letter replay finished workflow=orders trigger="heal" delivered=1 retained=0 purged=0 lost=0
```

## 4. Replay it

Four things start a replay, and all four run it inside the workflow's own
runner, between passes:

| Trigger | When |
|---|---|
| `startup` | the workflow's first pass, so a store left by an earlier run is drained before new work |
| `heal` | a self-healed sink's first successful write after a rebuild |
| `schedule` | a widening backoff while letters wait: one second, doubling to a minute |
| `manual` | `POST /api/workflows/{id}/dlq/replay` |

`replay "before_sources"`, the default, runs at the head of a pass, before the
first source is drained, so a stored batch reaches its sink ahead of anything
new. `replay "after_sources"` runs after the last node wrote, so new arrivals
go first.

To replay every letter now:

```bash,name=Linux/macOS
curl -s -X POST localhost:8080/api/workflows/orders/dlq/replay
```

```powershell,name=Windows (PowerShell)
Invoke-WebRequest -Method POST localhost:8080/api/workflows/orders/dlq/replay
```

A JSON body narrows it to the letters one sink refused, or the letters
carrying one reason. Everything else is written back untouched, replay count
included:

```bash,name=Linux/macOS
curl -s -X POST localhost:8080/api/workflows/orders/dlq/replay \
  -H 'content-type: application/json' \
  -d '{"sink": "orders_out"}'
```

```powershell,name=Windows (PowerShell)
Invoke-WebRequest -Method POST localhost:8080/api/workflows/orders/dlq/replay `
  -ContentType 'application/json' -Body '{"sink": "orders_out"}'
```

```json,name=Expected output
{"trigger":"manual","started_at_unix_ms":1789475034018,"duration_ms":25,"delivered":1,"retained":0,"purged":0,"lost":0}
```

The answer is `200` once the replay ran and `202` while it is still queued,
which is the normal answer for an idle workflow: the runner serves the request
when it next reaches its replay point. In `run_mode kind="stream"` the head of
the loop is reached only when an item arrives, the same limit a `pause` has,
so a silent stream holds a queued replay until something comes in.

To discard every letter instead, which nothing undoes:

```bash,name=Linux/macOS
curl -s -X POST localhost:8080/api/workflows/orders/dlq/purge
```

```powershell,name=Windows (PowerShell)
Invoke-WebRequest -Method POST localhost:8080/api/workflows/orders/dlq/purge
```

Purge is what a letter no replay can deliver needs. A letter whose sink you
removed from the config is one, written back every replay with one warning
naming the sink; a letter whose payload will not decode against its sink's
current schema is the other, written back with the decode failure as its
`reason`. Neither blocks the letters behind it. A purge discards each window it
reads without decoding it, so a stored row whose envelope columns this queue
cannot read goes the same way; a row the store's own format refuses never
reaches the purge, because the source half fails on it first.

## Which stores

| Store | Sink half | Source half | Notes |
|---|---|---|---|
| `redb` (default) | `RedbSink` | `RedbSource` | one embedded file on local disk, needs `connector-redb`, which is in the default bundle |
| `kafka` | `KafkaSink` | `KafkaSource` | one topic, needs `connector-kafka`; an elapsed poll window ends a drain, so an empty read is not a confirmed empty topic |
| `nats` | `NatsSink` | `NatsSource` | JetStream only, needs `connector-nats`; a drain ends only once the server reports nothing waiting and nothing unacknowledged |

A store's payload is always Arrow IPC bytes. IPC is already the host to
processor wire format, reproduces the batch and its schema metadata exactly,
and needs no encode pass over the values. All three stores hold arbitrary
bytes, so any batch any sink could refuse fits.

`redb` is exclusive: the sink half holds the file against every other handle,
so a replay finishes the sink half first, drains the source half, deletes what
it read, and rebuilds the sink half afterwards.

Every source half consumes a window at the head of the next fetch: redb
deletes the entries whose streams have ended, Kafka commits the offsets it
handed over last and JetStream acks them. A replay that stops before that next
fetch, on shutdown or on a row whose envelope it cannot read, leaves the window
it was reading where it was and writes none of it back, so each letter is held
once and offered again on the next replay. A letter that window had already
delivered is delivered a second time, which is the at-least-once rule all
three sources carry.

The one way a replay itself loses letters is a crash inside it. redb's delete
has to run before the sink half can reopen the file, so a crash between the
delete and the write-back loses the letters that failed again, and the two
brokers lose the same window the same way. The window is one replay wide in
all three, and a replay of a healthy sink retains nothing to lose. A store
that refuses a write counts `saci_dlq_letters_lost_total` instead, whether
that write is a new letter or one going back, and the first write-back it
refuses ends the write-back: the letters after it are counted lost too rather
than each paying the store's retry policy inline in the pass.

Core NATS is refused outright. It is at-most-once with no subscriber queue, so
a letter published with nothing listening is gone, which is the one thing a
dead letter store may not do.

## Every key

### dlq

| Key | Type | Default | What it does |
|---|---|---|---|
| the block's argument | string | `redb` | which store backs the queue: `redb`, `kafka` or `nats` |
| `replay` | string | `before_sources` | where in a pass a replay runs; the other value is `after_sources` |
| `source` | block | empty | keys only the store's source half takes |
| `sink` | block | empty | keys only the store's sink half takes |
| any other key | | | handed to both halves of the store's connector |

The keys a store's own halves accept are that connector's, documented on its
page: [redb](@/service/connectors/redb.md),
[Kafka](@/service/connectors/kafka.md), [NATS](@/service/connectors/nats.md).
A key the receiving factory does not know is refused by name at load time, as
in `RedbSource config: unknown field 'compact'`, which is the signal to move
it from the block into the `sink` half.

Three things the layer fills in, per half, where the block left the key out:

| Store | Key | Value |
|---|---|---|
| `redb` | `directory`, `file`, `table` | `<node.data_dir>/dlq`, `<workflow>.redb`, `dead_letters` |
| `redb` | `consume` on the source half | `#true`, so a delivered letter is deleted at the end of the drain |
| `kafka`, `nats` | `stop_at_end` on the source half | `#true`, so the drain ends once it is caught up |

`schema_fields` is always the layer's: both halves are built with the envelope
schema, and a declared `schema_fields` is replaced.

## When it refuses to start

| Message | What to change |
|---|---|
| `workflow 'orders': dlq store 'mongodb' is not one of redb, kafka, nats` | Name one of the three stores. |
| `` mode "cluster" does not take a `dlq` block (workflow 'w'): a cluster workflow declares no sink node, so there is nothing to dead-letter `` | Delete the block. A cluster node writes through claims and checkpoints, not sinks. |
| `workflow 'orders' dlq "nats": source needs a mode block` | Add `source { mode kind="jetstream" ... }`. The NATS connector needs a mode named. |
| `workflow 'orders' dlq "nats": source mode kind must be "jetstream"; core NATS is at-most-once and drops a letter published with no subscriber` | Use JetStream on both halves, or pick another store. |
| `` no source factory registered for type 'RedbSource' (required by source 'orders/dlq'): that is a built-in connector this binary was built without, so rebuild or reinstall with `--features connector-redb`, or register your own factory under that name `` | Build with the connector the store needs. Both halves are built when the workflow is, so the message names whichever one is missing. |
| `` RedbSource config: unknown field `compact` `` | A sink key sat at the block level or in `source`. Move it into `sink`. The mirror case, a source key the sink half refuses, reads `RedbSink config: unknown field …`. |

An unknown `replay` value is a parse error naming the value, because the key
takes exactly the two documented strings.

## Next

- [Self-healing](@/service/operate/self-healing.md) is what brings a refusing
  sink back, and what fires the `heal` replay.
- [The live dashboard](@/service/operate/dashboard.md) is the Dead letters tab
  and the three beside it.
- [Logs, metrics and traces](@/service/operate/observability.md) is every
  series, including the five above.
