+++
title = "Kafka"
description = "Topics in both directions, with every broker property reachable and topics created on first use."
template = "subpage.html"
weight = 3
aliases = ["/connectors/kafka/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "Required on both halves; one message per row, or per batch"
[[extra.facts]]
label = "Run modes"
value = "Stream, or any run mode with <code>stop_at_end</code> or <code>compacted</code>"
[[extra.facts]]
label = "In the default build"
value = "no: build with <code>--features connector-kafka</code>"
+++
A workflow drains one topic, processes the rows, and produces the result to another topic.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="kf-t kf-d">
        <title id="kf-t">One topic drained into a workflow and produced back to another topic</title>
        <desc id="kf-d">A topic box on the left feeds a source node named orders_in. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named orders_out. The sink produces to the topic box on the right.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">topic</text>
            <text class="t-sm" x="10" y="74">orders-raw</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#kf-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">orders_in</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#kf-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#kf-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">orders_out</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#kf-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">topic</text>
            <text class="t-sm" x="566" y="74">orders-enriched</text>
        </g>
        <defs>
            <marker id="kf-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> topics and nodes</span>
        <span class="k-boundary"><i></i> the processor</span>
    </div>
</div>

## What you need

- A binary built with `--features connector-kafka`; Kafka is one of five connectors that need a
  specific service installed and running, so it sits outside the default build, alongside NATS,
  PostgreSQL, S3 and Turso.
- A reachable broker. One in a container is enough:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>a single-node broker on 127.0.0.1:9092</em></div>

```text
docker run --rm -p 9092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 apache/kafka:3.9.0
```

</div>

- Plaintext reach to it. TLS and SASL are not in this build, so a deployment that needs either turns
  them on in its own build of the binary.
- No topics created up front. Both halves create what they name on first use unless you opt out.
- The processor component the config names, at `pipelines/orders.wasm`, for the `serve` step.
  [Build your own processor](@/service/processors/build/_index.md) is where that comes from;
  `validate --connectors-only` below runs without it.

## 1. Declare the format

A message is bytes, so both halves name a declared `transformer`.

<div class="code">
<div class="code-cap"><span>KDL</span><em>one JSON object per message</em></div>

```kdl
transformer "orders_json" format="ndjson"
```

</div>

[ndjson](@/service/formats/ndjson.md), [csv](@/service/formats/csv.md) and
[avro](@/service/formats/avro.md) put one row in each message.
[parquet](@/service/formats/parquet.md) and [arrow-ipc](@/service/formats/arrow-ipc.md) put a whole
batch in one message, which rules out the keyed features below.

## 2. Read from a topic: the source node

<div class="code">
<div class="code-cap"><span>KDL</span><em>named keys the connector interprets; everything else goes in properties</em></div>

```kdl
source "orders_in" type="KafkaSource" component="Order" transformer="orders_json" {
    config {
        brokers "localhost:9092"
        topic "orders-raw"
        group_id "saci-orders"
        stop_at_end #true

        provision create=#true partitions=3 replication_factor=1

        properties "session.timeout.ms"="45000"

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

`brokers` is the bootstrap list and `topic` takes several topics comma separated. `group_id` is the
consumer group, so two processes in one group split the partitions.

`stop_at_end #true` is what makes this source finite. It reports EOF once every assigned partition
is drained, and then any run mode can drive it. Without it the source is live and only
[stream mode](@/service/config/run-modes.md) accepts it. The flag is read as a real boolean, so
`stop_at_end="true"` in quotes is ignored and the source stays live.

`compacted #true` reads the topic as a keyed snapshot instead: the latest value per key from the log
start to a captured high watermark, tombstones removing keys, then EOF. It needs `key_field`, the
column the raw message key is written into.

## 3. Write to a topic: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>key_field renders one column into the message key</em></div>

```kdl
sink "orders_out" type="KafkaSink" component="EnrichedOrder" transformer="orders_json" {
    config {
        brokers "localhost:9092"
        topic "orders-enriched"
        key_field "id"
        flush_timeout_ms 30000

        provision create=#true partitions=3 replication_factor=1

        schema_fields "id" type="int64" nullable=#false
        schema_fields "status" type="utf8" nullable=#true
        schema_fields "total" type="float64" nullable=#true
    }
}
```

</div>

A sink writes one topic. `key_field` names the column whose rendered value becomes the message key.
`tombstones #true` turns a row whose other columns are all null into a NULL payload, which is how a
delete reaches a compacted topic; it needs `key_field` too.

## 4. Validate and run

`examples/configs/kafka.kdl` declares two workflows: `orders` drains `orders-raw` into
`orders-enriched`, and `customers` mirrors the compacted topic `customers-changelog` into
`customers-mirror`, keeping deletes as tombstones.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>no broker connection is opened by validation</em></div>

```text
saci-service validate --config examples/configs/kafka.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: orders (sources: 1, sinks: 1)
  workflow: customers (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The config sets `run_mode kind="interval" interval_ms=2000`, so each topic is redrained
every two seconds:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>the config reads KAFKA_BROKERS, defaulting to localhost:9092</em></div>

```bash
export KAFKA_BROKERS='localhost:9092'
saci-service serve --config examples/configs/kafka.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same two steps</em></div>

```powershell
$env:KAFKA_BROKERS = "localhost:9092"
saci-service serve --config examples/configs/kafka.kdl
```

</div>

Every two seconds the rows on `orders-raw` appear on `orders-enriched`, keyed by `id`.

## How it delivers

At-least-once. The source commits the previous batch's offsets at the start of the next poll, so a
crash between the two replays that batch. A `compacted` source is one-shot whatever `stop_at_end`
says, because the snapshot always ends.

One write sends every message its batch produced and waits for the broker's delivery report on each
one, so a returned write means the broker acknowledged all of them. `flush_timeout_ms`, 30 s by
default, bounds how long a full producer queue may hold up one send, and is also the budget for the
one flush `finish` runs.

`key_field` and `compacted` both need a format that emits one message per row, so csv, ndjson and
avro qualify and parquet and arrow-ipc are refused.

The broker connection is opened lazily, so `validate` needs no broker and an unreachable one
surfaces on the first drain instead. A topic that never appears reports
`KafkaSource: poll failed: topic(s) {:?} were never visible to the consumer; if provision.create =
false, they may not exist`.

`provision create=#false` makes no admin call at all, so a missing topic surfaces as a broker error
from the consumer or the producer. With `create=#true` a topic that already exists counts as
success, so two processes provisioning the same topic is safe.

## Every key

Source:

| Key | Type | Default | What it does |
|---|---|---|---|
| `brokers` | string | required | the bootstrap server list |
| `topic` | string | required | one topic, or several comma separated |
| `group_id` | string | `"saci"` | the consumer group this source joins |
| `poll_timeout_ms` | integer | `1000` | how long one poll window waits for messages |
| `auto_offset_reset` | string | `"earliest"` | where a new group starts: `earliest`, `latest` or `none` |
| `stop_at_end` | bool | `#false` | report EOF once every assigned partition is drained |
| `compacted` | bool | `#false` | read the topic once as a keyed snapshot; needs `key_field` |
| `key_field` | string | absent | the column the raw message key is written to, in compacted mode |
| `commit_on_drain` | bool | `#true` | commit the previous poll's offsets at the start of the next one; `#false` commits none |
| `batch_size` | integer | `1000` | rows per poll when the runner sends no admission hint |
| `provision` | node | see below | topic creation |
| `properties` | table | empty | any broker property, by its own name |
| `schema_fields` | list of fields | required | the declared column list the messages decode against |

Sink:

| Key | Type | Default | What it does |
|---|---|---|---|
| `brokers` | string | required | the bootstrap server list |
| `topic` | string | required | the one topic this sink writes |
| `key_field` | string | absent | the column rendered into the message key |
| `tombstones` | bool | `#false` | a row whose other columns are all null is produced with a NULL payload; needs `key_field` |
| `flush_timeout_ms` | integer | `30000` | how long a full producer queue may hold up one send, and the budget for the flush `finish` runs |
| `provision` | node | see below | topic creation |
| `properties` | table | empty | any broker property, by its own name |
| `schema_fields` | list of fields | required | the schema the messages are written with |

### provision

| Key | Type | Default | What it does |
|---|---|---|---|
| `create` | bool | `#true` | create the topic on first use |
| `partitions` | integer | `1` | partition count for a topic this node creates |
| `replication_factor` | integer | `1` | replication factor for a topic this node creates |
| `config` | table | empty | broker-side topic config for a topic this node creates |
| `timeout_ms` | integer | `10000` | budget for the admin call |

### properties

| Key | Type | Default | What it does |
|---|---|---|---|
| any broker property | string | empty | applied last, so it overrides this connector's own defaults |

One property is refused: set the brokers with `brokers`, not `properties.bootstrap.servers`.

A misspelled key anywhere in this `config` fails the parse and is named in the error.

## When it refuses to start

| Message | What to change |
|---|---|
| `KafkaSource config: {e}` | fix the key the parse error names; unknown keys are rejected here |
| `KafkaSource config: set the brokers with 'brokers', not properties.bootstrap.servers` | move the value to the `brokers` key |
| `KafkaSource moves bytes and needs a 'transformer' key naming a declared transformer` | add `transformer="<id>"` to the node |
| `source type 'KafkaSource' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | set `stop_at_end #true`, or `compacted #true`, or switch to `run_mode kind="stream"` |
| `KafkaSink config: 'key_field' needs a row-per-message format; '{format}' emits one message per batch` | name a csv, ndjson or avro transformer, or drop `key_field` |
| `KafkaSource config: 'compacted' needs a row-per-message format; '{format}' emits one message per batch, which carries no per-row key` | the same, for the source |
| `KafkaSink: format '{format}' has no message codec` | name a format that encodes discrete messages |

## Next

- [ndjson](@/service/formats/ndjson.md), the format this page declares.
- [Run modes and persistence](@/service/config/run-modes.md), for the stream mode a live consumer needs.
