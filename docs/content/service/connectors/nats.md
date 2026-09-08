+++
title = "NATS"
description = "Core subject pub/sub or JetStream, chosen by one kind key, with every connection, auth, consumer and publish knob named."
template = "subpage.html"
weight = 4
aliases = ["/connectors/nats/"]
[[extra.facts]]
label = "Direction"
value = "Source and sink"
[[extra.facts]]
label = "Format"
value = "Required on both halves; one message per row, or per batch"
[[extra.facts]]
label = "Run modes"
value = "Stream, or any run mode with <code>stop_at_end</code>"
[[extra.facts]]
label = "In the default build"
value = "no: build with <code>--features connector-nats</code>"
+++
A workflow pulls messages off a JetStream stream, processes the rows, and publishes the result to
another stream.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 124" role="img" aria-labelledby="nt-t nt-d">
        <title id="nt-t">A NATS stream drained into a workflow and published back to another stream</title>
        <desc id="nt-d">A subject or stream box on the left feeds a source node named orders_in. The source hands rows to a WebAssembly processor, drawn as a boundary box, which hands them to a sink node named orders_out. The sink publishes to the stream box on the right.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="104" height="6"/>
            <text class="t-lbl" x="10" y="50">stream</text>
            <text class="t-sm" x="10" y="74">or subject</text>
            <path class="arw arw-data" d="M104 62 H135" marker-end="url(#nt-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="139" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="139" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="139" y="48" width="104" height="6"/>
            <text class="t-lbl" x="149" y="50">source</text>
            <text class="t-sm" x="149" y="74">orders_in</text>
            <path class="arw arw-data" d="M243 62 H274" marker-end="url(#nt-a)"/>
            <rect class="blk blk-bnd" x="278" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-bnd" x="278" y="48" width="104" height="6"/>
            <text class="t-lbl" x="288" y="50">wasm</text>
            <text class="t-sm t-bnd" x="288" y="74">a processor</text>
            <path class="arw arw-data" d="M382 62 H413" marker-end="url(#nt-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="417" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="417" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="417" y="48" width="104" height="6"/>
            <text class="t-lbl" x="427" y="50">sink</text>
            <text class="t-sm" x="427" y="74">orders_out</text>
            <path class="arw arw-data" d="M521 62 H552" marker-end="url(#nt-a)"/>
            <rect class="blk blk-data" x="556" y="36" width="104" height="52" rx="8"/>
            <rect class="hd hd-data" x="556" y="36" width="104" height="18" rx="8"/>
            <rect class="hd hd-data" x="556" y="48" width="104" height="6"/>
            <text class="t-lbl" x="566" y="50">stream</text>
            <text class="t-sm" x="566" y="74">orders.enriched</text>
        </g>
        <defs>
            <marker id="nt-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> streams and nodes</span>
        <span class="k-boundary"><i></i> the processor</span>
    </div>
</div>

## What you need

- A binary built with `--features connector-nats`; NATS is one of five connectors that need a
  specific service installed and running, so it sits outside the default build.
- A reachable NATS server with JetStream enabled. One in a container is enough:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>the -js flag is what enables JetStream</em></div>

```text
docker run --rm -p 4222:4222 nats:2.11-alpine -js -sd /tmp/nats
```

</div>

- Nothing for `kind="core"` beyond the server itself: plain subject pub/sub needs no stream.
- No streams created up front. Both halves create the stream they name on first use unless you opt
  out.
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
batch in one message, which rules out the per-row keys below.

## 2. Read from a stream or a subject: the source node

One `mode` node decides which NATS this node speaks. `kind="jetstream"` reads a durable stream
through a pull consumer; `kind="core"` subscribes to a subject with no persistence and no acks.

<div class="code">
<div class="code-cap"><span>KDL</span><em>a JetStream source over a durable pull consumer</em></div>

```kdl
source "orders_in" type="NatsSource" component="Order" transformer="orders_json" {
    config {
        stop_at_end #true

        connection {
            servers "nats://localhost:4222"
        }

        mode kind="jetstream" {
            stream "ORDERS"
            durable_name "saci-orders"
            filter_subjects "orders.new"
        }

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

`connection.servers` takes one or more URLs. `nats://`, `tls://`, `ws://` and `wss://` all parse,
and a bare `host:port` means `nats://host:port` on port 4222. Each entry is checked while the config
is read, so a typo fails before the service starts.

`mode.stream` is the stream to read and `filter_subjects` narrows it; leave it out for every subject
the stream captures. `durable_name` makes the consumer durable, so a restart resumes where it
stopped, and leaving it out gives an ephemeral consumer instead.

`stop_at_end #true` is what makes this source finite, so any run mode can drive it. Without it the
source is live and only [stream mode](@/service/config/run-modes.md) accepts it. The flag is read as
a real boolean, so `stop_at_end="true"` in quotes is ignored and the source stays live.

For `kind="core"`, name a `subject` instead of a stream, and add a `queue_group` to spread one
subject across several instances: every subscriber in one group sees a disjoint share.

## 3. Write to a stream or a subject: the sink node

<div class="code">
<div class="code-cap"><span>KDL</span><em>a JetStream sink that publishes each batch atomically</em></div>

```kdl
sink "orders_out" type="NatsSink" component="EnrichedOrder" transformer="orders_json" {
    config {
        connection {
            servers "nats://localhost:4222"
            name "saci-enriched"
        }

        mode kind="jetstream" {
            stream "ORDERS_ENRICHED"
            subject "orders.enriched"
            message_id_field "id"
            expected_stream #true
            atomic_batch #true
            stream_provision allow_atomic=#true
        }

        schema_fields "id" type="int64" nullable=#false
        schema_fields "status" type="utf8" nullable=#true
        schema_fields "total" type="float64" nullable=#true
    }
}
```

</div>

`mode.stream` is named rather than inferred, because a typo then fails at startup instead of once
per batch. `subject` must be one that stream captures. `expected_stream #true` refuses a publish
that would land in another stream, and `message_id_field` renders a column into the message id the
stream's duplicate window deduplicates on.

`subject_field` sends each row to the subject its own cell names, with `subject` as the fallback for
a null cell. `headers` sets fixed headers and `header_fields` renders a column into a header value.

`atomic_batch #true` publishes a multi-row batch through JetStream's `Nats-Batch-*` protocol, so the
stream stores all of it or none of it. `write_batch` then waits for the batch's commit ack rather
than for one ack per row, and the server's 1000 message cap is checked before anything is published.

The stream must allow atomic publishes. `stream_provision allow_atomic=#true` grants that when this
node creates the stream, and for a stream that already exists it is the operator's to set.

`expected_last_sequence #true` adds `Nats-Expected-Last-Sequence` to every batch, which needs
`atomic_batch` and makes this sink the stream's only writer: a publish that would land after anyone
else's is refused instead of reordering the stream.

## 4. Validate and run

`examples/configs/nats.kdl` declares both halves over JetStream.

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Shell</span><em>no server connection is opened by validation</em></div>

```text
saci-service validate --config examples/configs/nats.kdl --connectors-only
```

</div>

```text
OK: config is structurally valid
OK: every source, sink and transformer built
  workflow: orders (sources: 1, sinks: 1)
OK: all declared types resolved in built-in registry
```

Then run it. The config sets `run_mode kind="interval" interval_ms=2000`, so the stream is redrained
every two seconds:

Linux/macOS:

<div class="code">
<div class="code-cap"><span>Bash</span><em>the config reads NATS_SERVERS, defaulting to nats://localhost:4222</em></div>

```bash
export NATS_SERVERS='nats://localhost:4222'
saci-service serve --config examples/configs/nats.kdl
```

</div>

Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>PowerShell</span><em>the same two steps</em></div>

```powershell
$env:NATS_SERVERS = "nats://localhost:4222"
saci-service serve --config examples/configs/nats.kdl
```

</div>

Both streams are created on the first cycle if they do not exist, and every two seconds the messages
on `ORDERS` reappear on `ORDERS_ENRICHED`.

## How it delivers

JetStream is at-least-once. The source acknowledges the previous batch at the start of the next
poll, so a crash between the two redelivers that batch. The sink waits for every publish ack, so a
returned write means the stream holds the rows. With `atomic_batch #true` it waits for the batch's
commit ack instead, which is the stronger claim: the stream holds all of the batch or none of it.
Either way a batch acknowledged and then lost, because the ack never arrived, is written twice, which
is what `message_id_field` and the stream's duplicate window are for.

Acks are per message, and the source sends a window's acks only once that whole window has decoded
and been handed over. One message the format cannot decode therefore leaves every ack in its window
unsent, and the server redelivers the whole window after `ack_wait`.

`max_decode_attempts`, five by default, bounds that one message rather than the window. It counts
the server's own delivery attempts for the message the decoder blamed, and on the last permitted
attempt the source terminates that message and reports the stream, the consumer and the sequence it
retired. Every other message in the window stays unacknowledged and comes back on the next window.
`max_decode_attempts=0` removes the bound, and an undecodable message then redelivers for as long
as it stays undecodable.

Core NATS is at-most-once and has no ack at all, so a message consumed while the pipeline later
fails is gone. It also drops a message with no subscriber, so a source that has not polled yet has
not subscribed yet. A core sink's `flush_every_batch` waits for that batch's bytes to leave the
client's own write buffer, which is as far as core NATS goes: there is no ack to wait for.

`subject_field`, `header_fields` and `message_id_field` each need a format that emits one message
per row, so csv, ndjson and avro qualify and parquet and arrow-ipc are refused.

The connection is opened lazily, so `validate` needs no server. Provisioning and the consumer are
created on the first poll or the first write. An existing stream is used as it stands and never
reconfigured, so `create=#true` against a stream someone else owns changes nothing. With
`create=#false` a misspelled `stream` is an error rather than a new empty stream nobody publishes
to.

## Every key

Source, at the top of `config`:

| Key | Type | Default | What it does |
|---|---|---|---|
| `connection` | node | required | how to reach the server |
| `mode` | node | required | `kind="core"` or `kind="jetstream"`, plus that mode's keys |
| `poll_timeout_ms` | integer | `1000` | how long one poll window waits for messages |
| `stop_at_end` | bool | `#false` | report end of stream once the server confirms nothing is pending |
| `batch_size` | integer | `1000` | rows per poll when the runner sends no admission hint |
| `schema_fields` | list of fields | required | the declared column list the messages decode against |

Sink, at the top of `config`:

| Key | Type | Default | What it does |
|---|---|---|---|
| `connection` | node | required | how to reach the server |
| `mode` | node | required | `kind="core"` or `kind="jetstream"`, plus that mode's keys |
| `schema_fields` | list of fields | required | the schema the messages are written with |

A misspelled key anywhere in this `config` fails the parse and is named in the error.

### connection

| Key | Type | Default | What it does |
|---|---|---|---|
| `servers` | string | required | one or more server URLs |
| `name` | string | absent | the client name the server reports |
| `connect_timeout_ms` | integer | `5000` | budget for one connect attempt |
| `request_timeout_ms` | integer | `10000` | budget for a request; `0` waits forever |
| `ping_interval_ms` | integer | `60000` | keepalive interval |
| `max_reconnects` | integer | `0` | reconnect attempts; `0` is unlimited |
| `reconnect_delay_ms` | integer | `0` | fixed delay between attempts; `0` keeps the client's own backoff |
| `retry_on_initial_connect` | bool | `#true` | keep retrying the very first connect |
| `subscription_capacity` | integer | `65536` | messages buffered per subscription |
| `client_capacity` | integer | `2048` | client-side command buffer |
| `read_buffer_capacity` | integer | `65535` | socket read buffer |
| `no_echo` | bool | `#false` | do not deliver this client's own messages back to it |
| `inbox_prefix` | string | absent, so `_INBOX` | prefix for reply subjects |
| `ignore_discovered_servers` | bool | `#false` | use only the declared servers |
| `retain_servers_order` | bool | `#false` | connect in the declared order instead of at random |
| `auth` | node | `kind="none"` | the auth scheme |
| `tls` | node | nothing required | encryption and client certificates |

### connection.auth

`kind` picks the scheme, and every secret has a `_file` twin for a mounted secret. Exactly one of
each pair is set.

| `kind` | Keys |
|---|---|
| `none` | nothing, and what an absent table means |
| `token` | `token` or `token_file` |
| `user_password` | `user`, plus `password` or `password_file` |
| `nkey` | `seed` or `seed_file` |
| `credentials` | `path` to a `.creds` file holding a JWT and its seed |

### connection.tls

| Key | Type | Default | What it does |
|---|---|---|---|
| `require` | bool | `#false` | demand encryption even on a `nats://` URL |
| `tls_first` | bool | `#false` | handshake before the server's info exchange; implies `require` |
| `root_certificates` | string | absent, so the OS trust store | a PEM bundle to verify the server with |
| `client_certificate` | string | absent | the client certificate, for mutual TLS |
| `client_key` | string | absent | its key; both keys or neither |

A `tls://` URL requires encryption on its own. `tls_first` needs the matching setting on the server.

### mode kind="core"

| Key | Type | Default | What it does |
|---|---|---|---|
| `subject` | string | required | the subject, or several comma separated |
| `queue_group` | string | absent | source only: share one subject across instances |
| `subject_field` | string | absent | sink only: the column whose cell is the subject |
| `headers` | table | empty | sink only: fixed headers on every message |
| `header_fields` | table | empty | sink only: header name to column name |
| `reply_subject` | string | absent | sink only: the reply subject set on each message |
| `flush_timeout_ms` | integer | `30000` | sink only: budget for a flush |
| `flush_every_batch` | bool | `#true` | sink only: wait for each batch's bytes to reach the socket |

### mode kind="jetstream", source

| Key | Type | Default | What it does |
|---|---|---|---|
| `stream` | string | required | the stream to read |
| `durable_name` | string | absent, so an ephemeral consumer | the durable consumer name |
| `consumer_name` | string | absent, so `durable_name` | the consumer name reported to the server |
| `description` | string | absent | the consumer description |
| `filter_subjects` | string | empty | which of the stream's subjects to read; empty means all |
| `deliver_policy` | node | `kind="all"` | where delivery starts |
| `ack_policy` | string | `"explicit"` | `explicit`, `all` or `none` |
| `double_ack` | bool | `#false` | wait for the server to confirm each ack |
| `max_decode_attempts` | integer | `5` | delivery attempts before a message that will not decode is retired; `0` never retires one |
| `max_batch` | integer | `0` | cap on one pull; `0` keeps the server default |
| `max_deliver` | integer | `0` | server-side redelivery cap; `0` is unlimited |
| `ack_wait_ms`, `max_ack_pending`, `max_waiting`, `max_bytes`, `max_expires_ms`, `inactive_threshold_ms`, `num_replicas`, `rate_limit_bps`, `sample_frequency` | integer | `0` | consumer limits; `0` keeps the server default |
| `memory_storage`, `headers_only` | bool | `#false` | consumer storage, and dropping payloads |
| `replay_policy` | string | `"instant"` | `instant` or `original` |
| `backoff_ms` | list | empty | redelivery backoff ladder |
| `metadata` | table | empty | consumer metadata |
| `fetch_expires_ms` | integer | `5000` | how long one pull waits at the server |
| `fetch_max_bytes` | integer | `0` | byte cap on one pull; `0` leaves the row count alone to bound it |
| `heartbeat_ms` | integer | `0` | idle heartbeat; `0` keeps the client default |
| `domain` | string | absent | the JetStream domain |
| `api_prefix` | string | absent, so `$JS.API` | the API subject prefix; `domain` says the same thing |
| `api_timeout_ms` | integer | `5000` | budget for an API call |
| `stream_provision` | node | see below | stream creation |

`deliver_policy.kind` is one of `all`, `last`, `new`, `last_per_subject`, `by_start_sequence` with
`start_sequence`, or `by_start_time` with an RFC 3339 `start_time`.

### mode kind="jetstream", sink

| Key | Type | Default | What it does |
|---|---|---|---|
| `stream` | string | required | the stream published to |
| `subject` | string | required | the subject, which must be one that stream captures |
| `subject_field` | string | absent | the column whose cell replaces `subject` |
| `headers`, `header_fields` | table | empty | fixed headers, and header name to column name |
| `message_id_field` | string | absent | the column rendered into the message id the duplicate window uses |
| `expected_stream` | bool | `#false` | refuse a publish that would land in another stream |
| `atomic_batch` | bool | `#false` | publish a multi-message batch as one atomic batch, stored all-or-nothing |
| `expected_last_sequence` | bool | `#false` | send `Nats-Expected-Last-Sequence` on each batch; needs `atomic_batch`, and makes this sink the stream's only writer |
| `api_timeout_ms` | integer | `5000` | budget for an API call, and for one awaited publish ack |
| `ack_timeout_ms` | integer | `30000` | budget for an ack orphaned by a mid-batch error |
| `max_ack_inflight` | integer | `5000` | publishes allowed in flight |
| `backpressure_on_inflight` | bool | `#true` | pause writing once that many are in flight |
| `domain`, `api_prefix` | string | absent | the JetStream domain, or the API subject prefix |
| `stream_provision` | node | see below | stream creation |

### stream_provision

| Key | Type | Default | What it does |
|---|---|---|---|
| `create` | bool | `#true` | create the stream on first use |
| `subjects` | string | empty | the stream's subject list; empty derives it from this half's own subjects |
| `retention` | string | `"limits"` | `limits`, `interest` or `work_queue` |
| `storage` | string | `"file"` | `file` or `memory` |
| `discard` | string | `"old"` | `old` or `new`, once a limit is hit |
| `compression` | string | `"none"` | `none` or `s2` |
| `num_replicas` | integer | `1` | within 1 to 5 |
| `max_messages`, `max_messages_per_subject`, `max_bytes`, `max_message_size`, `max_consumers` | integer | `-1` | limits; `-1` is unlimited |
| `max_age_ms` | integer | `0` | age limit; `0` is none |
| `duplicate_window_ms` | integer | `0` | deduplication window; `0` keeps the server default |
| `allow_rollup`, `deny_delete`, `deny_purge`, `allow_direct`, `allow_atomic` | bool | `#false` | stream permissions; `allow_atomic` is what `mode.atomic_batch` needs |
| `description` | string | absent | the stream description |
| `metadata` | table | empty | stream metadata |

## When it refuses to start

| Message | What to change |
|---|---|
| `NatsSource config: {e}` | fix the key the parse error names; unknown keys are rejected here |
| `NatsSource moves bytes and needs a 'transformer' key naming a declared transformer` | add `transformer="<id>"` to the node |
| `source type 'NatsSource' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | set `stop_at_end #true`, or switch to `run_mode kind="stream"` |
| `NatsSource: connection.servers entry '{s}' is not a NATS URL: {e}` | fix the URL, or write a bare `host:port` |
| `NatsSource: connection.auth kind = "token" needs exactly one of 'token' or 'token_file'` | set exactly one of the pair |
| `NatsSource: connection.tls needs both 'client_certificate' and 'client_key', or neither` | add the missing half, or drop both |
| `NatsSource config: 'mode.domain' and 'mode.api_prefix' are two ways to say the same thing; set one` | keep one of the two |
| `NatsSink config: 'mode.subject_field' needs a row-per-message format; '{format}' emits one message per batch` | name a csv, ndjson or avro transformer, or drop the key |
| `NatsSink: format '{format}' has no message codec` | name a format that encodes discrete messages |
| `NatsSink config: 'mode.expected_last_sequence' needs 'mode.atomic_batch #true'` | add `atomic_batch #true`, since the guard is a batch-level claim |
| `NatsSink config: 'mode.atomic_batch' needs 'stream_provision.allow_atomic #true' when the stream is provisioned` | add `allow_atomic #true` to `stream_provision`, or set `create #false` and allow atomic publishes on the pre-existing stream |

`double_ack` with `ack_policy="none"` is refused too, because an ack the server confirms needs an
ack policy that sends one.

One error waits for the first poll rather than the config check:
`NatsSource: cannot resolve stream '{name}': {e}; set mode.stream_provision.create = true to have
SACI create it` means the stream does not exist and this node was told not to create it.

## Next

- [ndjson](@/service/formats/ndjson.md), the format this page declares.
- [Run modes and persistence](@/service/config/run-modes.md), for the stream mode a live consumer needs.
