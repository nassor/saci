+++
title = "Flow control"
description = "How many rows each source admits per pass, the keys that bound it, and how to pin it or turn it off."
template = "page.html"
weight = 3
aliases = ["/service/flow-control/"]
+++
# Flow control

Flow control decides how many rows each source admits into a pass. It is on
with no configuration. Bounding it, pinning it, or switching it off takes one
`flow_control` block, and the dashboard reports each source's current target.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 216" role="img" aria-labelledby="svc-fc-t svc-fc-d">
        <title id="svc-fc-t">An admission target sizes the chunk each source hands the workflow, and errors or backlog shrink it</title>
        <desc id="svc-fc-d">
            A source on the left produces rows. A control-plane box holds that source's
            admission target in rows. The target decides the size of the chunk handed to
            the workflow, drawn as a data-plane box. Two arrows come back to the target
            from the workflow: a growing sink backlog and a failed pass. Either one
            divides the target immediately, which is why the number a source admits
            follows what the rest of the workflow can keep up with.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="52" width="120" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="120" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="64" width="120" height="8"/>
            <text class="t-lbl t-data" x="12" y="67">source</text>
            <text class="t-sm" x="12" y="90">orders_in</text>
            <path class="arw arw-data" d="M120 80 H156" marker-end="url(#svc-fc-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="160" y="52" width="170" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="160" y="52" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="160" y="64" width="170" height="8"/>
            <text class="t-lbl t-ctl" x="172" y="67">admission target</text>
            <text class="t-sm" x="172" y="90">4096 rows this pass</text>
            <path class="arw arw-data" d="M330 80 H366" marker-end="url(#svc-fc-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="370" y="52" width="120" height="56" rx="8"/>
            <text class="t-lbl" x="382" y="76">chunk</text>
            <text class="t-sm" x="382" y="94">one pass worth</text>
            <path class="arw arw-data" d="M490 80 H526" marker-end="url(#svc-fc-a)"/>
            <rect class="blk blk-data" x="530" y="52" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="530" y="52" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="530" y="64" width="130" height="8"/>
            <text class="t-lbl t-data" x="542" y="67">workflow</text>
            <text class="t-sm" x="542" y="90">processors, sinks</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-ctl" d="M595 108 V156 H249 V112" marker-end="url(#svc-fc-c)"/>
            <text class="t-sm t-ctl" x="300" y="150">sink backlog growing, or a failed pass</text>
            <text class="t-sm" x="300" y="176">divides the target at once</text>
            <path class="ln" d="M0 192 H654"/>
            <text class="t-sm" x="0" y="210">Every default is a working policy: a config that declares nothing behaves like one that spells it out.</text>
        </g>
        <defs>
            <marker id="svc-fc-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="svc-fc-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-control"><i></i> the admission decision</span>
    </div>
</div>

## 1. What it does for you

Every source gets its own admission target, in rows, and the runner admits at
most that many per pass, holding the remainder for the next one. The target
moves on evidence, growing while a bigger chunk moves more rows per second. It
is divided at once when a pass fails, when a sink's backlog grows on two
consecutive passes, or when the next chunk would exceed the memory bound. An
omitted `flow_control` block gets the whole policy at its built-in defaults.

The search rests once it stops moving the size. After `settle_after_epochs`
measurements leave the size alone, the runner admits at that size only, and
each further settled measurement doubles the run of measurements it skips, up
to sixteen. A guard trip or a missed `target_latency_ms` starts measuring
again, as does any measurement that moves the size.

## 2. The keys

Declared once at the top level, and optionally overridden per source:

```kdl,name=The flow_control block
flow_control {
    enabled #true
    min_rows 1024
    max_rows 65536
    start_rows 4096
    max_chunk_bytes 8388608
    target_latency_ms 250
    adjust_interval_ms 60000
    growth_factor 2.0
    improve_threshold 0.05
    min_samples_per_arm 4
    settle_after_epochs 3
    backoff_factor 2.0
    backoff_cooldown 4
}

workflow "w" {
    source "orders" type="NatsSource" component="Order" {
        flow_control { rows 4096 }
    }
}
```

| Key | Unit | Default | Trade-off |
|---|---|---|---|
| `enabled` | flag | `#true` | `#false` drains each source to EOF per pass and paces unconditionally |
| `min_rows` | rows | 1024 | floor; raising it protects throughput on a pipeline with heavy per-pass overhead |
| `max_rows` | rows | 65536 | ceiling; raising it trades memory and per-pass latency for throughput |
| `start_rows` | rows | 4096 | where a run begins before anything has been measured |
| `max_chunk_bytes` | bytes | 8 MiB | memory bound on one chunk; `0` unbounded; no effect on a path to a windowed node, where nothing is chunked |
| `target_latency_ms` | ms | 250 in stream mode, 0 in `continuous` and `interval` | a candidate breaching it loses; `0` pursues throughput alone |
| `adjust_interval_ms` | ms | 60000 | how long one measurement runs; longer measures more cleanly, shorter reacts sooner |
| `growth_factor` | ratio | 2.0 | candidate step size; must exceed 1.0 |
| `improve_threshold` | ratio | 0.05 | gain a candidate needs to win; lower chases noise, higher ignores real gains |
| `min_samples_per_arm` | passes | 4 | evidence each size needs; higher is stricter |
| `settle_after_epochs` | epochs | 3 | measurements that leave the size alone before the search rests on it; `0` never rests |
| `backoff_factor` | ratio | 2.0 | divisor on a guard trip; must exceed 1.0 |
| `backoff_cooldown` | passes | 4 | passes held after a back-off; `0` resumes at once |
| `rows` | rows | absent | pins the size and stops it adapting |

Every key is optional and every default is a working policy.
`target_latency_ms` is the one default that depends on the run mode. Stream
mode carries a 250 ms per-item objective, the latency contract a stream
workflow has to meet. `continuous` and `interval` default to `0`, because a
batch pass has no per-item latency contract. An explicit value, including `0`,
is honoured in either mode.

The row defaults bracket the flat part of the per-row cost curve. Per-row cost
falls steeply below a few thousand rows and is flat from roughly four thousand
to sixteen thousand. `start_rows` begins inside that band, and `min_rows` sits
two halvings below it, so a back-off has room to move.

## 3. Override one source

A source's own `flow_control` block layers field by field over the top-level
one. A source that declares only `rows` still inherits every other key, and
one that declares nothing gets the top-level policy verbatim. Omitting the
block is the only way to say "no override".

```kdl,name=One slow sink, one pinned source
flow_control {
    max_rows 16384
}

workflow "orders" {
    source "bulk_in" type="FileSource" component="Order" transformer="orders_csv" {
        config path="/data/in/orders.csv"
    }

    source "api_in" type="HttpSource" component="Order" transformer="orders_csv" {
        // Inherits max_rows 16384, and pins its own admission.
        flow_control { rows 2048 }
        config url="https://api.example.com/orders.csv"
    }
}
```

A top-level `rows` is inherited by every source that declares no `rows` of its
own, so pin globally only when you mean to pin every source.

## 4. Pin it or turn it off

Two settings take the measurement out of the decision:

- `flow_control { rows N }` pins the target at `N`, so no measurement, guard or
  candidate ever moves it.
- `flow_control { enabled #false }` drops the credit entirely, so each source
  drains to EOF per pass and the connector's own configured batch size governs.

Pin a source that feeds a windowed node. A window's results depend on how many
whole arrivals share a pass, and with the credit live that count follows a
throughput measurement. Declare one of the two settings above on every source
feeding the node, and set `allowed_lateness_ms` on the window to cover
event-time disorder: [Windowing](@/service/processors/windowing/_index.md).

How much of an arrival a source hands over also depends on the connector.
Kafka, NATS and PostgreSQL sources size their own fetch, so they take at most
the current target and may take less. Every other source hands over whatever
it produced and the runner slices it at the target instead.

```kdl,name=Pin every source that feeds a windowed processor
workflow "sessions" {
    source "events" type="NatsSource" component="Event" transformer="events_json" {
        flow_control { rows 4096 }
        config { /* ... */ }
    }

    wasm "sessionize" module="pipelines/sessions.wasm" {
        window kind="session" gap_ms=5000 time_field="event_ts" allowed_lateness_ms=2000
    }
}
```

## 5. Read its decisions

Three series report where a source's admission stands, all under
`source="<id>"`:

| Series | What it says |
|---|---|
| `saci_flow_target_rows` | the target in effect after the last pass |
| `saci_flow_throughput_rows_per_second` | that source's smoothed rate |
| `saci_flow_backoff_total` | how many times a guard has divided or held the target |

Every move is also recorded as a decision, one per episode rather than one per
pass, in four kinds:

| Kind | Means |
|---|---|
| `grew` | a measurement closed on a larger winner |
| `shrank` | it closed on a smaller one |
| `backed_off` | a guard divided the target |
| `held_at_floor` | a guard tripped with the target already at `min_rows` |

Each record carries when it landed, the workflow and source ids, and the rows
either side of the move. It also names the number that decided, such as
`experiment won: 12.4k rows/s against 9.1k rows/s` or
`sink backlog growing: 8192 rows pending`.
[The live dashboard](@/service/operate/dashboard.md) draws them as markers on
the throughput chart, on the same time axis as the three series.

Four log lines say the same thing on stdout, on the target
`saci::flow_control`, at INFO. That target is enabled whatever `log_level`
says and no sampling ratio drops it, so admission control is readable at the
default error-only level and a `RUST_LOG` cannot silence it.

| Line | When | Fields |
|---|---|---|
| `flow control starting` | one per governed source, as the runner builds its controller | `workflow`, `source`, `enabled`, `fixed_rows`, `start_rows`, `min_rows`, `max_rows`, `max_chunk_bytes`, `target_latency_ms`, `adjust_interval_ms` |
| `flow control decision` | one per episode, the same episodes the dashboard draws | `workflow`, `source`, `kind`, `from_rows`, `to_rows`, `reason` |
| `flow control finished` | one per governed source, as the runner exits | `workflow`, `source`, `final_rows`, `adjustments`, `backoffs` |
| `no flow control: source feeds a windowed node …` | one per ungoverned source in `stream` mode, at startup; this is what explains a missing `starting` line | `workflow`, `source` |

`adjustments` counts the episodes that opened; `backoffs` counts every guard
trip, deduplicated or not, so it matches `saci_flow_backoff_total`.

Three cases carry no controller and so publish none of the three series: every
source under `one_shot`, every source in `mode "cluster"`, and a source on a
path to a windowed node under `stream`. A source with
`flow_control { enabled #false }` publishes none either.

Linux/macOS:

```bash,name=Read the current target of every source
curl -s http://localhost:8080/metrics | grep '^saci_flow_target_rows'
# saci_flow_target_rows{source="orders_in",otel_scope_name="saci"} 8192
```

Windows (PowerShell):

```powershell
curl.exe -s http://localhost:8080/metrics | Select-String '^saci_flow_target_rows'
```

The arithmetic behind these numbers, the measurement it runs and the ceiling it
remembers, is [How flow control decides](@/library/flow-control.md).

## Every key

### flow_control

| Key | Type | Default | What it does |
|---|---|---|---|
| `enabled` | boolean | `#true` | `#false` drains each source to EOF per pass and sends no fetch hint |
| `min_rows` | integer | 1024 | floor on the admission target; at least 1 |
| `max_rows` | integer | 65536 | ceiling on the admission target; at least `min_rows` |
| `start_rows` | integer | 4096 | the target before anything is measured; must fall inside `[min_rows, max_rows]` |
| `max_chunk_bytes` | integer | 8388608 | memory bound on one chunk in bytes; `0` is unbounded |
| `target_latency_ms` | integer | 250 in `stream`, 0 otherwise | per-pass latency objective; `0` drops it |
| `adjust_interval_ms` | integer | 60000 | how long one measurement runs, in milliseconds; at least 1 while adaptive |
| `growth_factor` | number | 2.0 | step size between candidate sizes; finite and greater than 1.0 |
| `improve_threshold` | number | 0.05 | fractional gain a candidate needs to win; within 0.0 to 1.0 |
| `min_samples_per_arm` | integer | 4 | passes each size needs before a decision; at least 1 |
| `settle_after_epochs` | integer | 3 | measurements leaving the size alone before the search rests on it; `0` never rests |
| `backoff_factor` | number | 2.0 | divisor applied on a guard trip; finite and greater than 1.0 |
| `backoff_cooldown` | integer | 4 | passes the target is held after a division |
| `rows` | integer | absent | pins the target; cannot appear beside `min_rows`, `max_rows` or `start_rows` in the same block |

The same keys are valid in a `flow_control` child of a `source`, where they
override the top-level values field by field.

## When it refuses to start

Every key is checked at load time and rejected rather than clamped. A
top-level error is prefixed `flow_control`, and a source's own block also names
the workflow and the source, as in
`workflow 'orders' source 'pg_orders': flow_control: growth_factor must be a
finite number greater than 1.0, got 0.5`.

| Message | What to change |
|---|---|
| `flow_control: rows pins a fixed size and cannot be combined with min_rows/max_rows/start_rows; declare one intent or the other` | Keep either the pin or the range in that block. |
| `flow_control: min_rows must be at least 1` | Set `min_rows` to 1 or more. |
| `flow_control: rows must be at least 1` | Set `rows` to 1 or more, or drop the key to let it adapt. |
| `flow_control: max_rows (512) must be at least min_rows (1024)` | Raise `max_rows`, or lower `min_rows`. |
| `flow_control: start_rows (256) must be within min_rows (1024)..=max_rows (65536)` | Move `start_rows` inside the range, or widen the range. |
| `flow_control: adjust_interval_ms must be at least 1; it paces adjustment decisions` | Set a non-zero interval, or pin the source with `rows`. |
| `flow_control: growth_factor must be a finite number greater than 1.0, got 0.5` | Use a value above 1.0; a candidate has to differ from the size in effect. |
| `flow_control: backoff_factor must be a finite number greater than 1.0, got 1` | Use a value above 1.0; a divisor of 1.0 would back off to the same size. |
| `flow_control: improve_threshold must be within 0.0..1.0, got 5` | Use a fraction, not a percentage. |
| `flow_control: min_samples_per_arm must be at least 1` | Set it to 1 or more. |
| ``mode "cluster" does not take a `flow_control` block: a cluster workflow declares no source node, so there is no admission to govern`` | Delete the block; a cluster node ingests through claims, not sources. |

## Next

- [The live dashboard](@/service/operate/dashboard.md) plots every target and
  every decision this page describes.
- [Run modes and persistence](@/service/config/run-modes.md) is what a pass is,
  which is the unit flow control sizes.
