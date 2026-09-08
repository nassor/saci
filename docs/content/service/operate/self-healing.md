+++
title = "Self-healing"
description = "How a broken connector is replaced with a fresh one, the keys that pace it, and the connectors it does not apply to."
template = "page.html"
weight = 4
aliases = ["/service/self-healing/"]
+++
# Self-healing

A connector can end up holding a handle that will never work again. A `tcp`
sink whose collector went away fails every later write on that same dead
socket. Retrying the write cannot help. `saci-service` builds a fresh
connector from the same factory and the same `config` instead. It is on with
no configuration, and a `heal` block paces it or switches it off.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 208" role="img" aria-labelledby="svc-hl-t svc-hl-d">
        <title id="svc-hl-t">Consecutive failures schedule a rebuild, which replaces the connector instance</title>
        <desc id="svc-hl-d">
            A workflow node on the left holds a connector instance that is failing. After
            three consecutive failures a control-plane box schedules a rebuild for a
            deadline one second away. At the head of the first call past that deadline the
            broken instance is dropped and the node's factory builds a fresh one, which
            takes its place. The first operation that succeeds on the fresh instance marks
            the node healthy again; one that fails schedules the next attempt twice as far
            out.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="44" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="44" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="130" height="8"/>
            <text class="t-lbl t-data" x="12" y="59">sink</text>
            <text class="t-sm" x="12" y="82">write failed x3</text>
            <path class="arw arw-ctl" d="M130 72 H166" marker-end="url(#svc-hl-c)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="170" y="44" width="160" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="170" y="44" width="160" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="170" y="56" width="160" height="8"/>
            <text class="t-lbl t-ctl" x="182" y="59">rebuild scheduled</text>
            <text class="t-sm" x="182" y="82">in 1000 ms</text>
            <path class="arw arw-ctl" d="M330 72 H366" marker-end="url(#svc-hl-c)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="370" y="44" width="120" height="56" rx="8"/>
            <text class="t-lbl t-ctl" x="382" y="68">factory</text>
            <text class="t-sm" x="382" y="86">same config</text>
            <path class="arw arw-data" d="M490 72 H526" marker-end="url(#svc-hl-a)"/>
            <rect class="blk blk-data" x="530" y="44" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="530" y="44" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="530" y="56" width="130" height="8"/>
            <text class="t-lbl t-data" x="542" y="59">fresh sink</text>
            <text class="t-sm" x="542" y="82">dials again</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 148 H654"/>
            <text class="t-sm" x="0" y="168">The broken instance is dropped before the factory is called, so it releases whatever it held.</text>
            <text class="t-sm" x="0" y="192">A connector that recovers on its own, or that cannot be built twice, is never rebuilt.</text>
        </g>
        <defs>
            <marker id="svc-hl-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="svc-hl-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-control"><i></i> the rebuild decision</span>
    </div>
</div>

## 1. What it does for you

Each source and sink counts its consecutive failures. At `after_failures` the
node schedules a rebuild for `base_delay_ms` from now, and the first call past
that deadline drops the instance and asks the node's factory for another one
with the same config. The first operation that then succeeds marks the node
healthy; one that fails schedules the next attempt `multiplier` times further
out, up to `max_delay_ms`. With the default `max_attempts` of `0` it never
gives up, so a broker down for an hour costs about sixty attempts and comes
back on its own.

This sits above the node's `retry` policy, which re-drives the same instance.
A failure only counts here once retrying has already given up, so the two
compose. `retry` rides out a call that failed for a reason the connector
survived. `heal` replaces a connector that did not.

No call waits for a rebuild. It happens at the head of a later call rather
than inside the one that failed, so a connector that is down never blocks a
pass.

## 2. Which connectors it applies to

A connector declares whether building it a second time is sound:

| Connector | Rebuilt | Why |
|---|---|---|
| `tcp` source and sink, `kafka` source and sink, `nats` source and sink, `http` source and sink, `PostgresSource`, `TursoSource`, `FileSink` without `truncate` | yes | a fresh instance resumes from state outside the process: a committed consumer offset, an offset table, a replication slot, a request it simply issues again, or a file reopened for append |
| `PostgresSink`, `TursoSink`, `S3Sink` | no | rows accepted but not yet flushed survive an outage inside the instance, and both re-establish their own connection; replacing them would discard those rows |
| `FileSource`, `S3Source` | no | both read from their start, so a fresh instance would re-deliver everything already read |
| `FileSink` with `truncate #true` | no | a fresh instance would replace the file and erase what this run already wrote |
| `ChannelSource`, `ChannelSink` | no | a channel is one mpsc pair created once per process |

A connector from outside this repository is not rebuilt until it says so,
because the factory trait's default answer is that a second build is
undeclared.

## 3. Turn it off, or pace it

Declared once at the top level, and optionally overridden per node:

```kdl,name=The heal block
heal {
    enabled #true
    after_failures 3
    base_delay_ms 1000
    multiplier 2.0
    max_delay_ms 60000
    jitter 0.1
    max_attempts 0
}
```

To stop replacing connectors anywhere:

```kdl,name=Off for the whole service
heal { enabled #false }
```

To react faster on one node, or to leave that one node alone:

```kdl,name=Per node
workflow "orders" {
    sink "collector" type="tcp" component="Order" transformer="ipc" {
        heal { after_failures 1; base_delay_ms 200 }
        config {
            connect "collector.internal:9500"
            schema_fields "id" type="int64" nullable=#false
        }
    }

    sink "archive" type="tcp" component="Order" transformer="ipc" {
        heal { enabled #false }
        config {
            connect "archive.internal:9500"
            schema_fields "id" type="int64" nullable=#false
        }
    }
}
```

A node's block layers over the top-level one key by key, so the first sink
above still takes the top-level `multiplier`, `max_delay_ms`, `jitter` and
`max_attempts`.

## 4. Read whether it happened

Two counters, each recorded once with no attributes and once under the node's
own id:

```bash,name=Linux/macOS
curl -s localhost:8080/metrics | grep saci_connector_heal
```

```powershell,name=Windows (PowerShell)
(Invoke-WebRequest localhost:8080/metrics).Content -split "`n" | Select-String saci_connector_heal
```

```text,name=Expected output
saci_connector_heals_total{otel_scope_name="saci"} 1
saci_connector_heals_total{sink="collector",otel_scope_name="saci"} 1
saci_connector_heal_failures_total{otel_scope_name="saci"} 2
saci_connector_heal_failures_total{sink="collector",otel_scope_name="saci"} 2
```

Failures climbing with no heals means the connector cannot come back: the
factory is refusing, so the reason is in the log rather than the metric.

The log lines ride on the `saci::heal` target, enabled at `warn` whatever
`log_level` says and never sampled. They always appear for the same reason
the flow-control lines do. On every other number, a connector that had to be
replaced reads exactly like one that never failed.

```text,name=One episode
WARN saci::heal: connector heal scheduled workflow="orders" node="collector" role="sink" attempt=1 delay_ms=1000 consecutive=3
WARN saci::heal: connector healed workflow="orders" node="collector" role="sink" attempt=1
WARN saci::heal: connector recovered workflow="orders" node="collector" role="sink"
```

`connector heal failed` carries the reason the factory gave, and
`connector heal exhausted` appears once when a node with a `max_attempts`
budget spends it and stops trying.

## Every key

### heal

| Key | Type | Default | What it does |
|---|---|---|---|
| `enabled` | boolean | `#true` | `#false` keeps re-driving the same instance forever |
| `after_failures` | integer | 3 | consecutive failures before the first rebuild; at least 1 |
| `base_delay_ms` | integer | 1000 | delay before the first rebuild; at least 1 |
| `multiplier` | number | 2.0 | growth per further attempt; at least 1.0 |
| `max_delay_ms` | integer | 60000 | ceiling on the computed delay; at least `base_delay_ms` |
| `jitter` | number | 0.1 | fraction of the delay randomised, within 0.0 to 1.0 |
| `max_attempts` | integer | 0 | rebuilds before the node gives up; `0` never gives up |

The same keys are valid in a `heal` child of a `source` or a `sink`, where
they override the top-level values field by field.

## When it refuses to start

Every key is checked at load time and rejected rather than clamped. A
top-level error is prefixed `heal`, and a node's own block also names the
workflow and the node, as in
`workflow 'orders' sink 'collector': heal: jitter must be within 0.0..=1.0,
got 3`.

| Message | What to change |
|---|---|
| `heal: after_failures must be at least 1; it counts the failures that precede a rebuild` | Set it to 1 or more, or turn healing off with `enabled #false`. |
| `heal: base_delay_ms must be at least 1; a zero delay rebuilds in a hot loop` | Set a non-zero delay. |
| `heal: multiplier must be at least 1.0, got 0.5` | Use a value of 1.0 or above; below it the delay would shrink. |
| `heal: jitter must be within 0.0..=1.0, got 3` | Use a fraction, not a percentage. |
| `heal: max_delay_ms (10) must be at least base_delay_ms (100)` | Raise the ceiling, or lower the base. |
| ``workflow 'w' sink 'out': heal is not available for type 'S3Sink': rows accepted but not yet uploaded live in the open object and survive a failed upload; a second build would discard them, and every upload already opens its own connection`` | Delete the node's `heal` block. The connector already recovers on its own, and replacing it would lose rows. |
| ``mode "cluster" does not take a `heal` block: a cluster workflow declares no source or sink node, so there is no connector to rebuild`` | Delete the block; a cluster node ingests through claims, not connectors. |

A node with no `heal` block of its own is never refused: a connector that
cannot be rebuilt is left alone under the inherited policy. The refusal exists
so that a block written by name is never silently ignored.

## Next

- [Logs, metrics and traces](@/service/operate/observability.md) is every
  series, including the two counters above.
- [Writing the workflow](@/service/config/workflows.md) is the `retry` policy
  this composes with.
