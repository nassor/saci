+++
title = "Windowing"
description = "An aggregate over a bounded slice of event time. The three window kinds, the keys that declare one, and one runnable example each."
template = "section.html"
sort_by = "weight"
weight = 2
aliases = ["/service/windowing/"]
+++

<dl class="page-facts">
<dt>What it is</dt>
<dd>An aggregate over a <strong>bounded slice of event time</strong>, cut from an unbounded stream</dd>
<dt>Reach for it when</dt>
<dd>You need per key totals, and the rows carry <strong>their own timestamp</strong></dd>
<dt>Split of work</dt>
<dd>The host tracks the watermark; the <strong>processor</strong> holds the windows and aggregates</dd>
</dl>

A stream has no end, so a total over one is never finished. A window gives it a
moment by grouping rows whose event time falls in `[start, end)`. Event time is
the timestamp in the row, not the moment the service read it, so a re-run of
the same input produces the same totals.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 246" role="img" aria-labelledby="wc-title wc-desc">
        <title id="wc-title">The watermark passing a window's end is what closes it</title>
        <desc id="wc-desc">
            An event-time axis runs from zero to ninety seconds, divided into three tumbling
            thirty-second windows. Rows are scattered across the first two windows and the
            start of the third. The watermark, the highest event timestamp seen so far,
            stands at seventy-four seconds. Windows zero and one end before it, so both are
            closed and the processor has emitted one aggregate row per key for each of them.
            Window two ends at ninety seconds, past the watermark, so it stays open and
            emits nothing yet. The watermark only advances when rows arrive, so a stream
            that stops leaves its last window open.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="42" y="52" width="196" height="96" rx="8"/>
            <rect class="hd hd-data" x="42" y="52" width="196" height="20" rx="8"/>
            <rect class="hd hd-data" x="42" y="64" width="196" height="8"/>
            <text class="t-lbl" x="54" y="67">window 0</text>
            <rect class="blk blk-data" x="242" y="52" width="196" height="96" rx="8"/>
            <rect class="hd hd-data" x="242" y="52" width="196" height="20" rx="8"/>
            <rect class="hd hd-data" x="242" y="64" width="196" height="8"/>
            <text class="t-lbl" x="254" y="67">window 1</text>
            <rect class="blk" x="442" y="52" width="196" height="96" rx="8"/>
            <rect class="hd" x="442" y="52" width="196" height="20" rx="8"/>
            <rect class="hd" x="442" y="64" width="196" height="8"/>
            <text class="t-lbl" x="454" y="67">window 2</text>
            <path class="ax" d="M42 148 H638"/>
            <text class="t-ax t-mid" x="42" y="164">0s</text>
            <text class="t-ax t-mid" x="240" y="164">30s</text>
            <text class="t-ax t-mid" x="440" y="164">60s</text>
            <text class="t-ax t-mid" x="638" y="164">90s</text>
        </g>
        <g class="anim anim-2">
            <rect class="bar-data" x="62" y="126" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="96" y="112" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="134" y="130" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="178" y="104" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="210" y="124" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="258" y="118" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="296" y="132" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="340" y="106" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="392" y="128" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="462" y="120" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="504" y="134" width="7" height="7" rx="1"/>
        </g>
        <g class="anim anim-3">
            <path class="mark" d="M532 36 V152"/>
            <text class="t-sm t-ctl t-end" x="524" y="44">watermark 74s</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M140 150 V184" marker-end="url(#wc-d)"/>
            <path class="arw arw-data" d="M340 150 V184" marker-end="url(#wc-d)"/>
            <rect class="blk blk-data" x="42" y="186" width="396" height="48" rx="8"/>
            <rect class="hd hd-data" x="42" y="186" width="396" height="20" rx="8"/>
            <rect class="hd hd-data" x="42" y="198" width="396" height="8"/>
            <text class="t-lbl" x="54" y="201">closed: one aggregate row per window and key</text>
            <text class="t-sm" x="54" y="224">both ends are behind the watermark</text>
            <text class="t-sm" x="454" y="201">window 2 stays open:</text>
            <text class="t-sm" x="454" y="217">its end is ahead of the</text>
            <text class="t-sm" x="454" y="233">watermark, so it emits nothing</text>
        </g>
        <defs>
            <marker id="wc-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane: rows, and the aggregates they close into</span>
        <span class="k-control"><i></i> the watermark, tracked by the host</span>
        <span class="k-mute"><i></i> a window still open</span>
    </div>
    <figcaption class="dgm-cap">
        The watermark is the highest event timestamp seen so far, so it moves only when
        rows arrive. A stream that stops leaves its last window open.
    </figcaption>
</div>

The host injects the `window` block into the processor's `config` as `window.*` keys, tracks the
node's watermark (`saci_window_watermark_seconds`), and
[the live dashboard](@/service/operate/dashboard.md) shows the chip and the watermark as it moves.

A pass boundary is where the node observes event time. For a source feeding the node, the runner
never draws one inside an arrival. It never splits an arrival, and it never sends the connector a
fetch-size hint that would size one. How many arrivals share a pass still follows the admission
credit in `continuous` and `interval`.

`allowed_lateness_ms` has to cover that grouping, each stream's disorder across arrivals, and the
skew between fan-in sources. [Flow control](@/service/operate/flow-control.md) states the rule,
the two settings that pin the grouping, and the memory it costs.

## The three kinds

| Kind | Geometry | Geometry keys |
|---|---|---|
| `tumbling` | fixed `size_ms`, non-overlapping; each row lands in exactly one window | `size_ms`, `offset_ms` |
| `sliding` | overlapping; each row lands in `ceil(size_ms / slide_ms)` windows advancing by `slide_ms` | `size_ms`, `slide_ms` (no larger than `size_ms`), `offset_ms` |
| `session` | gap delimited; a new session starts wherever the silence between one key's consecutive events exceeds `gap_ms` | `gap_ms` |

The question being asked picks the kind. Each page below opens with the geometry in full and
what that kind costs, and carries one runnable example.

- [Tumbling](@/service/processors/windowing/tumbling.md): fixed periods, so one row of the sink is
  one interval. Revenue per hour, errors per minute, a daily rollup. The cheapest kind to carry,
  and the only one whose rows can be summed again without double counting.
- [Sliding](@/service/processors/windowing/sliding.md): the same length restarting every
  `slide_ms`, so the answer reads "the last `size_ms` as of now". Moving averages and rate
  alerts read this way, and so do trend lines. State and output both multiply by the overlap
  factor.
- [Session](@/service/processors/windowing/session.md): bounds taken from the data, ended by
  silence. A user's visit, a device's connected stretch, one conversation. The gap is the only
  number to choose, and a key that never falls silent never closes.

## Every key

The `window` block sits inside a `wasm` or `plugin` node and declares the event-time geometry
the processor works in.

### window

| Key | Type | Default | What it does |
|---|---|---|---|
| `kind` | string | required | `"tumbling"`, `"sliding"` or `"session"` |
| `size_ms` | integer | required for tumbling and sliding | window length in milliseconds |
| `slide_ms` | integer | required for sliding | how often a new window starts |
| `offset_ms` | integer | `0` | alignment offset, tumbling and sliding only |
| `gap_ms` | integer | required for session | event-time silence that ends a session |
| `time_field` | string | required | the event-time column, `Int64` milliseconds or an Arrow timestamp type |
| `key_field` | child node | none | the grouping keys, one child per key or several arguments on one child. Omit for a global aggregate |
| `allowed_lateness_ms` | integer | `0` | how far behind the watermark a row may sit and still be counted |

A geometry key that does not belong to the declared `kind` is a parse error, and so is an
unknown `kind`. Cluster mode takes no `window` block, because a clustered processor keeps its
windows in its own state.

## When the sinks go quiet

A windowed node's watermark only moves forward. An arrival whose newest event
timestamp sits more than `allowed_lateness_ms` below that watermark carries no
row the node can place in a window. The node opens none, closes none, and the
sinks behind it receive nothing. Every other number reads healthy: the sources
report throughput, the processor reports batches, and the sink reports zero
records per second.

Two producers reach that state. One is a stream whose event time restarted
behind where it stopped. `windowed_publish` begins its simulated clock at a
fixed base every run, so restarting it against a still running service rewinds
event time by the whole previous run. The other is a single row with a
far-future timestamp, which drags the watermark past everything real that
follows it.

A rewind ends itself. The node accepts rows again once the new run's event
time climbs back past the retained watermark. At a fixed publishing rate that
takes about as long as the previous run lasted. A 40-minute first run means 40
minutes of empty sink, which reads as permanent on a dashboard.

The service names the condition on the `saci::windowing` log target, which no
`log_level`, no `RUST_LOG` and no sample ratio silences. One line opens the
report, after four consecutive dropped arrivals, and one line ends it when an
arrival lands back inside the budget. That is a different condition from the
watermark moving: a row within `allowed_lateness_ms` of it is accepted and
re-fires its window. Four in a row separates a rewound stream from one lagging
fan-in source, whose dropped arrivals alternate with a faster peer's accepted
ones and are counted rather than reported:

```text
WARN saci::windowing: windowed node is dropping every arrival: the inbound
event time is behind its watermark by more than the allowed lateness, so no
window can open or close and its sinks receive nothing. A producer restarted
with rewound event time, a replay, or one far-future timestamp will do this;
the watermark is monotonic and never rewinds with the stream
workflow=windowing_tumbling processor=window_wasm newest_event_ms=1700000002000
watermark_ms=1700000120000 behind_ms=118000 allowed_lateness_ms=5000
```

`saci_window_late_arrivals_total` counts every arrival dropped that way, under
`processor="<id>"`:

```bash,name=Count the dropped arrivals
curl -s http://127.0.0.1:8080/metrics | grep saci_window_late_arrivals_total
```

Windows (PowerShell):

```powershell,name=Count the dropped arrivals
(Invoke-WebRequest http://127.0.0.1:8080/metrics).Content -split "`n" |
  Select-String saci_window_late_arrivals_total
```

A count that climbs while the sources are live is that condition and nothing
else. Restart the service together with the producer, so the node starts with
no watermark, or have the producer carry event time forward from where it
stopped. `allowed_lateness_ms` covers real event-time disorder. Covering a
rewind instead needs a budget as large as the rewind, which would keep every
window of that span open.

## When it refuses to start

| Message | What to change |
|---|---|
| `workflow 'windowing_tumbling': wasm node 'window_wasm' window is invalid: tumbling window size_ms must be > 0, got 0` | Give the window a positive `size_ms`. |
| `workflow 'windowing_sliding': wasm node 'window_sliding' window is invalid: sliding window slide_ms (75000) must be <= size_ms (60000)` | Shrink `slide_ms`, or grow `size_ms` to match. |
| `workflow 'windowing_session': wasm node 'window_session' window is invalid: session window gap_ms must be > 0, got 0` | Give the session window a positive `gap_ms`. |
| `workflow 'windowing_tumbling': wasm node 'window_wasm' window is invalid: window time_field must not be empty` | Name the event-time column in `time_field`. |
| `workflow 'windowing_tumbling': wasm node 'window_wasm' window is invalid: window allowed_lateness_ms must be >= 0, got -1` | Use zero or a positive lateness budget. |
| `slide_ms is only valid on a sliding window` | Drop the key, or change `kind` to `"sliding"`. The same shape rejects `gap_ms` outside a session window and `size_ms` or `offset_ms` inside one. |
| `workflow 'windowing_tumbling': processor 'window_wasm' declares window time_field 'timestamp_ms' but component 'Sale' delivered by an inbound link has no such field` | Add that column to the component every inbound link delivers, or point `time_field` at one it already carries. |
| `workflow 'windowing_tumbling': processor 'window_wasm' declares window time_field 'symbol' on component 'Sale' as Utf8, but the host watermark tracker reads Int64 milliseconds or an Arrow timestamp type` | Point `time_field` at an `Int64` millisecond column or an Arrow timestamp column. |

## Next

- [Several workflows in one process](@/service/processors/multiple-workflows.md): the tumbling
  windowed processor, fed by a channel from another workflow.
- [The command line](@/service/operate/_index.md): the same binary under a supervisor.
