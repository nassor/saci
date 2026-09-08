+++
title = "How flow control decides"
description = "The epoch experiment, the safety guards, and the decision record behind the keys."
template = "page.html"
weight = 9
+++
# How flow control decides

This is the arithmetic behind the keys on
[Flow control](@/service/operate/flow-control.md). `FlowController` sizes each
source's admission per pass. It runs a two-arm experiment for throughput, divides
the target on adverse evidence, and records every move it makes.

## Why it is on by default

A source may yield one giant batch, or a connector tuned for a different workload may hand
over whatever size it happens to produce. The runner reshapes it regardless, with a
zero-copy `RecordBatch::slice`, admitting only the controller's current target and holding
the remainder as carry-over for the next pass. A source does not have to cooperate for this
to apply: it works even against `FileSource`, `HttpSource`, `S3Source`, `TcpIngestSource`,
`ChannelSource` and `DataFusionSource`, none of which implement the advisory hint below.

## How the search is paced: an epoch, two arms

Adjustment decisions land at the close of an adjustment epoch (`adjust_interval_ms`, one
minute by default), never per pass. Each epoch runs an experiment between two arms: the
incumbent size currently in effect, and one candidate a step away from it. The arms alternate
pass by pass rather than running as two separate blocks, so drift in machine load, clock
speed or upstream arrival rate falls on both arms instead of being blamed on one.

At the epoch's close, the two arms' rows per second are compared. The candidate wins by
beating the incumbent's rate by more than `improve_threshold`. A win makes it the next
epoch's incumbent, and the next candidate steps further in the same direction, by
`growth_factor`. A loss flips the direction instead, so the search can discover that a
*smaller* chunk is faster for a pipeline whose per-batch cost is dominated by cache behaviour
rather than fixed overhead.

A bound or the congestion ceiling can collapse a step back onto the incumbent. Such a step
turns around instead of proposing an experiment that can never run, since the arms only
alternate while they differ. A controller that has climbed to `max_rows` still tests a
smaller size next epoch, and one that has fallen to `min_rows` still tests a larger one.
`min_rows == max_rows` is the one range with a single admissible size, so it runs no
experiment at all.

An epoch that closes with fewer than `min_samples_per_arm` usable samples in either arm
decides nothing: it starts again rather than committing to noise. A workload that finishes
before its first epoch closes therefore runs at `start_rows` for its whole life, which is
the correct answer for a job too short to measure.

## Safety is not paced

Three kinds of evidence act immediately, on the pass that produced them, and abandon the
epoch in progress. The latency objective differs, because it is judged only at an epoch's
close and never divides the target the way a guard does. While the incumbent breaches it, a
smaller candidate wins the epoch regardless of throughput, so the objective still drives the
target down, one epoch step at a time.

| Evidence | Effect |
|---|---|
| The pass reported an error | Divides the target of every source the failure is evidence for, by `backoff_factor`, immediately: every source, when a processor, a sink or a fan-out append failed; only the one source, when its own drain failed |
| The chunk's projected Arrow memory would exceed `max_chunk_bytes` | Divides the target by `backoff_factor`, immediately |
| A sink's `pending_rows()` backlog grew on two consecutive passes | Divides the target by `backoff_factor`, immediately |
| `target_latency_ms` is breached | A candidate that would also breach it cannot win; while the incumbent breaches it, a smaller candidate wins regardless of throughput, and the search descends until a size meets the objective or `min_rows` proves it cannot be met |

A large but steady sink backlog is not pressure, because a buffering sink doing its job
reports a constant number. Pressure is judged as a trend across passes, never a threshold.

The descent above can walk the target down to `min_rows` on its own. A breach there abandons
the objective entirely rather than losing one more epoch, because no size the controller
can choose would meet it and obeying it further would cost throughput for nothing. The
whole rule resumes once a clean pass at the floor meets the objective again.

The size a guard divides is the larger of the arm under test and the incumbent, not
necessarily the one that tripped. A candidate stepping down that fails says nothing about
the incumbent it was measured against, so dividing the smaller of the two would discard a
size that won an epoch and never failed.

That failed size is remembered as a congestion ceiling. While it stands, the next epoch's
first candidate steps *down* rather than up, and once climbing back is warranted, each
step is additive, an eighth of the ceiling, rather than multiplicative, and never passes
it. A clean epoch win at or above the ceiling clears it, which is the evidence that the
wall moved.

`min_rows` is a hard floor with nowhere left to divide to, so a guard tripping there holds
the target and the search state instead of thrashing them. That is still adverse evidence, so
it counts toward `saci_flow_backoff_total` too, because a source pinned against the floor is
exactly what an operator needs to see. For `backoff_cooldown` passes after a division, the target
is held at the reduced size before the search resumes; adverse evidence during that window
extends the cooldown rather than dividing a second time.

A search that stops moving rests. Once `settle_after_epochs` epochs close with enough
evidence and leave the size where it is, the controller schedules no candidate, and every
pass of a rest epoch is admitted at the incumbent. Each further settled epoch doubles the
run of rest epochs, up to 16. A converged source therefore experiments in one epoch out of
seventeen, and spends about 3% of its passes probing rather than half of them.

A guard trip ends a rest on the pass that saw it, and an incumbent that starts missing
`target_latency_ms` ends it at the boundary that measured it. The objective is checked at
every epoch boundary, so a stream workflow's latency objective stays reactive. Both start
the streak again, and so does an epoch that moves the size, which only a measuring epoch
can do. An epoch with too little evidence measured nothing and leaves the streak alone.
`settle_after_epochs 0` never rests, so the arms alternate for as long as the source runs.

`rows` skips all of this: throughput is still measured for `/status` and the `saci_flow_*` series,
but the target is the pinned constant, and no guard, epoch or candidate ever touches it.

## Where a pass is measured, and where it is not

Time spent waiting for input, and run-mode pacing between iterations, are never counted as
consumption: only the consumer chain's own busy time feeds a sample.

`Continuous` and `Interval` take one sample per iteration per source. An iteration that
spent its whole credit while the source was still live, or that left a carry-over slice, is
backlogged and re-enters at once instead of waiting out the run mode's pacing. That keeps
`interval_ms` the idle poll cadence rather than a throughput ceiling.

`Stream` takes one sample per chunk: each arriving batch is sliced at the source's current
target, and each slice is one workflow pass. A trailing slice smaller than the target runs
immediately rather than waiting to fill one, because holding it back would trade away the
latency stream mode exists for. A source on a path to a windowed node takes no samples at
all in `stream`, because it runs without a controller there.
[Flow control](@/service/operate/flow-control.md) has the pinning rule for such a source.

## What it does not do

`RunMode::OneShot` engages no controller at all, because a single pass must drain every
source to completion by definition. Admitting a credit-sized prefix and exiting would
silently drop the rest.

`mode "cluster"` has none either, for a different reason. A cluster workflow
declares exactly one processor node and no `source` node at all, ingesting through
`PartitionSource`'s claim-and-lease mechanism instead of `Source`, so there is no source node
to attach a controller to. A `flow_control` block there that sets any key is a
load-time refusal naming the mode, rather than a block that would sit unread.

## The advisory hint

`Source::request_batch_rows(rows)` is an optional trait method the runner calls with its
current target before each pass. A connector that can act on it sizes its own fetch to
match, instead of over- or under-fetching at the transport.

On an ordinary path it changes nothing about how much the runner admits. The credit is the
same either way and the runner reshapes whatever comes back, so implementing it only saves
a connector wasted work. It does move where the arrival boundary falls, which a
pass-sensitive node can see, and [Flow control](@/service/operate/flow-control.md) says
what to pin there.

On a path to a windowed node the runner does not send it at all. There the arrival is the
pass, and a hinted fetch size would be a measurement choosing a pass boundary. In
`stream` that source holds no controller to hint from.

| Connector | Acts on the hint | What it steers |
|---|---|---|
| `KafkaSource` | Yes | `batch_size`: the poll window's collection target |
| `NatsSource` | Yes | `batch_size`: the same, for a core subscription or a JetStream pull |
| `PostgresSource` | Yes | `batch_rows`: the cursor query's `LIMIT`, or the logical-decode chunk size |
| `FileSource`, `HttpSource`, `S3Source`, `TcpIngestSource`, `TursoSource`, `ChannelSource`, `DataFusionSource` | No (trait default) | Nothing; each is still governed on an ordinary path, because the runner reshapes its output |

## The decision record

A decision is one pass on which the target moved, or held against a guard it could not
back away from. Four kinds: an epoch that closed on a larger winner (`grew`) or a smaller
one (`shrank`), a guard that divided the target (`backed_off`), and a guard that tripped
with the target already at `min_rows` (`held_at_floor`). A pass that moved nothing
records none.

Each record carries when it landed, the workflow and source ids, the rows either side of
the move, and a reason naming the number that decided. The reason is one of:

- `experiment won: 12.4k rows/s against 9.1k rows/s`
- `latency objective breached: 420 ms mean pass against 250 ms`
- `sink backlog growing: 8192 rows pending`
- `chunk over max_chunk_bytes: 12.6 MiB projected against 8.0 MiB`
- `pass error`

One record is one **episode** per source, not one guard trip. Safety is unpaced, so a
sink losing ground divides the target to the same size for the same reason on pass after
pass, and at `min_rows` it divides nothing and reports on every pass. Only the pass that
opens such a run is recorded: a new record appears when the target lands somewhere else,
when the cause changes, or when a different kind of decision intervenes. Read
`saci_flow_backoff_total` for the number of trips.

The in-process inspector retains them for its configured window and serves them on
`/api/snapshot` as `flow_decisions`, oldest first. The dashboard reads them as event
markers on the same time axis as the series history, each showing its reason. The three
`saci_flow_*` series say where a source's admission control stands; the decisions say how
it got there.

[Tracing & metrics](@/library/tracing.md) has the full series table, and
[Logs, metrics and traces](@/service/operate/observability.md) covers attribution and the
double count.
