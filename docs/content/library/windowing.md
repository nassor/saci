+++
title = "Windowed aggregation"
description = "The saci-core `windows` feature: the three WindowSpec kinds, WindowedSystemBuilder, watermarks and the time field, the WindowResults resource, and WindowAccumulator persistence through CheckpointStore."
template = "page.html"
weight = 7
aliases = ["/reference/windowing/"]
+++
# Windowed aggregation

`saci-core`'s `windows` feature, part of the service default bundle, is the native route to windowed aggregation, and this page is its API reference. `WindowedSystem` and `WindowedSystemBuilder` assign rows to tumbling, sliding or session windows, track watermarks, aggregate per key, and publish results as a `WindowResults` resource.

A WASM processor or a plugin cannot use this path, because resources never cross the Arrow IPC boundary; those runtimes keep their windows in processor state instead.

## The three window kinds

| Kind | Geometry | Geometry keys |
|---|---|---|
| `Tumbling` | fixed `size_ms`, non-overlapping; each row lands in exactly one window | `size_ms`, `offset_ms` (alignment, default 0) |
| `Sliding` | overlapping; each row lands in `ceil(size_ms / slide_ms)` windows advancing by `slide_ms` | `size_ms`, `slide_ms` (must be ≤ `size_ms`), `offset_ms` (default 0) |
| `Session` | gap-delimited; a new session starts wherever the gap between consecutive events of one key exceeds `gap_ms` | `gap_ms` |

Tumbling boundaries use true floor division: `floor_div(ts - offset, size_ms)`. Sliding uses the same division with `slide_ms` as the step, once per window the row lands in. A geometry that is not well-defined, such as a non-positive `size_ms` or a `slide_ms` larger than `size_ms`, is a configuration error, caught once at load time instead of failing at run time.

`WindowSpec` serializes as an internally tagged object on the `kind` key, for example `{"kind":"tumbling","size_ms":30000}`, which is the shape a KDL `window` node reaches the runtime in. The table above is the Rust view: the same geometry written as KDL keys, with their defaults, lives on [Windowing](@/service/processors/windowing/_index.md).

## Building a windowed system

`WindowedSystemBuilder` collects the required pieces, then validates them in `build()`:

```rust,name=The builder shape
let sys = WindowedSystemBuilder::new()
    .source("Trade", "timestamp_ms")   // source component and its time field
    .keyed_by(&["symbol"])             // per-key aggregation; omit for one global window
    .window(WindowSpec::Tumbling { size_ms: 60_000, offset_ms: 0 })
    .function(WindowFunction::Reduce {
        input_field: "price",
        aggregate: ReduceAggregate::Sum,
    })
    .build()?;
```

`source`, `window` and `function` are required; missing one is a `SaciError::Configuration`. `.allowed_lateness(ms)` turns on watermark tracking.

`WindowFunction::Reduce` applies one built-in columnar aggregate, `Sum`, `Min`, `Max`, `Count` or `Mean`, over a single numeric field per window group. `WindowFunction::Process` hands the whole window's `RecordBatch` to a `ProcessWindowFn` implementation, which receives a `WindowContext` carrying the window id, the inclusive `window_start`, the exclusive `window_end`, the current watermark, and `is_late_firing`.

## Watermarks and the time field

The time field names the event-time column of the source component, and every inbound component must carry it: at run time a missing column fails with `WindowedSystem: time field '<field>' not found in component '<component>'`. The column must be `Int64` (milliseconds since epoch) or an Arrow timestamp, converted as:

- `Int64`: passed through as milliseconds.
- `Timestamp(Second)`: multiplied by 1000.
- `Timestamp(Millisecond)`: used directly.
- `Timestamp(Microsecond)` and `Timestamp(Nanosecond)`: divided by 1000 and 1 000 000, truncating.

The service tracks one watermark per windowed node, the maximum event timestamp observed across all of the node's inbound data. It exposes that watermark as a `WindowWatermark` resource on the batch dataset, and as the `saci_window_watermark_seconds` series. Watermarks do not advance by themselves. Each run advances from the observed timestamps.

A row whose event time is more than `allowed_lateness_ms` behind the current watermark is beyond the lateness budget and goes to the `SideOutput<DroppedLate>` resource instead of the aggregation. A window that had already been emitted re-fires with `is_late_firing` set when a late row within the budget arrives.

## WindowResults and WindowAccumulator

Each run replaces the `WindowResults` resource with the finalized windows for that run only. Downstream systems read it from the pipeline, and it is never persisted via IPC.

Cross-run accumulation belongs to the `WindowAccumulator` component, one row per `(source_component, window_id, key_hash)` triple. The windowed system appends its aggregates at the end of each run and reads them back at the start of the next. In a distributed run the host persists the accumulator through `CheckpointStore`, so open windows survive claims, checkpoints and node restarts.

The accumulator schema is versioned (`version` column, current version 1), so a checkpoint written by a newer binary is refused.

Run the windowing example: [Windowing](@/service/processors/windowing/_index.md).
