+++
title = "Windowed aggregation"
description = "The saci `windows` feature: the three WindowSpec kinds, WindowedSystem construction, watermarks and the time field, the WindowResults resource, and WindowAccumulator persistence through CheckpointStore."
template = "page.html"
weight = 7
aliases = ["/reference/windowing/"]
+++
# Windowed aggregation

The `windows` feature, part of the `server` bundle, is the native route to windowed aggregation, and this page is its API reference. `WindowedSystem` assigns rows to tumbling, sliding or session windows, tracks watermarks, aggregates per key, and publishes results as a `WindowResults` resource.

A WASM processor or a plugin cannot use this path, because resources never cross the Arrow IPC boundary; those runtimes keep their windows in processor state instead.

## The three window kinds

| Kind | Geometry | Geometry keys |
|---|---|---|
| `Tumbling` | fixed `size_ms`, non-overlapping; each row lands in exactly one window | `size_ms`, `offset_ms` (alignment, default 0) |
| `Sliding` | overlapping; each row lands in `ceil(size_ms / slide_ms)` windows advancing by `slide_ms` | `size_ms`, `slide_ms` (must be ≤ `size_ms`), `offset_ms` (default 0) |
| `Session` | gap-delimited; a new session starts wherever the gap between consecutive events of one key exceeds `gap_ms` | `gap_ms` |

Tumbling boundaries use true floor division: `floor_div(ts - offset, size_ms)`. Sliding uses the same division with `slide_ms` as the step, once per window the row lands in. A geometry that is not well-defined, such as a non-positive `size_ms` or a `slide_ms` larger than `size_ms`, is a configuration error, caught once at load time instead of failing at run time.

`WindowSpec` lives in `saci::window`, which every build compiles. It serializes as an internally tagged object on the `kind` key, for example `{"kind":"tumbling","size_ms":30000}`, which is the shape a KDL `window` node reaches the runtime in. The table above is the Rust view: the same geometry written as KDL keys, with their defaults, lives on [Windowing](@/service/processors/windowing/_index.md).

Build a spec with `WindowSpec::tumbling(size_ms)`, `WindowSpec::sliding(size_ms, slide_ms)` or `WindowSpec::session(gap_ms)`, and shift the boundaries with `.with_offset(offset_ms)`. A session has no offset. `kind()` returns `"tumbling"`, `"sliding"` or `"session"`, and `validate()` returns the configuration error for a malformed geometry. `saci::window::tumbling_id(ts, size_ms, offset_ms)` and `sliding_ids(ts, size_ms, slide_ms, offset_ms)` return the window ids a timestamp falls into, for a processor that assigns windows itself.

## Building a windowed system

`WindowedSystem::new` takes the time field of a `#[derive(Component)]` type (the derive emits one `FieldRef` const per field, such as `Trade::TIMESTAMP_MS`), the window geometry and the window function. A non-positive `size_ms` is refused at construction with a `SaciError::Configuration`.

```rust,name=Windowing a derived component
use saci::prelude::*;

let sys = WindowedSystem::new(
    Trade::TIMESTAMP_MS,               // time field of the component type
    WindowSpec::tumbling(60_000),
    WindowFunction::sum("price"),
)?
.key_by(["symbol"])                    // per-key aggregation; omit for one global window
.allowed_lateness_ms(5_000);           // turns on watermark tracking
```

`WindowedSystem::new_named(component, time_field, spec, function)` takes the component and field as strings, for a component whose schema exists only at runtime:

```rust,name=Windowing a runtime schema
let sys = WindowedSystem::new_named(
    "Trade",
    "timestamp_ms",
    WindowSpec::sliding(60_000, 15_000),
    WindowFunction::mean("price"),
)?;
```

`WindowFunction::sum`, `min`, `max`, `count`, `mean`, `count_distinct`, `variance`, `stddev`, `first`, `last`, `approx_percentile`, `string_agg`, and `list_agg` apply built-in columnar aggregates per window group, and `WindowFunction::multi(Vec<AggregateSpec>)` evaluates multiple aggregates together in one pass. `WindowFunction::process` hands the whole window's `RecordBatch` to a `ProcessWindowFn` implementation, which receives a `WindowContext` carrying the window id, the inclusive `window_start`, the exclusive `window_end`, the current watermark, and `is_late_firing`.

## Watermarks and the time field

The time field names the event-time column of the source component, and every inbound component must carry it: at run time a missing column fails with `WindowedSystem: time field '<field>' not found in component '<component>'`. The column must be `Int64` (milliseconds since epoch) or an Arrow timestamp, converted as:

- `Int64`: passed through as milliseconds.
- `Timestamp(Second)`: multiplied by 1000.
- `Timestamp(Millisecond)`: used directly.
- `Timestamp(Microsecond)` and `Timestamp(Nanosecond)`: divided by 1000 and 1 000 000, truncating.

The service tracks one watermark per windowed node, the maximum event timestamp observed across all of the node's inbound data. It exposes that watermark as a `WindowWatermark` resource on the batch dataset, and as the `saci_window_watermark_seconds` series. Watermarks do not advance by themselves. Each run advances from the observed timestamps.

A row whose event time is more than `allowed_lateness_ms` behind the current watermark is beyond the lateness budget and goes to `WindowResults::dropped` instead of the aggregation. A window that had already been emitted re-fires with `is_late_firing` set when a late row within the budget arrives; the re-fired rows land in `WindowResults::late`.

## WindowResults and WindowAccumulator

Each run replaces the `WindowResults` resource with the finalized windows for that run only. Downstream systems read it from the pipeline, and it is never persisted via IPC.

`WindowResults` holds four public fields. `schema` is shared by every result batch and is the same for an empty run and a non-empty one. `on_time` holds the batches of on-time firings, `late` the late re-firings, and `dropped` the source rows beyond the lateness budget, in the source component's schema.

Cross-run accumulation belongs to the `WindowAccumulator` component, one row per `(source_component, window_id, key_hash)` triple. The windowed system appends its aggregates at the end of each run and reads them back at the start of the next. In a distributed run the host persists the accumulator through `CheckpointStore`, so open windows survive claims, checkpoints and node restarts.

The accumulator schema is versioned (`version` column, current version 1), so a checkpoint written by a newer binary is refused.

## Cross-batch component aggregation (`WindowAggregateSystem`)

While `WindowedSystem` writes finalized rows for a single batch into the `WindowResults` resource,
`WindowAggregateSystem` (exported in `saci::operators` and `saci::prelude` behind `windows`)
writes closed windows directly to a named output `Component` across passes and retains open rows in
`WindowAggregateState` (checkpointed automatically when registered via
`Pipeline::state::<WindowAggregateState>()`).

```rust,name=Stateful component-to-component windowed aggregation in Rust
use saci::prelude::*;

let window_agg = WindowAggregateSystem::new(
    "Sale",
    "WindowTotal",
    "timestamp_ms",
    WindowSpec::tumbling(30_000),
    ["symbol"],
    vec![
        AggregateSpec::count_all().alias("count"),
        AggregateSpec::sum("amount").alias("sum"),
    ],
)?
.allowed_lateness_ms(5_000)
.window_columns(WindowColumns {
    window_id_field: Some("window_id".to_string()),
    window_start_field: None,
    window_end_field: None,
});

let pipeline = Pipeline::new("windowed_sales")
    .state::<WindowAggregateState>()
    .system(window_agg);
```

Run the windowing example: [Windowing](@/service/processors/windowing/_index.md).
