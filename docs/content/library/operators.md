+++
title = "Relational & stream operators"
description = "Tokio-free Arrow operators in saci::operators: FilterSystem, ProjectSystem, GroupBySystem, SortSystem, TopNSystem, UnnestSystem, JoinSystem, IntervalJoinSystem, LookupJoinSystem, DeduplicateSystem, Expr, and SqlSystem."
template = "page.html"
weight = 8
+++
# Relational & stream operators

The `saci::operators` module (re-exported in `saci::prelude`) provides a Tokio-free suite of relational, analytical, and streaming Arrow operators. Every operator implements [`System`](@/library/systems.md) so pipelines compose identically in native binaries, `wasm32-wasip3` WebAssembly processors, and native plugins.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 226" role="img" aria-labelledby="lib-op-title lib-op-desc">
        <title id="lib-op-title">Stateless and stateful Arrow operators reading and writing Dataset components</title>
        <desc id="lib-op-desc">
            On the left, a Dataset holds Orders and Customers components and a TierSummary output component.
            In the center top, stateless operators (FilterSystem, GroupBySystem, SqlSystem) transform batches
            via set_batch_named into output components on the right. In the center bottom, stateful operators
            (IntervalJoinSystem, LookupJoinSystem, DeduplicateSystem) read and update sdk::State across runs.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="34" width="150" height="114" rx="8"/>
            <rect class="hd hd-data" x="0" y="34" width="150" height="22" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="150" height="8"/>
            <text class="t-lbl t-data" x="12" y="49">Dataset</text>
            <rect class="row-r" x="8" y="64" width="134" height="22" rx="3"/>
            <text class="t-sm" x="16" y="79">Orders</text>
            <text class="t-sm t-ctl t-end" x="134" y="79">read</text>
            <rect class="row-r" x="8" y="92" width="134" height="22" rx="3"/>
            <text class="t-sm" x="16" y="107">Customers</text>
            <text class="t-sm t-ctl t-end" x="134" y="107">read</text>
            <rect class="row-w" x="8" y="120" width="134" height="22" rx="3"/>
            <text class="t-sm" x="16" y="135">TierSummary</text>
            <text class="t-sm t-data t-end" x="134" y="135">write</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-ctl" d="M150 75 C180 75 180 52 210 52" marker-end="url(#lib-op-c)"/>
            <rect class="blk blk-data" x="210" y="18" width="220" height="68" rx="8"/>
            <rect class="hd hd-data" x="210" y="18" width="220" height="20" rx="8"/>
            <rect class="hd hd-data" x="210" y="30" width="220" height="8"/>
            <text class="t-lbl" x="222" y="33">stateless operators</text>
            <text class="t-sm" x="222" y="56">Filter / GroupBy / SqlSystem</text>
            <text class="t-sm" x="222" y="74">in-batch Arrow transforms</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-ctl" d="M150 103 C180 103 180 140 210 140" marker-end="url(#lib-op-c)"/>
            <rect class="blk blk-bnd" x="210" y="104" width="220" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="104" width="220" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="116" width="220" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="119">stateful operators</text>
            <text class="t-sm" x="222" y="142">IntervalJoin / LookupJoin</text>
            <text class="t-sm" x="222" y="160">reads &amp; updates sdk::State&lt;T&gt;</text>
            <path class="arw arw-bnd" d="M375 176 V202 H265 V176" marker-end="url(#lib-op-b)"/>
            <text class="t-sm t-bnd t-mid" x="320" y="218">State&lt;T&gt; checkpointed via IPC across calls</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M430 52 H490" marker-end="url(#lib-op-d)"/>
            <path class="arw arw-data" d="M430 140 C460 140 460 112 490 112" marker-end="url(#lib-op-d)"/>
            <rect class="blk blk-data" x="490" y="42" width="170" height="88" rx="8"/>
            <rect class="hd hd-data" x="490" y="42" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="54" width="170" height="8"/>
            <text class="t-lbl" x="502" y="57">target component</text>
            <text class="t-sm" x="502" y="82">set_batch_named(&hellip;)</text>
            <text class="t-sm" x="502" y="102">resizes rows &amp; bitmap</text>
        </g>
        <defs>
            <marker id="lib-op-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="lib-op-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="lib-op-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg></div>
    <div class="dgm-key">
        <span class="k-control"><i></i> component columns read into operators</span>
        <span class="k-data"><i></i> resized RecordBatches written back to Dataset</span>
        <span class="k-boundary"><i></i> stateful checkpoint boundary (sdk::State&lt;T&gt;)</span>
    </div>
</div>

## Operator overview

| Operator | Semantics | State across batches |
|---|---|---|
| `FilterSystem` | Evaluates a boolean `Expr` predicate and keeps matching rows | Stateless |
| `ProjectSystem` | Evaluates expressions with aliases and optional wildcards (`*`) | Stateless |
| `GroupBySystem` | Keys batches and evaluates `AggregateSpec`s (`sum`, `min`, `max`, `count`, `count_all`, `mean`, `count_distinct`, `variance`, `stddev`, `first`, `last`, `approx_percentile`, `string_agg`, `list_agg`) | Stateless |
| `SortSystem`, `TopNSystem` | Lexicographic Arrow sort (`SortKey`) with optional `limit` and `offset` | Stateless |
| `UnnestSystem` | Explodes `List` / `LargeList` columns into one row per element (`preserve_empty`) | Stateless |
| `JoinSystem` | Equi-join (`Inner`, `Left`, `Right`, `Full`, `Semi`, `Anti`) between two components | Stateless |
| `IntervalJoinSystem` | Event-time bounded join (`[ts + lower_bound_ms, ts + upper_bound_ms]`) with watermark eviction | `sdk::State<IntervalJoinState>` |
| `LookupJoinSystem` | Enriches a fact stream against an upserted dimension table | `sdk::State<LookupJoinState>` |
| `DeduplicateSystem` | Drops duplicate keys within a batch (`KeepPolicy::{First, Last}`) or across batches with TTL and `max_keys` | Optional `sdk::State<DeduplicateState>` |
| `SqlSystem` | Embedded SQL `SELECT [DISTINCT] ... FROM ... [JOIN ...] [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT ... OFFSET ...]` | Stateless |

## Composing operators in Rust

All unary operators accept `.into_component("Out")` to write to a separate component instead of replacing their input component in place:

```rust,name=Pipeline with operators
use saci::prelude::*;

let pipeline = Pipeline::new("orders_analytics")
    .system(FilterSystem::parse("Orders", "amount > 50.0 AND status != 'void'")?)
    .system(
        ProjectSystem::parse("Orders", &["region", "qty * price AS revenue"])?
            .into_component("OrderLines"),
    )
    .system(GroupBySystem::new(
        "OrderLines",
        "RegionSummary",
        ["region"],
        vec![
            AggregateSpec::sum("revenue").alias("total_revenue"),
            AggregateSpec::count_all().alias("order_count"),
        ],
    )?)
    .system(TopNSystem::in_place(
        "RegionSummary",
        vec![SortKey::desc("total_revenue")],
        10,
    )?);
```

## Embedded SQL queries

`SqlSystem` parses standard SQL queries directly into native Arrow operations without external runtime engines or tokio:

```rust,name=Embedded SQL over Dataset components
use saci::prelude::*;

let sql = SqlSystem::parse(
    "TierSummary",
    "SELECT c.tier, SUM(o.amount) AS total, COUNT(DISTINCT o.order_id) AS orders \
     FROM Orders o \
     JOIN Customers c ON o.customer_id = c.id \
     WHERE o.amount >= 25.0 \
     GROUP BY c.tier \
     HAVING SUM(o.amount) > 100.0 \
     ORDER BY total DESC \
     LIMIT 5",
)?;
```

## Resizing batches with `set_batch`

Operators that change a component's row count (`FilterSystem`, `GroupBySystem`, `UnnestSystem`, `JoinSystem`, `DeduplicateSystem`, `TopNSystem`) write their output using `Dataset::set_batch` / `Dataset::set_batch_named`, which updates `Dataset::rows()` and keeps the alive bitmap consistent across IPC round trips.
