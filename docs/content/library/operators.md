+++
title = "Relational & stream operators"
description = "Tokio-free Arrow operators in saci::operators: FilterSystem, ProjectSystem, GroupBySystem, SortSystem, TopNSystem, UnnestSystem, JoinSystem, IntervalJoinSystem, LookupJoinSystem, DeduplicateSystem, Expr, and SqlSystem."
template = "page.html"
weight = 8
+++
# Relational & stream operators

The `saci::operators` module (re-exported in `saci::prelude`) provides a Tokio-free suite of relational, analytical, and streaming Arrow operators. Every operator implements [`System`](@/library/systems.md) so pipelines compose identically in native binaries, `wasm32-wasip3` WebAssembly processors, and native plugins.

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
