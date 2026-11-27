+++
title = "Declarative operators"
description = "Run SQL, filters, projections, joins, unnest, group_by, sort, top_n, and deduplicate natively inside a saci-service KDL workflow without external artifacts."
template = "page.html"
weight = 1
+++

# Declarative operators

An `operator` node runs relational, analytical, and stream-processing transforms directly on
SACI's columnar Arrow engine inside `saci-service`: no `.wasm` module, shared library, or compiler
toolchain required.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 226" role="img" aria-labelledby="op-title op-desc">
        <title id="op-title">Declarative operator nodes execute native Arrow pipelines between sources and sinks</title>
        <desc id="op-desc">
            Left: two sources, orders_in and customers_in, deliver typed RecordBatches over component edges.
            Top: inline config supplies parameters such as where, select, query, or on.
            Center: an operator node runs inside a native PipelineRuntime, optionally keeping cross-batch state
            in memory for stateful operators. Right: the transformed component reaches the sink.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="44" width="150" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="44" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="150" height="8"/>
            <text class="t-lbl" x="12" y="59">orders_in</text>
            <text class="t-sm" x="12" y="81">component Orders</text>
            <rect class="blk blk-data" x="0" y="112" width="150" height="52" rx="8"/>
            <rect class="hd hd-data" x="0" y="112" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="124" width="150" height="8"/>
            <text class="t-lbl" x="12" y="127">customers_in</text>
            <text class="t-sm" x="12" y="149">component Customers</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M150 70 C177 70 177 94 205 94" marker-end="url(#op-d)"/>
            <path class="arw arw-data" d="M150 138 C177 138 177 114 205 114" marker-end="url(#op-d)"/>
            <rect class="blk blk-bnd" x="205" y="64" width="220" height="80" rx="8"/>
            <rect class="hd hd-bnd" x="205" y="64" width="220" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="205" y="76" width="220" height="8"/>
            <text class="t-lbl t-bnd" x="217" y="79">operator &quot;enrich&quot;</text>
            <text class="t-sm" x="217" y="104">kind=&quot;sql&quot; | &quot;join&quot; | ...</text>
            <text class="t-sm" x="217" y="124">native Arrow PipelineRuntime</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="205" y="0" width="220" height="36" rx="8"/>
            <text class="t-lbl" x="217" y="23">config</text>
            <text class="t-sm t-ctl t-end" x="413" y="23">where / query / on</text>
            <path class="arw arw-ctl" d="M315 36 V64" marker-end="url(#op-c)"/>
            <path class="arw arw-data" d="M425 104 H500" marker-end="url(#op-d)"/>
            <rect class="blk blk-data" x="500" y="76" width="160" height="56" rx="8"/>
            <rect class="hd hd-data" x="500" y="76" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="500" y="88" width="160" height="8"/>
            <text class="t-lbl" x="512" y="91">summary_out</text>
            <text class="t-sm" x="512" y="114">sink (into component)</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M380 144 V182 H250 V144" marker-end="url(#op-b)"/>
            <text class="t-sm t-bnd t-mid" x="315" y="200">stateful operators retain buffer / seen keys across batches</text>
        </g>
        <defs>
            <marker id="op-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="op-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="op-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg></div>
    <div class="dgm-key">
        <span class="k-data"><i></i> component RecordBatches along links</span>
        <span class="k-boundary"><i></i> declarative operator runtime &amp; cross-batch state</span>
        <span class="k-control"><i></i> inline KDL operator configuration</span>
    </div>
</div>

## Chaining operators in KDL

Every `operator` node behaves as a workflow processor in `link` graphs. Unary operators default to
modifying `component` in place, or write to a new component when `into` is set.

```kdl,name=Chaining filter project group_by and top_n in KDL
workflow "order_analytics" {
    source "in" connector="file" component="Orders" codec="ndjson" {
        config path="orders.ndjson" {
            field "id" type="int64" nullable=#false
            field "region" type="utf8" nullable=#false
            field "qty" type="int64" nullable=#false
            field "price" type="float64" nullable=#false
        }
    }

    operator "high_value" kind="filter" component="Orders" {
        config where="qty * price >= 100.0"
    }

    operator "revenue" kind="project" component="Orders" into="Lines" {
        config {
            select "region"
            select "qty * price AS revenue"
        }
    }

    operator "by_region" kind="group_by" component="Lines" into="Summary" {
        config keys="region" {
            aggregates "sum(revenue) AS total_revenue"
            aggregates "count(*) AS order_count"
        }
    }

    operator "ranked" kind="top_n" component="Summary" {
        config order_by="total_revenue DESC" limit=10
    }

    sink "out" connector="file" component="Summary" codec="ndjson" {
        config path="summary.ndjson"
    }

    link from="in" to="high_value"
    link from="high_value" to="revenue"
    link from="revenue" to="by_region"
    link from="by_region" to="ranked"
    link from="ranked" to="out"
}
```

## SQL, joins, and stateful deduplication

An `sql` operator reads one or more upstream components by name, executes filters, joins,
aggregations, ordering, and limits, and writes its result to `into`:

```kdl,name=SQL join and aggregation
operator "enrich_and_sum" kind="sql" into="TierSummary" {
    config query="SELECT c.tier, SUM(o.amount) AS total FROM Orders o JOIN Customers c ON o.cust_id = c.id GROUP BY c.tier ORDER BY total DESC"
}
```

Streaming stateful operators retain their checkpoint state across passes automatically inside standalone continuous/interval/stream runners:

- `interval_join`: matches `left` and `right` events on key `on` where `right_time - left_time` lies within `[lower_bound_ms, upper_bound_ms]`, evicting expired buffers via watermark.
- `lookup_join`: maintains an upserted dimension table on `right` and enriches `left` batches.
- `deduplicate`: drops duplicate `keys` within a batch and across batches (with optional `time_field`, `ttl_ms`, and `max_keys`).

## Every key

| Key | Location | Default | What it does |
|---|---|---|---|
| `<id>` | leading argument | required | Unique workflow node id referenced by `link from`/`to`. |
| `kind` | attribute | required | `"sql"`, `"filter"`, `"project"`, `"group_by"`, `"sort"`, `"top_n"`, `"unnest"`, `"join"`, `"interval_join"`, `"lookup_join"`, or `"deduplicate"`. |
| `name` | attribute | none | Optional human-readable display name shown in the live dashboard. |
| `component` | attribute | none | Input component for unary operators (or SQL fallback output component). |
| `left` | attribute | none | Left input component for binary join kinds (`join`, `interval_join`, `lookup_join`). |
| `right` | attribute | none | Right input component for binary join kinds. |
| `into` | attribute | `component` (unary) | Output component name written back into the workflow `Dataset`. Required for joins and `sql`. |
| `config` | child block / props | empty | Operator-specific parameters (`where`, `predicate`, `select`, `keys`, `aggregates`, `order_by`, `limit`, `offset`, `field`, `alias`, `preserve_empty`, `on`, `join_type`, `left_time_field`, `right_time_field`, `lower_bound_ms`, `upper_bound_ms`, `allowed_lateness_ms`, `max_buffered_rows`, `right_prefix`, `keep`, `time_field`, `ttl_ms`, `max_keys`, `stateful`, `query`). |
| `window` | child block | none | Optional host-tracked event-time watermark declaration (`windows` feature). |
