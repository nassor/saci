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

An `sql` operator reads one or more upstream components by name, executes filters, multi-way joins,
aggregations, expressions, ordering, and limits, and writes its result to `into` (or updates
`component` in place when `into` is omitted).

### SQL across one or more components

```kdl,name=SQL join with CASE COALESCE HAVING and ORDER BY
workflow "tier_summary" {
    source "orders_in" connector="file" component="Orders" codec="ndjson" {
        config path="orders.ndjson"
    }
    source "customers_in" connector="file" component="Customers" codec="ndjson" {
        config path="customers.ndjson"
    }

    operator "enrich_and_sum" kind="sql" into="TierSummary" {
        config query="""
            SELECT c.tier,
                   COALESCE(SUM(o.amount), 0.0) AS total,
                   COUNT(DISTINCT o.order_id) AS orders,
                   CASE WHEN COUNT(*) > 10 THEN 'high' ELSE 'normal' END AS volume
            FROM Orders o
            JOIN Customers c ON o.cust_id = c.id
            WHERE o.amount >= 25.0
            GROUP BY c.tier
            HAVING SUM(o.amount) BETWEEN 100.0 AND 100000.0
            ORDER BY total DESC
            LIMIT 20
        """
    }

    sink "out" connector="file" component="TierSummary" codec="ndjson" {
        config path="tier_summary.ndjson"
    }

    link from="orders_in" to="enrich_and_sum"
    link from="customers_in" to="enrich_and_sum"
    link from="enrich_and_sum" to="out"
}
```

### Batch relational joins (`join`)

Use `kind="join"` for in-batch equi-joins (`inner`, `left`, `right`, `full`, `semi`, `anti`). Key pairs accept `"left_col=right_col"`, matching column names like `"id"`, or an array of keys for composite joins:

```kdl,name=In-batch left join with collision prefix
operator "attach_customer" kind="join" left="Orders" right="Customers" into="EnrichedOrders" {
    config on="cust_id=id" join_type="left" right_prefix="cust_"
}
```

### Event-time interval joins (`interval_join`)

`interval_join` correlates two event streams where `right_ts` lies within `[left_ts + lower_bound_ms, left_ts + upper_bound_ms]`. Buffered rows remain in memory (or in `store "redb"` when `batch_resume #true` is set) until the watermark advances past their horizon, bounded optionally by `max_buffered_rows`:

```kdl,name=Bounded stream-stream interval join in continuous mode
workflow "match_payments" {
    source "orders_in" connector="kafka" component="Orders" codec="ndjson" {
        config topic="orders"
    }
    source "payments_in" connector="kafka" component="Payments" codec="ndjson" {
        config topic="payments"
    }

    operator "order_payments" kind="interval_join" left="Orders" right="Payments" into="MatchedOrders" {
        config on="order_id" \
               left_time_field="created_at" \
               right_time_field="paid_at" \
               lower_bound_ms=0 \
               upper_bound_ms=300000 \
               allowed_lateness_ms=5000 \
               max_buffered_rows=10000
    }

    sink "matched_out" connector="file" component="MatchedOrders" codec="ndjson" {
        config path="matched.ndjson"
    }

    link from="orders_in" to="order_payments"
    link from="payments_in" to="order_payments"
    link from="order_payments" to="matched_out"
}
```

### Dimension table enrichment (`lookup_join`)

`lookup_join` upserts batches arriving on `right` into a keyed dimension table and joins every `left` batch against the latest snapshot (`inner` or `left`):

```kdl,name=Stateful dimension lookup join
workflow "enrich_orders" {
    source "orders_in" connector="nats" component="Orders" codec="ndjson" {
        config subject="orders.created"
    }
    source "dim_in" connector="nats" component="CustomerDim" codec="ndjson" {
        config subject="customers.updates"
    }

    operator "enrich" kind="lookup_join" left="Orders" right="CustomerDim" into="EnrichedOrders" {
        config on="customer_id=id" join_type="left" right_prefix="cust_"
    }

    sink "out" connector="file" component="EnrichedOrders" codec="ndjson" {
        config path="enriched.ndjson"
    }

    link from="orders_in" to="enrich"
    link from="dim_in" to="enrich"
    link from="enrich" to="out"
}
```

### Stateful deduplication and list unnesting (`deduplicate` and `unnest`)

`deduplicate` drops duplicate keys within a batch and across passes (`keep="first"` or `"last"`), optionally expiring old keys via `time_field` and `ttl_ms` or bounding state size via `max_keys`. Chain `unnest` to explode `List` / `LargeList` columns into one row per element:

```kdl,name=Stateful TTL deduplication followed by outer unnest
workflow "dedup_and_explode" {
    source "events_in" connector="kafka" component="Events" codec="ndjson" {
        config topic="events"
    }

    operator "dedup_events" kind="deduplicate" component="Events" {
        config keys="event_id" \
               keep="last" \
               time_field="event_ts" \
               ttl_ms=600000 \
               max_keys=100000 \
               stateful=#true
    }

    operator "explode_tags" kind="unnest" component="Events" into="EventTags" {
        config field="tags" alias="tag" preserve_empty=#true
    }

    sink "out" connector="file" component="EventTags" codec="ndjson" {
        config path="event_tags.ndjson"
    }

    link from="events_in" to="dedup_events"
    link from="dedup_events" to="explode_tags"
    link from="explode_tags" to="out"
}
```

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
| `config` | child block / props | empty | Operator-specific parameters summarized by `kind` below; unknown keys fail `validate`. |
| `window` | child block | none | Optional host-tracked event-time watermark declaration (`windows` feature). |

### Config keys per operator kind

| `kind` | Required `config` keys | Optional `config` keys |
|---|---|---|
| `filter` | `where` or `predicate` | |
| `project` | `select` | |
| `group_by` | `aggregates` (or `aggregate`) | `keys` |
| `sort` | `order_by` | `limit`, `offset` |
| `top_n` | `order_by`, `limit` | `offset` |
| `unnest` | `field` | `alias` (or `as`), `preserve_empty` |
| `join` | `on` | `join_type`, `right_prefix` |
| `interval_join` | `on`, `lower_bound_ms`, `upper_bound_ms`, `left_time_field` & `right_time_field` (or `time_field`) | `allowed_lateness_ms` (`watermark_delay_ms`), `max_buffered_rows`, `right_prefix` |
| `lookup_join` | `on` | `join_type`, `right_prefix` |
| `deduplicate` | `keys` | `keep`, `time_field`, `ttl_ms`, `max_keys`, `stateful` |
| `sql` | `query` | |
