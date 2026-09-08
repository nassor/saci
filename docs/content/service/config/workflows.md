+++
title = "Workflows and links"
description = "Six node kinds inside one workflow block, the links that wire them, and every key each kind takes."
template = "page.html"
weight = 1
+++
# Workflows and links

A `workflow` block is one graph. You declare the nodes, then you declare the
edges. Nothing is connected until a `link` says so. The example below reads
rows, hands them to a processor, and writes the result, with a second sink fed
only by a named branch.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 210" role="img" aria-labelledby="svc-wf-t svc-wf-d">
        <title id="svc-wf-t">Two links wire a source to a processor to a sink, and a labelled link adds a second sink</title>
        <desc id="svc-wf-d">
            A source node on the left is joined by a link to a wasm processor node in the
            middle, drawn on the WebAssembly boundary. A second link joins the processor to
            a sink node on the right. A third link, carrying the branch name rejected,
            leaves the same processor and reaches a second sink below the first. The
            processor decides per batch which of its labelled links the batch reaches, so
            the lower sink only receives what the processor routes to that branch.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="52" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="64" width="130" height="8"/>
            <text class="t-lbl t-data" x="12" y="67">source</text>
            <text class="t-sm" x="12" y="90">orders_in</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M130 80 H228" marker-end="url(#svc-wf-a)"/>
            <text class="t-sm" x="146" y="72">link from to</text>
            <rect class="blk blk-bnd" x="232" y="52" width="150" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="232" y="52" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="232" y="64" width="150" height="8"/>
            <text class="t-lbl t-bnd" x="244" y="67">wasm</text>
            <text class="t-sm" x="244" y="90">enrich</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M382 80 H500" marker-end="url(#svc-wf-a)"/>
            <text class="t-sm" x="398" y="72">link from to</text>
            <rect class="blk blk-data" x="504" y="52" width="150" height="56" rx="8"/>
            <rect class="hd hd-data" x="504" y="52" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="504" y="64" width="150" height="8"/>
            <text class="t-lbl t-data" x="516" y="67">sink</text>
            <text class="t-sm" x="516" y="90">orders_out</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M307 108 V148 H500" marker-end="url(#svc-wf-a)"/>
            <text class="t-sm" x="318" y="140">link branch=&quot;rejected&quot;</text>
            <rect class="blk blk-data" x="504" y="122" width="150" height="52" rx="8"/>
            <rect class="hd hd-data" x="504" y="122" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="504" y="134" width="150" height="8"/>
            <text class="t-lbl t-data" x="516" y="137">sink</text>
            <text class="t-sm" x="516" y="160">rejected_out</text>
            <path class="ln" d="M0 190 H654"/>
            <text class="t-sm" x="0" y="206">A labelled link starts at a processor, and the processor picks which labels a batch reaches.</text>
        </g>
        <defs>
            <marker id="svc-wf-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
    </div>
</div>

## 1. Declare the nodes

Six node kinds live inside `workflow`. Each but `link` opens with its id as the
leading argument, unique across the whole process. Every node id, workflow id
and branch name matches `^[A-Za-z0-9][A-Za-z0-9_-]*$` and is at most 64 bytes.
Five of the six are in the default binary. A `plugin` node needs one built with
`--features plugin`: the default build parses the file and then refuses it,
naming that flag.

| Node | Declares | Links |
|---|---|---|
| `transformer` | `format`, an optional `options` child | none: a source or sink names it |
| `source` | `type`, `component`, optional `transformer`, optional `retry`, a `config` child | outbound only |
| `wasm` | optional `module`, optional `sha3_256`, `config` and `window` children | inbound and outbound |
| `plugin` | optional `library`, otherwise the same as `wasm`; needs a build with `--features plugin` | inbound and outbound |
| `sink` | `type`, `component`, optional `transformer`, optional `retry`, a `config` child | inbound only |
| `link` | `from`, `to`, optional `branch` | it is the edge |

One source, one processor, one sink is the whole shape:

```kdl,name=A workflow with one source one processor and one sink
workflow "orders" name="Orders" {
    // A declared byte format. Each source and sink that moves bytes names one.
    transformer "orders_parquet" format="parquet"

    // type is the factory lookup key. component names the rows this node
    // produces, and the processor it feeds must declare that component.
    source "orders_in" type="FileSource" component="Order" transformer="orders_parquet" {
        // config is opaque: the connector reads it verbatim.
        config path="/data/in/orders.parquet"
    }

    // A wasm32-wasip2 component implementing the SACI world.
    wasm "enrich" name="Order enrichment" module="pipelines/orders.wasm" {
        // Optional integrity check, verified before the component is compiled.
        // A leading "sha3-256:" prefix is accepted.
        // sha3_256="6f1c...c4"

        // Strings the processor reads back at run time. It parses numerics
        // itself.
        config fx_eur="1.08" batch_size="500"
    }

    sink "orders_out" type="FileSink" component="Order" transformer="orders_parquet" {
        // schema_fields is the output schema, where the format needs one.
        config path="/data/out/orders.parquet" {
            schema_fields "id" type="Int64" nullable=#false
            schema_fields "usd_amount" type="Float64" nullable=#false
        }
    }

    // Declaration order wires nothing. Only a link connects two nodes.
    link from="orders_in" to="enrich"
    link from="enrich" to="orders_out"
}
```

`validate` accepts that file and reports its shape:

```text
OK: workflow graph validated (components and schemas agree end to end)
  workflow: orders
  processors: pipelines/orders.wasm
  sources:  1
  sinks:    1
```

## 2. Connect them with links

A `link` names two declared node ids. `from` is the upstream node and `to` the
downstream one. A transformer id is never either of them: a transformer is a
format that a source or a sink names, and it holds no place in the graph.

```kdl,name=Two edges make the graph
link from="orders_in" to="enrich"
link from="enrich" to="orders_out"
```

`component` is what the two ends have to agree on. A source produces the
component it names, and the processor it links into must declare that same
component; a sink reads the component it names, and its upstream processor must
declare it too. The check runs at load time, so a stale name fails `validate`
rather than the first batch.

An optional `branch` on a link labels it. The upstream processor then chooses
per batch which labels its output reaches, and a label the processor did not
choose delivers nothing: [Branching](@/service/processors/branching.md).

## 3. Chain processors

Two processors linked in sequence hand Arrow batches straight across in
memory, with no re-encoding between them.

```kdl,name=Two processors in sequence
link from="orders_in" to="enrich"
link from="enrich" to="settle"
link from="settle" to="orders_out"
```

The upstream must declare every component the downstream declares. A
disagreement fails with an error naming the link, so `validate` catches it:

```text
workflow 'orders': link 'enrich' -> 'settle': processor 'settle' declares component 'Fee',
which upstream processor 'enrich' does not; a processor-to-processor link must
deliver every component the downstream processor declares
```

## 4. Retry connector operations

Every `sink` retries a failed write with exponential backoff before the error
reaches the runner: 4 attempts, a 100 ms base delay, 2.0x growth, a 30 s cap
and 0.1 jitter. A `source` takes the same wrapper everywhere except
`run_mode kind="stream"`, where the stream runner's own re-poll already is the
retry loop. EOF from a source is not an error and is not retried.

An optional `retry` child on a `source` or `sink` overrides the policy for that
node. `max_attempts=1` disables retrying.

```kdl,name=A source that retries with a longer backoff
source "orders_in" type="HttpSource" component="Order" transformer="orders_csv" {
    retry max_attempts=8 base_delay_ms=500 multiplier=2.0 max_delay_ms=30000 jitter=0.1
    config url="https://api.example.com/orders.csv"
}
```

The wrapper retries every error, connection failures included. A connector's
own reconnect settings, such as the PostgreSQL `connection.reconnect` block,
compose beneath it: the connector re-establishes the session, and if it still
fails the wrapper retries the operation. `retry` must sit beside `config`,
never inside it, because each connector's `config` is strict about its own
keys.

When retrying has given up and the connector itself is broken, the host
replaces it with a fresh one built from the same `config`. That is a separate
`heal` child beside `retry`, on by default, and covered by
[Self-healing](@/service/operate/self-healing.md).

A batch the sink refuses after all of that is logged and dropped. A `dlq`
child on the `workflow` stores it instead and replays it when the sink starts
accepting again; see
[Dead letter queue](@/service/operate/dead-letter-queue.md).

## 5. Declare several workflows

`workflow` may repeat, and a standalone run walks every declared block. Node
ids stay unique across all of them, and two workflows in one process can be
joined by an in-process channel.

[Several workflows in one process](@/service/processors/multiple-workflows.md)
is the runnable example.

## Every key

### workflow

| Key | Type | Default | What it does |
|---|---|---|---|
| leading argument | string | required | the workflow id, unique across the process |
| `name` | string | the id | display name on the dashboard |
| `dlq` | child block | none | this workflow's dead letter queue; see [Dead letter queue](@/service/operate/dead-letter-queue.md) |

### transformer

| Key | Type | Default | What it does |
|---|---|---|---|
| leading argument | string | required | the transformer id a source or sink names |
| `name` | string | the id | display name on the dashboard |
| `format` | string | required | `csv`, `ndjson`, `parquet`, `avro` or `arrow-ipc`; see [Formats](@/service/formats/_index.md) |
| `options` | child block | empty | handed to that format verbatim; the keys are on the format's own page |

### source

| Key | Type | Default | What it does |
|---|---|---|---|
| leading argument | string | required | the source id |
| `name` | string | the id | display name on the dashboard |
| `type` | string | required | which connector to build; see [Sources and sinks](@/service/connectors/_index.md) |
| `component` | string | required | the rows this source produces, which its downstream processor must declare |
| `transformer` | string | none | id of the `transformer` that decodes its bytes; omitted by a connector that carries rows already |
| `retry` | child block | 4 attempts | per-node retry policy, below |
| `flow_control` | child block | the top-level policy | admission override for this source; see [Flow control](@/service/operate/flow-control.md) |
| `config` | child block | empty | read verbatim by the connector; the keys are on the connector's own page |

### sink

| Key | Type | Default | What it does |
|---|---|---|---|
| leading argument | string | required | the sink id |
| `name` | string | the id | display name on the dashboard |
| `type` | string | required | which connector to build; see [Sources and sinks](@/service/connectors/_index.md) |
| `component` | string | required | the rows this sink reads, which its upstream processor must declare |
| `transformer` | string | none | id of the `transformer` that encodes its bytes; omitted by a connector that takes rows directly |
| `retry` | child block | 4 attempts | per-node retry policy, below |
| `config` | child block | empty | read verbatim by the connector; the keys are on the connector's own page |

### wasm and plugin

| Key | Type | Default | What it does |
|---|---|---|---|
| leading argument | string | required | the processor node id |
| `name` | string | the id | display name on the dashboard |
| `module` | string | none | the `.wasm` component file, on a `wasm` node |
| `library` | string | none | the shared library file, on a `plugin` node |
| `sha3_256` | string | none | expected SHA3-256 hex digest of those bytes, checked before the artifact loads; a `sha3-256:` prefix is accepted |
| `config` | child block | empty | strings the processor reads back at run time |
| `window` | child block | none | event-time geometry; see [Windowing](@/service/processors/windowing/_index.md) |

A node with no `module` and no `library` takes a processor supplied from code
instead: [Embedding saci-service](@/library/service/_index.md).

### link

| Key | Type | Default | What it does |
|---|---|---|---|
| `from` | string | required | the upstream node id |
| `to` | string | required | the downstream node id |
| `branch` | string | none | labels the edge, so the upstream processor can choose it per batch |

### retry

| Key | Type | Default | What it does |
|---|---|---|---|
| `max_attempts` | integer | 4 | total attempts including the first; `1` disables retrying |
| `base_delay_ms` | integer | 100 | delay before the second attempt |
| `multiplier` | number | 2.0 | growth factor per attempt, at least 1.0 |
| `max_delay_ms` | integer | 30000 | ceiling on the computed delay |
| `jitter` | number | 0.1 | fraction of the delay randomised, 0.0 to 1.0 |

## When it refuses to start

Each of these is printed and the process exits. None is a warning. `'orders'`
is the workflow id in the message, and the quoted node ids are yours.

| Message | What to change |
|---|---|
| `workflow 'orders': declares no source, processor or sink node` | Declare at least one node inside the block. |
| `workflow 'orders': source id 'bad/id' is invalid; ids must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes` | Rename the node to letters, digits, `_` and `-`, starting with a letter or digit. |
| `workflow 'orders': id 'shared' is declared twice, as source and as sink` | Give the two nodes different ids; every kind shares one namespace. |
| `workflow 'orders': source 'orders_in' names transformer 'orders_csv', which is not declared` | Declare that `transformer` node, or correct the id the source names. |
| `workflow 'orders': link 'enrich' -> 'enrich' links a node to itself` | Point `to` at a different node. |
| `workflow 'orders': link names undeclared node 'enrich2'` | Correct the id, or declare the node the link names. |
| `workflow 'orders': link 'orders_in' -> 'enrich' is declared twice` | Delete the duplicate `link`. |
| `workflow 'orders': link 'enrich' -> 'orders_in' targets source 'orders_in'; a source has no input` | Reverse the link; a source is only ever a `from`. |
| `workflow 'orders': link 'orders_out' -> 'enrich' starts at sink 'orders_out'; a sink has no output` | Reverse the link; a sink is only ever a `to`. |
| `workflow 'orders': links contain a cycle` | Remove one edge; the graph has to be acyclic. |
| `workflow 'orders': source 'orders_in' has no outbound link` | Add a `link` out of that source, or delete it. |
| `workflow 'orders': sink 'orders_out' has no inbound link` | Add a `link` into that sink, or delete it. |
| `workflow 'orders': link 'enrich' -> 'orders_out' branch 'bad/branch' is invalid; branches must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes` | Rename the branch to the same charset as an id. |
| `workflow 'orders' source 'orders_in': retry.max_attempts must be at least 1` | Set `max_attempts` to 1 or more; `1` is how you disable retrying. |
| `workflow id 'orders' is declared twice` | Rename one of the two `workflow` blocks. |
| `node id 'enrich' is declared in workflow 'a' as processor and in workflow 'b' as processor; node ids must be unique across all workflows` | Rename one node; ids are unique across the whole file. |

## Next

- [Run modes and persistence](@/service/config/run-modes.md) decides how often
  the graph you just declared runs.
- [Processors in a workflow](@/service/processors/_index.md) fills in the
  `wasm` node the two links point at.
