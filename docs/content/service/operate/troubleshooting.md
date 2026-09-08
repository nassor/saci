+++
title = "When it refuses to start, and when it fails"
description = "The four load-time gates, the keys it does not know, and what a running process does with an error."
template = "page.html"
weight = 8
+++
# When it refuses to start, and when it fails

`saci-service` refuses to start on anything it can detect before the first row
moves, and once it is running it reports a failure rather than stopping. Each
message below names the line to change.

Four checks run in a fixed order, and each one is a refusal rather than a
warning. Nothing about your processor is touched until gate 2.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 452" role="img" aria-labelledby="svc-g-title svc-g-desc">
        <title id="svc-g-title">The four load-time gates a saci-service start must pass</title>
        <desc id="svc-g-desc">
            Four gates run in order. First the config file is read, environment placeholders are
            substituted, and the document is parsed strictly and cross-validated, which is where
            the graph rules on ids and links are enforced and where a config naming a host this
            binary was not built with is refused by feature name. Second
            the WASM module is read, digest-checked, compiled and instantiated, which is
            where the host matches the WIT world. Third every declared link is checked end
            to end: the components at its two ends must match and their Arrow fields must be
            identical. Fourth, in cluster mode only, the processor's Arrow schema fingerprint
            is compared with the fingerprint recorded in this node's persisted checkpoints.
        </desc>
        <text class="t-title" x="0" y="14">Load order</text>
        <text class="t-sm" x="0" y="30">each gate is a refusal to start, not a warning</text>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="44" width="430" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="44" width="430" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="58" width="430" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="59">1 &nbsp;read the config</text>
            <text class="t-sm" x="12" y="80">read the file, substitute ${VAR}, parse the KDL strictly</text>
            <text class="t-sm" x="12" y="94">then check data_dir, peer ids, the store block, the bind addr,</text>
            <text class="t-sm" x="12" y="108">unique node ids, link endpoints and the absence of a cycle</text>
            <text class="t-lbl t-ctl" x="448" y="59">rejects</text>
            <text class="t-sm" x="448" y="80">a link into a source</text>
            <text class="t-sm" x="448" y="94">a duplicate node id</text>
            <text class="t-sm" x="448" y="108">a store block in cluster mode</text>
        </g>
        <path class="arw arw-bnd" d="M215 120 V132" marker-end="url(#svc-b)"/>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="0" y="136" width="430" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="136" width="430" height="22" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="150" width="430" height="8"/>
            <text class="t-lbl t-bnd" x="12" y="151">2 &nbsp;load the processor</text>
            <text class="t-sm" x="12" y="172">read the module bytes, check the optional sha3_256</text>
            <text class="t-sm" x="12" y="186">compile, then instantiate against saci:pipeline@0.3.0</text>
            <text class="t-sm" x="12" y="200">describe() is called once here, not at the first batch</text>
            <text class="t-lbl t-bnd" x="448" y="151">rejects</text>
            <text class="t-sm" x="448" y="172">a digest mismatch</text>
            <text class="t-sm" x="448" y="186">a missing import</text>
            <text class="t-sm" x="448" y="200">a trap in describe()</text>
        </g>
        <path class="arw arw-ctl" d="M215 212 V224" marker-end="url(#svc-c)"/>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="0" y="228" width="430" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="228" width="430" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="242" width="430" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="243">3 &nbsp;check every link end to end</text>
            <text class="t-sm" x="12" y="264">the components at a link's two ends must match</text>
            <text class="t-sm" x="12" y="278">and their Arrow fields must be identical.</text>
            <text class="t-sm" x="12" y="292">Runs before the workflow is handed to a runner</text>
            <text class="t-lbl t-ctl" x="448" y="243">rejects</text>
            <text class="t-sm" x="448" y="264">sink 'orders_out' reads</text>
            <text class="t-sm" x="448" y="278">'Order', which the</text>
            <text class="t-sm" x="448" y="292">processor never declares</text>
        </g>
        <path class="arw arw-data" d="M215 304 V316" marker-end="url(#svc-d)"/>
        <g class="anim anim-4">
            <rect class="blk blk-data" x="0" y="320" width="430" height="76" rx="8"/>
            <rect class="hd hd-data" x="0" y="320" width="430" height="22" rx="8"/>
            <rect class="hd hd-data" x="0" y="334" width="430" height="8"/>
            <text class="t-lbl t-data" x="12" y="335">4 &nbsp;check the schema fingerprint</text>
            <text class="t-sm" x="12" y="356">the processor's Arrow schema fingerprint against the one</text>
            <text class="t-sm" x="12" y="370">written into this node's persisted checkpoints</text>
            <text class="t-sm" x="12" y="384">cluster mode only, once coordination settles</text>
            <text class="t-lbl t-data" x="448" y="335">rejects</text>
            <text class="t-sm" x="448" y="356">a schema change laid</text>
            <text class="t-sm" x="448" y="370">on top of checkpoints</text>
            <text class="t-sm" x="448" y="384">of the older shape</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 420 H654"/>
            <text class="t-sm" x="0" y="440">Gates 1 to 3 also run under <tspan class="t-ctl">saci-service validate</tspan>. Only <tspan class="t-ctl">serve</tspan> reaches gate 4.</text>
        </g>
        <defs>
            <marker id="svc-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="svc-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
            <marker id="svc-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> state already persisted</span>
        <span class="k-control"><i></i> config and host checks</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
    </div>
    <figcaption class="dgm-cap">
        Gate 3 is the only one that compares two things you wrote: a component name in
        your config against a component name in your processor. It is also the only gate that
        <b>silently passes</b> when a processor declares nothing. An empty component list
        opts that link out of the comparison rather than failing it.
    </figcaption>
</div>

## When it refuses to start

`saci-service validate --config <file>` runs gates 1 to 3 and exits, so all of
these except the last surface in CI without moving any data.

| Message | What to change |
|---|---|
| `error: Configuration error: reading config file saci.kdl: <os error>` | Point `--config` or `SACI_CONFIG` at a file that exists. |
| `node.data_dir must not be empty` | Set `data_dir` on the `node` line; it is required even when nothing writes there. |
| `invalid variable name "out dir": use [A-Za-z0-9_]` | Rename the entry in the `variables` block to letters, digits and `_`. |
| `parsing KDL: 109:18: Expected quoted string` followed by `note: ${OUT_DIR} holds a backslash` | Rewrite that variable's path with forward slashes; a backslash escapes inside the quoted string: [The config file](@/service/config/_index.md). |
| ``mode "cluster" does not take a `store` block: cluster state lives in node.data_dir`` | Delete the `store` block: [Running a cluster](@/service/operate/cluster.md). |
| `lease_ttl_ms (1000) must be >= 3 × election_timeout_ms (1000) = 3000` | Raise the lease, or lower the election timeout. |
| `store redb: path must not be empty` | Give the `store "redb"` block a real `path`. |
| `unknown store kind 'sqlite' (expected "redb")` | Change the store's leading argument to `"redb"`. |
| `source type 'tcp' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | Set `run_mode kind="stream"`: [Run modes and persistence](@/service/config/run-modes.md). |
| `workflow 'orders': links contain a cycle` | Remove one edge; every graph rule is on [Workflows and links](@/service/config/workflows.md). |
| `workflow 'orders': link 'enrich' -> 'orders_out': processor 'enrich' does not declare component 'Order', which sink 'orders_out' reads` | Correct the `component` name on the node, or declare that component in the processor. |
| `workflow 'orders': link 'enrich' -> 'orders_out': component 'Order' schema differs between processor 'enrich' and sink 'orders_out'` | Make the two ends agree field by field, including nullability and order. |
| `schema fingerprint mismatch: the pipeline declares 4f2a1c08 but this node's persisted checkpoints were written with 91bd3e77.` | Restore the previous processor, or start the node with an empty `data_dir`. |
| A digest mismatch on `sha3_256` | Recompute the digest of the artifact you are actually shipping, or drop the key. |
| `mode "cluster"` in a binary without cluster support | Rebuild with `--features service-cluster`: [Install saci-service](@/service/install.md). |
| A `wasm` or `plugin` node in a binary without that host | Rebuild with the `--features` flag the message names, `wasm` or `plugin`: [Install saci-service](@/service/install.md). |
| A `window` block on either, in a binary built without `windows` | Rebuild with `--features windows`: [Install saci-service](@/service/install.md). |
| `type="NatsSource"`, or any connector, in a binary built without it | Rebuild with the `--features` flag the message names: [Install saci-service](@/service/install.md). |
| `format="csv"`, or any format, on a `transformer` node in a binary built without it | Rebuild with the `--features` flag the message names: [Install saci-service](@/service/install.md). |

A `type` the registry cannot resolve is one of two things, and `validate`
separates them.

A connector this binary ships but was not built with is a refusal. Nothing
registered at serve time can supply it, so it fails in every mode and names
the feature that carries it:

```text
ERROR: no source factory registered for type 'NatsSource' (required by source 'nats_orders'): that is a built-in connector this binary was built without, so rebuild or reinstall with `--features connector-nats`, or register your own factory under that name
error: Configuration error: 1 declared type(s) name a built-in connector this binary was built without. Rebuild or reinstall with the feature named above.
```

A `type` no build of the binary provides may be a factory you register
yourself, so it stays a warning and the process exits 0:

```text
WARNING: no sink factory registered for type 'ClickHouseSink' (required by sink 'orders_out')
NOTE: 1 unknown type(s) above are not in the built-in registry. They may be
user-defined types registered at serve time. Use --strict to treat these as errors.
```

`--strict` turns that second kind into a failure and has no effect on the
first. Which check raises which message is
[What the loader validates](@/library/service/loading.md).

A `format` a `transformer` node names has no second kind. It is a refusal in
every `validate` mode and at serve time, since the format is either compiled
into this binary or registered by the embedder driving it. The message lists
what the binary does carry, and names the feature when the format is one this
crate ships:

```text
error: Configuration error: transformer 'csv_fmt' names format 'csv', which no transformer is registered for (registered: none): that is a built-in format this binary was built without, so rebuild or reinstall with `--features transformer-csv`, or register your own transformer under that name
```

A build capability has no second kind either, and nothing registered at serve
time can supply one. `mode "cluster"` needs the Raft stack, a `wasm` node the
wasmtime host, a `plugin` node the native plugin host, and a `window` block on
either the windowing engine. All four parse in every build, deliberately, so
the refusal can name the flag that would run them rather than reporting the
key or the mode as unknown. `validate` refuses first, ahead of
`--connectors-only`, and `serve` refuses before it binds a port:

```text
error: Configuration error: workflow 'orders': plugin node 'audit' needs the native plugin host, which this binary was built without, so rebuild or reinstall with `--features plugin`
```

`plugin` is not in the default bundle, so that is the one a
`cargo install saci-service` binary meets. `wasm` and `windows` are, so only a
build that trimmed them meets the other two.

A node's own host is answered first. A `window` block on a `wasm` node in a
binary carrying neither names `--features wasm`, and the window refusal
follows once that host is in:

```text
error: Configuration error: workflow 'orders': the `window` block on wasm node 'aggregate' needs the windowing engine, which this binary was built without, so rebuild or reinstall with `--features windows`
```

Geometry comes before both. `slide_ms` larger than `size_ms` is reported as an
invalid window in every build, because a bad geometry is a defect in the file
whichever binary reads it.

## A key it does not know

A misspelled key inside a `workflow` block is an error that names it, with one
exception below. Every node kind rejects a key it cannot honour: a `wasm` node
with a `watch` property, or a `sink` with a `truncat` key, fails the parse
instead of ignoring it. The same holds for `transformer`, `source`, `sink`,
`plugin`, `retry`, `heal`, `window` and `flow_control`.

A node kind is never one of those keys, and neither is `window`. Both node
kinds and the block they carry are part of the schema whichever hosts and
engine this binary was built with, so declaring one is never reported as an
unknown key: it is the capability refusal above.

Two kinds of place are looser. A key at the top level, or inside `node`,
`peer`, `http`, `observability` or a `link`, is accepted and ignored, so a
typo in one of those changes nothing silently. And a key inside a `config`
block belongs to the connector, which decides for itself whether it is an
error. The keys each one accepts are on its own page under
[Sources and sinks](@/service/connectors/_index.md).

## Failures while running

Errors do not stop the loop. A source failure stops draining that source for
this pass; a processor failure skips that processor's fan-out, so nothing
downstream of it is fed. Every one of them logs and increments
`iteration_errors`, which `/status` reports.

| Condition | What happens |
|---|---|
| A source fails to read | That source stops draining for this pass; the pass continues with the others |
| A processor call fails | That processor's downstream nodes are fed nothing this pass, and `saci_workflow_errors_total` increments |
| More than 128 processor calls run at once | The 129th fails to instantiate with `processor trap (instantiate): maximum concurrent limit of 128 for component instances reached`, and that batch is treated as failed |
| A sink fails to write | The write is retried with backoff first; the error reaches the runner only after the last attempt |
| The next chunk would exceed `max_chunk_bytes` | The source's admission target is divided: [Flow control](@/service/operate/flow-control.md) |
| The main loop wedges | `/health` returns 503 within 5 seconds |
| Coordination does not settle within 30 s | `serve` exits 1 |
| Shutdown exceeds the 30 s budget | The process exits 1 instead of 0 |

## Where to look

Three places answer "what went wrong", in increasing detail:

- `iteration_errors` on `/status`, per workflow, is the fastest yes or no. It
  counts every failure below, source, processor and sink alike.
- `saci_workflow_errors_total` on `/metrics`, under `workflow="<id>"`, is the
  narrower signal for an alert: a processor call that failed, and in cluster
  mode a claim the node could not renew. A source or sink failure never
  reaches it, so pair it with `iteration_errors`:
  [Logs, metrics and traces](@/service/operate/observability.md).
- The Logs tab of the dashboard is where a failure is triaged, because every
  error record carries `workflow`, the `iteration` it happened on, and the
  failing node's own field: [The live dashboard](@/service/operate/dashboard.md).

Linux/macOS:

```bash,name=Is anything failing right now
curl -s http://localhost:8080/status | jq '.standalone[] | {workflow_id, iterations, iteration_errors}'

{
  "workflow_id": "orders",
  "iterations": 41,
  "iteration_errors": 0
}
```

Windows (PowerShell):

```powershell
curl.exe -s http://localhost:8080/status | ConvertFrom-Json |
  Select-Object -ExpandProperty standalone |
  Select-Object workflow_id, iterations, iteration_errors
```

## Next

- [The command line](@/service/operate/_index.md) is the flag or variable that
  changes the config a failing process read.
- [The live dashboard](@/service/operate/dashboard.md) shows the failing node in
  the graph rather than in a log line.
