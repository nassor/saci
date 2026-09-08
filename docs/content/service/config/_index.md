+++
title = "The config file"
description = "One KDL document describes the whole process: nine top-level keys, the variables they can reference, and the command that proves the file."
template = "section.html"
sort_by = "weight"
weight = 3
aliases = ["/service/configuration/"]
+++
One KDL document describes the whole process. The file `saci-service validate`
accepts is the file `serve` runs.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 168" role="img" aria-labelledby="svc-cfg-t svc-cfg-d">
        <title id="svc-cfg-t">One config file feeds validate, and the same file feeds serve</title>
        <desc id="svc-cfg-d">
            The file saci.kdl carries nine top-level keys. Running saci-service validate
            against it parses the document, substitutes the environment placeholders and
            checks the workflow graph end to end, then exits, printing OK lines. Running
            saci-service serve against the same file does the same checks and then starts
            the workflow, which runs until it is stopped. A rejected file never reaches
            serve.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="40" width="150" height="70" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="40" width="150" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="54" width="150" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="55">saci.kdl</text>
            <text class="t-sm" x="12" y="80">nine top-level keys</text>
            <text class="t-sm" x="12" y="98">one or more workflows</text>
            <path class="arw arw-ctl" d="M150 74 H186" marker-end="url(#svc-cfg-c)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="190" y="40" width="200" height="70" rx="8"/>
            <rect class="hd hd-ctl" x="190" y="40" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="190" y="54" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="202" y="55">saci-service validate</text>
            <text class="t-sm" x="202" y="80">parse, substitute, check the</text>
            <text class="t-sm" x="202" y="98">graph, then exit</text>
            <path class="arw arw-ctl" d="M390 74 H426" marker-end="url(#svc-cfg-c)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="430" y="40" width="200" height="70" rx="8"/>
            <rect class="hd hd-ctl" x="430" y="40" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="430" y="54" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="442" y="55">saci-service serve</text>
            <text class="t-sm" x="442" y="80">the same checks, then the</text>
            <text class="t-sm" x="442" y="98">workflow runs until stopped</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 132 H654"/>
            <text class="t-sm" x="0" y="154">A file <tspan class="t-ctl">validate</tspan> rejects never starts under <tspan class="t-ctl">serve</tspan>. Nothing is half applied.</text>
        </g>
        <defs>
            <marker id="svc-cfg-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> the config file and the two commands that read it</span>
    </div>
</div>

## 1. Start from the skeleton

`mode`, `node` and `workflow` are the three required keys. Everything else has
a working default, so this is a complete file:

```kdl,name=The top level of a service config
mode "standalone"

// id is a u64 and stable across restarts. data_dir must be non-empty; only the
// cluster runner writes there. id and bootstrap accept a quoted string that
// parses as the value, so env substitution stays valid KDL.
node id=1 name="saci-1" data_dir="/var/lib/saci/node-1"

// continuous | one_shot | interval | stream
run_mode kind="continuous"

// The leading argument is the workflow id. Every node lives inside this block.
workflow "orders" name="Orders" {
    // transformer, source, wasm, plugin, sink and link nodes
}

// Names the file can reference. Wins over a same-named env var.
variables {
    OUT_DIR "/data/out"
}

http bind="0.0.0.0:8080" disabled=#false control=#true

observability log_format="pretty" log_level="error"
```

`workflow` may repeat. Standalone runs every declared block; cluster mode takes
exactly one. Fill one in with
[Workflows and links](@/service/config/workflows.md).

## 2. The nine top-level keys

| Key | Holds | Default |
|---|---|---|
| `mode` | which runner: `"standalone"` or `"cluster"`; see [Running a cluster](@/service/operate/cluster.md) | required |
| `node` | `id`, optional `name`, `data_dir`, plus the cluster keys beside it; see [Running a cluster](@/service/operate/cluster.md) | required |
| `run_mode` | how a standalone run paces itself; see [Run modes and persistence](@/service/config/run-modes.md) | `kind="continuous"` |
| `workflow` | the graph: transformers, sources, processors, sinks, links; see [Workflows and links](@/service/config/workflows.md) | required |
| `store` | standalone persistence, a `store "redb"` block; see [Run modes and persistence](@/service/config/run-modes.md) | none |
| `http` | the control-plane address, `disabled`, and `control`; see [Logs, metrics and traces](@/service/operate/observability.md) and [Workflow lifecycle](@/service/operate/workflows.md) | `bind="0.0.0.0:8080"`, control on |
| `observability` | log format and level, sampling, OTLP export, the in-process capture behind the dashboard; see [Logs, metrics and traces](@/service/operate/observability.md) | pretty at `error`, capture on |
| `flow_control` | adaptive admission pacing and chunk sizing for every source; see [Flow control](@/service/operate/flow-control.md) | on, at hard-coded defaults |
| `variables` | names usable as `${name}` anywhere in the file; step 3 below | none |

Which `type` strings a source or a sink may name, and which formats each one
carries, is the matrix on
[Sources and sinks](@/service/connectors/_index.md).

The `observability` key `log_level` changes what you can see and nothing
about what runs. The default `log_level="error"` materialises no span.
`log_level="info"` fills the dashboard's Traces tab with
`pipeline.run`-rooted traces from a native pipeline, and a workflow of
WebAssembly or plugin processors needs `"debug"`, which adds the per-item
trees. [The live dashboard](@/service/operate/dashboard.md) is where that
lands.

## 3. Variables and the environment

`${VAR}` placeholders are substituted before the parse. `${VAR}` is replaced
with the value of the environment variable `VAR`, and `${VAR:-default}` falls
back to `default` when `VAR` is unset. A bare `${VAR}` that is unset is an
error naming it, so a missing variable never becomes an empty string.

A top-level `variables` block declares names of its own. A declared name wins
over a same-named process environment variable, and the environment stays the
fallback for undeclared names, so the file can reference its own declarations
with the same `${name}` syntax. Names are restricted to `[A-Za-z0-9_]`.

```kdl,name=A file that reads its own declarations and the environment
variables {
    OUT_DIR "/data/out"
}

node id=1 data_dir="${SACI_DATA_DIR:-/var/lib/saci}"

workflow "orders" {
    sink "orders_out" type="FileSink" component="Order" transformer="orders_csv" {
        config path="${OUT_DIR}/orders.csv"
    }
}
```

Run it with the environment the file expects:

Linux/macOS:

```bash
export SACI_DATA_DIR=/srv/saci
saci-service validate --config service.kdl
```

Windows (PowerShell):

```powershell
$env:SACI_DATA_DIR = "/srv/saci"
saci-service validate --config service.kdl
```

The Windows path there is written with forward slashes on purpose. Substitution
is textual and runs before the parser, so the value lands inside a quoted KDL
string, where a backslash opens an escape sequence: a value like
`C:\Users\me\out` makes the parser reject the line with
`parsing KDL: 109:18: Expected quoted string`, followed by a note naming the
variable that carried the backslash. Give every path-valued variable forward
slashes, which Windows accepts in a path anyway.

## 4. Validate it

`validate` proves the file and touches nothing else: it parses the document,
substitutes the placeholders, loads every declared processor artifact, and walks
every link checking that both ends agree on their components and their fields.
Then it exits.

```bash,name=Validate a config and what it prints
saci-service validate --config service.kdl

OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
  node.id:  1
  node.name: saci-1
  mode:     standalone
  workflow: orders
  processors: pipelines/orders.wasm
  sources:  1
  sinks:    1
  http.bind: 0.0.0.0:8080
  log_level: info
OK: all declared types resolved in built-in registry
```

Runs the same on Linux, macOS and Windows (PowerShell). Two flags change what
it accepts:

- `--strict` promotes a warning into a failure. A `type` no build of this
  binary provides is a warning that still exits 0, because a config aimed at a
  custom binary names factories this one does not know. A connector this
  binary ships but was built without is an error instead, in every mode, and
  `--strict` does not reach it.
- `--connectors-only` builds every source, sink and transformer and skips the
  processor load and the graph check, so it validates a config whose processor
  artifact is not present yet.

A `FileSink` opens its output file while its factory runs, `validate` included,
so the parent directory must exist first. The file is created when it is
missing and appended to when it is not. `truncate #true` in the sink's
`config` replaces it instead.

What each refusal means, and what to change, is on
[When it refuses to start, and when it fails](@/service/operate/troubleshooting.md).

## Next

- [Workflows and links](@/service/config/workflows.md) fills in the `workflow`
  block: six node kinds and the links between them.
- [Sources and sinks](@/service/connectors/_index.md) is the `type` string on
  every source and sink, one page per system.
