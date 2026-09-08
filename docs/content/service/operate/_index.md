+++
title = "The command line"
description = "Every subcommand, the flags and environment variables they read, the shipped configs to start from, and the exit codes."
template = "section.html"
sort_by = "weight"
weight = 8
aliases = ["/operations/", "/operations/running-saci/"]
+++
`saci-service` has four subcommands, five global flags and seven environment
variables.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 200" role="img" aria-labelledby="svc-cli-t svc-cli-d">
        <title id="svc-cli-t">serve reads one config file and answers on five HTTP routes</title>
        <desc id="svc-cli-d">
            The config file saci.kdl feeds saci-service serve, drawn as a control-plane box.
            The running process answers on five routes: health for liveness, ready for
            readiness, status for the per-workflow counters, metrics for the Prometheus
            exposition, and ui for the dashboard. All five share the one address the http
            block binds.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="66" width="120" height="56" rx="8"/>
            <text class="t-lbl t-ctl" x="12" y="88">saci.kdl</text>
            <text class="t-sm" x="12" y="106">or SACI_CONFIG</text>
            <path class="arw arw-ctl" d="M120 94 H156" marker-end="url(#svc-cli-c)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="160" y="52" width="200" height="84" rx="8"/>
            <rect class="hd hd-ctl" x="160" y="52" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="160" y="66" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="172" y="67">saci-service serve</text>
            <text class="t-sm" x="172" y="94">binds the address in</text>
            <text class="t-sm" x="172" y="110">the http block</text>
            <text class="t-sm" x="172" y="126">0.0.0.0:8080 by default</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-ctl" d="M360 94 H396" marker-end="url(#svc-cli-c)"/>
            <rect class="blk blk-ctl" x="400" y="12" width="150" height="28" rx="6"/>
            <text class="t-sm t-ctl" x="412" y="31">GET /health</text>
            <rect class="blk blk-ctl" x="400" y="46" width="150" height="28" rx="6"/>
            <text class="t-sm t-ctl" x="412" y="65">GET /ready</text>
            <rect class="blk blk-ctl" x="400" y="80" width="150" height="28" rx="6"/>
            <text class="t-sm t-ctl" x="412" y="99">GET /status</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="400" y="114" width="150" height="28" rx="6"/>
            <text class="t-sm t-ctl" x="412" y="133">GET /metrics</text>
            <rect class="blk blk-ctl" x="400" y="148" width="150" height="28" rx="6"/>
            <text class="t-sm t-ctl" x="412" y="167">GET /ui</text>
            <text class="t-sm" x="562" y="31">alert on this one</text>
            <text class="t-sm" x="562" y="65">process, not workflow</text>
            <text class="t-sm" x="562" y="99">per-workflow counters</text>
            <text class="t-sm" x="562" y="133">Prometheus text</text>
            <text class="t-sm" x="562" y="167">the dashboard</text>
        </g>
        <defs>
            <marker id="svc-cli-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> the control plane</span>
    </div>
</div>

## 1. The subcommands

| Command | What it does |
|---|---|
| `serve` | Start and keep running. `--node-id N` overrides `node.id`; `--port P` overrides the port in `http.bind`, and `--port 0` prints the address the OS assigned on stdout. |
| `validate` | Check the config and exit without moving anything. A declared `type` that is a connector this binary was built without fails, naming the feature to rebuild with. `--strict` also turns a warning about a type no crate here provides into a failure. `--connectors-only` builds every source, sink and transformer instead of loading the processor and checking the graph, so a config whose processor artifact is missing still validates. |
| `status --addr URL` | One summary line from `/status`. `--full` prints the whole JSON document. |
| `cluster init` | Pre-flight only: confirms `mode "cluster"` and `bootstrap #true`, then tells you to run `serve`. It starts nothing. |
| `cluster status --addr URL` | The `cluster` field of `/status`. |
| `cluster join --leader URL`, `cluster leave` | Membership is manual. Both print the procedure, editing the `peer` nodes on every node and restarting, and exit 0. Every `cluster` subcommand needs `--features service-cluster`; without it the command exits 1. |

Start with `validate`: it is the one command that touches nothing.

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service validate --config examples/configs/standalone.kdl
```

```text
OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
```

## 2. Global flags and environment variables

Five flags work on every subcommand: `--config`/`-c`, `--addr`,
`--log-format`, `--log-level` and `--otlp-endpoint`. `--config` defaults to
`saci.kdl` in the current directory, so `serve` in a directory holding that file
needs no flags at all. Every flag below has an environment variable, and the
flag wins when both are set.

| Variable | Equivalent flag | What it sets |
|---|---|---|
| `SACI_CONFIG` | `-c`, `--config` | Config file path |
| `SACI_NODE_ID` | `--node-id` | Node id override, on `serve` |
| `SACI_HTTP_PORT` | `--port` | HTTP port override, on `serve` |
| `SACI_ADDR` | `--addr` | Control-plane address, on `status` and `cluster` |
| `SACI_LOG_FORMAT` | `--log-format` | `pretty` or `json` |
| `SACI_LOG_LEVEL` | `--log-level` | The log filter |
| `SACI_OTLP_ENDPOINT` | `--otlp-endpoint` | OTLP/HTTP collector root for span export |

The last three override the `observability` block:
[Logs, metrics and traces](@/service/operate/observability.md).

Linux/macOS:

```bash
export SACI_CONFIG=/etc/saci/orders.kdl
saci-service serve
```

Windows (PowerShell):

```powershell
$env:SACI_CONFIG = "C:\saci\orders.kdl"
saci-service serve
```

The config file reads the environment too, through `${VAR}` and
`${VAR:-default}` placeholders and a `variables` block that wins over it:
[The config file](@/service/config/_index.md).

## 3. Pick a starting config

The repository ships runnable configs under `examples/configs/`. Each names one
system, so copy the one nearest your own and edit its `config` blocks.

| Config | What it runs | In the default build |
|---|---|---|
| `standalone.kdl` | CSV in, one processor, CSV out: [File](@/service/connectors/file.md) | yes |
| `standalone_wasm.kdl` | the order-processing component over CSV: [File](@/service/connectors/file.md) | yes |
| `standalone_plugin.kdl` | the same shape with a native plugin: [Plugins in a workflow](@/service/plugins/_index.md) | no: `--features plugin` |
| `standalone_polyglot.kdl` | a processor built from another language: [Build your own processor](@/service/processors/build/_index.md) | yes, once the component is built |
| `nats.kdl` | JetStream at both ends: [NATS](@/service/connectors/nats.md) | no: `--features connector-nats` |
| `postgresql.kdl` | logical replication into a sink table: [PostgreSQL](@/service/connectors/postgresql.md) | no: `--features connector-postgresql` |
| `kafka.kdl` | a Kafka source and sink: [Kafka](@/service/connectors/kafka.md) | no: `--features connector-kafka` |
| `tcp.kdl` | a live socket ingest stream: [TCP](@/service/connectors/tcp.md) | yes |
| `saci.kdl` | a service-to-service link at both ends: [SACI](@/service/connectors/saci.md) | yes |
| `http.kdl` | an endpoint spooled through a format: [HTTP](@/service/connectors/http.md) | yes |
| `s3.kdl` | objects read and written in a bucket: [S3](@/service/connectors/s3.md) | no: `--features connector-s3` |
| `turso.kdl` | rows read and written in an embedded or synced database: [Turso](@/service/connectors/turso.md) | no: `--features connector-turso` |
| `redb.kdl` | a stream run persisting its cursors and priors: [Run modes and persistence](@/service/config/run-modes.md) | yes |
| `cluster.kdl` | a three-node cluster template: [Running a cluster](@/service/operate/cluster.md) | no: `--features service-cluster` |
| `extension_example.kdl` | the template for your own source and sink factories: [Embedding saci-service](@/library/service/_index.md) | no: `MongoSource` and `ClickHouseSink` come from no connector crate, so the stock binary cannot run it at any feature level |

Validate one, then serve it:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service validate --config examples/configs/standalone.kdl
saci-service serve --config examples/configs/standalone.kdl
```

The startup banner names the address it bound and the dashboard it mounted:

```text
saci-service listening on 127.0.0.1:8080
dashboard at http://127.0.0.1:8080/ui
```

## 4. Exit codes

| Exit | Condition |
|---|---|
| `0` | Clean exit: a completed `one_shot` run, or a drained shutdown after Ctrl-C or `SIGTERM` |
| `1` | A runner error, a rejected config, or a shutdown that exceeded the 30 second budget |

A `validate` run that names a connector left out of this build prints the
feature to add and exits 1:

```text
ERROR: no source factory registered for type 'PostgresSource' (required by source 'pg_orders'): that is a built-in connector this binary was built without, so rebuild or reinstall with `--features connector-postgresql`, or register your own factory under that name
```

[Install saci-service](@/service/install.md) lists the flag for each of the
five opt-in connectors.

`cluster join` and `cluster leave` change nothing in a binary built with
`--features service-cluster`: both print the manual procedure for a membership
change and exit 0. Every `cluster` subcommand exits 1 in a binary built
without that feature, naming the flag to rebuild with.

A clean stop prints `saci-service stopped cleanly` as its last line. A stop
without that line was not a drain:
[Logs, metrics and traces](@/service/operate/observability.md).

## Next

- [Logs, metrics and traces](@/service/operate/observability.md) is the log
  format, the four probes and the series `/metrics` serves.
- [When it refuses to start, and when it fails](@/service/operate/troubleshooting.md)
  turns a non-zero exit into the line to change.
