+++
title = "Your first pipeline"
description = "Build a Rust WebAssembly processor, run it through saci-service with a minimal file-based config, and read the result. About 15 minutes, no Docker needed."
template = "page.html"
weight = 2
aliases = ["/quickstart/running-it/"]
+++
# Your first pipeline

<dl class="page-facts">
<dt>In one line</dt>
<dd>CSV in, one <strong>WebAssembly processor</strong>, CSV out, and a live dashboard on <code>/ui</code></dd>
<dt>You need</dt>
<dd>Everything from <a href="../install/">Install saci-service</a>, and a checkout of the repository</dd>
<dt>Read this if</dt>
<dd>You want to see a real workflow move rows before reading any other page</dd>
</dl>

You build one processor component, the Rust port of the `scheduler_etl`
example, and run it under `saci-service` with a config you write yourself. A CSV
file of five transactions goes in, the processor validates each row and converts
it to USD, and a CSV file comes out. The whole route needs no Docker, no Go and
no .NET.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 168" role="img" aria-labelledby="svc-fp-t svc-fp-d">
        <title id="svc-fp-t">The workflow this page builds: one CSV file in, one processor, one CSV file out</title>
        <desc id="svc-fp-d">
            The fixture file order_processing_input.csv feeds the source node csv_orders.
            That source links to the wasm node process_orders, drawn on the WebAssembly
            boundary, which fills the valid and usd_amount columns. The processor links to
            the sink node csv_out, which writes saci-first-out.csv. Both the source and the
            sink decode and encode through the same declared transformer, csv_fmt.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="44" width="118" height="56" rx="8"/>
            <text class="t-lbl" x="10" y="66">orders.csv</text>
            <text class="t-sm" x="10" y="84">5 rows in</text>
            <path class="arw arw-data" d="M118 72 H140" marker-end="url(#svc-fp-a)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-data" x="144" y="44" width="126" height="56" rx="8"/>
            <rect class="hd hd-data" x="144" y="44" width="126" height="20" rx="8"/>
            <rect class="hd hd-data" x="144" y="56" width="126" height="8"/>
            <text class="t-lbl t-data" x="154" y="59">source</text>
            <text class="t-sm" x="154" y="82">csv_orders</text>
            <path class="arw arw-data" d="M270 72 H292" marker-end="url(#svc-fp-a)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="296" y="44" width="140" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="296" y="44" width="140" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="296" y="56" width="140" height="8"/>
            <text class="t-lbl t-bnd" x="306" y="59">wasm</text>
            <text class="t-sm" x="306" y="82">process_orders</text>
            <path class="arw arw-data" d="M436 72 H458" marker-end="url(#svc-fp-a)"/>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-data" x="462" y="44" width="90" height="56" rx="8"/>
            <rect class="hd hd-data" x="462" y="44" width="90" height="20" rx="8"/>
            <rect class="hd hd-data" x="462" y="56" width="90" height="8"/>
            <text class="t-lbl t-data" x="472" y="59">sink</text>
            <text class="t-sm" x="472" y="82">csv_out</text>
            <path class="arw arw-data" d="M552 72 H570" marker-end="url(#svc-fp-a)"/>
            <rect class="blk blk-data" x="574" y="44" width="86" height="56" rx="8"/>
            <text class="t-lbl" x="584" y="66">orders</text>
            <text class="t-sm" x="584" y="84">_out.csv</text>
            <path class="ln" d="M0 126 H654"/>
            <text class="t-sm" x="0" y="148">One declared transformer, csv_fmt, is what both the source and the sink name.</text>
        </g>
        <defs>
            <marker id="svc-fp-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
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

## 1. Build the processor component

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
cargo build --release -p order-processing-wasm --target wasm32-wasip2
```

`rustc` links a `wasm32-wasip2` cdylib into a Component Model component
itself, so plain `cargo build` is the whole toolchain. Confirm the finished
component landed:

Linux/macOS:

```bash
ls -l target/wasm32-wasip2/release/order_processing_wasm.wasm
```

Windows (PowerShell):

```powershell
Get-Item target\wasm32-wasip2\release\order_processing_wasm.wasm | Select-Object Length, Name
```

The component exports two functions, `describe()` and `run-batch`, which is
the whole [WIT contract](@/service/processors/build/wit-contract.md).
Listing those exports yourself needs one more tool, and
[Build your own processor](@/service/processors/build/_index.md) has the two
commands. Step 3 checks the same thing with `saci-service validate`.

## 2. Write a minimal config

Create `my-first-saci.kdl` in the repository root. Every key comes from
`examples/configs/standalone_wasm.kdl` and `examples/configs/standalone.kdl`,
which use the same shape:

```kdl,name=my-first-saci.kdl
mode "standalone"

node id=1 name="first-pipeline" data_dir="${SACI_DATA_DIR:-/tmp/saci-first}"

run_mode kind="interval" interval_ms=5000

workflow "orders" {
    transformer "csv_fmt" format="csv" {
        options has_headers=#true
    }

    source "csv_orders" type="FileSource" component="Transaction" transformer="csv_fmt" {
        config {
            path "examples/configs/fixtures/order_processing_input.csv"
            schema_fields "id" type="UInt64" nullable=#false
            schema_fields "amount" type="Float64" nullable=#false
            schema_fields "currency" type="Utf8" nullable=#false
            schema_fields "valid" type="Boolean" nullable=#false
            schema_fields "usd_amount" type="Float64" nullable=#false
        }
    }

    wasm "process_orders" module="target/wasm32-wasip2/release/order_processing_wasm.wasm" {
        config fx_eur="1.08" fx_gbp="1.27" fx_jpy="0.0067" fx_cad="0.74"
    }

    sink "csv_out" type="FileSink" component="Transaction" transformer="csv_fmt" {
        config {
            path "/tmp/saci-first-out.csv"
            truncate #true
            schema_fields "id" type="UInt64" nullable=#false
            schema_fields "amount" type="Float64" nullable=#false
            schema_fields "currency" type="Utf8" nullable=#false
            schema_fields "valid" type="Boolean" nullable=#false
            schema_fields "usd_amount" type="Float64" nullable=#false
        }
    }

    link from="csv_orders" to="process_orders"
    link from="process_orders" to="csv_out"
}

http bind="127.0.0.1:8080"

observability log_format="pretty" log_level="info"
```

What each part does:

- `mode "standalone"` runs one process with no distributed coordination.
- `run_mode kind="interval" interval_ms=5000` re-runs the workflow every five
  seconds, so the service stays alive between iterations. `kind="one_shot"`
  would exit after the first iteration instead:
  [Run modes and persistence](@/service/config/run-modes.md).
- One `workflow` node declares the whole graph. The `transformer "csv_fmt"`
  node names the byte format; both the source and the sink reference it by id.
- The `source` is a `FileSource` reading `order_processing_input.csv`, decoded
  into the five fields of the `Transaction` component.
- The `wasm` node names the component you built. The `config` block holds the
  FX rates the processor reads back at run time.
- The `sink` is a `FileSink`; `truncate #true` replaces the output file on
  every start instead of appending to it.
- Each `link` is one edge of the graph: source to processor, processor to sink.
- `http bind="127.0.0.1:8080"` turns on the control plane, which is also what
  serves the dashboard in step 7, and `observability` sets the log format and
  level.

The paths are relative to the directory you run from, so run everything from
the repository root. The output directory must exist before `FileSink` opens
the file. On Linux and macOS `/tmp` always does; on Windows the path
`/tmp/saci-first-out.csv` resolves to `C:\tmp\saci-first-out.csv` on the current
drive, so create it first:

Linux/macOS: nothing to do.

Windows (PowerShell):

```powershell
New-Item -ItemType Directory -Force C:\tmp
```

## 3. Validate the config

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service validate -c my-first-saci.kdl
```

`validate` reads the config, compiles the component, calls `describe()`, and
walks every `link` checking that both ends agree on the schema. Expected
output:

```text
OK: workflow graph validated (components and schemas agree end to end)
```

It exits 0. A processor that reports a different component list or schema
fingerprint fails here rather than at the first batch.

## 4. Run it

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service serve -c my-first-saci.kdl
```

The service starts, drains the CSV into the dataset, hands the batch to the
processor, and writes the result. Five seconds later it drains again, finds
the file source at EOF, and idles. Leave it running.

## 5. Observe the service

In a second terminal, ask the control plane:

Linux/macOS:

```bash
curl http://127.0.0.1:8080/health
```

Windows (PowerShell):

```powershell
Invoke-RestMethod http://127.0.0.1:8080/health
```

Expected output, a JSON document with a live `liveness_counter` that ticks up
once per second:

```json
{"status":"alive","uptime_seconds":7,"liveness_counter":7}
```

`/status` carries the workflow counters:

```json
{"node_id":1,"node_name":"first-pipeline","mode":"standalone","uptime_seconds":7,"build":{"version":"0.1.0"},"cluster":null,"standalone":[{"workflow_id":"orders","iterations":1,"rows_processed":5,"source_batches_drained":1,"sink_batches_written":1,"iteration_errors":0,"total_busy_micros":0,"max_item_micros":0}]}
```

`/ready` returns `{"status":"ready"}`, and `/metrics` serves the Prometheus
exposition. Every probe is described on
[Logs, metrics and traces](@/service/operate/observability.md).

## 6. Read the result

The sink wrote `saci-first-out.csv` (on Windows, `C:\tmp\saci-first-out.csv`).
Open it. The input fixture has five rows:

```text
id,amount,currency,valid,usd_amount
1,120.0,EUR,false,0.0
2,4300.0,USD,false,0.0
3,75.5,GBP,false,0.0
4,-50.0,EUR,false,0.0
5,900000.0,JPY,false,0.0
```

The output carries the same five rows with both empty columns filled:

```text
id,amount,currency,valid,usd_amount
1,120.0,EUR,true,129.60000000000002
2,4300.0,USD,true,4300.0
3,75.5,GBP,true,95.885
4,-50.0,EUR,false,-54.0
5,900000.0,JPY,true,6030.0
```

`valid` is `true` where the amount is positive, and `usd_amount` is the amount
converted at the configured rates (EUR 1.08, GBP 1.27, JPY 0.0067, USD 1.0).
Row 4 has a negative amount, so its `valid` stays `false` while the conversion
still runs.

## 7. Open the dashboard

The `http` block you wrote in step 2 already serves the dashboard, on the same
port as the probes. With the service still running, open
`http://127.0.0.1:8080/ui` in a browser. The startup log line
`dashboard at http://127.0.0.1:8080/ui` confirms it is mounted.

<img src="../../first-pipeline/dashboard.png" alt="The Pipelines tab of the dashboard draws the orders workflow as one card: the csv_orders source box, the process_orders wasm box and the csv_out sink box joined by two edges, each box carrying its own counter, with the sources and sinks tables and the throughput chart underneath.">

The Pipelines tab draws one card per declared workflow, so this config draws
one: three boxes and two edges. The source and sink boxes carry their records
per second and the processor box its mean batch time, and the tables below add
each source's admission target and each sink's backlog. A file source drains
once and then idles, so the rates settle back to zero while the mean batch time
stays. Clicking a box opens its detail sheet.
[The live dashboard](@/service/operate/dashboard.md) is every tab in it.

Stop the service with Ctrl-C. `serve` shuts down between iterations, so an
interrupt never lands mid-batch.

## Next

The route above moves a static file. The full Quick Start in
[`examples/quickstart/`](https://github.com/nassor/saci/tree/main/examples/quickstart)
moves live data. A publisher writes card authorisations to a NATS subject. One
`saci-service` process runs two linked WebAssembly processors in two languages
against the same in-memory dataset, upserts the result into PostgreSQL, and
serves a live dashboard on port 8080.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 212" role="img" aria-labelledby="qs-title qs-desc">
        <title id="qs-title">The Quick Start stack: a publisher, one saci-service running two processors, PostgreSQL</title>
        <desc id="qs-desc">
            A publisher sends NDJSON authorisations to the NATS subject
            authorizations.raw. One saci-service process, configured by
            quickstart.kdl, reads that subject and runs two linked WebAssembly
            processors against the same in-memory dataset. The Go processor
            validate-go.wasm writes the valid column, then the C# processor
            settle-cs.wasm writes the fee and review_tier columns. The service
            upserts all twelve columns into the PostgreSQL table
            public.settlements and serves one dashboard on port 8080.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="56" width="136" height="68" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="136" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="68" width="136" height="8"/>
            <text class="t-lbl" x="12" y="71">publisher</text>
            <text class="t-sm" x="12" y="92">NDJSON</text>
            <text class="t-sm" x="12" y="108">authorizations.raw</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M136 90 H160" marker-end="url(#qs-d)"/>
            <rect class="blk blk-bnd" x="166" y="36" width="360" height="112" rx="8"/>
            <rect class="hd hd-bnd" x="166" y="36" width="360" height="22" rx="8"/>
            <rect class="hd hd-bnd" x="166" y="50" width="360" height="8"/>
            <text class="t-lbl t-bnd" x="178" y="51">saci-service &middot; quickstart.kdl</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="178" y="68" width="152" height="44" rx="6"/>
            <text class="t-lbl" x="188" y="86">validate-go.wasm</text>
            <text class="t-sm" x="188" y="102">writes valid</text>
            <path class="arw arw-data" d="M330 90 H342" marker-end="url(#qs-d)"/>
            <rect class="blk blk-bnd" x="346" y="68" width="168" height="44" rx="6"/>
            <text class="t-lbl" x="356" y="86">settle-cs.wasm</text>
            <text class="t-sm" x="356" y="102">writes fee, review_tier</text>
            <text class="t-sm t-ctl" x="178" y="134">/ui on :8080</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M526 90 H556" marker-end="url(#qs-d)"/>
            <rect class="blk blk-data" x="562" y="64" width="98" height="52" rx="8"/>
            <text class="t-lbl" x="574" y="86">Postgres</text>
            <text class="t-sm" x="574" y="104">settlements</text>
            <path class="ln" d="M0 166 H654"/>
            <text class="t-sm" x="0" y="186">One process, one workflow. NATS carries the inbound NDJSON only.</text>
            <text class="t-sm" x="0" y="202">Between the two processors the dataset stays in memory, so nothing is re-encoded.</text>
        </g>
        <defs>
            <marker id="qs-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
        <span class="k-control"><i></i> control plane</span>
    </div>
    <figcaption class="dgm-cap">
        Two <code>wasm</code> nodes joined by an explicit <code>link</code> run
        two processors against the same dataset, with nothing re-encoded between
        them. Both must declare the same components and report the same Arrow
        schema fingerprint.
    </figcaption>
</div>

It needs Docker (NATS 2.11 and PostgreSQL 18), the Go and C# toolchains from
[A Go processor](@/service/processors/build/go.md) and
[A C# processor](@/service/processors/build/csharp.md), and about ten minutes.
The commands, the `quickstart.kdl` config, and the expected `review_tier`
breakdown are all in
[`examples/quickstart/README.md`](https://github.com/nassor/saci/blob/main/examples/quickstart/README.md).

Two pages carry on from here:

- [The config file](@/service/config/_index.md) is every key you can put beside
  the ones you just wrote.
- [Build your own processor](@/service/processors/build/_index.md) replaces the
  Rust component with one in your own language.
