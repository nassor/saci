+++
title = "Branching"
description = "Run the branching example: one NATS stream, three fan-out splits, and five CSV files to check."
template = "page.html"
weight = 1
aliases = ["/service/branching/"]
+++
# Branching

<dl class="page-facts">
<dt>What it is</dt>
<dd>Delivery a processor decides: <strong>one list of branch names per pass</strong></dd>
<dt>Reach for it when</dt>
<dd>One inbound stream has to leave through <strong>different sinks by content</strong></dd>
<dt>Granularity</dt>
<dd>The <strong>pass</strong>, not the row: one decision per admitted chunk, whatever row count it carries</dd>
</dl>

Every `link` delivers by default: a node's output reaches all of its downstream
links. Branching makes that conditional. A processor names one or more branches
for the batch it just produced, and the host delivers that batch only to the
links carrying those names. The host never looks at a row to decide.

Flow control can slice one arriving batch into several passes. Each pass
gets its own routing decision, so two rows from the same arrival can leave
through different sinks if they land in different passes: see
[Flow control](@/service/operate/flow-control.md).

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 246" role="img" aria-labelledby="brc-title brc-desc">
        <title id="brc-title">A processor returns a branch name and the host delivers to that link alone</title>
        <desc id="brc-desc">
            The source orders_in hands one batch to the processor classify, which is a
            WebAssembly component or a native plugin. classify returns a routing decision
            naming the single branch eu. Two labelled links leave it: one labelled eu to the
            sink eu_out and one labelled us to the sink us_out. Only the eu link carries this
            batch, drawn in the data-plane colour; the us link is drawn muted because the
            decision did not name it, so us_out receives nothing from this batch.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="76" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="76" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="88" width="130" height="8"/>
            <text class="t-lbl" x="12" y="91">orders_in</text>
            <text class="t-sm" x="12" y="114">one batch</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M130 104 H210" marker-end="url(#brc-d)"/>
            <rect class="blk blk-bnd" x="210" y="66" width="190" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="66" width="190" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="78" width="190" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="81">classify</text>
            <text class="t-sm" x="222" y="104">wasm or plugin</text>
            <text class="t-sm t-bnd" x="222" y="124">RouteDecision(["eu"])</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M400 104 C450 104 450 54 500 54" marker-end="url(#brc-d)"/>
            <text class="t-sm t-data t-end" x="492" y="48">eu</text>
            <rect class="blk blk-data" x="500" y="30" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="500" y="30" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="500" y="42" width="160" height="8"/>
            <text class="t-lbl" x="512" y="45">eu_out</text>
            <text class="t-sm" x="512" y="66">carries this batch</text>
        </g>
        <g class="anim anim-4">
            <path class="arw" d="M400 104 C450 104 450 154 500 154" marker-end="url(#brc-m)"/>
            <text class="t-sm t-end" x="492" y="148">us</text>
            <rect class="blk" x="500" y="130" width="160" height="48" rx="8"/>
            <rect class="hd" x="500" y="130" width="160" height="20" rx="8"/>
            <rect class="hd" x="500" y="142" width="160" height="8"/>
            <text class="t-lbl" x="512" y="145">us_out</text>
            <text class="t-sm" x="512" y="166">not named, gets nothing</text>
            <path class="ln" d="M0 202 H654"/>
            <text class="t-sm" x="0" y="222">The host reads the decision after the systems run, then walks the node's outbound links once.</text>
            <text class="t-sm" x="0" y="238">A node labels every outbound link or none, so an unconditional copy is taken upstream of the router.</text>
        </g>
        <defs>
            <marker id="brc-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="brc-m" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--muted-foreground)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane: the batch and the link it took</span>
        <span class="k-boundary"><i></i> the processor, and the decision it returns</span>
        <span class="k-mute"><i></i> a link this batch did not select</span>
    </div>
    <figcaption class="dgm-cap">
        Routing is a filter over one node's outbound edges. Sinks receive the same Arrow
        buffers either way, so a second branch costs no re-encoding.
    </figcaption>
</div>

## What the example demonstrates

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 366" role="img" aria-labelledby="br-title br-desc">
        <title id="br-title">One source multicasts to three nodes, and each router writes one of its two branches</title>
        <desc id="br-desc">
            The NATS source node in feeds three downstream nodes over unlabelled links: the
            sink out_mirror, the WebAssembly processor router_wasm and the native plugin
            router_plugin. router_wasm has two labelled links, high to out_wasm_high and low
            to out_wasm_low, and router_plugin has two more, premium to out_plugin_premium
            and standard to out_plugin_standard. Each router writes only the branch its
            decision names, so the five CSV files divide the stream between them.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="100" width="124" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="100" width="124" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="112" width="124" height="8"/>
            <text class="t-lbl" x="12" y="115">in</text>
            <text class="t-sm" x="12" y="138">NatsSource</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M124 128 C167 128 167 54 210 54" marker-end="url(#br-d)"/>
            <path class="arw arw-data" d="M124 128 H210" marker-end="url(#br-d)"/>
            <path class="arw arw-data" d="M124 128 C167 128 167 198 210 198" marker-end="url(#br-d)"/>
            <rect class="blk blk-data" x="210" y="30" width="150" height="48" rx="8"/>
            <rect class="hd hd-data" x="210" y="30" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="210" y="42" width="150" height="8"/>
            <text class="t-lbl" x="222" y="45">out_mirror</text>
            <text class="t-sm" x="222" y="66">out-mirror.csv</text>
            <rect class="blk blk-bnd" x="210" y="100" width="150" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="100" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="112" width="150" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="115">router_wasm</text>
            <text class="t-sm" x="222" y="138">wasm component</text>
            <rect class="blk blk-bnd" x="210" y="170" width="150" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="170" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="182" width="150" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="185">router_plugin</text>
            <text class="t-sm" x="222" y="208">native plugin</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M360 128 C425 128 425 54 490 54" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="48">high</text>
            <path class="arw arw-data" d="M360 128 C425 128 425 124 490 124" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="118">low</text>
            <rect class="blk blk-data" x="490" y="30" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="30" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="42" width="160" height="8"/>
            <text class="t-lbl" x="502" y="45">out_wasm_high</text>
            <text class="t-sm" x="502" y="66">out-wasm-high.csv</text>
            <rect class="blk blk-data" x="490" y="100" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="100" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="112" width="160" height="8"/>
            <text class="t-lbl" x="502" y="115">out_wasm_low</text>
            <text class="t-sm" x="502" y="136">out-wasm-low.csv</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M360 198 C425 198 425 194 490 194" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="188">premium</text>
            <path class="arw arw-data" d="M360 198 C425 198 425 264 490 264" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="240">standard</text>
            <rect class="blk blk-data" x="490" y="170" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="170" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="182" width="160" height="8"/>
            <text class="t-lbl" x="502" y="185">out_plugin_premium</text>
            <text class="t-sm" x="502" y="206">out-plugin-premium.csv</text>
            <rect class="blk blk-data" x="490" y="240" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="240" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="252" width="160" height="8"/>
            <text class="t-lbl" x="502" y="255">out_plugin_standard</text>
            <text class="t-sm" x="502" y="276">out-plugin-standard.csv</text>
            <path class="ln" d="M0 316 H654"/>
            <text class="t-sm" x="0" y="336">An unlabelled link always delivers, so out_mirror, router_wasm and router_plugin each see every message.</text>
            <text class="t-sm" x="0" y="352">A labelled link delivers only when that router's decision names its branch.</text>
        </g>
        <defs>
            <marker id="br-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the two processor runtimes</span>
    </div>
</div>

| Split | Edge | Where each batch goes |
|-------|------|----------------------|
| Source | `in` → `out_mirror`, `router_wasm`, `router_plugin` | Every downstream, unconditionally |
| WASM processor | `router_wasm` → `out_wasm_high` / `out_wasm_low` | Only the branch the decision names |
| Plugin | `router_plugin` → `out_plugin_premium` / `out_plugin_standard` | Only the branch the decision names |

The publisher draws `priority` 50/50 between `"high"` and `"low"`. Both routers
read the first row's priority: the wasm one names branch `high` or `low`, the
plugin maps the same values to `premium` and `standard`. One publisher therefore
fills four branch files, and `out_mirror` keeps a verbatim copy of the whole
stream. Seven `link` lines are the whole graph:

```kdl,name=Seven links are the whole graph
link from="in" to="out_mirror"
link from="in" to="router_wasm"
link from="in" to="router_plugin"

link from="router_wasm" to="out_wasm_high" branch="high"
link from="router_wasm" to="out_wasm_low" branch="low"

link from="router_plugin" to="out_plugin_premium" branch="premium"
link from="router_plugin" to="out_plugin_standard" branch="standard"
```

The source's three links carry no `branch`, so they multicast unconditionally.
A processor labels every outbound link or none; the load-time rules behind that are in
[workflows and links](@/service/config/workflows.md).

## Prerequisites

- A clone of the repository, from the root of which every command runs.
- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`.
- Docker for the NATS broker.
- NATS is an opt-in connector too: the serve and validate commands
  below pass `--features connector-file,connector-nats,transformer-csv,wasm,plugin`.

## 1. Build the processors

```bash,name=Build both processors
cargo build --release -p branching-wasm --target wasm32-wasip2
cargo build --release -p branching-plugin
```

Runs the same on Linux, macOS and Windows (PowerShell). The plugin artifact name
is platform specific. The config defaults to the Linux name, so only macOS and
Windows need `SACI_PLUGIN_LIB`:

| Platform | Plugin artifact | `SACI_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | `target/release/libbranching_plugin.so` | not needed (config default) |
| macOS | `target/release/libbranching_plugin.dylib` | `target/release/libbranching_plugin.dylib` |
| Windows | `target/release/branching_plugin.dll` | `target/release/branching_plugin.dll` |

## 2. Validate the config

A mislabelled graph is caught before anything runs. On macOS and Windows set
`SACI_PLUGIN_LIB` as in the table above:

Linux:

```bash,name=Validate the branching config
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- validate \
  --config examples/branching/branching.kdl --strict
```

macOS:

```bash,name=Validate with the dylib name
SACI_PLUGIN_LIB=target/release/libbranching_plugin.dylib \
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- validate \
  --config examples/branching/branching.kdl --strict
```

Windows (PowerShell):

```powershell
$env:SACI_PLUGIN_LIB = "target/release/branching_plugin.dll"
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- validate --config examples/branching/branching.kdl --strict
```

Expected output ends with `OK: all declared types resolved in built-in
registry`.

## 3. Start NATS

```bash,name=Start NATS
docker run -d --name saci-nats -p 4222:4222 nats:2.11-alpine
```

Runs the same on all three platforms. Stop it later with
`docker rm -f saci-nats`.

## 4. Start the service

`SACI_OUT_DIR` names the directory the five sink files land in, and it must
exist before `serve` starts. The config writes forward-slash paths, which work
on Windows too, so `C:/saci-branching` is fine as it stands.

Linux:

```bash,name=Start the service
mkdir -p /tmp/saci-branching

SACI_OUT_DIR=/tmp/saci-branching \
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- serve \
  --config examples/branching/branching.kdl
```

macOS:

```bash,name=Start the service with the dylib name
mkdir -p /tmp/saci-branching

SACI_OUT_DIR=/tmp/saci-branching \
SACI_PLUGIN_LIB=target/release/libbranching_plugin.dylib \
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- serve \
  --config examples/branching/branching.kdl
```

Windows (PowerShell):

```powershell
mkdir C:\saci-branching

$env:SACI_OUT_DIR = "C:/saci-branching"
$env:SACI_PLUGIN_LIB = "target/release/branching_plugin.dll"
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- serve --config examples/branching/branching.kdl
```

The stream runner waits for messages, so no output files exist yet. Leave this
terminal running.

## 5. Run the publisher

In another terminal:

```bash,name=Run the publisher
cargo run -p saci-service --features connector-nats --example branching_publish -- --rate 50
```

Runs the same on all three platforms. The publisher's flags: `--count` (0 runs
until Ctrl-C, the default), `--rate` messages per second (default 50), `--url`
(default `nats://localhost:4222`), `--subject` (default `branching.orders`),
`--seed`.

## 6. Check the output files

After a few seconds all five files exist and grow while the publisher runs.
Every sink in this config sets `truncate #true`, so each `serve` starts its
five files over, then appends and writes its CSV header once. The mirror's
line count splitting across each pair is the proof:

Linux/macOS:

```bash,name=Confirm each branch got its share
wc -l /tmp/saci-branching/*.csv
```

Windows (PowerShell):

```powershell
Get-ChildItem C:\saci-branching\*.csv | ForEach-Object {
  "$($_.Name): $((Get-Content $_.FullName).Count) lines"
}
```

| Output | Holds |
|--------|-------|
| `out-mirror.csv` | every message, in arrival order |
| `out-wasm-high.csv` | the `"high"` messages |
| `out-wasm-low.csv` | the `"low"` messages |
| `out-plugin-premium.csv` | the `"high"` messages |
| `out-plugin-standard.csv` | the `"low"` messages |

Each router pair partitions the same stream under different names, so
`out-wasm-high.csv` and `out-plugin-premium.csv` hold the same rows. Stop the
publisher and the service with Ctrl-C when you are done.

## Files

| Path | What it is |
|------|-----------|
| `examples/branching/branching.kdl` | the workflow: one source, two routers, five sinks, seven links |
| `examples/branching/wasm/` | `branching-wasm`, the WebAssembly router (component) |
| `examples/branching/plugin/` | `branching-plugin`, the native router (cdylib) |
| `examples/branching/branching_publish.rs` | the publisher, a `saci-service` example |

## Next

- [Windowing](@/service/processors/windowing/_index.md): the other thing a processor node declares in
  its own block.
- [The live dashboard](@/service/operate/dashboard.md): the branch edges, drawn with their live
  rates.
