# Branching: three fan-out splits in one workflow

Every `link` in a workflow delivers by default, so a node's output reaches all
of its downstream links at once. A `branch` name on a link makes delivery
conditional: the upstream processor names branches for the batch it just
produced, and the host delivers that batch only to the links carrying those
names. `docs/content/service/processors/branching.md` covers what routing is,
when to use it and what load-time validation refuses. This file is the
commands.

`branching.kdl` is a long-running `saci-service` stream carrying every way
output fans out, each split to its own sink:

| Split | Edge | Where each batch goes |
|-------|------|----------------------|
| Source | `in` to `out_mirror`, `router_wasm`, `router_plugin` | Every downstream, unconditionally |
| WASM processor | `router_wasm` to `out_wasm_high` / `out_wasm_low` | Only the branch the decision names |
| Plugin | `router_plugin` to `out_plugin_premium` / `out_plugin_standard` | Only the branch the decision names |

A NATS core subject feeds the workflow in `run_mode kind="stream"`, so every
message is its own batch and the routers decide per message. The publisher
(`branching_publish`, a `saci-service` example) draws priority 50/50 between
`"high"` and `"low"`, so both branches of both processor splits fire
continuously while it runs.

## Prerequisites

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- A Docker daemon, for the NATS container

## Build the processors

The same two commands work on every platform, from the repository root:

```text
cargo build --release -p branching-wasm --target wasm32-wasip2
cargo build --release -p branching-plugin
```

The plugin artifact name is platform specific. The config's default library
path (`target/release/libbranching_plugin.so`) is the Linux name, so only macOS
and Windows need `SACI_PLUGIN_LIB`:

| Platform | Plugin artifact | `SACI_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | `target/release/libbranching_plugin.so` | not needed (config default) |
| macOS | `target/release/libbranching_plugin.dylib` | `target/release/libbranching_plugin.dylib` |
| Windows | `target/release/branching_plugin.dll` | `target/release/branching_plugin.dll` |

## Run it

Start NATS, then the service, then the publisher. `SACI_OUT_DIR` names the
directory the five sink files land in; it must exist before `serve` starts.

1. Start NATS:

```text
docker run -d --name saci-nats -p 4222:4222 nats:2.11-alpine
```

Runs the same on all three platforms.

Stop it with `docker rm -f saci-nats`.

2. Start the service. NATS is an opt-in connector too.

Linux:

```text
mkdir -p /tmp/saci-branching
SACI_OUT_DIR=/tmp/saci-branching \
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- serve \
  --config examples/branching/branching.kdl
```

macOS:

```text
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

The config's paths use forward slashes, which work on Windows too; the
`C:/saci-branching` output path above follows the same rule.

3. Publish, in another terminal. Runs the same on all three platforms:

```text
cargo run -p saci-service --features connector-nats --example branching_publish -- --rate 50
```

## What each file holds

The publisher's messages carry `priority "high"` or `priority "low"`.

| Output | Holds |
|--------|-------|
| `out-mirror.csv` | every message, in arrival order |
| `out-wasm-high.csv` | the `"high"` messages |
| `out-wasm-low.csv` | the `"low"` messages |
| `out-plugin-premium.csv` | the `"high"` messages |
| `out-plugin-standard.csv` | the `"low"` messages |

`out_mirror` proves the source split: every batch is written there verbatim
while the same batch feeds both routers. The wasm router
(`examples/branching/wasm`) reads the first row's priority and inserts a
`RouteDecision` naming branch `high` or `low`; the plugin
(`examples/branching/plugin`) maps the same values to branches `premium` and
`standard`. The host delivers each batch only to the links whose `branch` the
decision names, so the sinks stay separate. The CSV header is written
once, before the first batch, so each file stays a readable table however long
the stream runs.

The publisher's flags: `--count` (0 runs until Ctrl-C, the default), `--rate`
messages per second (default 50), `--url` (default `nats://localhost:4222`),
`--subject` (default `branching.orders`), `--seed`.

Validate the config first, if you like (set `SACI_PLUGIN_LIB` as in the table
above on macOS and Windows):

```text
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- validate \
  --config examples/branching/branching.kdl --strict
```
Windows (PowerShell), on one line:

```powershell
cargo run -p saci-service --features connector-file,connector-nats,transformer-csv,wasm,plugin -- validate --config examples/branching/branching.kdl --strict
```

## Files

| File / directory | What it is |
|------------------|------------|
| `branching.kdl` | the stream workflow with all three fan-out splits |
| `branching_publish.rs` | the `saci-service` example that feeds the NATS subject |
| `wasm/` | the `branching-wasm` processor component (cdylib, wasm32-wasip2) |
| `plugin/` | the `branching-plugin` native plugin (cdylib) |
