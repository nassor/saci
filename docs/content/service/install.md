+++
title = "Install saci-service"
description = "Build the binary from a checkout with no feature flags, add the wasm32-wasip2 target, and confirm the version."
template = "page.html"
weight = 1
aliases = ["/quickstart/installation/"]
+++
# Install saci-service

<dl class="page-facts">
<dt>In one line</dt>
<dd>One build from a checkout, then <code>saci-service serve</code> reads <code>saci.kdl</code></dd>
<dt>You need</dt>
<dd>Rust 1.95 or newer, and <code>git</code></dd>
<dt>Read this if</dt>
<dd>You want the binary on your PATH before running the tutorial</dd>
</dl>

`saci-service` on your PATH and the `wasm32-wasip2` target are everything
[Your first pipeline](@/service/first-pipeline.md) needs.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 176" role="img" aria-labelledby="svc-inst-t svc-inst-d">
        <title id="svc-inst-t">A checkout builds one binary, and one added target lets it host a processor</title>
        <desc id="svc-inst-d">
            A git checkout of the repository feeds one cargo install, which puts the
            saci-service binary in the cargo bin directory that is already on the PATH, so
            the binary answers everywhere. Alongside it, rustup adds the wasm32-wasip2
            target, which is what compiles the WebAssembly processor the first pipeline
            runs. Both arrows end at one ready state: the binary answers its version flag.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="28" width="120" height="52" rx="8"/>
            <text class="t-lbl" x="12" y="50">git clone</text>
            <text class="t-sm" x="12" y="68">nassor/saci</text>
            <path class="arw arw-ctl" d="M120 54 H156" marker-end="url(#svc-inst-c)"/>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="160" y="28" width="190" height="52" rx="8"/>
            <rect class="hd hd-ctl" x="160" y="28" width="190" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="160" y="40" width="190" height="8"/>
            <text class="t-lbl t-ctl" x="172" y="43">cargo install --path</text>
            <text class="t-sm" x="172" y="70">the default bundle, no flags</text>
            <path class="arw arw-ctl" d="M350 54 H386" marker-end="url(#svc-inst-c)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="390" y="28" width="220" height="52" rx="8"/>
            <text class="t-lbl t-ctl" x="402" y="50">saci-service on PATH</text>
            <text class="t-sm" x="402" y="68">in the cargo bin directory</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-bnd" x="160" y="102" width="190" height="46" rx="8"/>
            <text class="t-lbl t-bnd" x="172" y="124">rustup target add</text>
            <text class="t-sm" x="172" y="142">wasm32-wasip2</text>
            <path class="arw arw-bnd" d="M350 125 H386" marker-end="url(#svc-inst-b)"/>
            <text class="t-sm" x="390" y="121">compiles the processor</text>
            <text class="t-sm" x="390" y="139">the first pipeline runs</text>
        </g>
        <defs>
            <marker id="svc-inst-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control-ink)"/>
            </marker>
            <marker id="svc-inst-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> the binary and its build</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
    </div>
</div>

## 1. Install the binary

The crates are not published to crates.io, so install from a checkout. The three
commands run the same on Linux, macOS and Windows (PowerShell):

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
git clone https://github.com/nassor/saci
cd saci
cargo install --path crates/saci-service
```

No feature flags. The default build is what a running service needs on a machine with nothing
else installed: the WebAssembly component runtime, five connectors, five formats and windowed
aggregation.

| Capability | In the default build |
|---|---|
| Sources and sinks | File, redb, HTTP, TCP and in-process channels |
| Formats | csv, ndjson, parquet, avro and arrow-ipc |
| Processors | the WebAssembly component runtime, windowing included |
| PostgreSQL | no: opt in with `--features connector-postgresql` |
| NATS | no: opt in with `--features connector-nats` |
| S3 | no: opt in with `--features connector-s3` |
| Kafka | no: opt in with `--features connector-kafka` |
| Turso | no: opt in with `--features connector-turso` |
| Native plugins | no: opt in with `--features plugin` |
| Cluster mode | no: opt in with `--features service-cluster` |

`cargo install` writes the binary into the cargo bin directory, which the Rust
installer already put on your PATH, so `saci-service` answers in any shell.

## 2. Add wasm32-wasip2

A processor is a WebAssembly component, and `rustc` builds one only when the
target is installed. [Your first pipeline](@/service/first-pipeline.md) compiles
one, so add it now:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
rustup target add wasm32-wasip2
```

The command downloads and installs the standard library for that target, and
exits 0. Run it a second time to see it confirm the target is present:

```text
info: component rust-std for target wasm32-wasip2 is up to date
```

## 3. Check it

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
saci-service --version
```

Expected output:

```text
saci-service 0.1.0
```

A `command not found` here means the cargo bin directory is not on your PATH.
`rustup` prints it as part of its own install output; add it and reopen the shell.

## What stays opt-in

A connector ships by default only when nothing has to be installed or already running for it to
work. PostgreSQL, NATS, S3, Kafka and Turso each need their own database, broker or object store
running before the connector does anything, so each sits outside the default build alongside
native plugins and cluster mode. Add one by reinstalling with its flag, from the same checkout:

- PostgreSQL: `--features connector-postgresql`.
- NATS: `--features connector-nats`.
- S3: `--features connector-s3`.
- Kafka needs `cmake` and a C toolchain too, because its client library builds
  vendored C. Install with `--features connector-kafka` when you need
  [Kafka](@/service/connectors/kafka.md).
- Turso needs no extra toolchain, but its synced mode pulls hyper and rustls and replicates from a
  Turso endpoint, so it stays opt-in. Install with `--features connector-turso`.
- Native plugins replace the sandbox with a shared library, so opting in is an
  explicit choice. Install with `--features plugin`, then follow
  [A Rust plugin](@/service/plugins/rust.md).
- Cluster mode carries the replication stack, so a cluster node is a
  deliberate deployment choice. Install with `--features service-cluster`, then
  follow [Running a cluster](@/service/operate/cluster.md).

`--features all` turns on every capability at once: all five connectors above, native plugins and
cluster mode. It excludes only `conformance`, the Arrow IPC conformance corpus generator's own
switch, not a capability a running service needs.

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
cargo install --path crates/saci-service --features connector-kafka
```

`cargo install` replaces whatever it installed before, so the flag is the whole
difference and the command is the same on every platform.

## Next

- [Your first pipeline](@/service/first-pipeline.md) builds a processor, runs it
  through `saci-service`, and reads the result. About fifteen minutes.
- [The command line](@/service/operate/_index.md) is every subcommand, flag and
  environment variable the binary you just built accepts.
